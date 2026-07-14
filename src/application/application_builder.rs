use std::{any::{Any, TypeId}, collections::HashMap};
use futures::Stream;
#[cfg(feature = "logging")]
use tracing::Level;
#[cfg(feature = "tracing")]
use tracing::info;
#[cfg(feature = "tracing")]
use tracing_subscriber::fmt;

use crate::{
    EventStream, activities::{
        ActivityState, AnyActivity, Activity
    }, application::Application, events::{
        ApplicationEvent, 
        EventHandlerReturn
    }
};




pub struct ApplicationBuilder<State> {
    pub(super) state: State,
    pub(super) starting_activity: Option<Box<dyn ActivityState>>,
    pub(super) activities: HashMap<TypeId, Box<dyn AnyActivity<State>>>,
    pub(super) event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent) -> EventHandlerReturn>>,
    pub(super) event_producers: Vec<EventStream<Box<dyn ApplicationEvent>>>,
    pub(super) post_event: Option<Box<dyn FnMut(&mut State, &dyn ApplicationEvent)>>,
    pub(super) has_backstack: bool,
    pub(super) log_file: Option<String>,
}

impl <State> ApplicationBuilder<State> 
where 
    State: 'static + Send,
{
    pub fn add_activity<A: ActivityState>(mut self, activity: Activity<A, State>) -> Self {
        self.activities.insert(TypeId::of::<A>(), Box::new(activity));
        self
    }

    pub fn add_event_producer<S>(mut self, producer: S) -> Self
    where
        S: Stream<Item = Box<dyn ApplicationEvent>> + Send + 'static,
    {
        self.event_producers.push(Box::pin(producer));
        self
    }

    pub fn on_event<F, E: ApplicationEvent>(mut self, mut handler: F) -> Self
    where
        F: FnMut(&mut State, &E) -> EventHandlerReturn + Send + 'static,
    {
        self.event_handlers.insert(TypeId::of::<E>(), Box::new(move |state, event| {
            // Trait upcasting (stable since Rust 1.76)
            let concrete = (event as &dyn Any).downcast_ref::<E>().unwrap();
            handler(state, concrete)
        }));
        self
    }

    pub fn post_event<F>(mut self, handler: F) -> Self
    where
        F: FnMut(&mut State, &dyn ApplicationEvent) + Send + 'static,
    {
        self.post_event = Some(Box::new(handler));
        self
    }

    pub fn backstack(mut self) -> Self {
        self.has_backstack = true;
        self
    }

    pub fn starting_activity<A: ActivityState>(mut self, activity: A) -> Self {
        self.starting_activity = Some(Box::new(activity) as Box<(dyn ActivityState + 'static)>);
        self
    }

    #[cfg(feature = "logging")]
    pub fn log_file(mut self, _log_level: Level, _directory_path: &str, file_path: &str) -> Self {
        self.log_file = Some(file_path.to_string());
        self
    }

    pub fn build(self) -> Application<State> {
        if self.starting_activity.is_none() {
            panic!("Starting activity must be set before building the application.");
        }
        #[cfg(feature = "tracing")]
        if self.log_file.is_some() {
            let file_appender = tracing_appender::rolling::never("./logs", self.log_file.unwrap());
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

            // Build subscriber
            let subscriber = fmt()
                .with_writer(non_blocking)
                .with_max_level(Level::INFO)
                .with_target(false)
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");

            info!("Logging initialized.");
        }
        Application {
            state: self.state,
            events: futures::stream::select_all(self.event_producers),
            event_handlers: self.event_handlers,
            post_event: self.post_event,
            activities: self.activities,
            active_activity: self.starting_activity.unwrap(),
            backstack: if self.has_backstack { Some(Vec::new()) } else { None },
        }
    }
}
