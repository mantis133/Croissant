use std::any::{Any, TypeId};
use std::collections::HashMap;

use futures::Stream;
#[cfg(feature = "logging")]
use tracing::{Level, info};
#[cfg(feature = "logging")]
use tracing_subscriber::fmt;

use crate::{
    EventStream,
    activities::{Activity, ActivityState, AnyActivity},
    application::{
        AppHandle, Application,
        application::{AppEventHandler, AppPostEventHandler},
        value_store::ValueStore,
    },
    events::{ApplicationEvent, EventHandlerReturn},
};

/// Fluent builder for an [`Application`].
///
/// ```no_run
/// # use croissant::{activities::{ActivityBuilder, ActivityState}, application::Application};
/// # #[derive(Debug)] struct Home;
/// # impl ActivityState for Home {}
/// let app = Application::builder()
///     .value("counter", 0u32)
///     .add_activity(ActivityBuilder::<Home>::new().build())
///     .starting_activity(Home)
///     .backstack()
///     .build();
/// ```
pub struct ApplicationBuilder {
    pub(super) store: ValueStore,
    pub(super) starting_activity: Option<Box<dyn ActivityState>>,
    pub(super) activities: HashMap<TypeId, Box<dyn AnyActivity>>,
    pub(super) event_handlers: HashMap<TypeId, AppEventHandler>,
    pub(super) event_producers: Vec<EventStream<Box<dyn ApplicationEvent>>>,
    pub(super) post_event: Option<AppPostEventHandler>,
    pub(super) has_backstack: bool,
    pub(super) log_file: Option<String>,
}

impl ApplicationBuilder {
    pub fn new() -> Self {
        ApplicationBuilder {
            store: ValueStore::new(),
            starting_activity: None,
            activities: HashMap::new(),
            event_handlers: HashMap::new(),
            event_producers: Vec::new(),
            post_event: None,
            has_backstack: false,
            log_file: None,
        }
    }

    /// Seeds a named value into the store every activity shares.
    pub fn value<T: Any + Send>(mut self, key: impl Into<String>, value: T) -> Self {
        self.store.set(key, value);
        self
    }

    pub fn add_activity<A: ActivityState>(mut self, activity: Activity<A>) -> Self {
        self.activities
            .insert(TypeId::of::<A>(), Box::new(activity));
        self
    }

    pub fn add_event_producer<S>(mut self, producer: S) -> Self
    where
        S: Stream<Item = Box<dyn ApplicationEvent>> + Send + 'static,
    {
        self.event_producers.push(Box::pin(producer));
        self
    }

    /// Registers an application-wide handler for one event type. It runs only for events the
    /// active activity did not consume.
    pub fn on_event<F, E: ApplicationEvent>(mut self, mut handler: F) -> Self
    where
        F: FnMut(&E, &mut AppHandle) -> EventHandlerReturn + Send + 'static,
    {
        self.event_handlers.insert(
            TypeId::of::<E>(),
            Box::new(move |event, app| {
                // Trait upcasting (stable since Rust 1.76)
                let concrete = (event as &dyn Any)
                    .downcast_ref::<E>()
                    .expect("handler was dispatched for the event type it was registered under");
                handler(concrete, app)
            }),
        );
        self
    }

    /// Registers a callback run after every event, consumed or not.
    pub fn post_event<F>(mut self, handler: F) -> Self
    where
        F: FnMut(&dyn ApplicationEvent, &mut AppHandle) + Send + 'static,
    {
        self.post_event = Some(Box::new(handler));
        self
    }

    /// Keeps pushed-aside activities so [`AppHandle::pop`] can return to them.
    pub fn backstack(mut self) -> Self {
        self.has_backstack = true;
        self
    }

    pub fn starting_activity<A: ActivityState>(mut self, activity: A) -> Self {
        self.starting_activity = Some(Box::new(activity));
        self
    }

    #[cfg(feature = "logging")]
    pub fn log_file(mut self, _log_level: Level, _directory_path: &str, file_path: &str) -> Self {
        self.log_file = Some(file_path.to_string());
        self
    }

    /// # Panics
    ///
    /// Panics if no starting activity was set.
    pub fn build(self) -> Application {
        let Some(starting_activity) = self.starting_activity else {
            panic!("Starting activity must be set before building the application.");
        };

        #[cfg(feature = "logging")]
        if let Some(log_file) = self.log_file {
            let file_appender = tracing_appender::rolling::never("./logs", log_file);
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

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
            handle: AppHandle::new(self.store),
            active_activity: starting_activity,
            activities: self.activities,
            events: futures::stream::select_all(self.event_producers),
            event_handlers: self.event_handlers,
            post_event: self.post_event,
            backstack: if self.has_backstack {
                Some(Vec::new())
            } else {
                None
            },
        }
    }
}

impl Default for ApplicationBuilder {
    fn default() -> Self {
        Self::new()
    }
}
