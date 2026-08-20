use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::{
    activities::{
        Activity, ActivityState,
        activity::{EventHandler, LifecycleHandler, PostEventHandler},
    },
    application::AppHandle,
    events::{ApplicationEvent, EventHandlerReturn},
};

/// Fluent builder for an [`Activity`].
///
/// Every callback is handed the activity's own `State` and an [`AppHandle`], which is how it
/// reaches the application's named values, emits events, and navigates.
///
/// ```no_run
/// # use croissant::{activities::{ActivityBuilder, ActivityState}, application::AppHandle, events::{ApplicationEvent, EventHandlerReturn}};
/// #[derive(Debug, Default)]
/// struct Menu { cursor: usize }
/// impl ActivityState for Menu {}
///
/// #[derive(Debug)]
/// struct Down;
/// impl ApplicationEvent for Down {}
///
/// let menu = ActivityBuilder::<Menu>::new()
///     .on_resume(|state, app| {
///         state.cursor = 0;
///         app.set("current_screen", "menu");
///     })
///     .on_event(|state: &mut Menu, _event: &Down, _app: &mut AppHandle| {
///         state.cursor += 1;
///         EventHandlerReturn::Consumed
///     })
///     .build();
/// ```
pub struct ActivityBuilder<State>
where
    State: ActivityState,
{
    on_create: Option<LifecycleHandler<State>>,
    on_resume: Option<LifecycleHandler<State>>,
    on_pause: Option<LifecycleHandler<State>>,
    on_destroy: Option<LifecycleHandler<State>>,
    event_handlers: HashMap<TypeId, EventHandler<State>>,
    post_event: Option<PostEventHandler<State>>,
}

impl<State> ActivityBuilder<State>
where
    State: ActivityState,
{
    pub fn new() -> Self {
        ActivityBuilder {
            on_create: None,
            on_resume: None,
            on_pause: None,
            on_destroy: None,
            event_handlers: HashMap::new(),
            post_event: None,
        }
    }

    pub fn on_create<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppHandle) + Send + 'static,
    {
        self.on_create = Some(Box::new(callback));
        self
    }

    pub fn on_resume<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppHandle) + Send + 'static,
    {
        self.on_resume = Some(Box::new(callback));
        self
    }

    pub fn on_pause<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppHandle) + Send + 'static,
    {
        self.on_pause = Some(Box::new(callback));
        self
    }

    pub fn on_destroy<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppHandle) + Send + 'static,
    {
        self.on_destroy = Some(Box::new(callback));
        self
    }

    /// Registers a handler for one concrete event type. Registering twice for the same `E`
    /// replaces the earlier handler.
    pub fn on_event<F, E: ApplicationEvent>(mut self, mut callback: F) -> Self
    where
        F: FnMut(&mut State, &E, &mut AppHandle) -> EventHandlerReturn + Send + 'static,
    {
        self.event_handlers.insert(
            TypeId::of::<E>(),
            Box::new(move |state, event, app| {
                // Trait upcasting (stable since Rust 1.76)
                let concrete = (event as &dyn Any)
                    .downcast_ref::<E>()
                    .expect("handler was dispatched for the event type it was registered under");
                callback(state, concrete, app)
            }),
        );
        self
    }

    /// Registers a callback run after every event this activity sees, consumed or not.
    pub fn post_event<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &dyn ApplicationEvent, &mut AppHandle) + Send + 'static,
    {
        self.post_event = Some(Box::new(callback));
        self
    }

    pub fn build(self) -> Activity<State> {
        Activity {
            on_create: self.on_create,
            on_resume: self.on_resume,
            on_pause: self.on_pause,
            on_destroy: self.on_destroy,
            event_handlers: self.event_handlers,
            post_event: self.post_event,
        }
    }
}

impl<State> Default for ActivityBuilder<State>
where
    State: ActivityState,
{
    fn default() -> Self {
        Self::new()
    }
}
