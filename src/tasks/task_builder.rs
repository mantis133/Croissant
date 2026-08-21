use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::{
    ManagedState,
    application::AppHandle,
    events::{ApplicationEvent, EventHandlerReturn},
    tasks::{
        Task,
        task::{EventHandler, LifecycleHandler, PostEventHandler},
    },
};

/// Fluent builder for a [`Task`].
///
/// ```
/// # use croissant::{ManagedState, application::AppHandle, events::{ApplicationEvent, EventHandlerReturn}, tasks::TaskBuilder};
/// #[derive(Debug, Default)]
/// struct Audit { seen: usize }
/// impl ManagedState for Audit {}
///
/// #[derive(Debug)]
/// struct Quit;
/// impl ApplicationEvent for Quit {}
///
/// let audit = TaskBuilder::<Audit>::new()
///     // Handled the same way no matter which activity is in front.
///     .on_event(|_task: &mut Audit, _event: &Quit, app: &mut AppHandle| {
///         app.exit();
///         EventHandlerReturn::Consumed
///     })
///     .post_event(|task, _event, _app| task.seen += 1)
///     .build();
/// ```
pub struct TaskBuilder<State>
where
    State: ManagedState,
{
    on_start: Option<LifecycleHandler<State>>,
    on_stop: Option<LifecycleHandler<State>>,
    event_handlers: HashMap<TypeId, EventHandler<State>>,
    post_event: Option<PostEventHandler<State>>,
}

impl<State> TaskBuilder<State>
where
    State: ManagedState,
{
    pub fn new() -> Self {
        TaskBuilder {
            on_start: None,
            on_stop: None,
            event_handlers: HashMap::new(),
            post_event: None,
        }
    }

    /// Runs once at start-up, on a `Default`-constructed instance with `#[inject]` fields
    /// already resolved. This is where a task initialises itself — and, because it is
    /// bracketed like every callback, where it can seed a `#[global]`.
    ///
    /// Every task starts before the first activity is created, so an activity's `on_create`
    /// can rely on whatever a task set up.
    pub fn on_start<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppHandle) + Send + 'static,
    {
        self.on_start = Some(Box::new(callback));
        self
    }

    /// Runs once as the application shuts down, after every activity has been destroyed.
    pub fn on_stop<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppHandle) + Send + 'static,
    {
        self.on_stop = Some(Box::new(callback));
        self
    }

    /// Registers a handler for one concrete event type. Registering twice for the same `E`
    /// replaces the earlier handler.
    ///
    /// Tasks sit between the active activity and the application-level handler, so an event
    /// the activity consumed never arrives here — the activity handles what it cares about
    /// and the task is the shared fallback. Returning
    /// [`EventHandlerReturn::Consumed`] likewise stops the event before the
    /// application-level handler. Use [`TaskBuilder::post_event`] to see events regardless.
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

    /// Registers a callback run after every event, consumed or not.
    pub fn post_event<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &dyn ApplicationEvent, &mut AppHandle) + Send + 'static,
    {
        self.post_event = Some(Box::new(callback));
        self
    }

    pub fn build(self) -> Task<State> {
        Task {
            on_start: self.on_start,
            on_stop: self.on_stop,
            event_handlers: self.event_handlers,
            post_event: self.post_event,
        }
    }
}

impl<State> Default for TaskBuilder<State>
where
    State: ManagedState,
{
    fn default() -> Self {
        Self::new()
    }
}
