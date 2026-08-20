use std::any::{Any, TypeId};
use std::collections::HashMap;

#[cfg(feature = "logging")]
use tracing::info;

use crate::{
    EventStream,
    activities::{ActivityState, AnyActivity},
    application::{
        AppHandle, ApplicationBuilder, ServiceRegistry, command::Command, value_store::ValueStore,
    },
    events::{ApplicationEvent, EventHandlerReturn},
};

pub(super) type AppEventHandler =
    Box<dyn FnMut(&dyn ApplicationEvent, &mut AppHandle) -> EventHandlerReturn>;
pub(super) type AppPostEventHandler = Box<dyn FnMut(&dyn ApplicationEvent, &mut AppHandle)>;

/// The running application: the activity registry, the backstack, the event producers, and
/// the [`AppHandle`] that every callback is given.
///
/// `handle` is deliberately a plain field rather than something reached through `&mut self`.
/// Handing an activity a `&mut Application` is impossible — the activity is itself borrowed
/// out of `self.activities` for the duration of the call — so the pieces an activity is
/// allowed to touch live in their own field and are borrowed disjointly from the registry.
pub struct Application {
    pub(super) handle: AppHandle,
    pub(super) services: ServiceRegistry,
    pub(super) active_activity: Box<dyn ActivityState>,
    pub(super) activities: HashMap<TypeId, Box<dyn AnyActivity>>,
    pub(super) events: futures::stream::SelectAll<EventStream<Box<dyn ApplicationEvent>>>,
    pub(super) event_handlers: HashMap<TypeId, AppEventHandler>,
    pub(super) post_event: Option<AppPostEventHandler>,
    pub(super) backstack: Option<Vec<Box<dyn ActivityState>>>,
}

impl Application {
    pub fn builder() -> ApplicationBuilder {
        ApplicationBuilder::new()
    }

    /// The handle activities are given. Use it to seed named values before [`run`](Self::run),
    /// or to read them back once it returns.
    pub fn handle(&mut self) -> &mut AppHandle {
        &mut self.handle
    }

    /// The application's named-value store.
    pub fn values(&self) -> &ValueStore {
        self.handle.values()
    }

    /// The application's named-value store.
    pub fn values_mut(&mut self) -> &mut ValueStore {
        self.handle.values_mut()
    }

    /// The application's registered services.
    pub fn services(&self) -> &ServiceRegistry {
        &self.services
    }

    /// Runs the event loop until an activity or handler calls
    /// [`AppHandle::exit`], or until every event producer is exhausted.
    pub async fn run(&mut self) {
        self.handle.exiting = false;

        // on_create then on_resume for the starting activity.
        self.create_active();
        self.resume_active();
        self.drain_commands();

        while !self.handle.exiting {
            // Events emitted from a callback jump the queue: they are dispatched before the
            // application asks its producers for anything new.
            let event = match self.handle.pending_events.pop_front() {
                Some(event) => event,
                None => match futures::StreamExt::next(&mut self.events).await {
                    Some(event) => event,
                    None => break,
                },
            };

            self.dispatch(event.as_ref());
            self.drain_commands();
        }

        // Tear down whatever was active when the loop stopped, then unwind the backstack
        // from the top so activities are destroyed in the reverse of their creation order.
        self.pause_active();
        self.destroy_active();
        while let Some(activity) = self.backstack.as_mut().and_then(Vec::pop) {
            self.destroy(activity);
        }
    }

    /// Runs one event past the activity handler, the application handler, and both
    /// post-event hooks.
    fn dispatch(&mut self, event: &dyn ApplicationEvent) {
        let event_type_id = (event as &dyn Any).type_id();
        let activity_type_id = self.active_activity_type_id();

        // Activity-level handler for this event type.
        let consumed = self.with_globals(|app| {
            let Some(activity) = app.activities.get_mut(&activity_type_id) else {
                return false;
            };
            let ret = activity.handle_event(app.active_activity.as_mut(), event, &mut app.handle);
            #[cfg(feature = "logging")]
            info!("activity event handler returned {ret:?}");
            ret.is_consumed()
        });

        // Background task handlers.
        // TODO: Implement background tasks

        // Application-level handler, unless the activity swallowed the event.
        if !consumed && let Some(handler) = self.event_handlers.get_mut(&event_type_id) {
            // Nothing runs after this handler, so its return value has nothing left to
            // gate. It is kept for symmetry with activity handlers, and for the
            // background-task stage that will slot in ahead of it.
            let _ret = handler(event, &mut self.handle);
            #[cfg(feature = "logging")]
            info!("application event handler returned {_ret:?}");
        }

        // Post-event hooks see every event, consumed or not.
        self.with_globals(|app| {
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.post_event(app.active_activity.as_mut(), event, &mut app.handle);
            }
        });
        if let Some(handler) = self.post_event.as_mut() {
            handler(event, &mut self.handle);
        }
    }

    /// Applies every queued [`Command`], including ones queued by the lifecycle callbacks
    /// that running earlier commands triggered.
    fn drain_commands(&mut self) {
        while let Some(command) = self.handle.commands.pop_front() {
            #[cfg(feature = "logging")]
            info!("applying {command:?}");
            match command {
                Command::Exit => {
                    self.handle.exiting = true;
                    self.handle.commands.clear();
                    return;
                }
                Command::Push(new_activity) => {
                    self.pause_active();
                    let old_activity = std::mem::replace(&mut self.active_activity, new_activity);
                    match self.backstack.as_mut() {
                        Some(backstack) => backstack.push(old_activity),
                        // Without a backstack there is nothing to return to, so the
                        // outgoing activity is finished rather than stashed.
                        None => self.destroy(old_activity),
                    }
                    self.create_active();
                    self.resume_active();
                }
                Command::Pop => {
                    let previous = self.backstack.as_mut().and_then(Vec::pop);
                    match previous {
                        Some(previous) => {
                            self.pause_active();
                            let finished = std::mem::replace(&mut self.active_activity, previous);
                            self.destroy(finished);
                            // No create_active: the restored instance already exists, so it
                            // resumes rather than being created again.
                            self.resume_active();
                        }
                        None => {
                            #[cfg(feature = "logging")]
                            info!("pop ignored: backstack is disabled or empty");
                        }
                    }
                }
                Command::Replace(new_activity) => {
                    self.pause_active();
                    let finished = std::mem::replace(&mut self.active_activity, new_activity);
                    self.destroy(finished);
                    self.create_active();
                    self.resume_active();
                }
            }
        }
    }

    fn active_activity_type_id(&self) -> TypeId {
        (self.active_activity.as_ref() as &dyn Any).type_id()
    }

    /// Brackets one activity callback with the `#[global]` field check-out and check-in.
    ///
    /// Every callback gets its own window rather than one window around the whole of
    /// `dispatch`, so anything running between two activity callbacks — the
    /// application-level handler, most importantly — reads a coherent store.
    fn with_globals<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        self.active_activity
            .checkout_globals(&mut self.handle.store);
        let result = f(self);
        self.active_activity.checkin_globals(&mut self.handle.store);
        result
    }

    /// Resolves `#[inject]` fields and runs `on_create` for a newly created instance.
    ///
    /// `on_create` is bracketed like any other callback, which is what lets it seed a
    /// `#[global]` — writing `state.counter = 10` there reaches the store on check-in.
    fn create_active(&mut self) {
        self.active_activity.inject_services(&self.services);
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_create(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    fn resume_active(&mut self) {
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_resume(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    fn pause_active(&mut self) {
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_pause(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    fn destroy_active(&mut self) {
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_destroy(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    /// `on_destroy` for an instance that is no longer the active one — a replaced activity,
    /// or a backstack entry being unwound at shutdown.
    fn destroy(&mut self, mut state: Box<dyn ActivityState>) {
        let activity_type_id = (state.as_ref() as &dyn Any).type_id();
        state.checkout_globals(&mut self.handle.store);
        if let Some(activity) = self.activities.get_mut(&activity_type_id) {
            activity.on_destroy(state.as_mut(), &mut self.handle);
        }
        state.checkin_globals(&mut self.handle.store);
    }
}
