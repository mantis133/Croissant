use std::any::{Any, TypeId};
use std::collections::HashMap;

use super::{ActivityState, any_activity::AnyActivity};
use crate::{
    application::AppHandle,
    events::{ApplicationEvent, EventHandlerReturn},
};

pub(super) type LifecycleHandler<State> = Box<dyn FnMut(&mut State, &mut AppHandle)>;
pub(super) type EventHandler<State> =
    Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppHandle) -> EventHandlerReturn>;
pub(super) type PostEventHandler<State> =
    Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppHandle)>;

/// A screen: a `State` struct plus the lifecycle and event callbacks registered for it.
///
/// Build one with [`ActivityBuilder`](crate::activities::ActivityBuilder) and register it on
/// the application. Every callback receives the activity's own `State` and an
/// [`AppHandle`] for everything shared with the rest of the application.
pub struct Activity<State>
where
    State: ActivityState,
{
    pub(super) on_create: Option<LifecycleHandler<State>>,
    pub(super) on_resume: Option<LifecycleHandler<State>>,
    pub(super) on_pause: Option<LifecycleHandler<State>>,
    pub(super) on_destroy: Option<LifecycleHandler<State>>,
    pub(super) event_handlers: HashMap<TypeId, EventHandler<State>>,
    pub(super) post_event: Option<PostEventHandler<State>>,
}

impl<State: ActivityState> Activity<State> {
    /// Downcasts the erased state back to the type this activity was built for.
    fn downcast(state: &mut dyn ActivityState) -> &mut State {
        (state as &mut dyn Any)
            .downcast_mut::<State>()
            .expect("activity was invoked with state of a different type")
    }
}

impl<State: ActivityState> AnyActivity for Activity<State> {
    fn on_create(&mut self, state: &mut dyn ActivityState, app: &mut AppHandle) {
        if let Some(ref mut f) = self.on_create {
            f(Self::downcast(state), app);
        }
    }

    fn on_resume(&mut self, state: &mut dyn ActivityState, app: &mut AppHandle) {
        if let Some(ref mut f) = self.on_resume {
            f(Self::downcast(state), app);
        }
    }

    fn on_pause(&mut self, state: &mut dyn ActivityState, app: &mut AppHandle) {
        if let Some(ref mut f) = self.on_pause {
            f(Self::downcast(state), app);
        }
    }

    fn on_destroy(&mut self, state: &mut dyn ActivityState, app: &mut AppHandle) {
        if let Some(ref mut f) = self.on_destroy {
            f(Self::downcast(state), app);
        }
    }

    fn handle_event(
        &mut self,
        state: &mut dyn ActivityState,
        event: &dyn ApplicationEvent,
        app: &mut AppHandle,
    ) -> EventHandlerReturn {
        let event_type_id = (event as &dyn Any).type_id();
        match self.event_handlers.get_mut(&event_type_id) {
            Some(handler) => handler(Self::downcast(state), event, app),
            None => EventHandlerReturn::Ignored,
        }
    }

    fn post_event(
        &mut self,
        state: &mut dyn ActivityState,
        event: &dyn ApplicationEvent,
        app: &mut AppHandle,
    ) {
        if let Some(ref mut f) = self.post_event {
            f(Self::downcast(state), event, app);
        }
    }
}
