use std::any::{Any, TypeId};
use std::collections::HashMap;

use super::any_task::AnyTask;
use crate::{
    ManagedState,
    application::AppHandle,
    events::{ApplicationEvent, EventHandlerReturn},
};

pub(super) type LifecycleHandler<State> = Box<dyn FnMut(&mut State, &mut AppHandle)>;
pub(super) type EventHandler<State> =
    Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppHandle) -> EventHandlerReturn>;
pub(super) type PostEventHandler<State> =
    Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppHandle)>;

/// An always-active handler with its own state — an activity without a screen.
///
/// A task is registered once, `Default`-constructed at start-up, and sees events for as long
/// as the application runs, whichever activity happens to be in front. That is what it is
/// for: behaviour several activities share, which would otherwise be copy-pasted into each
/// of them.
///
/// Its state is an ordinary [`ManagedState`], so `#[global]` and `#[inject]` fields work
/// exactly as they do on an activity.
///
/// Tasks are *synchronous*. To do work that awaits, a task calls
/// [`AppHandle::spawn`](crate::application::AppHandle::spawn) and receives the result back
/// as an event — which is how one task supervises many concurrent jobs without the framework
/// needing to track task instances.
///
/// Build one with [`TaskBuilder`](crate::tasks::TaskBuilder).
pub struct Task<State>
where
    State: ManagedState,
{
    pub(super) on_start: Option<LifecycleHandler<State>>,
    pub(super) on_stop: Option<LifecycleHandler<State>>,
    pub(super) event_handlers: HashMap<TypeId, EventHandler<State>>,
    pub(super) post_event: Option<PostEventHandler<State>>,
}

impl<State: ManagedState> Task<State> {
    /// Downcasts the erased state back to the type this task was built for.
    fn downcast(state: &mut dyn ManagedState) -> &mut State {
        (state as &mut dyn Any)
            .downcast_mut::<State>()
            .expect("task was invoked with state of a different type")
    }
}

impl<State: ManagedState> AnyTask for Task<State> {
    fn on_start(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle) {
        if let Some(ref mut f) = self.on_start {
            f(Self::downcast(state), app);
        }
    }

    fn on_stop(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle) {
        if let Some(ref mut f) = self.on_stop {
            f(Self::downcast(state), app);
        }
    }

    fn handle_event(
        &mut self,
        state: &mut dyn ManagedState,
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
        state: &mut dyn ManagedState,
        event: &dyn ApplicationEvent,
        app: &mut AppHandle,
    ) {
        if let Some(ref mut f) = self.post_event {
            f(Self::downcast(state), event, app);
        }
    }
}
