use crate::{
    ManagedState,
    application::AppHandle,
    events::{ApplicationEvent, EventHandlerReturn},
};

/// Type-erased view of a [`Task`](crate::tasks::Task), letting the application hold tasks of
/// every state type in one list.
///
/// Each method takes the task's state as `&mut dyn ManagedState` — the implementation
/// downcasts it back to the concrete type it was built for.
pub(crate) trait AnyTask {
    fn on_start(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle);
    fn on_stop(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle);
    fn handle_event(
        &mut self,
        state: &mut dyn ManagedState,
        event: &dyn ApplicationEvent,
        app: &mut AppHandle,
    ) -> EventHandlerReturn;
    fn post_event(
        &mut self,
        state: &mut dyn ManagedState,
        event: &dyn ApplicationEvent,
        app: &mut AppHandle,
    );
}

/// One registered task: its handlers, paired with the state instance they run against.
///
/// Unlike activities — which are keyed by `TypeId` because only one is active at a time —
/// tasks are all active at once and held in a `Vec`, so registration order is dispatch order.
pub(crate) struct TaskEntry {
    pub(crate) handlers: Box<dyn AnyTask>,
    pub(crate) state: Box<dyn ManagedState>,
}
