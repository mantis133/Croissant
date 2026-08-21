use crate::{
    ManagedState,
    application::AppHandle,
    events::{ApplicationEvent, EventHandlerReturn},
};

/// Type-erased view of an [`Activity`](crate::activities::Activity), letting the application
/// hold activities of every state type in one map.
///
/// Each method takes the activity's state as `&mut dyn ManagedState` — the implementation
/// downcasts it back to the concrete type it was built for.
pub(crate) trait AnyActivity {
    fn on_create(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle);
    fn on_resume(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle);
    fn on_pause(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle);
    fn on_destroy(&mut self, state: &mut dyn ManagedState, app: &mut AppHandle);
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
