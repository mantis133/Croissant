use crate::{activities::ActivityState, events::{ApplicationEvent, EventHandlerReturn}};

pub(crate) trait AnyActivity<AppState> {
    fn on_create(&mut self, state: &mut dyn ActivityState, app: &mut AppState);
    fn on_resume(&mut self, state: &mut dyn ActivityState, app: &mut AppState);
    fn on_pause(&mut self, state: &mut dyn ActivityState, app: &mut AppState);
    fn on_destroy(&mut self, state: &mut dyn ActivityState, app: &mut AppState);
    fn handle_event(&mut self, state: &mut dyn ActivityState, event: &dyn ApplicationEvent, app: &mut AppState) -> EventHandlerReturn;
    fn post_event(&mut self, state: &mut dyn ActivityState, event: &dyn ApplicationEvent, app: &mut AppState);
}