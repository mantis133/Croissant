use crate::{activities::ActivityState, events::{ApplicationEvent, EventHandlerReturn}};

pub trait AnyActivity {
    fn on_create(&mut self, state: &mut dyn ActivityState);
    fn on_resume(&mut self, state: &mut dyn ActivityState);
    fn on_pause(&mut self, state: &mut dyn ActivityState);
    fn on_destroy(&mut self, state: &mut dyn ActivityState);
    fn handle_event(&mut self, state: &mut dyn ActivityState, event: &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>>;
}