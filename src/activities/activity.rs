use std::{any::TypeId, collections::HashMap};

use super::{ActivityState, any_activity::AnyActivity};
use crate::events::{ApplicationEvent, EventHandlerReturn, Pagination};
use std::any::Any;

pub struct Activity<State, AppState> 
    where 
        State: ActivityState,
{
    pub(super) on_create: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    pub(super) on_resume: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    pub(super) on_pause: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    pub(super) on_destroy: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    pub(super) event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppState) -> EventHandlerReturn>>,
    pub(super) post_event: Option<Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppState)>>,
}


impl<State: ActivityState, AppState> AnyActivity<AppState> for Activity<State, AppState> {
    fn on_create(&mut self, state: &mut dyn ActivityState, app: &mut AppState) {
        if let Some(ref mut f) = self.on_create {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            f(state, app);
        }
    }
    fn on_resume(&mut self, state: &mut dyn ActivityState, app: &mut AppState) {
        if let Some(ref mut f) = self.on_resume {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            f(state, app);
        }
    }
    fn on_pause(&mut self, state: &mut dyn ActivityState, app: &mut AppState) {
        if let Some(ref mut f) = self.on_pause {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            f(state, app);
        }
    }
    fn on_destroy(&mut self, state: &mut dyn ActivityState, app: &mut AppState) {
        if let Some(ref mut f) = self.on_destroy {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            f(state, app);
        }
    }
    fn handle_event(&mut self, state: &mut dyn ActivityState, event: &dyn ApplicationEvent, app: &mut AppState) -> EventHandlerReturn {
        let event_type_id = (event as &dyn Any).type_id();
        if let Some(handler) = self.event_handlers.get_mut(&event_type_id) {
            // Trait upcasting (stable since Rust 1.76)
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            handler(state, event, app)
        } else {
            EventHandlerReturn {
                consumed: false,
                pagination: Pagination::None,
                exit: false,
            }
        }
    }
    fn post_event(&mut self, state: &mut dyn ActivityState, event: &dyn ApplicationEvent, app: &mut AppState) {
        if let Some(ref mut f) = self.post_event {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            f(state, event, app);
        }
    }
}


