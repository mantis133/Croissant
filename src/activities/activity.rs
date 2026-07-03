use std::{any::TypeId, collections::HashMap};

use super::{ActivityState, any_activity::AnyActivity};
use crate::application::Application;
use crate::events::{ApplicationEvent, EventHandlerReturn, Pagination};
use std::any::Any;

pub struct Activity<State, AppState> 
    where 
        State: ActivityState,
{
    pub(super) on_create: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    pub(super) on_resume: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    pub(super) on_pause: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    pub(super) on_destroy: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    pub(super) event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut Application<AppState>) -> EventHandlerReturn<Box<dyn ApplicationEvent>>>>,
}


impl<State: ActivityState, AppState> AnyActivity for Activity<State, AppState> {
    fn on_create(&mut self, state: &mut dyn ActivityState) {
        if let Some(ref mut f) = self.on_create {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            #[allow(deref_nullptr)]
            let app = unsafe { &mut *(std::ptr::null_mut() as *mut Application<AppState>) };
            f(state, app);
        }
    }
    fn on_resume(&mut self, state: &mut dyn ActivityState) {
        if let Some(ref mut f) = self.on_resume {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            #[allow(deref_nullptr)]
            let app = unsafe { &mut *(std::ptr::null_mut() as *mut Application<AppState>) };
            f(state, app);
        }
    }
    fn on_pause(&mut self, state: &mut dyn ActivityState) {
        if let Some(ref mut f) = self.on_pause {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            #[allow(deref_nullptr)]
            let app = unsafe { &mut *(std::ptr::null_mut() as *mut Application<AppState>) };
            f(state, app);
        }
    }
    fn on_destroy(&mut self, state: &mut dyn ActivityState) {
        if let Some(ref mut f) = self.on_destroy {
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            #[allow(deref_nullptr)]
            let app = unsafe { &mut *(std::ptr::null_mut() as *mut Application<AppState>) };
            f(state, app);
        }
    }
    fn handle_event(&mut self, state: &mut dyn ActivityState, event: &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>> {
        let event_type_id = (event as &dyn Any).type_id();
        if let Some(handler) = self.event_handlers.get_mut(&event_type_id) {
            // Trait upcasting (stable since Rust 1.76)
            let state = (state as &mut dyn Any).downcast_mut::<State>().unwrap();
            #[allow(deref_nullptr)]
            let app = unsafe { &mut *(std::ptr::null_mut() as *mut Application<AppState>) };
            handler(state, event, app)
        } else {
            EventHandlerReturn {
                event: None,
                pagination: Pagination::None,
                exit: false,
            }
        }
    }
}