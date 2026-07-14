use std::{any::TypeId, collections::HashMap};

use crate::{activities::{ActivityState, Activity}, application::Application, events::{ApplicationEvent, EventHandlerReturn}};
use std::any::Any;

pub struct ActivityBuilder<State, AppState> 
    where 
        State: ActivityState,
{
    on_create: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    on_resume: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    on_pause: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    on_destroy: Option<Box<dyn FnMut(&mut State, &mut AppState)>>,
    event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppState) -> EventHandlerReturn>>,
    post_event: Option<Box<dyn FnMut(&mut State, &dyn ApplicationEvent, &mut AppState)>>,
}


impl <State, AppState> ActivityBuilder<State, AppState> 
where 
    State: ActivityState,
{
    pub fn on_create<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppState) + Send + 'static,
    {
        self.on_create = Some(Box::new(callback));
        self
    }

    pub fn on_resume<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppState) + Send + 'static,
    {
        self.on_resume = Some(Box::new(callback));
        self
    }

    pub fn on_pause<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppState) + Send + 'static,
    {
        self.on_pause = Some(Box::new(callback));
        self
    }

    pub fn on_destroy<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut AppState) + Send + 'static,
    {
        self.on_destroy = Some(Box::new(callback));
        self
    }

    pub fn on_event<F, E: ApplicationEvent>(mut self, mut callback: F) -> Self
    where
        F: FnMut(&mut State, &E, &mut AppState) -> EventHandlerReturn + Send + 'static,
    {
        self.event_handlers.insert(TypeId::of::<E>(), Box::new(move |state, event, app_state: &mut AppState| {
            // Trait upcasting (stable since Rust 1.76)
            let concrete = (event as &dyn Any).downcast_ref::<E>().unwrap();
            callback(state, concrete, app_state)
        }));
        self
    }

    pub fn post_event<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &dyn ApplicationEvent, &mut AppState) + Send + 'static,
    {
        self.post_event = Some(Box::new(callback));
        self
    }

    pub fn new() -> Self {
        ActivityBuilder {
            on_create: None,
            on_resume: None,
            on_pause: None,
            on_destroy: None,
            event_handlers: HashMap::new(),
            post_event: None,
        }
    }

    pub fn build(self) -> Activity<State, AppState> {
        Activity {
            on_create: self.on_create,
            on_resume: self.on_resume,
            on_pause: self.on_pause,
            on_destroy: self.on_destroy,
            event_handlers: self.event_handlers,
            post_event: self.post_event,
        }
    }
}
