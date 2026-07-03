use std::{any::TypeId, collections::HashMap};

use tracing::info;

use crate::{
    EventStream, 
    application::{
        application_builder::ApplicationBuilder,
    },
    activities::{
        ActivityState,
        AnyActivity,
    },
    events::{
        EventHandlerReturn, 
        ApplicationEvent, 
        Pagination
    }
};
use std::any::Any;


pub struct Application<State> {
    pub(super) state: State,
    pub(super) active_activity: Box<dyn ActivityState>,
    pub(super) activities: HashMap<TypeId, Box<dyn AnyActivity>>,
    pub(super) events: futures::stream::SelectAll<EventStream<Box<dyn ApplicationEvent>>>,
    pub(super) event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>>>>,
    pub(super) post_event: Option<Box<dyn FnMut(&mut State, &dyn ApplicationEvent)>>,
    pub(super) backstack: Option<Vec<Box<dyn ActivityState>>>,
}

impl <State> Application<State> 
where 
    State: 'static + Send,
{
    pub fn with_state(state: State) -> ApplicationBuilder<State> {
        ApplicationBuilder {
            state,
            activities: HashMap::new(),
            event_handlers: HashMap::new(),
            event_producers: Vec::new(),
            post_event: None,
            has_backstack: false,
            starting_activity: None,
            log_file: None,
        }
    }

    pub fn get_state(&self) -> &State {
        &self.state
    }

    pub fn get_state_mut(&mut self) -> &mut State {
        &mut self.state
    }

    pub fn set_global_value(&mut self, key: &str, value: Box<dyn Any>) {
        // Implement your logic to set a value in the state
    }

    pub fn get_global_value(&self, key: &str) -> Option<&Box<dyn Any>> {
        // Implement your logic to get a value from the state
        None
    }

    pub fn get_global_value_mut(&mut self, key: &str) -> Option<&mut Box<dyn Any>> {
        // Implement your logic to get a mutable value from the state
        None
    }

    pub async fn run(&mut self){
        // On Create


        // on Resume for the starting activity


        // Event loop
        while let Some(event) = futures::StreamExt::next(&mut self.events).await {
            let event_ref = event.as_ref();
            let event_type_id = (event_ref as &dyn Any).type_id();
            // let mut handler_return = EventHandlerReturn {
            //     event: Some(event),
            //     pagination: Pagination::None,
            //     exit: false,
            // };
            let mut current_event = Some(event_ref);
            let mut pagination = Pagination::None;


            // println!("[APPLICATION] Event received: {:?}", event_ref);

            let activity_type_id = (self.active_activity.as_ref() as &dyn Any).type_id();
            let activities = &mut self.activities;
            let active_activity = &mut self.active_activity;
            
            // Handle activity-level event handlers
            if let Some(activity) = activities.get_mut(&activity_type_id) {
                let ret = activity.handle_event(active_activity.as_mut(), current_event.unwrap());
                // handle return...
                println!("Activity event handler return: {:?}", ret);
                info!("Activity event handler return: {:?}", ret);
                if ret.exit {
                    println!("Exiting application due to activity event handler return.");
                    break;
                }
                if ret.event.is_none() {
                    println!("Activity event handler returned None event, discarding event.");
                    current_event = None;
                }
                pagination = ret.pagination;
            }

            // Handle background task event handlers 
            // TODO: Implement background tasks

            // Handle application-level event handlers
            if let Some(event) = current_event {
                if let Some(handler) = self.event_handlers.get_mut(&event_type_id) {
                    let ret = handler(&mut self.state, event);
                    // handle return...
                    println!("Event handler return: {:?}", ret);
                    if ret.exit {
                        println!("Exiting application due to event handler return.");
                        break;
                    }
                    pagination = ret.pagination;
                }
            }

            self.post_event.as_mut().map(|handler| handler(&mut self.state, event_ref));

            // Handle pagination
            match pagination {
                Pagination::None => {},
                Pagination::Push(new_activity) => {
                    activities.get_mut(&activity_type_id).map(|activity| activity.on_pause(active_activity.as_mut()));
                    let old_activity = std::mem::replace(&mut self.active_activity, new_activity);
                    if let Some(backstack) = self.backstack.as_mut() {
                        backstack.push(old_activity);
                    }
                    let activity_type_id = (self.active_activity.as_ref() as &dyn Any).type_id();
                    let active_activity = &mut self.active_activity;
                    activities.get_mut(&activity_type_id).map(|activity| activity.on_resume(active_activity.as_mut()));
                }
                Pagination::Pop => {
                    if let Some(backstack) = self.backstack.as_mut() {
                        if let Some(old_activity) = backstack.pop() {
                            activities.get_mut(&activity_type_id).map(|activity| activity.on_pause(active_activity.as_mut()));
                            self.active_activity = old_activity;
                            let activity_type_id = (self.active_activity.as_ref() as &dyn Any).type_id();
                            let active_activity = &mut self.active_activity;
                            activities.get_mut(&activity_type_id).map(|activity| activity.on_resume(active_activity.as_mut()));
                        } else {
                            println!("Backstack is empty, cannot pop activity.");
                        }
                    } else {
                        println!("Backstack is not enabled, cannot pop activity.");
                    }
                }
                Pagination::Replace(new_activity) => {
                    activities.get_mut(&activity_type_id).map(|activity| activity.on_pause(active_activity.as_mut()));
                    self.active_activity = new_activity;
                    let activity_type_id = (self.active_activity.as_ref() as &dyn Any).type_id();
                    let active_activity = &mut self.active_activity;
                    activities.get_mut(&activity_type_id).map(|activity| activity.on_resume(active_activity.as_mut()));
                }
            }

        }

        // On Pause for active activity
        let activity_type_id = (self.active_activity.as_ref() as &dyn Any).type_id();
        let active_activity = &mut self.active_activity;
        let activities = &mut self.activities;
        if let Some(activity) = activities.get_mut(&activity_type_id) {
            activity.on_pause(active_activity.as_mut());
        }

        // on Destroy for all activities

    }
}