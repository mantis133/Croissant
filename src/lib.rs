#[cfg(feature = "crossterm")]
pub mod crossterm;
#[cfg(feature = "logging")]
pub mod logging;

use std::any::{Any, TypeId};
use std::fmt::Debug;
use futures::Stream;
use tracing::{Level, info};
use tracing_subscriber::fmt;
use std::pin::Pin;
use std::collections::HashMap;

pub type EventStream<EventType> = Pin<Box<dyn Stream<Item = EventType> + Send>>;

pub trait ActivityState: Any + Send + 'static + Debug {}
pub trait ApplicationEvent: Any + Send + 'static + Debug {}

#[derive(Debug)]
pub struct OnCreateEvent;
impl ApplicationEvent for OnCreateEvent {}

#[derive(Debug)]
pub struct OnResumeEvent;
impl ApplicationEvent for OnResumeEvent {}

#[derive(Debug)]
pub struct OnPauseEvent;
impl ApplicationEvent for OnPauseEvent {}

#[derive(Debug)]
pub struct OnDestroyEvent;
impl ApplicationEvent for OnDestroyEvent {}

#[derive(Debug)]
pub enum Pagination {
    None,
    Push(Box<dyn ActivityState>),
    Pop,
    Replace(Box<dyn ActivityState>),
}

#[derive(Debug)]
pub struct EventHandlerReturn<EventType> {
    pub event: Option<EventType>,
    pub pagination: Pagination,
    pub exit: bool,
}

pub struct Application<State> {
    state: State,
    active_activity: Box<dyn ActivityState>,
    activities: HashMap<TypeId, Box<dyn AnyActivity>>,
    events: futures::stream::SelectAll<EventStream<Box<dyn ApplicationEvent>>>,
    event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>>>>,
    post_event: Option<Box<dyn FnMut(&mut State, &dyn ApplicationEvent)>>,
    backstack: Option<Vec<Box<dyn ActivityState>>>,
}

pub struct ApplicationBuilder<State> {
    state: State,
    starting_activity: Option<Box<dyn ActivityState>>,
    activities: HashMap<TypeId, Box<dyn AnyActivity>>,
    event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>>>>,
    event_producers: Vec<EventStream<Box<dyn ApplicationEvent>>>,
    post_event: Option<Box<dyn FnMut(&mut State, &dyn ApplicationEvent)>>,
    has_backstack: bool,
    log_file: Option<String>,
}

pub struct Activity<State, AppState> 
    where 
        State: ActivityState,
{
    on_create: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    on_resume: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    on_pause: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    on_destroy: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>>>>,
}

pub struct ActivityBuilder<State, AppState> 
    where 
        State: ActivityState,
{
    on_create: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    on_resume: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    on_pause: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    on_destroy: Option<Box<dyn FnMut(&mut State, &mut Application<AppState>)>>,
    event_handlers: HashMap<TypeId, Box<dyn FnMut(&mut State, &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>>>>,
}

trait AnyActivity {
    fn on_create(&mut self, state: &mut dyn ActivityState);
    fn on_resume(&mut self, state: &mut dyn ActivityState);
    fn on_pause(&mut self, state: &mut dyn ActivityState);
    fn on_destroy(&mut self, state: &mut dyn ActivityState);
    fn handle_event(&mut self, state: &mut dyn ActivityState, event: &dyn ApplicationEvent) -> EventHandlerReturn<Box<dyn ApplicationEvent>>;
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
            handler(state, event)
        } else {
            EventHandlerReturn {
                event: None,
                pagination: Pagination::None,
                exit: false,
            }
        }
    }
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

impl <State> ApplicationBuilder<State> 
where 
    State: 'static + Send,
{
    pub fn add_activity<A: ActivityState>(mut self, activity: Activity<A, State>) -> Self {
        self.activities.insert(TypeId::of::<A>(), Box::new(activity));
        self
    }

    pub fn add_event_producer<S>(mut self, producer: S) -> Self
    where
        S: Stream<Item = Box<dyn ApplicationEvent>> + Send + 'static,
    {
        self.event_producers.push(Box::pin(producer));
        self
    }

    pub fn on_event<F, E: ApplicationEvent>(mut self, mut handler: F) -> Self
    where
        F: FnMut(&mut State, &E) -> EventHandlerReturn<Box<dyn ApplicationEvent>> + Send + 'static,
    {
        self.event_handlers.insert(TypeId::of::<E>(), Box::new(move |state, event| {
            // Trait upcasting (stable since Rust 1.76)
            let concrete = (event as &dyn Any).downcast_ref::<E>().unwrap();
            handler(state, concrete)
        }));
        self
    }

    pub fn post_event<F>(mut self, handler: F) -> Self
    where
        F: FnMut(&mut State, &dyn ApplicationEvent) + Send + 'static,
    {
        self.post_event = Some(Box::new(handler));
        self
    }

    pub fn backstack(mut self) -> Self {
        self.has_backstack = true;
        self
    }

    pub fn starting_activity<A: ActivityState>(mut self, activity: A) -> Self {
        self.starting_activity = Some(Box::new(activity) as Box<(dyn ActivityState + 'static)>);
        self
    }

    #[cfg(feature = "logging")]
    pub fn log_file(mut self, _log_level: Level, _directory_path: &str, file_path: &str) -> Self {
        self.log_file = Some(file_path.to_string());
        self
    }

    pub fn build(self) -> Application<State> {
        if self.starting_activity.is_none() {
            panic!("Starting activity must be set before building the application.");
        }
        if self.log_file.is_some() {
            let file_appender = tracing_appender::rolling::never("./logs", self.log_file.unwrap());
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

            // Build subscriber
            let subscriber = fmt()
                .with_writer(non_blocking)
                .with_max_level(Level::INFO)
                .with_target(false)
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");

            info!("Logging initialized.");
        }
        Application {
            state: self.state,
            events: futures::stream::select_all(self.event_producers),
            event_handlers: self.event_handlers,
            post_event: self.post_event,
            activities: self.activities,
            active_activity: self.starting_activity.unwrap(),
            backstack: if self.has_backstack { Some(Vec::new()) } else { None },
        }
    }
}

impl <State, AppState> ActivityBuilder<State, AppState> 
where 
    State: ActivityState,
{
    pub fn on_create<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut Application<AppState>) + Send + 'static,
    {
        self.on_create = Some(Box::new(callback));
        self
    }

    pub fn on_resume<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut Application<AppState>) + Send + 'static,
    {
        self.on_resume = Some(Box::new(callback));
        self
    }

    pub fn on_pause<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut Application<AppState>) + Send + 'static,
    {
        self.on_pause = Some(Box::new(callback));
        self
    }

    pub fn on_destroy<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut State, &mut Application<AppState>) + Send + 'static,
    {
        self.on_destroy = Some(Box::new(callback));
        self
    }

    pub fn on_event<F, E: ApplicationEvent>(mut self, mut callback: F) -> Self
    where
        F: FnMut(&mut State, &E) -> EventHandlerReturn<Box<dyn ApplicationEvent>> + Send + 'static,
    {
        self.event_handlers.insert(TypeId::of::<E>(), Box::new(move |state, event| {
            // Trait upcasting (stable since Rust 1.76)
            let concrete = (event as &dyn Any).downcast_ref::<E>().unwrap();
            callback(state, concrete)
        }));
        self
    }

    pub fn new() -> Self {
        ActivityBuilder {
            on_create: None,
            on_resume: None,
            on_pause: None,
            on_destroy: None,
            event_handlers: HashMap::new(),
        }
    }

    pub fn build(self) -> Activity<State, AppState> {
        Activity {
            on_create: self.on_create,
            on_resume: self.on_resume,
            on_pause: self.on_pause,
            on_destroy: self.on_destroy,
            event_handlers: self.event_handlers,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    struct DataElement {
        data: Box<[u8]>,
    }

    #[test]
    fn it_works() {
        let mut data: HashMap<&str, DataElement> = HashMap::new();

        // assume we want a global int under the name "current time"
        data.insert("current time", DataElement { data: Box::new(6u32.to_le_bytes()) });

        // now we pull the data out and cast as a &mut u32
        let current_time = data.get_mut("current time").unwrap();
        let current_time: &mut u32 = unsafe { &mut *(current_time.data.as_mut_ptr() as *mut u32) };

        println!("current time: {}", current_time);

        *current_time += 1;

        println!("current time: {}", current_time);

        let new_current_time = data.get_mut("current time").unwrap();
        let new_current_time: &mut u32 = unsafe { &mut *(new_current_time.data.as_mut_ptr() as *mut u32) };

        println!("new current time: {}", new_current_time);
    }
}
