use std::{any::Any, fmt::Debug};


pub trait ApplicationEvent: Any + Send + 'static + Debug {}
