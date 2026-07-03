use std::{any::Any, fmt::Debug};

pub trait ActivityState: Any + Send + 'static + Debug {}
