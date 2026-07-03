use crate::activities::{ActivityState,};

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