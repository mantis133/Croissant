use crate::activities::{ActivityState,};

#[derive(Debug)]
pub enum Pagination {
    None,
    Push(Box<dyn ActivityState>),
    Pop,
    Replace(Box<dyn ActivityState>),
}

#[derive(Debug)]
pub struct EventHandlerReturn {
    pub consumed: bool,
    pub pagination: Pagination,
    pub exit: bool,
}