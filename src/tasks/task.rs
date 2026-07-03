use std::{future::Future, pin::Pin};

use crate::tasks::TaskBuilder;

type AsyncCallback<Retval> = Box<dyn FnMut() -> Pin<Box<dyn Future<Output = Retval>>>>;

pub struct Task<State> {
    pub(super) state: State,
    pub(super) on_start: Option<AsyncCallback<()>>,
    pub(super) on_completion: Option<AsyncCallback<()>>,
    pub(super) on_cancel: Option<AsyncCallback<()>>,
    pub(super) on_event: Option<AsyncCallback<()>>,
}

impl<State> Task<State> {
    pub fn with_state(state: State) -> TaskBuilder<State> {
        TaskBuilder { state }
    }
}