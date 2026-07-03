use super::task::Task;

pub struct TaskBuilder<State> {
    pub(super) state: State
}

impl <State> TaskBuilder<State> {
    pub fn build(self, state: State) -> Task<State> {
        Task { 
            state,
            on_start: None,
            on_completion: None,
            on_cancel: None,
            on_event: None,
        }
    }

    pub fn on_event(){}

    pub fn on_start(){}

    pub fn on_completion(){}

    pub fn on_cancel(){}
}