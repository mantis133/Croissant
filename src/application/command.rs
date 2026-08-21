use crate::ManagedState;

/// An action an activity asked the application to take, queued until the current
/// callback returns so that handlers never need to borrow the application itself.
pub(crate) enum Command {
    Push(Box<dyn ManagedState>),
    Pop,
    Replace(Box<dyn ManagedState>),
    Exit,
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Push(activity) => write!(f, "Push({activity:?})"),
            Command::Pop => write!(f, "Pop"),
            Command::Replace(activity) => write!(f, "Replace({activity:?})"),
            Command::Exit => write!(f, "Exit"),
        }
    }
}
