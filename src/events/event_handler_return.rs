/// What an event handler tells the application about the event it just saw.
///
/// Navigation and exit no longer travel on this value — an activity requests those through
/// its [`AppHandle`](crate::application::AppHandle). All that is left is whether the event
/// should keep travelling: a `Consumed` event is not passed on to the application-level
/// handler for its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventHandlerReturn {
    /// Pass the event on to the next handler.
    #[default]
    Ignored,
    /// Stop the event here.
    Consumed,
}

impl EventHandlerReturn {
    pub fn is_consumed(self) -> bool {
        matches!(self, EventHandlerReturn::Consumed)
    }
}

impl From<bool> for EventHandlerReturn {
    /// `true` means consumed.
    fn from(consumed: bool) -> Self {
        if consumed {
            EventHandlerReturn::Consumed
        } else {
            EventHandlerReturn::Ignored
        }
    }
}
