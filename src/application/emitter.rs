use tokio::sync::mpsc::UnboundedSender;

use crate::events::ApplicationEvent;

/// A cloneable, sendable channel back into the application's event loop.
///
/// This is how work spawned with [`AppHandle::spawn`](crate::application::AppHandle::spawn)
/// reports back. Unlike [`AppHandle`](crate::application::AppHandle), an `Emitter` is
/// `Send + Sync + 'static`, so it can be moved into a `tokio` task and outlive the callback —
/// and the activity — that created it.
///
/// ```no_run
/// # use croissant::{application::AppHandle, events::ApplicationEvent};
/// # #[derive(Debug)] struct Loaded(String);
/// # impl ApplicationEvent for Loaded {}
/// # async fn fetch() -> String { String::new() }
/// # fn example(app: &mut AppHandle) {
/// let emitter = app.emitter();
/// let _ = app.spawn(async move {
///     let body = fetch().await;
///     emitter.emit(Loaded(body));
/// });
/// # }
/// ```
///
/// The channel is unbounded on purpose: a bounded one would let a spawned task block on send
/// while the loop is busy elsewhere, which is a deadlock waiting to happen given the loop is
/// the only reader.
#[derive(Clone)]
pub struct Emitter {
    sender: UnboundedSender<Box<dyn ApplicationEvent>>,
}

impl Emitter {
    pub(super) fn new(sender: UnboundedSender<Box<dyn ApplicationEvent>>) -> Self {
        Emitter { sender }
    }

    /// Sends `event` to the application.
    ///
    /// Returns `false` if the event loop has already stopped, which is a spawned task's cue
    /// that there is no longer anyone to report to.
    pub fn emit<E: ApplicationEvent>(&self, event: E) -> bool {
        self.emit_boxed(Box::new(event))
    }

    /// [`Emitter::emit`] for an already-boxed event.
    pub fn emit_boxed(&self, event: Box<dyn ApplicationEvent>) -> bool {
        self.sender.send(event).is_ok()
    }

    /// Whether the event loop is still running and able to receive events.
    pub fn is_open(&self) -> bool {
        !self.sender.is_closed()
    }
}

impl std::fmt::Debug for Emitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Emitter")
            .field("open", &self.is_open())
            .finish()
    }
}
