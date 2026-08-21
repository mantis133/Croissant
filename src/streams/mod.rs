mod timer;
pub use timer::timer_event_stream;

#[cfg(feature = "watch")]
mod watch;
#[cfg(feature = "watch")]
pub use watch::{FileChange, FileChangeKind, WatchError, directory_event_stream, file_event_stream};
