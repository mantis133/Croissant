use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::{Stream, StreamExt};

use crate::EventStream;
use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use tokio::sync::mpsc;

/// What happened to a watched path.
///
/// Deliberately coarser than the underlying backend's event model: reacting to a config file
/// rarely needs to know *how* it changed, and keeping the distinction shallow keeps the
/// backend an implementation detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileChangeKind {
    Created,
    Modified,
    Removed,
    /// Something happened that does not map to the three above — an access, or a
    /// platform-specific event.
    Other,
}

impl From<&EventKind> for FileChangeKind {
    fn from(kind: &EventKind) -> Self {
        match kind {
            EventKind::Create(_) => FileChangeKind::Created,
            EventKind::Modify(_) => FileChangeKind::Modified,
            EventKind::Remove(_) => FileChangeKind::Removed,
            _ => FileChangeKind::Other,
        }
    }
}

/// A single change to a watched path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileChange {
    pub path: PathBuf,
    pub kind: FileChangeKind,
}

/// Watching a path failed.
///
/// The underlying backend error is available through
/// [`source`](std::error::Error::source), but its type is not part of this crate's public
/// API, so the backend can be replaced without breaking callers.
#[derive(Debug)]
pub struct WatchError {
    path: PathBuf,
    source: Option<notify::Error>,
}

impl WatchError {
    /// The path that could not be watched.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl fmt::Display for WatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "could not watch `{}`", self.path.display())
    }
}

impl std::error::Error for WatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|error| error as &(dyn std::error::Error + 'static))
    }
}

/// Emits an event whenever `path` changes on disk.
///
/// The mapping function receives each [`FileChange`] and returns `Some(event)` to emit it or
/// `None` to discard it. `debounce` is how long to wait for a burst of activity to settle: a
/// single editor save typically produces several filesystem events, and this collapses them
/// into one. Something in the region of 200ms is usually right — too short and a reload fires
/// repeatedly, too long and it feels laggy.
///
/// ```no_run
/// # use std::time::Duration;
/// # use croissant::{events::ApplicationEvent, streams::{file_event_stream, FileChangeKind}};
/// # #[derive(Debug)] struct ConfigChanged;
/// # impl ApplicationEvent for ConfigChanged {}
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = file_event_stream("config.toml", Duration::from_millis(200), |change| {
///     match change.kind {
///         FileChangeKind::Created | FileChangeKind::Modified => {
///             Some(Box::new(ConfigChanged) as Box<dyn ApplicationEvent>)
///         }
///         _ => None,
///     }
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// The file does **not** have to exist yet — its directory is what is watched, so a file
/// created later is picked up. That indirection is also what keeps this working across
/// editor saves: many editors write a temporary file and rename it over the target, which
/// replaces the inode and would silently orphan a watch registered on the file itself.
///
/// # Errors
///
/// Fails if `path` has no filename component, or if its directory cannot be watched —
/// usually because the directory does not exist.
pub fn file_event_stream<AppEvent, F, P>(
    path: P,
    debounce: Duration,
    f: F,
) -> Result<EventStream<AppEvent>, WatchError>
where
    F: Fn(FileChange) -> Option<AppEvent> + Send + 'static,
    P: AsRef<Path>,
    AppEvent: Send + 'static,
{
    let path = path.as_ref();
    let name = path.file_name().ok_or_else(|| WatchError {
        path: path.to_path_buf(),
        source: None,
    })?;

    // A bare filename has an empty parent, which is not a watchable directory.
    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };

    let changes = change_stream(
        directory,
        RecursiveMode::NonRecursive,
        Some(name.to_os_string()),
        debounce,
    )?;
    Ok(map_changes(changes, f))
}

/// Emits an event whenever anything inside `path` changes.
///
/// As [`file_event_stream`], but for a whole directory and without a filename filter. Set
/// `recursive` to follow subdirectories.
///
/// Note that a rename within the directory reports **two** changes — a [`FileChangeKind::Removed`]
/// for the old name and a [`FileChangeKind::Created`] for the new one — because from the
/// directory's point of view that is what happened.
///
/// # Errors
///
/// Fails if `path` cannot be watched, usually because it does not exist or is not a
/// directory.
pub fn directory_event_stream<AppEvent, F, P>(
    path: P,
    recursive: bool,
    debounce: Duration,
    f: F,
) -> Result<EventStream<AppEvent>, WatchError>
where
    F: Fn(FileChange) -> Option<AppEvent> + Send + 'static,
    P: AsRef<Path>,
    AppEvent: Send + 'static,
{
    let mode = if recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    let changes = change_stream(path.as_ref().to_path_buf(), mode, None, debounce)?;
    Ok(map_changes(changes, f))
}

/// Applies the caller's mapping and boxes the result.
///
/// Boxing is what keeps the returned stream free of the path's lifetime: an `impl Stream`
/// would capture every lifetime in scope under edition 2024, so passing `&path` would tie
/// the producer to that borrow for no reason. `add_event_producer` boxes anyway, so this
/// costs nothing.
fn map_changes<AppEvent, F>(
    changes: impl Stream<Item = FileChange> + Send + 'static,
    f: F,
) -> EventStream<AppEvent>
where
    F: Fn(FileChange) -> Option<AppEvent> + Send + 'static,
    AppEvent: Send + 'static,
{
    Box::pin(changes.filter_map(move |change| {
        let mapped = f(change);
        async move { mapped }
    }))
}

/// The shared machinery: watch `directory`, optionally keeping only entries whose filename
/// matches `only`, and deliver each change as a [`FileChange`].
fn change_stream(
    directory: PathBuf,
    mode: RecursiveMode,
    only: Option<OsString>,
    debounce: Duration,
) -> Result<impl Stream<Item = FileChange> + Send + 'static, WatchError> {
    let (sender, receiver) = mpsc::unbounded_channel();

    let mut debouncer = new_debouncer(debounce, None, move |result: DebounceEventResult| {
        // A backend error means this batch is unusable; there is nothing sensible to report
        // through a stream of changes, so drop it and keep watching.
        let Ok(events) = result else {
            return;
        };
        for event in events {
            let kind = FileChangeKind::from(&event.event.kind);
            for path in &event.event.paths {
                if let Some(name) = &only
                    && path.file_name() != Some(name.as_os_str())
                {
                    continue;
                }
                // Failure means the stream was dropped; the watcher goes with it.
                let _ = sender.send(FileChange {
                    path: path.clone(),
                    kind,
                });
            }
        }
    })
    .map_err(|error| WatchError {
        path: directory.clone(),
        source: Some(error),
    })?;

    debouncer.watch(&directory, mode).map_err(|error| WatchError {
        path: directory.clone(),
        source: Some(error),
    })?;

    // The debouncer owns the watcher, and dropping it silently stops delivery — so it rides
    // along in the stream's state and lives exactly as long as the stream does.
    Ok(futures::stream::unfold(
        (receiver, debouncer),
        |(mut receiver, debouncer)| async move {
            let change = receiver.recv().await?;
            Some((change, (receiver, debouncer)))
        },
    ))
}
