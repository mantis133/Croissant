//! File-watching event producers, exercised against real files on disk.
//!
//! Timings here are generous on purpose: filesystem notification latency varies by platform
//! and by load, and a flaky watcher test is worse than none.

#![cfg(feature = "watch")]

use std::path::PathBuf;
use std::time::Duration;

use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application},
    events::{ApplicationEvent, EventHandlerReturn},
    streams::{FileChange, FileChangeKind, directory_event_stream, file_event_stream},
};
use futures::StreamExt;

const DEBOUNCE: Duration = Duration::from_millis(100);
const PATIENCE: Duration = Duration::from_secs(10);

/// A scratch directory that cleans itself up.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("croissant-watch-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create scratch dir");
        TempDir(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Waits for the next change, failing rather than hanging if nothing arrives.
async fn next_change(stream: &mut (impl StreamExt<Item = FileChange> + Unpin)) -> FileChange {
    tokio::time::timeout(PATIENCE, stream.next())
        .await
        .expect("timed out waiting for a file change")
        .expect("stream ended unexpectedly")
}

#[tokio::test]
async fn a_file_change_is_reported() {
    let dir = TempDir::new("basic");
    let file = dir.join("config.toml");
    std::fs::write(&file, "a = 1").unwrap();

    let mut changes = Box::pin(
        file_event_stream(&file, DEBOUNCE, Some).expect("watch the file"),
    );

    std::fs::write(&file, "a = 2").unwrap();

    let change = next_change(&mut changes).await;
    assert_eq!(change.path.file_name().unwrap(), "config.toml");
    assert_ne!(change.kind, FileChangeKind::Removed);
}

/// The reason the helper watches the parent directory: editors replace files by renaming a
/// temporary over the target, which orphans a watch registered on the file itself.
#[tokio::test]
async fn a_rename_over_the_target_is_still_seen() {
    let dir = TempDir::new("rename");
    let file = dir.join("config.toml");
    let temp = dir.join("config.toml.tmp");
    std::fs::write(&file, "a = 1").unwrap();

    let mut changes = Box::pin(
        file_event_stream(&file, DEBOUNCE, Some).expect("watch the file"),
    );

    // Exactly what an editor does on save.
    std::fs::write(&temp, "a = 2").unwrap();
    std::fs::rename(&temp, &file).unwrap();

    let change = next_change(&mut changes).await;
    assert_eq!(change.path.file_name().unwrap(), "config.toml");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "a = 2");
}

/// A file that does not exist yet is picked up when it appears.
#[tokio::test]
async fn a_file_created_later_is_reported() {
    let dir = TempDir::new("created");
    let file = dir.join("appears-later.toml");

    let mut changes = Box::pin(
        file_event_stream(&file, DEBOUNCE, Some).expect("watch a not-yet-existing file"),
    );

    std::fs::write(&file, "hello").unwrap();

    let change = next_change(&mut changes).await;
    assert_eq!(change.path.file_name().unwrap(), "appears-later.toml");
}

/// Only the named file is reported, not its neighbours.
#[tokio::test]
async fn siblings_in_the_same_directory_are_filtered_out() {
    let dir = TempDir::new("filter");
    let watched = dir.join("watched.toml");
    let ignored = dir.join("ignored.toml");
    std::fs::write(&watched, "a = 1").unwrap();
    std::fs::write(&ignored, "b = 1").unwrap();

    let mut changes = Box::pin(
        file_event_stream(&watched, DEBOUNCE, Some).expect("watch the file"),
    );

    // Touch the neighbour first; it must not surface.
    std::fs::write(&ignored, "b = 2").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::write(&watched, "a = 2").unwrap();

    let change = next_change(&mut changes).await;
    assert_eq!(
        change.path.file_name().unwrap(),
        "watched.toml",
        "the neighbour's change should never have been emitted"
    );
}

#[tokio::test]
async fn a_directory_reports_any_entry() {
    let dir = TempDir::new("directory");

    let mut changes = Box::pin(
        directory_event_stream(&dir.0, false, DEBOUNCE, Some).expect("watch the directory"),
    );

    std::fs::write(dir.join("anything.txt"), "x").unwrap();

    let change = next_change(&mut changes).await;
    assert_eq!(change.path.file_name().unwrap(), "anything.txt");
}

#[tokio::test]
async fn watching_a_missing_directory_fails_loudly() {
    let missing = std::env::temp_dir().join("croissant-watch-definitely-not-here");
    let _ = std::fs::remove_dir_all(&missing);

    let result = directory_event_stream(&missing, false, DEBOUNCE, Some);
    let error = match result {
        Ok(_) => panic!("watching a missing directory should fail"),
        Err(error) => error,
    };

    assert_eq!(error.path(), missing.as_path());
    // The backend error is reachable without naming its type.
    assert!(std::error::Error::source(&error).is_some());
    assert!(error.to_string().contains("could not watch"));
}

// ---------------------------------------------------------------- end to end

#[derive(Debug, Default)]
struct Screen;
impl ManagedState for Screen {}

#[derive(Debug)]
struct ConfigChanged;
impl ApplicationEvent for ConfigChanged {}

/// The whole point: a file change becomes an ordinary application event.
#[tokio::test]
async fn a_file_change_drives_the_application() {
    let dir = TempDir::new("endtoend");
    let file = dir.join("config.toml");
    std::fs::write(&file, "a = 1").unwrap();

    let producer = file_event_stream(&file, DEBOUNCE, |change: FileChange| match change.kind {
        FileChangeKind::Created | FileChangeKind::Modified => {
            Some(Box::new(ConfigChanged) as Box<dyn ApplicationEvent>)
        }
        _ => None,
    })
    .expect("watch the config file");

    let screen = ActivityBuilder::<Screen>::new()
        .on_event(
            |_state: &mut Screen, _event: &ConfigChanged, app: &mut AppHandle| {
                *app.get_or_insert_with("reloads", || 0u32) += 1;
                app.exit(); // one reload is enough to prove the wiring
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .add_activity(screen)
        .starting_activity::<Screen>()
        .add_event_producer(producer)
        .build();

    let edit = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(&file, "a = 2").unwrap();
    });

    tokio::time::timeout(PATIENCE, app.run())
        .await
        .expect("the application should have exited after the config changed");
    edit.await.unwrap();

    assert_eq!(app.values().get::<u32>("reloads"), Some(&1));
}
