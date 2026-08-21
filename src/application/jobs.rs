use std::collections::HashMap;

use tokio::task::AbortHandle;

use crate::events::ApplicationEvent;

/// A handle to one piece of background work started with
/// [`AppHandle::spawn`](crate::application::AppHandle::spawn).
///
/// Ids are minted at the moment `spawn` is called, before the future reaches the runtime, and
/// are never reused — not even across two calls to
/// [`Application::run`](crate::application::Application::run). A `JobId` stashed in a
/// `#[global]` field can therefore never come to mean a different job later; it simply stops
/// being live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(u64);

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// How a job stopped being live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    /// The future ran to completion.
    Completed,
    /// The application stopped caring — [`cancel`](crate::application::AppHandle::cancel) and
    /// friends. Note this does *not* promise the future was interrupted: a job that had
    /// finished but had not yet been reaped by the loop still reports `Cancelled`.
    Cancelled,
    /// The future panicked. Its siblings, and the application, carry on.
    Panicked,
}

/// Emitted into the event loop when a job stops being live, whatever the reason.
///
/// Every job produces exactly one `JobEnded`, keyed or not. Two things it is deliberately
/// *not*:
///
/// - **It is not a completion barrier.** The loop takes job results and
///   [`Emitter`](crate::application::Emitter) events from two independent sources and does not
///   favour either, so a job's own `emitter.emit(Done)` may still be waiting when
///   `JobEnded { outcome: Completed }` is dispatched. Read the job's result from the event the
///   job sent, never from this one.
/// - **It is not a shutdown notice.** Jobs killed by [`AppHandle::exit`] are not eulogised:
///   the loop has already stopped, so there is nobody left to hear it.
///
/// [`AppHandle::exit`]: crate::application::AppHandle::exit
#[derive(Debug, Clone)]
pub struct JobEnded {
    pub id: JobId,
    /// The key the job was spawned under, if it was given one.
    pub key: Option<String>,
    pub outcome: JobOutcome,
}

impl ApplicationEvent for JobEnded {}

/// Why a future was not spawned.
///
/// Returned rather than swallowed so that the `must_use` on `Result` puts the case in front of
/// whoever asked for the work: a job that never started is rarely something to shrug at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpawnError {
    /// A live job already holds this key, and it keeps it. The new future was **not** started.
    ///
    /// `running` is the incumbent, so the caller can take the key by force rather than
    /// guessing: `app.cancel(running)` and then spawn again.
    ///
    /// A key stays taken until the loop reaps its job, which is the same moment
    /// [`JobEnded`] is dispatched — so "the key is free once `JobEnded` arrives" is the rule,
    /// not "once the future returned".
    KeyInUse { key: String, running: JobId },
    /// [`AppHandle::exit`](crate::application::AppHandle::exit) has already taken effect, so
    /// there is no loop left to report back to. In practice this is reached from the teardown
    /// callbacks — `on_pause`, `on_destroy`, `on_stop` — which run after the loop has stopped.
    Exiting,
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnError::KeyInUse { key, running } => {
                write!(f, "job {running} already holds the key {key:?}")
            }
            SpawnError::Exiting => write!(f, "the application is exiting"),
        }
    }
}

impl std::error::Error for SpawnError {}

/// One tracked job.
pub(super) struct JobEntry {
    pub(super) key: Option<String>,
    /// `None` until `drain_spawns` hands the future to the runtime. Spawning is deferred, so
    /// every job spends the remainder of the callback that started it in this state — long
    /// enough to be cancelled before it ever runs.
    pub(super) abort: Option<AbortHandle>,
}

/// Every live job, by id.
///
/// One map, like [`ValueStore`](crate::application::ValueStore) and
/// [`ServiceRegistry`](crate::application::ServiceRegistry): lookup by key and by
/// [`tokio::task::Id`] are linear scans. Live jobs number in the tens, so a secondary index
/// would buy nothing measurable while adding two derived invariants to maintain across every
/// removal path — and a forgotten removal there is a phantom `KeyInUse` that never clears.
#[derive(Default)]
pub(super) struct JobRegistry {
    jobs: HashMap<JobId, JobEntry>,
    next: u64,
}

impl JobRegistry {
    /// Registers a new job and returns its id. The `AbortHandle` is attached later, by
    /// [`JobRegistry::attach`], once the future actually reaches the runtime.
    pub(super) fn mint(&mut self, key: Option<String>) -> JobId {
        let id = JobId(self.next);
        // Never reset, not even between runs: a stale id must not alias a fresh job.
        self.next += 1;
        self.jobs.insert(id, JobEntry { key, abort: None });
        id
    }

    pub(super) fn attach(&mut self, id: JobId, abort: AbortHandle) {
        if let Some(entry) = self.jobs.get_mut(&id) {
            entry.abort = Some(abort);
        }
    }

    pub(super) fn contains(&self, id: JobId) -> bool {
        self.jobs.contains_key(&id)
    }

    pub(super) fn find_key(&self, key: &str) -> Option<JobId> {
        self.jobs
            .iter()
            .find(|(_, entry)| entry.key.as_deref() == Some(key))
            .map(|(id, _)| *id)
    }

    /// The job a runtime task belongs to, or `None` if it is not tracked any more — which
    /// means it was cancelled and already reported.
    pub(super) fn find_task(&self, task: tokio::task::Id) -> Option<JobId> {
        self.jobs
            .iter()
            .find(|(_, entry)| entry.abort.as_ref().is_some_and(|abort| abort.id() == task))
            .map(|(id, _)| *id)
    }

    pub(super) fn remove(&mut self, id: JobId) -> Option<JobEntry> {
        self.jobs.remove(&id)
    }

    pub(super) fn ids(&self) -> Vec<JobId> {
        self.jobs.keys().copied().collect()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (JobId, Option<&str>)> {
        self.jobs
            .iter()
            .map(|(id, entry)| (*id, entry.key.as_deref()))
    }

    pub(super) fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Forgets every job without aborting or reporting anything. For the one caller that has
    /// already killed the tasks by other means.
    pub(super) fn clear(&mut self) {
        self.jobs.clear();
    }
}

impl std::fmt::Debug for JobRegistry {
    /// Abort handles say nothing useful, so this prints the live ids and their keys.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut jobs: Vec<String> = self
            .jobs
            .iter()
            .map(|(id, entry)| match &entry.key {
                Some(key) => format!("{id}:{key}"),
                None => format!("{id}"),
            })
            .collect();
        jobs.sort();
        f.debug_struct("JobRegistry")
            .field("count", &self.jobs.len())
            .field("jobs", &jobs)
            .finish()
    }
}
