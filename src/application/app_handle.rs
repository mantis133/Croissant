use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;

use crate::{
    ManagedState,
    application::{
        Emitter,
        command::Command,
        jobs::{JobEnded, JobId, JobOutcome, JobRegistry, SpawnError},
        value_store::ValueStore,
    },
    events::ApplicationEvent,
};

/// A queued background future, boxed so futures of different concrete types can share a
/// queue.
pub(super) type SpawnedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// An activity's handle to the application it is running inside.
///
/// Every lifecycle and event callback receives `&mut AppHandle`. Through it an activity can
/// read and write the application's named [`ValueStore`], emit further events, and navigate
/// between activities — without ever holding a borrow on the `Application` itself.
///
/// Navigation, emitted events, and exit are *queued*: they take effect once the current
/// callback returns, in the order they were requested. Emitted events are dispatched before
/// the application pulls the next event from its producers.
///
/// ```no_run
/// # use croissant::{activities::ActivityBuilder, application::AppHandle, events::EventHandlerReturn};
/// # #[derive(Debug, Default)] struct Screen;
/// # impl croissant::ManagedState for Screen {}
/// # #[derive(Debug)] struct Tick;
/// # impl croissant::events::ApplicationEvent for Tick {}
/// ActivityBuilder::<Screen>::new().on_event(|_state: &mut Screen, _event: &Tick, app: &mut AppHandle| {
///     let ticks = app.get_or_insert_with("ticks", || 0u32);
///     *ticks += 1;
///     if *ticks >= 10 {
///         app.exit();
///     }
///     EventHandlerReturn::Consumed
/// });
/// ```
pub struct AppHandle {
    pub(super) store: ValueStore,
    pub(super) commands: VecDeque<Command>,
    pub(super) pending_events: VecDeque<Box<dyn ApplicationEvent>>,
    pub(super) pending_spawns: Vec<(JobId, SpawnedFuture)>,
    pub(super) jobs: JobRegistry,
    pub(super) emitter: Emitter,
    pub(super) exiting: bool,
}

impl AppHandle {
    pub(super) fn new(store: ValueStore, emitter: Emitter) -> Self {
        AppHandle {
            store,
            commands: VecDeque::new(),
            pending_events: VecDeque::new(),
            pending_spawns: Vec::new(),
            jobs: JobRegistry::default(),
            emitter,
            exiting: false,
        }
    }

    // ---------------------------------------------------------------- named values

    /// Stores `value` under `key`, replacing whatever was there before.
    pub fn set<T: Any + Send>(&mut self, key: impl Into<String>, value: T) {
        self.store.set(key, value);
    }

    /// Borrows the named value, or `None` if it is absent or not a `T`.
    pub fn get<T: Any + Send>(&self, key: &str) -> Option<&T> {
        self.store.get::<T>(key)
    }

    /// Mutably borrows the named value, or `None` if it is absent or not a `T`.
    pub fn get_mut<T: Any + Send>(&mut self, key: &str) -> Option<&mut T> {
        self.store.get_mut::<T>(key)
    }

    /// Mutably borrows the named value, inserting `f()` first if it is absent.
    ///
    /// # Panics
    ///
    /// Panics if `key` already holds a value of a different type.
    pub fn get_or_insert_with<T, F>(&mut self, key: impl Into<String>, f: F) -> &mut T
    where
        T: Any + Send,
        F: FnOnce() -> T,
    {
        self.store.get_or_insert_with(key, f)
    }

    /// Removes the named value and returns it, if it is present and is a `T`.
    pub fn take<T: Any + Send>(&mut self, key: &str) -> Option<T> {
        self.store.take::<T>(key)
    }

    /// Removes the named value regardless of type. Returns whether one was there.
    pub fn remove(&mut self, key: &str) -> bool {
        self.store.remove(key)
    }

    /// Whether a value of any type is stored under `key`.
    pub fn contains(&self, key: &str) -> bool {
        self.store.contains(key)
    }

    /// The full named-value store, for bulk access.
    pub fn values(&self) -> &ValueStore {
        &self.store
    }

    /// The full named-value store, for bulk access.
    pub fn values_mut(&mut self) -> &mut ValueStore {
        &mut self.store
    }

    // ---------------------------------------------------------------- events

    /// Queues `event` for dispatch. It is delivered after the current event finishes
    /// processing and before the application takes another event from its producers.
    ///
    /// This is the *inside a callback* path. To report an event from a spawned future, use
    /// an [`Emitter`] from [`AppHandle::emitter`] instead: it is `Send` and sends through a
    /// channel, so it arrives the next time the loop polls rather than jumping the queue.
    pub fn emit<E: ApplicationEvent>(&mut self, event: E) {
        self.emit_boxed(Box::new(event));
    }

    /// [`AppHandle::emit`] for an already-boxed event.
    pub fn emit_boxed(&mut self, event: Box<dyn ApplicationEvent>) {
        self.pending_events.push_back(event);
    }

    // ---------------------------------------------------------------- background work

    /// A cloneable handle for reporting events back from a spawned future.
    ///
    /// Unlike `AppHandle`, an [`Emitter`] is `Send + Sync + 'static`, so it can be moved into
    /// a task and outlive both this callback and the activity that made it.
    pub fn emitter(&self) -> Emitter {
        self.emitter.clone()
    }

    /// Runs `future` in the background, on the application's `tokio` runtime.
    ///
    /// The work is *not* tied to the activity that started it: navigating elsewhere, or even
    /// destroying that activity, leaves it running. Report results with an [`Emitter`], which
    /// delivers them back into the event loop as ordinary events.
    ///
    /// The future is queued and handed to the runtime once the current callback returns —
    /// the same deferral as [`AppHandle::push`] and [`AppHandle::emit`] — so this is safe to
    /// call from anywhere, including outside a runtime context. The returned [`JobId`] is
    /// minted immediately even so, and is valid straight away: the job can be cancelled
    /// before it has ever run.
    ///
    /// [`Application::run`](crate::application::Application::run) does not return while work
    /// is still in flight, and [`AppHandle::exit`] aborts whatever is still running.
    ///
    /// # Errors
    ///
    /// [`SpawnError::Exiting`] if the application is already shutting down, which is the case
    /// inside the teardown callbacks. Nothing is queued.
    pub fn spawn<F>(&mut self, future: F) -> Result<JobId, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.check_spawnable(None)?;
        Ok(self.enqueue(None, Box::pin(future)))
    }

    /// [`AppHandle::spawn`] under a name, so the job can be found and cancelled later without
    /// anyone having to carry its [`JobId`] around.
    ///
    /// At most one live job holds a given key. That makes the restart pattern explicit rather
    /// than accidental — a second `spawn_keyed("search", ..)` is refused while the first is
    /// still running, and the caller decides whether to keep the old work or
    /// [`cancel`](AppHandle::cancel) it and try again:
    ///
    /// ```no_run
    /// # use croissant::application::{AppHandle, SpawnError};
    /// # fn example(app: &mut AppHandle) {
    /// # let fresh = async {};
    /// if let Err(SpawnError::KeyInUse { running, .. }) = app.spawn_keyed("search", fresh) {
    ///     app.cancel(running);
    ///     // ... and spawn again
    /// }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`SpawnError::KeyInUse`] if a live job already holds `key` — that job is left alone and
    /// `future` is dropped without running. [`SpawnError::Exiting`] if the application is
    /// shutting down.
    pub fn spawn_keyed<F>(&mut self, key: impl Into<String>, future: F) -> Result<JobId, SpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let key = key.into();
        self.check_spawnable(Some(&key))?;
        Ok(self.enqueue(Some(key), Box::pin(future)))
    }

    /// [`AppHandle::spawn`] for the common case of a future that needs an [`Emitter`],
    /// saving the separate `let emitter = app.emitter();` binding.
    ///
    /// ```no_run
    /// # use croissant::{application::AppHandle, events::ApplicationEvent};
    /// # #[derive(Debug)] struct Loaded(String);
    /// # impl ApplicationEvent for Loaded {}
    /// # async fn fetch() -> String { String::new() }
    /// # fn example(app: &mut AppHandle) {
    /// let _ = app.spawn_with(|emitter| async move {
    ///     emitter.emit(Loaded(fetch().await));
    /// });
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// As [`AppHandle::spawn`]. `f` is not called if the spawn is refused.
    pub fn spawn_with<F, Fut>(&mut self, f: F) -> Result<JobId, SpawnError>
    where
        F: FnOnce(Emitter) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.check_spawnable(None)?;
        let emitter = self.emitter();
        Ok(self.enqueue(None, Box::pin(f(emitter))))
    }

    /// [`AppHandle::spawn_keyed`] handed an [`Emitter`], the way [`AppHandle::spawn_with`] is
    /// to [`AppHandle::spawn`].
    ///
    /// # Errors
    ///
    /// As [`AppHandle::spawn_keyed`]. `f` is not called if the spawn is refused, so a rejected
    /// key costs nothing beyond the check.
    pub fn spawn_keyed_with<F, Fut>(
        &mut self,
        key: impl Into<String>,
        f: F,
    ) -> Result<JobId, SpawnError>
    where
        F: FnOnce(Emitter) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let key = key.into();
        self.check_spawnable(Some(&key))?;
        let emitter = self.emitter();
        Ok(self.enqueue(Some(key), Box::pin(f(emitter))))
    }

    // ---------------------------------------------------------------- cancellation

    /// Stops the job with this id, returning whether one was live to stop.
    ///
    /// The job is forgotten immediately — [`AppHandle::is_running`] reports `false` before
    /// this call even returns — and a [`JobEnded`] with [`JobOutcome::Cancelled`] is queued
    /// like any other emitted event. A job still waiting to reach the runtime is dropped
    /// without ever being polled.
    ///
    /// Cancelling is not a promise that the future was interrupted: work that had already
    /// finished but had not yet been collected by the loop still reports `Cancelled`.
    pub fn cancel(&mut self, id: JobId) -> bool {
        self.finish(id, JobOutcome::Cancelled)
    }

    /// [`AppHandle::cancel`] for a job spawned with [`AppHandle::spawn_keyed`].
    ///
    /// This is the point of keys: an activity can stop work another activity started without
    /// either of them passing a handle to the other.
    pub fn cancel_key(&mut self, key: &str) -> bool {
        match self.jobs.find_key(key) {
            Some(id) => self.cancel(id),
            None => false,
        }
    }

    /// Stops every live job, each reporting its own [`JobEnded`].
    ///
    /// Unlike [`AppHandle::exit`] this leaves the application running.
    pub fn cancel_all(&mut self) {
        for id in self.jobs.ids() {
            self.cancel(id);
        }
    }

    /// Whether this job is still live — queued, running, or finished but not yet collected.
    pub fn is_running(&self, id: JobId) -> bool {
        self.jobs.contains(id)
    }

    /// [`AppHandle::is_running`] for a key.
    ///
    /// A key is free once its [`JobEnded`] has been dispatched, which can be a moment after
    /// the future itself returned.
    pub fn is_key_running(&self, key: &str) -> bool {
        self.jobs.find_key(key).is_some()
    }

    /// Every live job and its key, if it has one, in no particular order.
    pub fn jobs(&self) -> impl Iterator<Item = (JobId, Option<&str>)> {
        self.jobs.iter()
    }

    /// How many jobs are live.
    pub fn job_count(&self) -> usize {
        self.jobs.len()
    }

    /// The one gate every `spawn` goes through, checked before the caller's closure runs so a
    /// refusal costs nothing.
    fn check_spawnable(&self, key: Option<&str>) -> Result<(), SpawnError> {
        if self.exiting {
            return Err(SpawnError::Exiting);
        }
        if let Some(key) = key
            && let Some(running) = self.jobs.find_key(key)
        {
            return Err(SpawnError::KeyInUse {
                key: key.to_string(),
                running,
            });
        }
        Ok(())
    }

    fn enqueue(&mut self, key: Option<String>, future: SpawnedFuture) -> JobId {
        let id = self.jobs.mint(key);
        self.pending_spawns.push((id, future));
        id
    }

    /// The one exit every job takes, so aborting and reporting can never drift apart.
    fn finish(&mut self, id: JobId, outcome: JobOutcome) -> bool {
        let Some(entry) = self.jobs.remove(id) else {
            return false;
        };
        // Absent for a job that has not reached the runtime yet; `drain_spawns` drops that
        // one on the floor when it finds the id no longer registered.
        if let Some(abort) = &entry.abort {
            abort.abort();
        }
        self.emit(JobEnded {
            id,
            key: entry.key,
            outcome,
        });
        true
    }

    /// Binds a job to the runtime task now carrying it. Called by `drain_spawns`.
    pub(super) fn attach_abort(&mut self, id: JobId, abort: tokio::task::AbortHandle) {
        self.jobs.attach(id, abort);
    }

    /// Reports a job the loop collected from the `JoinSet`.
    pub(super) fn report_finished(&mut self, task: tokio::task::Id, outcome: JobOutcome) {
        // An untracked task was cancelled, and `cancel` already reported it. The id cannot
        // have been recycled by a later job in the meantime: a task keeps its `Id` reserved
        // for as long as the `JoinSet` holds it, which is right up to this collection.
        let Some(id) = self.jobs.find_task(task) else {
            return;
        };
        let Some(entry) = self.jobs.remove(id) else {
            return;
        };
        self.emit(JobEnded {
            id,
            key: entry.key,
            outcome,
        });
    }

    /// Drops futures queued behind an `exit`, for the same reason the command queue is
    /// cleared: nothing after that point takes effect.
    pub(super) fn discard_pending_spawns(&mut self) {
        for (id, _) in self.pending_spawns.drain(..) {
            self.jobs.remove(id);
        }
    }

    /// Forgets every job, silently. For after `JoinSet::shutdown`, which kills the tasks
    /// without ever handing their results back — so nothing would otherwise clear them, and a
    /// second `run` would inherit a registry full of jobs that died with the first.
    pub(super) fn discard_all_jobs(&mut self) {
        self.pending_spawns.clear();
        self.jobs.clear();
    }

    // ---------------------------------------------------------------- navigation

    /// Pauses the active activity and makes a new `A` current.
    ///
    /// The instance is default-constructed and its `on_create` runs before `on_resume`, so
    /// `on_create` is where its fields get their real values — there is no need for an
    /// `A::new()` that spells out every field.
    ///
    /// The outgoing activity is kept on the backstack if the application was built with
    /// [`ApplicationBuilder::backstack`](crate::application::ApplicationBuilder::backstack);
    /// otherwise it is destroyed.
    pub fn push<A: ManagedState + Default>(&mut self) {
        self.push_with(A::default());
    }

    /// [`AppHandle::push`] with an instance you construct yourself, for states that are not
    /// `Default` or that need a value passed straight in. `on_create` still runs.
    pub fn push_with<A: ManagedState>(&mut self, activity: A) {
        self.commands.push_back(Command::Push(Box::new(activity)));
    }

    /// Returns to the activity beneath the current one on the backstack.
    ///
    /// Does nothing if there is no backstack or it is empty.
    pub fn pop(&mut self) {
        self.commands.push_back(Command::Pop);
    }

    /// Swaps the active activity for a new `A` without touching the backstack.
    ///
    /// The outgoing activity is destroyed. The incoming one is default-constructed and gets
    /// `on_create` before `on_resume`.
    pub fn replace<A: ManagedState + Default>(&mut self) {
        self.replace_with(A::default());
    }

    /// [`AppHandle::replace`] with an instance you construct yourself.
    pub fn replace_with<A: ManagedState>(&mut self, activity: A) {
        self.commands
            .push_back(Command::Replace(Box::new(activity)));
    }

    /// Stops the event loop. Any commands queued after this one are discarded.
    pub fn exit(&mut self) {
        self.commands.push_back(Command::Exit);
    }

    /// Whether [`AppHandle::exit`] has already taken effect.
    pub fn is_exiting(&self) -> bool {
        self.exiting
    }
}

impl std::fmt::Debug for AppHandle {
    /// Queued futures are opaque, so they are reported by count.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppHandle")
            .field("store", &self.store)
            .field("commands", &self.commands)
            .field("pending_events", &self.pending_events)
            .field("pending_spawns", &self.pending_spawns.len())
            .field("jobs", &self.jobs)
            .field("emitter", &self.emitter)
            .field("exiting", &self.exiting)
            .finish()
    }
}
