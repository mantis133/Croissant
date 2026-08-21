use std::any::{Any, TypeId};
use std::collections::HashMap;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinSet;

#[cfg(feature = "logging")]
use tracing::info;

use crate::{
    EventStream, ManagedState,
    activities::AnyActivity,
    application::{
        AppHandle, ApplicationBuilder, JobOutcome, ServiceRegistry, command::Command,
        value_store::ValueStore,
    },
    events::{ApplicationEvent, EventHandlerReturn},
    tasks::TaskEntry,
};

pub(super) type AppEventHandler =
    Box<dyn FnMut(&dyn ApplicationEvent, &mut AppHandle) -> EventHandlerReturn>;
pub(super) type AppPostEventHandler = Box<dyn FnMut(&dyn ApplicationEvent, &mut AppHandle)>;

/// The running application: the activity registry, the backstack, the event producers, and
/// the [`AppHandle`] that every callback is given.
///
/// `handle` is deliberately a plain field rather than something reached through `&mut self`.
/// Handing an activity a `&mut Application` is impossible — the activity is itself borrowed
/// out of `self.activities` for the duration of the call — so the pieces an activity is
/// allowed to touch live in their own field and are borrowed disjointly from the registry.
pub struct Application {
    pub(super) handle: AppHandle,
    pub(super) services: ServiceRegistry,
    pub(super) active_activity: Box<dyn ManagedState>,
    pub(super) activities: HashMap<TypeId, Box<dyn AnyActivity>>,
    pub(super) events: futures::stream::SelectAll<EventStream<Box<dyn ApplicationEvent>>>,
    pub(super) event_handlers: HashMap<TypeId, AppEventHandler>,
    pub(super) post_event: Option<AppPostEventHandler>,
    pub(super) backstack: Option<Vec<Box<dyn ManagedState>>>,
    /// Always-active handlers, in registration order.
    pub(super) tasks_registry: Vec<TaskEntry>,
    /// Background work spawned through [`AppHandle::spawn`], tracked so the loop knows what
    /// is still in flight and can abort it on exit.
    pub(super) tasks: JoinSet<()>,
    /// The receiving end of every [`Emitter`](crate::application::Emitter).
    pub(super) task_events: UnboundedReceiver<Box<dyn ApplicationEvent>>,
}

/// What the loop picked up on one turn.
enum Next {
    Event(Box<dyn ApplicationEvent>),
    /// Every external producer has ended; only background work can speak now.
    ProducersDone,
    /// A spawned future finished. `Err` means it panicked or was aborted. The task id is what
    /// ties the result back to the job that asked for it.
    Finished(Result<(tokio::task::Id, ()), tokio::task::JoinError>),
}

impl Application {
    pub fn builder() -> ApplicationBuilder {
        ApplicationBuilder::new()
    }

    /// The handle activities are given. Use it to seed named values before [`run`](Self::run),
    /// or to read them back once it returns.
    pub fn handle(&mut self) -> &mut AppHandle {
        &mut self.handle
    }

    /// The application's named-value store.
    pub fn values(&self) -> &ValueStore {
        self.handle.values()
    }

    /// The application's named-value store.
    pub fn values_mut(&mut self) -> &mut ValueStore {
        self.handle.values_mut()
    }

    /// The application's registered services.
    pub fn services(&self) -> &ServiceRegistry {
        &self.services
    }

    /// Runs the event loop until an activity or handler calls [`AppHandle::exit`], or until
    /// every event producer is exhausted *and* no background work is still in flight.
    ///
    /// Waiting on in-flight work is what makes [`AppHandle::spawn`] useful: a spawned future
    /// still gets to report its result even if the producers dried up meanwhile. A
    /// deliberately endless spawned future would therefore keep the application alive — such
    /// work belongs in an event producer, or the application should end via
    /// [`AppHandle::exit`], which aborts whatever is still running.
    pub async fn run(&mut self) {
        self.handle.exiting = false;

        // Tasks start first, so an activity's on_create can rely on what they set up.
        self.start_tasks();
        self.settle();

        // on_create then on_resume for the starting activity.
        self.create_active();
        self.resume_active();
        self.settle();

        let mut producers_done = false;

        while !self.handle.exiting {
            // Events emitted from a callback jump the queue: they are dispatched before the
            // application asks its producers or its background work for anything new.
            if let Some(event) = self.handle.pending_events.pop_front() {
                self.dispatch(event.as_ref());
                self.settle();
                continue;
            }

            // Nothing left to wait on: no producer can speak again and nothing is running.
            //
            // Testing `is_empty` *before* draining the channel is what makes this safe. Once
            // every task has finished, nothing new can arrive, so whatever `try_recv` hands
            // back is the whole remainder. Draining first would leave a window in which a
            // task emits and finishes between the two checks, and its event would be
            // dropped on the way out.
            //
            // Draining `pending_events` above this check is the other half of that: anything
            // queueing into it must do so from a point the loop reaches before the next
            // turn's pop. The `Finished` arm — which is where `JobEnded` comes from —
            // qualifies, so a job's ending is always dispatched exactly once, even when it
            // is the last thing the application does.
            if producers_done && self.tasks.is_empty() {
                match self.task_events.try_recv() {
                    Ok(event) => {
                        self.dispatch(event.as_ref());
                        self.settle();
                        continue;
                    }
                    Err(_) => break,
                }
            }

            // Destructured so the three futures borrow disjoint fields. Left unbiased, so a
            // chatty producer cannot starve task results or the other way round.
            let next = {
                let Self {
                    events,
                    task_events,
                    tasks,
                    ..
                } = self;
                tokio::select! {
                    maybe = futures::StreamExt::next(events), if !producers_done => match maybe {
                        Some(event) => Next::Event(event),
                        None => Next::ProducersDone,
                    },
                    Some(event) = task_events.recv() => Next::Event(event),
                    Some(result) = tasks.join_next_with_id(), if !tasks.is_empty() => Next::Finished(result),
                }
            };

            match next {
                Next::Event(event) => {
                    self.dispatch(event.as_ref());
                    self.settle();
                }
                Next::ProducersDone => producers_done = true,
                Next::Finished(result) => {
                    let (task_id, outcome) = match result {
                        Ok((task_id, ())) => (task_id, JobOutcome::Completed),
                        Err(error) => {
                            // A panicking background task is reported, not fatal: the rest of
                            // the application has no reason to come down with it.
                            #[cfg(feature = "logging")]
                            info!("background task ended abnormally: {error}");
                            let outcome = if error.is_panic() {
                                JobOutcome::Panicked
                            } else {
                                JobOutcome::Cancelled
                            };
                            (error.id(), outcome)
                        }
                    };
                    // Queues a `JobEnded`, which the top of the next turn dispatches. That
                    // ordering is exactly what keeps the event from being lost: see the
                    // termination check above.
                    self.handle.report_finished(task_id, outcome);
                }
            }
        }

        // Tear down whatever was active when the loop stopped, then unwind the backstack
        // from the top so activities are destroyed in the reverse of their creation order.
        self.pause_active();
        self.destroy_active();
        while let Some(activity) = self.backstack.as_mut().and_then(Vec::pop) {
            self.destroy(activity);
        }
        self.stop_tasks();

        // Abort anything still running. After a drained exit the set is already empty, so
        // this only bites when the loop was stopped early by `exit`.
        self.tasks.shutdown().await;

        // `shutdown` kills those tasks without ever handing their results back, so nothing
        // else will ever clear them from the registry — and the teardown callbacks above can
        // queue spawns that no `settle` will drain. Forget both, silently: the loop has
        // stopped, so a `JobEnded` now would only be dispatched at the start of the next
        // `run`, before its first activity is even created.
        self.handle.discard_all_jobs();

        // The application is no longer *exiting*; it has exited. Leaving the flag set would
        // make `is_exiting` depend on which way the loop happened to end, and would have the
        // handle refuse work seeded for a second `run`.
        self.handle.exiting = false;
    }

    /// Applies everything a callback left behind: queued navigation, then queued futures.
    fn settle(&mut self) {
        self.drain_commands();
        self.drain_spawns();
    }

    /// Hands queued futures to the runtime. Deferring to here rather than spawning inside
    /// [`AppHandle::spawn`] keeps that method callable outside a runtime context.
    ///
    /// Taken out of the handle first rather than drained in place: the loop body needs the
    /// handle back to record the abort handle, which a live `drain` iterator would still be
    /// borrowing. Nothing can refill the queue meanwhile — `spawn` is only reachable from a
    /// callback, and `settle` runs every callback before this point.
    fn drain_spawns(&mut self) {
        for (id, future) in std::mem::take(&mut self.handle.pending_spawns) {
            // Cancelled while it was still queued, so it never runs at all. `cancel` has
            // already reported it.
            if !self.handle.is_running(id) {
                continue;
            }
            let abort = self.tasks.spawn(future);
            self.handle.attach_abort(id, abort);
        }
    }

    /// Runs one event past the activity handler, the application handler, and both
    /// post-event hooks.
    fn dispatch(&mut self, event: &dyn ApplicationEvent) {
        let event_type_id = (event as &dyn Any).type_id();
        let activity_type_id = self.active_activity_type_id();

        // Activity-level handler for this event type.
        let mut consumed = self.with_globals(|app| {
            let Some(activity) = app.activities.get_mut(&activity_type_id) else {
                return false;
            };
            let ret = activity.handle_event(app.active_activity.as_mut(), event, &mut app.handle);
            #[cfg(feature = "logging")]
            info!("activity event handler returned {ret:?}");
            ret.is_consumed()
        });

        // Always-active task handlers, in registration order. Destructured rather than run
        // through `with_globals`, which needs `&mut self` and so cannot be called while the
        // task vector is borrowed — so the check-out bracket is inlined here.
        if !consumed {
            let Self {
                tasks_registry,
                handle,
                ..
            } = self;
            for entry in tasks_registry.iter_mut() {
                entry.state.checkout_globals(&mut handle.store);
                let ret = entry
                    .handlers
                    .handle_event(entry.state.as_mut(), event, handle);
                entry.state.checkin_globals(&mut handle.store);
                if ret.is_consumed() {
                    consumed = true;
                    break;
                }
            }
        }

        // Application-level handler, unless the activity or a task swallowed the event.
        if !consumed && let Some(handler) = self.event_handlers.get_mut(&event_type_id) {
            // Nothing runs after this handler, so its return value has nothing left to
            // gate. It is kept for symmetry with activity handlers, and for the
            // background-task stage that will slot in ahead of it.
            let _ret = handler(event, &mut self.handle);
            #[cfg(feature = "logging")]
            info!("application event handler returned {_ret:?}");
        }

        // Post-event hooks see every event, consumed or not.
        self.with_globals(|app| {
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.post_event(app.active_activity.as_mut(), event, &mut app.handle);
            }
        });
        {
            let Self {
                tasks_registry,
                handle,
                ..
            } = self;
            for entry in tasks_registry.iter_mut() {
                entry.state.checkout_globals(&mut handle.store);
                entry
                    .handlers
                    .post_event(entry.state.as_mut(), event, handle);
                entry.state.checkin_globals(&mut handle.store);
            }
        }
        if let Some(handler) = self.post_event.as_mut() {
            handler(event, &mut self.handle);
        }
    }

    /// Resolves services and runs `on_start` for every registered task.
    ///
    /// Called before the first activity is created, so an activity's `on_create` can rely on
    /// whatever a task set up.
    fn start_tasks(&mut self) {
        let Self {
            tasks_registry,
            services,
            handle,
            ..
        } = self;
        for entry in tasks_registry.iter_mut() {
            entry.state.inject_services(services);
            entry.state.checkout_globals(&mut handle.store);
            entry.handlers.on_start(entry.state.as_mut(), handle);
            entry.state.checkin_globals(&mut handle.store);
        }
    }

    /// Runs `on_stop` for every registered task, after the activities are torn down.
    fn stop_tasks(&mut self) {
        let Self {
            tasks_registry,
            handle,
            ..
        } = self;
        for entry in tasks_registry.iter_mut() {
            entry.state.checkout_globals(&mut handle.store);
            entry.handlers.on_stop(entry.state.as_mut(), handle);
            entry.state.checkin_globals(&mut handle.store);
        }
    }

    /// Applies every queued [`Command`], including ones queued by the lifecycle callbacks
    /// that running earlier commands triggered.
    fn drain_commands(&mut self) {
        while let Some(command) = self.handle.commands.pop_front() {
            #[cfg(feature = "logging")]
            info!("applying {command:?}");
            match command {
                Command::Exit => {
                    self.handle.exiting = true;
                    self.handle.commands.clear();
                    // Same reasoning as the command queue: work asked for alongside an exit
                    // would be spawned only to be aborted moments later, so it never starts.
                    self.handle.discard_pending_spawns();
                    return;
                }
                Command::Push(new_activity) => {
                    self.pause_active();
                    let old_activity = std::mem::replace(&mut self.active_activity, new_activity);
                    match self.backstack.as_mut() {
                        Some(backstack) => backstack.push(old_activity),
                        // Without a backstack there is nothing to return to, so the
                        // outgoing activity is finished rather than stashed.
                        None => self.destroy(old_activity),
                    }
                    self.create_active();
                    self.resume_active();
                }
                Command::Pop => {
                    let previous = self.backstack.as_mut().and_then(Vec::pop);
                    match previous {
                        Some(previous) => {
                            self.pause_active();
                            let finished = std::mem::replace(&mut self.active_activity, previous);
                            self.destroy(finished);
                            // No create_active: the restored instance already exists, so it
                            // resumes rather than being created again.
                            self.resume_active();
                        }
                        None => {
                            #[cfg(feature = "logging")]
                            info!("pop ignored: backstack is disabled or empty");
                        }
                    }
                }
                Command::Replace(new_activity) => {
                    self.pause_active();
                    let finished = std::mem::replace(&mut self.active_activity, new_activity);
                    self.destroy(finished);
                    self.create_active();
                    self.resume_active();
                }
            }
        }
    }

    fn active_activity_type_id(&self) -> TypeId {
        (self.active_activity.as_ref() as &dyn Any).type_id()
    }

    /// Brackets one activity callback with the `#[global]` field check-out and check-in.
    ///
    /// Every callback gets its own window rather than one window around the whole of
    /// `dispatch`, so anything running between two activity callbacks — the
    /// application-level handler, most importantly — reads a coherent store.
    fn with_globals<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        self.active_activity
            .checkout_globals(&mut self.handle.store);
        let result = f(self);
        self.active_activity.checkin_globals(&mut self.handle.store);
        result
    }

    /// Resolves `#[inject]` fields and runs `on_create` for a newly created instance.
    ///
    /// `on_create` is bracketed like any other callback, which is what lets it seed a
    /// `#[global]` — writing `state.counter = 10` there reaches the store on check-in.
    fn create_active(&mut self) {
        self.active_activity.inject_services(&self.services);
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_create(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    fn resume_active(&mut self) {
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_resume(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    fn pause_active(&mut self) {
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_pause(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    fn destroy_active(&mut self) {
        self.with_globals(|app| {
            let activity_type_id = app.active_activity_type_id();
            if let Some(activity) = app.activities.get_mut(&activity_type_id) {
                activity.on_destroy(app.active_activity.as_mut(), &mut app.handle);
            }
        });
    }

    /// `on_destroy` for an instance that is no longer the active one — a replaced activity,
    /// or a backstack entry being unwound at shutdown.
    fn destroy(&mut self, mut state: Box<dyn ManagedState>) {
        let activity_type_id = (state.as_ref() as &dyn Any).type_id();
        state.checkout_globals(&mut self.handle.store);
        if let Some(activity) = self.activities.get_mut(&activity_type_id) {
            activity.on_destroy(state.as_mut(), &mut self.handle);
        }
        state.checkin_globals(&mut self.handle.store);
    }
}
