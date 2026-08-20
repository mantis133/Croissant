use std::any::Any;
use std::collections::VecDeque;

use crate::{
    activities::ActivityState,
    application::{command::Command, value_store::ValueStore},
    events::ApplicationEvent,
};

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
/// # impl croissant::activities::ActivityState for Screen {}
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
#[derive(Debug, Default)]
pub struct AppHandle {
    pub(super) store: ValueStore,
    pub(super) commands: VecDeque<Command>,
    pub(super) pending_events: VecDeque<Box<dyn ApplicationEvent>>,
    pub(super) exiting: bool,
}

impl AppHandle {
    pub(super) fn new(store: ValueStore) -> Self {
        AppHandle {
            store,
            commands: VecDeque::new(),
            pending_events: VecDeque::new(),
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
    pub fn emit<E: ApplicationEvent>(&mut self, event: E) {
        self.emit_boxed(Box::new(event));
    }

    /// [`AppHandle::emit`] for an already-boxed event.
    pub fn emit_boxed(&mut self, event: Box<dyn ApplicationEvent>) {
        self.pending_events.push_back(event);
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
    pub fn push<A: ActivityState + Default>(&mut self) {
        self.push_with(A::default());
    }

    /// [`AppHandle::push`] with an instance you construct yourself, for states that are not
    /// `Default` or that need a value passed straight in. `on_create` still runs.
    pub fn push_with<A: ActivityState>(&mut self, activity: A) {
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
    pub fn replace<A: ActivityState + Default>(&mut self) {
        self.replace_with(A::default());
    }

    /// [`AppHandle::replace`] with an instance you construct yourself.
    pub fn replace_with<A: ActivityState>(&mut self, activity: A) {
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
