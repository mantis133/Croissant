use std::{any::Any, fmt::Debug};

use crate::application::{ServiceRegistry, ValueStore};

/// A struct whose fields the framework injects into and keeps in sync.
///
/// Both [activity](crate::activities::Activity) state and [task](crate::tasks::Task) state
/// implement this — an activity is a screen, a task is always-on background handling, but
/// the framework manages their fields identically.
///
/// Every method is defaulted to a no-op, so a plain state type needs nothing more than:
///
/// ```
/// # use croissant::ManagedState;
/// #[derive(Debug, Default)]
/// struct Home;
/// impl ManagedState for Home {}
/// ```
///
/// The hooks exist for `#[derive(ManagedState)]`, which implements them to move `#[global]`
/// fields in and out of the application's [`ValueStore`] around each callback and to resolve
/// `#[inject]` fields from the [`ServiceRegistry`]. Hand-written implementations leave them
/// alone.
pub trait ManagedState: Any + Send + 'static + Debug {
    /// Resolves `#[inject]` fields. Called once, when the instance is created — before
    /// `on_create` for an activity, before `on_start` for a task.
    fn inject_services(&mut self, _services: &ServiceRegistry) {}

    /// Moves `#[global]` field values out of the store and into `self`, before a callback.
    fn checkout_globals(&mut self, _store: &mut ValueStore) {}

    /// Writes `#[global]` field values back to the store, after a callback.
    fn checkin_globals(&mut self, _store: &mut ValueStore) {}
}
