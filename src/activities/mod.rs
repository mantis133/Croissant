mod activity;
mod activity_builder;
mod activity_state;
mod any_activity;

pub use activity::Activity;
pub use activity_builder::ActivityBuilder;
pub use activity_state::ActivityState;
pub(crate) use any_activity::AnyActivity;

/// Derives [`ActivityState`], wiring `#[global]` and `#[inject]` fields to the application.
///
/// The trait and the derive share a name, the way `serde::Serialize` does — types and
/// macros live in separate namespaces, so one import brings both.
///
/// ```
/// # use std::sync::Arc;
/// # use croissant::{activities::ActivityState, application::Injected};
/// # trait UserRepo: Send + Sync { fn count(&self) -> usize; }
/// #[derive(Debug, Default, ActivityState)]
/// struct Dashboard {
///     #[global]
///     counter: u32,                     // key "counter", read-write
///     #[global("app.host")]
///     host: String,                     // explicit key
///     #[global(readonly)]
///     app_name: String,                 // writes discarded
///     #[inject]
///     repo: Injected<dyn UserRepo>,     // resolved by type
///     #[inject("cache")]
///     fast: Injected<dyn UserRepo>,     // ...and by qualifier
///     cursor: usize,                    // plain local field
/// }
/// ```
///
/// `#[global]` and `#[inject]` are *inert helper attributes*: the compiler ignores them, so
/// `counter` stays a genuine `u32` and `self.counter += 1` is a plain field access with no
/// indirection. The generated impl swaps `#[global]` values in and out of the
/// [`ValueStore`](crate::application::ValueStore) around each callback, and resolves
/// `#[inject]` fields from the [`ServiceRegistry`](crate::application::ServiceRegistry) once
/// when the instance is created.
///
/// The two attributes key differently, and the difference matters:
///
/// - `#[global]` is keyed by **field name**. That is what lets `username: String` and
///   `hostname: String` be two separate values — they are the same type, so no type-based
///   lookup could tell them apart.
/// - `#[inject]` is keyed by **field type**, optionally narrowed by a qualifier. That is
///   what lets an activity depend on `dyn UserRepo` and have a fake supplied in a test.
///
/// Using `#[global]` without this derive is a compile error — a helper attribute only
/// exists while its derive is applied — so globals cannot silently fail to sync.
#[cfg(feature = "derive")]
pub use croissant_macros::ActivityState;
