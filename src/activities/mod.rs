mod activity;
mod activity_builder;
mod any_activity;

pub use activity::Activity;
pub use activity_builder::ActivityBuilder;
pub(crate) use any_activity::AnyActivity;

/// Legacy name for [`ManagedState`](crate::ManagedState), kept so existing bounds and
/// `impl` blocks keep compiling. Prefer `croissant::ManagedState`: the trait covers task
/// state too, so the old name is a misnomer.
///
/// Note that `#[deprecated]` has no effect on a re-export, so this produces no warning. The
/// derive is only available under its new name.
pub use crate::ManagedState as ActivityState;
