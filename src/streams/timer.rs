use futures::{Stream, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{self, Instant};

/// Returns a stream that emits an event on every `interval`, mapped into the caller's event type.
///
/// The mapping function receives the `Instant` of each tick and returns `Some(AppEvent)` to emit
/// it or `None` to skip it.
///
/// ```no_run
/// # use std::time::Duration;
/// # use croissant::{activities::ActivityBuilder, ManagedState, application::Application, events::ApplicationEvent};
/// # #[derive(Debug, Default)] struct Home;
/// # impl ManagedState for Home {}
/// #[derive(Debug)]
/// struct TimerTick;
/// impl ApplicationEvent for TimerTick {}
///
/// let app = Application::builder()
///     .add_activity(ActivityBuilder::<Home>::new().build())
///     .starting_activity::<Home>()
///     .add_event_producer(croissant::streams::timer_event_stream(
///         Duration::from_secs(30),
///         |_instant| Some(Box::new(TimerTick) as Box<dyn ApplicationEvent>),
///     ))
///     .build();
/// ```
pub fn timer_event_stream<AppEvent, F>(interval: Duration, f: F) -> impl Stream<Item = AppEvent>
where
    F: Fn(Instant) -> Option<AppEvent> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    futures::stream::unfold(time::interval(interval), move |mut ticker| {
        let f = Arc::clone(&f);
        async move {
            let tick = ticker.tick().await;
            Some((f(tick), ticker))
        }
    })
    .filter_map(|item| async move { item })
}
