use ::crossterm::event::{Event, EventStream};
use futures::{Stream, StreamExt};

/// Returns a single stream of crossterm events mapped into the caller's event type.
///
/// The mapping function receives each raw `crossterm::event::Event` and returns `Some(AppEvent)`
/// to emit it or `None` to discard it. Use a single call to this function to handle all
/// crossterm events — calling it multiple times creates multiple independent readers of the
/// terminal event queue, causing events to be silently dropped.
pub fn crossterm_event_stream<AppEvent, F>(f: F) -> impl Stream<Item = AppEvent>
where
    F: Fn(Event) -> Option<AppEvent> + Send + 'static,
{
    EventStream::new().filter_map(move |event| {
        let result = event.ok().and_then(|e| f(e));
        async move { result }
    })
}