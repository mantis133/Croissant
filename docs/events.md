# Events

Everything in Croissant is driven by events. They arrive from **producers**, travel a fixed
**dispatch chain**, and stop wherever something consumes them.

## Defining an event

Any type that is `Debug + Send + 'static`:

```rust
use croissant::events::ApplicationEvent;

#[derive(Debug)]
struct Tick;
impl ApplicationEvent for Tick {}

#[derive(Debug)]
struct KeyPress(char);
impl ApplicationEvent for KeyPress {}

#[derive(Debug)]
struct Downloaded { id: u32, body: String }
impl ApplicationEvent for Downloaded {}
```

The trait has no methods — it exists so events can be boxed as `dyn ApplicationEvent` and
recovered by type. Handlers are registered per concrete type and the framework downcasts on
the way in, so a handler always receives its own `&E` rather than a trait object.

Give *unrelated* events separate types rather than lumping them into one enum. Dispatch is
keyed on `TypeId`, so distinct types can be handled in different places, by different
activities; one enum forces a single handler to match on everything.

An enum is the right shape when the variants really are one kind of event — `KeyPress` with a
variant per key is one event type that handlers match on internally, and that is fine.

## The dispatch chain

Each event visits, in order:

```
1. active activity   on_event      → may consume
2. tasks             on_event      → registration order, may consume, stops at the first consumer
3. application       on_event      → only if nothing consumed it

4. active activity   post_event    ┐
5. tasks             post_event    ├─ always run, consumed or not
6. application       post_event    ┘
```

Stages 1–3 respect consumption. A handler returns:

- `EventHandlerReturn::Consumed` — stop; later stages of 1–3 never see it.
- `EventHandlerReturn::Ignored` — pass it on. Also the `Default`.

`EventHandlerReturn` also has `is_consumed()` and a `From<bool>` impl where `true` means
consumed.

Stages 4–6 always run and cannot consume. That is the difference in one line:

> **`on_event` is the chain of responsibility. `post_event` is the audit trail.**

Use `on_event` when someone should handle this and the others should not. Use `post_event`
for things that must see everything regardless.

**`post_event` is where drawing goes.** It was designed as the draw hook and then left
generic, because a Croissant application does not have to have a visual output at all — so
rather than mandating an `on_draw` that headless apps would leave empty, the after-every-event
hook covers it. Render there, or set a dirty flag there and render on the next pass. Metrics
and logging are the other natural fit.

The layering is why [tasks](tasks.md) work: an activity handles what it cares about, and a
task is the shared fallback for everything the activity ignored.

> **Note.** The application-level `on_event` handler is last in the consuming chain, so
> nothing acts on its return value. It exists for symmetry, and for the stage that will slot
> in ahead of it later.

## Emitting events

There are two paths, and they are not interchangeable.

|  | `AppHandle::emit` | `Emitter::emit` |
| --- | --- | --- |
| Called from | inside a callback | inside a spawned future |
| Needs | `&mut AppHandle` | a cloneable `Emitter` (`Send + Sync + 'static`) |
| Delivery | queue-jumps: before the next producer event | channel: the next time the loop polls |
| Returns | `()` | `bool` — `false` once the loop has stopped |

```rust
// From a callback:
app.emit(Refresh);

// From background work:
let _ = app.spawn_with(|emitter| async move {
    emitter.emit(Downloaded { id, body: fetch().await });
});
```

### Events the framework itself raises

`JobEnded` is the one type in the crate that implements `ApplicationEvent` itself; the
[stream helpers](#built-in-producers) stay generic over your own event type instead. It is
dispatched when a spawned job stops — see [tasks.md](tasks.md#jobended).

Because the two paths above are independent, a job's `JobEnded` can be dispatched *before* the
result the job sent through its `Emitter`. Read a result from the event carrying it, never
from `JobEnded`.

`AppHandle::emit` events are dispatched **before** the application asks its producers for
anything new, so a chain of emits resolves fully before the next external event arrives:

```
producer: Ping, Ping
handler:  on Ping → emit(Pong)

dispatched: Ping, Pong, Ping, Pong     (not Ping, Ping, Pong, Pong)
```

Both have `emit_boxed` variants for an already-boxed `Box<dyn ApplicationEvent>`.

## Event producers

A producer is any `Stream<Item = Box<dyn ApplicationEvent>> + Send + 'static`. All registered
producers are merged, and the loop treats them as one source.

```rust
Application::builder()
    .add_event_producer(keys)
    .add_event_producer(timer)
```

**Producers are how long-lived sources belong in the application.** A stream that never ends
keeps the app running; a spawned future that never ends does too, but blocks shutdown instead
of feeding it. If something produces events forever, make it a producer.

When every producer is exhausted and no background work is in flight, `run()` returns. See
[application.md](application.md#shutdown).

### Built-in producers

**Terminal input** (`crossterm` feature):

```rust
use croissant::crossterm::crossterm_event_stream;

.add_event_producer(crossterm_event_stream(|event| match event {
    Event::Key(key) => Some(Box::new(KeyPress(key)) as Box<dyn ApplicationEvent>),
    _ => None,   // returning None discards the event
}))
```

> Call this **once**. Each call creates an independent reader of the terminal queue, and
> several readers silently drop each other's events.

**A timer**:

```rust
use croissant::streams::timer_event_stream;
use std::time::Duration;

.add_event_producer(timer_event_stream(Duration::from_secs(30), |_instant| {
    Some(Box::new(Tick) as Box<dyn ApplicationEvent>)
}))
```

The closure receives each tick's `Instant` and returns `Some(event)` to emit or `None` to
skip.

**File changes** (`watch` feature):

```rust
use croissant::streams::{FileChangeKind, file_event_stream};

.add_event_producer(file_event_stream("config.toml", Duration::from_millis(200), |change| {
    match change.kind {
        FileChangeKind::Created | FileChangeKind::Modified => {
            Some(Box::new(ConfigChanged) as Box<dyn ApplicationEvent>)
        }
        _ => None,
    }
})?)
```

There is also `directory_event_stream(path, recursive, debounce, f)` for a whole folder.

This is the framework-shaped half of configuration: *loading* config is `config`/`figment`'s
job, but noticing a change is an event producer, and reacting to one is an event. The helper
absorbs three things that are easy to get wrong:

- **The watcher must stay alive.** Dropping it silently stops delivery, so the stream owns it
  and they live and die together.
- **Watching a file directly breaks on the first save.** Most editors write a temporary file
  and rename it over the target, orphaning a watch on the original inode. `file_event_stream`
  watches the *parent directory* and filters by filename, which also means the file does not
  have to exist yet.
- **One save fires several events.** Write, chmod, rename. The `debounce` argument collapses
  a burst into one change; ~200ms is usually right.

Both return `Result`, because a path that cannot be watched should fail loudly rather than
produce a stream that is silently never going to emit anything.

### Writing your own

Anything from the `futures` stream toolkit works. A fixed list, useful in tests, ends on its
own and so stops the loop:

```rust
use futures::stream;

.add_event_producer(stream::iter(vec![
    Box::new(Ping) as Box<dyn ApplicationEvent>,
    Box::new(Ping),
]))
```

For anything custom, `futures::stream::unfold` is usually enough — it is what the built-in
timer is written with, and it needs no extra dependency. For a channel-backed producer,
reach for an [`Emitter`](tasks.md#emitter) first: it is already a channel into the loop, so
you rarely need to build one. (Wrapping a `tokio::sync::mpsc` receiver directly would mean
adding the separate `tokio-stream` crate.)

The `croissant::EventStream<T>` alias (`Pin<Box<dyn Stream<Item = T> + Send>>`) is the boxed
form the application stores internally.

## See also

- [application.md](application.md) — the loop that drives all this
- [activities.md](activities.md) — the first stage of the chain
- [tasks.md](tasks.md) — the second stage, and where `Emitter` comes from
