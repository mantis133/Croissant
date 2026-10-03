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

### What the loop does with them

They are merged with `futures::stream::select_all` and polled without bias, so a chatty
producer cannot starve a quiet one. A producer that yields `None` is **finished**: it is
dropped from the set and never polled again. When the set empties *and* no background work is
in flight, `run()` returns — so an application with no producers registered at all runs its
start-up, dispatches whatever start-up emitted, and stops. See
[application.md](application.md#shutdown).

Producers are polled from `run()`'s own future, on `run()`'s own thread. That is the one thing
to hold on to while writing one: a producer that *awaits* lets the rest of the application get
on with its life, and a producer that *blocks* stops all of it. See
[never block the loop](#never-block-the-loop).

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

## Writing your own producer

There is no trait to implement and nothing Croissant-specific to learn. A producer is an
ordinary `futures` stream; the work is entirely in getting your source into that shape.

### The contract

```rust
Stream<Item = Box<dyn ApplicationEvent>> + Send + 'static
```

| Part | What it means when you write one |
| --- | --- |
| `Stream` | the loop polls it; each poll yields an item, parks until it can, or returns `None` — *finished for good* |
| `Item = Box<dyn ApplicationEvent>` | every producer yields that one erased type, which is what lets unrelated producers merge into a single source |
| `Send` | the stream, and every future it awaits inside itself, must be safe to move between threads |
| `'static` | it is stored in the application and outlives the scope that built it, so it can borrow nothing from the stack — own everything it needs |

Pick the recipe by the shape of what you have:

| Your source | Reach for |
| --- | --- |
| already a `Stream` | [`.map` / `.filter_map`](#start-from-a-stream-you-already-have) |
| a fixed list (mostly tests) | [`stream::iter`](#a-fixed-list) |
| a loop with an `.await` in it | [`stream::unfold`](#unfold-the-general-purpose-one) |
| a channel receiver | [`unfold` over the receiver](#sources-that-push-instead-of-being-polled) |
| a callback API, or another thread | a channel, then as above |
| something that blocks | [`spawn_blocking` inside `unfold`](#never-block-the-loop) |

### Everything yields the same type

The erasure is the first thing that bites, and it is worth getting out of the way before the
async part:

```rust
// ✗ this is a Stream<Item = Box<Ping>>
stream::iter(vec![Box::new(Ping), Box::new(Ping)])

// ✓ this is a Stream<Item = Box<dyn ApplicationEvent>>
stream::iter(vec![
    Box::new(Ping) as Box<dyn ApplicationEvent>,
    Box::new(Ping),
])
```

Only the first element needs the cast — it fixes the vec's element type and the rest coerce
into it. The same reasoning is why the cast appears on every `Box::new` inside the built-in
producers' closures: a closure body returning `Some(Box::new(Tick))` infers
`Option<Box<Tick>>`, with nothing in sight to coerce it towards.

### A fixed list

Useful in tests. It ends on its own, which stops the loop:

```rust
use futures::stream;

.add_event_producer(stream::iter(vec![
    Box::new(Ping) as Box<dyn ApplicationEvent>,
    Box::new(Ping),
]))
```

### Start from a stream you already have

If the source is already a `Stream`, the producer is one `map`:

```rust
use futures::StreamExt;

let producer = numbers.map(|n| Box::new(Tick(n)) as Box<dyn ApplicationEvent>);
```

`filter_map` drops the ones you do not want, and it is what all three built-ins use — but its
closure returns a **future**, not an `Option`, and that shape is worth seeing once:

```rust
let producer = numbers.filter_map(|n| {
    let event = (n % 2 == 0).then(|| Box::new(Tick(n)) as Box<dyn ApplicationEvent>);
    async move { event }        // decide first; move only the answer into the block
});
```

Doing the work *inside* `async move { .. }` is the tempting version, and it stops compiling as
soon as the closure has a capture the block needs — the block would have to move that capture
into the future, and the closure is `FnMut`, so it is going to be called again. Compute first,
move only the result in. That is why the built-ins all look like this rather than the shorter
thing you would write by hand.

### `unfold`: the general-purpose one

For a source that is not already a stream, `futures::stream::unfold` is the tool. It turns
"state in → one item out → state back" into a stream, and one call of the closure is one item:

```rust
stream::unfold(initial_state, |state| async move {
    // await whatever produces the next item
    Some((item, next_state))    // ...or None to end the stream for good
})
```

The whole of it, worked:

```rust
use croissant::events::ApplicationEvent;
use futures::stream;
use std::time::Duration;

#[derive(Debug)]
struct Countdown(u32);
impl ApplicationEvent for Countdown {}

let counting_down = stream::unfold(5u32, |remaining| async move {
    if remaining == 0 {
        return None;                    // finished — the loop drops this producer
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    let event = Box::new(Countdown(remaining)) as Box<dyn ApplicationEvent>;
    Some((event, remaining - 1))        // (emit this, resume with that)
});
```

| The bit | Why it is there |
| --- | --- |
| `5u32` | the state handed to the first call |
| `\|remaining\|` | the state arrives **by value**, so the closure can move it into the async block |
| the `.await` | the point of the whole exercise: the producer parks here and the loop gets on with dispatching |
| `Some((event, remaining - 1))` | what to emit now, and the state the next call receives |
| `None` | this producer is done. It is not polled again |

The built-in timer and the file watcher are both written this way, so
[`streams/timer.rs`](../src/streams/timer.rs) and [`streams/watch.rs`](../src/streams/watch.rs)
are two more worked examples.

If threading state through a closure feels inside-out for what is really just a loop, the
`async-stream` crate gives you `stream! { loop { .. yield event; } }` instead. It is a
perfectly good choice — Croissant simply does not depend on it, which is why the crate's own
producers are written the long way.

### The state is for anything that must survive between polls

This is the part that is easy to get wrong, and it explains a shape that otherwise looks like
noise.

The closure is `FnMut` — the loop calls it once per item. So whatever it **captures**, it can
only borrow or clone, never consume; moving a captured `String` into the async block is
`cannot move out of ..., a captured variable in an FnMut closure`. Whatever it receives as
**state**, though, it owns outright, because the previous call handed it back.

So put it in the state. That applies to resources as much as to values: a watcher, a
connection, a subscription handle — anything whose drop silently stops delivery — belongs in
the state tuple next to the receiver.

```rust
stream::unfold((receiver, watcher), |(mut receiver, watcher)| async move {
    let change = receiver.recv().await?;
    let event = Box::new(Changed(change)) as Box<dyn ApplicationEvent>;
    Some((event, (receiver, watcher)))
});
```

`watcher` is never read. It rides along so that it lives exactly as long as the stream does
and dies with it — which is precisely what `file_event_stream` does with its debouncer.

The alternative, for something shared rather than owned, is an `Arc` and a clone per call:

```rust
let f = Arc::new(f);
stream::unfold(ticker, move |mut ticker| {
    let f = Arc::clone(&f);     // one clone per call, so the block below can move it
    async move { .. }
})
```

That is the timer's shape, and only because `f` is a caller-supplied generic that cannot live
in the state. When you own the thing, prefer the state tuple.

### Sources that push instead of being polled

Callback APIs, C libraries, another thread — anything that calls you rather than waiting to be
asked. Bridge it with a channel: the sender goes wherever the callbacks fire, and `unfold`
turns the receiver into the stream.

```rust
let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<String>();

let producer = stream::unfold(receiver, |mut receiver| async move {
    let line = receiver.recv().await?;   // None once every sender is gone
    Some((Box::new(Line(line)) as Box<dyn ApplicationEvent>, receiver))
});
```

The `?` is doing real work there. `recv` returns `None` when the last sender drops, and `?` in
a block returning `Option` turns that into the stream's own `None` — so the producer's life is
exactly the senders' life. Hand a clone to each source and the application stays alive for as
long as anything can still speak.

The `unfold` wrapper is needed because a channel receiver is not itself a `Stream`; wrapping
one directly is what the separate `tokio-stream` crate exists for, and this is the version
that costs no dependency.

> **Do you need a producer at all?** For work that starts *during* the run, reach for an
> [`Emitter`](tasks.md#emitter) instead — it is already an unbounded channel into the loop and
> needs nothing registered at build time. A producer is for a source that exists **before**
> `run()` and lives as long as the application. The difference that matters at shutdown: an
> unexhausted producer keeps the loop alive, whereas a live `Emitter` by itself does not — the
> loop can stop underneath it, after which `emit` returns `false`.

### Never block the loop

Producers are polled from `run()`'s own future. A producer that **blocks** that thread — a
synchronous read, `std::thread::sleep`, a lock held across real work — stops the entire
application: no dispatch, no drawing, no other producer, no background results collected.

Hand blocking work to `tokio::task::spawn_blocking`, and pass the resource in and back out so
the next poll still owns it:

```rust
use std::io::{BufRead, BufReader};

let lines = stream::unfold(BufReader::new(std::io::stdin()), |mut stdin| async move {
    let (stdin, line) = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        let read = stdin.read_line(&mut line).unwrap_or(0);
        (stdin, (read > 0).then_some(line))
    })
    .await
    .ok()?;                 // the blocking task panicked — end the stream

    // `line?` is None at end of input, which ends the stream the same way.
    Some((Box::new(Line(line?)) as Box<dyn ApplicationEvent>, stdin))
});
```

Returning the reader out of the blocking closure is the trick worth remembering: it was moved
in, so it has to come back to be threaded on as the next state.

### When it will not compile

Five errors cover almost everything.

| Error | What it actually means | Fix |
| --- | --- | --- |
| `expected Box<dyn ApplicationEvent>, found Box<Ping>` | the erasure has not happened | add `as Box<dyn ApplicationEvent>` — to the first vec element, or to the value the closure returns |
| ``the trait bound `UnboundedReceiver<_>: Stream` is not satisfied`` | channel receivers are not streams | wrap it in [`unfold`](#sources-that-push-instead-of-being-polled) |
| `future cannot be sent between threads safely` / `not Send as this value is used across an await` | something non-`Send` (an `Rc`, a `RefCell` borrow, a `MutexGuard`) is still alive across an `.await` inside the producer | end its life before the await, or use the `Send` equivalent |
| `cannot move out of X, a captured variable in an FnMut closure` | the closure runs once per item and you consumed a capture | [move it into the state](#the-state-is-for-anything-that-must-survive-between-polls), or `Arc` it and clone per call |
| `borrowed value does not live long enough`, at the **call site** of a function returning a producer | under edition 2024 `impl Stream` captures every lifetime in scope, so a `&path` argument ties the producer to that borrow | return `EventStream<Box<dyn ApplicationEvent>>` and `Box::pin` it |

That last fix is what `file_event_stream` does, and it is free: `add_event_producer` boxes the
stream anyway. `croissant::EventStream<T>` is the alias for that boxed form —
`Pin<Box<dyn Stream<Item = T> + Send>>` — and the type the application stores internally.

## See also

- [application.md](application.md) — the loop that drives all this
- [activities.md](activities.md) — the first stage of the chain
- [tasks.md](tasks.md) — the second stage, and where `Emitter` comes from
