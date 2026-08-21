# Croissant

Croissant brings Android's activity structure to Rust: an application is a set of **screens**
with lifecycles, driven by a single async event loop, sharing state through dependency
injection.

```rust
use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application},
    events::{ApplicationEvent, EventHandlerReturn},
};

#[derive(Debug, Default, ManagedState)]
struct Counter {
    #[global]
    count: u32,
}

#[derive(Debug)]
struct Tick;
impl ApplicationEvent for Tick {}

#[tokio::main]
async fn main() {
    let counter = ActivityBuilder::<Counter>::new()
        .on_event(|state: &mut Counter, _event: &Tick, app: &mut AppHandle| {
            state.count += 1;
            if state.count >= 10 {
                app.exit();
            }
            EventHandlerReturn::Consumed
        })
        .build();

    Application::builder()
        .add_activity(counter)
        .starting_activity::<Counter>()
        .add_event_producer(/* a stream of events */)
        .build()
        .run()
        .await;
}
```

## The guides

| Guide | What it covers |
| --- | --- |
| [application.md](application.md) | `Application`, the builder, the event loop, when the app stops |
| [activities.md](activities.md) | Screens, their lifecycle, navigation and the backstack |
| [events.md](events.md) | Defining events, the dispatch chain, event producers |
| [tasks.md](tasks.md) | Always-active handlers, and async work that outlives an activity |
| [dependency_injection.md](dependency_injection.md) | `#[global]` and `#[inject]` fields, the value store, services |

## A quick tour

### Activities

An activity is a state struct plus lifecycle callbacks. Every callback receives the
activity's own state and an `AppHandle` — its view of everything shared.

```rust
ActivityBuilder::<Menu>::new()
    .on_create(|state, app| { /* once per instance, on a Default value */ })
    .on_resume(|state, app| { /* entering the foreground */ })
    .on_pause(|state, app| { /* leaving the foreground */ })
    .on_destroy(|state, app| { /* the instance is finished */ })
    .on_event(|state: &mut Menu, event: &KeyPress, app: &mut AppHandle| {
        app.push::<Details>();
        EventHandlerReturn::Consumed
    })
    .build()
```

Navigation goes through the handle — `push`, `pop`, `replace`, `exit` — and an optional
backstack remembers what you pushed away from. See [activities.md](activities.md).

### Events

Any `Debug + Send + 'static` type can be an event. Events arrive from *producers* (async
streams) and travel a fixed chain, stopping at whoever consumes them:

```
active activity  →  tasks  →  application handler
```

Croissant ships producers for terminal input, timers, and file changes; anything else is a
stream you write. See [events.md](events.md).

### Dependency injection

Shared state is declared as a **field** and used as a plain field.
`#[derive(ManagedState)]` wires it up:

```rust
#[derive(Debug, Default, ManagedState)]
struct Dashboard {
    #[global] counter: u32,                  // shared, keyed by field name
    #[inject] repo: Injected<dyn UserRepo>,  // shared service, keyed by type
    cursor: usize,                           // ordinary local state
}

// in any callback:
state.counter += 1;
let users = state.repo.find_all();
```

`#[global]` and `#[inject]` are inert helper attributes — the compiler ignores them, so
`counter` stays a genuine `u32` with no wrapper and no indirection. See
[dependency_injection.md](dependency_injection.md).

### Tasks and background work

Two separate mechanisms, because there are two separate problems:

- A **`Task`** is an always-active activity without a screen — stateful, synchronous, and
  handling events whichever activity is in front. Use it for logic several activities share,
  which would otherwise be copy-pasted into each of them.
- **`app.spawn(..)`** runs a future in the background. It is not tied to the activity that
  started it, and reports results back as events through an `Emitter`.

```rust
let job = app.spawn_with(|emitter| async move {
    let body = fetch(&url).await;
    emitter.emit(Downloaded { id, body });
})?;
```

Give a job a name with `spawn_keyed` and anything holding an `AppHandle` can stop it —
`app.cancel_key("download")` — without the two ever having to share a handle.

See [tasks.md](tasks.md).

## Features

| Feature | Default | Effect |
| --- | --- | --- |
| `derive` | yes | `#[derive(ManagedState)]` and its `#[global]`/`#[inject]` attributes |
| `crossterm` | no | `crossterm_event_stream` for terminal input |
| `logging` | no | `tracing` integration — see the caveat in [application.md](application.md) |
| `watch` | no | File-change event producers — see [events.md](events.md#built-in-producers) |

## Status

Croissant is pre-1.0 and the API still moves. Known gaps, so you do not discover them the
hard way:

- **Logging is incomplete.** `ApplicationBuilder::log_file` ignores its level and directory
  arguments and drops the appender guard, so output is unreliable. Details in
  [application.md](application.md).
- **Argument and config-file parsing are deliberately out of scope** — use `clap` and
  `config`/`figment`, and hand the result over with `.service(..)`. The framework-shaped half
  of config, noticing a file change, is the `watch` feature.
- **Jobs cannot be cancelled from inside another job.** Cancellation is a callback-side
  operation; a spawned future asks for one by emitting an event. See [tasks.md](tasks.md).
- `croissant::activities::ActivityState` is a legacy alias for `ManagedState`; prefer the
  new name, which is what the derive emits.
