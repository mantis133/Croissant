# Croissant

**Android activity structure for Rust.**

Croissant is a framework for building event-driven applications — TUIs in particular — out of
**activities**: screens with lifecycles, driven by a single async event loop, sharing state
through dependency injection.

- **Activities** — a state struct plus `on_create` / `on_resume` / `on_pause` / `on_destroy`,
  with `push`/`pop`/`replace` navigation and an optional backstack.
- **Field injection** — `#[global] counter: u32` is shared application state you use as a
  plain field. No wrapper type, no deref, no lock.
- **Service injection** — `#[inject] repo: Injected<dyn UserRepo>` resolves by type, so a
  test can register a fake without the activity knowing.
- **Tasks** — always-active handlers for logic several activities share, instead of
  copy-pasting it into each of them.
- **Background work** — `app.spawn(..)` runs a future that outlives the activity that
  started it and reports back as an event. Name a job with `spawn_keyed` and any activity can
  cancel it by name, without a handle being passed around.

## Example

```rust
use std::time::Duration;

use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application},
    events::{ApplicationEvent, EventHandlerReturn},
    streams::timer_event_stream,
};

/// A screen. `#[global]` fields are shared with every other screen and task.
#[derive(Debug, Default, ManagedState)]
struct Counter {
    #[global]
    ticks: u32,
}

#[derive(Debug)]
struct Tick;
impl ApplicationEvent for Tick {}

#[tokio::main]
async fn main() {
    let counter = ActivityBuilder::<Counter>::new()
        .on_event(|state: &mut Counter, _event: &Tick, app: &mut AppHandle| {
            state.ticks += 1; // a plain field access, backed by shared state
            println!("tick {}", state.ticks);
            if state.ticks == 3 {
                app.exit();
            }
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(counter)
        .starting_activity::<Counter>()
        .add_event_producer(timer_event_stream(Duration::from_millis(200), |_instant| {
            Some(Box::new(Tick) as Box<dyn ApplicationEvent>)
        }))
        .build();

    app.run().await;
    println!("finished with {:?}", app.values().get::<u32>("ticks"));
}
```

```
tick 1
tick 2
tick 3
finished with Some(3)
```

## Installation

Croissant is not published to crates.io yet. Depend on it by git:

```toml
[dependencies]
croissant = { git = "https://github.com/mantis133/Croissant" }
tokio = { version = "1", features = ["full"] }
```

## Feature flags

| Feature | Default | Effect |
| --- | --- | --- |
| `derive` | ✅ | `#[derive(ManagedState)]` and its `#[global]` / `#[inject]` attributes |
| `crossterm` | | `crossterm_event_stream` for terminal input |
| `logging` | | `tracing` integration — currently incomplete, see the docs |
| `watch` | | `file_event_stream` / `directory_event_stream` for reacting to file changes |

## Documentation

Start with **[docs/introduction.md](docs/introduction.md)** for an overview of every feature,
then read the guide you need:

| Guide | Covers |
| --- | --- |
| [application.md](docs/application.md) | The builder, the event loop, when the app stops |
| [activities.md](docs/activities.md) | Screens, lifecycle, navigation, the backstack |
| [events.md](docs/events.md) | Defining events, the dispatch chain, event producers |
| [tasks.md](docs/tasks.md) | Always-active handlers, and async work |
| [dependency_injection.md](docs/dependency_injection.md) | `#[global]`, `#[inject]`, the value store, services |

## Status

Pre-1.0 and the API still moves. Known gaps, so you do not find them the hard way:

- **Logging is incomplete** — `log_file` ignores its level and directory arguments and drops
  the appender guard, so output is unreliable.
- **Argument and config-file parsing are deliberately out of scope** — use `clap` and
  `config`/`figment`, and hand the result over with `.service(..)`. The framework-shaped half
  of config, noticing a file change, is the `watch` feature.
- **A cancelled job cannot be cancelled from inside another job** — cancellation is a
  callback-side operation. A spawned future asks for one by emitting an event, the way it
  reports everything else.

## Minimum supported Rust version

**Rust 1.88**, verified against the toolchain. The crate uses let-chains in edition 2024, and
the `logging` feature pulls in dependencies with the same floor. The MSRV is not yet pinned
in CI and may rise before 1.0.

## Development

```sh
cargo test --all-features        # unit, integration and doc tests
cargo clippy --all-features --all-targets
cargo build --no-default-features   # check the derive feature is genuinely optional
```

## License

No license has been chosen yet, so the usual default applies: all rights reserved. If you
intend this to be usable by others, add a `LICENSE` file and a `license` field to
`Cargo.toml` — crates.io requires both to publish.
