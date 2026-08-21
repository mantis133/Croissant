# Application

The `Application` owns everything: the activity registry, the backstack, the registered
tasks, the event producers, the shared value store, and the service registry. It is built
once with `Application::builder()` and then driven by `run().await`.

```rust
use croissant::{activities::ActivityBuilder, application::Application};

let mut app = Application::builder()
    .add_activity(home_activity)
    .starting_activity::<Home>()
    .backstack()
    .add_event_producer(key_events)
    .build();

app.run().await;
```

## Building

`ApplicationBuilder` is a consuming fluent builder. Every method returns `Self`.

### Activities and tasks

```rust
.add_activity(activity)          // Activity<A>, registered under A's type
.starting_activity::<Home>()     // Default-constructed; on_create initialises it
.starting_activity_with(home)    // ...or hand over an instance you built
.backstack()                     // remember pushed-aside activities so pop() can return
.add_task(task)                  // Task<S>, always active — see tasks.md
```

Exactly one activity may be registered per state type: `add_activity` keys on
`TypeId::of::<A>()`, so registering twice for the same state type replaces the first.

`build()` **panics** if no starting activity was set.

### Events

```rust
.add_event_producer(stream)      // any Stream<Item = Box<dyn ApplicationEvent>> + Send
.on_event(|event: &Tick, app: &mut AppHandle| { .. })   // application-wide handler
.post_event(|event, app| { .. })                        // runs after every event
```

Producers are merged into one stream. See [events.md](events.md) for the dispatch chain and
for the built-in producers.

### Shared state and services

```rust
.value("counter", 0u32)                                  // seed a named value
.service::<dyn UserRepo>(Arc::new(PostgresRepo::new()))  // register a service by type
.service_named::<dyn UserRepo>("cache", Arc::new(Redis)) // ...under a qualifier
```

See [dependency_injection.md](dependency_injection.md).

## The event loop

`run()` is one `async fn`. It never spawns a thread for the loop itself; background work
goes through [`spawn`](tasks.md#spawn-fire-and-forget-async), which uses `tokio` tasks.

### Start-up

1. Every registered task gets its `#[inject]` fields resolved, then its `on_start` runs, in
   registration order.
2. The starting activity gets its `#[inject]` fields resolved, then `on_create`.
3. The starting activity gets `on_resume`.

Tasks start **before** the first activity is created, so an activity's `on_create` can rely
on anything a task set up.

### Each turn

The loop takes one event from the first of these that has something:

1. **Events emitted from inside a callback** (`AppHandle::emit`), plus the `JobEnded` the
   loop raises when a job stops. These jump the queue and are dispatched before anything
   external is polled — which is also what guarantees a job's ending is never lost, even when
   it is the last thing the application does.
2. **External producers** and **background work** (`Emitter::emit`), selected fairly so a
   chatty producer cannot starve task results, or the other way round.

The event is then [dispatched](events.md#the-dispatch-chain), and any navigation or spawn
requests the callbacks queued are applied.

### Shutdown

`run()` returns when either:

- an activity, task, or handler called `AppHandle::exit()`; or
- **every external producer is exhausted _and_ no background work is still in flight.**

That second rule is what makes `spawn` reliable: a result still lands even if the producers
dried up while the work was running. It has one consequence worth knowing —

> A deliberately endless spawned future keeps the application alive. Long-lived listening
> belongs in an **event producer**, not a spawned future. Otherwise, end the application
> with `exit()`, which aborts whatever is still running.

On the way out, in order:

1. `on_pause` then `on_destroy` for the active activity.
2. `on_destroy` for every backstack entry, unwound from the top — so activities are
   destroyed in the reverse of their creation order.
3. `on_stop` for every task.
4. Any still-running spawned futures are aborted.

Those teardown callbacks run with the loop already stopped, so `spawn` refuses them —
`Err(SpawnError::Exiting)` rather than work that quietly never runs. For the same reason,
jobs killed by step 4 do **not** emit a [`JobEnded`](tasks.md#jobended): there would be nobody
left to receive it.

## Reading state after the run

`run()` takes `&mut self`, so the `Application` is still yours afterwards. This is the usual
way to assert on results in a test:

```rust
app.run().await;
assert_eq!(app.values().get::<u32>("counter"), Some(&12));
```

| Method | Use |
| --- | --- |
| `app.handle()` | the same `AppHandle` callbacks get — seed values before `run()`, read them after |
| `app.values()` / `app.values_mut()` | the named-value store |
| `app.services()` | the registered services |

## Why callbacks get an `AppHandle`, not `&mut Application`

While a callback is running, that activity is borrowed out of the application's registry, so
a second `&mut Application` cannot exist — the borrow checker rejects it outright. `AppHandle`
is a *separate field* of `Application` holding exactly the pieces a callback is allowed to
touch, so the two are borrowed disjointly.

The visible consequence: **navigation, emitted events, spawns and `exit()` are queued, not
immediate.** They take effect once the callback returns, in the order requested.

```rust
app.push::<Details>();
// still on the current activity right here — Details takes over after this callback returns
```

## Logging

> **This feature is incomplete.** Enable it if you want, but do not rely on it yet.

With the `logging` feature on, `ApplicationBuilder::log_file` is available:

```rust
.log_file(Level::INFO, "./logs", "app.log")
```

Three things are wrong with it today, all tracked under "fix logging" in `TODO.md`:

- the `log_level` argument is ignored — the subscriber is hard-coded to `INFO`;
- the `directory_path` argument is ignored — output always goes to `./logs`;
- the `tracing_appender` worker guard is dropped at the end of `build()`, which shuts the
  background writer down, so lines can be lost.

The framework's own `info!` calls (dispatch results, applied navigation commands, background
tasks ending abnormally) are all behind this same feature.

## See also

- [activities.md](activities.md) — what the lifecycle callbacks mean
- [events.md](events.md) — the dispatch chain and event producers
- [tasks.md](tasks.md) — always-active handlers and background work
