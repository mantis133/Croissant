# Tasks and background work

Two separate mechanisms, because there are two separate problems.

| Problem | Mechanism |
| --- | --- |
| Logic several activities share, which would otherwise be copy-pasted into each | a **`Task`** |
| Work triggered now that finishes later, possibly after the user navigated away | **`app.spawn(..)`** |

They compose: a task spawns jobs and correlates their results. That combination is why the
framework does not need a registry of live task instances — the concurrency lives in the
spawned futures, and the coordinating state lives in one place.

---

## Tasks: always active, stateful, synchronous

A `Task` is **an activity without a screen**. It is registered once, `Default`-constructed at
start-up, and handles events for as long as the application runs, whichever activity is in
front.

```rust
use croissant::{ManagedState, application::AppHandle, events::EventHandlerReturn, tasks::TaskBuilder};

#[derive(Debug, Default, ManagedState)]
struct Shortcuts {
    #[global]
    quits_seen: u32,
}

let shortcuts = TaskBuilder::<Shortcuts>::new()
    .on_event(|task: &mut Shortcuts, _event: &Quit, app: &mut AppHandle| {
        task.quits_seen += 1;
        app.exit();
        EventHandlerReturn::Consumed
    })
    .build();

Application::builder()
    .add_task(shortcuts)
    // ...
```

Its state is an ordinary [`ManagedState`](dependency_injection.md), so `#[global]` and
`#[inject]` fields work exactly as they do on an activity.

### Where a task sits

```
active activity  →  tasks (registration order)  →  application handler
```

So an activity handles what it cares about and tasks are the shared fallback. Consuming an
event in a task stops it before the application handler, and before any task registered
later.

`post_event` runs for every event the task sees, consumed or not — including events the task
consumed itself.

Registration order **is** dispatch order: tasks are held in a list, unlike activities, which
are keyed by type because only one is active at a time.

### Lifecycle

| Callback | When |
| --- | --- |
| `on_start` | once at start-up, on a `Default` value with `#[inject]` fields already resolved |
| `on_stop` | once at shutdown, after every activity has been destroyed |

Every task starts **before the first activity is created**, so an activity's `on_create` can
rely on whatever a task set up:

```rust
TaskBuilder::<Config>::new()
    .on_start(|task: &mut Config, app: &mut AppHandle| {
        task.base_url = String::from("https://example.invalid");
    })
    .build()
```

Because `on_start` is bracketed like every other callback, assigning a `#[global]` there
writes through to the shared store.

There is no dynamic start or stop. A task is either registered or it is not — for work that
comes and goes, use `spawn`.

### Builder reference

```rust
TaskBuilder::<S>::new()
    .on_start(|state, app| { .. })
    .on_stop(|state, app| { .. })
    .on_event(|state: &mut S, event: &E, app: &mut AppHandle| -> EventHandlerReturn { .. })
    .post_event(|state, event: &dyn ApplicationEvent, app| { .. })
    .build()
```

`add_task` requires `S: ManagedState + Default`.

---

## spawn: fire-and-forget async

Tasks are synchronous; they cannot `await`. For that, call `spawn`:

```rust
let job = app.spawn_with(|emitter| async move {
    let body = fetch(&url).await;
    emitter.emit(Downloaded { id, body });
})?;
```

The work is **not tied to the activity that started it**. Navigating away, or destroying
that activity outright, leaves it running — the result is delivered to whoever is active when
it lands.

Four forms — with or without an `Emitter`, with or without a name:

```rust
app.spawn(future);                              // any Future<Output = ()> + Send + 'static
app.spawn_with(|emitter| async move { });       // the same, handed an Emitter
app.spawn_keyed("search", future);              // ...and findable later by name
app.spawn_keyed_with("search", |emitter| async move { });
```

`spawn_with` just saves the `let emitter = app.emitter();` binding.

Each returns `Result<JobId, SpawnError>`. The `JobId` is what
[cancellation](#cancellation) works on; the `Result` is there because a keyed spawn can be
refused, and a job that never started is rarely something to shrug at.

### Emitter

`Emitter` is the channel back into the loop. Unlike `AppHandle` it is
`Clone + Send + Sync + 'static`, so it can be moved into a `tokio` task and outlive the
callback that made it.

| Method | |
| --- | --- |
| `emit(event) -> bool` | send an event; `false` means the loop has stopped |
| `emit_boxed(boxed) -> bool` | the same, already boxed |
| `is_open() -> bool` | whether the loop is still listening |

The channel is unbounded on purpose: a bounded one would let a task block on send while the
loop is busy elsewhere, and the loop is the only reader.

See [events.md](events.md#emitting-events) for how `Emitter::emit` differs from
`AppHandle::emit`.

### When work is queued

`spawn` queues the future the same way `push` and `emit` queue their requests; the loop hands
it to the runtime once the current callback returns. That is what keeps `spawn` callable
outside a runtime context, and it means the work starts a moment after the call, not during
it.

The `JobId` is minted immediately even so, and is live from the moment you have it — a job
cancelled in the same callback that started it is dropped without ever being polled.

### Shutdown

`run()` does not return while spawned work is in flight, so a result still lands even if the
event producers dried up meanwhile.

> A deliberately endless spawned future therefore keeps the application alive. Long-lived
> listening belongs in an [event producer](events.md#event-producers). Otherwise end the
> application with `exit()`, which aborts whatever is still running.

A spawned future that panics is reported as `JobEnded { outcome: Panicked }` — and logged,
under the `logging` feature — and the loop carries on. Its siblings keep running; there is no
reason for the rest of the application to come down too.

### Cancellation

`exit()` still aborts everything. To stop one job, cancel it by id or by name:

```rust
app.cancel(job);              // a JobId from spawn
app.cancel_key("search");     // a name from spawn_keyed
app.cancel_all();             // everything, without ending the application
```

Both return whether there was a live job to stop.

Keys are the point of the whole thing: **one activity can stop work another activity started
without either of them passing a handle to the other.** There is no registry to build and no
`JobId` to thread through your state — the application keeps it.

At most one live job holds a given key, which makes the restart pattern explicit rather than
accidental. A second `spawn_keyed` under a busy key is *refused*, and the incumbent comes back
with the error so you can take the key by force:

```rust
if let Err(SpawnError::KeyInUse { running, .. }) = app.spawn_keyed("search", fresh_query()) {
    app.cancel(running);
    app.spawn_keyed("search", fresh_query())?;
}
```

| Query | |
| --- | --- |
| `is_running(id) -> bool` | is this job still live |
| `is_key_running(key) -> bool` | is this key taken |
| `jobs() -> impl Iterator<Item = (JobId, Option<&str>)>` | everything live |
| `job_count() -> usize` | how many |

Cancelling takes effect immediately as far as the application is concerned: `is_running`
reports `false` before `cancel` even returns. What it does **not** promise is that the future
was interrupted — work that had already finished but had not yet been collected by the loop
still reports `Cancelled`.

### JobEnded

Every job emits exactly one `JobEnded` when it stops being live, keyed or not:

```rust
pub struct JobEnded {
    pub id: JobId,
    pub key: Option<String>,
    pub outcome: JobOutcome,   // Completed | Cancelled | Panicked
}
```

It is an ordinary event, so it goes down the [normal dispatch chain](events.md#the-dispatch-chain)
and reaches `post_event` like anything else. Two things it is deliberately **not**:

> **It is not a completion barrier.** The loop takes job results and `Emitter` events from two
> independent sources and does not favour either, so a job's own `emitter.emit(Done)` may still
> be waiting when its `JobEnded { outcome: Completed }` is dispatched. Read a job's result from
> the event the job sent, never from this one.

> **It is not a shutdown notice.** Jobs killed by `exit()` are not eulogised — the loop has
> already stopped, so there would be nobody to hear it.

One consequence worth internalising: a key is free once you have received `JobEnded` for it,
not once the future returned. Those are usually the same instant and occasionally are not.

And the obvious footgun, for the same reason an endless future is one: a `JobEnded` handler
that unconditionally spawns replacement work gives the loop something new to wait for every
time, so it never runs dry.

---

## Putting them together

One task supervising several concurrent downloads, naming each job after what it is fetching:

```rust
#[derive(Debug, Default, ManagedState)]
struct Downloader {
    #[inject] fetcher: Injected<dyn Fetcher>,
    #[global] completed: u32,
}

TaskBuilder::<Downloader>::new()
    .on_event(|_task: &mut Downloader, event: &StartJob, app: &mut AppHandle| {
        let (id, url) = (event.id, event.url.clone());
        // Re-requesting the same url restarts it rather than racing itself.
        app.cancel_key(&url);
        let _ = app.spawn_keyed_with(url.clone(), move |emitter| async move {
            emitter.emit(JobDone { id, body: fetch(&url).await });
        });
        EventHandlerReturn::Consumed
    })
    .on_event(|task: &mut Downloader, _event: &JobDone, _app| {
        task.completed += 1;
        EventHandlerReturn::Ignored   // let the active activity react too
    })
    .on_event(|_task: &mut Downloader, _event: &CancelAll, app: &mut AppHandle| {
        app.cancel_all();
        EventHandlerReturn::Consumed
    })
    .build()
```

The task is the supervisor; each job is an independent future. Note the `JobDone` handler
returning `Ignored` — the task updates its bookkeeping and still lets whichever screen is in
front render the result.

What the task no longer needs is an `in_flight: Vec<u32>` of its own. Before keys, tracking
which jobs were live meant pushing on start and retaining on finish, in two handlers that had
to agree — and it still gave you no way to *stop* one. `app.jobs()` is that list, kept by the
application, and the screen that wants to cancel a download does not have to go through the
supervisor to do it.

## See also

- [events.md](events.md) — the dispatch chain and event producers
- [dependency_injection.md](dependency_injection.md) — `#[global]` and `#[inject]` on task state
- [application.md](application.md#shutdown) — exactly when the loop stops
