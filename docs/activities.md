# Activities

An activity is a **screen**: a state struct plus a set of lifecycle and event callbacks.
Exactly one activity is active at a time, and it is the first thing to see every event.

```rust
use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application},
    events::{ApplicationEvent, EventHandlerReturn},
};

#[derive(Debug, Default, ManagedState)]
struct Menu {
    cursor: usize,
}

let menu = ActivityBuilder::<Menu>::new()
    .on_create(|state: &mut Menu, _app| state.cursor = 0)
    .on_event(|state: &mut Menu, event: &KeyPress, app: &mut AppHandle| {
        match event {
            KeyPress::Down => state.cursor += 1,
            KeyPress::Enter => app.push::<Details>(),
            _ => return EventHandlerReturn::Ignored,
        }
        EventHandlerReturn::Consumed
    })
    .build();
```

Two things go into that: the **state type**, which must implement
[`ManagedState`](dependency_injection.md), and the **activity**, which holds the callbacks.
They are separate because one set of callbacks serves every instance of that state type.

## The state type

Anything `Debug + Send + 'static` works. The simplest possible state:

```rust
#[derive(Debug, Default)]
struct Home;
impl ManagedState for Home {}
```

`ManagedState`'s methods are all defaulted to no-ops, so a hand-written `impl` is a single
empty block. Use `#[derive(ManagedState)]` instead when you want `#[global]` or `#[inject]`
fields — see [dependency_injection.md](dependency_injection.md).

`Default` is required by `push::<A>()` and `starting_activity::<A>()`, which construct the
instance for you. The `_with` variants take an instance instead and drop that requirement.

## Lifecycle

Callbacks fire **once per instance**, not once per type. Pushing the same activity twice
creates two instances and runs `on_create` twice.

| Callback | When |
| --- | --- |
| `on_create` | the instance is created — at start-up, or on `push`/`replace`. Runs on a `Default` value. |
| `on_resume` | the activity enters the foreground, including every return from the backstack |
| `on_pause` | the activity leaves the foreground, including at application exit |
| `on_destroy` | the instance is finished: popped, replaced, or still alive when the app exits |

`on_create` and `on_destroy` pair up; `on_resume` and `on_pause` pair up and may run many
times in between. Returning to an activity from the backstack resumes it — it does **not**
create it again.

### Initialising in `on_create`

Because the instance is `Default`-constructed and `on_create` runs before anything else sees
it, `on_create` replaces the `Menu::new(..)` constructor that would otherwise have to spell
out every field:

```rust
.on_create(|state: &mut Menu, app: &mut AppHandle| {
    state.cursor = 0;
    state.entries = app.get::<Vec<String>>("entries").cloned().unwrap_or_default();
})
```

This also works for `#[global]` fields: assigning one in `on_create` writes through to the
shared store, which is how a global gets seeded explicitly rather than by accident of
construction order.

## Handling events

`on_event` is registered per concrete event type. Registering twice for the same type
replaces the earlier handler.

```rust
.on_event(|state: &mut Menu, event: &Tick, app: &mut AppHandle| -> EventHandlerReturn {
    EventHandlerReturn::Consumed
})
```

The return value says whether the event keeps travelling:

- `EventHandlerReturn::Consumed` — stop here. Tasks and the application handler never see it.
- `EventHandlerReturn::Ignored` — pass it on. This is also the `Default`.

`post_event` runs after *every* event this activity sees, consumed or not, and cannot
consume:

```rust
.post_event(|state: &mut Menu, event: &dyn ApplicationEvent, app: &mut AppHandle| {
    state.redraw_needed = true;
})
```

See [events.md](events.md) for the full chain.

## Navigation

All navigation goes through the `AppHandle`, and all of it is **queued** — it takes effect
once the current callback returns.

| Method | Effect |
| --- | --- |
| `app.push::<A>()` | pause the current activity, create and resume a new `A` |
| `app.push_with(a)` | the same, with an instance you constructed |
| `app.pop()` | destroy the current activity and resume the one beneath it |
| `app.replace::<A>()` | destroy the current activity and create a new `A` in its place |
| `app.replace_with(a)` | the same, with an instance you constructed |
| `app.exit()` | stop the event loop |

`push::<A>()` and `replace::<A>()` need `A: Default`; the `_with` forms do not. Either way
`on_create` runs.

### The backstack

`push` only remembers the outgoing activity if the application was built with `.backstack()`:

```rust
Application::builder()
    .backstack()
    // ...
```

Without it, `push` **destroys** the outgoing activity rather than stashing it, and `pop()`
does nothing — there is nothing to return to. With it:

```
push(Details)   Home.on_pause → Details.on_create → Details.on_resume
                backstack: [Home]

pop()           Details.on_pause → Details.on_destroy → Home.on_resume
                backstack: []
```

`replace` never touches the backstack: the outgoing activity is destroyed and the new one
takes its slot, leaving whatever was underneath alone.

`pop()` on an empty or disabled backstack is a no-op, logged under the `logging` feature.

### Passing data to the next screen

Activities are `Default`-constructed, so there is nowhere to put a constructor argument.
Two options:

```rust
// 1. Through shared state — the usual way.
app.set("selected_id", row.id);
app.push::<Details>();

// 2. Directly, when the value is genuinely local to that one screen.
app.push_with(Details { selected_id: row.id, ..Default::default() });
```

## Ordering example

Starting on `Root`, which pushes `Leaf`, which immediately pops:

```
root:create
root:resume
root:pause        ← push
leaf:create
leaf:resume
leaf:pause        ← pop
leaf:destroy
root:resume       ← resumed, not created again
root:pause        ← loop ends
root:destroy
```

## See also

- [dependency_injection.md](dependency_injection.md) — `#[global]` and `#[inject]` fields
- [events.md](events.md) — where an activity sits in the dispatch chain
- [tasks.md](tasks.md) — for behaviour shared across many activities
