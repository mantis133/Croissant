# What is Croissant

Croissant is a framework for bringing Android activity structure to Rust. 



# Features


## Activities
Activities are a combination of a `struct` as state and a collection of life-cycle methods that are registered with the global Application `struct`

Croissant comes with a few builtin methods, called once per *instance* of an activity:
- on_create: called when an instance is created — at start-up for the first activity, and on
  every `push`/`replace` after that. The instance is `Default`-constructed, so this is where
  it gets its real values. Returning to an activity from the backstack does *not* create it
  again.
- on_resume: Called when the activity comes into the foreground. This includes the first
  activity that is run, and every return from the backstack.
- on_pause: Called when an activity is removed from view, via navigation or application exit
- on_destroy: called when an instance is finished — popped off the backstack, replaced, or
  still alive when the application exits. Paired with on_create.

## Custom Events


## Background Tasks



## Dependency Injection

Shared state is declared as a **field** on the activity struct and used as a plain field.
`#[derive(ActivityState)]` wires it up:

```rust
#[derive(Debug, Default, ActivityState)]
struct Dashboard {
    #[global]              counter: u32,                  // key "counter", read-write
    #[global("app.host")]  host: String,                  // explicit key
    #[global(readonly)]    app_name: String,              // writes discarded
    #[inject]              repo: Injected<dyn UserRepo>,  // resolved by type
    #[inject("cache")]     fast: Injected<dyn UserRepo>,  // ...and by qualifier
                           cursor: usize,                 // plain local field
}
```

```rust
ActivityBuilder::<Dashboard>::new()
    .on_create(|state, _app| {
        state.counter = 10;   // seeds the global
        state.cursor = 0;     // ordinary local init — no `new()` needed
    })
    .on_event(|state: &mut Dashboard, _e: &Tick, app: &mut AppHandle| {
        state.counter += 1;                 // a plain field access
        let users = state.repo.find_all();  // deref straight through Injected
        EventHandlerReturn::Consumed
    })
```

`global` and `inject` are **inert helper attributes**: the compiler ignores them, so
`counter` stays a genuine `u32` with no wrapper and no indirection.

### Two attributes, two keying strategies

|                  | `#[global]`              | `#[inject]`                  |
| ---------------- | ------------------------ | ---------------------------- |
| Keyed by         | field name (overridable) | field type (+ qualifier)     |
| Semantics        | value moved in and out   | `Arc` handle, read-only      |
| Spring analogue  | `@Value`                 | `@Autowired` / `@Service`    |
| Backing store    | `ValueStore`             | `ServiceRegistry`            |

The split is load-bearing. Name-keying is what makes several `String`s work: `username` and
`hostname` are the same `TypeId`, so no type-based lookup could separate them, but they are
trivially two field names. Type-keying is what makes `Injected<dyn UserRepo>` work, so an
activity depends on a trait and a fake can be registered instead in a test:

```rust
Application::builder()
    .service::<dyn UserRepo>(Arc::new(PostgresRepo::new()))
    .service_named::<dyn UserRepo>("cache", Arc::new(RedisRepo::new()))
```

Two activities that declare the same `#[global]` field name share one value — that is the
sharing. Two registrations of one service type are told apart by their qualifier.

### How it works

```
checkout (before each callback):  slot exists → swap(field, slot); absent → leave field
checkin  (after each callback):   slot exists → swap(field, slot); absent → set(key, take(field))
```

Only one activity is active at a time and callbacks never nest, so there is no aliasing.
Swaps are free, so a `#[global] Vec<Record>` costs nothing per event; the only bound is
`T: Default`. `#[inject]` fields resolve once, when the instance is created.

Because `on_create` is bracketed like every other callback, an assignment there reaches the
store — which is why `on_create` is the place to seed a global and to initialise local
fields, instead of writing an `A::new()` that spells out every field.

Forgetting the derive is caught by the compiler: a helper attribute only exists while its
derive is applied, so a bare `#[global]` fails with *cannot find attribute `global` in this
scope* rather than silently never syncing.

### Sharp edges

- **Do not read your own global by name.** While a callback runs, the field is
  authoritative and the store slot holds a `T::default()` placeholder, so
  `app.get::<T>("counter")` for that activity's own `#[global] counter` reads `0`/`""`/empty
  rather than the real value. `contains` still reports `true`. Writes via `app.set` during
  the callback are clobbered at check-in.
- **`#[global(readonly)]` clones per callback** (`T: Clone`). For zero-cost read-only config
  prefer `#[inject("app.name")] app_name: Injected<String>`.
- **`on_create` fires once per instance**, so pushing the same activity twice re-runs its
  seeding. Guard with `app.contains(key)` if that matters.
- **An unresolved `#[inject]` panics on first deref**, naming the type and qualifier. A
  mismatch with the registration is a runtime failure, not a compile error.

## Global State

Underneath the attributes, the application owns a `ValueStore`: a map of named, type-erased
values. Every callback is handed an `&mut AppHandle`, which reaches it directly — useful for
values that do not warrant a field, and the only option for hand-written `ActivityState`
impls:

```rust
*app.get_or_insert_with("keys_pressed", || 0u32) += 1;
app.emit(Redraw);
app.push::<Details>();
```

Values are keyed by name *and* type. `get::<T>("key")` returns `None` if the key is absent
*or* holds something that is not a `T`, so a mistyped read is a miss rather than a panic.
`get_or_insert_with` is the exception: it panics on a type collision rather than silently
overwriting data another activity depends on.

Seed values at build time with `.value(key, value)`, and read them back after `run()` returns
via `app.values()`.

### Why a handle and not `&mut Application`

While an activity's callback is running, that activity is borrowed out of the application's
registry, so a second `&mut Application` cannot exist. `AppHandle` is a separate field of
`Application` holding exactly the pieces an activity is allowed to touch, borrowed
disjointly from the registry.

That has one visible consequence: navigation, emitted events, and `exit()` are **queued**,
not immediate. They take effect once the callback returns, in the order requested. Emitted
events are dispatched before the application asks its producers for anything new.
