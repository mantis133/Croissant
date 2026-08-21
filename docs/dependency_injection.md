# Dependency injection

Shared state is declared as a **field** on the state struct and used as a plain field.

```rust
use croissant::{ManagedState, application::Injected};

#[derive(Debug, Default, ManagedState)]
struct Dashboard {
    #[global]              counter: u32,                  // shared value, keyed by name
    #[global("app.host")]  host: String,                  // ...under an explicit key
    #[global(readonly)]    app_name: String,              // ...writes discarded
    #[inject]              repo: Injected<dyn UserRepo>,  // shared service, keyed by type
    #[inject("cache")]     fast: Injected<dyn UserRepo>,  // ...under a qualifier
                           cursor: usize,                 // ordinary local state
}
```

```rust
// in any callback:
state.counter += 1;
let users = state.repo.find_all();
```

`global` and `inject` are **inert helper attributes**: the compiler ignores them entirely, so
`counter` stays a genuine `u32`. `state.counter += 1` is a plain field access — no wrapper,
no deref, no lock.

This works for [activity](activities.md) state and [task](tasks.md) state alike.

## Two attributes, two keying strategies

|  | `#[global]` | `#[inject]` |
| --- | --- | --- |
| Keyed by | field **name** (overridable) | field **type** (+ optional qualifier) |
| Semantics | a value, moved in and out | an `Arc` handle, read-only |
| Mutable | yes | no |
| Spring analogue | `@Value` | `@Autowired` / `@Service` |
| Backing store | `ValueStore` | `ServiceRegistry` |

The split is load-bearing, and each half covers what the other cannot.

**Name-keying is what makes several values of one type work.** `username: String` and
`hostname: String` are the same `TypeId`, so no type-based lookup could ever tell them
apart — but they are trivially two different field names.

**Type-keying is what makes `dyn Trait` work.** An activity depending on
`Injected<dyn UserRepo>` has no idea which implementation it got, so a test can register a
fake instead of the real one.

---

## `#[global]` — shared values

Two structs declaring the same field name share one value. That *is* the sharing:

```rust
#[derive(Debug, Default, ManagedState)]
struct Dashboard { #[global] counter: u32 }

#[derive(Debug, Default, ManagedState)]
struct Sidebar   { #[global] counter: u32 }   // the same "counter"
```

| Form | Effect |
| --- | --- |
| `#[global]` | key is the field name; read-write |
| `#[global("app.host")]` | explicit key, for when the field name is not the shared name |
| `#[global(readonly)]` | populated before each callback, never written back |
| `#[global("k", readonly)]` | both |

Requirements: `T: Default + Send + 'static`. `readonly` additionally needs `T: Clone`.

### Seeding a value

Globals start at `T::default()`. Give one a real starting value either at build time or in
`on_create` / `on_start`:

```rust
Application::builder().value("counter", 41u32)      // at build time

.on_create(|state: &mut Dashboard, _app| state.counter = 41)   // in code
```

The second works because `on_create` runs on a `Default` instance and is bracketed like every
other callback, so the assignment writes through to the store. A build-time value wins over
the field's own initial value: it is already in the store when the first check-out happens.

### How it works

Around **each** callback:

```
check-out:  slot exists → swap(field, slot);  absent → leave the field as it is
check-in:   slot exists → swap(field, slot);  absent → store the field's value
```

Only one activity is active at a time and callbacks never nest, so there is no aliasing.
Swaps are free, so a `#[global] Vec<Record>` costs nothing per event — the `Default` bound is
only needed for the very first check-in.

Read-only fields are the exception: they are *cloned* in and never checked back in, which is
what discards writes.

### Sharp edges

> **Do not read your own global by name.** While a callback runs, the field is authoritative
> and the store slot holds a `T::default()` placeholder. `app.get::<u32>("counter")` inside
> that activity's own callback reads `0`, not the real value — though `app.contains("counter")`
> is still `true`. Writes via `app.set` during the callback are clobbered at check-in.

- **`#[global(readonly)]` clones on every callback.** For zero-cost read-only config, prefer
  `#[inject("app.name")] app_name: Injected<String>` — an `Arc` clone, and genuinely
  immutable.
- **Key collisions panic loudly.** If a key is already held by a different type, check-out
  panics naming both. That is deliberate: silently overwriting another activity's data would
  be far worse.
- **`on_create` fires once per instance**, so pushing the same activity twice re-runs its
  seeding. Guard with `app.contains(key)` if that matters.

---

## `#[inject]` — shared services

Register services on the builder, naming the type explicitly so `Arc<Concrete>` coerces to
`Arc<dyn Trait>`:

```rust
Application::builder()
    .service::<dyn UserRepo>(Arc::new(PostgresRepo::new()))
    .service_named::<dyn UserRepo>("cache", Arc::new(RedisRepo::new()))
    .service(Arc::new(SystemClock))              // a concrete type needs no turbofish
```

And declare them as fields:

```rust
#[inject]          repo: Injected<dyn UserRepo>,
#[inject("cache")] fast: Injected<dyn UserRepo>,
```

Registrations are keyed on `(TypeId, Option<qualifier>)`, so one type can have several
implementations told apart by name. A second registration for the same pair replaces the
first.

### `Injected<T>`

`Injected<T>` derefs to `T`, so an injected service is called like an ordinary field:

```rust
let users = state.repo.find_all();
```

`T` is `?Sized`, which is what allows `Injected<dyn UserRepo>`.

| Method | |
| --- | --- |
| `is_present()` | whether a service was resolved |
| `get()` | `Option<&Arc<T>>`, for when a miss should be handled rather than panic |

Services resolve **once**, when the instance is created — before `on_create` for an activity,
before `on_start` for a task.

> **An unresolved `#[inject]` panics on first deref**, naming the type and qualifier. A
> mismatch between the attribute and the registration is a runtime failure, not a compile
> error — that is the cost of string qualifiers. Use `is_present()` if a service is genuinely
> optional.

Services are shared across threads, so `T: Send + Sync`. `Injected<T>` derefs to `&T` only —
for mutable shared state use `#[global]`, or register an `Injected<Mutex<T>>` and lock
explicitly.

### Why trait objects matter

This is the payoff. The same activity, with a fake supplied:

```rust
// production
.service::<dyn UserRepo>(Arc::new(PostgresRepo::new()))

// test
.service::<dyn UserRepo>(Arc::new(FakeRepo::with(vec![user("ada")])))
```

The activity is unchanged and knows nothing about either.

---

## The derive

`#[derive(ManagedState)]` generates an `impl ManagedState`, implementing only the hooks the
struct actually needs. A struct with no attributes gets the trait's no-op defaults.

Compile-time errors it produces:

- both `#[global]` and `#[inject]` on one field;
- an unknown option, e.g. `#[global(writeonly)]`;
- a `#[global]` field of a tuple struct with no explicit key — there is no field name to use.

Generic structs work; the impl generics pass through.

**Forgetting the derive is caught by the compiler.** A helper attribute only exists while its
derive is applied, so a bare `#[global]` fails with *cannot find attribute `global` in this
scope* rather than silently never syncing.

## Doing it by hand

The derive is optional. `ManagedState`'s three methods are all defaulted, so a plain state
type is one empty impl:

```rust
#[derive(Debug, Default)]
struct Home;
impl ManagedState for Home {}
```

To hand-roll what the derive emits, use the `ValueStore` helpers directly:

```rust
impl ManagedState for Adder {
    fn inject_services(&mut self, services: &ServiceRegistry) {
        self.repo = services.resolve(None);
    }
    fn checkout_globals(&mut self, store: &mut ValueStore) {
        store.checkout_field("counter", &mut self.counter);
    }
    fn checkin_globals(&mut self, store: &mut ValueStore) {
        store.checkin_field("counter", &mut self.counter);
    }
}
```

(For a read-only field, use `clone_field` in check-out and implement no check-in at all.)

---

## Reaching the store directly

Underneath the attributes, the application owns a `ValueStore`: a map of named, type-erased
values reachable from every callback through the `AppHandle`. Useful for values that do not
warrant a field, and the only option for hand-written impls that skip the sync hooks.

```rust
app.set("last_key", 'q');
let n = app.get::<u32>("count");
*app.get_or_insert_with("hits", || 0u32) += 1;
let owned = app.take::<String>("pending");
app.remove("stale");
app.contains("count");
```

Values are keyed by name **and** type: `get::<T>("key")` returns `None` if the key is absent
*or* holds something that is not a `T`, so a mistyped read is a miss rather than a panic.
`get_or_insert_with` is the exception — it panics on a type collision rather than silently
overwriting data another activity depends on.

`app.values()` / `app.values_mut()` reach the whole store; `app.services()` on the
`Application` reaches the registry. Both are available after `run()` returns, which is the
usual way to assert on results in tests.

## See also

- [activities.md](activities.md) — where activity state lives
- [tasks.md](tasks.md) — task state uses the same machinery
- [application.md](application.md) — registering values and services on the builder
