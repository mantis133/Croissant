# What is Croissant

Croissant is a framework for bringing Android activity structure to Rust. 



# Features


## Activities
Activities are a combination of a `struct` as state and a collection of life-cycle methods that are registered with the global Application `struct`

Croissant comes with a few builtin methods 
- on_create: called once on application start-up 
- on_resume: Called when the activity comes into the foreground. This includes the first activity that is run.
- on_pause: Called when an activity is removed from view, via navigation or application exit
- on_destroy: called once per activity on 

## Custom Events


## Background Tasks



## Global State

Activities do not share a typed `AppState` struct. Instead the application owns a
`ValueStore`: a map of named, type-erased values that any activity can read and write.

Every callback is handed an `&mut AppHandle`, which is the activity's whole view of the
application:

```rust
ActivityBuilder::<Menu>::new()
    .on_resume(|state, app| {
        app.set("current_screen", "menu");
    })
    .on_event(|state: &mut Menu, event: &KeyPress, app: &mut AppHandle| {
        // named values, shared with every other activity
        *app.get_or_insert_with("keys_pressed", || 0u32) += 1;

        // propagate a new event
        app.emit(Redraw);

        // navigate
        if event.is_enter() {
            app.push(Details::new(state.cursor));
        }

        EventHandlerReturn::Consumed
    })
```

Values are keyed by name *and* type. `get::<T>("key")` returns `None` if the key is absent
*or* holds something that is not a `T`, so a mistyped read is a miss rather than a panic.
`get_or_insert_with` is the exception: it panics on a type collision rather than silently
overwriting data another activity depends on.

Seed values at build time with `.value(key, value)`, and read them back after `run()`
returns via `app.values()`.

### Why a handle and not `&mut Application`

While an activity's callback is running, that activity is borrowed out of the application's
registry, so a second `&mut Application` cannot exist. `AppHandle` is a separate field of
`Application` holding exactly the pieces an activity is allowed to touch, borrowed
disjointly from the registry.

That has one visible consequence: navigation, emitted events, and `exit()` are **queued**,
not immediate. They take effect once the callback returns, in the order requested. Emitted
events are dispatched before the application asks its producers for anything new.

