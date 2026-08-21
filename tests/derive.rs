//! The `#[derive(ManagedState)]` path: the same runtime contract as `tests/injection.rs`,
//! but reached through the attributes rather than hand-written impls.

use std::sync::Arc;

use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application, Injected},
    events::{ApplicationEvent, EventHandlerReturn},
};
use futures::stream;

#[derive(Debug)]
struct Ping;
impl ApplicationEvent for Ping {}

fn producer(
    events: Vec<Box<dyn ApplicationEvent>>,
) -> impl stream::Stream<Item = Box<dyn ApplicationEvent>> + Send {
    stream::iter(events)
}

trait UserRepo: Send + Sync {
    fn label(&self) -> &'static str;
}

struct Postgres;
impl UserRepo for Postgres {
    fn label(&self) -> &'static str {
        "postgres"
    }
}

struct Cache;
impl UserRepo for Cache {
    fn label(&self) -> &'static str {
        "cache"
    }
}

#[derive(Debug, Default, ManagedState)]
struct Dashboard {
    #[global]
    counter: u32,
    #[global("app.host")]
    host: String,
    #[global(readonly)]
    app_name: String,
    #[inject]
    repo: Injected<dyn UserRepo>,
    #[inject("cache")]
    fast: Injected<dyn UserRepo>,
    cursor: usize,
}

/// A second activity sharing `counter` by declaring the same field name.
#[derive(Debug, Default, ManagedState)]
struct Sidebar {
    #[global]
    counter: u32,
}

#[tokio::test]
async fn the_dream_access_pattern() {
    let dashboard = ActivityBuilder::<Dashboard>::new()
        .on_create(|state: &mut Dashboard, _app| {
            // Seeds the global through the field, on a Default-constructed instance.
            state.counter = 10;
            state.host = String::from("localhost");
            state.cursor = 3; // ordinary local field, no `new()` needed
        })
        .on_event(
            |state: &mut Dashboard, _event: &Ping, app: &mut AppHandle| {
                state.counter += 1; // <-- the whole point
                state.cursor += 1;
                state.host.push('!');

                app.set("repo_label", state.repo.label());
                app.set("fast_label", state.fast.label());
                app.set("app_name_seen", state.app_name.clone());
                app.set("cursor", state.cursor);
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .value("app_name", String::from("croissant"))
        .service::<dyn UserRepo>(Arc::new(Postgres))
        .service_named::<dyn UserRepo>("cache", Arc::new(Cache))
        .add_activity(dashboard)
        .starting_activity::<Dashboard>()
        .add_event_producer(producer(vec![Box::new(Ping), Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("counter"), Some(&12), "10 + 1 + 1");
    assert_eq!(
        app.values().get::<String>("app.host").map(String::as_str),
        Some("localhost!!"),
        "explicit key, mutated across two callbacks"
    );
    assert_eq!(app.values().get::<usize>("cursor"), Some(&5), "3 + 1 + 1");

    assert_eq!(
        app.values().get::<&str>("repo_label").copied(),
        Some("postgres")
    );
    assert_eq!(
        app.values().get::<&str>("fast_label").copied(),
        Some("cache"),
        "the qualifier should pick the second registration"
    );

    assert_eq!(
        app.values()
            .get::<String>("app_name_seen")
            .map(String::as_str),
        Some("croissant"),
        "readonly field is populated from the store"
    );
    assert_eq!(
        app.values().get::<String>("app_name").map(String::as_str),
        Some("croissant"),
        "readonly field is never written back"
    );
}

#[tokio::test]
async fn the_same_field_name_is_the_same_global_across_activities() {
    let dashboard = ActivityBuilder::<Dashboard>::new()
        .on_event(
            |state: &mut Dashboard, _event: &Ping, app: &mut AppHandle| {
                state.counter += 5;
                app.push::<Sidebar>();
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let sidebar = ActivityBuilder::<Sidebar>::new()
        .on_resume(|state: &mut Sidebar, _app| state.counter *= 3)
        .build();

    let mut app = Application::builder()
        .service::<dyn UserRepo>(Arc::new(Postgres))
        .service_named::<dyn UserRepo>("cache", Arc::new(Cache))
        .add_activity(dashboard)
        .add_activity(sidebar)
        .starting_activity::<Dashboard>()
        .backstack()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("counter"), Some(&15), "(0 + 5) * 3");
}

#[derive(Debug, Default, ManagedState)]
struct Probe {
    #[global]
    counter: u32,
}

/// Pins the documented sharp edge: while a callback holds a `#[global]` field checked out,
/// the store slot holds a `T::default()` placeholder rather than the live value. Reading
/// your own global by name inside your own callback gets you `0`, not the real count.
#[tokio::test]
async fn a_checked_out_global_reads_as_default_by_name() {
    let probe = ActivityBuilder::<Probe>::new()
        .on_event(|state: &mut Probe, _event: &Ping, app: &mut AppHandle| {
            state.counter += 1;
            let seen_by_name = app.get::<u32>("counter").copied();
            let present = app.contains("counter");
            app.get_or_insert_with("observed", Vec::<(bool, Option<u32>)>::new)
                .push((present, seen_by_name));
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(probe)
        .starting_activity::<Probe>()
        .add_event_producer(producer(vec![
            Box::new(Ping),
            Box::new(Ping),
            Box::new(Ping),
        ]))
        .build();

    app.run().await;

    // The field itself accumulated correctly...
    assert_eq!(app.values().get::<u32>("counter"), Some(&3));
    // ...but by-name reads from inside the callback saw the placeholder every time.
    assert_eq!(
        app.values()
            .get::<Vec<(bool, Option<u32>)>>("observed")
            .unwrap(),
        &vec![(true, Some(0)), (true, Some(0)), (true, Some(0))],
        "the key stays present, but holds T::default() while checked out"
    );
}

/// A struct with no attributes still derives fine — it just gets the trait's no-op defaults.
#[derive(Debug, Default, ManagedState)]
struct Plain {
    local: usize,
}

#[tokio::test]
async fn a_state_with_no_attributes_derives_to_no_ops() {
    let plain = ActivityBuilder::<Plain>::new()
        .on_event(|state: &mut Plain, _event: &Ping, app: &mut AppHandle| {
            state.local += 1;
            app.set("local", state.local);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(plain)
        .starting_activity::<Plain>()
        .add_event_producer(producer(vec![Box::new(Ping), Box::new(Ping)]))
        .build();

    app.run().await;

    // Local state persists on the instance across callbacks, untouched by the store.
    assert_eq!(app.values().get::<usize>("local"), Some(&2));
    assert!(!app.values().contains("local_field"));
}

/// Generic states work: the impl generics are passed through.
#[derive(Debug, Default, ManagedState)]
struct Generic<T: std::fmt::Debug + Send + Default + 'static> {
    #[global("generic.value")]
    value: u32,
    marker: std::marker::PhantomData<T>,
}

#[tokio::test]
async fn generic_states_derive() {
    let activity = ActivityBuilder::<Generic<String>>::new()
        .on_event(
            |state: &mut Generic<String>, _event: &Ping, _app: &mut AppHandle| {
                state.value += 2;
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .add_activity(activity)
        .starting_activity::<Generic<String>>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("generic.value"), Some(&2));
}
