//! Runtime behaviour of `#[global]` field check-out/check-in and `#[inject]` service
//! resolution, written against hand-rolled `ManagedState` impls.
//!
//! These are the impls `#[derive(ManagedState)]` generates. Testing them directly keeps the
//! runtime contract pinned independently of the macro, and keeps the hand-written path —
//! which stays supported — covered.

use std::sync::Arc;

use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application, Injected, ServiceRegistry, ValueStore},
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

// ---------------------------------------------------------------- shared mutable globals

/// `#[global] counter: u32` plus a plain local field.
#[derive(Debug, Default)]
struct Adder {
    counter: u32,
    local_hits: usize,
}

impl ManagedState for Adder {
    fn checkout_globals(&mut self, store: &mut ValueStore) {
        store.checkout_field("counter", &mut self.counter);
    }
    fn checkin_globals(&mut self, store: &mut ValueStore) {
        store.checkin_field("counter", &mut self.counter);
    }
}

/// A second activity declaring the same global, to prove they share one value.
#[derive(Debug, Default)]
struct Doubler {
    counter: u32,
}

impl ManagedState for Doubler {
    fn checkout_globals(&mut self, store: &mut ValueStore) {
        store.checkout_field("counter", &mut self.counter);
    }
    fn checkin_globals(&mut self, store: &mut ValueStore) {
        store.checkin_field("counter", &mut self.counter);
    }
}

#[tokio::test]
async fn plain_field_assignment_reaches_the_store() {
    let adder = ActivityBuilder::<Adder>::new()
        .on_event(|state: &mut Adder, _event: &Ping, _app| {
            state.counter += 1; // the whole point: a plain field access
            state.local_hits += 1;
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(adder)
        .starting_activity::<Adder>()
        .add_event_producer(producer(vec![
            Box::new(Ping),
            Box::new(Ping),
            Box::new(Ping),
        ]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("counter"), Some(&3));
}

#[tokio::test]
async fn two_activities_share_one_global() {
    let adder = ActivityBuilder::<Adder>::new()
        .on_event(|state: &mut Adder, _event: &Ping, app: &mut AppHandle| {
            state.counter += 1;
            app.push::<Doubler>();
            EventHandlerReturn::Consumed
        })
        .build();

    let doubler = ActivityBuilder::<Doubler>::new()
        // Reads what Adder wrote, through its own field.
        .on_resume(|state: &mut Doubler, app: &mut AppHandle| {
            state.counter *= 2;
            app.set("seen_by_doubler", state.counter);
        })
        .build();

    let mut app = Application::builder()
        .add_activity(adder)
        .add_activity(doubler)
        .starting_activity::<Adder>()
        .backstack()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("counter"), Some(&2));
    assert_eq!(app.values().get::<u32>("seen_by_doubler"), Some(&2));
}

#[tokio::test]
async fn on_create_seeds_the_global_through_the_field() {
    let adder = ActivityBuilder::<Adder>::new()
        // on_create runs on a Default instance and is bracketed like any other callback,
        // so this assignment reaches the store.
        .on_create(|state: &mut Adder, _app| state.counter = 100)
        .on_event(|state: &mut Adder, _event: &Ping, _app| {
            state.counter += 1;
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(adder)
        .starting_activity::<Adder>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("counter"), Some(&101));
}

#[tokio::test]
async fn a_builder_seeded_value_wins_over_the_field_default() {
    let adder = ActivityBuilder::<Adder>::new()
        .on_event(|state: &mut Adder, _event: &Ping, _app| {
            state.counter += 1;
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .value("counter", 41u32)
        .add_activity(adder)
        .starting_activity::<Adder>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("counter"), Some(&42));
}

// ---------------------------------------------------------------- read-only globals

/// `#[global(readonly)] app_name: String` — cloned in, never checked back in.
#[derive(Debug, Default)]
struct Banner {
    app_name: String,
}

impl ManagedState for Banner {
    fn checkout_globals(&mut self, store: &mut ValueStore) {
        store.clone_field("app_name", &mut self.app_name);
    }
    // No checkin: that is what makes it read-only.
}

#[tokio::test]
async fn readonly_globals_are_readable_but_writes_are_discarded() {
    let banner = ActivityBuilder::<Banner>::new()
        .on_event(|state: &mut Banner, _event: &Ping, app: &mut AppHandle| {
            app.set("observed", state.app_name.clone());
            state.app_name = String::from("clobbered"); // compiles, but goes nowhere
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .value("app_name", String::from("croissant"))
        .add_activity(banner)
        .starting_activity::<Banner>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<String>("observed").map(String::as_str),
        Some("croissant"),
        "the field should have been populated from the store"
    );
    assert_eq!(
        app.values().get::<String>("app_name").map(String::as_str),
        Some("croissant"),
        "the write should not have reached the store"
    );
}

// ---------------------------------------------------------------- type collisions

#[test]
#[should_panic(expected = "already held by a different type")]
fn a_type_collision_on_a_global_key_panics() {
    let mut store = ValueStore::new();
    store.set("counter", String::from("not a number"));

    let mut field = 0u32;
    store.checkout_field("counter", &mut field);
}

// ---------------------------------------------------------------- injected services

trait Greeter: Send + Sync {
    fn greet(&self) -> String;
}

struct RealGreeter;
impl Greeter for RealGreeter {
    fn greet(&self) -> String {
        String::from("hello from the real one")
    }
}

struct FakeGreeter;
impl Greeter for FakeGreeter {
    fn greet(&self) -> String {
        String::from("hello from the fake")
    }
}

/// `#[inject] greeter: Injected<dyn Greeter>` and `#[inject("loud")] loud: ...`
#[derive(Debug, Default)]
struct Front {
    greeter: Injected<dyn Greeter>,
    loud: Injected<dyn Greeter>,
}

impl ManagedState for Front {
    fn inject_services(&mut self, services: &ServiceRegistry) {
        self.greeter = services.resolve(None);
        self.loud = services.resolve(Some("loud"));
    }
}

#[tokio::test]
async fn services_resolve_by_type_and_by_qualifier() {
    let front = ActivityBuilder::<Front>::new()
        .on_event(|state: &mut Front, _event: &Ping, app: &mut AppHandle| {
            // Deref straight through the field, as if it were the service itself.
            app.set("plain", state.greeter.greet());
            app.set("loud", state.loud.greet());
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .service::<dyn Greeter>(Arc::new(RealGreeter))
        .service_named::<dyn Greeter>("loud", Arc::new(FakeGreeter))
        .add_activity(front)
        .starting_activity::<Front>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<String>("plain").map(String::as_str),
        Some("hello from the real one")
    );
    assert_eq!(
        app.values().get::<String>("loud").map(String::as_str),
        Some("hello from the fake")
    );
}

#[tokio::test]
async fn a_test_can_swap_a_fake_in_behind_the_trait() {
    let front = ActivityBuilder::<Front>::new()
        .on_event(|state: &mut Front, _event: &Ping, app: &mut AppHandle| {
            app.set("plain", state.greeter.greet());
            EventHandlerReturn::Consumed
        })
        .build();

    // Same activity, different registration — this is the payoff of keying by trait type.
    let mut app = Application::builder()
        .service::<dyn Greeter>(Arc::new(FakeGreeter))
        .add_activity(front)
        .starting_activity::<Front>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<String>("plain").map(String::as_str),
        Some("hello from the fake")
    );
}

#[test]
#[should_panic(expected = "no service of type")]
fn dereferencing_an_unresolved_service_panics() {
    let services = ServiceRegistry::new();
    let greeter: Injected<dyn Greeter> = services.resolve(None);
    assert!(!greeter.is_present());
    let _ = greeter.greet();
}

// ---------------------------------------------------------------- lifecycle

#[derive(Debug, Default)]
struct Root;
impl ManagedState for Root {}

#[derive(Debug, Default)]
struct Leaf;
impl ManagedState for Leaf {}

fn trail(app: &mut AppHandle, entry: &'static str) {
    app.get_or_insert_with("trail", Vec::<&'static str>::new)
        .push(entry);
}

#[tokio::test]
async fn on_create_fires_once_per_instance_and_on_destroy_pairs_with_it() {
    let root = ActivityBuilder::<Root>::new()
        .on_create(|_state, app| trail(app, "root:create"))
        .on_resume(|_state, app| trail(app, "root:resume"))
        .on_pause(|_state, app| trail(app, "root:pause"))
        .on_destroy(|_state, app| trail(app, "root:destroy"))
        .on_event(|_state: &mut Root, _event: &Ping, app: &mut AppHandle| {
            app.push::<Leaf>();
            EventHandlerReturn::Consumed
        })
        .build();

    let leaf = ActivityBuilder::<Leaf>::new()
        .on_create(|_state, app| trail(app, "leaf:create"))
        .on_resume(|_state, app| {
            trail(app, "leaf:resume");
            app.pop();
        })
        .on_pause(|_state, app| trail(app, "leaf:pause"))
        .on_destroy(|_state, app| trail(app, "leaf:destroy"))
        .build();

    let mut app = Application::builder()
        .add_activity(root)
        .add_activity(leaf)
        .starting_activity::<Root>()
        .backstack()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<Vec<&'static str>>("trail").unwrap(),
        &vec![
            "root:create",
            "root:resume",
            "root:pause",  // push
            "leaf:create", // new instance
            "leaf:resume", // ...which pops straight back
            "leaf:pause",
            "leaf:destroy", // popped instances are finished
            "root:resume",  // restored: resume, but NOT create again
            "root:pause",   // loop ends
            "root:destroy",
        ]
    );
}

#[tokio::test]
async fn replace_destroys_the_outgoing_activity() {
    let root = ActivityBuilder::<Root>::new()
        .on_destroy(|_state, app| trail(app, "root:destroy"))
        .on_event(|_state: &mut Root, _event: &Ping, app: &mut AppHandle| {
            app.replace::<Leaf>();
            EventHandlerReturn::Consumed
        })
        .build();

    let leaf = ActivityBuilder::<Leaf>::new()
        .on_create(|_state, app| trail(app, "leaf:create"))
        .on_destroy(|_state, app| trail(app, "leaf:destroy"))
        .build();

    let mut app = Application::builder()
        .add_activity(root)
        .add_activity(leaf)
        .starting_activity::<Root>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<Vec<&'static str>>("trail").unwrap(),
        &vec!["root:destroy", "leaf:create", "leaf:destroy"]
    );
}

#[tokio::test]
async fn push_with_accepts_a_constructed_instance() {
    let root = ActivityBuilder::<Root>::new()
        .on_event(|_state: &mut Root, _event: &Ping, app: &mut AppHandle| {
            app.push_with(Adder {
                counter: 0,
                local_hits: 7,
            });
            EventHandlerReturn::Consumed
        })
        .build();

    let adder = ActivityBuilder::<Adder>::new()
        .on_resume(|state: &mut Adder, app: &mut AppHandle| {
            app.set("local_hits", state.local_hits);
        })
        .build();

    let mut app = Application::builder()
        .add_activity(root)
        .add_activity(adder)
        .starting_activity::<Root>()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<usize>("local_hits"),
        Some(&7),
        "the constructed instance's local field should survive"
    );
}
