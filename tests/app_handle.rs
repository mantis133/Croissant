//! End-to-end checks that an activity can reach the application through its `AppHandle`.

use croissant::{
    activities::{ActivityBuilder, ActivityState},
    application::Application,
    events::{ApplicationEvent, EventHandlerReturn},
};
use futures::stream;

#[derive(Debug, Default)]
struct Home;
impl ActivityState for Home {}

#[derive(Debug, Default)]
struct Details;
impl ActivityState for Details {}

#[derive(Debug)]
struct Ping;
impl ApplicationEvent for Ping {}

#[derive(Debug)]
struct Pong;
impl ApplicationEvent for Pong {}

/// A producer that yields `events` and then ends, which stops the loop on its own.
fn producer(
    events: Vec<Box<dyn ApplicationEvent>>,
) -> impl stream::Stream<Item = Box<dyn ApplicationEvent>> + Send {
    stream::iter(events)
}

#[tokio::test]
async fn named_values_are_shared_between_activities() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Ping, app| {
            *app.get_or_insert_with("counter", || 0u32) += 1;
            app.set("visited_home", true);
            app.push::<Details>();
            EventHandlerReturn::Consumed
        })
        .build();

    let details = ActivityBuilder::<Details>::new()
        // Reads a value written by a different activity, and writes one of its own.
        .on_resume(|_state, app| {
            let counter = *app.get::<u32>("counter").expect("home wrote counter");
            app.set("counter_seen_by_details", counter);
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .add_activity(details)
        .starting_activity::<Home>()
        .backstack()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("counter"), Some(&1));
    assert_eq!(app.values().get::<bool>("visited_home"), Some(&true));
    assert_eq!(
        app.values().get::<u32>("counter_seen_by_details"),
        Some(&1),
        "Details should have read the value Home wrote"
    );
}

#[tokio::test]
async fn emitted_events_are_dispatched_before_the_next_producer_event() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Ping, app| {
            app.get_or_insert_with("order", Vec::<&'static str>::new)
                .push("ping");
            app.emit(Pong);
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Pong, app| {
            app.get_or_insert_with("order", Vec::<&'static str>::new)
                .push("pong");
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Ping), Box::new(Ping)]))
        .build();

    app.run().await;

    // Each emitted Pong jumps ahead of the second Ping still sitting in the producer.
    assert_eq!(
        app.values().get::<Vec<&'static str>>("order").unwrap(),
        &vec!["ping", "pong", "ping", "pong"]
    );
}

#[tokio::test]
async fn push_and_pop_move_across_the_backstack() {
    let home = ActivityBuilder::<Home>::new()
        .on_resume(|_state, app| {
            app.get_or_insert_with("trail", Vec::<&'static str>::new)
                .push("home:resume");
        })
        .on_pause(|_state, app| {
            app.get_or_insert_with("trail", Vec::<&'static str>::new)
                .push("home:pause");
        })
        .on_event(|_state: &mut Home, _event: &Ping, app| {
            app.push::<Details>();
            EventHandlerReturn::Consumed
        })
        .build();

    let details = ActivityBuilder::<Details>::new()
        .on_resume(|_state, app| {
            app.get_or_insert_with("trail", Vec::<&'static str>::new)
                .push("details:resume");
            // Immediately hand control back to Home.
            app.pop();
        })
        .on_pause(|_state, app| {
            app.get_or_insert_with("trail", Vec::<&'static str>::new)
                .push("details:pause");
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .add_activity(details)
        .starting_activity::<Home>()
        .backstack()
        .add_event_producer(producer(vec![Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<Vec<&'static str>>("trail").unwrap(),
        &vec![
            "home:resume",    // starting activity
            "home:pause",     // push(Details)
            "details:resume", // ...which pops straight back
            "details:pause",
            "home:resume",
            "home:pause", // loop ends, active activity paused
        ]
    );
}

#[tokio::test]
async fn exit_stops_the_loop_early() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Ping, app| {
            *app.get_or_insert_with("seen", || 0u32) += 1;
            app.exit();
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![
            Box::new(Ping),
            Box::new(Ping),
            Box::new(Ping),
        ]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<u32>("seen"),
        Some(&1),
        "the loop should stop after the first Ping"
    );
}

#[tokio::test]
async fn consumed_events_skip_the_application_handler() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, event: &Ping, app| {
            let _ = event;
            match app.get::<u32>("count").copied().unwrap_or(0) {
                // Consume the first Ping, ignore the second.
                0 => {
                    app.set("count", 1u32);
                    EventHandlerReturn::Consumed
                }
                _ => EventHandlerReturn::Ignored,
            }
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .on_event(|_event: &Ping, app| {
            *app.get_or_insert_with("reached_app_handler", || 0u32) += 1;
            EventHandlerReturn::Ignored
        })
        .add_event_producer(producer(vec![Box::new(Ping), Box::new(Ping)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<u32>("reached_app_handler"),
        Some(&1),
        "only the unconsumed Ping should reach the application handler"
    );
}

#[tokio::test]
async fn wrong_type_reads_as_a_miss_and_take_removes() {
    let mut app = Application::builder()
        .value("port", 8080u16)
        .add_activity(ActivityBuilder::<Home>::new().build())
        .starting_activity::<Home>()
        .build();

    let handle = app.handle();
    assert_eq!(handle.get::<u16>("port"), Some(&8080));
    assert_eq!(handle.get::<u32>("port"), None, "right name, wrong type");
    assert_eq!(handle.get::<u16>("missing"), None);

    assert_eq!(handle.take::<u32>("port"), None, "type mismatch leaves it");
    assert!(handle.contains("port"));
    assert_eq!(handle.take::<u16>("port"), Some(8080));
    assert!(!handle.contains("port"));
}
