//! `AppHandle::spawn` and the `Emitter` round-trip back into the event loop.

use std::time::Duration;

use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application},
    events::{ApplicationEvent, EventHandlerReturn},
};
use futures::stream;

#[derive(Debug)]
struct Start;
impl ApplicationEvent for Start {}

#[derive(Debug)]
struct Done(u32);
impl ApplicationEvent for Done {}

fn producer(
    events: Vec<Box<dyn ApplicationEvent>>,
) -> impl stream::Stream<Item = Box<dyn ApplicationEvent>> + Send {
    stream::iter(events)
}

#[derive(Debug, Default)]
struct Home;
impl ManagedState for Home {}

#[derive(Debug, Default)]
struct Other;
impl ManagedState for Other {}

#[tokio::test]
async fn a_spawned_future_reports_back_through_the_emitter() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let _ = app.spawn_with(|emitter| async move {
                emitter.emit(Done(7));
            });
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &Done, app: &mut AppHandle| {
            app.set("done_with", event.0);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<u32>("done_with"),
        Some(&7),
        "the loop should have drained the spawned result before returning"
    );
}

#[tokio::test]
async fn slow_work_still_lands_after_the_producers_run_dry() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let _ = app.spawn_with(|emitter| async move {
                // Outlives the producer stream, which ends immediately.
                tokio::time::sleep(Duration::from_millis(50)).await;
                emitter.emit(Done(1));
            });
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Done, app: &mut AppHandle| {
            app.set("landed", true);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<bool>("landed"), Some(&true));
}

/// The case that motivated the feature: work started by one activity finishes after the user
/// has navigated away, and is still handled.
#[tokio::test]
async fn work_survives_the_activity_that_started_it() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let _ = app.spawn_with(|emitter| async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                emitter.emit(Done(42));
            });
            // Leave immediately: Home is destroyed while the work is still running.
            app.replace::<Other>();
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Done, app: &mut AppHandle| {
            app.set("handled_by", "home");
            EventHandlerReturn::Consumed
        })
        .build();

    let other = ActivityBuilder::<Other>::new()
        .on_destroy(|_state, app| app.set("other_destroyed", true))
        .on_event(|_state: &mut Other, event: &Done, app: &mut AppHandle| {
            app.set("handled_by", "other");
            app.set("payload", event.0);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .add_activity(other)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<&str>("handled_by").copied(),
        Some("other"),
        "the result should reach whichever activity is active when it lands"
    );
    assert_eq!(app.values().get::<u32>("payload"), Some(&42));
    assert_eq!(app.values().get::<bool>("other_destroyed"), Some(&true));
}

#[tokio::test]
async fn many_concurrent_jobs_all_land() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            // Staggered so they cannot finish in submission order by accident.
            for id in 0..5u32 {
                let _ = app.spawn_with(move |emitter| async move {
                    tokio::time::sleep(Duration::from_millis(u64::from(5 - id) * 10)).await;
                    emitter.emit(Done(id));
                });
            }
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &Done, app: &mut AppHandle| {
            app.get_or_insert_with("landed", Vec::<u32>::new)
                .push(event.0);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    let mut landed = app.values().get::<Vec<u32>>("landed").cloned().unwrap();
    assert_eq!(landed.len(), 5, "every job should report");
    landed.sort_unstable();
    assert_eq!(landed, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn exit_aborts_work_still_in_flight() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let _ = app.spawn_with(|emitter| async move {
                // Far longer than the test should take; abort must cut it short.
                tokio::time::sleep(Duration::from_secs(30)).await;
                emitter.emit(Done(0));
            });
            app.exit();
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    // If exit did not abort the sleep, this would hang for 30s rather than fail.
    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("exit should abort in-flight work rather than wait for it");
}

#[tokio::test]
async fn a_panicking_job_does_not_bring_the_application_down() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let _ = app.spawn(async {
                panic!("boom");
            });
            let _ = app.spawn_with(|emitter| async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                emitter.emit(Done(9));
            });
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &Done, app: &mut AppHandle| {
            app.set("survivor", event.0);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<u32>("survivor"),
        Some(&9),
        "the sibling job should still have completed"
    );
}

#[tokio::test]
async fn an_emitter_reports_a_closed_loop() {
    let mut app = Application::builder()
        .add_activity(ActivityBuilder::<Home>::new().build())
        .starting_activity::<Home>()
        .build();

    let emitter = app.handle().emitter();
    assert!(emitter.is_open());
    assert!(emitter.emit(Done(1)));

    drop(app);
    assert!(!emitter.is_open());
    assert!(!emitter.emit(Done(2)), "emit should report the closed loop");
}
