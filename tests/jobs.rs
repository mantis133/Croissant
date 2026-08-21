//! Per-job cancellation: the job registry, `JobId`/keys, and the `JobEnded` event.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application, JobEnded, JobId, JobOutcome, SpawnError},
    events::{ApplicationEvent, EventHandlerReturn},
};
use futures::stream;

#[derive(Debug)]
struct Start;
impl ApplicationEvent for Start {}

#[derive(Debug)]
struct Stop;
impl ApplicationEvent for Stop {}

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

/// A job long enough that only cancellation can end it inside the test's timeout.
async fn forever() {
    tokio::time::sleep(Duration::from_secs(30)).await;
}

// ---------------------------------------------------------------- cancelling

#[tokio::test]
async fn cancel_stops_a_running_job() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let id = app
                .spawn_with(|emitter| async move {
                    forever().await;
                    emitter.emit(Done(1));
                })
                .expect("the first spawn should be accepted");
            app.set("job", id);
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Stop, app: &mut AppHandle| {
            let id = *app.get::<JobId>("job").unwrap();
            let cancelled = app.cancel(id);
            app.set("cancelled", cancelled);
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Done, app: &mut AppHandle| {
            app.set("reported", true);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start), Box::new(Stop)]))
        .build();

    // Without cancellation the loop would wait 30s for the job rather than fail.
    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("cancelling the only job should let the loop finish");

    assert_eq!(app.values().get::<bool>("cancelled"), Some(&true));
    assert_eq!(
        app.values().get::<bool>("reported"),
        None,
        "a cancelled job should never reach its emit"
    );
}

/// The pending path: spawning is deferred, so a job cancelled in the same callback that
/// started it must be dropped before it ever reaches the runtime.
#[tokio::test]
async fn cancel_a_job_that_never_reached_the_runtime() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ran);

    let home = ActivityBuilder::<Home>::new()
        .on_event(
            move |_state: &mut Home, _event: &Start, app: &mut AppHandle| {
                let flag = Arc::clone(&flag);
                let id = app
                    .spawn(async move {
                        flag.store(true, Ordering::SeqCst);
                    })
                    .expect("spawn should be accepted");
                assert!(app.is_running(id), "the job is live before it is polled");
                assert!(app.cancel(id));
                assert!(!app.is_running(id));
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    assert!(
        !ran.load(Ordering::SeqCst),
        "a job cancelled while queued should never be polled at all"
    );
    assert_eq!(app.handle().job_count(), 0);
}

#[tokio::test]
async fn cancelling_emits_job_ended_immediately() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let id = app.spawn_keyed("work", forever()).unwrap();
            assert!(app.is_key_running("work"));
            app.cancel(id);
            // Queries are consistent within the callback that cancelled, not a turn later.
            assert!(!app.is_running(id));
            assert!(!app.is_key_running("work"));
            app.set("job", id);
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &JobEnded, app: &mut AppHandle| {
            app.set("ended_id", event.id);
            app.set("ended_key", event.key.clone());
            app.set("ended_outcome", event.outcome);
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("the cancelled job should not hold the loop open");

    let expected = *app.values().get::<JobId>("job").unwrap();
    assert_eq!(app.values().get::<JobId>("ended_id"), Some(&expected));
    assert_eq!(
        app.values().get::<Option<String>>("ended_key"),
        Some(&Some(String::from("work")))
    );
    assert_eq!(
        app.values().get::<JobOutcome>("ended_outcome"),
        Some(&JobOutcome::Cancelled)
    );
}

#[tokio::test]
async fn cancel_key_finds_the_job() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            app.spawn_keyed("refresh", forever()).unwrap();
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Stop, app: &mut AppHandle| {
            let cancelled = app.cancel_key("refresh");
            let missing = app.cancel_key("never-existed");
            app.set("cancelled", cancelled);
            app.set("missing", missing);
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &JobEnded, app: &mut AppHandle| {
            app.set("ended_key", event.key.clone());
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start), Box::new(Stop)]))
        .build();

    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("cancel_key should end the job");

    assert_eq!(app.values().get::<bool>("cancelled"), Some(&true));
    assert_eq!(app.values().get::<bool>("missing"), Some(&false));
    assert_eq!(
        app.values().get::<Option<String>>("ended_key"),
        Some(&Some(String::from("refresh")))
    );
}

#[tokio::test]
async fn cancel_all_ends_every_job() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            for key in ["a", "b", "c"] {
                app.spawn_keyed(key, forever()).unwrap();
            }
            app.set("live", app.job_count());
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Stop, app: &mut AppHandle| {
            app.cancel_all();
            app.set("after_cancel", app.job_count());
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &JobEnded, app: &mut AppHandle| {
            assert_eq!(event.outcome, JobOutcome::Cancelled);
            *app.get_or_insert_with("ended", || 0u32) += 1;
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start), Box::new(Stop)]))
        .build();

    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("cancel_all should leave nothing holding the loop open");

    assert_eq!(app.values().get::<usize>("live"), Some(&3));
    assert_eq!(app.values().get::<usize>("after_cancel"), Some(&0));
    assert_eq!(app.values().get::<u32>("ended"), Some(&3));
}

/// The termination-check regression: a `JobEnded` queued as the very last thing the
/// application does must still be dispatched, and must not keep the loop spinning.
#[tokio::test]
async fn cancelling_the_last_job_still_lets_the_loop_finish() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            app.spawn_keyed("only", forever()).unwrap();
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Stop, app: &mut AppHandle| {
            app.cancel_key("only");
            EventHandlerReturn::Consumed
        })
        .on_event(
            |_state: &mut Home, _event: &JobEnded, app: &mut AppHandle| {
                app.set("dispatched", true);
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start), Box::new(Stop)]))
        .build();

    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("the loop should end once its last job is cancelled");

    assert_eq!(
        app.values().get::<bool>("dispatched"),
        Some(&true),
        "the last JobEnded must not be dropped on the way out"
    );
}

// ---------------------------------------------------------------- outcomes

#[tokio::test]
async fn a_completed_job_reports_job_ended_completed() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let id = app.spawn(async {}).unwrap();
            app.set("job", id);
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &JobEnded, app: &mut AppHandle| {
            app.set("ended_id", event.id);
            app.set("ended_outcome", event.outcome);
            *app.get_or_insert_with("ended", || 0u32) += 1;
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    let expected = *app.values().get::<JobId>("job").unwrap();
    assert_eq!(app.values().get::<JobId>("ended_id"), Some(&expected));
    assert_eq!(
        app.values().get::<JobOutcome>("ended_outcome"),
        Some(&JobOutcome::Completed)
    );
    assert_eq!(
        app.values().get::<u32>("ended"),
        Some(&1),
        "each job ends exactly once"
    );
}

#[tokio::test]
async fn a_panicking_job_reports_job_ended_panicked() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            app.spawn_keyed("boom", async { panic!("boom") }).unwrap();
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
        .on_event(|_state: &mut Home, event: &JobEnded, app: &mut AppHandle| {
            if event.key.as_deref() == Some("boom") {
                app.set("boom_outcome", event.outcome);
            }
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
        app.values().get::<JobOutcome>("boom_outcome"),
        Some(&JobOutcome::Panicked)
    );
    assert_eq!(
        app.values().get::<u32>("survivor"),
        Some(&9),
        "the sibling job should still have completed"
    );
}

// ---------------------------------------------------------------- keys

#[tokio::test]
async fn a_keyed_job_rejects_a_second_spawn_under_the_same_key() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let first = app
                .spawn_keyed_with("search", |emitter| async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    emitter.emit(Done(1));
                })
                .expect("the first spawn takes the key");

            let second = app.spawn_keyed("search", async {
                unreachable!("a rejected spawn must never run");
            });

            assert_eq!(
                second,
                Err(SpawnError::KeyInUse {
                    key: String::from("search"),
                    running: first,
                }),
                "the incumbent id should come back with the error"
            );
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &Done, app: &mut AppHandle| {
            app.set("first_finished", event.0);
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
        app.values().get::<u32>("first_finished"),
        Some(&1),
        "rejecting the second spawn must leave the first job alone"
    );
}

#[tokio::test]
async fn a_key_is_reusable_after_its_job_ends() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let id = app.spawn_keyed("slot", forever()).unwrap();
            app.cancel(id);
            EventHandlerReturn::Consumed
        })
        .on_event(
            |_state: &mut Home, _event: &JobEnded, app: &mut AppHandle| {
                // Only react to the first ending, or this recurses forever.
                if app.contains("retaken") {
                    return EventHandlerReturn::Consumed;
                }
                let retaken = app.spawn_keyed("slot", async {}).is_ok();
                app.set("retaken", retaken);
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("the replacement job finishes immediately");

    assert_eq!(
        app.values().get::<bool>("retaken"),
        Some(&true),
        "a key is free once its JobEnded has been dispatched"
    );
}

// ---------------------------------------------------------------- shutdown

#[tokio::test]
async fn spawning_during_teardown_is_refused() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            app.exit();
            EventHandlerReturn::Consumed
        })
        .on_destroy(|_state, app| {
            let refused = app.spawn(async {});
            app.set("refused", refused == Err(SpawnError::Exiting));
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<bool>("refused"),
        Some(&true),
        "work asked for during teardown has no loop left to report to"
    );
}

#[tokio::test]
async fn exit_discards_spawns_queued_alongside_it() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ran);

    let home = ActivityBuilder::<Home>::new()
        .on_event(
            move |_state: &mut Home, _event: &Start, app: &mut AppHandle| {
                let flag = Arc::clone(&flag);
                app.spawn(async move {
                    flag.store(true, Ordering::SeqCst);
                })
                .expect("exiting has not taken effect yet, so this is accepted");
                app.exit();
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    assert!(
        !ran.load(Ordering::SeqCst),
        "a spawn queued behind an exit should never start"
    );
    assert_eq!(app.handle().job_count(), 0);
}

/// `JoinSet::shutdown` kills tasks without reporting them, so without an explicit discard the
/// registry would keep every job that died at exit — lying about what is live and holding
/// their keys hostage for the rest of the process.
#[tokio::test]
async fn the_registry_is_empty_after_a_run_that_exited() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            app.spawn_keyed("long", forever()).unwrap();
            app.exit();
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("exit should abort in-flight work rather than wait for it");

    assert_eq!(
        app.handle().job_count(),
        0,
        "jobs killed by shutdown must not linger in the registry"
    );
    assert!(!app.handle().is_key_running("long"));
    assert!(
        app.handle().spawn_keyed("long", async {}).is_ok(),
        "the key must be free again for a second run"
    );
}

#[tokio::test]
async fn job_ids_are_not_reused_across_runs() {
    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Start, app: &mut AppHandle| {
            let id = app.spawn(async {}).unwrap();
            // Only the first run records one; the second must not collide with it.
            if !app.contains("first_id") {
                app.set("first_id", id);
            } else {
                app.set("second_id", id);
            }
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(producer(vec![Box::new(Start)]))
        .build();

    app.run().await;

    // A fresh producer, since the first one is exhausted.
    app.handle().emit(Start);
    app.run().await;

    let first = *app.values().get::<JobId>("first_id").unwrap();
    let second = *app.values().get::<JobId>("second_id").unwrap();
    assert_ne!(first, second, "ids must not be recycled between runs");
    assert!(!app.handle().is_running(first));
    assert!(!app.handle().is_running(second));
}

/// Everything else here drives the loop with a `stream::iter` producer that ends immediately,
/// which is a very forgiving shape. This one uses a real timer that never runs dry, so the
/// loop only stops because `exit` says so — and a job has to be cancelled out from under a
/// producer that is still talking.
#[tokio::test]
async fn cancellation_works_under_a_producer_that_never_ends() {
    #[derive(Debug)]
    struct Tick;
    impl ApplicationEvent for Tick {}

    let home = ActivityBuilder::<Home>::new()
        .on_event(|_state: &mut Home, _event: &Tick, app: &mut AppHandle| {
            let ticks = app.get_or_insert_with("ticks", || 0u32);
            *ticks += 1;
            match *ticks {
                1 => {
                    app.spawn_keyed_with("poll", |emitter| async move {
                        forever().await;
                        emitter.emit(Done(1));
                    })
                    .expect("the key is free on the first tick");
                }
                2 => {
                    let cancelled = app.cancel_key("poll");
                    app.set("cancelled", cancelled);
                    // The key is free again straight away, so the poll can restart.
                    let restarted = app.spawn_keyed("poll", forever()).is_ok();
                    app.set("restarted", restarted);
                }
                _ => app.exit(),
            }
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, _event: &Done, app: &mut AppHandle| {
            app.set("reported", true);
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut Home, event: &JobEnded, app: &mut AppHandle| {
            assert_eq!(event.outcome, JobOutcome::Cancelled);
            *app.get_or_insert_with("ended", || 0u32) += 1;
            EventHandlerReturn::Consumed
        })
        .build();

    let mut app = Application::builder()
        .add_activity(home)
        .starting_activity::<Home>()
        .add_event_producer(croissant::streams::timer_event_stream(
            Duration::from_millis(10),
            |_instant| Some(Box::new(Tick) as Box<dyn ApplicationEvent>),
        ))
        .build();

    tokio::time::timeout(Duration::from_secs(5), app.run())
        .await
        .expect("the third tick exits");

    assert_eq!(app.values().get::<bool>("cancelled"), Some(&true));
    assert_eq!(app.values().get::<bool>("restarted"), Some(&true));
    assert_eq!(app.values().get::<bool>("reported"), None);
    assert_eq!(
        app.values().get::<u32>("ended"),
        Some(&1),
        "only the cancelled job reports; the restarted one dies with the loop, silently"
    );
    assert_eq!(app.handle().job_count(), 0);
}
