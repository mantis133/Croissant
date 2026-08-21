//! Always-active [`Task`]s: stateful handlers with no screen, shared across activities.

use std::sync::Arc;

use croissant::{
    ManagedState,
    activities::ActivityBuilder,
    application::{AppHandle, Application, Injected},
    events::{ApplicationEvent, EventHandlerReturn},
    tasks::TaskBuilder,
};
use futures::stream;

#[derive(Debug)]
struct Quit;
impl ApplicationEvent for Quit {}

#[derive(Debug)]
struct Tick;
impl ApplicationEvent for Tick {}

#[derive(Debug)]
struct Go;
impl ApplicationEvent for Go {}

fn producer(
    events: Vec<Box<dyn ApplicationEvent>>,
) -> impl stream::Stream<Item = Box<dyn ApplicationEvent>> + Send {
    stream::iter(events)
}

#[derive(Debug, Default)]
struct First;
impl ManagedState for First {}

#[derive(Debug, Default)]
struct Second;
impl ManagedState for Second {}

/// The headline case: `Quit` is handled identically no matter which activity is in front,
/// without either activity registering a handler for it.
#[tokio::test]
async fn a_task_handles_events_across_activity_changes() {
    #[derive(Debug, Default, ManagedState)]
    struct Shortcuts {
        #[global]
        quits_seen: u32,
    }

    let shortcuts = TaskBuilder::<Shortcuts>::new()
        .on_event(
            |task: &mut Shortcuts, _event: &Quit, _app: &mut AppHandle| {
                task.quits_seen += 1;
                EventHandlerReturn::Consumed
            },
        )
        .build();

    // Neither activity knows anything about Quit.
    let first = ActivityBuilder::<First>::new()
        .on_event(|_state: &mut First, _event: &Go, app: &mut AppHandle| {
            app.push::<Second>();
            EventHandlerReturn::Consumed
        })
        .build();
    let second = ActivityBuilder::<Second>::new().build();

    let mut app = Application::builder()
        .add_task(shortcuts)
        .add_activity(first)
        .add_activity(second)
        .starting_activity::<First>()
        .backstack()
        .add_event_producer(producer(vec![
            Box::new(Quit), // while First is in front
            Box::new(Go),   // navigate
            Box::new(Quit), // while Second is in front
            Box::new(Quit),
        ]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<u32>("quits_seen"),
        Some(&3),
        "the task should have handled every Quit regardless of the active activity"
    );
}

#[tokio::test]
async fn an_activity_handles_first_and_a_task_is_the_fallback() {
    #[derive(Debug, Default, ManagedState)]
    struct Fallback {
        #[global]
        fell_through: u32,
    }

    let fallback = TaskBuilder::<Fallback>::new()
        .on_event(|task: &mut Fallback, _event: &Tick, _app: &mut AppHandle| {
            task.fell_through += 1;
            EventHandlerReturn::Consumed
        })
        .build();

    // First consumes Tick itself; Second does not, so its Ticks reach the task.
    let first = ActivityBuilder::<First>::new()
        .on_event(|_state: &mut First, _event: &Tick, app: &mut AppHandle| {
            *app.get_or_insert_with("handled_by_activity", || 0u32) += 1;
            EventHandlerReturn::Consumed
        })
        .on_event(|_state: &mut First, _event: &Go, app: &mut AppHandle| {
            app.replace::<Second>();
            EventHandlerReturn::Consumed
        })
        .build();
    let second = ActivityBuilder::<Second>::new().build();

    let mut app = Application::builder()
        .add_task(fallback)
        .add_activity(first)
        .add_activity(second)
        .starting_activity::<First>()
        .add_event_producer(producer(vec![
            Box::new(Tick),
            Box::new(Go),
            Box::new(Tick),
            Box::new(Tick),
        ]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("handled_by_activity"), Some(&1));
    assert_eq!(
        app.values().get::<u32>("fell_through"),
        Some(&2),
        "only the Ticks the activity ignored should reach the task"
    );
}

#[tokio::test]
async fn a_consuming_task_stops_the_application_handler_but_not_post_event() {
    #[derive(Debug, Default, ManagedState)]
    struct Gate {
        #[global]
        post_seen: u32,
    }

    let gate = TaskBuilder::<Gate>::new()
        .on_event(|_task: &mut Gate, _event: &Tick, _app: &mut AppHandle| {
            EventHandlerReturn::Consumed
        })
        .post_event(|task: &mut Gate, _event, _app| task.post_seen += 1)
        .build();

    let mut app = Application::builder()
        .add_task(gate)
        .add_activity(ActivityBuilder::<First>::new().build())
        .starting_activity::<First>()
        .on_event(|_event: &Tick, app: &mut AppHandle| {
            *app.get_or_insert_with("reached_app", || 0u32) += 1;
            EventHandlerReturn::Ignored
        })
        .add_event_producer(producer(vec![Box::new(Tick), Box::new(Tick)]))
        .build();

    app.run().await;

    assert!(
        !app.values().contains("reached_app"),
        "the task consumed both events"
    );
    assert_eq!(
        app.values().get::<u32>("post_seen"),
        Some(&2),
        "post_event runs even for events the task itself consumed"
    );
}

#[tokio::test]
async fn tasks_start_before_the_first_activity_is_created() {
    #[derive(Debug, Default, ManagedState)]
    struct Seeder {
        #[global]
        base: u32,
    }

    let seeder = TaskBuilder::<Seeder>::new()
        .on_start(|task: &mut Seeder, app: &mut AppHandle| {
            task.base = 100;
            app.get_or_insert_with("order", Vec::<&'static str>::new)
                .push("task:start");
        })
        .on_stop(|_task, app| {
            app.get_or_insert_with("order", Vec::<&'static str>::new)
                .push("task:stop");
        })
        .build();

    #[derive(Debug, Default, ManagedState)]
    struct Screen {
        #[global]
        base: u32,
    }

    let screen = ActivityBuilder::<Screen>::new()
        .on_create(|state: &mut Screen, app: &mut AppHandle| {
            app.get_or_insert_with("order", Vec::<&'static str>::new)
                .push("activity:create");
            // Reads the value the task seeded moments earlier.
            app.set("base_seen_by_activity", state.base);
        })
        .build();

    let mut app = Application::builder()
        .add_task(seeder)
        .add_activity(screen)
        .starting_activity::<Screen>()
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<u32>("base_seen_by_activity"),
        Some(&100),
        "the activity should see what the task seeded"
    );
    assert_eq!(
        app.values().get::<Vec<&'static str>>("order").unwrap(),
        &vec!["task:start", "activity:create", "task:stop"]
    );
}

// ---------------------------------------------------------------- tasks + services + spawn

trait Fetcher: Send + Sync {
    fn fetch(&self, id: u32) -> String;
}

struct FakeFetcher;
impl Fetcher for FakeFetcher {
    fn fetch(&self, id: u32) -> String {
        format!("payload-{id}")
    }
}

#[derive(Debug)]
struct StartJob(u32);
impl ApplicationEvent for StartJob {}

#[derive(Debug)]
struct JobDone {
    id: u32,
    body: String,
}
impl ApplicationEvent for JobDone {}

/// A task supervising several concurrent spawned jobs and correlating them by id — the
/// combination that replaces a registry of live task instances.
#[tokio::test]
async fn a_task_supervises_many_concurrent_jobs() {
    #[derive(Debug, Default, ManagedState)]
    struct Downloader {
        #[inject]
        fetcher: Injected<dyn Fetcher>,
        #[global]
        completed: u32,
        in_flight: Vec<u32>,
    }

    let downloader = TaskBuilder::<Downloader>::new()
        .on_event(
            |task: &mut Downloader, event: &StartJob, app: &mut AppHandle| {
                task.in_flight.push(event.0);
                // The injected service does the work, off the event loop.
                let body = task.fetcher.fetch(event.0);
                let id = event.0;
                let _ = app.spawn_with(move |emitter| async move {
                    tokio::task::yield_now().await;
                    emitter.emit(JobDone { id, body });
                });
                EventHandlerReturn::Consumed
            },
        )
        .on_event(
            |task: &mut Downloader, event: &JobDone, app: &mut AppHandle| {
                task.in_flight.retain(|id| *id != event.id);
                task.completed += 1;
                app.get_or_insert_with("bodies", Vec::<String>::new)
                    .push(event.body.clone());
                app.set("still_in_flight", task.in_flight.len());
                EventHandlerReturn::Consumed
            },
        )
        .build();

    let mut app = Application::builder()
        .service::<dyn Fetcher>(Arc::new(FakeFetcher))
        .add_task(downloader)
        .add_activity(ActivityBuilder::<First>::new().build())
        .starting_activity::<First>()
        .add_event_producer(producer(vec![
            Box::new(StartJob(1)),
            Box::new(StartJob(2)),
            Box::new(StartJob(3)),
        ]))
        .build();

    app.run().await;

    assert_eq!(app.values().get::<u32>("completed"), Some(&3));
    assert_eq!(app.values().get::<usize>("still_in_flight"), Some(&0));

    let mut bodies = app.values().get::<Vec<String>>("bodies").cloned().unwrap();
    bodies.sort();
    assert_eq!(bodies, vec!["payload-1", "payload-2", "payload-3"]);
}

#[tokio::test]
async fn tasks_dispatch_in_registration_order() {
    #[derive(Debug, Default, ManagedState)]
    struct Recorder;

    fn recorder(label: &'static str, consume: bool) -> croissant::tasks::Task<Recorder> {
        TaskBuilder::<Recorder>::new()
            .on_event(
                move |_task: &mut Recorder, _event: &Tick, app: &mut AppHandle| {
                    app.get_or_insert_with("order", Vec::<&'static str>::new)
                        .push(label);
                    if consume {
                        EventHandlerReturn::Consumed
                    } else {
                        EventHandlerReturn::Ignored
                    }
                },
            )
            .build()
    }

    let mut app = Application::builder()
        .add_task(recorder("first", false))
        .add_task(recorder("second", true))
        .add_task(recorder("third", false))
        .add_activity(ActivityBuilder::<First>::new().build())
        .starting_activity::<First>()
        .add_event_producer(producer(vec![Box::new(Tick)]))
        .build();

    app.run().await;

    assert_eq!(
        app.values().get::<Vec<&'static str>>("order").unwrap(),
        &vec!["first", "second"],
        "dispatch follows registration order and stops at the consumer"
    );
}
