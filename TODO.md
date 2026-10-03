# TODO

- [ ] **A draw hook, maybe.** `post_event` is the draw hook today, kept generic so headless
      applications are not forced into a view concept. Revisit only if that proves awkward in
      a real UI.

## Decided against

**Command-line argument handling** — out of scope. Parsing argv touches nothing the framework
owns, and any wrapper would be strictly less capable than `clap`. The integration is already
one line:

```rust
let args = Args::parse();
Application::builder().service(Arc::new(args))   // then #[inject] args: Injected<Args>
```

Worth a docs section, not code.

**Configuration file parsing** — same reasoning; `config`/`figment` do it better, and the
result reaches the app through `.service(..)` or `.value(..)`.

The half that *was* framework-shaped is done: a file watcher is an event producer and a
config change is an event, so `file_event_stream` / `directory_event_stream` now exist behind
the `watch` feature. See [docs/events.md](docs/events.md#built-in-producers).

**Logging** — removed, `logging` feature and all. Users bring their own `tracing` or `log`
setup, and the framework's own lines would only clutter their files. Installing a subscriber
happens in `main` before `run()`, so it was always theirs anyway — see the principle below.

## The principle

Croissant owns the event loop, not the process. The user still owns `main`, so anything that
happens before `run()` — parsing argv, reading a file, installing a subscriber — is theirs.
The framework should only absorb something when its data has to flow through
framework-managed channels: the store, injection, or the event loop.
