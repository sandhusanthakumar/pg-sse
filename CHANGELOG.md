# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-10-12

### Added

- `revalidate=` on `hub.stream` and `hub.sse`: an async check (type `pg_sse.Revalidate`) that ends a stream with
  `revoked` when it says no. It runs about every `Hub(revalidate_seconds=15)`; a run over `Hub(revalidate_timeout=10)`
  counts as failed.
- `Hub(revalidate_failures=3)`: that many failed checks in a row end the stream with the new goodbye `recheck`
  (`None`: never). `HubStats.revalidate_errors_total` counts failed checks.
- `pg_sse.event_stream_responses()` gives FastAPI's `responses=` for a stream route (use it with
  `response_class=StreamingResponse`), so the OpenAPI document shows `text/event-stream` and the no-cache headers.
- `on_error` may take a third argument, `on_error(key, exc, info)`, the stream's `StreamInfo` with its `owner` and
  `age_seconds` (`pg_sse.ErrorHandler` types it). A two-argument `on_error` is called as before.
- `Hub(log_owner=True)` adds the owner to the log line for a failed stream. Off by default.

### Changed

- `Hub(on_error=...)` raises `TypeError` when the hub is made if the function takes neither `(key, exc)` nor
  `(key, exc, info)`; it used to fail at its first report. The failed-stream log line also says how long it was open.
- `EndedTotals` has a `recheck` field and `HubStats` a `revalidate_errors_total` field, so `dataclasses.asdict` of the
  stats has two new keys.

### Fixed

- On Python 3.14, a `read(after, wake: Wake)` annotated with a name imported only for type checking raised `NameError`
  when its stream was made, instead of getting its `Wake`.

## [0.3.0] - 2026-10-11

### Added

- `hub.close(owner=..., key=...)` ends this process's streams opened with that owner and/or on that key, with the
  goodbye reason `revoked`, so a removed operator or revoked session does not keep a stream until `max_stream_seconds`.
  A read or `start` that is running when it is called is cut short and its goodbye goes out at once.
- `Hub(resync_spread=2.0)`: after the listener connects or reconnects, each stream waits a random 0 to that many
  seconds before its re-read, so a database that has just come back is not hit by every stream at once. `0` reads at
  once, as before.
- `Hub(on_error=...)`: `on_error(key, exc)` is called when a stream fails, for an error tracker. A slow
  tracker holds up neither the response nor other streams.
- `Hub(read_executor=...)`: an executor for plain `read` and `start` functions, instead of asyncio's shared pool.
- `Hub(application_name=...)` names the listener connection in `pg_stat_activity` (default `pg-sse-listener`).
- `hub.stream(..., max_stream_seconds=...)` and `hub.sse(...)` override the hub's age limit for one stream.
- `hub.streams()` lists every open stream as `StreamInfo(key, owner, age_seconds, ended)`, for an admin view.
- `HubStats.ended_total` counts goodbyes by reason (`max_age`, `shutdown`, `revoked`, `behind`), and
  `HubStats.read_errors_total` counts failed streams.
- `pg_sse.signal_read(...)` builds the `read(cursor, wake)` for streams that tell the page what to refetch: an event
  name for the start, a resync, notified keys and each signal. A malformed argument raises when it is called.
- `Wake.signal_keys` maps each signal to the keys it was sent for.
- `pg_sse.IntCursor(parts=1, *, sep=".", max_digits=15)` parses and formats an integer cursor such as `42` or `12.3`
  from a client: ASCII digits only, `ValueError` for anything else, `None` for no cursor. `IntCursor.parse_one(text)` is the
  one-part shortcut for `hub.sse(cursor=...)`.
- `Ephemeral.reset()` forgets every key and signal without waking any stream, for test fixtures.

### Changed

- Every re-read after a listener connect or reconnect is delayed by up to `resync_spread` (2 s by default). Set it to
  `0` where a test needs it at once.
- A stream takes its slot when `hub.stream` or `hub.sse` is called, not when its first frame is read, so the caps are
  exact and the `busy` goodbye is gone. A stream that is never read gives its slot back when it is garbage-collected.
  One that was closed or dropped keeps its slot until it has said goodbye (10 seconds at most).
- A collection or raw bytes as a `cursor`, a `start()` result or an `Event` id raises `TypeError`; it used to be sent
  as `id: (12, 3)`. A text cursor with a line break or NUL starts the stream from now. For a tuple, pass `parsed[0]`
  or `IntCursor.format(*parsed)`.
- An int `owner` is turned into text, so `close(owner=7)` finds a stream opened with `owner=7` or `"7"`. Any other
  non-`str` owner (a `UUID`, a `bool`) raises `TypeError`; use `str(owner)`.
- `Subscription.queue` is private: read a `hub.subscribe` queue with `subscription.wait()`. `Subscription.ended` is
  read-only; end one with `subscription.close()` and `hub.unsubscribe(subscription)`.
- After `hub.lifespan()` has exited, `hub.stream` and `hub.subscribe` raise `TooManySubscribers` (a 503 from
  `hub.sse`), as during shutdown, instead of `RuntimeError`. A hub that never ran still raises `RuntimeError`.
- `hub.close_all()` is safe from any thread.
- `Hub(end_event=...)`, `end_event` and `listen_timeout` are checked when given, not at the first goodbye.
- A stream closed while its `read` runs says goodbye at once, and a failure of that read is not reported as an error.
- `hub.wake` and `Ephemeral` refuse a signal that is not a `str`.
- Through the hub, `distinct` runs a sync `read` in `read_executor`.

### Fixed

- `max_stream_seconds`, `reread_seconds` and `ping_seconds` refuse NaN, which used to switch the limit or the re-read
  off silently. A `Decimal` duration is accepted and stored as `float`; it used to fail inside every stream.
- A stream created before the hub stopped and read after it started again says goodbye with reason `shutdown`; it used
  to subscribe to the new run.
- `Ephemeral` refuses a NaN or infinite `ttl` or `grace`, or a `bool`.
- The README and `examples/chat.py` parsed `Last-Event-ID` with `isdigit()` and `int()`, and `"²"` passes the first and
  fails the second, so a client could make the stream route return a 500. They use `IntCursor` now.
- `trigger_sql` accepted a name with a trailing newline (`"room\n"`) because the check used `$`.

## [0.2.0] - 2026-10-10

### Added

- `Hub(max_total_per_owner=...)`: a cap on one owner's open streams across all keys (off by default). `max_per_owner`
  counts one key at a time, so without a total an owner can hold that many streams on every key it can name, up to
  `max_subscribers`.
- `trigger_sql(..., on=("insert",))` and `update_of=(...)` limit which writes notify, so a table with edits, read
  receipts or soft deletes does not wake every reader for a read that returns nothing. The defaults are unchanged
  (insert, update and delete); running the SQL again changes an installed trigger.
- `Hub(conninfo=...)` may be a function returning the DSN. It is called on every connect, so a rotating password or
  settings that are only readable at startup need no rebuilt hub; a function that raises is retried with backoff like
  a failed connect.
- `hub.call_later(seconds, callback, *args)` runs a callback on the hub's event loop after a delay, from any thread:
  follow-up work on ephemeral state, such as checking that a typing signal lapsed and then calling `hub.wake`.
- `hub.sse(..., refuse_with=...)`: a function that gets the `TooManySubscribers` and returns the exception to raise
  instead of the 503, e.g. one an error tracker is set to ignore.
- `pg_sse.Read` and `pg_sse.Start`, the types of the `read` and `start` arguments, are public.
- `examples/inbox.py`: a runnable signal-stream example (inbox, typing indicator with `hub.ephemeral`).
- `hub.ephemeral(name, *, ttl, ...)` and `pg_sse.Ephemeral`: per-key signals that lapse unless renewed (typing,
  presence). `set(key, who)` renews or starts a signal and `set(key, who, on=False)` or `clear(key)` removes it; the
  key's streams are woken (with `hub.wake`) when what a reader sees changes and when a signal lapses, so a closed tab
  never leaves a stale "typing" on another screen. `live(key)` lists who has not lapsed. State is in this process's
  memory.
- `pg_sse.distinct(read, key)`: wraps a `read` so a snapshot is sent only when it changed. It drops an event whose
  key equals the last one sent; `None` and `final` events are always sent, and the event after a `None` is compared
  with nothing, so it is sent too. State is per stream, so call it inside the route. A dropped event's `id` is never
  applied: drop only events that would not advance the cursor.
- `pg_sse.testing`: `next_frame`, `next_event` (skips pings and id-only frames), `goodbye`, `ended` and `until` for
  reading `hub.stream` in tests, and `decode` and `Frame` to parse an SSE frame. No test framework or database needed.

### Changed

- A notification reaches its streams through an index by key, so fan-out costs the streams it reaches, not every open
  stream. Before, each notification scanned all of them.
- `hub.stream`, `hub.sse`, `hub.subscribe` and `hub.wake` raise a `TypeError` for a key that is not a `str`. A
  notification payload is text, so an int (say, a FastAPI path parameter) used to subscribe fine and never be woken,
  with nothing logged. A key that is not text now fails where it is passed; use `str(room_id)`.
- The order trigger no longer searches the catalogs for the sequence on every insert, while it holds the key's lock:
  `trigger_sql` resolves it once, when the SQL runs, and writes its name into the function. A trigger installed by
  0.1.0 keeps working; run `trigger_sql` again to get the faster one. If you rename the sequence or move it to another
  schema, run it again then too.
- The version is `__version__` in `src/pg_sse/__init__.py` and nowhere else: `pyproject.toml` reads it at build
  time, so the two can no longer drift apart.
- Leaving `hub.lifespan` forgets every subscription, including `hub.subscribe` ones never unsubscribed, so a hub that
  is stopped and started again (tests on fresh event loops) starts clean.

### Removed

- `CLOSING`, `DROPPED` and `RESYNC` are no longer exported from `pg_sse` (they were in 0.1.0). They are internal queue
  markers that no documentation mentioned. A custom loop on `hub.subscribe` reads `subscription.ended` and treats
  any `("marker", ...)` item as "re-read everything"; the README now says so.

## [0.1.0] - 2026-10-10

First release.

- `Hub`: one LISTEN connection per process, fanned out to bounded per-stream queues, with caps, a stream age limit,
  pings, periodic re-reads and reconnect with backoff.
- `hub.stream` yields encoded SSE frames for any async framework; `hub.sse` wraps it as a Starlette or FastAPI
  response (`pg-sse[starlette]`, `pg-sse[fastapi]`).
- `trigger_sql` and `drop_trigger_sql`: a notify trigger per table, and an optional ordering trigger that keeps ids in
  commit order within a key. The objects are named `"pg_sse$notify$<table>$<key>"` and
  `"pg_sse$order$<table>$<order_column>"`; `$` cannot appear in a validated identifier, so two tables never share a
  name.
- `Event`, `hub.wake`, `hub.stats` and `hub.subscribe` for custom loops.
- `read(cursor, wake)`: a `read` whose second parameter is named `wake`, or is required, gets a `Wake` saying why it
  runs (`start`, `keys`, `signals`, `resync`, `reread`), for streams that tell the client what to refetch instead of
  sending rows. A second parameter with a default under another name (`read(after, limit=500)`) is left alone, and so
  is `*args`.
- `hub.wake(key, signal="wake", *, all_keys=True)`: a label the `Wake` carries, and `all_keys=False` to leave the
  all-keys streams alone for ephemeral per-key state.
- A goodbye before a planned close: `retry:` always (1 s for `max_age` and `behind`, 1-5 s jittered for `shutdown`,
  30 s for `busy`) and, with `Hub(end_event=...)` or `stream(end_event=...)`, a named event with `{"reason": ...}`.
- `Hub(listen_timeout=5.0)`: `lifespan` waits up to that long for the first LISTEN so a health check and the first
  requests see a live listener. It never fails startup; `None` does not wait. Every stream re-reads when the listener
  connects or reconnects, so a write committed before its LISTEN existed is never missed.
- The listener's connect has a 10 s timeout (unless the DSN sets `connect_timeout`) and its queries a 30 s statement
  timeout.
- `lifespan` chains onto an existing SIGTERM/SIGINT handler; a default action is left alone, so a script with no
  server still ends on SIGTERM.
- Identical wake-ups are queued once per stream, so a hot key costs one queue slot. A stream falls behind only when more
  than `queue_size` distinct keys or signals arrive during one read.
- Python 3.11+, Postgres 14+, psycopg 3.

[Unreleased]: https://github.com/sansanth/pg-sse/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/sansanth/pg-sse/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/sansanth/pg-sse/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/sansanth/pg-sse/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sansanth/pg-sse/releases/tag/v0.1.0
