# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- README: SQLAlchemy and Alembic snippets (run migrations on the psycopg driver: asyncpg rejects the multi-statement
  SQL), the API reference as one section per name, a Logging section, and a link to `llms.txt`.
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
- README: the priority a signal stream's `read` should answer causes in, an async `read` example, a table of the
  frames a stream yields, and how to test a stream with `async with hub.lifespan():` and `pg_sse.testing`.

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

[Unreleased]: https://github.com/sandhusanthakumar/pg-sse/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/sandhusanthakumar/pg-sse/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sandhusanthakumar/pg-sse/releases/tag/v0.1.0
