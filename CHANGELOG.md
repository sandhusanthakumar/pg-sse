# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.1.0

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
