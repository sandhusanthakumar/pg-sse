# pg-sse

[![CI](https://github.com/sandhusanthakumar/pg-sse/actions/workflows/ci.yml/badge.svg)](https://github.com/sandhusanthakumar/pg-sse/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pg-sse)](https://pypi.org/project/pg-sse/)
[![Python versions](https://img.shields.io/pypi/pyversions/pg-sse)](https://pypi.org/project/pg-sse/)
[![License](https://img.shields.io/pypi/l/pg-sse)](LICENSE)

Live updates from the Postgres you already run. No Redis, no broker, no WebSocket service: a table trigger sends `NOTIFY`, one listener per process wakes the open streams, and each stream re-reads from its cursor and sends server-sent events. Works with Starlette and FastAPI out of the box, and with any other framework through `hub.stream`.

```sh
pip install "pg-sse[starlette]"   # or "pg-sse[fastapi]"; plain "pg-sse" is enough for hub.stream
pip install "psycopg[binary]"     # on a machine without libpq; see the psycopg install docs for the alternatives
```

## The one rule

A notification carries only a key (a room id, a conversation id) and means **"something changed here, re-read from your cursor"**. It never carries the data. So a lost notification costs a delay, never an event, and there's no 8000-byte payload limit to hit.

## Why not X

- **Polling.** Every client hits the database on a timer, so load grows with clients times frequency, and latency is half the interval. Here an idle stream costs one read a minute, and a write reaches clients within milliseconds.
- **WebSockets.** You usually end up running a second service and writing your own reconnect and catch-up logic. `EventSource` reconnects by itself and sends `Last-Event-ID`, and one-way updates are all most pages need.
- **Redis pub/sub.** It is another system to run, secure and keep consistent with your database, and a message missed during a disconnect is gone. Here the database is the source of truth and a missed wake-up costs a delay.
- **Supabase Realtime.** It ties you to one vendor's platform and its protocol. This runs against any Postgres 14+ you can open a direct connection to.

## Use it

**1. Once, in a migration:** wire the table to a channel.

```python
from pg_sse import trigger_sql

conn.execute(trigger_sql("messages", key_column="room", channel="room_change", order_column="id"))
```

That creates two triggers:

| Trigger | Why |
|---|---|
| `pg_notify('room_change', room)` after every insert, update and delete | No writer can forget it: not a cron job, not a psql session. Delivered on commit; a rollback announces nothing. |
| With `order_column` (a serial, bigserial or identity column): ids follow commit order within a key | Without it, a reader at cursor 11 can skip id 10 committing late. An insert locks its key until it commits, then draws the next id (see Limits for the cost). |

**2. The app:**

```python
from typing import Annotated
from fastapi import FastAPI, Header
from pg_sse import Event, Hub

hub = Hub(DIRECT_DATABASE_URL, channel="room_change")  # a direct connection, not a pooled one
app = FastAPI(lifespan=hub.lifespan)


def read_after(room: str, after: int) -> list[Event]:
    rows = db.fetch("SELECT id, body FROM messages WHERE room = %s AND id > %s ORDER BY id LIMIT 500", (room, after))
    return [Event({"id": id, "body": body}, id=id) for id, body in rows]


def latest_id(room: str) -> int:
    return db.fetchval("SELECT coalesce(max(id), 0) FROM messages WHERE room = %s", (room,))


@app.get("/rooms/{room}/stream")
async def stream(room: str, last_event_id: Annotated[str | None, Header()] = None):
    cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else None  # client input: parse it
    return hub.sse(room, read=lambda after: read_after(room, after), cursor=cursor, start=lambda: latest_id(room))
```

**3. The browser:**

```js
const source = new EventSource("/rooms/lobby/stream");
source.onmessage = (e) => render(JSON.parse(e.data));
```

`EventSource` reconnects by itself and sends `Last-Event-ID`; the stream catches up from there.

A full runnable app, with async reads on a connection pool and a browser page: [examples/chat.py](examples/chat.py).

## Other frameworks

`hub.sse` is a thin wrapper over `hub.stream`, which yields encoded SSE frames (`str`) and knows nothing about Starlette. It raises `TooManySubscribers` at the call, before any response exists, so turn that into your framework's 503. The Django and Litestar snippets below are illustrative, not tested in CI.

```python
# Django (ASGI; run the hub's lifespan from your ASGI app or a lifespan wrapper)
from django.http import HttpResponse, StreamingHttpResponse


async def stream(request, room):
    try:
        frames = hub.stream(room, read=lambda after: read_after(room, after), start=lambda: latest_id(room))
    except TooManySubscribers:
        return HttpResponse(status=503, headers={"Retry-After": "30"})
    return StreamingHttpResponse(frames, content_type="text/event-stream", headers={"Cache-Control": "no-cache"})
```

```python
# Litestar
from litestar import get
from litestar.response import Stream


@get("/rooms/{room:str}/stream")
async def stream(room: str) -> Stream:
    frames = hub.stream(room, read=lambda after: read_after(room, after), start=lambda: latest_id(room))
    return Stream(
        frames, media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )
```

Aiohttp and Quart work the same way: write each frame to the response as it arrives.

## Signal streams

A stream doesn't have to carry data. It can tell the page *what* to refetch: "this list changed", "only the typing indicator changed", "you may have missed something, refetch everything". For that, give `read` a second parameter named `wake` (or a required second parameter). It receives a `Wake` that says why the read is running; several causes can arrive together, since a burst is one read.

| Field | Meaning |
|---|---|
| `start` | The stream's first read. |
| `keys` | Keys with a database notification since the last read. |
| `signals` | Labels from `hub.wake(key, signal=...)` since the last read. |
| `resync` | The listener connected or reconnected: anything may have changed. |
| `reread` | Nothing woke the stream for `reread_seconds`. |

`wake.data_may_have_changed` is True unless the only causes are `hub.wake` signals.

```python
from pg_sse import Event, Wake


def signals(cursor, wake: Wake) -> list[Event]:
    if wake.start:
        return [Event({}, event="ready")]  # the page loads everything once
    if wake.resync or wake.reread:
        return [Event({}, event="resync")]  # anything may have changed
    if wake.keys:
        return [Event({"keys": sorted(wake.keys)}, event="change")]
    if "typing" in wake.signals:
        return [Event({}, event="typing")]  # refetch only the typing indicator
    return []


@app.get("/inbox/stream")
async def inbox(user: User = Depends(current_user)):
    return hub.sse(None, read=signals, owner=user.id)  # every key on the channel


# elsewhere, from any thread:
hub.wake(room_id, signal="typing", all_keys=False)
```

A one-parameter `read(cursor)` works too, and so does one whose second parameter has a default under another name (`read(after, limit=500)`): only a parameter named `wake`, or a required one, receives the `Wake`. `hub.wake(key)` reaches the streams for `key` and the all-keys streams (`key=None`); `all_keys=False` keeps it on its key, which is right for ephemeral per-key state (typing, presence, a cursor position) that an all-keys view has nothing to show for. Database notifications always reach the all-keys streams: they report real writes.

## What `hub.sse` does for you

- **Subscribes before the catch-up read**, so a write between the two is a queued wake-up, not a gap.
- **Calls `start()` after subscribing** when there's no cursor, and sends that cursor as the first frame's `id`, so a reconnect resumes from it even if no event arrived.
- **Re-reads on every wake-up**, and a burst of notifications costs one read.
- **Pings every 15–25 s** (`ping_seconds`) with an SSE comment, so proxies do not time an idle stream out. A ping costs no database read.
- **Re-reads after 60 s of quiet** (`reread_seconds`) even when nothing woke it. This is the safety net under every lost notification: the worst case is a one-minute delay.
- **Wakes every stream after the listener connects or reconnects**, since anything committed before its LISTEN existed (at startup, or while it was down) was missed. Reconnects with backoff; a `SELECT 1` every minute and TCP keepalives catch a half-open connection.
- **Bounded queue per stream** (64). Identical wake-ups are queued once, so a hot key costs one slot, not one per write. A stream ends, rather than buffers, when more than `queue_size` distinct keys or signals arrive during one read (only an all-keys stream on a wide channel gets there); the browser reconnects and catches up.
- **Caps** open streams per process (500) and, when you pass `owner`, per key and owner (5). Past a cap, `hub.sse` raises a `503` with `Retry-After: 30`. Without `owner`, one client can hold every slot in a worker; pass `owner` (user id, session, or client address) on public endpoints.
- **Ends streams after 15 minutes.** A stream is authenticated once, when it opens; this bounds how long a revoked session keeps one.
- **Ends every stream on SIGTERM**, so a deploy doesn't wait out uvicorn's graceful-shutdown timeout. It chains onto the server's handler; with no handler installed (a plain script) the default action is left alone and ends the process as usual.
- **Says goodbye before a planned close.** The last frame carries `retry:`, which `EventSource` applies by itself, and, with `end_event` set, a named event with the reason:

| Reason | When | `retry` (ms) | What the client should do |
|---|---|---|---|
| `max_age` | The stream reached `max_stream_seconds`. | 1000 | Reconnect now, with its cursor. |
| `shutdown` | SIGTERM, or the lifespan ending. | 1000–5000, random | Reconnect after the delay. The jitter spreads a deploy's reconnects out. |
| `behind` | The stream's queue filled. | 1000 | Reconnect and catch up from its cursor. |
| `busy` | A cap filled between the check in `hub.sse` and the stream starting. | 30000 | Back off, as for a `503` with `Retry-After: 30`. |

There is no goodbye after `Event(final=True)`, since the application already said what it meant, or when `read` raised, since the stream's state is unknown and an abrupt close is the honest signal.

```js
// EventSource: nothing to write. `retry` is applied automatically; the reason is there if you want it.
source.addEventListener("end", (e) => console.debug("stream ended:", JSON.parse(e.data).reason));
```

```ts
// A fetch-based reader decides for itself.
if (event.event === "end") {
  const { reason } = JSON.parse(event.data);
  scheduleReconnect(reason === "busy" ? 30_000 : event.retry ?? 1_000);
}
```

## API

| | |
|---|---|
| `Hub(conninfo, channel, *, max_subscribers=500, max_per_owner=5, queue_size=64, max_stream_seconds=900, ping_seconds=(15, 25), reread_seconds=60, json_default=None, end_event=None, handle_signals=True, listen_timeout=5.0)` | One per channel per process. `max_per_owner` applies only to streams opened with an `owner`; anonymous streams are limited by `max_subscribers` alone. `queue_size` must be at least 1. `json_default` is `json.dumps`'s `default` for `Event` data that is not JSON-native: `json_default=str` is the common choice for datetimes, UUIDs and Decimals. `end_event` names the event sent with the reason before a planned close; off by default, the `retry:` alone is always sent. `listen_timeout` is how many seconds `lifespan` waits for the first LISTEN before the app starts (`None`: do not wait); it never fails startup. |
| `hub.lifespan` | `FastAPI(lifespan=hub.lifespan)`, or `async with hub.lifespan():` inside your own lifespan. Chains onto an existing SIGTERM/SIGINT handler (a default action is left alone) and restores it when it exits. Starts without a database: the listener keeps retrying and `listener_connected` says whether it is live. |
| `hub.stream(key, read, *, cursor=None, start=None, owner=None, end_event=None)` | The primary API: an async iterator of encoded SSE frames, for any framework. `key=None` streams every key on the channel. `key` is the text form of the key column (`str(room_id)` for an integer column): the notification payload is `key::text`, and a NULL key notifies `""`. `read` takes `(cursor)` or `(cursor, wake)`, and returns a list, not a generator: a generator would run its queries on the event loop. It and `start` may be plain functions (run in a thread) or async. `end_event` overrides the hub's. Raises `TooManySubscribers` when it is called. |
| `hub.sse(key, read, *, cursor=None, start=None, owner=None, end_event=None, headers=None)` | `stream` as a Starlette/FastAPI response (needs the `starlette` extra). Past a cap it raises a 503 with `Retry-After`. |
| `Event(data, id=None, event=None, final=False)` | `data`: a str as is, anything else as JSON. `id`: the cursor after this event, handed back to `read` unchanged. `final=True` ends the stream after it (a closed conversation). |
| `Wake(start=False, keys=frozenset(), signals=frozenset(), resync=False, reread=False)` | Why `read` runs, for a `read(cursor, wake)`. `data_may_have_changed` is True unless the only causes are `hub.wake` signals. |
| `hub.wake(key, signal="wake", *, all_keys=True)` | Wakes one key's streams for a change that isn't a database write (someone is typing). Safe from any thread. `signal` shows up in `wake.signals`; `all_keys=False` leaves the all-keys streams alone. |
| `hub.stats()` | A frozen `HubStats`: `open_streams`, `refused_total`, `dropped_total`, `listener_connected`, `listener_reconnects_total`, `last_notification_at` (Unix time or `None`). Counters are per process and start at zero. |
| `hub.listening` | A `threading.Event`, set while the listener holds its LISTEN. `hub.stats().listener_connected` says the same. |
| `hub.subscribe(key, owner)` / `hub.unsubscribe(s)` | The low-level queue under `stream`, for a custom stream loop. |
| `trigger_sql(table, key_column, channel, *, order_column=None)` | The trigger SQL. Plain lower-case names; `table` may be `schema.table`. `order_column` must be a serial, bigserial or identity column; installing fails with a clear error otherwise. Creates a function and a trigger named `"pg_sse$notify$<table>$<key>"` (and `"pg_sse$order$<table>$<order_column>"`), which is what you will see in `\d`. Postgres 14+. |
| `drop_trigger_sql(table, key_column, *, order_column=None)` | The SQL that removes what `trigger_sql` created, for a downgrade migration. |

### Health check

```python
import dataclasses

from fastapi.responses import JSONResponse


@app.get("/healthz")
async def healthz():
    stats = hub.stats()
    return JSONResponse(dataclasses.asdict(stats), status_code=200 if stats.listener_connected else 503)
```

## Testing your app

The hub needs a real Postgres; there is nothing to mock, because the database is the source of truth. Point the tests at a scratch database and enter the hub's lifespan. Starlette's `TestClient` runs the lifespan off the main thread, where the signal hook is skipped anyway, but pass `handle_signals=False` in tests so a test never touches the process's signal handlers.

```python
hub = Hub(TEST_DSN, channel="room_change", handle_signals=False)
with TestClient(app) as client:  # runs the lifespan
    ...
```

## Multiple workers

Each process (each uvicorn or gunicorn worker) holds one LISTEN connection and its own caps and counters. Nothing is shared between workers and nothing needs to be: Postgres delivers every notification to every listener, and each worker wakes its own streams. Budget one connection per worker.

## Limits

- **LISTEN needs a direct connection.** Behind a transaction-mode pooler (PgBouncer in transaction mode, the pooled connection strings Supabase and Neon give out) LISTEN succeeds and nothing ever arrives; streams then update only on the re-read. Give the hub the direct URL; your queries can still use the pooler.
- **The cursor sees new rows, not updates.** If a stream must show edits, have `read` return the current state alongside new rows, or use a cursor that moves on update (a version column with `order_column`).
- **Sync reads run in a thread pool.** A plain-function `read` or `start` runs in asyncio's default executor (`min(32, cpu + 4)` threads), and under Starlette other sync routes share anyio's pool of 40. Hundreds of streams re-reading at once queue behind it. Prefer async `read` and `start` on an `psycopg_pool.AsyncConnectionPool`; see [examples/chat.py](examples/chat.py). Use `functools.partial(read_after, room)` rather than a lambda to keep an async function off the pool.
- **`order_column` serialises inserts per key.** The ordering trigger takes `pg_advisory_xact_lock` on the key and holds it until the transaction ends, so every other insert to that key waits for the whole transaction. Two transactions inserting into keys in opposite order can deadlock, and Postgres aborts one of them. Keep the transaction that inserts short, or skip `order_column` and tolerate a late id arriving on the next re-read. The lock is `pg_advisory_xact_lock(hashtext(channel), hashtext(key))`; advisory locks share one namespace per database, so an application taking the same two-int lock would serialise against these inserts, and since `hashtext` is 32-bit two different keys can collide and briefly wait on each other (a delay, never wrong ids).
- **Per process.** Each worker runs its own listener, which works, but the caps are per process.
- **The database may be down at startup.** `lifespan` waits up to `listen_timeout` for the first LISTEN and then starts anyway; until the listener connects, streams update on their re-read and `listener_connected` is false.
- **Disconnect detection depends on the server.** Starlette 1.7 under an ASGI server that advertises `spec_version` 2.4 no longer listens for `http.disconnect` during a streaming response: a dead client is noticed at the next write, at most `ping_seconds[1]` later, and its slot is held until then. uvicorn 0.54 advertises 2.3 and frees the slot at once.
- **psycopg 3** for the listener; your own queries can use any driver.
- **A framework for the response.** `hub.sse` needs Starlette or FastAPI; any other framework uses `hub.stream`.

## For AI coding agents

If you are generating code with this library, follow these rules:

1. Run `trigger_sql(table, key_column, channel, order_column="id")` once in a migration. Do not call `pg_notify` from application code, and do not put row data in a notification.
2. Give `Hub` a **direct** database URL (not a pooled / PgBouncer / Supabase port 6543 / Neon `-pooler` URL).
3. Register the hub with `FastAPI(lifespan=hub.lifespan)`, or `async with hub.lifespan():` inside an existing lifespan.
4. Call `hub.sse(...)` (Starlette/FastAPI) or `hub.stream(...)` (any framework; catch `TooManySubscribers`) from an `async def` route, after any 404 or permission check.
5. `read(after)` returns `pg_sse.Event` items **after** the cursor, oldest first, each with `id` set. Use `WHERE id > %s ORDER BY id LIMIT n`.
6. Parse `Last-Event-ID` before passing it as `cursor`; it is client input.
7. Pass `start=` (the current max id) when the stream should start from now. Do not compute it before calling `hub.sse`: that reintroduces a gap.
8. Prefer async `read` and `start` on a connection pool; plain functions run in a limited thread pool.
9. In the browser use `EventSource`; it resumes with `Last-Event-ID` by itself.
10. `read(cursor, wake)` is optional: use it only when the stream must tell the client *what kind* of change happened (see Signal streams). A stream that sends rows keeps `read(cursor)`.
11. Use `hub.wake(key, signal=..., all_keys=False)` for ephemeral per-key state (typing, presence). The default also wakes the all-keys streams.
12. Set `Hub(end_event="end")` when the client is not `EventSource`, so it can read the reason and the retry before a planned close.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). In short:

```sh
uv sync
PG_SSE_TEST_DSN=postgresql://sse:sse@127.0.0.1:55432/pg_sse_test uv run pytest --cov
uv run ruff check . && uv run mypy
```

The tests create and drop a schema per test in the database you point them at; use a scratch database.

## License

MIT
