# pg-sse

[![CI](https://github.com/sandhusanthakumar/pg-sse/actions/workflows/ci.yml/badge.svg)](https://github.com/sandhusanthakumar/pg-sse/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pg-sse)](https://pypi.org/project/pg-sse/)
[![Python versions](https://img.shields.io/pypi/pyversions/pg-sse)](https://pypi.org/project/pg-sse/)
[![License](https://img.shields.io/pypi/l/pg-sse)](https://github.com/sandhusanthakumar/pg-sse/blob/main/LICENSE)

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
| `pg_notify('room_change', room)` after every insert, update and delete (or only the writes you pick with `on`) | No writer can forget it: not a cron job, not a psql session. Delivered on commit; a rollback announces nothing. |
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

A full runnable app, with async reads on a connection pool and a browser page: [examples/chat.py](https://github.com/sandhusanthakumar/pg-sse/blob/main/examples/chat.py).

**Notify only for the writes a reader can see.** By default every insert, update and delete notifies. A cursor-based `read` only finds new rows, so on a table with edits, read receipts or soft deletes, each update would wake every reader of that key for a read that returns nothing. Say which writes matter:

```python
trigger_sql("messages", key_column="room", channel="room_change", order_column="id", on=("insert",))
# Inserts, deletes, and edits of body (the key column is watched too):
trigger_sql("messages", key_column="room", channel="room_change", update_of=("body",))
```

`on` takes any of `"insert"`, `"update"` and `"delete"`. `update_of` narrows updates to those whose `SET` list names one of the columns; the key column is always added, so a row that moves to another key still notifies. Run the SQL again with different options to change an installed trigger.

## SQLAlchemy and Alembic

`trigger_sql` returns plain SQL, so it goes through whatever runs your migrations. With Alembic, install the trigger after the table exists in `upgrade`, and remove it before the table goes in `downgrade` (dropping a table drops its triggers but leaves the functions behind):

```python
# alembic/versions/0001_messages.py
import sqlalchemy as sa
from alembic import op
from pg_sse import drop_trigger_sql, trigger_sql


def upgrade() -> None:
    op.create_table(
        "messages",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("room_id", sa.Integer, nullable=False),
        sa.Column("body", sa.Text, nullable=False),
    )
    op.execute(trigger_sql("messages", key_column="room_id", channel="room_change", order_column="id", on=("insert",)))


def downgrade() -> None:
    op.execute(drop_trigger_sql("messages", key_column="room_id", order_column="id"))
    op.drop_table("messages")
```

The SQL is several statements in one string. psycopg runs that; **asyncpg refuses it** ("cannot insert multiple commands into a prepared statement"). If your app uses asyncpg, point Alembic at `postgresql+psycopg://` (psycopg 3 is already a dependency; the async migration template works with it too). Your app's own engine can stay on asyncpg.

`read` and `start` on an `AsyncSession`, wired the same way as the chat example. The room id is an integer here, and a key is always text, so the route passes `str(room_id)`:

```python
from functools import partial
from typing import Annotated

from fastapi import FastAPI, Header
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pg_sse import Event, Hub

engine = create_async_engine("postgresql+asyncpg://...")  # the app's own connection, pooled however you like
Session = async_sessionmaker(engine, expire_on_commit=False)
hub = Hub(DIRECT_DATABASE_URL, channel="room_change")  # psycopg; a direct connection, not a pooled one
app = FastAPI(lifespan=hub.lifespan)


async def read_after(room_id: int, after: int) -> list[Event]:
    async with Session() as session:
        query = select(Message).where(Message.room_id == room_id, Message.id > after).order_by(Message.id).limit(500)
        return [Event({"id": m.id, "body": m.body}, id=m.id) for m in await session.scalars(query)]


async def latest_id(room_id: int) -> int:
    async with Session() as session:
        return (
            await session.scalar(select(func.coalesce(func.max(Message.id), 0)).where(Message.room_id == room_id)) or 0
        )


@app.get("/rooms/{room_id}/stream")
async def stream(room_id: int, last_event_id: Annotated[str | None, Header()] = None):
    cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else None
    return hub.sse(
        str(room_id),  # not room_id: an int subscribes fine, never wakes, and pg_sse raises a TypeError for it
        read=partial(read_after, room_id),
        cursor=cursor,
        start=partial(latest_id, room_id),
    )
```

Both blocks were run against a real database (an Alembic upgrade and downgrade, and a live stream on the psycopg and asyncpg drivers) but are not part of the test suite, so treat them as illustrative.

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

When several causes arrive together, answer them in this order: `start`, then `resync` or `reread`, then `keys`, then `signals`. The first two already tell the page to refetch everything, so anything after them would repeat it. `read` may be `async def`: it then runs on the event loop with no thread hop, which suits a read that only looks at `wake`.

```python
from pg_sse import Event, Wake


async def signals(cursor, wake: Wake) -> list[Event]:
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


typing = hub.ephemeral("typing", ttl=8)  # at import time, next to the hub
# elsewhere, from any thread, on every keystroke:
typing.set(room_id, user_id)
```

A runnable version, with a typing indicator that lapses on its own and an inbox that watches every room: [examples/inbox.py](https://github.com/sandhusanthakumar/pg-sse/blob/main/examples/inbox.py).

A one-parameter `read(cursor)` works too, and so does one whose second parameter has a default under another name (`read(after, limit=500)`): only a parameter named `wake`, or a required one, receives the `Wake`. `hub.wake(key)` reaches the streams for `key` and the all-keys streams (`key=None`); `all_keys=False` keeps it on its key, which is right for ephemeral per-key state (typing, presence, a cursor position) that an all-keys view has nothing to show for. Database notifications always reach the all-keys streams: they report real writes.

### Frames you will see

What `hub.stream` yields, and what `EventSource` does with each:

| Frame | Looks like | Meaning |
|---|---|---|
| Id only | `id: 42` | The first frame when you pass a `cursor`, so the browser records it as its `Last-Event-ID`. Dispatches nothing. |
| Ping | `: ping` | A comment, sent when nothing else was. Dispatches nothing. |
| Event | `id: 43`, `event: change`, `data: {...}` | Your `Event`. The `id` becomes the browser's `Last-Event-ID`. |
| Goodbye | `retry: 1000`, or with `end_event`, `retry: 1000`, `event: end`, `data: {"reason":"max_age"}` | The last frame before a planned close. No `id`, so `Last-Event-ID` stays at the last event. |

### Ephemeral state

Typing and presence are true only while a client keeps saying so. `hub.ephemeral` keeps them for you: a signal per key and person that lapses unless renewed, and wakes the key's streams when it starts, is removed or lapses, so a closed tab never leaves a stale "typing" on another screen.

```python
typing = hub.ephemeral("typing", ttl=8)

typing.set(room_id, user_id)  # a keystroke: wakes the room's streams the first time, then only renews
typing.set(room_id, user_id, on=False)  # they sent the message: removed, wakes
typing.live(room_id)  # ["ann", "bo"]: who has not lapsed, for the GET the page refetches
```

## What `hub.sse` does for you

- **Subscribes before the catch-up read**, so a write between the two is a queued wake-up, not a gap.
- **Calls `start()` after subscribing** when there's no cursor, and sends that cursor as the first frame's `id`, so a reconnect resumes from it even if no event arrived.
- **Re-reads on every wake-up**, and a burst of notifications costs one read.
- **Pings every 15–25 s** (`ping_seconds`) with an SSE comment, so proxies do not time an idle stream out. A ping costs no database read.
- **Re-reads after 60 s of quiet** (`reread_seconds`) even when nothing woke it. This is the safety net under every lost notification: the worst case is a one-minute delay.
- **Wakes every stream after the listener connects or reconnects**, since anything committed before its LISTEN existed (at startup, or while it was down) was missed. Reconnects with backoff; a `SELECT 1` every minute and TCP keepalives catch a half-open connection.
- **Bounded queue per stream** (64). Identical wake-ups are queued once, so a hot key costs one slot, not one per write. A stream ends, rather than buffers, when more than `queue_size` distinct keys or signals arrive during one read (only an all-keys stream on a wide channel gets there); the browser reconnects and catches up.
- **Caps** open streams per process (500) and, when you pass `owner` (a user id, session, or client address), per key and owner (5). Past a cap, `hub.sse` raises a `503` with `Retry-After: 30`. The per-owner cap counts one key at a time: an owner may hold 5 streams on *every* key it can name, up to the process cap. To bound one client's total, set `max_total_per_owner` (off by default). Without `owner`, a client is not counted at all and can hold every slot in a worker, so pass `owner` on public endpoints, and size the caps on the total rather than on the per-key number.
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

Everything is importable from `pg_sse`. Each name below has a short description; the docstrings carry the rest.

### `Hub`

```python
Hub(conninfo, channel, *, max_subscribers=500, max_per_owner=5, max_total_per_owner=None, queue_size=64,
    max_stream_seconds=900, ping_seconds=(15, 25), reread_seconds=60, json_default=None, end_event=None,
    handle_signals=True, listen_timeout=5.0)
```

One per channel per process.

- `conninfo`: a DSN, or a function returning one. A function is called on every connect, so a rotating password or settings that are only readable at startup need no rebuilt hub.
- `max_subscribers`: open streams per process.
- `max_per_owner`: open streams per key and owner, so several tabs on one room are one client. It does not limit how many keys an owner opens streams on.
- `max_total_per_owner`: open streams per owner across all keys; `None` (the default) sets no total. This is the cap that keeps one client from filling `max_subscribers`.
- Both owner caps apply only to streams opened with an `owner`; anonymous streams are limited by `max_subscribers` alone. A refused stream is a `TooManySubscribers`, a 503 from `hub.sse`.
- `queue_size`: wake-ups queued per stream before it is ended as too far behind. At least 1.
- `max_stream_seconds`: how long a stream lives before a planned close.
- `ping_seconds`: the `(low, high)` range, jittered, between pings on an idle stream.
- `reread_seconds`: how long a stream may sit quiet before it re-reads anyway.
- `json_default`: `json.dumps`'s `default` for `Event` data that is not JSON-native. `json_default=str` is the common choice for datetimes, UUIDs and Decimals.
- `end_event`: names the event sent with the reason before a planned close. Off by default; the `retry:` alone is always sent.
- `handle_signals`: chain onto SIGTERM and SIGINT so streams end at once on a deploy. Pass `False` in tests.
- `listen_timeout`: seconds `lifespan` waits for the first LISTEN before the app starts (`None`: do not wait). It never fails startup.

### `hub.lifespan`

`FastAPI(lifespan=hub.lifespan)`, or `async with hub.lifespan():` inside your own lifespan. Chains onto an existing SIGTERM/SIGINT handler (a default action is left alone) and restores it when it exits. Starts without a database: the listener keeps retrying and `listener_connected` says whether it is live.

### `hub.stream`

```python
hub.stream(key, read, *, cursor=None, start=None, owner=None, end_event=None) -> AsyncIterator[str]
```

The primary API: an async iterator of encoded SSE frames, for any framework. Raises `TooManySubscribers` when it is called, before any response exists.

- `key`: what the stream watches, or `None` for every key on the channel. It must be a `str`, the text form of the key column (`str(room_id)` for an integer column): the notification payload is `key::text`, a NULL key notifies `""`, and any other type raises a `TypeError`.
- `read`: takes `(cursor)` or `(cursor, wake)` and returns a list of `Event`, not a generator, which would run its queries on the event loop. A plain function runs in a thread; an async one runs on the loop. Its type is `pg_sse.Read`.
- `cursor`: where to start, already parsed from the client (`Last-Event-ID`, a query parameter).
- `start`: gives the current cursor when `cursor` is `None`, so the stream starts from now. Plain or async. Its type is `pg_sse.Start`.
- `owner`: a user id or session, for the per-owner cap.
- `end_event`: overrides the hub's.

### `hub.sse`

```python
hub.sse(key, read, *, cursor=None, start=None, owner=None, end_event=None, headers=None, refuse_with=None)
```

`stream` as a Starlette/FastAPI response (needs the `starlette` extra). Past a cap it raises a 503 with `Retry-After: 30`; `refuse_with(err)` returns your own exception to raise instead, e.g. one your error tracker ignores. `headers` are added to the response.

### `Event`

```python
Event(data, id=None, event=None, final=False)
```

- `data`: a `str` as is, anything else as JSON.
- `id`: the cursor after this event, handed back to `read` unchanged.
- `event`: the SSE event name; `None` means the browser's `message`.
- `final`: end the stream after this event (a closed conversation).

### `Wake`

```python
Wake(start=False, keys=frozenset(), signals=frozenset(), resync=False, reread=False)
```

Why `read` runs, for a `read(cursor, wake)`. See [Signal streams](#signal-streams) for the fields. `wake.data_may_have_changed` is `True` unless the only causes are `hub.wake` signals.

### `hub.wake`

```python
hub.wake(key, signal="wake", *, all_keys=True)
```

Wakes one key's streams for a change that is not a database write (someone is typing). Safe from any thread, and reaches only this process's streams. `signal` shows up in `wake.signals`; `all_keys=False` leaves the all-keys streams alone. `key` must be a `str`.

### `hub.call_later`

```python
hub.call_later(seconds, callback, *args)
```

Runs `callback(*args)` on the hub's event loop after `seconds`, for follow-up work on ephemeral state (check that a typing signal lapsed, then `hub.wake`). Safe from any thread; does nothing while the hub has no running loop.

### `distinct`

```python
distinct(read, key)  # -> a read
```

Wraps `read` so a snapshot is sent only when it changed. `key(event)` summarises what the event shows, as a hashable value; an event whose key equals the key of the last one sent is dropped. `None` means "always send" (the next event is then compared with nothing, so it is sent too), and a `final` event is always sent. `read` may be `read(cursor)` or `read(cursor, wake)`, sync or async; the wrapper keeps that shape.

```python
@app.get("/rooms/{room}/stream")
async def stream(room: str):
    # a read that returns a snapshot every time it runs; the page is told only when it differs
    return hub.sse(room, read=distinct(partial(snapshot, room), key=lambda e: e.data["unread"]))
```

Two things to get right:

- **The state is per stream.** Call `distinct` inside the route, once per request. Built once at import time, one wrapper would be shared by every stream, and a stream that has never been sent a snapshot would be told nothing.
- **A dropped event's `id` is never applied.** The hub advances the cursor only on events it yields, and the browser records only ids it receives. Drop only events whose id would not have advanced the cursor: a snapshot with no id, or one that repeats the last. Never drop an event that carries a new row, or a reconnect would replay from before it. Have `key` return `None` for any event that carries rows.

### `hub.ephemeral` and `Ephemeral`

```python
hub.ephemeral(name, *, ttl, signal=None, grace=0.25, all_keys=False, clock=time.monotonic) -> Ephemeral
```

Signals per key that lapse unless renewed (typing, presence), announced with `hub.wake`. `hub.ephemeral(...)` is `Ephemeral(hub, name, ...)`.

| Argument | Meaning |
|---|---|
| `ttl` | Seconds a signal lasts without renewal. |
| `signal` | The label `hub.wake` carries, so `wake.signals` contains it. Defaults to `name`. Two instances may share one: a `typing` (8 s) and a `viewing` (45 s) can both wake one `presence` frame. |
| `grace` | The lapse check runs this long after expiry, so it never finds a signal a hair from expiring. |
| `all_keys` | Passed to `hub.wake`. False by default: ephemeral state is per key, and a view of every key has nothing to show for it. |
| `clock` | Seconds, monotonic. Replace it in tests. |

| Method | Behaviour |
|---|---|
| `set(key, who, on=True)` | Renews or starts (`on`) or removes (`not on`) `who`'s signal on `key`. Returns whether what a reader sees changed, and wakes the key's streams only then. A renewal returns False. Removing returns True if there was an entry, even one that lapsed a moment ago: someone may have read it before it lapsed. |
| `clear(key, who=None)` | Removes one `who`, or everyone on `key`. Returns and wakes as `set(..., on=False)` does. |
| `live(key)` | Who has not lapsed right now, in the order they started. Never lists an expired entry, even if its lapse check has not run. |

`set` and `clear` are safe from any thread; `key` must be a `str`. Each renewal schedules one lapse check with `hub.call_later`; a check that finds the signal renewed leaves it alone, and the renewal's own check handles it later. State lives in this process only, so with several workers a signal set on one is invisible to the streams on the others, and it is lost on restart. While the hub is stopped nothing checks, so entries linger until the next `set` or `clear` for their key (`live` still skips them).

### `hub.stats`

A frozen `HubStats`: `open_streams`, `refused_total`, `dropped_total`, `listener_connected`, `listener_reconnects_total` and `last_notification_at` (Unix time or `None`). Counters are per process and start at zero.

### `hub.listening`

A `threading.Event`, set while the listener holds its LISTEN. `hub.stats().listener_connected` says the same.

### `hub.subscribe` and `hub.unsubscribe`

```python
hub.subscribe(key, owner=None) -> Subscription
hub.unsubscribe(subscription)
```

The low-level queue under `stream`, for a custom stream loop. `key` is checked like `stream`'s. Unsubscribe in a `finally`.

`await subscription.wait(timeout)` returns everything queued once there is anything, or `None` on timeout. An item is `("notify", key)` for a database notification, `("wake", key, signal)` for a `hub.wake`, or `("marker", name)`, which means re-read everything. Once `subscription.ended` is true the stream is over: it fell behind, or the hub is shutting down (`hub.closing`).

### `Read` and `Start`

The types of `read` and `start`: `Read` is `Callable[..., Iterable[Event] | Awaitable[Iterable[Event]]]` and `Start` is `Callable[[], Any]`. Use them to annotate a helper that wraps `hub.sse`:

```python
from pg_sse import Hub, Read, Start


def room_stream(hub: Hub, room_id: int, read: Read, start: Start):
    return hub.sse(str(room_id), read=read, start=start)
```

### `trigger_sql`

```python
trigger_sql(table, key_column, channel, *, order_column=None, on=("insert", "update", "delete"), update_of=None)
```

The SQL that wires a table to a channel, to run once in a migration. Postgres 14+.

- Names are plain lower-case identifiers; `table` may be `schema.table`.
- `order_column` must be a serial, bigserial or identity column; installing fails with a clear error otherwise.
- `on` limits which writes notify: any of `"insert"`, `"update"`, `"delete"`.
- `update_of` limits updates to those whose `SET` list names one of the columns (plus the key column). Needs `"update"` in `on`.
- It creates a function and a trigger named `"pg_sse$notify$<table>$<key>"` (and `"pg_sse$order$<table>$<order_column>"`), which is what you will see in `\d`. Running it again replaces them.
- The order trigger looks up the column's sequence once, when the SQL runs, and writes its name into the function, so an insert does no catalog search while it holds the key's lock. Dropping and recreating the sequence under the same name is fine; if you rename it or move it to another schema, run `trigger_sql` again.

### `drop_trigger_sql`

```python
drop_trigger_sql(table, key_column, *, order_column=None)
```

The SQL that removes what `trigger_sql` created, for a downgrade migration. It does not need `channel`, `on` or `update_of`.

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

To read a stream without a server, run the hub on the test's event loop with `async with hub.lifespan():` (leaving it forgets every subscription, so the next test starts clean) and read frames with `pg_sse.testing`:

```python
from pg_sse.testing import goodbye, next_event


async def test_a_new_message_arrives():
    async with hub.lifespan():
        frames = hub.stream("lobby", read=read_after_lobby, cursor=0)
        insert_message("lobby", "hi")
        assert (await next_event(frames)).json() == {"id": 1, "body": "hi"}
```

`next_event` skips pings and the id-only frame that opens a stream with a cursor. Keep that: a ping comes before any frame a re-read produces, because every idle timeout pings, even the one that leads to the re-read, and before a goodbye when the age limit and the ping timeout coincide. A test that expects the very next frame to be the event is flaky without it. Use `next_frame` when the ping or the id is what you are testing.

### `pg_sse.testing`

Reading a stream in tests, with no test framework and no database: the helpers take any async iterator of frames (what `hub.stream` returns, or a response's `body_iterator`). See [Testing your app](#testing-your-app).

| Name | Behaviour |
|---|---|
| `next_frame(frames, timeout=5.0)` | The next frame, whatever it is. |
| `next_event(frames, timeout=5.0, *, event=None)` | The next event: skips pings and id-only frames, and with `event=` frames of any other name. |
| `goodbye(frames, timeout=5.0)` | The `retry:` frame before a planned close. Skips pings; any other frame fails. |
| `ended(frames, timeout=5.0)` | Fails unless the stream is over. |
| `until(predicate, timeout=5.0)` | Waits, yielding to the event loop, until `predicate()` is true. |
| `decode(frame)` | Parses one frame into a `Frame`: `id`, `event`, `data` (lines joined with `\n`; None without one), `retry`, `comment`, `raw`, `json()`, and `is_ping`, `is_cursor`, `is_goodbye`. |

Waiting past `timeout` raises `TimeoutError` naming the frames seen so far; a stream that ends early fails with an `AssertionError` that does the same. A timed-out read cancels the generator, so the stream is over afterwards.

## Multiple workers

Each process (each uvicorn or gunicorn worker) holds one LISTEN connection and its own caps and counters. Nothing is shared between workers and nothing needs to be: Postgres delivers every notification to every listener, and each worker wakes its own streams. Budget one connection per worker.

The exception is `hub.wake`: it reaches only the streams of the process that calls it, since nothing is written to the database. State that lives in one worker's memory, which includes everything in `hub.ephemeral` (the typing indicator in the inbox example), is invisible to readers on the other workers; keep it where every worker can see it before you run more than one.

## Logging

pg-sse logs to the standard-library logger named `pg_sse` and adds no handlers, so configure it like any other. At `WARNING` it logs a stream refused at a cap or during shutdown (back-pressure, not a fault) and the listener losing its connection (with the traceback and the retry delay). At `ERROR` it logs, with a traceback, a stream whose `read` or `start` raised or returned something other than `Event` items; that stream ends and the client reconnects.

## Limits

- **LISTEN needs a direct connection.** Behind a transaction-mode pooler (PgBouncer in transaction mode, the pooled connection strings Supabase and Neon give out) LISTEN succeeds and nothing ever arrives; streams then update only on the re-read. Give the hub the direct URL; your queries can still use the pooler.
- **The cursor sees new rows, not updates.** If a stream must show edits, have `read` return the current state alongside new rows, or use a cursor that moves on update (a version column with `order_column`). If it does not need to show them, stop them from notifying: `trigger_sql(..., on=("insert",))`, or `update_of=(...)` to wake only for edits of the columns a reader shows. Otherwise every update wakes every reader of that key for a read that returns nothing. A write the trigger leaves out never wakes a stream; the stream finds it on its next re-read.
- **A key is text.** `hub.stream`, `hub.sse`, `hub.subscribe` and `hub.wake` raise a `TypeError` for a key that is not a `str`, because a notification carries the key column as text and an int would subscribe and never wake. Pass `str(room_id)`.
- **Sync reads run in a thread pool.** A plain-function `read` or `start` runs in asyncio's default executor (`min(32, cpu + 4)` threads), and under Starlette other sync routes share anyio's pool of 40. Hundreds of streams re-reading at once queue behind it. Prefer async `read` and `start` on an `psycopg_pool.AsyncConnectionPool`; see [examples/chat.py](https://github.com/sandhusanthakumar/pg-sse/blob/main/examples/chat.py). Use `functools.partial(read_after, room)` rather than a lambda to keep an async function off the pool.
- **`order_column` serialises inserts per key.** The ordering trigger takes `pg_advisory_xact_lock` on the key and holds it until the transaction ends, so every other insert to that key waits for the whole transaction. Two transactions inserting into keys in opposite order can deadlock, and Postgres aborts one of them. Keep the transaction that inserts short, or skip `order_column` and tolerate a late id arriving on the next re-read. The lock is `pg_advisory_xact_lock(hashtext(channel), hashtext(key))`; advisory locks share one namespace per database, so an application taking the same two-int lock would serialise against these inserts, and since `hashtext` is 32-bit two different keys can collide and briefly wait on each other (a delay, never wrong ids).
- **Ephemeral state is per process.** `hub.ephemeral` keeps its signals in memory: with several workers a signal set on one is invisible to the streams on the others, and a restart loses it. That costs nothing for typing or presence; anything that must be seen everywhere belongs in a table with a trigger.
- **Per process.** Each worker runs its own listener, which works, but the caps are per process.
- **The database may be down at startup.** `lifespan` waits up to `listen_timeout` for the first LISTEN and then starts anyway; until the listener connects, streams update on their re-read and `listener_connected` is false.
- **Disconnect detection depends on the server.** Starlette 1.7 under an ASGI server that advertises `spec_version` 2.4 no longer listens for `http.disconnect` during a streaming response: a dead client is noticed at the next write, at most `ping_seconds[1]` later, and its slot is held until then. uvicorn 0.54 advertises 2.3 and frees the slot at once.
- **psycopg 3** for the listener; your own queries can use any driver.
- **A framework for the response.** `hub.sse` needs Starlette or FastAPI; any other framework uses `hub.stream`.

## For AI coding agents

If you are generating code with this library, follow these rules. [llms.txt](https://github.com/sandhusanthakumar/pg-sse/blob/main/llms.txt) carries the same rules in a form meant to be fed to an agent.

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
11. Use `hub.ephemeral(name, ttl=...)` for ephemeral per-key state (typing, presence): `set(key, who)` on every keystroke or heartbeat, `live(key)` to answer the refetch. For other changes that are not database writes, `hub.wake(key, signal=..., all_keys=False)`; the default also wakes the all-keys streams.
12. Set `Hub(end_event="end")` when the client is not `EventSource`, so it can read the reason and the retry before a planned close.
13. The key is always a `str`: pass `str(room_id)` for an integer or UUID column. Anything else raises a `TypeError`.
14. On a table with edits, read receipts or soft deletes, use `trigger_sql(..., on=("insert",))` so only new rows wake readers.
15. In an Alembic migration, `op.execute(trigger_sql(...))` in `upgrade` and `op.execute(drop_trigger_sql(...))` in `downgrade`, on the psycopg driver (asyncpg rejects the multi-statement SQL).

## Development

See [CONTRIBUTING.md](https://github.com/sandhusanthakumar/pg-sse/blob/main/CONTRIBUTING.md). In short:

```sh
uv sync
PG_SSE_TEST_DSN=postgresql://sse:sse@127.0.0.1:55432/pg_sse_test uv run pytest --cov
uv run ruff check . && uv run mypy
```

The tests create and drop a schema per test in the database you point them at; use a scratch database.

## License

MIT
