"""
Signal streams: a stream that tells the page *what* to refetch instead of sending rows. Rooms with a typing
indicator, and an inbox that watches every room.

    pip install "pg-sse[fastapi]" uvicorn "psycopg[binary]" psycopg-pool
    DATABASE_URL=postgresql://localhost/demo python examples/inbox.py --setup   # once: the table and trigger
    DATABASE_URL=postgresql://localhost/demo uvicorn examples.inbox:app

    curl -N localhost:8000/rooms/lobby/stream        # a room: "messages" and "typing" signals
    curl -N localhost:8000/inbox/stream              # every room: "change" signals, never typing
    curl -X POST localhost:8000/rooms/lobby/messages -H 'content-type: application/json' -d '{"body": "hi"}'
    curl -X POST localhost:8000/rooms/lobby/typing -H 'content-type: application/json' -d '{"user": "ann"}'
    curl localhost:8000/rooms/lobby/typing           # who is typing right now

Two kinds of wake-up reach `read(cursor, wake)`. A database write arrives as `wake.keys`, from the trigger. Typing is
not a write: nothing is stored, so `hub.ephemeral` keeps who is typing in memory, wakes the room when that changes and
wakes it again when a signal lapses. It leaves the inbox alone (`all_keys=False`), which has nothing to show for it.

`signal_read` turns those causes into events: `ready` on the first read, `resync` after a reconnect or a quiet re-read,
and a name per kind of wake-up. See the README's "Signal streams" for the same thing written out over `Wake`.

Typing lives in this process's memory, and `hub.wake` reaches only this process's streams. With several workers a
typist's signal would miss the readers on the other workers: keep typing in a shared place (a table with the same
trigger, or Redis) before you scale out.
"""

import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from pg_sse import Hub, signal_read, trigger_sql

DATABASE_URL = os.environ["DATABASE_URL"]
TYPING_SECONDS = 8.0  # how long after the last keystroke someone still counts as typing

hub = Hub(DATABASE_URL, channel="inbox_change")
pool = AsyncConnectionPool(DATABASE_URL, open=False)

# Who is typing, per room. A keystroke renews it; a lapse wakes the room's streams with the signal "typing".
typing = hub.ephemeral("typing", ttl=TYPING_SECONDS)

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox_messages (
    id   bigserial PRIMARY KEY,
    room text NOT NULL,
    body text NOT NULL
);
"""


def setup() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(SCHEMA)
        # Only a new message wakes anyone: an edit or a read receipt on this table would not change what a page
        # shows, so `on=("insert",)` keeps those writes from waking every reader of the room.
        conn.execute(trigger_sql("inbox_messages", key_column="room", channel="inbox_change", on=("insert",)))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await pool.open()
    try:
        async with hub.lifespan():
            yield
    finally:
        await pool.close()


app = FastAPI(lifespan=lifespan)


# A page watching one room: a new message, or a change in who is typing. There is no cursor: the events carry no data.
room_read = signal_read(change="messages", signals={"typing": "typing"})
# A page watching every room: which rooms got a message.
inbox_read = signal_read(change_data=lambda keys: {"rooms": sorted(keys)})


@app.get("/rooms/{room}/stream")
async def room_stream(room: str):
    return hub.sse(room, read=room_read)


@app.get("/inbox/stream")
async def inbox_stream():
    return hub.sse(None, read=inbox_read)  # every key on the channel


class Typing(BaseModel):
    user: str


@app.post("/rooms/{room}/typing")
async def post_typing(room: str, body: Typing) -> dict[str, bool]:
    typing.set(room, body.user)  # wakes the room only when this user was not already typing
    return {"ok": True}


@app.get("/rooms/{room}/typing")
async def get_typing(room: str) -> dict[str, list[str]]:
    return {"typing": sorted(typing.live(room))}


class NewMessage(BaseModel):
    body: str


@app.post("/rooms/{room}/messages")
async def post_message(room: str, message: NewMessage) -> dict[str, int | None]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO inbox_messages (room, body) VALUES (%s, %s) RETURNING id", (room, message.body)
        )
        row = await cur.fetchone()
    return {"id": row[0] if row else None}


@app.get("/rooms/{room}/messages")
async def get_messages(room: str) -> list[dict[str, int | str]]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, body FROM inbox_messages WHERE room = %s ORDER BY id DESC LIMIT 50", (room,)
        )
        return [{"id": message_id, "body": body} for message_id, body in await cur.fetchall()]


if __name__ == "__main__" and "--setup" in sys.argv:
    setup()
    print("inbox_messages table and trigger ready")
