"""
A chat room as server-sent events: async reads on a connection pool, the recommended shape.

    pip install "pg-sse[fastapi]" uvicorn "psycopg[binary]" psycopg-pool
    DATABASE_URL=postgresql://localhost/demo python examples/chat.py --setup   # once: the table and triggers
    DATABASE_URL=postgresql://localhost/demo uvicorn examples.chat:app

    open http://localhost:8000/        # a browser page (examples/index.html), or with curl:
    curl -N localhost:8000/rooms/lobby/stream
    curl -X POST localhost:8000/rooms/lobby/messages -H 'content-type: application/json' -d '{"body": "hi"}'
"""

import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Annotated

import psycopg
from fastapi import FastAPI, Header
from fastapi.responses import HTMLResponse
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from pg_sse import Event, Hub, IntCursor, trigger_sql

# The hub needs a direct connection: LISTEN never delivers behind a transaction-mode pooler. The pool may use one.
DATABASE_URL = os.environ["DATABASE_URL"]

hub = Hub(DATABASE_URL, channel="room_change")
cursors = IntCursor()
pool = AsyncConnectionPool(DATABASE_URL, open=False)

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id   bigserial PRIMARY KEY,
    room text NOT NULL,
    body text NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_room_id ON messages (room, id);
"""


def setup() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(SCHEMA)
        # Notify on every write, and keep ids in commit order within a room.
        conn.execute(trigger_sql("messages", key_column="room", channel="room_change", order_column="id"))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await pool.open()  # on the running loop, so the pool belongs to it
    try:
        async with hub.lifespan():
            yield
    finally:
        await pool.close()


app = FastAPI(lifespan=lifespan)


async def read_after(room: str, after: int) -> list[Event]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, body FROM messages WHERE room = %s AND id > %s ORDER BY id LIMIT 500", (room, after)
        )
        rows = await cur.fetchall()
    return [Event({"id": message_id, "body": body}, id=message_id) for message_id, body in rows]


async def latest_id(room: str) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT coalesce(max(id), 0) FROM messages WHERE room = %s", (room,))
        row = await cur.fetchone()
    return row[0] if row else 0


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(Path(__file__).with_name("index.html").read_text())


@app.get("/rooms/{room}/stream")
async def stream(room: str, last_event_id: Annotated[str | None, Header()] = None):
    # Last-Event-ID is client input: only digits get through. No header, or one that is not a cursor, starts from
    # now: EventSource gives up for good on a non-200, so a 422 would end live updates in that tab.
    cursor = cursors.parse_one(last_event_id)
    # partial, not a lambda: an async function passed directly never touches the thread pool.
    return hub.sse(room, read=partial(read_after, room), cursor=cursor, start=partial(latest_id, room))


class NewMessage(BaseModel):
    body: str


@app.post("/rooms/{room}/messages")
async def post_message(room: str, message: NewMessage) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute("INSERT INTO messages (room, body) VALUES (%s, %s) RETURNING id", (room, message.body))
        row = await cur.fetchone()
    return {"id": row[0] if row else None}


if __name__ == "__main__" and "--setup" in sys.argv:
    setup()
    print("messages table and triggers ready")
