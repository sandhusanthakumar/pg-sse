"""The example app under a real server: what a browser's EventSource sees."""

import contextlib
import importlib
import json
import socket
import sys
import threading
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import uvicorn

from pg_sse import trigger_sql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))


@contextlib.contextmanager
def serve(name, db, monkeypatch, trigger):
    """Runs the example module `name` under uvicorn on its own channel; yields (module, base url)."""
    monkeypatch.setenv("DATABASE_URL", db.dsn)
    example = importlib.reload(importlib.import_module(name))  # a fresh copy, so it reads this test's DATABASE_URL
    example.hub.channel = db.channel
    example.hub.handle_signals = False
    # This test runs on its own channel, so the trigger is set up here rather than by the example's setup().
    with psycopg.connect(db.dsn) as conn:
        conn.execute(example.SCHEMA)
        conn.execute(trigger(db.channel))

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(example.app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not (server.started and example.hub.listening.is_set()):
        assert time.monotonic() < deadline, "server did not start"
        time.sleep(0.05)
    try:
        yield example, f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


@pytest.fixture
def server(db, monkeypatch):
    def trigger(channel):
        return trigger_sql("messages", key_column="room", channel=channel, order_column="id")

    with serve("chat", db, monkeypatch, trigger) as (_, url):
        yield url


@pytest.fixture
def inbox(db, monkeypatch):
    def trigger(channel):
        return trigger_sql("inbox_messages", key_column="room", channel=channel, on=("insert",))

    with serve("inbox", db, monkeypatch, trigger) as (module, url):
        monkeypatch.setattr(module.typing, "ttl", 0.5)  # so the lapse is quick to wait for
        yield module, url


def read_frame(lines) -> dict:
    frame = {}
    for line in lines:
        if line == "":
            if frame:
                return frame
            continue
        name, _, value = line.partition(": ")
        frame[name] = value
    raise AssertionError("stream ended")


def test_live_then_resume_from_last_event_id(server):
    with httpx.Client(base_url=server, timeout=10) as client:
        with client.stream("GET", "/rooms/lobby/stream") as stream:
            lines = stream.iter_lines()
            assert read_frame(lines) == {"id": "0"}  # from now: nothing yet
            first = client.post("/rooms/lobby/messages", json={"body": "hi"}).json()["id"]
            client.post("/rooms/other/messages", json={"body": "not ours"})
            assert read_frame(lines) == {"id": str(first), "data": f'{{"id":{first},"body":"hi"}}'}

        # Offline for a while, then the browser reconnects with its Last-Event-ID.
        second = client.post("/rooms/lobby/messages", json={"body": "while away"}).json()["id"]
        with client.stream("GET", "/rooms/lobby/stream", headers={"Last-Event-ID": str(first)}) as stream:
            lines = stream.iter_lines()
            assert read_frame(lines) == {"id": str(first)}
            assert read_frame(lines)["id"] == str(second)


def test_signal_streams_say_what_to_refetch_and_typing_lapses(inbox):
    _, url = inbox
    with (
        httpx.Client(base_url=url, timeout=10) as client,
        client.stream("GET", "/rooms/lobby/stream") as room,
        client.stream("GET", "/inbox/stream") as every_room,
    ):
        room_lines, inbox_lines = room.iter_lines(), every_room.iter_lines()
        assert read_frame(room_lines) == {"event": "ready", "data": "{}"}
        assert read_frame(inbox_lines) == {"event": "ready", "data": "{}"}

        # Typing is no database write: the app announces it, and the room's page refetches who is typing.
        client.post("/rooms/lobby/typing", json={"user": "ann"})
        assert read_frame(room_lines) == {"event": "typing", "data": "{}"}
        assert client.get("/rooms/lobby/typing").json() == {"typing": ["ann"]}

        # A message is a database write: the room refetches messages, the inbox learns which room changed.
        client.post("/rooms/lobby/messages", json={"body": "hi"})
        client.post("/rooms/other/messages", json={"body": "elsewhere"})
        assert read_frame(room_lines) == {"event": "messages", "data": "{}"}
        changed = set()
        while changed != {"lobby", "other"}:
            frame = read_frame(inbox_lines)
            assert frame["event"] == "change"
            changed |= set(json.loads(frame["data"])["rooms"])
        assert [m["body"] for m in client.get("/rooms/lobby/messages").json()] == ["hi"]

        # Nobody typed again: the lapse check announces it and the room refetches an empty list.
        assert read_frame(room_lines) == {"event": "typing", "data": "{}"}
        assert client.get("/rooms/lobby/typing").json() == {"typing": []}


def test_a_last_event_id_that_is_not_a_cursor_starts_the_stream_from_now(server):
    # b"\xb2" reaches the app as "²", which str.isdigit() accepts and int() rejects: a 500 if parsed by hand. A 422
    # would be no better: EventSource stops for good on any response that is not a 200.
    with httpx.Client(base_url=server, timeout=10) as client:
        latest = client.post("/rooms/lobby/messages", json={"body": "before"}).json()["id"]
        for value in (b"\xb2", b"abc", b"-1", b"1.5", b"(1,)"):
            with client.stream("GET", "/rooms/lobby/stream", headers={"Last-Event-ID": value}) as response:
                assert response.status_code == 200, value
                assert read_frame(response.iter_lines()) == {"id": str(latest)}, value  # from now: not "before"
