"""The example app under a real server: what a browser's EventSource sees."""

import importlib
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


@pytest.fixture
def server(db, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", db.dsn)
    chat = importlib.reload(importlib.import_module("chat"))  # a fresh copy, so it reads this test's DATABASE_URL
    chat.hub.channel = db.channel
    chat.hub.handle_signals = False
    # This test runs on its own channel, so the trigger is set up here rather than by chat.setup().
    with psycopg.connect(db.dsn) as conn:
        conn.execute(chat.SCHEMA)
        conn.execute(trigger_sql("messages", key_column="room", channel=db.channel, order_column="id"))

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(chat.app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not (server.started and chat.hub.listening.is_set()):
        assert time.monotonic() < deadline, "server did not start"
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


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
