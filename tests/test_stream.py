import asyncio
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from functools import partial

import pytest
from helpers import execute, make_hub, make_table, terminate_listener, until
from starlette.exceptions import HTTPException

from pg_sse import Event, Hub, TooManySubscribers, Wake

pytestmark = pytest.mark.anyio


def insert(db, room: str, body: str) -> int:
    return execute(db.dsn, "INSERT INTO messages (room, body) VALUES (%s, %s) RETURNING id", (room, body))[0][0]


def reader(db, room: str):
    def read(after):
        rows = execute(
            db.dsn, "SELECT id, body FROM messages WHERE room = %s AND id > %s ORDER BY id", (room, after or 0)
        )
        return [Event(body, id=message_id) for message_id, body in rows]

    return read


async def next_frame(body, timeout: float = 3.0) -> str:
    return await asyncio.wait_for(anext(body), timeout)


def consume(body) -> asyncio.Task:
    """Reads frames in the background: a stream only runs while a client reads it."""

    async def drain():
        async for _ in body:
            pass

    return asyncio.ensure_future(drain())


async def stop(consumer: asyncio.Task) -> None:
    consumer.cancel()
    await asyncio.gather(consumer, return_exceptions=True)


def goodbye(frame: str) -> tuple[int, str | None]:
    """The (retry ms, reason) of a goodbye frame; the reason is None when no event was asked for."""
    fields = dict(line.split(": ", 1) for line in frame.strip().split("\n"))
    return int(fields["retry"]), json.loads(fields["data"])["reason"] if "data" in fields else None


async def ended(body) -> None:
    with pytest.raises(StopAsyncIteration):
        await next_frame(body)


async def test_catch_up_then_live(db):
    make_table(db, order_column="id")
    first, second = insert(db, "lobby", "one"), insert(db, "lobby", "two")
    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        response = hub.sse("lobby", read=reader(db, "lobby"), cursor=0)
        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["x-accel-buffering"] == "no"
        body = response.body_iterator
        assert await next_frame(body) == "id: 0\n\n"
        assert await next_frame(body) == f"id: {first}\ndata: one\n\n"
        assert await next_frame(body) == f"id: {second}\ndata: two\n\n"
        insert(db, "elsewhere", "not ours")
        third = insert(db, "lobby", "three")
        assert await next_frame(body, timeout=2) == f"id: {third}\ndata: three\n\n"
        await body.aclose()
        assert hub.stats().open_streams == 0


async def test_start_begins_from_now(db):
    make_table(db, order_column="id")
    insert(db, "lobby", "old")
    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)

        def latest():
            return execute(db.dsn, "SELECT max(id) FROM messages")[0][0]

        body = hub.sse("lobby", read=reader(db, "lobby"), start=latest).body_iterator
        assert await next_frame(body) == f"id: {latest()}\n\n"
        new = insert(db, "lobby", "new")
        assert await next_frame(body) == f"id: {new}\ndata: new\n\n"
        await body.aclose()


async def test_without_a_cursor_read_decides(db):
    hub = make_hub(db)
    seen = []

    def read(after):
        seen.append(after)
        return [Event("hello", id=1)] if after is None else []

    async with hub.lifespan():
        body = hub.sse("lobby", read=read).body_iterator
        assert await next_frame(body) == "id: 1\ndata: hello\n\n"
        reading = asyncio.ensure_future(anext(body))  # the stream only runs while a client reads it
        await asyncio.sleep(0.05)
        hub.wake("lobby")
        await until(lambda: len(seen) == 2)
        assert seen == [None, 1]
        reading.cancel()
        await asyncio.gather(reading, return_exceptions=True)
        await body.aclose()


async def test_async_read_and_start(db):
    hub = make_hub(db)

    async def start():
        return 5

    async def read(after):
        return [Event({"after": after}, id=after + 1, event="tick")] if after == 5 else []

    async with hub.lifespan():
        body = hub.sse("lobby", read=read, start=start).body_iterator
        assert await next_frame(body) == "id: 5\n\n"
        assert await next_frame(body) == 'id: 6\nevent: tick\ndata: {"after":5}\n\n'
        await body.aclose()


async def test_a_lambda_returning_a_coroutine(db):
    hub = make_hub(db)

    async def read_async(after):
        return [Event("x", id=1)] if after is None else []

    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: read_async(after)).body_iterator
        assert await next_frame(body) == "id: 1\ndata: x\n\n"
        await body.aclose()


async def test_a_quiet_stream_re_reads_without_a_notification(db):
    hub = make_hub(db, ping_seconds=(0.05, 0.05), reread_seconds=0.2)
    pending: list[Event] = []
    wakes: list[Wake] = []

    def read(after, wake):
        wakes.append(wake)
        events, pending[:] = list(pending), []
        return events

    async with hub.lifespan():
        body = hub.sse("lobby", read=read, cursor=0).body_iterator
        assert await next_frame(body) == "id: 0\n\n"
        assert await next_frame(body) == ": ping\n\n"
        pending.append(Event("missed", id=1))
        frames = [await next_frame(body) for _ in range(5)]
        assert "id: 1\ndata: missed\n\n" in frames
        assert wakes[0] == Wake(start=True)
        assert Wake(reread=True) in wakes[1:]
        await body.aclose()


async def test_read_learns_why_it_runs(db):
    make_table(db, order_column="id")
    hub = make_hub(db, reread_seconds=30)
    wakes: list[Wake] = []

    def read(after, wake):
        wakes.append(wake)
        return []

    async with hub.lifespan():
        await until(hub.listening.is_set)
        consumer = consume(hub.stream("lobby", read=read, cursor=0))
        await until(lambda: wakes)
        assert wakes == [Wake(start=True)]
        insert(db, "lobby", "x")
        await until(lambda: len(wakes) == 2)
        assert wakes[1] == Wake(keys=frozenset({"lobby"}))
        hub.wake("lobby", signal="typing")
        await until(lambda: len(wakes) == 3)
        assert wakes[2] == Wake(signals=frozenset({"typing"}))
        assert not wakes[2].data_may_have_changed
        terminate_listener(db)
        await until(lambda: len(wakes) == 4)
        assert wakes[3] == Wake(resync=True)
        await stop(consumer)


async def test_an_all_keys_stream_sees_every_key_in_a_burst(db):
    hub = make_hub(db)
    wakes: list[Wake] = []

    async def read(after, wake):
        wakes.append(wake)
        return []

    async with hub.lifespan():
        body = hub.stream(None, read=partial(read), cursor=0)
        assert await next_frame(body) == "id: 0\n\n"  # subscribed, and the first read not yet run
        for key in ("a", "b", "a"):
            hub._dispatch(("notify", key), key)
        consumer = consume(body)
        await until(lambda: len(wakes) == 2)
        assert wakes == [Wake(start=True), Wake(keys=frozenset({"a", "b"}))]
        await stop(consumer)


async def test_pings_cost_no_database_read(db):
    hub = make_hub(db, ping_seconds=(0.05, 0.05), reread_seconds=30)
    reads = []

    def read(after):
        reads.append(after)
        return []

    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        assert [await next_frame(body) for _ in range(3)] == [": ping\n\n"] * 3
        assert reads == [0]
        await body.aclose()


async def test_a_final_event_ends_the_stream_with_no_goodbye(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.sse(
            "lobby", read=lambda after: [Event("closed", event="end", final=True), Event("never")]
        ).body_iterator
        assert await next_frame(body) == "event: end\ndata: closed\n\n"
        await ended(body)
        assert hub.stats().open_streams == 0


async def test_a_stream_at_its_age_limit_says_goodbye_with_a_retry_and_ends(db, caplog):
    hub = make_hub(db, max_stream_seconds=0.2)
    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: [], cursor=3).body_iterator
        assert await next_frame(body) == "id: 3\n\n"
        with caplog.at_level(logging.ERROR, logger="pg_sse"):
            assert (
                await next_frame(body) == "retry: 1000\n\n"
            )  # no end_event: the retry alone, which dispatches nothing
            await ended(body)
    assert caplog.text == ""
    assert hub.stats().open_streams == 0


async def test_the_stream_end_event_name_wins_over_the_hub_default(db):
    hub = make_hub(db, max_stream_seconds=0.2, end_event="end")
    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: [], cursor=3, end_event="bye").body_iterator
        assert await next_frame(body) == "id: 3\n\n"
        assert await next_frame(body) == 'retry: 1000\nevent: bye\ndata: {"reason":"max_age"}\n\n'
        await ended(body)


async def test_a_stream_that_falls_behind_says_so_and_ends(db):
    hub = make_hub(db, queue_size=1, end_event="end")
    async with hub.lifespan():
        body = hub.stream("a", read=lambda after: [], cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        last = asyncio.ensure_future(anext(body))
        await asyncio.sleep(0.05)  # the stream reads, then waits
        hub.wake("a", "x")
        hub.wake(
            "a", "y"
        )  # distinct from the first, which fills the queue of one: identical wake-ups are not queued twice
        assert goodbye(await asyncio.wait_for(last, 2)) == (1000, "behind")
        await ended(body)
        assert hub.stats().open_streams == 0


async def test_shutdown_says_goodbye_to_every_stream_with_a_spread_of_retries(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        bodies = [hub.stream(f"k{i}", read=lambda after: [], cursor=0) for i in range(12)]
        for body in bodies:
            assert await next_frame(body) == "id: 0\n\n"
        lasts = [asyncio.ensure_future(anext(body)) for body in bodies]
        await asyncio.sleep(0.05)  # every stream reads, then waits
        hub.close_all()
        goodbyes = [goodbye(frame) for frame in await asyncio.gather(*lasts)]
        assert {reason for _, reason in goodbyes} == {"shutdown"}
        retries = [retry for retry, _ in goodbyes]
        assert all(1000 <= retry <= 5000 for retry in retries)
        assert len(set(retries)) > 1  # jittered, so a deploy's reconnects do not all land at once
        for body in bodies:
            await ended(body)


async def test_a_stream_refused_after_its_check_says_busy_with_a_long_retry(db):
    hub = make_hub(db, max_subscribers=1, end_event="end")
    async with hub.lifespan():
        late = hub.stream("b", read=lambda after: [], cursor=0)  # passes the check, not yet subscribed
        await next_frame(hub.stream("a", read=lambda after: [], cursor=0))  # takes the one slot
        assert goodbye(await next_frame(late)) == (30000, "busy")
        await ended(late)


async def test_a_stream_that_starts_after_shutdown_began_says_goodbye_quietly(db, caplog):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [], cursor=0)  # passed the caps; not yet subscribed
        hub.close_all()
        with caplog.at_level(logging.ERROR, logger="pg_sse"):
            assert goodbye(await next_frame(body))[1] == "shutdown"
            await ended(body)
    assert caplog.text == ""


async def test_a_failing_read_ends_the_stream_with_no_goodbye_and_logs(db, caplog):
    hub = make_hub(db, end_event="end")

    def read(after):
        raise ConnectionError("database went away")

    async with hub.lifespan():
        body = hub.sse("lobby", read=read).body_iterator
        with caplog.at_level(logging.ERROR, logger="pg_sse"), pytest.raises(StopAsyncIteration):
            await next_frame(body)
    assert "database went away" in caplog.text
    assert hub.stats().open_streams == 0


async def test_read_must_return_events(db, caplog):
    hub = make_hub(db)
    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: [{"not": "an event"}]).body_iterator
        with caplog.at_level(logging.ERROR, logger="pg_sse"), pytest.raises(StopAsyncIteration):
            await next_frame(body)
    assert "pg_sse.Event" in caplog.text


async def test_past_a_cap_the_client_gets_a_503(db):
    hub = make_hub(db, max_subscribers=1)
    async with hub.lifespan():
        await next_frame(hub.sse("lobby", read=lambda after: [], cursor=0).body_iterator)
        with pytest.raises(HTTPException) as refused:
            hub.sse("lobby", read=lambda after: [])
        assert refused.value.status_code == 503
        assert refused.value.headers == {"Retry-After": "30"}


async def test_extra_headers(db):
    hub = make_hub(db)
    async with hub.lifespan():
        response = hub.sse("lobby", read=lambda after: [], headers={"X-Accel-Buffering": "yes", "X-Extra": "1"})
        assert response.headers["x-accel-buffering"] == "yes"
        assert response.headers["x-extra"] == "1"


async def test_stream_refuses_at_a_cap_before_any_frame(db):
    hub = make_hub(db, max_subscribers=1)
    async with hub.lifespan():
        await next_frame(hub.stream("lobby", read=lambda after: [], cursor=0))
        with pytest.raises(TooManySubscribers):
            hub.stream("lobby", read=lambda after: [])


async def test_json_default_encodes_what_json_cannot(db):
    hub = make_hub(db, json_default=str)
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [Event({"at": datetime(2026, 1, 2)}, id=1)], cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        assert await next_frame(body) == 'id: 1\ndata: {"at":"2026-01-02 00:00:00"}\n\n'
        await body.aclose()


async def test_without_json_default_the_stream_ends_and_logs(db, caplog):
    hub = make_hub(db)
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [Event({"at": datetime(2026, 1, 2)})])
        with caplog.at_level(logging.ERROR, logger="pg_sse"), pytest.raises(StopAsyncIteration):
            await next_frame(body)
    assert "datetime" in caplog.text


def test_the_package_imports_and_streams_without_starlette():
    script = """
import asyncio, sys
sys.modules["starlette"] = None
import pg_sse
from pg_sse import Event, Hub

async def main():
    hub = Hub("postgresql://nobody@127.0.0.1:1/none", "c", handle_signals=False)
    hub._loop = asyncio.get_running_loop()  # no listener needed to drive a stream
    body = hub.stream("k", read=lambda after: [Event("x", id=1)], cursor=0)
    assert await anext(body) == "id: 0\\n\\n"
    assert await anext(body) == "id: 1\\ndata: x\\n\\n"
    await body.aclose()

asyncio.run(main())
"""
    result = subprocess.run([sys.executable, "-I", "-c", script], capture_output=True, text=True, check=False)  # noqa: S603
    assert result.returncode == 0, result.stderr


def test_sse_without_starlette_says_what_to_install(monkeypatch):
    monkeypatch.setitem(sys.modules, "starlette.responses", None)
    with pytest.raises(ImportError, match=r"pg-sse\[starlette\]"):
        Hub("postgresql://x", "c").sse("k", read=lambda after: [])


async def test_an_idle_stream_never_goes_quiet_for_longer_than_a_ping_interval(db):
    hub = make_hub(db, ping_seconds=(0.2, 0.2), reread_seconds=0.3, max_stream_seconds=2)
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [], cursor=0)
        gaps, last = [], time.monotonic()
        while True:
            try:
                await next_frame(body)
            except StopAsyncIteration:
                break
            now = time.monotonic()
            gaps.append(now - last)
            last = now
    assert len(gaps) > 5
    assert max(gaps) < 0.2 + 0.1
