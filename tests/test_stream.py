import asyncio
import gc
import json
import logging
import math
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from unittest import mock

import pytest
from helpers import execute, make_hub, make_table, terminate_listener, until
from starlette.exceptions import HTTPException

from pg_sse import EndedTotals, Event, Hub, TooManySubscribers, Wake
from pg_sse import _hub as hub_module

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
    hub = make_hub(db, reread_seconds=30, resync_spread=0)
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
        consumer = consume(hub.stream(None, read=read, cursor=0))
        await until(lambda: len(wakes) == 1)
        await asyncio.sleep(0.05)  # the stream waits for a wake-up
        for key in ("a", "b", "a"):  # all before the stream wakes: one read
            hub._notify(key)
        await until(lambda: len(wakes) == 2)
        assert wakes == [Wake(start=True), Wake(keys=frozenset({"a", "b"}))]
        await stop(consumer)


async def test_an_all_keys_stream_sees_which_keys_each_signal_was_sent_for(db):
    hub = make_hub(db)
    wakes: list[Wake] = []

    async def read(after, wake):
        wakes.append(wake)
        return []

    async with hub.lifespan():
        consumer = consume(hub.stream(None, read=read, cursor=0))
        await until(lambda: len(wakes) == 1)
        await asyncio.sleep(0.05)  # the stream waits for a wake-up
        for key, label in (("a", "typing"), ("b", "typing"), ("a", "presence")):  # posted together: one read
            hub.wake(key, label)
        await until(lambda: len(wakes) == 2)
        assert wakes[1].signal_keys == {"typing": frozenset({"a", "b"}), "presence": frozenset({"a"})}
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


async def test_a_stream_may_have_its_own_age_limit_longer_or_shorter_than_the_hubs(db):
    hub = make_hub(db, max_stream_seconds=60)
    async with hub.lifespan():
        short = hub.sse("lobby", read=lambda after: [], cursor=0, max_stream_seconds=0.2).body_iterator
        assert await next_frame(short) == "id: 0\n\n"
        assert await next_frame(short) == "retry: 1000\n\n"
        await ended(short)
    hub = make_hub(db, max_stream_seconds=0.2, ping_seconds=(0.05, 0.05))
    async with hub.lifespan():
        long = hub.stream("lobby", read=lambda after: [], cursor=0, max_stream_seconds=60)
        assert await next_frame(long) == "id: 0\n\n"
        await asyncio.sleep(0.4)  # past the hub's limit
        assert await next_frame(long) == ": ping\n\n"
        await long.aclose()
    with pytest.raises(ValueError, match="max_stream_seconds"):
        hub.stream("lobby", read=lambda after: [], max_stream_seconds=0)
    with pytest.raises(ValueError, match="line break"):
        hub.stream("lobby", read=lambda after: [], end_event="bye\n")


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
            # no end_event: the retry alone, which dispatches nothing
            assert await next_frame(body) == "retry: 1000\n\n"
            await ended(body)
    assert caplog.text == ""
    assert hub.stats().open_streams == 0
    assert hub.stats().ended_total == EndedTotals(max_age=1)


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
        # distinct from the first, which fills the queue of one: identical wake-ups are not queued twice
        hub.wake("a", "y")
        assert goodbye(await asyncio.wait_for(last, 2)) == (1000, "behind")
        await ended(body)
        assert hub.stats().open_streams == 0
        assert hub.stats().ended_total == EndedTotals(behind=1)


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
        assert hub.stats().ended_total == EndedTotals(shutdown=12)


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


async def test_a_revoked_streams_goodbye_says_so_and_the_stream_ends(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: [], cursor=0, owner="ann").body_iterator
        assert await next_frame(body) == "id: 0\n\n"
        waiting = asyncio.ensure_future(next_frame(body))
        await until(lambda: hub.stats().open_streams == 1)
        hub.close(owner="ann")
        frame = await waiting
        while frame.startswith(":"):  # a ping may come first
            frame = await next_frame(body)
        assert goodbye(frame) == (1000, "revoked")
        await ended(body)
        assert hub.stats().open_streams == 0
        assert hub.stats().ended_total == EndedTotals(revoked=1)


async def test_a_failing_read_is_counted_and_reported_to_on_error(db):
    errors = []
    hub = make_hub(db, on_error=lambda key, err: errors.append((key, err)))

    def read(after):
        raise ConnectionError("database went away")

    async with hub.lifespan():
        assert hub.stats().read_errors_total == 0
        body = hub.sse("lobby", read=read).body_iterator
        with pytest.raises(StopAsyncIteration):
            await next_frame(body)
    assert [(key, str(err)) for key, err in errors] == [("lobby", "database went away")]
    assert hub.stats().read_errors_total == 1


async def test_a_plain_on_error_runs_off_the_loop_and_out_of_the_read_executor(db):
    names = []

    def report(key, err):
        names.append(threading.current_thread().name)

    with ThreadPoolExecutor(1, thread_name_prefix="reads") as executor:
        hub = make_hub(db, read_executor=executor, on_error=report)
        async with hub.lifespan():
            body = hub.stream("lobby", read=lambda after: [{"not": "an event"}])
            with pytest.raises(StopAsyncIteration):
                await next_frame(body)
            await until(lambda: not hub._error_reports)
    # Not behind the reads: in an outage they hold every worker of their pool.
    assert len(names) == 1
    assert names[0] != threading.current_thread().name
    assert not names[0].startswith("reads")


async def test_an_async_on_error_is_run(db, caplog):
    errors = []

    async def report(key, err):
        await asyncio.sleep(0)
        errors.append((key, str(err)))

    hub = make_hub(db, on_error=report)

    def read(after):
        raise ConnectionError("database went away")

    async with hub.lifespan():
        body = hub.sse("lobby", read=read).body_iterator
        with caplog.at_level(logging.ERROR, logger="pg_sse"), pytest.raises(StopAsyncIteration):
            await next_frame(body)
        await until(lambda: not hub._error_reports)
    assert errors == [("lobby", "database went away")]
    assert "on_error callback failed" not in caplog.text


async def test_leaving_the_lifespan_lets_a_running_error_report_finish_and_cancels_a_hung_one(db, monkeypatch):
    monkeypatch.setattr(hub_module, "_ERROR_REPORT_GRACE_SECONDS", 0.3)
    finished, cancelled = [], []

    async def report(key, err):
        try:
            await asyncio.sleep(0.1 if key == "quick" else 30)
            finished.append(key)
        except asyncio.CancelledError:
            cancelled.append(key)
            raise

    def read(after):
        raise ConnectionError("database went away")

    hub = make_hub(db, on_error=report)
    async with hub.lifespan():
        for key in ("quick", "hung"):
            with pytest.raises(StopAsyncIteration):
                await next_frame(hub.stream(key, read=read))
    assert finished == ["quick"]
    await until(lambda: cancelled == ["hung"])


async def test_a_stream_closed_while_the_client_takes_its_first_frame_never_reads(db):
    reads = []
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: reads.append(after) or [], cursor=0, owner="ann")
        assert await next_frame(body) == "id: 0\n\n"
        hub.close(owner="ann")
        assert goodbye(await next_frame(body)) == (1000, "revoked")
        await ended(body)
    assert reads == []


async def test_a_dropped_subscription_frees_its_slot_before_it_is_unsubscribed(db):
    hub = make_hub(db, queue_size=1, max_subscribers=1)
    async with hub.lifespan():
        dropped = hub.subscribe("a")
        hub.wake("a", "x")
        hub.wake("a", "y")
        await until(lambda: dropped.ended)
        assert hub.stats().open_streams == 0
        assert hub.streams() == []
        kept = hub.subscribe("b")  # the one slot is free
        hub.unsubscribe(dropped)
        hub.unsubscribe(kept)


async def test_a_stream_closed_while_the_client_takes_a_ping_says_revoked_and_does_not_read(db):
    reads = []
    hub = make_hub(db, end_event="end", ping_seconds=(0.05, 0.05), reread_seconds=0.05, max_stream_seconds=0.3)
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: reads.append(after) or [], cursor=0, owner="ann")
        assert await next_frame(body) == "id: 0\n\n"
        assert await next_frame(body) == ": ping\n\n"  # the re-read is due once the client takes this frame
        await asyncio.sleep(0.35)  # and so is the age limit
        hub.close(owner="ann")
        assert goodbye(await next_frame(body)) == (1000, "revoked")
        await ended(body)
    assert reads == [0]


async def test_leaving_the_lifespan_also_waits_for_a_report_that_starts_while_it_waits(db):
    finished = []

    async def report(key, err):
        await asyncio.sleep(0.05)
        finished.append(key)
        if key == "first":
            hub._report("late", err)  # as a read still running in a thread would, failing during shutdown

    hub = make_hub(db, on_error=report)
    async with hub.lifespan():
        hub._report("first", ConnectionError("database went away"))
    assert finished == ["first", "late"]


async def test_a_goodbye_the_client_never_took_is_not_counted(db):
    hub = make_hub(db, max_stream_seconds=0.2)
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [], cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        assert goodbye(await next_frame(body)) == (1000, None)  # handed over; the client leaves before taking it
        await body.aclose()
        assert hub.stats().ended_total.max_age == 0


async def test_an_async_on_error_runs_without_holding_the_stream_open(db, caplog):
    errors = []
    tracker_up = asyncio.Event()

    async def report(key, err):
        await tracker_up.wait()  # a slow error tracker
        errors.append((key, str(err)))
        raise RuntimeError("reporter down")

    hub = make_hub(db, on_error=report)

    def read(after):
        raise ConnectionError("database went away")

    async with hub.lifespan():
        body = hub.sse("lobby", read=read).body_iterator
        with pytest.raises(StopAsyncIteration):
            await next_frame(body)
        assert hub.stats().open_streams == 0  # the slot is free while the report is still waiting
        assert errors == []
        with caplog.at_level(logging.ERROR, logger="pg_sse"):
            tracker_up.set()
            await until(lambda: not hub._error_reports)
    assert errors == [("lobby", "database went away")]
    assert "reporter down" in caplog.text


async def test_a_tuple_cursor_is_refused_in_the_route_and_from_start(db):
    # IntCursor.parse returns a tuple. Sent as "id: (12, 3)", it would come back as a Last-Event-ID nothing parses.
    errors = []
    hub = make_hub(db, on_error=lambda key, err: errors.append(err))
    async with hub.lifespan():
        with pytest.raises(TypeError, match=r"IntCursor\.format"):
            hub.stream("lobby", read=lambda after: [], cursor=(12, 3))
        assert hub.stats().open_streams == 0
        body = hub.stream("lobby", read=lambda after: [], start=lambda: (12,))  # a fetched row, not its value
        with pytest.raises(StopAsyncIteration):
            await next_frame(body)
    assert [str(err) for err in errors] == [
        "pg_sse: the cursor start() returned cannot be a tuple: pass the value itself (row[0], not the row), "
        "IntCursor.parse_one's int, or IntCursor.format(*values)"
    ]


async def test_a_start_that_finds_no_cursor_leaves_it_to_read(db):
    reads = []
    hub = make_hub(db)
    async with hub.lifespan():
        consumer = consume(hub.stream("lobby", read=lambda after: reads.append(after) or [], start=lambda: None))
        await until(lambda: reads == [None])
        await stop(consumer)


async def test_an_on_error_that_raises_is_logged_and_the_stream_still_ends(db, caplog):
    def broken(key, err):
        raise RuntimeError("reporter down")

    hub = make_hub(db, on_error=broken)
    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: [{"not": "an event"}]).body_iterator
        with caplog.at_level(logging.ERROR, logger="pg_sse"):
            with pytest.raises(StopAsyncIteration):
                await next_frame(body)
            await until(lambda: not hub._error_reports)
    assert "reporter down" in caplog.text
    assert hub.stats().read_errors_total == 1


async def test_a_plain_read_and_start_run_in_the_given_executor_and_an_async_read_does_not(db):
    names = []

    def read(after):
        names.append(("read", threading.current_thread().name))
        return []

    def start():
        names.append(("start", threading.current_thread().name))
        return 0

    async def async_read(after):
        names.append(("async", threading.current_thread().name))
        return []

    with ThreadPoolExecutor(1, thread_name_prefix="reads") as executor:
        hub = make_hub(db, read_executor=executor)
        async with hub.lifespan():
            for fn in (read, async_read):
                frames = hub.stream("lobby", read=fn, start=start)
                consumer = consume(frames)
                await until(lambda fn=fn: any(kind == fn.__name__.split("_")[0] for kind, _ in names))
                await stop(consumer)
    by_kind = dict(names)
    assert by_kind["read"].startswith("reads")
    assert by_kind["start"].startswith("reads")
    assert not by_kind["async"].startswith("reads")


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
        first = hub.sse("lobby", read=lambda after: [], cursor=0)  # held, as a server holds the response it sends
        with pytest.raises(HTTPException) as refused:
            hub.sse("lobby", read=lambda after: [])
        assert refused.value.status_code == 503
        assert refused.value.headers == {"Retry-After": "30"}
        del first


async def test_refuse_with_replaces_the_503(db):
    class Unavailable(Exception):
        pass

    hub = make_hub(db, max_subscribers=1)
    async with hub.lifespan():
        first = hub.sse("lobby", read=lambda after: [], cursor=0)
        with pytest.raises(Unavailable) as refused:
            hub.sse("lobby", read=lambda after: [], refuse_with=lambda err: Unavailable(str(err)))
        assert str(refused.value) == "too many open streams"
        del first


async def test_extra_headers_are_added_to_the_response(db):
    hub = make_hub(db)
    async with hub.lifespan():
        response = hub.sse("lobby", read=lambda after: [], headers={"X-Accel-Buffering": "yes", "X-Extra": "1"})
        assert response.headers["x-accel-buffering"] == "yes"
        assert response.headers["x-extra"] == "1"


async def test_what_is_queued_before_the_first_read_costs_no_second_read(db):
    hub = make_hub(db)
    wakes: list[Wake] = []

    async def read(after, wake):
        wakes.append(wake)
        return []

    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0)
        hub._notify("lobby")  # after the slot was taken, before the first read
        consumer = consume(body)
        await until(lambda: len(wakes) == 1)
        await asyncio.sleep(0.1)
        assert wakes == [Wake(start=True)]
        await stop(consumer)


async def test_a_burst_before_the_first_read_does_not_drop_the_stream(db):
    hub = make_hub(db, queue_size=2)
    async with hub.lifespan():
        body = hub.stream(None, read=lambda after: [], cursor=0)
        for key in ("a", "b", "c"):  # more than the queue holds, all before the first read
            hub._notify(key)
        assert hub.stats().dropped_total == 0
        assert await next_frame(body) == "id: 0\n\n"
        assert hub.stats().open_streams == 1
        await body.aclose()


async def test_a_resync_before_the_first_read_is_covered_by_it(db):
    hub = make_hub(db)
    wakes: list[Wake] = []

    def read(after, wake):
        wakes.append(wake)
        return []

    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0)
        (subscription,) = hub._subscriptions
        subscription.resync()  # the listener reconnected before the stream read anything
        consumer = consume(body)
        await until(lambda: len(wakes) == 1)
        assert wakes == [Wake(start=True)]
        await stop(consumer)


@pytest.mark.parametrize("max_stream_seconds", [60, math.inf])
async def test_a_stream_never_read_gives_its_slot_back_and_then_just_ends(db, monkeypatch, max_stream_seconds):
    monkeypatch.setattr(hub_module, "_UNREAD_SECONDS", 0.2)
    hub = make_hub(db, max_stream_seconds=max_stream_seconds, end_event="end")
    async with hub.lifespan():
        unread = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")  # its response was never sent
        await asyncio.sleep(0.3)  # held all along (a reference cycle, a runtime without reference counting)
        assert hub.stats().open_streams == 0
        assert hub._by_owner == {}
        await ended(unread)  # it never streamed: no goodbye, and no age limit counted
        assert hub.stats().ended_total == EndedTotals()


async def test_a_stream_being_read_is_not_unread_however_slow_its_start(db, monkeypatch):
    monkeypatch.setattr(hub_module, "_UNREAD_SECONDS", 0.1)
    release = asyncio.Event()

    async def start():
        await release.wait()  # a database stall
        return 0

    hub = make_hub(db)
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [], start=start)
        first = asyncio.ensure_future(next_frame(body))
        await asyncio.sleep(0.3)  # past the unread limit, still in `start`
        assert hub.stats().open_streams == 1
        release.set()
        assert await first == "id: 0\n\n"
        await body.aclose()


async def test_a_stream_takes_its_slot_when_it_is_made_not_when_it_is_first_read(db):
    hub = make_hub(db, max_subscribers=1)
    async with hub.lifespan():
        first = hub.stream("lobby", read=lambda after: [], cursor=0)
        assert hub.stats().open_streams == 1
        with pytest.raises(TooManySubscribers):
            hub.stream("lobby", read=lambda after: [])
        assert await next_frame(first) == "id: 0\n\n"
        await first.aclose()
        assert hub.stats().open_streams == 0


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


def resync_reads(hub, wakes: list[tuple[float, Wake]], *, streams: int = 1):
    """Opens `streams` streams on "lobby" whose reads log (time, why) in `wakes`; returns their consumers."""

    def read(after, wake):
        wakes.append((time.monotonic(), wake))
        return []

    return [consume(hub.stream("lobby", read=read, cursor=0)) for _ in range(streams)]


async def test_a_resync_read_waits_a_random_time_and_a_notification_ends_the_wait(db, monkeypatch):
    monkeypatch.setattr(hub_module.random, "uniform", lambda low, high: high)  # the longest wait
    hub = make_hub(db, resync_spread=0.6)
    wakes: list[tuple[float, Wake]] = []
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0.05)  # the first LISTEN's resync is delivered before the stream opens
        (consumer,) = resync_reads(hub, wakes)
        await until(lambda: len(wakes) == 1)
        hub._resync()
        await asyncio.sleep(0.3)
        assert len(wakes) == 1  # held back, not dropped
        await until(lambda: len(wakes) == 2)
        assert wakes[1][1] == Wake(resync=True)
        hub._resync()
        hub._notify("lobby")
        await until(lambda: len(wakes) == 3, timeout=0.4)  # the notification did not wait for the spread
        assert wakes[2][1] == Wake(keys=frozenset({"lobby"}), resync=True)
        await stop(consumer)


async def test_the_resync_reads_of_many_streams_are_spread_out(db):
    hub = make_hub(db, resync_spread=0.5)
    wakes: list[tuple[float, Wake]] = []
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0.05)
        consumers = resync_reads(hub, wakes, streams=40)
        await until(lambda: len(wakes) == 40)
        hub._resync()
        await until(lambda: len(wakes) == 80)
        times = [at for at, wake in wakes if wake.resync]
        assert len(times) == 40
        assert max(times) - min(times) > 0.1
        for consumer in consumers:
            await stop(consumer)


async def test_a_second_resync_while_one_is_waiting_does_not_move_its_time(db, monkeypatch):
    draws: list[tuple[float, float]] = []

    def uniform(low, high):
        draws.append((low, high))
        return high

    monkeypatch.setattr(hub_module.random, "uniform", uniform)
    hub = make_hub(db, resync_spread=0.5, ping_seconds=(30, 30))
    wakes: list[tuple[float, Wake]] = []
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0.05)
        (consumer,) = resync_reads(hub, wakes)
        await until(lambda: len(wakes) == 1)
        draws.clear()
        hub._resync()
        await asyncio.sleep(0.1)
        hub._resync()  # the listener reconnected again; the first wait stands
        await until(lambda: len(wakes) == 2)
        assert wakes[1][1] == Wake(resync=True)
        assert draws.count((0, 0.5)) == 1  # one draw: the second marker did not start another wait
        await stop(consumer)


@pytest.mark.parametrize("bad", [float("nan"), 0, -1, True, "60"])
async def test_an_age_limit_that_is_not_a_positive_number_is_refused(db, bad):
    with pytest.raises(ValueError, match="max_stream_seconds"):
        make_hub(db, max_stream_seconds=bad)
    hub = make_hub(db)
    async with hub.lifespan():
        with pytest.raises(ValueError, match="max_stream_seconds"):
            hub.stream("lobby", read=lambda after: [], max_stream_seconds=bad)


async def test_a_close_between_creating_a_stream_and_reading_its_first_frame_still_ends_it(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: [], cursor=0, owner="ann").body_iterator
        other = hub.sse("lobby", read=lambda after: [], cursor=0, owner="bob").body_iterator
        hub.close(owner="ann")
        await asyncio.sleep(0.1)  # the close runs on the loop before either stream has subscribed
        assert goodbye(await next_frame(body)) == (1000, "revoked")
        await ended(body)
        assert await next_frame(other) == "id: 0\n\n"
        await other.aclose()
        assert hub.stats().open_streams == 0


async def test_a_stream_closed_while_its_read_runs_sends_the_goodbye_and_not_what_the_read_found(db):
    hub = make_hub(db, end_event="end")
    release = threading.Event()

    def read(after):
        release.wait(3)
        return [Event("late", id=1)]

    async with hub.lifespan():
        body = hub.sse("lobby", read=read, cursor=0, owner="ann").body_iterator
        assert await next_frame(body) == "id: 0\n\n"
        waiting = asyncio.ensure_future(next_frame(body))
        await until(lambda: hub.stats().open_streams == 1)
        hub.close(owner="ann")
        await asyncio.sleep(0.1)
        release.set()
        assert goodbye(await waiting) == (1000, "revoked")
        await ended(body)


async def test_a_stream_closed_while_its_start_runs_sends_the_goodbye_and_no_cursor(db):
    hub = make_hub(db, end_event="end")
    release = threading.Event()

    def start():
        release.wait(3)
        return 7

    async with hub.lifespan():
        body = hub.sse("lobby", read=lambda after: [], start=start, owner="ann").body_iterator
        first = asyncio.ensure_future(next_frame(body))
        await until(lambda: hub.stats().open_streams == 1)
        hub.close(owner="ann")
        await asyncio.sleep(0.1)
        release.set()
        assert goodbye(await first) == (1000, "revoked")
        await ended(body)


async def test_a_notification_during_a_held_back_resync_reads_at_once_and_covers_the_resync(db, monkeypatch):
    monkeypatch.setattr(hub_module.random, "uniform", lambda low, high: high)  # a random wait could be under 0.1 s
    hub = make_hub(db, resync_spread=5, ping_seconds=(30, 30))
    wakes: list[tuple[float, Wake]] = []
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0.05)
        (consumer,) = resync_reads(hub, wakes)
        await until(lambda: len(wakes) == 1)
        hub._resync()
        await asyncio.sleep(0.1)  # the resync is now held back, with the notification not yet sent
        hub._notify("lobby")
        await until(lambda: len(wakes) == 2, timeout=1)
        assert wakes[1][1] == Wake(keys=frozenset({"lobby"}), resync=True)
        await stop(consumer)


async def test_a_resync_wakes_a_waiting_stream_at_once(db):
    hub = make_hub(db, ping_seconds=(30, 30))
    wakes: list[Wake] = []

    def read(after, wake):
        wakes.append(wake)
        return []

    async with hub.lifespan():
        consumer = consume(hub.stream("lobby", read=read, cursor=0))
        await until(lambda: len(wakes) == 1)
        (subscription,) = hub._subscriptions
        subscription.resync()
        await until(lambda: len(wakes) == 2, timeout=1)
        assert wakes[1] == Wake(resync=True)
        assert not subscription.resync_pending
        await stop(consumer)


async def test_a_read_that_takes_no_wake_covers_a_held_back_resync_whatever_woke_it(db, monkeypatch):
    monkeypatch.setattr(hub_module.random, "uniform", lambda low, high: high)
    hub = make_hub(db, resync_spread=0.3, ping_seconds=(30, 30))
    reads: list[int] = []

    def read(after):  # re-reads everything after its cursor, whatever woke it
        reads.append(after)
        return []

    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0.05)
        consumer = consume(hub.stream("lobby", read=read, cursor=0))
        await until(lambda: len(reads) == 1)
        hub._resync()
        hub.wake("lobby", "typing")  # read at once, and that read is the resync
        await until(lambda: len(reads) == 2)
        await asyncio.sleep(0.5)  # past the held-back resync's time
        assert len(reads) == 2
        await stop(consumer)


async def test_a_signal_during_a_held_back_resync_reads_at_once_and_the_resync_still_comes(db, monkeypatch):
    monkeypatch.setattr(hub_module.random, "uniform", lambda low, high: high)
    hub = make_hub(db, resync_spread=0.5, ping_seconds=(30, 30))
    wakes: list[tuple[float, Wake]] = []
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0.05)
        (consumer,) = resync_reads(hub, wakes)
        await until(lambda: len(wakes) == 1)
        hub._resync()
        await asyncio.sleep(0.1)
        hub.wake("lobby", "typing")
        await until(lambda: len(wakes) == 2, timeout=0.3)
        assert wakes[1][1] == Wake(signals=frozenset({"typing"}))  # a signal is not a reason to re-read early
        await until(lambda: len(wakes) == 3)
        assert wakes[2][1] == Wake(resync=True)
        assert wakes[2][0] - wakes[0][0] >= 0.45
        await stop(consumer)


async def test_a_stream_closed_partway_through_a_batch_sends_the_goodbye_and_not_the_rest(db):
    hub = make_hub(db)
    async with hub.lifespan():
        batch = [Event(n, id=n) for n in range(1, 6)]
        body = hub.stream("lobby", read=lambda after: batch if after == 0 else [], cursor=0, owner="ann")
        assert await next_frame(body) == "id: 0\n\n"
        assert await next_frame(body) == "id: 1\ndata: 1\n\n"
        hub.close(owner="ann")
        assert goodbye(await next_frame(body)) == (1000, None)
        await ended(body)


async def test_a_stream_opened_right_after_a_close_on_the_loop_is_not_revoked(db):
    hub = make_hub(db)
    async with hub.lifespan():
        old = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(old) == "id: 0\n\n"
        hub.close(owner="ann", key="lobby")
        new = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert goodbye(await next_frame(old)) == (1000, None)
        await ended(old)
        assert await next_frame(new) == "id: 0\n\n"
        assert hub.stats().open_streams == 1
        await new.aclose()


async def test_an_ended_stream_whose_client_never_takes_the_goodbye_gives_its_slot_back_after_a_grace(db, monkeypatch):
    monkeypatch.setattr(hub_module, "_GOODBYE_GRACE_SECONDS", 0.2)
    hub = make_hub(db, max_per_owner=1)
    async with hub.lifespan():
        stalled = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(stalled) == "id: 0\n\n"  # then its client stops reading: a half-open connection
        hub.close(owner="ann")
        with pytest.raises(TooManySubscribers):
            hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        await asyncio.sleep(0.3)
        assert hub.stats().open_streams == 0
        new = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(new) == "id: 0\n\n"
        await new.aclose()
        await stalled.aclose()


async def test_a_stream_closed_while_its_client_takes_the_final_event_says_no_goodbye(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [Event("bye", id=1, final=True)], cursor=0, owner="ann")
        assert await next_frame(body) == "id: 0\n\n"
        assert await next_frame(body) == "id: 1\ndata: bye\n\n"
        hub.close(owner="ann")  # while the client takes the final event
        await ended(body)
        assert hub.stats().ended_total.revoked == 0


async def test_a_stronger_end_does_not_stretch_the_goodbye_grace(db, monkeypatch):
    monkeypatch.setattr(hub_module, "_GOODBYE_GRACE_SECONDS", 0.3)
    hub = make_hub(db)
    async with hub.lifespan():
        stalled = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(stalled) == "id: 0\n\n"
        hub.close(owner="ann")
        await asyncio.sleep(0.2)
        hub.close_all()  # shutdown replaces the revoke, but the grace still runs from the revoke
        await asyncio.sleep(0.2)
        assert hub.stats().open_streams == 0
        await stalled.aclose()


async def test_a_stream_closed_while_start_finds_no_cursor_does_not_read(db):
    reads = []
    release = asyncio.Event()

    async def start():
        await release.wait()
        return None  # an empty table: no cursor, so no frame before the first read

    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: reads.append(after) or [], start=start, owner="ann")
        first = asyncio.ensure_future(next_frame(body))
        await asyncio.sleep(0.05)  # in `start`
        hub.close(owner="ann")
        release.set()
        assert goodbye(await first) == (1000, "revoked")
        await ended(body)
    assert reads == []


async def test_a_close_all_from_another_thread_does_not_reach_a_later_run(db):
    hub = make_hub(db)
    posted = []
    async with hub.lifespan():
        # Handed to the loop, and held back here until the hub has restarted.
        with mock.patch.object(hub, "_post", side_effect=lambda callback, *args: posted.append((callback, args))):
            closer = threading.Thread(target=hub.close_all)
            closer.start()
            closer.join()
    ((callback, args),) = posted
    async with hub.lifespan():
        callback(*args)  # it finally runs
        assert not hub.closing
        body = hub.stream("lobby", read=lambda after: [], cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        await body.aclose()


async def test_a_read_that_fails_once_shutdown_closed_its_stream_says_shutdown_and_reports_nothing(db, caplog):
    reports = []
    release = threading.Event()

    def read(after):
        release.wait(3)
        raise ConnectionError("the pool closed first")  # the app's pool, closed after the hub during shutdown

    hub = make_hub(db, end_event="end", on_error=lambda key, err: reports.append(err))
    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        failing = asyncio.ensure_future(next_frame(body))  # in its read when the hub stops
        await asyncio.sleep(0.05)
    release.set()
    with caplog.at_level(logging.ERROR, logger="pg_sse"):
        assert goodbye(await failing)[1] == "shutdown"
    await ended(body)
    assert reports == []
    assert hub.stats().read_errors_total == 0
    assert "failed" not in caplog.text


async def test_a_stream_dropped_before_its_body_started_gives_its_slot_back_at_once(db):
    hub = make_hub(db, max_per_owner=1)
    async with hub.lifespan():
        hub.sse("lobby", read=lambda after: [], cursor=0, owner="ann")  # the browser left before the body started
        gc.collect()
        await asyncio.sleep(0)  # the collector may run on any thread: the loop gives the slot back
        assert hub.stats().open_streams == 0
        reload = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")  # the reload is let in at once
        assert await next_frame(reload) == "id: 0\n\n"
        await reload.aclose()


async def test_a_stream_at_its_age_limit_is_ended_so_a_burst_during_its_goodbye_does_not_drop_it(db):
    hub = make_hub(db, max_stream_seconds=0.3, queue_size=2)
    async with hub.lifespan():
        body = hub.stream("k", read=lambda after: [], cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        assert goodbye(await next_frame(body)) == (1000, None)  # handed over, not yet taken
        for n in range(5):
            hub.wake("k", signal=f"s{n}")
        await asyncio.sleep(0.05)
        assert hub.stats().dropped_total == 0
        assert [info.ended for info in hub.streams()] == [True]
        await ended(body)
        assert hub.stats().ended_total.max_age == 1


async def test_what_arrives_during_start_costs_no_second_read(db):
    wakes: list[Wake] = []
    release = asyncio.Event()

    async def start():
        await release.wait()
        return 0

    def read(after, wake):
        wakes.append(wake)
        return []

    hub = make_hub(db, ping_seconds=(30, 30))
    async with hub.lifespan():
        consumer = consume(hub.stream("lobby", read=read, start=start))
        await asyncio.sleep(0.05)  # in `start`
        hub._notify("lobby")
        hub.wake("lobby", "typing")
        await asyncio.sleep(0.05)
        release.set()
        await until(lambda: len(wakes) == 1)
        await asyncio.sleep(0.1)
        assert wakes == [Wake(start=True)]  # the first read covered both
        await stop(consumer)


async def test_a_client_cursor_that_cannot_be_an_id_starts_the_stream_from_now(db, caplog):
    hub = make_hub(db)
    async with hub.lifespan():
        with caplog.at_level(logging.WARNING, logger="pg_sse"):
            body = hub.stream("lobby", read=lambda after: [], cursor="12\n3", start=lambda: 7)
        assert await next_frame(body) == "id: 7\n\n"  # as if there were no cursor: `start` gives one
        await body.aclose()
        with caplog.at_level(logging.WARNING, logger="pg_sse"):
            again = hub.stream("lobby", read=lambda after: [], cursor="\x00", start=lambda: 7)
        await again.aclose()
        with pytest.raises(TypeError):
            hub.stream("lobby", read=lambda after: [], cursor=(12, 3))  # a tuple is the code's mistake
    assert caplog.text.count("cannot be an SSE id") == 1  # a client sends what it likes: once a minute is enough


async def test_a_cursor_the_code_got_wrong_still_raises_in_the_route(db):
    hub = make_hub(db)
    async with hub.lifespan():
        with pytest.raises(ValueError, match="integer string conversion"):
            hub.stream("lobby", read=lambda after: [], cursor=10**5000)  # not a client's line break: the code's int
        assert hub.stats().open_streams == 0


async def test_a_close_cancels_an_async_read_in_flight_and_says_goodbye_at_once(db):
    cancelled = []

    async def read(after):
        if after == 0:
            try:
                await asyncio.sleep(30)  # a query stuck on a stalled database
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
        return []

    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0, owner="ann")
        assert await next_frame(body) == "id: 0\n\n"
        pending = asyncio.ensure_future(next_frame(body, timeout=1))
        await asyncio.sleep(0.05)  # in its read
        hub.close(owner="ann")
        assert goodbye(await pending) == (1000, "revoked")  # not after the 30 s read
        await ended(body)
    assert cancelled == [True]


async def test_reads_run_in_the_streams_own_task(db):
    tasks = []

    async def read(after, wake):
        tasks.append(asyncio.current_task())  # what an app scopes to the task, a session say, is the stream's
        return []

    hub = make_hub(db, ping_seconds=(30, 30))
    async with hub.lifespan():
        consumer = consume(hub.stream("lobby", read=read, cursor=0))
        await until(lambda: len(tasks) == 1)
        hub.wake("lobby", "typing")
        await until(lambda: len(tasks) == 2)
        assert tasks == [consumer, consumer]
        await stop(consumer)


@pytest.mark.parametrize("swallows", [False, True])
async def test_a_read_that_mishandles_the_closes_cancellation_still_says_goodbye(db, swallows):
    reports = []

    async def read(after):
        if after == 0:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                if swallows:  # carries on as if nothing happened
                    return [Event("late", id=1)]
                raise ConnectionError("the pool closed under it") from None  # its cleanup fails
        return []

    hub = make_hub(db, end_event="end", on_error=lambda key, err: reports.append(err))
    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0)
        assert await next_frame(body) == "id: 0\n\n"

        async def drive():
            frame = await next_frame(body, timeout=1)
            return frame, asyncio.current_task().cancelling()

        pending = asyncio.ensure_future(drive())
        await asyncio.sleep(0.05)
        hub.close_all()
        frame, cancelling = await pending
        assert goodbye(frame)[1] == "shutdown"
        assert cancelling == 0  # the close's cancellation was given back
        await ended(body)
    assert reports == []


async def test_a_close_does_not_wait_for_a_plain_read_running_in_a_thread(db):
    release = threading.Event()

    def read(after):
        release.wait(5)  # cannot be cancelled: it finishes in its thread, unsent
        return [Event("late", id=1)]

    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        pending = asyncio.ensure_future(next_frame(body, timeout=1))
        await asyncio.sleep(0.05)
        hub.close_all()
        assert goodbye(await pending)[1] == "shutdown"
        await ended(body)
        release.set()


async def test_a_response_that_cannot_be_built_gives_its_slot_back(db):
    hub = make_hub(db)
    async with hub.lifespan():
        with pytest.raises(UnicodeEncodeError):  # Starlette encodes headers as Latin-1 when it builds the response
            hub.sse("lobby", read=lambda after: [], headers={"X-Room": "café ☕"})
        assert hub.stats().open_streams == 0


async def test_closing_an_ended_stream_again_does_not_restart_its_grace(db, monkeypatch):
    monkeypatch.setattr(hub_module, "_GOODBYE_GRACE_SECONDS", 0.3)
    hub = make_hub(db)
    async with hub.lifespan():
        stalled = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(stalled) == "id: 0\n\n"
        hub.close(owner="ann")
        await asyncio.sleep(0.2)
        hub.close(owner="ann")  # each logout or failed re-auth calls it again
        await asyncio.sleep(0.2)
        assert hub.stats().open_streams == 0  # 0.4 s after the first close: the grace ran from it
        await stalled.aclose()


async def test_close_all_from_another_thread_ends_every_stream_on_the_loop(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        body = hub.stream("lobby", read=lambda after: [], cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        await asyncio.to_thread(hub.close_all)
        assert goodbye(await next_frame(body))[1] == "shutdown"
        await ended(body)
        with pytest.raises(TooManySubscribers):
            hub.stream("lobby", read=lambda after: [])


async def test_a_revoked_stream_holds_its_slot_until_it_has_said_goodbye(db):
    hub = make_hub(db, max_per_owner=1)
    async with hub.lifespan():
        old = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(old) == "id: 0\n\n"
        hub.close(owner="ann")
        with pytest.raises(TooManySubscribers):  # a client that stopped reading cannot pile up connections
            hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert [info.ended for info in hub.streams()] == [True]
        assert goodbye(await next_frame(old)) == (1000, None)
        assert hub.stats().open_streams == 1  # handed over, not yet taken: the slot is still held
        await ended(old)
        new = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(new) == "id: 0\n\n"
        await new.aclose()


async def test_a_stream_revoked_before_it_starts_says_revoked_and_gives_its_slot_back(db):
    hub = make_hub(db, max_subscribers=1)
    async with hub.lifespan():
        revoked = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        hub.close(owner="ann")
        assert goodbye(await next_frame(revoked)) == (1000, None)
        await ended(revoked)
        assert hub.stats().ended_total.revoked == 1
        other = hub.stream("lobby", read=lambda after: [], cursor=0, owner="bob")  # the one slot is free again
        assert await next_frame(other) == "id: 0\n\n"
        await other.aclose()


def first_frame_now(body) -> str:
    """Runs a stream to its first frame without yielding to the loop, so nothing posted from a thread runs meanwhile."""
    with pytest.raises(StopIteration) as done:
        anext(body).send(None)
    return done.value.value


async def test_a_close_from_another_thread_spares_streams_opened_after_it_was_called(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        old = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        assert await next_frame(old) == "id: 0\n\n"
        pending = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        # Blocking the loop while the thread calls close: the close it posts runs only after the streams below exist.
        closer = threading.Thread(target=hub.close, kwargs={"owner": "ann"})
        closer.start()
        closer.join()
        assert first_frame_now(pending) == "id: 0\n\n"  # created before the call, subscribed after it
        new = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")  # the user signed in again
        unread = hub.stream("lobby", read=lambda after: [], cursor=0, owner="ann")
        await asyncio.sleep(0.05)  # the posted close runs
        assert goodbye(await next_frame(old)) == (1000, "revoked")
        assert goodbye(await next_frame(pending)) == (1000, "revoked")
        assert await next_frame(new) == "id: 0\n\n"
        assert await next_frame(unread) == "id: 0\n\n"
        await ended(old)
        await ended(pending)
        assert hub.stats().ended_total.revoked == 2
        await new.aclose()
        await unread.aclose()


async def test_a_stream_created_before_a_restart_and_read_after_it_says_shutdown(db):
    hub = make_hub(db, end_event="end")
    async with hub.lifespan():
        stale = hub.stream("lobby", read=lambda after: [], cursor=0)
    async with hub.lifespan():
        assert goodbye(await next_frame(stale))[1] == "shutdown"
        await ended(stale)
        assert hub.stats().open_streams == 0


async def test_an_infinite_age_limit_turns_the_limit_off(db):
    hub = make_hub(db, max_stream_seconds=math.inf, ping_seconds=(0.05, 0.05))
    async with hub.lifespan():
        for body in (
            hub.stream("lobby", read=lambda after: [], cursor=0),
            hub.stream("lobby", read=lambda after: [], cursor=0, max_stream_seconds=math.inf),
        ):
            assert await next_frame(body) == "id: 0\n\n"
            assert await next_frame(body) == ": ping\n\n"
            await body.aclose()


@pytest.mark.parametrize(
    ("close", "reason"), [(lambda hub: hub.close(owner="ann"), "revoked"), (Hub.close_all, "shutdown")]
)
async def test_a_stream_dropped_while_its_read_runs_then_closed_does_not_send_what_the_read_found(db, close, reason):
    hub = make_hub(db, queue_size=2, end_event="end")
    release = threading.Event()
    reading = threading.Event()

    def read(after):
        reading.set()
        release.wait(3)
        return [Event(1, id=1)]

    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0, owner="ann")
        assert await next_frame(body) == "id: 0\n\n"
        waiting = asyncio.ensure_future(next_frame(body))
        await until(reading.is_set)  # a wake-up before the first read starts is covered by that read, so not queued
        for signal_name in ("x", "y", "z"):  # three distinct wakes overflow a queue of two
            hub.wake("lobby", signal_name)
        await until(lambda: hub.stats().dropped_total == 1)
        close(hub)
        release.set()
        assert goodbye(await waiting)[1] == reason  # not "id: 1": the read's result is dropped
        await ended(body)
        assert hub._subscriptions == set()


async def test_a_stream_dropped_while_its_read_runs_still_sends_what_the_read_found_then_says_behind(db):
    hub = make_hub(db, queue_size=2, end_event="end")
    release = threading.Event()
    reading = threading.Event()

    def read(after):
        if after == 0:
            reading.set()
            release.wait(3)
            return [Event(1, id=1), Event(2, id=2)]
        return []

    async with hub.lifespan():
        body = hub.stream("lobby", read=read, cursor=0)
        assert await next_frame(body) == "id: 0\n\n"
        waiting = asyncio.ensure_future(next_frame(body))
        await until(reading.is_set)  # a wake-up before the first read starts is covered by that read, so not queued
        for signal_name in ("x", "y", "z"):  # three distinct wakes overflow a queue of two
            hub.wake("lobby", signal_name)
        await until(lambda: hub.stats().dropped_total == 1)
        release.set()
        # The cursor moves on, so the reconnect has less to catch up instead of the same backlog again.
        assert await waiting == "id: 1\ndata: 1\n\n"
        assert await next_frame(body) == "id: 2\ndata: 2\n\n"
        assert goodbye(await next_frame(body)) == (1000, "behind")
        await ended(body)
