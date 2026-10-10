import asyncio

import pytest
from helpers import make_hub, until

from pg_sse import Event, distinct
from pg_sse._hub import _accepts_wake
from pg_sse.testing import next_event

pytestmark = pytest.mark.anyio


def summary(event: Event):
    return event.data.get("summary")


async def run(read, times: int) -> list[list[Event]]:
    return [await read(None) for _ in range(times)]


async def test_the_first_event_is_always_sent_an_unchanged_one_is_dropped_and_a_changed_one_is_sent():
    a, b = Event({"summary": 1}), Event({"summary": 2})
    sequence = iter([[a], [a], [b], [b], [a]])
    read = distinct(lambda cursor: next(sequence), key=summary)
    assert await run(read, 5) == [[a], [], [b], [], [a]]


async def test_none_always_sends_and_the_next_event_is_compared_with_nothing():
    snapshot, rows = Event({"summary": 1}), Event({"summary": None, "rows": [1]})
    sequence = iter([[snapshot], [rows], [rows], [snapshot], [snapshot]])
    read = distinct(lambda cursor: next(sequence), key=summary)
    assert await run(read, 5) == [[snapshot], [rows], [rows], [snapshot], []]


async def test_a_final_event_is_always_sent():
    done = Event({"summary": 1}, final=True)
    sequence = iter([[Event({"summary": 1})], [done]])
    read = distinct(lambda cursor: next(sequence), key=summary)
    assert await run(read, 2) == [[Event({"summary": 1})], [done]]


async def test_one_read_may_return_several_events():
    a, b = Event({"summary": 1}), Event({"summary": 2})
    read = distinct(lambda cursor: [a, a, b, b, a], key=summary)
    assert await read(None) == [a, b, a]


async def test_two_streams_do_not_share_state():
    a = Event({"summary": 1})

    def plain(cursor):
        return [a]

    first, second = distinct(plain, key=summary), distinct(plain, key=summary)
    assert await first(None) == [a]
    assert await first(None) == []
    assert await second(None) == [a]  # the second stream has never been sent it


async def test_the_wrapper_keeps_the_shape_of_read():
    def sync_cursor(cursor):
        return []

    async def async_wake(cursor, wake):
        return []

    def defaulted(cursor, limit=5):
        return []

    assert not _accepts_wake(distinct(sync_cursor, key=summary))
    assert not _accepts_wake(distinct(defaulted, key=summary))
    wrapped = distinct(async_wake, key=summary)
    assert _accepts_wake(wrapped)
    assert await wrapped(None, "a wake") == []


async def test_a_stream_through_the_hub_sends_a_snapshot_only_when_it_changed(db):
    hub = make_hub(db)
    shown = {"summary": 1}
    reads = []

    def read(cursor, wake):
        reads.append(wake)
        return [Event(dict(shown), event="snapshot")]

    async with hub.lifespan():
        frames = hub.stream("room", read=distinct(read, key=summary))
        assert (await next_event(frames)).json() == {"summary": 1}
        waiting = asyncio.ensure_future(next_event(frames, timeout=0.3))  # a stream only runs while it is read
        hub.wake("room")
        await until(lambda: len(reads) == 2)
        with pytest.raises(TimeoutError):  # the read ran and nothing changed, so nothing is sent
            await waiting

    async with hub.lifespan():
        frames = hub.stream("room", read=distinct(read, key=summary))
        await next_event(frames)
        shown["summary"] = 2
        waiting = asyncio.ensure_future(next_event(frames))
        hub.wake("room")
        assert (await waiting).json() == {"summary": 2}
        await frames.aclose()
