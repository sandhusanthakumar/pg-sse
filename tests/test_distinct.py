import asyncio
import inspect
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import pytest
from helpers import make_hub, until

from pg_sse import Event, distinct
from pg_sse._hub import _accepts_wake, _call, _is_async
from pg_sse.testing import next_event

pytestmark = pytest.mark.anyio


def summary(event: Event):
    return event.data.get("summary")


async def run(read, times: int) -> list[list[Event]]:
    return [await _call(read, None) for _ in range(times)]


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
    assert await _call(read, None) == [a, b, a]


async def test_two_streams_do_not_share_state():
    a = Event({"summary": 1})

    def plain(cursor):
        return [a]

    first, second = distinct(plain, key=summary), distinct(plain, key=summary)
    assert await _call(first, None) == [a]
    assert await _call(first, None) == []
    assert await _call(second, None) == [a]  # the second stream has never been sent it


async def test_the_wrapper_takes_reads_arguments_and_is_async():
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
    assert wrapped.__name__ == "async_wake"  # for logs and tracebacks
    assert await wrapped(None, "a wake") == []
    assert _accepts_wake(distinct(partial(async_wake, None), key=summary)) is False  # the partial bound the cursor
    assert inspect.iscoroutinefunction(distinct(sync_cursor, key=summary))  # async, so it never blocks the loop

    def sync_wake(cursor, wake):
        return [Event({"summary": wake})]

    async def async_cursor(cursor):
        return []

    sync_wrapped = distinct(sync_wake, key=summary)
    assert _accepts_wake(sync_wrapped) and inspect.iscoroutinefunction(sync_wrapped)
    assert [event.data for event in await sync_wrapped(None, 7)] == [{"summary": 7}]  # the wake reaches `read`
    assert not _accepts_wake(distinct(async_cursor, key=summary))


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


async def test_a_sync_read_that_returns_a_coroutine_is_filtered_after_it_is_awaited():
    a = Event({"summary": 1})

    async def later():
        return [a]

    read = distinct(lambda cursor: later(), key=summary)
    assert await _call(read, None) == [a]
    assert await _call(read, None) == []


async def test_a_sync_read_runs_in_the_hubs_executor_through_distinct_and_its_key_on_the_loop(db):
    names: dict[str, str] = {}

    def read(cursor):
        names["read"] = threading.current_thread().name
        return [Event({"summary": 1})]

    def key(event):
        names["key"] = threading.current_thread().name
        asyncio.get_running_loop()  # loop-bound state is within reach, as for an async read
        return summary(event)

    with ThreadPoolExecutor(1, thread_name_prefix="reads") as executor:
        hub = make_hub(db, read_executor=executor)
        async with hub.lifespan():
            frames = hub.stream("room", read=distinct(read, key=key), cursor=0)
            await next_event(frames)
            await frames.aclose()
    assert names["read"].startswith("reads")
    assert names["key"] == threading.current_thread().name


async def test_called_outside_a_stream_a_sync_read_runs_off_the_loop():
    names = []

    def read(cursor):
        names.append(threading.current_thread().name)
        return [Event({"summary": 1})]

    assert await distinct(read, key=summary)(None) == [Event({"summary": 1})]  # as a `hub.subscribe` loop calls it
    assert names and names[0] != threading.current_thread().name


async def test_a_generator_based_coroutine_from_a_read_is_awaited():
    @types.coroutine
    def legacy(cursor):
        yield from asyncio.sleep(0).__await__()
        return [Event({"summary": 1})]

    assert await distinct(lambda cursor: legacy(cursor), key=summary)(None) == [Event({"summary": 1})]


async def test_an_async_read_of_a_cursor_stays_async_and_is_filtered():
    a = Event({"summary": 1})

    async def read(cursor):
        return [a]

    wrapped = distinct(read, key=summary)
    assert inspect.iscoroutinefunction(wrapped)
    assert await wrapped(None) == [a]
    assert await wrapped(None) == []


async def test_an_object_with_an_async_call_stays_async():
    a = Event({"summary": 1})

    class Reader:
        async def __call__(self, cursor):
            return [a]

    wrapped = distinct(Reader(), key=summary)
    assert inspect.iscoroutinefunction(wrapped)
    assert await wrapped(None) == [a]
    assert not _is_async(Reader)  # calling the class makes an instance, not a coroutine
    assert _is_async(partial(partial(Reader()), None))
