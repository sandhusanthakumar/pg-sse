"""
Reading a stream in tests: decoded frames, with the waiting and skipping a test would otherwise repeat.

    frames = hub.stream("room", read=read, cursor=0)
    assert (await next_event(frames)).json() == {"id": 1}

It needs no test framework and no database: the helpers take any async iterator of frames (what `hub.stream`
returns, or a `StreamingResponse`'s `body_iterator`). Run the hub itself with `async with hub.lifespan():`.

Waiting past `timeout` raises `TimeoutError`, naming the frames seen so far. A timed-out read cancels the generator,
so the stream is over afterwards.
"""

import asyncio
import time
from collections.abc import AsyncIterator, Callable

from ._event import Frame, decode

__all__ = ["Frame", "decode", "ended", "goodbye", "next_event", "next_frame", "until"]


async def _next(frames: AsyncIterator[str | bytes], deadline: float, seen: list[str]) -> Frame:
    try:
        async with asyncio.timeout(max(deadline - time.monotonic(), 0)):
            raw = await anext(frames)
    except TimeoutError:
        raise TimeoutError(f"no frame in time; frames seen so far: {seen}") from None
    except StopAsyncIteration:
        raise AssertionError(f"the stream ended; frames seen so far: {seen}") from None
    raw = raw.decode() if isinstance(raw, bytes) else raw
    seen.append(raw)
    return decode(raw)


async def next_frame(frames: AsyncIterator[str | bytes], timeout: float = 5.0) -> Frame:  # noqa: ASYNC109 -- a deadline for the whole call
    """The next frame, whatever it is: a ping, a cursor, an event, a goodbye."""
    return await _next(frames, time.monotonic() + timeout, [])


async def next_event(frames: AsyncIterator[str | bytes], timeout: float = 5.0, *, event: str | None = None) -> Frame:  # noqa: ASYNC109 -- a deadline for the whole call
    """
    The next event. Skips pings and id-only frames, and with `event=` frames of any other name.

    Pings are skipped because one comes before any frame a re-read produces (every idle timeout pings, even the one
    that leads to the re-read) and before a goodbye when the age limit and a ping coincide; a test that expects the
    next frame to be the event is flaky without it.
    """
    deadline = time.monotonic() + timeout
    seen: list[str] = []
    while True:
        frame = await _next(frames, deadline, seen)
        if frame.is_ping or frame.is_cursor:
            continue
        if event is None or frame.event == event:
            return frame


async def goodbye(frames: AsyncIterator[str | bytes], timeout: float = 5.0) -> Frame:  # noqa: ASYNC109 -- a deadline for the whole call
    """The frame that announces a planned close (`retry:`, and the end event if the hub has one). Skips pings."""
    deadline = time.monotonic() + timeout
    seen: list[str] = []
    while True:
        frame = await _next(frames, deadline, seen)
        if frame.is_ping:
            continue
        if not frame.is_goodbye:
            raise AssertionError(f"expected a goodbye, got {frame.raw!r}; frames seen so far: {seen}")
        return frame


async def ended(frames: AsyncIterator[str | bytes], timeout: float = 5.0) -> None:  # noqa: ASYNC109 -- a deadline for the whole call
    """Asserts the stream is over: the next read raises `StopAsyncIteration`, with no frame first."""
    try:
        async with asyncio.timeout(timeout):
            raw = await anext(frames)
    except StopAsyncIteration:
        return
    except TimeoutError:
        raise TimeoutError("the stream did not end in time") from None
    raise AssertionError(f"expected the stream to end, got {raw!r}")


async def until(predicate: Callable[[], object], timeout: float = 5.0) -> None:  # noqa: ASYNC109 -- a deadline for the whole call
    """Waits, yielding to the event loop, until `predicate()` is true."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)
