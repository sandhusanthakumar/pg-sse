import asyncio

import pytest

from pg_sse import Event
from pg_sse._event import encode, encode_cursor, encode_goodbye
from pg_sse.testing import Frame, decode, ended, goodbye, next_event, next_frame, until

pytestmark = pytest.mark.anyio

PING = ": ping\n\n"


async def stream(*frames: str | bytes, then=None):
    """A stream of the given frames; after them it ends, or waits forever with `then="wait"`."""
    for frame in frames:
        yield frame
    if then == "wait":
        await asyncio.Event().wait()


def test_the_module_re_exports_the_decoder():
    assert decode(": ping\n\n").is_ping
    assert isinstance(decode("data: x\n\n"), Frame)


async def test_next_frame_returns_whatever_comes_next():
    frames = stream(PING, encode_cursor(4), encode(Event("x")), encode_goodbye(None, "max_age", 1000))
    assert (await next_frame(frames)).is_ping
    assert (await next_frame(frames)).is_cursor
    assert (await next_frame(frames)).data == "x"
    assert (await next_frame(frames)).is_goodbye


async def test_a_frame_may_be_bytes():
    assert (await next_frame(stream(b"data: x\n\n"))).data == "x"


async def test_next_event_skips_pings_and_cursors():
    frames = stream(encode_cursor(1), PING, PING, encode(Event({"n": 1}, id=2)))
    assert (await next_event(frames)).json() == {"n": 1}


async def test_next_event_with_a_name_skips_the_other_events():
    frames = stream(encode(Event("a", event="one")), PING, encode(Event("b")), encode(Event("c", event="two")))
    assert (await next_event(frames, event="two")).data == "c"
    unnamed = stream(encode(Event("a", event="one")), encode(Event("b")))
    assert (await next_event(unnamed)).event == "one"  # without `event=` the first event is the answer


async def test_goodbye_skips_pings_and_returns_the_retry_frame():
    assert (await goodbye(stream(PING, encode_goodbye(None, "max_age", 1000)))).retry == 1000
    named = await goodbye(stream(PING, PING, encode_goodbye("end", "shutdown", 30000)))
    assert (named.event, named.json()) == ("end", {"reason": "shutdown"})


async def test_goodbye_refuses_any_other_frame():
    with pytest.raises(AssertionError, match="expected a goodbye"):
        await goodbye(stream(PING, encode(Event("x"))))


async def test_ended_passes_when_the_stream_stops_and_fails_on_a_frame():
    await ended(stream())
    with pytest.raises(AssertionError, match="expected the stream to end"):
        await ended(stream(PING))


async def test_a_stream_that_ends_early_is_an_assertion_not_a_stop_iteration():
    with pytest.raises(AssertionError, match=r"the stream ended; frames seen so far: \[': ping\\n\\n'\]"):
        await next_event(stream(PING))
    with pytest.raises(AssertionError, match="the stream ended"):
        await next_frame(stream())
    with pytest.raises(AssertionError, match="the stream ended"):
        await goodbye(stream())


async def test_timeouts_name_the_frames_seen_so_far():
    with pytest.raises(TimeoutError, match=r"frames seen so far: \['id: 1\\n\\n', ': ping\\n\\n'\]"):
        await next_event(stream(encode_cursor(1), PING, then="wait"), timeout=0.05)
    with pytest.raises(TimeoutError, match=r"frames seen so far: \[\]"):
        await next_frame(stream(then="wait"), timeout=0.05)
    with pytest.raises(TimeoutError, match=r"frames seen so far: \[': ping\\n\\n'\]"):
        await goodbye(stream(PING, then="wait"), timeout=0.05)
    with pytest.raises(TimeoutError, match="did not end"):
        await ended(stream(then="wait"), timeout=0.05)


async def test_until_waits_for_the_predicate_and_fails_after_the_timeout():
    flags = []
    asyncio.get_running_loop().call_later(0.05, flags.append, 1)
    await until(lambda: flags)
    with pytest.raises(AssertionError, match="condition not met"):
        await until(lambda: False, timeout=0.05)
