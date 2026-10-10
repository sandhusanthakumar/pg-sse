import asyncio
import threading
from unittest import mock

import pytest
from helpers import make_hub, until

from pg_sse import Ephemeral, Event

pytestmark = pytest.mark.anyio


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def make(db, **kwargs):
    """A hub, its `typing` signals (ttl 8, grace 0.25) on a clock the test moves, and the timers it asked for."""
    hub = make_hub(db)
    clock = Clock()
    timers = mock.Mock()
    hub.call_later = timers  # the test runs the lapse check itself, when it has moved the clock
    typing = hub.ephemeral("typing", **{"ttl": 8, "clock": clock, **kwargs})
    return hub, typing, clock, timers


async def woken(subscription) -> bool:
    return await subscription.wait(0.05) is not None


async def test_a_new_signal_wakes_the_keys_streams_once_and_a_renewal_does_not(db):
    hub, typing, clock, _ = make(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        assert typing.set("room", "ann") is True
        assert await subscription.wait(1) == [("wake", "room", "typing")]
        clock.now += 4
        assert typing.set("room", "ann") is False
        assert not await woken(subscription)
        assert typing.set("room", "bob") is True  # someone else: what a reader sees changes
        assert await woken(subscription)


async def test_a_signal_that_is_not_renewed_lapses(db):
    hub, typing, clock, timers = make(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        typing.set("room", "ann")
        await subscription.wait(1)
        timers.assert_called_once_with(8.25, typing._check, "room")  # ttl + grace
        clock.now += 8
        typing._check("room")
        assert await subscription.wait(1) == [("wake", "room", "typing")]
        assert typing.live("room") == []


async def test_a_renewed_signal_survives_the_first_check_and_the_last_check_removes_it(db):
    hub, typing, clock, timers = make(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        typing.set("room", "ann")
        clock.now += 4
        typing.set("room", "ann")
        await subscription.wait(1)
        assert timers.call_count == 2  # one timer per renewal
        clock.now += 4.5  # the first timer fires: the renewal still has 3.5 s
        typing._check("room")
        assert not await woken(subscription)
        assert typing.live("room") == ["ann"]
        clock.now += 4  # the last timer fires
        typing._check("room")
        assert await woken(subscription)
        assert typing.live("room") == []


async def test_removing_a_signal_wakes_even_one_that_lapsed_a_moment_ago(db):
    hub, typing, clock, _ = make(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        typing.set("room", "ann")
        await subscription.wait(1)
        assert typing.set("room", "ann", on=False) is True
        assert await woken(subscription)
        assert typing.set("room", "ann", on=False) is False  # nobody is there any more
        assert not await woken(subscription)
        typing.set("room", "bob")
        await subscription.wait(1)
        clock.now += 8.1  # bob lapsed, and nobody has run the check yet
        assert typing.live("room") == []
        assert typing.set("room", "bob", on=False) is True
        assert await woken(subscription)


async def test_clear_removes_everyone_with_one_wake_or_one_person(db):
    hub, typing, _, _ = make(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        typing.set("room", "ann")
        typing.set("room", "bob")
        typing.set("room", "cy")
        await subscription.wait(1)
        assert typing.clear("room", "bob") is True
        assert await subscription.wait(1) == [("wake", "room", "typing")]
        assert typing.live("room") == ["ann", "cy"]
        assert typing.clear("room") is True
        assert await subscription.wait(1) == [("wake", "room", "typing")]
        assert typing.live("room") == []
        assert typing.clear("room") is False
        assert typing.clear("room", "ann") is False
        assert not await woken(subscription)


async def test_the_wake_reaches_the_key_and_not_the_all_keys_streams_unless_told_to(db):
    hub, typing, _, _ = make(db)
    everything = hub.ephemeral("viewing", ttl=45, all_keys=True)
    async with hub.lifespan():
        in_room, elsewhere, all_keys = hub.subscribe("room"), hub.subscribe("other"), hub.subscribe(None)
        typing.set("room", "ann")
        assert await in_room.wait(1) == [("wake", "room", "typing")]
        assert not await woken(elsewhere)
        assert not await woken(all_keys)
        everything.set("room", "ann")
        assert await all_keys.wait(1) == [("wake", "room", "viewing")]


async def test_two_instances_may_share_one_signal(db):
    hub, typing, _, _ = make(db, signal="presence")
    viewing = hub.ephemeral("viewing", ttl=45, signal="presence")
    assert (typing.signal, viewing.signal) == ("presence", "presence")
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        typing.set("room", "ann")
        assert await subscription.wait(1) == [("wake", "room", "presence")]
        viewing.set("room", "ann")  # the same item, queued once; either would have woken the stream
        assert typing.live("room") == viewing.live("room") == ["ann"]


async def test_live_skips_an_expired_entry_whose_check_never_ran(db):
    _hub, typing, clock, _ = make(db)  # no lifespan: the hub is stopped and no check will run
    typing.set("room", "ann")
    clock.now += 4
    typing.set("room", "bob")
    assert typing.live("room") == ["ann", "bob"]
    clock.now += 4.5
    assert typing.live("room") == ["bob"]
    clock.now += 4
    assert typing.live("room") == []
    assert typing.live("nowhere") == []


async def test_a_lapse_that_live_has_seen_is_still_announced(db):
    hub, typing, clock, _ = make(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        typing.set("room", "ann")
        await subscription.wait(1)
        clock.now += 8.1
        assert typing.live("room") == []  # a read between the expiry and the check must not eat the wake
        typing._check("room")
        assert await woken(subscription)


async def test_the_check_removes_the_keys_entry_when_it_is_empty_and_ignores_an_unknown_key(db):
    _hub, typing, clock, _ = make(db)
    typing.set("room", "ann")
    assert "room" in typing._entries
    clock.now += 9
    typing._check("room")
    assert typing._entries == {}
    typing._check("room")  # a later timer for the same key finds nothing
    assert typing.clear("room", "ann") is False


async def test_set_works_from_a_plain_thread_and_from_the_loop(db):
    hub, typing, _, timers = make(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        results: list[bool] = []
        thread = threading.Thread(target=lambda: results.append(typing.set("room", "ann")))
        thread.start()
        thread.join()
        assert results == [True]
        assert await subscription.wait(1) == [("wake", "room", "typing")]
        assert typing.set("room", "bob") is True
        assert await woken(subscription)
        assert timers.call_count == 2


async def test_a_signal_lapses_on_the_real_loop(db):
    hub = make_hub(db)
    typing = hub.ephemeral("typing", ttl=0.05, grace=0.02)
    async with hub.lifespan():
        subscription = hub.subscribe("room")
        typing.set("room", "ann")
        assert await subscription.wait(1) == [("wake", "room", "typing")]
        assert typing.live("room") == ["ann"]
        assert await subscription.wait(2) == [("wake", "room", "typing")]  # the lapse
        await until(lambda: typing.live("room") == [])
        assert typing._entries == {}


async def test_a_key_that_is_not_text_is_refused_like_hub_wake(db):
    hub, typing, _, _ = make(db)
    with pytest.raises(TypeError, match="key must be a str"):
        hub.wake(1, "typing")  # type: ignore[arg-type]
    for call in (lambda: typing.set(1, "ann"), lambda: typing.clear(1)):  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="key must be a str"):
            call()
    with pytest.raises(TypeError, match="key must be a str"):
        typing.set(None, "ann", on=False)  # type: ignore[arg-type]


@pytest.mark.parametrize(("kwargs", "message"), [({"ttl": 0}, "ttl"), ({"ttl": 1, "grace": -1}, "grace")])
def test_bad_settings_are_refused(kwargs, message):
    with pytest.raises(ValueError, match=message):
        Ephemeral(mock.Mock(), "typing", **kwargs)


async def test_the_signal_defaults_to_the_name(db):
    hub = make_hub(db)
    assert hub.ephemeral("typing", ttl=8).signal == "typing"
    assert hub.ephemeral("typing", ttl=8, signal="presence").signal == "presence"


async def test_a_read_sees_the_signal_in_wake_signals(db):
    hub, typing, _, _ = make(db)
    seen: list[frozenset[str]] = []

    def read(cursor, wake):
        seen.append(wake.signals)
        return [Event({}, event="typing")] if "typing" in wake.signals else []

    async with hub.lifespan():
        frames = hub.stream("room", read=read)
        first = asyncio.ensure_future(anext(frames))
        await until(lambda: len(seen) == 1)  # the start read, before any signal
        typing.set("room", "ann")
        assert "event: typing" in await asyncio.wait_for(first, 2)
        assert frozenset({"typing"}) in seen
        await frames.aclose()
