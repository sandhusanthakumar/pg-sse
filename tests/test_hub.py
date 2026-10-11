import asyncio
import concurrent.futures
import contextlib
import dataclasses
import os
import signal
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from decimal import Decimal
from fractions import Fraction
from functools import partial
from unittest import mock

import psycopg
import pytest
from helpers import execute, make_hub, terminate_listener, until
from starlette.exceptions import HTTPException

from pg_sse import EndedTotals, Hub, HubStats, TooManySubscribers, Wake, distinct
from pg_sse import _hub as hub_module
from pg_sse._hub import CLOSING, DROPPED, RESYNC, REVOKED, _accepts_wake, _wake_from

pytestmark = pytest.mark.anyio


def notify(db, key: str) -> None:
    execute(db.dsn, "SELECT pg_notify(%s, %s)", (db.channel, key))


async def test_a_notification_wakes_the_streams_for_its_key(db):
    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        room_a, every_room, room_b = hub.subscribe("a"), hub.subscribe(None), hub.subscribe("b")
        notify(db, "a")
        assert await room_a.wait(3) == [("notify", "a")]
        assert await every_room.wait(3) == [("notify", "a")]
        assert await room_b.wait(0.3) is None


async def test_a_burst_is_one_wake_up(db):
    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        subscription, every_key = hub.subscribe("a"), hub.subscribe(None)
        for _ in range(3):
            notify(db, "a")
        notify(db, "b")  # notifications arrive in order: once b is queued, all three a's have been offered
        await until(lambda: len(every_key._items) == 2)
        assert await subscription.wait(1) == [("notify", "a")]  # the repeats were not queued again
        assert await every_key.wait(1) == [("notify", "a"), ("notify", "b")]


async def test_identical_wake_ups_do_not_fill_the_queue(db):
    hub = make_hub(db, queue_size=2)
    async with hub.lifespan():
        subscription = hub.subscribe("a")
        for _ in range(100):
            hub.wake("a")
        await until(lambda: len(subscription._items) == 1)
        await asyncio.sleep(0.05)  # the rest of the 100 have been dispatched too
        assert not subscription.ended
        assert await subscription.wait(1) == [("wake", "a", "wake")]
        assert not subscription._items
        hub.wake("a")  # what was read is no longer pending: the next one is queued again
        assert await subscription.wait(1) == [("wake", "a", "wake")]


async def test_distinct_keys_past_the_queue_size_still_drop_an_all_keys_stream(db):
    hub = make_hub(db, queue_size=2)
    async with hub.lifespan():
        every_key = hub.subscribe(None)
        for key in ("a", "b", "c"):
            hub.wake(key)
        await until(lambda: every_key.ended)
        assert await every_key.wait(1) == [("marker", DROPPED)]
        assert hub.stats().dropped_total == 1


async def test_a_stream_opened_before_the_first_listen_is_resynced(db):
    hub = make_hub(db)
    hub._loop = asyncio.get_running_loop()  # a stream can be opened before the listener has its LISTEN
    subscription = hub.subscribe("a")
    hub.start()
    try:
        await until(hub.listening.is_set)
        assert await subscription.wait(2) == [("marker", RESYNC)]
        assert hub.stats().listener_reconnects_total == 0  # the first connect is not a reconnect
    finally:
        await asyncio.to_thread(hub.stop)


async def test_a_stream_opened_once_the_listener_is_live_is_not_resynced(db):
    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        # The first RESYNC is posted before `listening` is set; yield once so the loop delivers it before we subscribe.
        await asyncio.sleep(0)
        assert await hub.subscribe("a").wait(0.3) is None


async def test_every_stream_re_reads_after_the_listener_reconnects(db):
    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        subscription = hub.subscribe("a")
        terminate_listener(db)
        assert await subscription.wait(5) == [("marker", RESYNC)]
        await until(hub.listening.is_set)
        assert hub.stats().listener_reconnects_total == 1  # only the reconnect counts, not the first connect
        notify(db, "a")
        assert await subscription.wait(3) == [("notify", "a")]


async def test_a_quiet_connection_still_delivers_a_notification(db, monkeypatch):
    monkeypatch.setattr(hub_module, "_KEEPALIVE_QUERY_SECONDS", 0.0)
    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        subscription = hub.subscribe("a")
        await asyncio.sleep(0.2)
        notify(db, "a")
        assert await subscription.wait(3) == [("notify", "a")]


async def test_a_stream_that_falls_behind_is_dropped(db):
    hub = make_hub(db, queue_size=2)
    async with hub.lifespan():
        subscription = hub.subscribe("a")
        for signal_name in ("x", "y", "z"):  # distinct: identical wake-ups are not queued twice
            hub.wake("a", signal_name)
        await until(lambda: subscription.ended)
        assert await subscription.wait(1) == [("marker", DROPPED)]
        assert (hub.stats().open_streams, hub.stats().dropped_total) == (0, 1)  # the hub forgot it


async def test_caps_per_process_and_per_owner(db):
    hub = make_hub(db, max_subscribers=3, max_per_owner=1)
    async with hub.lifespan():
        hub.subscribe("a", owner="adam")
        with pytest.raises(TooManySubscribers, match="owner"):
            hub.subscribe("a", owner="adam")
        hub.subscribe("a", owner="john")
        hub.subscribe("b", owner="adam")
        with pytest.raises(TooManySubscribers, match="too many open streams"):
            hub.subscribe("c")


async def test_the_per_owner_cap_counts_one_key_and_the_total_counts_every_key(db):
    hub = make_hub(db, max_subscribers=50, max_per_owner=2)
    async with hub.lifespan():
        for key in "abcdefgh":  # no total by default: one owner may hold two streams on every key it can name
            hub.subscribe(key, owner="adam")
            hub.subscribe(key, owner="adam")
        assert hub.stats().open_streams == 16

    hub = make_hub(db, max_subscribers=50, max_per_owner=2, max_total_per_owner=3)
    async with hub.lifespan():
        hub.subscribe("a", owner="adam")
        hub.subscribe("a", owner="adam")
        with pytest.raises(TooManySubscribers, match=r"for this owner$"):  # the per-key cap answers first
            hub.subscribe("a", owner="adam")
        hub.subscribe("b", owner="adam")
        with pytest.raises(TooManySubscribers, match="across all keys"):
            hub.subscribe("c", owner="adam")
        hub.subscribe("c", owner="john")  # another owner, and an anonymous stream, are not counted against adam
        hub.subscribe("c")
        assert hub.stats().open_streams == 5
        assert hub.stats().refused_total == 2


async def test_a_total_per_owner_cap_refuses_a_stream_and_frees_its_slot_on_unsubscribe(db):
    hub = make_hub(db, max_total_per_owner=1)
    async with hub.lifespan():
        first = hub.subscribe("a", owner="adam")
        with pytest.raises(TooManySubscribers):
            hub.stream("b", read=lambda after: [], owner="adam")
        hub.unsubscribe(first)
        hub.stream("b", read=lambda after: [], owner="adam")


async def test_anonymous_streams_share_no_owner_cap(db):
    hub = make_hub(db, max_per_owner=1)
    async with hub.lifespan():
        for _ in range(3):
            hub.subscribe("a")
        hub.subscribe("a", owner="x")
        with pytest.raises(TooManySubscribers, match="owner"):
            hub.subscribe("a", owner="x")
        assert hub.stats().open_streams == 4


async def test_close_all_ends_every_stream_and_refuses_new_ones(db):
    hub = make_hub(db)
    async with hub.lifespan():
        subscription = hub.subscribe("a")
        hub.close_all()
        assert await subscription.wait(1) == [("marker", CLOSING)]
        subscription.close()  # closing twice is harmless
        hub.wake("a")  # and an ended stream takes nothing more
        await asyncio.sleep(0.05)
        assert not subscription._items
        with pytest.raises(TooManySubscribers, match="shutting down"):
            hub.subscribe("a")


async def test_leaving_the_lifespan_forgets_subscriptions_nobody_unsubscribed(db):
    hub = make_hub(db, max_subscribers=1)
    async with hub.lifespan():
        hub.subscribe("a")  # never unsubscribed
        assert hub.stats().open_streams == 1
    assert hub.stats().open_streams == 0
    async with hub.lifespan():
        hub.subscribe("a")  # the cap of 1 is free again


@pytest.mark.parametrize("key", [7, 1.5, b"a", object(), True])
async def test_a_key_that_is_not_text_is_refused_where_it_is_passed(db, key):
    # An int from a FastAPI path parameter would subscribe and never be woken: the payload is always text.
    hub = make_hub(db)
    async with hub.lifespan():
        with pytest.raises(TypeError, match=r"key must be a str or None, got .*str\(key\)"):
            hub.stream(key, read=lambda after: [])
        with pytest.raises(TypeError, match="key must be a str"):
            hub.subscribe(key)
        with pytest.raises(TypeError, match=r"key must be a str, got"):
            hub.wake(key)
        assert hub.stats().open_streams == 0
        assert hub.stats().refused_total == 0  # a mistake in the caller, not back-pressure


async def test_sse_refuses_a_key_that_is_not_text_instead_of_answering_503(db):
    hub = make_hub(db)
    async with hub.lifespan():
        with pytest.raises(TypeError, match="key must be a str"):
            hub.sse(42, read=lambda after: [])


async def test_none_still_means_every_key_and_wake_still_needs_a_key(db):
    hub = make_hub(db)
    async with hub.lifespan():
        hub.subscribe(None)
        hub.stream(None, read=lambda after: [])
        with pytest.raises(TypeError, match=r"key must be a str, got NoneType"):
            hub.wake(None)  # type: ignore[arg-type]


async def test_a_str_subclass_is_a_key(db):
    class Room(str):
        pass

    hub = make_hub(db)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        subscription = hub.subscribe(Room("a"))
        notify(db, "a")
        assert await subscription.wait(3) == [("notify", "a")]


async def test_a_notification_is_offered_only_to_the_streams_it_reaches(db):
    hub = make_hub(db)
    async with hub.lifespan():
        for room in range(200):
            hub.subscribe(f"room{room}")
        same_room = [hub.subscribe("a"), hub.subscribe("a")]
        every_room = hub.subscribe(None)
        with mock.patch.object(hub_module.Subscription, "offer", autospec=True, return_value=True) as offer:
            hub._notify("a")
            assert {call.args[0] for call in offer.call_args_list} == {*same_room, every_room}
            offer.reset_mock()
            hub._dispatch(("wake", "a", "typing"), "a", all_keys=False)
            assert {call.args[0] for call in offer.call_args_list} == set(same_room)
            offer.reset_mock()
            hub._notify("nobody")
            assert {call.args[0] for call in offer.call_args_list} == {every_room}
        with mock.patch.object(hub_module.Subscription, "resync", autospec=True) as resync:
            hub._resync()  # every stream; `hub.subscribe` queues get it at once
            assert resync.call_count == 200 + 3


async def test_the_indexes_are_empty_however_a_subscription_ends(db):
    hub = make_hub(db, queue_size=1)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        first, second, dropped = hub.subscribe("a", "ann"), hub.subscribe("a", "ann"), hub.subscribe("b", "bob")
        kept_all, unsubscribed_all = hub.subscribe(None, "cy"), hub.subscribe(None)
        hub.unsubscribe(first)
        assert hub._by_key["a"] == {second}  # the key stays while another stream watches it
        assert hub._by_owner["ann"] == {second}
        hub.unsubscribe(second)
        hub.unsubscribe(second)  # twice is harmless
        assert set(hub._by_key) == {"b"}
        assert set(hub._by_owner) == {"bob", "cy"}
        hub.unsubscribe(unsubscribed_all)
        assert hub._watching_all == {kept_all}
        hub._dispatch(("notify", "x"), "b", all_keys=False)
        hub._dispatch(("notify", "y"), "b", all_keys=False)  # a second distinct item overflows a queue of one
        assert dropped.ended
        assert hub._by_key == {}  # a `hub.subscribe` queue is forgotten when it is dropped
        assert set(hub._by_owner) == {"cy"}
        assert hub._watching_all == {kept_all}
        hub.subscribe("c", "dee")
    assert hub._subscriptions == set()  # leaving the lifespan forgets the rest
    assert hub._by_key == {}
    assert hub._by_owner == {}
    assert hub._watching_all == set()


async def test_a_closed_subscribe_queue_is_forgotten_at_once_even_if_never_unsubscribed(db):
    hub = make_hub(db, max_per_owner=1)
    async with hub.lifespan():
        revoked = hub.subscribe("a", "ann")
        hub.close(owner="ann")
        hub.close(owner="ann")  # twice is harmless
        assert revoked.ended
        assert await revoked.wait(0.5) == [("marker", REVOKED)]  # its loop still learns why
        assert hub.stats().open_streams == 0  # no read of the hub's is left to stop: its slot is free
        assert hub.subscribe("a", "ann")


async def test_a_wake_reaches_its_key_and_the_all_keys_streams_unless_told_not_to(db):
    hub = make_hub(db)
    async with hub.lifespan():
        room_a, every_room, room_b = hub.subscribe("a"), hub.subscribe(None), hub.subscribe("b")
        hub.wake("a", signal="typing", all_keys=False)
        assert await room_a.wait(2) == [("wake", "a", "typing")]
        assert await every_room.wait(0.3) is None
        assert await room_b.wait(0.3) is None
        hub.wake("a")
        assert await room_a.wait(2) == [("wake", "a", "wake")]
        assert await every_room.wait(2) == [("wake", "a", "wake")]


async def test_wake_works_from_another_thread(db):
    hub = make_hub(db)
    async with hub.lifespan():
        room_a, every_room = hub.subscribe("a"), hub.subscribe(None)
        threading.Thread(target=hub.wake, args=("a",)).start()
        assert await room_a.wait(2) == [("wake", "a", "wake")]
        assert await every_room.wait(2) == [("wake", "a", "wake")]
        threading.Thread(target=hub.wake, args=("a", "typing"), kwargs={"all_keys": False}).start()
        assert await room_a.wait(2) == [("wake", "a", "typing")]
        assert await every_room.wait(0.3) is None


async def test_subscribing_needs_a_running_hub(db):
    hub = make_hub(db)
    with pytest.raises(RuntimeError, match="not running"):
        hub.subscribe("a")


async def test_subscribing_from_another_loop_is_refused(db):
    hub = make_hub(db)
    other = asyncio.new_event_loop()
    try:
        hub._loop = other
        with pytest.raises(RuntimeError, match="another event loop"):
            hub.subscribe("a")
    finally:
        other.close()


def test_subscribing_off_the_loop_is_refused(db):
    with pytest.raises(RuntimeError, match="async def"):
        make_hub(db).subscribe("a")


async def test_start_twice_and_stop_twice_are_harmless(db):
    hub = make_hub(db)
    hub.start()
    hub.start()
    await until(hub.listening.is_set)
    began = time.monotonic()
    await asyncio.to_thread(hub.stop)
    assert time.monotonic() - began < 1.5  # the listener polls the stop flag
    hub.stop()
    assert not hub.listening.is_set()


async def test_an_unreachable_database_keeps_retrying_until_stopped():
    hub = Hub("postgresql://nobody@127.0.0.1:1/none?connect_timeout=1", "c", handle_signals=False)
    with mock.patch.object(hub_module.logger, "warning") as warning:
        hub.start()
        await until(lambda: warning.call_count >= 2)
        await asyncio.to_thread(hub.stop)
    assert not hub.listening.is_set()


async def test_lifespan_waits_for_the_listener(db):
    hub = make_hub(db)
    async with hub.lifespan():
        assert hub.listening.is_set()
        assert hub.stats().listener_connected


async def test_lifespan_does_not_fail_without_a_database():
    hub = Hub("postgresql://nobody@127.0.0.1:1/none?connect_timeout=1", "c", handle_signals=False, listen_timeout=0.2)
    began = time.monotonic()
    with mock.patch.object(hub_module.logger, "warning"):
        async with hub.lifespan():
            assert time.monotonic() - began < 2  # waited for the timeout, not for a database
            assert not hub.listening.is_set()
    assert not hub.listening.is_set()


async def test_lifespan_does_not_wait_when_told_not_to(db):
    hub = make_hub(db, listen_timeout=None)
    with mock.patch.object(hub.listening, "wait") as wait:
        async with hub.lifespan():
            pass
    wait.assert_not_called()


async def test_a_connect_that_hangs_does_not_outlive_a_restart(db):
    hub = make_hub(db)
    hub.listening = mock.Mock(wraps=threading.Event())  # counts who sets it
    release = threading.Event()
    connects = []
    real_connect = psycopg.connect

    def connect(*args, **kwargs):
        connects.append(threading.current_thread())
        if len(connects) == 1:
            release.wait(10)  # the first listener hangs in its connect
        return real_connect(*args, **kwargs)

    with mock.patch.object(hub_module.psycopg, "connect", side_effect=connect):
        hub.start()
        await until(lambda: connects)
        abandoned = connects[0]
        await asyncio.to_thread(hub.stop)  # gives up on the hung thread
        assert abandoned.is_alive()
        hub.start()
        await until(hub.listening.is_set)
        release.set()
        await asyncio.to_thread(abandoned.join, 5)
        assert not abandoned.is_alive()  # the connect returned and the old listener noticed it was told to stop
        assert hub.listening.is_set()
        assert hub.listening.set.call_count == 1  # only the current listener ever reported live
        await asyncio.to_thread(hub.stop)


async def test_wake_does_nothing_once_the_loop_is_gone(db):
    hub = make_hub(db)
    closed = asyncio.new_event_loop()
    closed.close()
    hub._loop = closed
    hub.wake("a")
    hub._loop = mock.Mock(is_closed=lambda: False, call_soon_threadsafe=mock.Mock(side_effect=RuntimeError))
    hub.wake("a")


async def test_call_later_runs_on_the_loop_after_the_delay_from_any_thread(db):
    hub = make_hub(db)
    ran: list[tuple[str, int | None]] = []
    async with hub.lifespan():
        loop_thread = threading.get_ident()
        thread = threading.Thread(
            target=hub.call_later, args=(0.05, lambda value: ran.append((value, threading.get_ident())), "x")
        )
        thread.start()
        thread.join()
        assert ran == []  # not before the delay
        await until(lambda: bool(ran))
    assert ran == [("x", loop_thread)]


async def test_call_later_does_nothing_without_a_loop(db):
    hub = make_hub(db)
    callback = mock.Mock()
    hub.call_later(0, callback)
    closed = asyncio.new_event_loop()
    closed.close()
    hub._loop = closed
    hub.call_later(0, callback)
    hub._loop = mock.Mock(is_closed=lambda: False, call_soon_threadsafe=mock.Mock(side_effect=RuntimeError))
    hub.call_later(0, callback)
    callback.assert_not_called()


async def test_the_dsn_may_be_a_function_called_on_every_connect(db):
    hub = make_hub(db)
    calls = []

    def dsn() -> str:
        calls.append(1)
        return db.dsn

    hub.conninfo = dsn
    async with hub.lifespan():
        await until(hub.listening.is_set)
        assert len(calls) == 1
        terminate_listener(db)
        await until(lambda: len(calls) == 2)
        await until(hub.listening.is_set)


async def test_a_dsn_function_that_raises_is_retried_like_a_failed_connect(db, monkeypatch):
    monkeypatch.setattr(hub_module, "_BACKOFF_START_SECONDS", 0.01)
    hub = make_hub(db)
    attempts = []

    def dsn() -> str:
        attempts.append(1)
        if len(attempts) <= 2:
            raise RuntimeError("settings are not readable yet")
        return db.dsn

    hub.conninfo = dsn
    with mock.patch.object(hub_module.logger, "warning") as warning:
        async with hub.lifespan():
            await until(hub.listening.is_set)
    assert len(attempts) == 3
    assert warning.call_count >= 2


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"channel": ""}, "channel"),
        ({"channel": "x" * 64}, "channel"),
        ({"ping_seconds": (0, 1)}, "ping_seconds"),
        ({"ping_seconds": (2, 1)}, "ping_seconds"),
        ({"ping_seconds": (float("nan"), 1)}, "ping_seconds"),
        ({"ping_seconds": (1, float("nan"))}, "ping_seconds"),
        ({"ping_seconds": (1, float("inf"))}, "ping_seconds"),
        ({"ping_seconds": (1,)}, "ping_seconds"),
        ({"ping_seconds": 15}, "ping_seconds"),
        ({"listen_timeout": float("nan")}, "listen_timeout"),
        ({"listen_timeout": -1}, "listen_timeout"),
        ({"end_event": "end\nx"}, "line break"),
        ({"end_event": ""}, "end_event"),
        ({"reread_seconds": 0}, "reread_seconds"),
        ({"reread_seconds": float("nan")}, "reread_seconds"),
        ({"reread_seconds": "60"}, "reread_seconds"),
        ({"reread_seconds": True}, "reread_seconds"),  # an int, but not a duration
        ({"reread_seconds": Decimal("sNaN")}, "reread_seconds"),  # float() raises for it
        ({"reread_seconds": 10**400}, "reread_seconds"),  # too large for a float
        ({"ping_seconds": ("1", 2)}, "ping_seconds"),
        ({"resync_spread": -1}, "resync_spread"),
        ({"resync_spread": float("nan")}, "resync_spread"),
        ({"resync_spread": float("inf")}, "resync_spread"),
        ({"application_name": ""}, "application_name"),
        ({"max_total_per_owner": 0}, "max_total_per_owner"),
        ({"queue_size": 0}, "queue_size"),
        ({"queue_size": -1}, "queue_size"),
    ],
)
def test_bad_settings_are_refused(kwargs, message):
    with pytest.raises(ValueError, match=message):
        Hub("postgresql://x", **{"channel": "c", **kwargs})


def test_a_process_pool_is_refused_as_the_read_executor():
    with ProcessPoolExecutor(1) as pool, pytest.raises(ValueError, match="ThreadPoolExecutor"):
        Hub("postgresql://x", "c", read_executor=pool)


@pytest.mark.skipif(not hasattr(concurrent.futures, "InterpreterPoolExecutor"), reason="Python 3.14+")
def test_an_interpreter_pool_is_refused_as_the_read_executor():
    # A subclass of ThreadPoolExecutor whose workers cannot run a closure from this interpreter.
    with concurrent.futures.InterpreterPoolExecutor(1) as pool, pytest.raises(ValueError, match="interpreters"):
        Hub("postgresql://x", "c", read_executor=pool)


def test_durations_of_any_real_number_type_are_kept_as_floats():
    hub = Hub(
        "postgresql://x",
        "c",
        max_stream_seconds=Decimal(600),  # e.g. from a settings loader
        ping_seconds=(Fraction(1, 2), 1),
        reread_seconds=Decimal("30.5"),
        resync_spread=0,
    )
    assert (hub.max_stream_seconds, hub.ping_seconds, hub.reread_seconds, hub.resync_spread) == (600, (0.5, 1), 30.5, 0)
    assert all(type(value) is float for value in (hub.max_stream_seconds, *hub.ping_seconds, hub.reread_seconds))


async def test_sigterm_ends_open_streams_at_once_and_once_per_lifespan(db, restore_signals):
    calls = []
    signal.signal(signal.SIGTERM, lambda signum, frame: calls.append(signum))
    original = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    hub = Hub(db.dsn, db.channel)
    for _ in range(2):
        async with hub.lifespan():
            assert signal.getsignal(signal.SIGTERM) is not original[signal.SIGTERM]
        # Restored on exit, so lifespans do not stack a handler layer each.
        assert {sig: signal.getsignal(sig) for sig in original} == original
    async with hub.lifespan():
        subscription = hub.subscribe("a")
        os.kill(os.getpid(), signal.SIGTERM)
        assert await subscription.wait(2) == [("marker", CLOSING)]
    assert calls == [signal.SIGTERM]  # the server's own handler still runs, once


async def test_the_signal_hook_leaves_a_default_action_alone(db, restore_signals):
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    hub = Hub(db.dsn, db.channel)
    async with hub.lifespan():
        # Chaining onto SIG_DFL would swallow SIGTERM: the default action ends the process, and that is the point.
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
        assert signal.SIGTERM not in hub._signal_hooks
        assert signal.SIGINT in hub._signal_hooks  # Python's own SIGINT handler is callable: chained
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


async def test_an_ignored_signal_still_ends_streams_and_the_process_carries_on(db, restore_signals):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    hub = Hub(db.dsn, db.channel)
    async with hub.lifespan():
        subscription = hub.subscribe("a")
        os.kill(os.getpid(), signal.SIGTERM)
        assert await subscription.wait(2) == [("marker", CLOSING)]


async def test_a_hub_that_exits_out_of_order_stops_reacting_to_signals(db, restore_signals):
    calls = []
    signal.signal(signal.SIGTERM, lambda signum, frame: calls.append(signum))
    first, second = Hub(db.dsn, db.channel), Hub(db.dsn, db.channel)
    async with contextlib.AsyncExitStack() as stack:
        exit_first = contextlib.AsyncExitStack()
        await exit_first.enter_async_context(first.lifespan())  # entered first, left first: its handler is not on top
        await stack.enter_async_context(second.lifespan())
        await exit_first.aclose()
        with mock.patch.object(first, "close_all") as first_closes:
            subscription = second.subscribe("a")
            os.kill(os.getpid(), signal.SIGTERM)
            assert await subscription.wait(2) == [("marker", CLOSING)]
            first_closes.assert_not_called()  # its stale handler stays in the chain but only passes the signal on
    assert calls == [signal.SIGTERM]


async def test_a_handler_installed_over_ours_is_left_alone(db, restore_signals):
    signal.signal(signal.SIGTERM, lambda signum, frame: None)
    hub = Hub(db.dsn, db.channel)
    async with hub.lifespan():
        wrapper = lambda signum, frame: None  # noqa: E731
        signal.signal(signal.SIGTERM, wrapper)
    assert signal.getsignal(signal.SIGTERM) is wrapper


async def test_stats_count_streams_refusals_drops_and_the_listener(db):
    hub = make_hub(db, max_subscribers=2, queue_size=1)
    assert hub.stats() == HubStats(0, 0, 0, False, 0, None, 0, EndedTotals())
    async with hub.lifespan():
        await until(hub.listening.is_set)
        subscription = hub.subscribe("a")
        hub.subscribe("b")
        with pytest.raises(TooManySubscribers):
            hub.subscribe("c")
        hub.wake("a", "x")
        hub.wake("a", "y")
        await until(lambda: subscription.ended)
        stats = hub.stats()
        assert (stats.open_streams, stats.refused_total, stats.dropped_total) == (1, 1, 1)
        assert stats.last_notification_at is None
        notify(db, "b")
        await until(lambda: hub.stats().last_notification_at is not None)
        stats = hub.stats()
        assert stats.listener_connected
        assert stats.listener_reconnects_total == 0
        assert stats.last_notification_at == pytest.approx(time.time(), abs=5)
    assert not hub.stats().listener_connected


def test_the_signal_hook_is_skipped_off_the_main_thread(db):
    before = signal.getsignal(signal.SIGTERM)
    thread = threading.Thread(target=make_hub(db)._install_signal_hook, args=(mock.Mock(),))
    thread.start()
    thread.join()
    assert signal.getsignal(signal.SIGTERM) is before


def test_the_signal_hook_tolerates_a_closed_loop(db, restore_signals):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    loop = asyncio.new_event_loop()
    loop.close()
    make_hub(db)._install_signal_hook(loop)
    signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)


def test_installing_the_signal_hook_twice_is_a_no_op(db, restore_signals):
    signal.signal(signal.SIGTERM, lambda signum, frame: None)
    original = signal.getsignal(signal.SIGTERM)
    hub = make_hub(db)
    loop = asyncio.new_event_loop()
    hub._install_signal_hook(loop)
    installed = signal.getsignal(signal.SIGTERM)
    assert installed is not original
    hub._install_signal_hook(loop)
    assert signal.getsignal(signal.SIGTERM) is installed
    hub._remove_signal_hook()
    assert signal.getsignal(signal.SIGTERM) is original
    loop.close()


def test_a_listener_told_to_stop_while_failing_returns_quietly(db):
    hub = make_hub(db)
    stop = threading.Event()

    def stop_then_fail(*args, **kwargs):
        stop.set()
        raise OSError

    with (
        mock.patch.object(hub_module.psycopg, "connect", side_effect=stop_then_fail),
        mock.patch.object(hub_module.logger, "warning") as warning,
    ):
        hub._listen(stop)
    warning.assert_not_called()


def test_a_wake_is_built_from_everything_that_arrived_together():
    assert _wake_from([("notify", "a")]) == Wake(keys=frozenset({"a"}))
    assert _wake_from([("wake", "a", "typing")]) == Wake(signals=frozenset({"typing"}))
    assert _wake_from([("marker", RESYNC)]) == Wake(resync=True)
    mixed = _wake_from([("notify", "a"), ("notify", "b"), ("wake", "a", "typing"), ("marker", RESYNC), ("notify", "a")])
    assert mixed == Wake(keys=frozenset({"a", "b"}), signals=frozenset({"typing"}), resync=True)
    assert mixed.data_may_have_changed
    assert not Wake(signals=frozenset({"typing"})).data_may_have_changed


def test_a_wake_remembers_which_keys_each_signal_was_sent_for():
    wake = _wake_from([("wake", "a", "typing"), ("wake", "b", "typing"), ("wake", "a", "presence"), ("notify", "c")])
    assert wake.signal_keys == {"typing": frozenset({"a", "b"}), "presence": frozenset({"a"})}
    assert wake.signals == frozenset(wake.signal_keys)
    assert _wake_from([("notify", "a")]).signal_keys == {}


def test_signal_keys_are_not_part_of_equality_or_the_hash():
    found = _wake_from([("wake", "a", "typing")])
    assert found == Wake(signals=frozenset({"typing"}))  # what a test would write
    assert hash(found) == hash(Wake(signals=frozenset({"typing"})))
    assert found != Wake(signals=frozenset({"presence"}))


def _one(after):
    return []


def _two(after, wake):
    return []


def _one_with_room(room, after):
    return []


def _two_with_room(room, after, wake):
    return []


async def _async_two(after, wake):
    return []


def _star(*args):
    return []


def _three_with_room(room, after, limit=500):
    return []


class _Reader:
    def read(self, after, wake):
        return []


@pytest.mark.parametrize(
    ("read", "wants_wake"),
    [
        (_one, False),
        (_two, True),
        (lambda after: [], False),
        (lambda after, wake: [], True),
        (partial(_one_with_room, "r"), False),
        (partial(_two_with_room, "r"), True),
        (_async_two, True),
        # A defaulted second parameter is not a wake: the Wake would land in `limit` and the query would fail.
        (lambda after, limit=10: [], False),
        (partial(_three_with_room, "r"), False),
        # Unless it is named `wake`: then it is one, defaulted or not.
        (lambda after, wake=None: [], True),
        # Not callable as read(cursor, wake), so not a wake reader.
        (lambda after, *, wake: [], False),
        # `*args` binds one argument too, so it is a read(cursor): a Wake is never passed to it.
        (_star, False),
        (_Reader().read, True),
    ],
)
def test_a_read_that_takes_a_second_argument_gets_a_wake(read, wants_wake):
    assert _accepts_wake(read) is wants_wake


def test_a_read_without_a_signature_is_assumed_to_take_the_cursor_only():
    with mock.patch.object(hub_module.inspect, "signature", side_effect=ValueError):
        assert _accepts_wake(_two) is False


async def test_the_listener_connects_under_the_application_name_it_is_given(db):
    name = f"svc-{db.channel}"
    hub = make_hub(db, application_name=name)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        rows = execute(db.dsn, "SELECT count(*) FROM pg_stat_activity WHERE application_name = %s", (name,))
        assert rows == [(1,)]


async def test_close_ends_the_streams_of_one_owner_and_frees_their_slots(db):
    hub = make_hub(db)
    async with hub.lifespan():
        ann = [hub.subscribe("a", owner="ann"), hub.subscribe("b", owner="ann")]
        bob = hub.subscribe("a", owner="bob")
        hub.close(owner="ann")
        await until(lambda: all(s.ended for s in ann))
        assert await ann[0].wait(1) == [("marker", REVOKED)]
        assert not bob.ended
        for subscription in ann:
            hub.unsubscribe(subscription)  # as each stream does once it has said goodbye
        assert hub.stats().open_streams == 1
        assert hub.subscribe("a", owner="ann")  # revoking is not a ban: the route decides whether this one is let in


async def test_close_by_key_ends_that_keys_streams_and_not_the_all_keys_streams(db):
    hub = make_hub(db)
    async with hub.lifespan():
        in_room = [hub.subscribe("room", owner="ann"), hub.subscribe("room")]
        elsewhere, everything = hub.subscribe("other"), hub.subscribe(None)
        hub.close(key="room")
        await until(lambda: all(s.ended for s in in_room))
        assert not elsewhere.ended
        assert not everything.ended


async def test_close_with_an_owner_and_a_key_ends_only_streams_that_match_both(db):
    hub = make_hub(db)
    async with hub.lifespan():
        match = hub.subscribe("room", owner="ann")
        wrong_key, wrong_owner = hub.subscribe("other", owner="ann"), hub.subscribe("room", owner="bob")
        hub.close(owner="ann", key="room")
        await until(lambda: match.ended)
        assert not wrong_key.ended
        assert not wrong_owner.ended


async def test_close_on_the_event_loop_takes_effect_before_it_returns(db):
    hub = make_hub(db)
    async with hub.lifespan():
        old = hub.subscribe("room", owner="ann")
        hub.close(owner="ann", key="room")
        assert old.ended
        new = hub.subscribe("room", owner="ann")  # opened after the call: not ended with the old one
        assert not new.ended


async def test_close_works_from_a_plain_thread(db):
    hub = make_hub(db)
    async with hub.lifespan():
        subscription = hub.subscribe("room", owner="ann")
        thread = threading.Thread(target=lambda: hub.close(owner="ann"))
        thread.start()
        thread.join()
        await until(lambda: subscription.ended)


async def test_close_needs_something_to_match_and_a_text_key(db):
    hub = make_hub(db)
    with pytest.raises(ValueError, match="close_all"):
        hub.close()
    with pytest.raises(TypeError, match="key must be a str"):
        hub.close(key=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"owner must be a str, an int or None, got UUID; pass str\(owner\)"):
        hub.close(owner=uuid.uuid4())  # type: ignore[arg-type]  # a stream opened with its text would never match
    with pytest.raises(TypeError, match="got bool"):
        hub.close(owner=True)


async def test_a_stream_cannot_be_opened_with_an_owner_close_could_never_match(db):
    hub = make_hub(db)
    async with hub.lifespan():
        with pytest.raises(TypeError, match="owner must be a str, an int or None"):
            hub.subscribe("a", 7.0)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="owner must be a str, an int or None"):
            hub.stream("a", read=lambda after: [], owner=b"7")  # type: ignore[arg-type]
        assert hub.stats().open_streams == 0


async def test_an_int_owner_is_text_so_close_finds_it_either_way(db):
    hub = make_hub(db, max_per_owner=1)
    async with hub.lifespan():
        first = hub.subscribe("a", 7)
        assert first.owner == "7"
        with pytest.raises(TooManySubscribers):
            hub.subscribe("a", "7")  # the same owner
        hub.close(owner="7")
        assert first.ended
        hub.unsubscribe(first)
        second = hub.subscribe("a", "7")
        hub.close(owner=7)
        assert second.ended


async def test_leaving_the_lifespan_cancels_resyncs_still_held_back(db):
    hub = make_hub(db, resync_spread=5)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)
        subscription = hub.subscribe("a")
        subscription.hub_stream = True
        hub._resync()
        timer = subscription.resync_timer
        assert timer is not None
    assert timer.cancelled()
    assert subscription.resync_timer is None


async def test_a_notification_ends_the_held_back_resync_of_its_key_but_not_of_the_all_keys_streams(db):
    # Every notification reaches an all-keys stream: on a busy channel the first one would end all their waits at once.
    hub = make_hub(db, resync_spread=5)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)
        room, every_room = hub.subscribe("a"), hub.subscribe(None)
        room.hub_stream = every_room.hub_stream = True
        hub._resync()
        hub._notify("a")
        assert await room.wait(0.5) == [("notify", "a"), ("marker", RESYNC)]
        assert await every_room.wait(0.5) == [("notify", "a")]
        assert every_room.resync_timer is not None  # still waits its turn


@pytest.mark.parametrize("by", ["notification", "timer"])
async def test_a_resync_takes_no_queue_slot_so_it_never_drops_a_busy_stream(db, by):
    hub = make_hub(db, resync_spread=5, queue_size=1)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)
        room = hub.subscribe("a")
        room.hub_stream = True
        hub._resync()
        if by == "notification":
            item = ("notify", "a")
            hub._notify("a")
        else:
            item = ("wake", "a", "typing")
            hub._dispatch(item, "a")  # fills the queue without ending the wait
            hub._fire_resync(room)  # the timer's time came
        assert not room.ended
        assert hub.stats().dropped_total == 0
        assert room.resync_pending
        assert room.resync_timer is None
        assert await room.wait(0.5) == [item, ("marker", RESYNC)]
        assert not room.resync_pending


async def test_a_stream_asked_for_after_the_hub_stopped_is_refused_as_during_shutdown(db):
    hub = make_hub(db)
    async with hub.lifespan():
        pass
    with pytest.raises(HTTPException) as refused:
        hub.sse("a", read=lambda after: [])
    assert refused.value.status_code == 503
    with pytest.raises(TooManySubscribers, match="shutting down"):
        hub.subscribe("a")


@pytest.mark.parametrize("hub_stream", [True, False])
async def test_with_no_spread_a_resync_comes_at_once_even_to_a_full_queue(db, hub_stream):
    hub = make_hub(db, resync_spread=0, queue_size=1)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)
        room = hub.subscribe("a")
        room.hub_stream = hub_stream
        hub._dispatch(("wake", "a", "typing"), "a")
        hub._resync()
        assert not room.ended
        assert await room.wait(0.5) == [("wake", "a", "typing"), ("marker", RESYNC)]


async def test_shutdown_replaces_a_revoke_not_yet_said(db):
    hub = make_hub(db)
    async with hub.lifespan():
        revoked = hub.subscribe("a", "ann")
        revoked.hub_stream = True  # as `hub.stream` marks its own: it stays known until it has said goodbye
        hub.close(owner="ann")
        hub.close_all()
        assert revoked.end_marker == CLOSING  # its goodbye says shutdown, with the spread-out retry
        assert await revoked.wait(0.5) == [("marker", CLOSING)]


async def test_a_resync_skips_a_stream_that_already_ended(db):
    hub = make_hub(db, resync_spread=5)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)
        ended = hub.subscribe("a", "ann")
        ended.hub_stream = True
        hub.close(owner="ann")
        hub._resync()
        assert ended.resync_timer is None


def test_a_signal_that_is_not_text_is_refused(db):
    hub = make_hub(db)
    with pytest.raises(TypeError, match="a signal must be a str"):
        hub.wake("a", 1)  # type: ignore[arg-type]  # it would never match a label
    with pytest.raises(TypeError, match="a signal must be a str"):
        hub.ephemeral("typing", ttl=8, signal=1)  # type: ignore[arg-type]


def test_an_on_error_that_cannot_be_called_is_refused():
    with pytest.raises(TypeError, match="on_error must be a function"):
        Hub("postgresql://x", "c", on_error="sentry")  # type: ignore[arg-type]


async def test_close_all_before_the_hub_ever_ran_leaves_it_saying_it_is_not_running(db):
    hub = make_hub(db)
    hub.close_all()  # a teardown or a signal hook, with no lifespan yet
    assert not hub.closing
    with pytest.raises(RuntimeError, match="the hub is not running"):
        hub.subscribe("a")


async def test_a_read_that_cannot_be_called_is_refused_clearly(db):
    hub = make_hub(db)
    async with hub.lifespan():
        with pytest.raises(TypeError, match="read must be a function"):
            hub.stream("a", read=None)  # type: ignore[arg-type]
        assert hub.stats().open_streams == 0
    with pytest.raises(TypeError, match="read must be a function"):
        distinct("not a read", key=lambda event: event)  # type: ignore[arg-type]


def test_every_goodbye_reason_is_counted_and_has_a_retry():
    reasons = {reason for _, reason, _ in hub_module._ENDS.values() if reason}
    assert reasons == set(dataclasses.asdict(EndedTotals())) == set(hub_module._RETRY_MS)


async def test_a_stopped_hub_posts_nothing_to_the_loop_it_ran_on(db):
    hub = make_hub(db)
    async with hub.lifespan():
        pass
    closer = threading.Thread(target=hub.close, kwargs={"owner": "ann"})
    with mock.patch.object(asyncio.get_running_loop(), "call_soon_threadsafe") as post:
        hub.wake("a")
        hub.call_later(1, print)
        hub.close(owner="ann")
        closer.start()
        closer.join()
    post.assert_not_called()


async def test_a_repeated_notification_needs_room_only_for_the_held_back_resync(db):
    hub = make_hub(db, resync_spread=5, queue_size=2)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)
        room = hub.subscribe("a")
        room.hub_stream = True
        hub._notify("a")  # queued: one slot left
        hub._resync()
        hub._notify("a")  # a repeat takes no slot, so the resync gets the last one
        assert room.resync_timer is None
        assert await room.wait(0.5) == [("notify", "a"), ("marker", RESYNC)]


async def test_streams_lists_every_open_subscription_oldest_first_with_its_age(db):
    hub = make_hub(db)
    assert hub.streams() == []
    async with hub.lifespan():
        first = hub.subscribe("a", owner="ann")
        await asyncio.sleep(0.05)
        hub.subscribe(None)
        info = hub.streams()
        assert [(s.key, s.owner) for s in info] == [("a", "ann"), (None, None)]
        assert info[0].age_seconds >= 0.05
        assert info[0].age_seconds > info[1].age_seconds
        hub.unsubscribe(first)
        assert [s.key for s in hub.streams()] == [None]
    assert hub.streams() == []


async def test_a_held_back_resync_reaches_a_subscription_late_and_never_one_that_is_gone(db):
    hub = make_hub(db, resync_spread=0.2)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)  # the first LISTEN's resync is delivered before these subscribe
        kept, gone = hub.subscribe("a"), hub.subscribe("b")
        kept.hub_stream = gone.hub_stream = True  # as `hub.stream` marks its own
        hub._resync()
        hub.unsubscribe(gone)
        assert gone.resync_timer is None
        assert await kept.wait(1) == [("marker", RESYNC)]
        await asyncio.sleep(0.25)
        assert not gone._items


async def test_a_subscribe_queue_gets_its_resync_at_once_while_a_stream_waits_its_turn(db, monkeypatch):
    monkeypatch.setattr(hub_module.random, "uniform", lambda low, high: high)  # a random wait could be under 0.2 s
    hub = make_hub(db, resync_spread=5)
    reads: list[Wake] = []

    def read(after, wake):
        reads.append(wake)
        return []

    async with hub.lifespan():
        await until(hub.listening.is_set)
        await asyncio.sleep(0)
        raw = hub.subscribe("a")
        body = hub.stream("a", read=read, cursor=0)
        assert await anext(body) == "id: 0\n\n"
        reading = asyncio.ensure_future(anext(body))  # a stream only runs while it is read
        await until(lambda: len(reads) == 1)
        hub._resync()
        assert await raw.wait(0.5) == [("marker", RESYNC)]
        await asyncio.sleep(0.2)
        assert len(reads) == 1  # the stream's resync is still held back
        reading.cancel()
        await asyncio.gather(reading, return_exceptions=True)
        await body.aclose()
