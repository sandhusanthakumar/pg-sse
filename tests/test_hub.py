import asyncio
import contextlib
import os
import signal
import threading
import time
from functools import partial
from unittest import mock

import psycopg
import pytest
from helpers import execute, make_hub, terminate_listener, until

from pg_sse import Hub, HubStats, TooManySubscribers, Wake
from pg_sse import _hub as hub_module
from pg_sse._hub import CLOSING, DROPPED, RESYNC, _accepts_wake, _wake_from

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
        await until(lambda: every_key.queue.qsize() == 2)
        assert await subscription.wait(1) == [("notify", "a")]  # the repeats were not queued again
        assert await every_key.wait(1) == [("notify", "a"), ("notify", "b")]


async def test_identical_wake_ups_do_not_fill_the_queue(db):
    hub = make_hub(db, queue_size=2)
    async with hub.lifespan():
        subscription = hub.subscribe("a")
        for _ in range(100):
            hub.wake("a")
        await until(lambda: subscription.queue.qsize() == 1)
        await asyncio.sleep(0.05)  # the rest of the 100 have been dispatched too
        assert not subscription.ended
        assert await subscription.wait(1) == [("wake", "a", "wake")]
        assert subscription.queue.empty()
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


async def test_the_listener_checks_a_quiet_connection(db, monkeypatch):
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
        assert subscription.queue.empty()
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
            hub._dispatch(("notify", "a"), "a")
            assert {call.args[0] for call in offer.call_args_list} == {*same_room, every_room}
            offer.reset_mock()
            hub._dispatch(("wake", "a", "typing"), "a", all_keys=False)
            assert {call.args[0] for call in offer.call_args_list} == set(same_room)
            offer.reset_mock()
            hub._dispatch(("notify", "nobody"), "nobody")
            assert {call.args[0] for call in offer.call_args_list} == {every_room}
            offer.reset_mock()
            hub._dispatch(("marker", RESYNC))  # no key: every stream
            assert offer.call_count == 200 + 3


async def test_the_fan_out_index_is_empty_however_a_subscription_ends(db):
    hub = make_hub(db, queue_size=1)
    async with hub.lifespan():
        await until(hub.listening.is_set)
        first, second, dropped = hub.subscribe("a"), hub.subscribe("a"), hub.subscribe("b")
        kept_all, unsubscribed_all = hub.subscribe(None), hub.subscribe(None)
        hub.unsubscribe(first)
        assert hub._by_key["a"] == {second}  # the key stays while another stream watches it
        hub.unsubscribe(second)
        hub.unsubscribe(second)  # twice is harmless
        assert set(hub._by_key) == {"b"}
        hub.unsubscribe(unsubscribed_all)
        assert hub._watching_all == {kept_all}
        hub._dispatch(("notify", "x"), "b", all_keys=False)
        hub._dispatch(("notify", "y"), "b", all_keys=False)  # a second distinct item overflows a queue of one
        assert dropped.ended
        assert hub._by_key == {}
        assert hub._watching_all == {kept_all}
        hub.subscribe("c")
    assert hub._subscriptions == set()  # leaving the lifespan forgets the rest
    assert hub._by_key == {}
    assert hub._watching_all == set()


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


async def test_lifespan_waits_briefly_for_the_listener(db):
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
        ({"reread_seconds": 0}, "reread_seconds"),
        ({"max_total_per_owner": 0}, "max_total_per_owner"),
        ({"queue_size": 0}, "queue_size"),
        ({"queue_size": -1}, "queue_size"),
    ],
)
def test_bad_settings_are_refused(kwargs, message):
    with pytest.raises(ValueError, match=message):
        Hub("postgresql://x", **{"channel": "c", **kwargs})


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
    assert hub.stats() == HubStats(0, 0, 0, False, 0, None)
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
