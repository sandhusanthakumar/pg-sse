"""
The hub: one LISTEN connection per process, fanned out to every open stream.

A notification carries only a key and means "re-read from your cursor"; it never carries the data. So a lost
notification costs a delay, never an event:

- every stream subscribes before its catch-up read, so a write between the two is a queued wake-up, not a gap;
- after the listener connects or reconnects, every stream re-reads (anything committed before its LISTEN existed
  was missed);
- every stream re-reads when nothing woke it for a while anyway.

The listener is a thread holding one autocommit connection; each notification is handed to the event loop, where
each stream has a bounded queue. A full queue ends that stream instead of blocking or growing: the browser reconnects
with Last-Event-ID and catches up from the database.
"""

import asyncio
import contextlib
import inspect
import logging
import random
import signal
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import FrameType
from typing import TYPE_CHECKING, Any, NoReturn

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from ._event import Event, encode, encode_cursor, encode_goodbye

if TYPE_CHECKING:
    from starlette.responses import StreamingResponse

    from ._ephemeral import Ephemeral

logger = logging.getLogger("pg_sse")

# A queue holds ("notify", key) for a NOTIFY, ("wake", key, signal) for a `hub.wake`, or ("marker", m) with one of:
# Internal: not exported, since a custom loop only needs `Subscription.ended` and the item shapes `subscribe` documents.
RESYNC = "\x00resync"
DROPPED = "\x00dropped"
CLOSING = "\x00closing"
QueueItem = tuple[str, ...]

# The listener runs a trivial query this often, so a half-open connection (no FIN, no notifications) is noticed and
# replaced; TCP keepalives bound how long that query can hang.
_KEEPALIVE_QUERY_SECONDS = 60.0
_TCP_KEEPALIVES = {"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 3}
# A connect that hangs (a black-holed host) must not pin the listener thread; a DSN's own connect_timeout wins.
_CONNECT_TIMEOUT_SECONDS = 10
# The listener runs nothing but LISTEN and SELECT 1, so a query that takes this long is stuck.
_STATEMENT_TIMEOUT = "30s"
_BACKOFF_START_SECONDS = 0.5
_BACKOFF_MAX_SECONDS = 30.0
# How often the listener wakes from a quiet connection to check whether it was asked to stop.
_LISTEN_POLL_SECONDS = 1.0

STREAM_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}

# Before a planned close, the last frame says why and how long the client should wait before reconnecting (ms).
_RETRY_MS = {"max_age": 1000, "behind": 1000, "busy": 30_000}
_SHUTDOWN_RETRY_MS = (1000, 5000)  # jittered, so a deploy's reconnects are spread out

# The types of `read` and `start`, public so a helper that wraps `hub.sse` can annotate them.
Read = Callable[..., Iterable[Event] | Awaitable[Iterable[Event]]]  # read(cursor) or read(cursor, wake)
Start = Callable[[], Any]  # start() gives the current cursor; plain or async


class TooManySubscribers(Exception):
    """A cap is reached or the hub is shutting down. `Hub.sse` turns it into a 503 with Retry-After."""


@dataclass(frozen=True, slots=True)
class HubStats:
    """
    A snapshot for a health check or a metrics endpoint.

    - `open_streams`: subscriptions currently open.
    - `refused_total`, `dropped_total`: streams refused at a cap (or during shutdown) and streams ended for falling
      behind, since the process started.
    - `listener_connected`: the listener holds its LISTEN right now.
    - `listener_reconnects_total`: how often the listener got its connection back after losing it.
    - `last_notification_at`: Unix time of the last notification the listener received, or None if none yet.
    """

    open_streams: int
    refused_total: int
    dropped_total: int
    listener_connected: bool
    listener_reconnects_total: int
    last_notification_at: float | None


@dataclass(frozen=True, slots=True)
class Wake:
    """
    Why `read` is running, for a `read(cursor, wake)`. Several causes can arrive together: a burst is one read.

    - `start`: the stream's first read.
    - `keys`: keys with a database notification since the last read.
    - `signals`: labels from `hub.wake(key, signal=...)` since the last read.
    - `resync`: the listener connected or reconnected, so anything may have changed.
    - `reread`: nothing woke the stream for `reread_seconds`.
    """

    start: bool = False
    keys: frozenset[str] = frozenset()
    signals: frozenset[str] = frozenset()
    resync: bool = False
    reread: bool = False

    @property
    def data_may_have_changed(self) -> bool:
        """True unless the only causes are `hub.wake` signals."""
        return self.start or self.resync or self.reread or bool(self.keys)


class Subscription:
    """One stream's queue. `key` is what it watches, or None for every key. Touched only on the event loop."""

    def __init__(self, key: str | None, owner: str | None, queue_size: int) -> None:
        self.key = key
        self.owner = owner
        self.queue: asyncio.Queue[QueueItem] = asyncio.Queue(queue_size)
        # What is queued, so a repeat of it is not queued again: the queue holds at most `queue_size` distinct items.
        self._pending: set[QueueItem] = set()
        self.ended = False

    def offer(self, item: QueueItem) -> bool:
        """Queues a wake-up. False when the queue was full, so this call ended the stream instead."""
        if self.ended or item in self._pending:  # a duplicate wake-up adds nothing to the next read
            return True
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            self._end(DROPPED)
            return False
        self._pending.add(item)
        return True

    def _end(self, marker: str) -> None:
        """Replaces whatever is queued with a marker the stream reads as its last item."""
        self.ended = True
        while not self.queue.empty():
            self.queue.get_nowait()
        self._pending.clear()
        self.queue.put_nowait(("marker", marker))

    def close(self) -> None:
        if not self.ended:
            self._end(CLOSING)

    async def wait(self, timeout: float) -> list[QueueItem] | None:  # noqa: ASYNC109 -- the ping interval, handed to wait_for
        """Everything queued once there is anything (a burst costs one re-read), or None if the timeout came first."""
        try:
            first = await asyncio.wait_for(self.queue.get(), timeout)
        except TimeoutError:
            return None
        items = [first]
        while not self.queue.empty():
            items.append(self.queue.get_nowait())
        self._pending.difference_update(items)
        return items


class Hub:
    """
    Live updates for one NOTIFY channel.

        hub = Hub(DIRECT_DATABASE_URL, channel="room_change")
        app = FastAPI(lifespan=hub.lifespan)

        @app.get("/rooms/{room}/stream")
        async def stream(room: str, last_event_id: Annotated[str | None, Header()] = None):
            return hub.sse(room, read=lambda after: read_after(room, after), cursor=..., start=...)

    `conninfo` must be a direct connection: behind a transaction-mode pooler (PgBouncer in transaction mode, the
    pooled URLs Supabase and Neon hand out) LISTEN succeeds and never delivers, and streams only update on their
    periodic re-read.
    """

    def __init__(
        self,
        conninfo: str | Callable[[], str],
        channel: str,
        *,
        max_subscribers: int = 500,
        max_per_owner: int = 5,
        max_total_per_owner: int | None = None,
        queue_size: int = 64,
        max_stream_seconds: float = 15 * 60,
        ping_seconds: tuple[float, float] = (15.0, 25.0),
        reread_seconds: float = 60.0,
        json_default: Callable[[Any], Any] | None = None,
        end_event: str | None = None,
        handle_signals: bool = True,
        listen_timeout: float | None = 5.0,
    ) -> None:
        if not channel or "\x00" in channel or len(channel.encode()) > 63:
            raise ValueError("channel must be a non-empty identifier of at most 63 bytes")
        if max_total_per_owner is not None and max_total_per_owner < 1:
            raise ValueError("max_total_per_owner must be at least 1, or None for no total")
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1 (a zero-size asyncio queue is unbounded)")
        if ping_seconds[0] <= 0 or ping_seconds[0] > ping_seconds[1]:
            raise ValueError("ping_seconds must be (low, high) with 0 < low <= high")
        if reread_seconds <= 0:
            raise ValueError("reread_seconds must be greater than 0")
        # A function is called on every connect, so a rotating password or settings that are only readable at startup
        # need no rebuilt hub.
        self.conninfo = conninfo
        self.channel = channel
        # Open streams per process; past it a stream is refused with a 503.
        self.max_subscribers = max_subscribers
        # Open streams per (key, owner): several tabs on one room are one client, but this does not stop that owner
        # from opening streams on many keys.
        self.max_per_owner = max_per_owner
        # Open streams per owner across all keys, or None for no total. This is the cap that keeps one client from
        # filling `max_subscribers`; without it, an owner may hold `max_per_owner` streams on every key it can name.
        self.max_total_per_owner = max_total_per_owner
        self.queue_size = queue_size
        # A stream is authenticated once, when it opens; ending it after this long bounds how long a revoked session
        # keeps one. The browser reconnects at once with Last-Event-ID.
        self.max_stream_seconds = max_stream_seconds
        # A stream sends a `: ping` comment this often while idle, so a proxy does not time the connection out. It
        # costs no database read. Jittered so streams opened together do not all ping together. Keep a proxy's read
        # timeout above the high end.
        self.ping_seconds = ping_seconds
        # A stream re-reads when nothing woke it for this long: the net under every lost notification, so the worst
        # case for one is this delay. Streams read on their own clocks, so this is a steady trickle, not a spike.
        self.reread_seconds = reread_seconds
        # Passed to `json.dumps(default=...)` for Event data that is not JSON-native, e.g. `str` for datetimes.
        self.json_default = json_default
        # Before a planned close (age limit, shutdown, fell behind, busy) a stream always sends `retry:`, which
        # EventSource applies by itself. With a name here it also sends that event with {"reason": ...}, for clients
        # that are not EventSource and want to know why.
        self.end_event = end_event
        self.handle_signals = handle_signals
        # How long `lifespan` waits for the first LISTEN before it lets the app start, so a health check and the first
        # requests see a live listener. Startup proceeds either way: the listener keeps retrying and `stats` reports it.
        self.listen_timeout = listen_timeout
        self._loop: asyncio.AbstractEventLoop | None = None
        # Every open subscription, and the same ones indexed for fan-out: by key, and the ones watching every key. A
        # notification then costs the streams it reaches, not every open stream. Change them only through
        # `_track` and `_forget`.
        self._subscriptions: set[Subscription] = set()
        self._by_key: dict[str, set[Subscription]] = {}
        self._watching_all: set[Subscription] = set()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        # Set while the listener holds a LISTEN: report it from a health check.
        self.listening = threading.Event()
        self.closing = False
        self._signal_hooks: dict[signal.Signals, _SignalHook] = {}
        self._refused = 0
        self._dropped = 0
        self._reconnects = 0
        self._last_notification_at: float | None = None

    def stats(self) -> HubStats:
        return HubStats(
            open_streams=len(self._subscriptions),
            refused_total=self._refused,
            dropped_total=self._dropped,
            listener_connected=self.listening.is_set(),
            listener_reconnects_total=self._reconnects,
            last_notification_at=self._last_notification_at,
        )

    # --- streams --------------------------------------------------------------------------------

    def stream(
        self,
        key: str | None,
        read: Read,
        *,
        cursor: Any = None,
        start: Start | None = None,
        owner: str | None = None,
        end_event: str | None = None,
    ) -> AsyncIterator[str]:
        """
        The encoded SSE frames for one key (None: every key on the channel), for any framework to send as
        `text/event-stream`.

        - `read(cursor)` returns the events after `cursor`, oldest first, each with its `id` set to the cursor after
          it. It may be a plain function (run in a thread) or async. It runs once at the start and again on every
          wake-up and after `reread_seconds` of quiet, so it must be cheap when nothing changed. A `read` that takes
          a second argument gets a `Wake` saying why it runs.
        - `cursor` is where to start, already parsed from the client (Last-Event-ID or a query parameter). Validate it
          first: it is client input.
        - `start()` gives the current cursor when `cursor` is None, so the stream starts from now. It runs after the
          subscription, so nothing written in between is missed. Without it, `read(None)` decides what None means.
        - `owner` (a user id, a session) applies the per-owner cap. Streams without an owner have no such cap.
        - `end_event`, when given, overrides the hub's: the event name sent with the reason before a planned close.

        Call it from the event loop, after any 404 or permission check: the caps are checked here, so
        `TooManySubscribers` is raised before any response exists. The stream subscribes when its first frame is read
        and unsubscribes when it ends, however it ends.

        `key` must be a `str` (or None): raises `TypeError` for anything else, since a notification payload is text
        and an int would subscribe fine and never be woken.
        """
        _check_key(key, allow_none=True)
        self._require_loop()
        self._check_caps(key, owner)
        if end_event is None:
            end_event = self.end_event
        return self._events(key, owner, read, _accepts_wake(read), cursor, start, end_event)

    def sse(
        self,
        key: str | None,
        read: Read,
        *,
        cursor: Any = None,
        start: Start | None = None,
        owner: str | None = None,
        end_event: str | None = None,
        headers: Mapping[str, str] | None = None,
        refuse_with: Callable[[TooManySubscribers], Exception] | None = None,
    ) -> "StreamingResponse":
        """
        `stream` as a Starlette/FastAPI `text/event-stream` response. Arguments as for `stream`; past a cap it raises
        a 503 with `Retry-After: 30`. Needs `pip install "pg-sse[starlette]"`. Call it from an `async def` route.

        `refuse_with` replaces that 503: it gets the `TooManySubscribers` and returns the exception to raise instead,
        e.g. one your error tracker is set to ignore, since a refused stream is back-pressure, not a fault.
        """
        try:
            from starlette.exceptions import HTTPException
            from starlette.responses import StreamingResponse
        except ImportError:
            raise ImportError('hub.sse needs Starlette: pip install "pg-sse[starlette]" (or use hub.stream)') from None
        try:
            frames = self.stream(key, read, cursor=cursor, start=start, owner=owner, end_event=end_event)
        except TooManySubscribers as err:
            logger.warning("pg_sse refused a stream (%s)", err)
            if refuse_with is not None:
                raise refuse_with(err) from None
            raise HTTPException(
                status_code=503, detail="Live updates are unavailable.", headers={"Retry-After": "30"}
            ) from None
        return StreamingResponse(frames, media_type="text/event-stream", headers={**STREAM_HEADERS, **(headers or {})})

    async def _events(
        self,
        key: str | None,
        owner: str | None,
        read: Read,
        wants_wake: bool,
        cursor: Any,
        start: Start | None,
        end_event: str | None,
    ) -> AsyncIterator[str]:
        try:
            subscription = self.subscribe(key, owner)
        except TooManySubscribers:  # a cap filled, or shutdown began, since `stream` checked
            yield _goodbye(end_event, "shutdown" if self.closing else "busy")
            return
        deadline = time.monotonic() + self.max_stream_seconds
        try:
            if cursor is None and start is not None:
                cursor = await _call(start)
            if cursor is not None:
                # The browser reconnects from here even if no event ever arrives on this stream.
                yield encode_cursor(cursor)
            wake = Wake(start=True)
            while True:
                for event in await (_call(read, cursor, wake) if wants_wake else _call(read, cursor)):
                    if not isinstance(event, Event):
                        raise TypeError(f"read must return pg_sse.Event items, got {type(event).__name__}")
                    yield encode(event, self.json_default)
                    if event.id is not None:
                        cursor = event.id
                    if event.final:
                        return
                reread_at = time.monotonic() + self.reread_seconds
                while True:  # idle: ping until something wakes the stream or a re-read is due
                    now = time.monotonic()
                    if now >= deadline:
                        yield _goodbye(end_event, "max_age")
                        return
                    if now >= reread_at:
                        wake = Wake(reread=True)
                        break
                    timeout = min(random.uniform(*self.ping_seconds), deadline - now, reread_at - now)  # noqa: S311 -- jitter
                    woke = await subscription.wait(timeout)
                    if woke is None:
                        # Every timeout pings, even the one that leads to a re-read: the re-read may send nothing,
                        # and the silence must never outlast a ping interval.
                        if time.monotonic() < deadline:
                            yield ": ping\n\n"
                    elif subscription.ended:  # the marker is the only item left: dropped, or closed at shutdown
                        yield _goodbye(end_event, "shutdown" if woke[0] == ("marker", CLOSING) else "behind")
                        return
                    else:
                        wake = _wake_from(woke)
                        break
        except Exception:
            logger.exception("pg_sse stream for key %r failed; the client reconnects", key)
        finally:
            self.unsubscribe(subscription)

    # --- subscriptions (on the event loop) ---------------------------------------------------------

    def subscribe(self, key: str | None, owner: str | None = None) -> Subscription:
        """
        The low-level API under `stream`: a queue of notifications, wakes and markers. Unsubscribe in a `finally`.
        `key` must be a `str` or None, as for `stream`.

        `await subscription.wait(timeout)` returns everything queued once there is anything, or None on timeout. An
        item is `("notify", key)` for a database notification, `("wake", key, signal)` for a `hub.wake`, or
        `("marker", name)`: re-read everything. Once `subscription.ended` is true the queue holds one marker and the
        stream is over: it fell behind, or the hub is shutting down (`hub.closing`).
        """
        _check_key(key, allow_none=True)
        self._require_loop()
        self._check_caps(key, owner)
        subscription = Subscription(key, owner, self.queue_size)
        self._track(subscription)
        return subscription

    def _track(self, subscription: Subscription) -> None:
        self._subscriptions.add(subscription)
        if subscription.key is None:
            self._watching_all.add(subscription)
        else:
            self._by_key.setdefault(subscription.key, set()).add(subscription)

    def _forget(self, subscription: Subscription) -> None:
        self._subscriptions.discard(subscription)
        self._watching_all.discard(subscription)
        if subscription.key is not None and (watchers := self._by_key.get(subscription.key)) is not None:
            watchers.discard(subscription)
            if not watchers:
                del self._by_key[subscription.key]

    def _check_caps(self, key: str | None, owner: str | None) -> None:
        if self.closing:
            self._refuse("shutting down")
        if len(self._subscriptions) >= self.max_subscribers:
            self._refuse("too many open streams")
        # Without an owner there is nobody to cap: anonymous streams on one key are not one client.
        if owner is None:
            return
        mine = [s for s in self._subscriptions if s.owner == owner]
        if self.max_total_per_owner is not None and len(mine) >= self.max_total_per_owner:
            self._refuse("too many open streams for this owner across all keys")
        if sum(1 for s in mine if s.key == key) >= self.max_per_owner:
            self._refuse("too many open streams for this owner")

    def _refuse(self, reason: str) -> NoReturn:
        self._refused += 1
        raise TooManySubscribers(reason)

    def unsubscribe(self, subscription: Subscription) -> None:
        self._forget(subscription)

    def wake(self, key: str, signal: str = "wake", *, all_keys: bool = True) -> None:
        """
        Wakes one key's streams for a change that is not a database write (someone is typing). Safe from any thread.

        A `read(cursor, wake)` sees `signal` in `wake.signals`. With `all_keys=False` the streams watching every key
        (`key=None`) are left alone: right for ephemeral per-key state that an all-keys view has nothing to show for.
        `key` must be a `str`, as for `stream`.
        """
        _check_key(key, allow_none=False)
        self._post(self._dispatch, ("wake", key, signal), key, all_keys)

    def call_later(self, seconds: float, callback: Callable[..., None], *args: Any) -> None:
        """
        Runs `callback(*args)` on the hub's event loop after `seconds`. Safe from any thread; does nothing while the
        hub has no running loop. For follow-up work on ephemeral state, e.g. checking that a typing signal lapsed, then
        calling `wake`.
        """
        loop = self._loop
        if loop is None:
            return

        def schedule() -> None:
            loop.call_later(seconds, callback, *args)

        self._post(schedule)

    def ephemeral(
        self,
        name: str,
        *,
        ttl: float,
        signal: str | None = None,
        grace: float = 0.25,
        all_keys: bool = False,
        clock: Callable[[], float] = time.monotonic,
    ) -> "Ephemeral":
        """`Ephemeral(self, name, ...)`: per-key signals that lapse unless renewed (typing, presence)."""
        from ._ephemeral import Ephemeral

        return Ephemeral(self, name, ttl=ttl, signal=signal, grace=grace, all_keys=all_keys, clock=clock)

    def close_all(self) -> None:
        """Ends every stream and refuses new ones, so a shutdown does not wait out the graceful-shutdown timeout."""
        self.closing = True
        for subscription in list(self._subscriptions):
            subscription.close()

    def _require_loop(self) -> None:
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "pg_sse: call hub.stream(), hub.sse() or hub.subscribe() from an `async def` route"
            ) from None
        if self._loop is None:
            raise RuntimeError(
                "pg_sse: the hub is not running; use FastAPI(lifespan=hub.lifespan) or `async with hub.lifespan()`"
            )
        if running is not self._loop:
            raise RuntimeError("pg_sse: the hub runs on another event loop")

    def _dispatch(self, item: QueueItem, key: str | None = None, all_keys: bool = True) -> None:
        """Offers `item` to the streams for `key` and (unless `all_keys` is False) the all-keys streams; to every
        stream when `key` is None."""
        if key is None:
            targets = list(self._subscriptions)
        else:
            targets = list(self._by_key.get(key, ()))
            if all_keys:
                targets.extend(self._watching_all)
        for subscription in targets:
            self._offer(subscription, item)

    def _offer(self, subscription: Subscription, item: QueueItem) -> None:
        if not subscription.offer(item):
            # Dropped for falling behind: its stream ends on its next read, and the hub forgets it now.
            self._dropped += 1
            self._forget(subscription)

    # --- lifecycle --------------------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, _app: Any = None) -> AsyncIterator[None]:
        """
        `FastAPI(lifespan=hub.lifespan)`, or `async with hub.lifespan():` inside your own lifespan. On exit the hub
        forgets every subscription, including `hub.subscribe` ones never unsubscribed, so it can start again clean.
        """
        loop = asyncio.get_running_loop()
        self.start(loop)
        try:
            if self.handle_signals:
                self._install_signal_hook(loop)
            if self.listen_timeout is not None:
                await asyncio.to_thread(self.listening.wait, self.listen_timeout)
            yield
        finally:
            self.close_all()
            self._remove_signal_hook()
            await asyncio.to_thread(self.stop)
            self._subscriptions.clear()
            self._by_key.clear()
            self._watching_all.clear()

    def start(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        if self._thread is not None:
            return
        self._loop = loop or asyncio.get_running_loop()
        # One flag per start, held by its own thread: a listener that `stop` gave up waiting for (a hung connect) must
        # never see the flag of a later start cleared and resume.
        self._stop = threading.Event()
        self.closing = False
        self._thread = threading.Thread(target=self._listen, args=(self._stop,), name="pg-sse-listener", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        thread, self._thread = self._thread, None
        if thread is None:
            return
        self._stop.set()
        self.listening.clear()
        thread.join(timeout=_LISTEN_POLL_SECONDS + 1)  # the listener checks the flag every poll

    def _install_signal_hook(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        uvicorn waits for open connections to finish before it runs the lifespan's shutdown, so an idle stream would
        hold a deploy for the whole graceful-shutdown timeout. Chain onto SIGTERM/SIGINT (after the server installed
        its own handlers) to end the streams the moment the signal arrives. A no-op off the main thread.
        """
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in (signal.SIGTERM, signal.SIGINT):
            if sig in self._signal_hooks:
                continue
            previous = signal.getsignal(sig)
            if previous == signal.SIG_DFL:
                # The default action terminates the process at once, which already ends every stream. Nothing to
                # shorten, and chaining would swallow the signal: a script with no server would never exit.
                continue
            hook = _SignalHook(previous)

            def handler(signum: int, frame: FrameType | None, hook: _SignalHook = hook) -> None:
                if hook.active and not loop.is_closed():
                    loop.call_soon_threadsafe(self.close_all)
                if callable(hook.previous):
                    hook.previous(signum, frame)

            hook.handler = handler
            signal.signal(sig, handler)
            self._signal_hooks[sig] = hook

    def _remove_signal_hook(self) -> None:
        """Puts the handlers back, so entering `lifespan` again does not stack another layer on this one."""
        for sig, hook in self._signal_hooks.items():
            # Someone wrapped a handler around ours since (another hub, say): removing ours would remove theirs too, so
            # ours stays in the chain, inert, and only passes the signal on.
            hook.active = False
            if hook.previous is not None and signal.getsignal(sig) is hook.handler:
                signal.signal(sig, hook.previous)
        self._signal_hooks.clear()

    # --- the listener thread ----------------------------------------------------------------------

    def _post(self, callback: Callable[..., None], *args: Any) -> None:
        loop = self._loop
        if loop is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):  # the loop closed between the check and the call
                loop.call_soon_threadsafe(callback, *args)

    def _listener_conninfo(self) -> str:
        base = self.conninfo() if callable(self.conninfo) else self.conninfo
        settings: dict[str, Any] = dict(_TCP_KEEPALIVES)
        if "connect_timeout" not in conninfo_to_dict(base):
            settings["connect_timeout"] = _CONNECT_TIMEOUT_SECONDS
        return make_conninfo(base, **settings)

    def _listen(self, stop: threading.Event) -> None:
        backoff = _BACKOFF_START_SECONDS
        connected_before = False
        while not stop.is_set():
            try:
                conninfo = self._listener_conninfo()
                with psycopg.connect(conninfo, autocommit=True, application_name="pg-sse-listener") as conn:
                    if stop.is_set():  # `stop` gave up on this thread while it was connecting: it is not the listener
                        return
                    conn.execute(f"SET statement_timeout = '{_STATEMENT_TIMEOUT}'")
                    conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel)))
                    backoff = _BACKOFF_START_SECONDS
                    if connected_before:
                        self._reconnects += 1
                    connected_before = True
                    # Anything committed before this LISTEN existed (first connect or reconnect) was missed: every
                    # stream re-reads. Posted before `listening` is set, so whoever sees it set subscribes after the
                    # marker has been dispatched. With no stream open yet it is a no-op.
                    self._post(self._dispatch, ("marker", RESYNC))
                    self.listening.set()
                    last_checked = time.monotonic()
                    while not stop.is_set():
                        if time.monotonic() - last_checked >= _KEEPALIVE_QUERY_SECONDS:
                            conn.execute("SELECT 1")
                            last_checked = time.monotonic()
                        for notification in conn.notifies(timeout=_LISTEN_POLL_SECONDS):
                            self._last_notification_at = time.time()
                            self._post(self._dispatch, ("notify", notification.payload), notification.payload)
            except Exception:
                if stop.is_set():  # `stop` already cleared `listening`; a later start may have set it again
                    return
                self.listening.clear()
                logger.warning("pg_sse listener lost its connection; retrying in %.1fs", backoff, exc_info=True)
                connected_before = True
                stop.wait(backoff * random.uniform(0.5, 1.0))  # noqa: S311 -- reconnect jitter
                backoff = min(backoff * 2, _BACKOFF_MAX_SECONDS)


@dataclass(slots=True)
class _SignalHook:
    """A handler this hub installed over `previous`. Once `active` is False it only passes the signal on."""

    previous: Any
    handler: Callable[[int, FrameType | None], None] | None = None
    active: bool = True


def _check_key(key: object, *, allow_none: bool) -> None:
    """
    A notification's payload is text, so a key that is not a `str` compares unequal to every payload: the stream
    subscribes, nothing ever wakes it, and nothing logs. Refuse it where the mistake is made.
    """
    if isinstance(key, str) or (allow_none and key is None):
        return
    raise TypeError(
        f"pg_sse: key must be a str{' or None' if allow_none else ''}, got {type(key).__name__}. "
        "A notification carries the key column as text: pass str(key) for an integer or UUID column."
    )


def _goodbye(event_name: str | None, reason: str) -> str:
    retry_ms = random.randint(*_SHUTDOWN_RETRY_MS) if reason == "shutdown" else _RETRY_MS[reason]  # noqa: S311
    return encode_goodbye(event_name, reason, retry_ms)


def _wake_from(items: list[QueueItem]) -> Wake:
    keys = frozenset(item[1] for item in items if item[0] == "notify")
    signals = frozenset(item[2] for item in items if item[0] == "wake")
    return Wake(keys=keys, signals=signals, resync=("marker", RESYNC) in items)


def _accepts_wake(fn: Callable[..., Any]) -> bool:
    """
    Whether `read` is read(cursor, wake): its second positional parameter is named `wake` or has no default. A
    defaulted second parameter of another name (`limit=500`) is not a wake, and neither is `*args`. Handles lambdas,
    partials (their bound arguments are gone from the signature) and methods.
    """
    try:
        parameters = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):  # no signature available (some builtins): assume read(cursor)
        return False
    positional = [p for p in parameters if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    if len(positional) < 2:
        return False
    second = positional[1]
    return second.name == "wake" or second.default is second.empty


async def _call(fn: Callable[..., Any], *args: Any) -> Any:
    """Awaits an async callable on the loop; runs a plain one in a thread (a lambda returning a coroutine works too)."""
    if inspect.iscoroutinefunction(fn):
        return await fn(*args)
    result = await asyncio.to_thread(fn, *args)
    if inspect.isawaitable(result):
        return await result
    return result
