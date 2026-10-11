"""
The hub: one LISTEN connection per process, fanned out to every open stream.

A notification carries only a key and means "re-read from your cursor"; it never carries the data. So a lost
notification costs a delay, never an event:

- every stream subscribes before its catch-up read, so a write between the two is a queued wake-up, not a gap;
- after the listener connects or reconnects, every stream re-reads (anything committed before its LISTEN existed
  was missed), each after its own random delay of up to `resync_spread` seconds so they do not all read at once;
- every stream re-reads when nothing woke it for a while anyway.

The listener is a thread holding one autocommit connection; each notification is handed to the event loop, where
each stream has a bounded queue. A full queue ends that stream instead of blocking or growing: the browser reconnects
with Last-Event-ID and catches up from the database.
"""

import asyncio
import concurrent.futures
import contextlib
import contextvars
import decimal
import functools
import inspect
import itertools
import logging
import math
import numbers
import random
import signal
import sys
import threading
import time
import weakref
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from types import FrameType
from typing import TYPE_CHECKING, Any, NoReturn, cast

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from ._event import Event, Throttle, check_event_name, encode, encode_cursor, encode_goodbye

if TYPE_CHECKING:
    from starlette.responses import StreamingResponse

    from ._ephemeral import Ephemeral

logger = logging.getLogger("pg_sse")

# A queue holds ("notify", key) for a NOTIFY, ("wake", key, signal) for a `hub.wake`, or ("marker", m): RESYNC (re-read
# everything), or, as its last item, the end marker that says why the subscription ended. The markers are internal, not
# exported: a custom loop needs only `Subscription.ended` and the item shapes `subscribe` documents.
RESYNC = "\x00resync"
DROPPED = "\x00dropped"
CLOSING = "\x00closing"
REVOKED = "\x00revoked"
RECHECK = "\x00recheck"  # its `revalidate` failed, so the route is asked again
UNREAD = "\x00unread"  # not read within `_UNREAD_SECONDS` of being made
AGED = "\x00aged"  # reached its age limit
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
_bad_cursor = Throttle()  # a cursor that cannot be an SSE id: any client can send one
# How long a client refused at a cap waits before it retries (the 503's Retry-After).
_RETRY_AFTER_SECONDS = 30
# A server reads a stream as soon as it sends the response. One never read (the route raised after making it, or the
# client left before the body started) gives its slot back when it is garbage-collected, at once on CPython, and this
# long after `stream()` made it at the latest: a reference cycle or a runtime without reference counting can hold off
# the collector. As long as the Retry-After, so a refused client's first retry gets in.
_UNREAD_SECONDS = float(_RETRY_AFTER_SECONDS)
# A stream the hub ended (dropped, revoked, shut down) keeps its slot until it has said goodbye, so a client that stops
# reading cannot open one stream after another; at most this long, so a half-open connection, whose send blocks until
# TCP gives up, does not hold it for minutes.
_GOODBYE_GRACE_SECONDS = 10.0
# How long leaving `lifespan` waits for async `on_error` calls still running before it cancels them.
_ERROR_REPORT_GRACE_SECONDS = 5.0
# The hub's `read_executor` while a stream's `read` runs, for a wrapper such as `distinct` to run a plain `read` in.
current_read_executor: contextvars.ContextVar[concurrent.futures.Executor | None] = contextvars.ContextVar(
    "pg_sse_read_executor", default=None
)

# Executors whose workers are not threads of this interpreter (InterpreterPoolExecutor is new in Python 3.14).
_NOT_THREADS = tuple(
    getattr(concurrent.futures, name)
    for name in ("ProcessPoolExecutor", "InterpreterPoolExecutor")
    if hasattr(concurrent.futures, name)
)

STREAM_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


def event_stream_responses(status_code: int = 200, description: str | None = None) -> dict[int | str, dict[str, Any]]:
    """
    A new dict for FastAPI's `responses=`, so the OpenAPI document shows what `hub.sse` sends: `text/event-stream` and
    its headers, under `status_code` for a route that sets another one. Use it with `response_class=StreamingResponse`:
    without that FastAPI lists `application/json` for the route as well. A new dict each time, to add responses to.
    Without a `description`, a route's `response_description` applies to its own status code. The headers are the
    standard ones; `hub.sse(headers=...)` can add to or replace them, and the 503 for a refused stream is not listed.
    """
    response: dict[str, Any] = {
        "content": {"text/event-stream": {}},
        "headers": {name: {"schema": {"type": "string"}} for name in STREAM_HEADERS},
    }
    if description is not None:
        response["description"] = description
    return {status_code: response}


# Each end marker's rank, goodbye reason, and how long the goodbye tells the client to wait before reconnecting (ms;
# None: jittered, `_SHUTDOWN_RETRY_MS`). A stronger end replaces a weaker one not said yet: a stream dropped for falling
# behind may still be in its read, and closed it does not send what that read found; shutdown replaces a revoke or the
# age limit, for its spread-out retry. A stream not read in time is already forgotten: nothing replaces that. Every end
# but a drop is a close: what a running read found is not sent. The reasons are `EndedTotals`'s fields.
_ENDS: dict[str | None, tuple[int, str, int | None]] = {
    None: (0, "", None),
    DROPPED: (1, "behind", 1000),
    RECHECK: (2, "recheck", None),  # below a revoke, the age limit and shutdown, which say more
    AGED: (3, "max_age", 1000),
    REVOKED: (3, "revoked", 1000),
    CLOSING: (4, "shutdown", None),
    UNREAD: (4, "", None),  # no goodbye: it never streamed
}
_RETRY_MS = {reason: retry_ms for _, reason, retry_ms in _ENDS.values() if reason}
_SHUTDOWN_RETRY_MS = (1000, 5000)  # jittered, so a deploy's reconnects are spread out

# The types of `read` and `start`, public so a helper that wraps `hub.sse` can annotate them.
Read = Callable[..., Iterable[Event] | Awaitable[Iterable[Event]]]  # read(cursor) or read(cursor, wake)
Start = Callable[[], Any]  # start() gives the current cursor; plain or async
# `await revalidate()` says whether a stream may go on. Checked when the stream is made: an `async def`, a partial of
# one, a bound async method or an object with an async `__call__`, not a lambda that returns a coroutine.
Revalidate = Callable[[], Awaitable[Any]]


class TooManySubscribers(Exception):
    """A cap is reached or the hub is shutting down. `Hub.sse` turns it into a 503 with Retry-After."""


@dataclass(frozen=True, slots=True)
class EndedTotals:
    """`HubStats.ended_total`: goodbyes sent since the process started, one count per reason."""

    max_age: int = 0
    shutdown: int = 0
    revoked: int = 0
    behind: int = 0
    recheck: int = 0


@dataclass(frozen=True, slots=True)
class HubStats:
    """
    A snapshot for a health check or a metrics endpoint.

    - `open_streams`: subscriptions holding a slot, including ended ones still saying goodbye (10 seconds at most).
    - `refused_total`, `dropped_total`: streams refused at a cap (or during shutdown) and streams ended for falling
      behind, since the process started.
    - `listener_connected`: the listener holds its LISTEN right now.
    - `listener_reconnects_total`: how often the listener got its connection back after losing it.
    - `last_notification_at`: Unix time of the last notification the listener received, or None if none yet.
    - `read_errors_total`: streams that failed: `read` or `start` raised, or `read` returned something the hub could
      not send (not an `Event`, data `json.dumps` refuses, a tuple id).
    - `revalidate_errors_total`: runs of a stream's `revalidate` that raised or timed out, and checks that stopped
      for good. A stream they ended is counted in `ended_total.recheck`, not in `read_errors_total`.
    - `ended_total`: goodbyes sent, by reason. A stream whose client left, or that ended with a `final` event or a
      failed `read` or `start`, sent none.
    """

    open_streams: int
    refused_total: int
    dropped_total: int
    listener_connected: bool
    listener_reconnects_total: int
    last_notification_at: float | None
    read_errors_total: int = 0
    ended_total: EndedTotals = EndedTotals()
    revalidate_errors_total: int = 0


@dataclass(frozen=True, slots=True)
class StreamInfo:
    """
    One subscription still holding its slot, from `hub.streams()` (and as the third argument of an `on_error` that takes
    one): what it watches, its owner, how long it has been open, and whether it ended (closed or dropped) and is only
    saying goodbye.
    """

    key: str | None
    owner: str | None
    age_seconds: float
    ended: bool = False


_Reported = Awaitable[None] | None
# on_error(key, exc), or on_error(key, exc, info) with the stream's StreamInfo
ErrorHandler = Callable[[str | None, Exception], _Reported] | Callable[[str | None, Exception, StreamInfo], _Reported]


@dataclass(frozen=True, slots=True)
class Wake:
    """
    Why `read` is running, for a `read(cursor, wake)`. Several causes can arrive together: a burst is one read.

    - `start`: the stream's first read.
    - `keys`: keys with a database notification since the last read.
    - `signals`: labels from `hub.wake(key, signal=...)` since the last read.
    - `signal_keys`: for each of those labels, the keys it was sent for. A per-key stream sees its own key; an
      all-keys stream sees which keys signalled. Not part of equality, so `Wake(signals=...)` still equals what the
      hub passes.
    - `resync`: the listener connected or reconnected, so anything may have changed.
    - `reread`: nothing woke the stream for `reread_seconds`.
    """

    start: bool = False
    keys: frozenset[str] = frozenset()
    signals: frozenset[str] = frozenset()
    resync: bool = False
    reread: bool = False
    signal_keys: Mapping[str, frozenset[str]] = field(default_factory=dict, compare=False)

    @property
    def data_may_have_changed(self) -> bool:
        """True unless the only causes are `hub.wake` signals."""
        return self.start or self.resync or self.reread or bool(self.keys)


@dataclass(frozen=True, slots=True)
class _StreamSpec:
    """What `stream()` was given and worked out once, for the stream it made."""

    read: Read
    wants_wake: bool  # `read(cursor, wake)`, not `read(cursor)`
    read_is_async: bool
    cursor: Any
    first_frame: str | None  # the cursor as the first frame, if there is one yet
    start: Start | None
    end_event: str | None
    age_limit: float
    revalidate: Revalidate | None


class Subscription:
    """One stream's queue. `key` is what it watches, or None for every key. Touched only on the event loop."""

    def __init__(
        self,
        key: str | None,
        owner: str | None,
        queue_size: int,
        *,
        serial: int = 0,
        hub_stream: bool = False,
    ) -> None:
        self.key = key
        self.owner = owner
        # What is queued, in order, each once: a repeat adds nothing to the next read. At most `queue_size` items.
        self._items: dict[QueueItem, None] = {}
        self._size = queue_size
        self._woken = asyncio.Event()  # something for `wait`: an item queued, a resync due, or the end
        # The stream's task while it is in the app's `start` or `read`, which a close (not a drop) cancels; and whether
        # it did, so the stream can tell that cancellation from any other.
        self.app_task: asyncio.Task[Any] | None = None
        self.cancelled_by_close = False
        self.end_marker: str | None = None  # why it ended, once `ended`
        self.opened_at = time.monotonic()
        # Increases with every stream the hub opens, so `hub.close` can tell one opened after it was called.
        self.serial = serial
        # One of the hub's own streams (`stream`, `sse`), not a `hub.subscribe` queue: it spreads its resync out, and
        # stays known after it is dropped, until its stream finishes. A `hub.subscribe` loop paces its own reads.
        self.hub_stream = hub_stream
        # A hub stream takes its slot before its first read, which covers whatever arrives until then, so nothing is
        # queued before it. A `hub.subscribe` loop reads from the start.
        self.reading = not hub_stream
        self.resync_timer: asyncio.TimerHandle | None = None  # a resync held back by `resync_spread`
        # Gives a hub stream's slot back if it does not get there itself: never read, or ended and not saying goodbye.
        self.release_timer: asyncio.TimerHandle | None = None
        # A resync is a flag, not a queued item: it takes no slot, so it never ends a stream that is only busy.
        self.resync_pending = False
        self.watch: asyncio.Task[None] | None = None  # runs its `revalidate` about every `revalidate_seconds`

    def offer(self, item: QueueItem) -> bool:
        """Queues a wake-up. False when the queue was full, so this call ended the stream instead."""
        # Ended: the marker is all that is left. Not reading yet: the first read covers it. A repeat adds nothing.
        if self.ended or not self.reading or item in self._items:
            return True
        if len(self._items) >= self._size:
            self._end(DROPPED)
            return False
        self._items[item] = None
        self._woken.set()
        return True

    def resync(self) -> None:
        """Makes the next read a resync (anything may have changed), at once if the stream is waiting."""
        if not self.ended and self.reading:
            self.resync_pending = True
            self._woken.set()

    @property
    def ended(self) -> bool:
        return self.end_marker is not None

    def _end(self, marker: str) -> None:
        """Replaces whatever is queued with a marker the stream reads as its last item."""
        self.end_marker = marker
        self.resync_timer = _cancel(self.resync_timer)  # its stream says goodbye on its next read
        self._items = {("marker", marker): None}
        self.resync_pending = False
        self._woken.set()
        # A drop lets the running read finish: its batch is still sent.
        if marker != DROPPED and self.app_task is not None and not self.cancelled_by_close:
            self.cancelled_by_close = True
            self.app_task.cancel()

    def close(self) -> None:
        """Ends it as a shutdown would. To give its slot back as well, `hub.unsubscribe` it."""
        self._close(CLOSING)

    def _close(self, marker: str) -> None:
        """Ends it with `marker`, or replaces a weaker end not yet said (see `_ENDS`)."""
        if _ENDS[marker][0] > _ENDS[self.end_marker][0]:
            self._end(marker)

    async def wait(self, timeout: float) -> list[QueueItem] | None:  # noqa: ASYNC109 -- the ping interval, handed to wait_for
        """
        Everything queued once there is anything (a burst costs one re-read), with the resync marker last if one is
        due, or None if the timeout came first.
        """
        try:
            await asyncio.wait_for(self._woken.wait(), timeout)
        except TimeoutError:
            return None
        return self.take()

    def take(self) -> list[QueueItem]:
        """Everything queued right now, as `wait` returns it, without waiting."""
        self._woken.clear()
        items, self._items = list(self._items), {}
        if self.resync_pending:
            self.resync_pending = False
            items.append(("marker", RESYNC))
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
        resync_spread: float = 2.0,
        application_name: str = "pg-sse-listener",
        read_executor: concurrent.futures.Executor | None = None,
        on_error: ErrorHandler | None = None,
        log_owner: bool = False,
        revalidate_seconds: float = 15.0,
        revalidate_failures: int | None = 3,
        revalidate_timeout: float = 10.0,
    ) -> None:
        if not channel or "\x00" in channel or len(channel.encode()) > 63:
            raise ValueError("channel must be a non-empty identifier of at most 63 bytes")
        if max_total_per_owner is not None and max_total_per_owner < 1:
            raise ValueError("max_total_per_owner must be at least 1, or None for no total")
        max_stream_seconds = _seconds("max_stream_seconds", max_stream_seconds, infinite=True)
        if queue_size < 1:
            raise ValueError(
                "queue_size must be at least 1 (with no room, a stream would be dropped at its first wake-up)"
            )
        try:
            low, high = ping_seconds
        except (TypeError, ValueError):
            raise ValueError("ping_seconds must be (low, high)") from None
        low, high = _seconds("ping_seconds", low), _seconds("ping_seconds", high)
        if low > high:
            raise ValueError("ping_seconds must be (low, high) with low <= high")
        reread_seconds = _seconds("reread_seconds", reread_seconds, infinite=True)
        resync_spread = _seconds("resync_spread", resync_spread, zero=True)
        revalidate_seconds = _seconds("revalidate_seconds", revalidate_seconds)
        revalidate_timeout = _seconds("revalidate_timeout", revalidate_timeout)
        if revalidate_failures is not None and (
            not isinstance(revalidate_failures, int) or isinstance(revalidate_failures, bool) or revalidate_failures < 1
        ):
            raise ValueError("revalidate_failures must be an int, at least 1, or None for no limit")
        if listen_timeout is not None:
            listen_timeout = _seconds("listen_timeout", listen_timeout, zero=True)
        check_event_name("end_event", end_event)  # here, not at the first goodbye, where nothing can report it
        if not application_name or "\x00" in application_name:
            raise ValueError("application_name must be non-empty text")
        if not isinstance(log_owner, bool):
            raise TypeError(f"log_owner must be True or False; got {log_owner!r}")
        if isinstance(read_executor, _NOT_THREADS):
            # A read runs with the caller's context variables, which, like a lambda, cannot leave this interpreter.
            raise ValueError("read_executor must run threads (a ThreadPoolExecutor), not processes or interpreters")
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
        # keeps one. The browser reconnects at once with Last-Event-ID. `math.inf` turns the limit off.
        self.max_stream_seconds = max_stream_seconds
        # A stream sends a `: ping` comment this often while idle, so a proxy does not time the connection out. It
        # costs no database read. Jittered so streams opened together do not all ping together. Keep a proxy's read
        # timeout above the high end.
        self.ping_seconds = (low, high)
        # A stream re-reads when nothing woke it for this long: the net under every lost notification, so the worst
        # case for one is this delay. Streams read on their own clocks, so this is a steady trickle, not a spike.
        self.reread_seconds = reread_seconds
        # Passed to `json.dumps(default=...)` for Event data that is not JSON-native, e.g. `str` for datetimes.
        self.json_default = json_default
        # Before a planned close (age limit, shutdown, revoked, recheck, fell behind) a stream always sends `retry:`,
        # which EventSource applies by itself. With a name here it also sends that event with {"reason": ...}, for
        # clients that are not EventSource and want to know why.
        self.end_event = end_event
        self.handle_signals = handle_signals
        # How long `lifespan` waits for the first LISTEN before it lets the app start, so a health check and the first
        # requests see a live listener. Startup proceeds either way: the listener keeps retrying and `stats` reports it.
        self.listen_timeout = listen_timeout
        # After the listener connects or reconnects, every stream re-reads. Each waits a random 0 to this many seconds
        # first, so a database that just came back is not hit by every open stream at once. A notification for the
        # stream's key during the wait starts the resync early (not for an all-keys stream, which every notification
        # reaches). Any other read that re-reads everything (any read of a `read(cursor)`, a quiet re-read) is the
        # resync; a `read(cursor, wake)` woken by a signal alone still gets its resync later. 0 reads at once.
        self.resync_spread = resync_spread
        # What `pg_stat_activity` shows for the listener's connection.
        self.application_name = application_name
        # The thread pool a plain (not async) `read` or `start` runs in: asyncio's default pool (at most 32 threads,
        # shared with everything else that uses it) when None. The hub never shuts a given executor down.
        self.read_executor = read_executor
        # Called as `on_error(key, exc)` when a stream fails (as `HubStats.read_errors_total` counts) and when a failing
        # `revalidate` ends one, after it is logged: a plain function in asyncio's default pool, an async one on the
        # loop, either as its own task, so a slow error tracker holds up nothing else. One that takes a third argument
        # gets the stream's `StreamInfo`. (The log line for a stream `revalidate` ended is said once a minute; the call
        # is made for each.)
        if on_error is not None:
            _check_on_error(on_error)
        self.on_error = on_error
        # Whether the log line for a failed stream names its owner. Off unless asked for, since an owner may be a
        # session token or an address that must stay out of the logs; `on_error` gets it either way.
        self.log_owner = log_owner
        # About how long a stream waits between two runs of its `revalidate` (jittered, like the pings), how long one
        # run may take, and how many runs in a row must fail to end the stream with `recheck`, so a check that raises
        # when it should say no cannot leave a revoked session its stream. None: none does, which suits clients that
        # do not come back through a route that needs the same store as the check.
        self.revalidate_seconds = revalidate_seconds
        self.revalidate_timeout = revalidate_timeout
        self.revalidate_failures = revalidate_failures
        self._revalidate_errors = 0
        # The log lines for a failed `revalidate`, one that does not end its stream and one that does. When the session
        # store goes down every stream has one of each, so each is said once a minute for the hub.
        self._revalidate_blip = Throttle()
        self._revalidate_ended = Throttle()
        self._loop: asyncio.AbstractEventLoop | None = None
        # Every subscription not unsubscribed yet, and the same ones indexed: by key and the ones watching every key,
        # for fan-out (a notification costs the streams it reaches, not every open stream), and by owner, for the
        # per-owner caps and `hub.close(owner=...)`. One that ended (dropped, closed) stays until its stream finishes,
        # and keeps counting against the caps until then: its connection is still open, and a client that stops
        # reading must not open one stream after another. `hub.close` and shutdown still reach it while its read runs.
        # Change them only through `_track` and `_forget`.
        self._subscriptions: set[Subscription] = set()
        self._by_key: dict[str, set[Subscription]] = {}
        self._watching_all: set[Subscription] = set()
        self._by_owner: dict[str, set[Subscription]] = {}
        self._by_owner_key: dict[tuple[str, str | None], set[Subscription]] = {}  # for `max_per_owner`
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        # Set while the listener holds a LISTEN: report it from a health check.
        self.listening = threading.Event()
        self.closing = False
        self._signal_hooks: dict[signal.Signals, _SignalHook] = {}
        self._refused = 0
        self._dropped = 0
        self._reconnects = 0
        self._read_errors = 0
        self._ended = asdict(EndedTotals())  # goodbyes sent, by reason
        self._error_reports: set[asyncio.Task[Any]] = set()  # async `on_error` calls still running
        self._serials = itertools.count()
        self._serials_lock = threading.Lock()  # `close` takes one from any thread
        self._last_notification_at: float | None = None

    def stats(self) -> HubStats:
        return HubStats(
            open_streams=len(self._subscriptions),
            refused_total=self._refused,
            dropped_total=self._dropped,
            listener_connected=self.listening.is_set(),
            listener_reconnects_total=self._reconnects,
            last_notification_at=self._last_notification_at,
            read_errors_total=self._read_errors,
            ended_total=EndedTotals(**self._ended),
            revalidate_errors_total=self._revalidate_errors,
        )

    def streams(self) -> list[StreamInfo]:
        """
        Every subscription still holding a slot, oldest first, for an admin view or a debug endpoint. `ended` marks one
        that was closed or dropped and is still saying goodbye.

        It copies a set the event loop changes (`sorted` copies it in one step). That is safe from another thread under
        CPython's GIL, but not guaranteed on a free-threaded build: there, call it on the event loop.
        """
        now = time.monotonic()
        found = sorted(self._subscriptions, key=lambda s: s.opened_at)
        return [self._info(s, now) for s in found]

    @staticmethod
    def _info(subscription: Subscription, now: float) -> StreamInfo:
        return StreamInfo(subscription.key, subscription.owner, now - subscription.opened_at, subscription.ended)

    def stream(
        self,
        key: str | None,
        read: Read,
        *,
        cursor: Any = None,
        start: Start | None = None,
        owner: str | int | None = None,
        end_event: str | None = None,
        max_stream_seconds: float | None = None,
        revalidate: Revalidate | None = None,
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
        - `owner` (a user id, a session) applies the per-owner caps. A `str`, or an int, which is turned into text.
          Streams without an owner have no such cap.
        - `end_event`, when given, overrides the hub's: the event name sent with the reason before a planned close.
        - `max_stream_seconds`, when given, overrides the hub's age limit for this stream: a visitor's stream may
          expire sooner than a dashboard session's.
        - `await revalidate()` says whether the stream may go on: True to keep it, anything falsy (None too) to end
          it with the goodbye `revoked`, so a removed member or a signed-out session loses its stream without a
          `hub.close` at every place that revokes. Return False for a session that is gone; raise only when the check
          could not be made (a helper that raises to deny is a failure, not a no). It runs about every
          `revalidate_seconds`, on its own clock, the first time about that long after the stream is first read (the
          route has just checked), in a task of its own with the context variables of the task that reads it, for
          every open stream: make it a cached lookup. It runs on the event loop, so it must be async: an `async def`,
          a `functools.partial` of one (not a lambda, which cannot be told from one that blocks), a bound async method
          or an object with an async `__call__`; anything else is a `TypeError`. A failure (it raises or runs over
          `revalidate_timeout`) warns and is counted in `HubStats.revalidate_errors_total`; `revalidate_failures` in
          a row end the stream with the goodbye `recheck`, and the client reconnects after a jittered delay, through
          the route's own check.

        Call it from the event loop, after any 404 or permission check: the stream takes its slot here, so
        `TooManySubscribers` is raised before any response exists. It gives the slot back when it ends, however it ends,
        or, if it is never read (its response was never sent), `_UNREAD_SECONDS` after it was made.

        `key` must be a `str` (or None): raises `TypeError` for anything else, since a notification payload is text
        and an int would subscribe fine and never be woken.
        """
        return self._stream(
            key,
            read,
            cursor=cursor,
            start=start,
            owner=owner,
            end_event=end_event,
            max_stream_seconds=max_stream_seconds,
            revalidate=revalidate,
        )[1]

    def _stream(
        self,
        key: str | None,
        read: Read,
        *,
        cursor: Any,
        start: Start | None,
        owner: str | int | None,
        end_event: str | None,
        max_stream_seconds: float | None,
        revalidate: Revalidate | None,
    ) -> tuple[Subscription, AsyncIterator[str]]:
        """`stream`, with the subscription, for a caller that must give its slot back if what it builds fails."""
        if end_event is None:
            end_event = self.end_event
        else:
            check_event_name("end_event", end_event)
        if max_stream_seconds is None:
            age_limit = self.max_stream_seconds
        else:
            age_limit = _seconds("max_stream_seconds", max_stream_seconds, infinite=True)
        _check_read(read)
        if revalidate is not None:
            _check_revalidate(revalidate)
        # A cursor the browser could not hand back fails here, not on every reconnect: of the wrong type (a row, bytes)
        # it is the code's mistake, and raises. Text with a line break or NUL is the client's, as from a raw query
        # parameter: the stream starts from now, as `IntCursor.parse_one` does for a header that is not a cursor, and
        # never answers it with a 500, which `EventSource` would give up on for good.
        try:
            first_frame = None if cursor is None else encode_cursor(cursor)
        except ValueError:
            if not isinstance(cursor, str):  # an int too long for `str`, say: the code's mistake, not the client's
                raise
            if _bad_cursor.ready():
                logger.warning(
                    "pg_sse: a cursor with a line break or NUL cannot be an SSE id; the stream starts from now"
                )
            cursor = first_frame = None
        spec = _StreamSpec(
            read=read,
            wants_wake=_accepts_wake(read),  # worked out once, not on every read
            read_is_async=_is_async(read),
            cursor=cursor,
            first_frame=first_frame,
            start=start,
            end_event=end_event,
            age_limit=age_limit,
            revalidate=revalidate,
        )
        subscription = self._subscribe(key, owner, hub_stream=True)
        frames = self._events(subscription, spec)
        # Nothing may raise between taking the slot and this: a stream that is never read never reaches its `finally`.
        weakref.finalize(frames, self._forget_unread, subscription).atexit = False
        loop = asyncio.get_running_loop()
        subscription.release_timer = loop.call_later(_UNREAD_SECONDS, self._expire_unread, subscription)
        return subscription, frames

    def sse(
        self,
        key: str | None,
        read: Read,
        *,
        cursor: Any = None,
        start: Start | None = None,
        owner: str | int | None = None,
        end_event: str | None = None,
        max_stream_seconds: float | None = None,
        revalidate: Revalidate | None = None,
        headers: Mapping[str, str] | None = None,
        refuse_with: Callable[[TooManySubscribers], Exception] | None = None,
    ) -> "StreamingResponse":
        """
        `stream` as a Starlette/FastAPI `text/event-stream` response. Arguments as for `stream`; past a cap it raises
        a 503 with `Retry-After: 30`. Needs `pip install "pg-sse[starlette]"`. Call it from an `async def` route.

        `refuse_with` replaces that 503: it gets the `TooManySubscribers` and returns the exception to raise instead,
        e.g. one your error tracker is set to ignore, since a refused stream is back-pressure, not a fault.

        FastAPI's OpenAPI document does not know the response: pass `responses=pg_sse.event_stream_responses()` and
        `response_class=StreamingResponse` to the route.
        """
        try:
            from starlette.exceptions import HTTPException
            from starlette.responses import StreamingResponse
        except ImportError:
            raise ImportError('hub.sse needs Starlette: pip install "pg-sse[starlette]" (or use hub.stream)') from None
        try:
            subscription, frames = self._stream(
                key,
                read,
                cursor=cursor,
                start=start,
                owner=owner,
                end_event=end_event,
                max_stream_seconds=max_stream_seconds,
                revalidate=revalidate,
            )
        except TooManySubscribers as err:
            logger.warning("pg_sse refused a stream (%s)", err)
            if refuse_with is not None:
                raise refuse_with(err) from None
            raise HTTPException(
                status_code=503,
                detail="Live updates are unavailable.",
                headers={"Retry-After": str(_RETRY_AFTER_SECONDS)},
            ) from None
        try:
            return StreamingResponse(
                frames, media_type="text/event-stream", headers={**STREAM_HEADERS, **(headers or {})}
            )
        except BaseException:
            self._forget(subscription)  # no response, so nothing will read the stream: its slot is free now
            raise

    async def _events(self, subscription: Subscription, spec: _StreamSpec) -> AsyncIterator[str]:
        try:
            reason = None  # of the goodbye to say, if it ends with one
            try:
                # Closed (revoked, shutdown) before its first frame was read, while it worked on a frame (`start`,
                # `read`), or while the client took the last one: that frame is not sent, and nothing more runs. A
                # stream dropped for falling behind is not stopped here: it sends its last batch, so its cursor moves
                # on, then says `behind`.
                _stop_if_closed(subscription)
                # Closed with this stream, so a client that left does not leave it suspended until garbage collection.
                async with contextlib.aclosing(self._frames(subscription, spec)) as frames:
                    async for frame, last in frames:
                        _stop_if_closed(subscription)
                        yield frame
                        if last:  # an `Event(final=True)`: the stream is over, and no goodbye follows it
                            return
                        _stop_if_closed(subscription)
            except _Goodbye as goodbye:
                reason = goodbye.reason
            except Exception as err:
                self._read_errors += 1
                self._log_failure(subscription, err)
                self._report(subscription, err)
            if reason:  # empty for a stream never read: it never streamed, so it just ends
                # The slot is held until the client took this too: one that stopped reading keeps counting.
                yield _goodbye(spec.end_event, reason)
                self._ended[reason] += 1  # once the client took it: the next `anext` resumes the stream here
        finally:
            self.unsubscribe(subscription)

    async def _frames(self, subscription: Subscription, spec: _StreamSpec) -> AsyncGenerator[tuple[str, bool], None]:
        """
        The stream's frames, each with whether it is the last (a final event), up to a planned close, which raises
        `_Goodbye`. `_events` stops it once it is closed.
        """
        subscription.release_timer = _cancel(subscription.release_timer)  # being read, however long `start` takes
        read, wants_wake, cursor, first_frame = spec.read, spec.wants_wake, spec.cursor, spec.first_frame
        deadline = time.monotonic() + spec.age_limit
        if spec.revalidate is not None:
            # From here, so one that hangs in `start` or in its first frame is checked too. The route has just checked:
            # the first check is due one interval on. Its own task, with this one's context variables.
            subscription.watch = asyncio.ensure_future(self._watch(subscription, spec.revalidate))
            subscription.watch.add_done_callback(functools.partial(self._watch_done, subscription))
        if cursor is None and spec.start is not None:
            cursor = await self._app_call(subscription, spec.start)
            if cursor is not None:
                first_frame = encode_cursor(cursor, "the cursor start() returned")  # says where a bad one came from
            # Closed while `start` ran. `_events` checks between frames, but with no cursor no frame comes before the
            # first read.
            _stop_if_closed(subscription)
        if first_frame is not None:
            # The browser reconnects from here even if no event ever arrives on this stream.
            yield first_frame, False
        # From here on a wake-up is queued, since the first read may miss it. Nothing before it was, `start` included:
        # that read covers it, so it costs no second read.
        subscription.reading = True
        wake = Wake(start=True)
        while True:
            if not wants_wake or wake.start or wake.resync or wake.reread:
                # This read already re-reads everything a held-back resync would.
                subscription.resync_timer = _cancel(subscription.resync_timer)
            args = (cursor, wake) if wants_wake else (cursor,)
            # Set and reset within this one await, so it reaches `read` and nothing else in the task that drives this.
            token = current_read_executor.set(self.read_executor)
            try:
                events = await self._app_call(subscription, read, *args, is_async=spec.read_is_async)
            finally:
                current_read_executor.reset(token)
            for event in events:
                if not isinstance(event, Event):
                    raise TypeError(f"read must return pg_sse.Event items, got {type(event).__name__}")
                yield encode(event, self.json_default), event.final
                if event.id is not None:
                    cursor = event.id
            reread_at = time.monotonic() + self.reread_seconds
            while True:  # idle: ping until something wakes the stream or a re-read is due
                if subscription.ended:  # closed, or dropped for falling behind: its last batch is sent
                    raise _Goodbye(_end_reason(subscription))
                now = time.monotonic()
                if now >= deadline:
                    # Ended like any other close: no more wake-ups queued for it, and its slot free after the grace if
                    # its client never takes the goodbye.
                    self._end_stream(subscription, AGED)
                    raise _Goodbye("max_age")
                if now >= reread_at:
                    # Whatever came in while the last ping was taken rides along, or it would cost a second read.
                    wake = _wake_from(subscription.take(), reread=True)
                    break
                timeout = min(random.uniform(*self.ping_seconds), deadline - now, reread_at - now)  # noqa: S311 -- jitter
                woke = await subscription.wait(timeout)
                if woke is None:
                    # Every timeout pings, even the one that leads to a re-read: the re-read may send nothing, and
                    # the silence must never outlast a ping interval.
                    if time.monotonic() < deadline:
                        yield ": ping\n\n", False
                elif subscription.ended:  # the marker is the only item left: dropped, or closed
                    raise _Goodbye(_end_reason(subscription))
                else:
                    wake = _wake_from(woke)
                    break

    async def _watch(self, subscription: Subscription, revalidate: Revalidate) -> None:
        """
        Runs the app's `revalidate` about every `revalidate_seconds` for as long as the stream lasts, on its own clock,
        so a quiet stream, a stream in a read and a slow check do not hold each other up. A no ends the stream as
        `hub.close` does. A check that raises or takes too long is a failure; `revalidate_failures` in a row end the
        stream with `recheck`: its client comes back through the route after a jittered delay, instead of every stream
        failing at the same moment. A stream that ended before its check ran is left alone; a no replaces a weaker end.
        """
        failures = 0
        while True:
            # Jittered, so streams opened together (a deploy's reconnects) do not check, and fail, in step.
            await asyncio.sleep(self.revalidate_seconds * random.uniform(0.75, 1.25))  # noqa: S311
            if subscription.ended or subscription not in self._subscriptions:  # ended, or its client left
                return
            error: Exception | None = None
            limit = asyncio.timeout(self.revalidate_timeout)
            try:
                async with limit:
                    allowed = bool(await revalidate())
                if limit.expired():  # a check that swallowed the cancel of the timeout, and answered
                    raise TimeoutError
            except BaseException as err:
                if isinstance(err, KeyboardInterrupt | SystemExit | GeneratorExit) or (
                    isinstance(err, asyncio.CancelledError) and _cancelling()
                ):
                    raise  # the process is stopping, or `_forget` ended the watch
                if limit.expired():  # our limit, not a timeout of the app's own: that one keeps its message
                    error = TimeoutError(f"revalidate took more than {self.revalidate_timeout:g} s")
                elif isinstance(err, Exception):
                    error = err
                else:  # a cancel the check raised itself, or an exception type of the app's own that is no Exception
                    error = RuntimeError(f"revalidate raised {err!r}")
            if _cancelling():  # `_forget` cancelled the watch, and the check ended without letting it show
                return
            if error is None:
                failures = 0
                if not allowed:
                    # As `hub.close` does: it replaces a weaker end, and leaves a stronger one.
                    self._end_stream(subscription, REVOKED)
                    return
                continue
            if subscription.ended:  # a check that failed because its stream was closed
                return
            self._revalidate_errors += 1
            failures += 1
            if self.revalidate_failures is not None and failures >= self.revalidate_failures:
                self._recheck(subscription, error, f"its revalidate failed {failures} times in a row")
                return
            if self._revalidate_blip.ready():
                logger.warning(
                    "pg_sse revalidate failed for key %r (%d in a row, trying again in about %g s): %r",
                    subscription.key,
                    failures,
                    self.revalidate_seconds,
                    error,
                    exc_info=error,
                )

    def _recheck(self, subscription: Subscription, error: Exception, why: str) -> None:
        """
        Ends the stream with `recheck`: its client comes back through the route. The callers count the failure in
        `revalidate_errors_total`; the log line is said once a minute for the hub, `on_error` hears of each.
        """
        self._end_stream(subscription, RECHECK)
        if self._revalidate_ended.ready():
            self._log_failure(subscription, error, f"ended: {why}")
        self._report(subscription, error)

    def _watch_done(self, subscription: Subscription, watch: "asyncio.Future[None]") -> None:
        """
        A watch that stopped while its stream goes on, because something in it broke, so the stream is no longer
        checked: with a failure limit it ends with `recheck`; without one (`None`: failures never end a stream) it
        only warns. Not one that ended its stream, or that `_forget` cancelled.
        """
        error = None if watch.cancelled() else watch.exception()  # read, so asyncio does not warn it was never
        if subscription.ended or subscription not in self._subscriptions:
            return
        if not isinstance(error, Exception):
            error = RuntimeError("the revalidate check stopped")
        self._revalidate_errors += 1
        if self.revalidate_failures is not None:
            self._recheck(subscription, error, "its revalidate broke")
        elif self._revalidate_blip.ready():
            logger.warning(
                "pg_sse revalidate stopped for key %r, so it is no longer checked: %r", subscription.key, error
            )

    async def _app_call(
        self, subscription: Subscription, fn: Callable[..., Any], *args: Any, is_async: bool | None = None
    ) -> Any:
        """
        The app's `start` or `read`, in the stream's own task (so what the app scopes to it, a session say, is the
        stream's), cut short if the stream is closed meanwhile: a close cancels the task, so an async call stops at
        once and the goodbye goes out rather than after a stuck query; a plain one runs on in its thread, its result
        dropped. A close wins a tie, so a read that fails because of it (during a deploy, the app's pool closing under
        it) says the goodbye rather than reaching the error tracker.
        """
        task = asyncio.current_task()
        subscription.app_task = task
        try:
            result = await _call(fn, *args, executor=self.read_executor, is_async=is_async)
        except asyncio.CancelledError:
            # The close's cancellation, and no other on top (the client leaving): it ends with the goodbye.
            if subscription.cancelled_by_close and task is not None and task.uncancel() == 0:
                raise _Goodbye(_end_reason(subscription)) from None
            raise
        except Exception:
            # A close's cancellation the call turned into another error (its cleanup failed) is given back too.
            if subscription.cancelled_by_close and task is not None:
                task.uncancel()
            _stop_if_closed(subscription)  # closed as it failed: the close's goodbye, not an error
            raise
        finally:
            subscription.app_task = None
        if subscription.cancelled_by_close and task is not None:
            task.uncancel()  # one the call swallowed
        _stop_if_closed(subscription)  # closed just as it returned
        return result

    def _forget_unread(self, subscription: Subscription) -> None:
        """A stream collected before it was ever read. The collector may run on any thread: the loop does the rest."""
        if not subscription.reading and subscription in self._subscriptions:
            self._post(self._forget, subscription)

    def _expire_unread(self, subscription: Subscription) -> None:
        """A stream not read `_UNREAD_SECONDS` after it was made is over: read after this, it ends with no goodbye."""
        subscription._close(UNREAD)
        self._forget(subscription)

    def _log_failure(self, subscription: Subscription, err: BaseException, what: str = "failed") -> None:
        detail = f"open {time.monotonic() - subscription.opened_at:.0f} s"
        if self.log_owner and subscription.owner is not None:
            detail = f"owner {subscription.owner!r}, {detail}"
        logger.error(
            "pg_sse stream for key %r (%s) %s; the client reconnects", subscription.key, detail, what, exc_info=err
        )

    def _report(self, subscription: Subscription, err: Exception) -> None:
        handler = self.on_error
        if handler is None:
            return
        args: tuple[Any, ...] = (subscription.key, err)
        if _accepts_info(handler):
            args += (self._info(subscription, time.monotonic()),)
        # As a task, and a plain function in asyncio's default pool: a slow error tracker holds up neither this stream's
        # end nor the loop. Not in `read_executor`, where in an outage it would queue behind the very reads failing.
        task = asyncio.ensure_future(_call(handler, *args))
        self._error_reports.add(task)  # a task nothing references can be garbage-collected while it runs
        task.add_done_callback(self._report_done)

    def _report_done(self, task: "asyncio.Future[Any]") -> None:
        self._error_reports.discard(task)
        if not task.cancelled() and (err := task.exception()) is not None:
            logger.error("pg_sse on_error callback failed", exc_info=err)

    def subscribe(self, key: str | None, owner: str | int | None = None) -> Subscription:
        """
        The low-level API under `stream`: a queue of notifications, wakes and markers. Unsubscribe in a `finally`.
        `key` and `owner` are checked as for `stream`.

        `await subscription.wait(timeout)` returns everything queued once there is anything, or None on timeout. An
        item is `("notify", key)` for a database notification, `("wake", key, signal)` for a `hub.wake`, or
        `("marker", name)`: re-read everything. Once `subscription.ended` is true the queue holds one marker and the
        stream is over: it fell behind, the hub is shutting down (`hub.closing`), or `hub.close` ended it. After a
        listener connect or reconnect the resync marker comes at once: `resync_spread` applies to `stream` and `sse`
        only, so a custom loop spreads its own reads if it needs to.
        """
        return self._subscribe(key, owner)

    def _subscribe(self, key: str | None, owner: str | int | None, *, hub_stream: bool = False) -> Subscription:
        _check_key(key, allow_none=True)
        owner = _owner_text(owner)
        self._require_loop()
        self._check_caps(key, owner)
        subscription = Subscription(
            key,
            owner,
            self.queue_size,
            serial=self._next_serial(),
            hub_stream=hub_stream,
        )
        self._track(subscription)
        return subscription

    def _next_serial(self) -> int:
        with self._serials_lock:
            return next(self._serials)

    def _track(self, subscription: Subscription) -> None:
        self._subscriptions.add(subscription)
        if subscription.key is None:
            self._watching_all.add(subscription)
        else:
            self._by_key.setdefault(subscription.key, set()).add(subscription)
        if subscription.owner is not None:
            self._by_owner.setdefault(subscription.owner, set()).add(subscription)
            self._by_owner_key.setdefault((subscription.owner, subscription.key), set()).add(subscription)

    def _forget(self, subscription: Subscription) -> None:
        if subscription.watch is not None:
            subscription.watch.cancel()
        subscription.resync_timer = _cancel(subscription.resync_timer)
        subscription.release_timer = _cancel(subscription.release_timer)
        self._subscriptions.discard(subscription)
        self._watching_all.discard(subscription)
        if subscription.key is not None:
            _discard(self._by_key, subscription.key, subscription)
        if subscription.owner is not None:
            _discard(self._by_owner, subscription.owner, subscription)
            _discard(self._by_owner_key, (subscription.owner, subscription.key), subscription)

    def _check_caps(self, key: str | None, owner: str | None) -> None:
        if self.closing:
            self._refuse("shutting down")
        if len(self._subscriptions) >= self.max_subscribers:
            self._refuse("too many open streams")
        # Without an owner there is nobody to cap: anonymous streams on one key are not one client.
        if owner is None:
            return
        mine = self._by_owner.get(owner, set())
        if self.max_total_per_owner is not None and len(mine) >= self.max_total_per_owner:
            self._refuse("too many open streams for this owner across all keys")
        if len(self._by_owner_key.get((owner, key), ())) >= self.max_per_owner:
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
        check_signal(signal)
        self._post(self._dispatch, ("wake", key, signal), key, all_keys)

    def call_later(self, seconds: float, callback: Callable[..., None], *args: Any) -> None:
        """
        Runs `callback(*args)` on the hub's event loop after `seconds`. Safe from any thread; does nothing while the
        hub has no running loop. For follow-up work on ephemeral state, e.g. checking that a typing signal lapsed, then
        calling `wake`.
        """

        def schedule() -> None:  # on whichever loop runs it: after a restart, the new one
            asyncio.get_running_loop().call_later(seconds, callback, *args)

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
        """
        Ends every stream and refuses new ones, so a shutdown does not wait out the graceful-shutdown timeout. Safe
        from any thread: from another one it is handed to the loop, and reaches only the run it was called in.
        """
        if self._loop is None:  # not running: nothing to end, and a stream asked for must still learn why
            return
        if not self._on_loop():
            self._post(self._close_all_of, self._stop)
            return
        self.closing = True
        for subscription in list(self._subscriptions):
            self._end_stream(subscription, CLOSING)

    def _close_all_of(self, run: threading.Event) -> None:
        """`close_all` handed to the loop: only while the run it was called in lasts, not in a later start."""
        if run is self._stop:
            self.close_all()

    def close(self, *, owner: str | int | None = None, key: str | None = None) -> None:
        """
        Ends this process's streams opened with `owner` and/or on `key`, so a revoked session or a removed member does
        not keep a stream until `max_stream_seconds`. Safe from any thread. A stream opened after the call (the user
        signed in again) is not ended with the old ones. Called on the event loop it takes effect before it returns;
        from another thread it is handed to the loop. The old streams count against the caps until they have said
        goodbye, which takes their client about a second to read; a stream opened in that moment can be refused. Like
        `wake` it does nothing, and says nothing, while the hub is not running: there are no streams to end then.

        With both, only streams matching both. A stream watching every key (`key=None`) matches `owner` only. The
        client gets a goodbye with reason `revoked` and reconnects, so revoke in the database first: its route must
        then refuse it, and `EventSource` stops on a 401 or 403. Only streams opened with `owner=` can be found by
        owner, and only this process's: with several workers, call it in each. An int owner is turned into text, as
        `stream` does. `close_all` ends every stream. A route that checked the session before the revoke but opens its
        stream after this call is not reached: `max_stream_seconds` bounds that window.
        """
        if owner is None and key is None:
            raise ValueError("close needs an owner or a key; close_all ends every stream")
        owner = _owner_text(owner)
        if key is not None:
            _check_key(key, allow_none=False)
        opened_by = self._next_serial()
        if self._on_loop():
            self._close_matching(owner, key, opened_by)
        else:
            self._post(self._close_matching, owner, key, opened_by)

    def _close_matching(self, owner: str | None, key: str | None, opened_by: int) -> None:
        """Ends the matching streams opened before serial `opened_by`: one opened since the call is a new session."""
        # Only an owner's or a key's indexed streams can match (an all-keys stream matches an owner only). `close`
        # needs one of the two, so without an owner there is a key.
        index, name = (self._by_owner, owner) if owner is not None else (self._by_key, cast(str, key))
        candidates = index.get(name, set())
        for subscription in list(candidates):
            # An owner's candidates all match it; a key's do too, but with both, the key must match as well.
            if subscription.serial < opened_by and (key is None or subscription.key == key):
                self._end_stream(subscription, REVOKED)

    def _on_loop(self) -> bool:
        return self._loop is not None and _running_loop() is self._loop

    def _require_loop(self) -> None:
        running = _running_loop()
        if running is None:
            raise RuntimeError("pg_sse: call hub.stream(), hub.sse() or hub.subscribe() from an `async def` route")
        if self._loop is None:
            if self.closing:  # it ran and stopped: refused like any stream during shutdown (a 503 from `hub.sse`)
                self._refuse("shutting down")
            raise RuntimeError(
                "pg_sse: the hub is not running; use FastAPI(lifespan=hub.lifespan) or `async with hub.lifespan()`"
            )
        if running is not self._loop:
            raise RuntimeError("pg_sse: the hub runs on another event loop")

    def _dispatch(self, item: QueueItem, key: str, all_keys: bool = True) -> None:
        """
        Offers `item` to the streams for `key` and, unless `all_keys` is False, the all-keys streams. A notification
        also ends a held-back resync's wait for the key's streams: both are read together, now. Not for an all-keys
        stream, which every notification reaches: on a busy channel the first one would end every such wait in the same
        tick. A `hub.wake` signal does not either.
        """
        targets = [*self._by_key.get(key, ()), *self._watching_all] if all_keys else list(self._by_key.get(key, ()))
        for subscription in targets:
            self._offer(subscription, item)
            if item[0] == "notify" and subscription.key is not None and subscription.resync_timer is not None:
                self._fire_resync(subscription)

    def _notify(self, key: str) -> None:
        """A database notification for `key`, from the listener."""
        self._dispatch(("notify", key), key)

    def _resync(self) -> None:
        """
        Every stream re-reads: the listener connected or reconnected. Each stream gets the marker after its own random
        delay of up to `resync_spread`, so the reads do not all hit the database at once. One already waiting keeps its
        time. A `hub.subscribe` queue gets it at once; `wait` hands it over as a marker.
        """
        loop = asyncio.get_running_loop()  # posted to the hub's loop
        for subscription in list(self._subscriptions):
            # Ended: it says goodbye on its next read. Not reading yet: its first read covers the reconnect.
            if subscription.ended or not subscription.reading:
                continue
            if not subscription.hub_stream or self.resync_spread <= 0:
                subscription.resync()
            elif subscription.resync_timer is None:
                delay = random.uniform(0, self.resync_spread)  # noqa: S311 -- jitter
                subscription.resync_timer = loop.call_later(delay, self._fire_resync, subscription)

    def _fire_resync(self, subscription: Subscription) -> None:
        subscription.resync_timer = _cancel(subscription.resync_timer)
        subscription.resync()

    def _offer(self, subscription: Subscription, item: QueueItem) -> None:
        if not subscription.offer(item):
            # Dropped for falling behind: its stream ends on its next read, and its slot is free once it has.
            self._dropped += 1
            self._release_soon(subscription)

    def _end_stream(self, subscription: Subscription, marker: str) -> None:
        """Ends a stream for `hub.close` or shutdown. It says goodbye, and its slot is free once it has (or soon)."""
        # Only its first end starts the grace: a repeat close, or a stronger end replacing a weaker one, must not
        # stretch it.
        first_end = not subscription.ended
        subscription._close(marker)
        if first_end:
            self._release_soon(subscription)

    def _release_soon(self, subscription: Subscription) -> None:
        """
        An ended stream's slot, given back once it has said goodbye or `_GOODBYE_GRACE_SECONDS` from now. Forgotten
        then, a dropped stream still in a long read is out of reach of a later shutdown: it says `behind`, not
        `shutdown`. A `hub.subscribe` queue is forgotten at once, however it ended: no read of the hub's is left to
        stop, and a loop that never unsubscribes must not leave it behind.
        """
        if not subscription.hub_stream:
            self._forget(subscription)
        else:
            subscription.release_timer = _cancel(subscription.release_timer)
            loop = asyncio.get_running_loop()
            subscription.release_timer = loop.call_later(_GOODBYE_GRACE_SECONDS, self._forget, subscription)

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
            # Everything that needs no waiting first, so a shutdown cancelled during the waits still leaves it clean.
            self.closing = True
            # As `close_all` ends them, a stream still read after this says goodbye with `shutdown`; but the hub stops
            # now, so it forgets them at once rather than holding their slots for the goodbye grace.
            for subscription in list(self._subscriptions):
                subscription._close(CLOSING)
                self._forget(subscription)
            self._remove_signal_hook()
            self._loop = None  # stopped: `close`, `wake` and `call_later` do nothing until the next start
            await asyncio.to_thread(self.stop)
            # A failure during shutdown is still reported, unless the tracker hangs. A report started during the wait
            # (a read that failed late) is waited for too.
            give_up_at = loop.time() + _ERROR_REPORT_GRACE_SECONDS
            while self._error_reports and (left := give_up_at - loop.time()) > 0:
                await asyncio.wait(set(self._error_reports), timeout=left, return_when=asyncio.FIRST_COMPLETED)
            hung = list(self._error_reports)
            for task in hung:
                task.cancel()
            await asyncio.gather(*hung, return_exceptions=True)  # so their cleanup runs before the loop closes

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
                with psycopg.connect(conninfo, autocommit=True, application_name=self.application_name) as conn:
                    if stop.is_set():  # `stop` gave up on this thread while it was connecting: it is not the listener
                        return
                    conn.execute(f"SET statement_timeout = '{_STATEMENT_TIMEOUT}'")
                    conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel)))
                    backoff = _BACKOFF_START_SECONDS
                    if connected_before:
                        self._reconnects += 1
                    connected_before = True
                    # Anything committed before this LISTEN existed (first connect or reconnect) was missed: every
                    # stream re-reads (a hub stream after its `resync_spread` delay). Posted before `listening` is set,
                    # so a stream that subscribes once it sees it set is not resynced again. With no stream open yet it
                    # is a no-op.
                    self._post(self._resync)
                    self.listening.set()
                    last_checked = time.monotonic()
                    while not stop.is_set():
                        if time.monotonic() - last_checked >= _KEEPALIVE_QUERY_SECONDS:
                            conn.execute("SELECT 1")
                            last_checked = time.monotonic()
                        for notification in conn.notifies(timeout=_LISTEN_POLL_SECONDS):
                            self._last_notification_at = time.time()
                            self._post(self._notify, notification.payload)
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


def _seconds(name: str, value: object, *, zero: bool = False, infinite: bool = False) -> float:
    """
    `value` as a float if it is a number above 0 (or 0, with `zero`) and finite (or `math.inf` for no limit, with
    `infinite`); `ValueError` otherwise. NaN compares false with everything, so it would pass for no limit unchecked,
    and a timer heap would get a NaN deadline. A bool is an int, but `True` is not a duration.
    """
    if isinstance(value, numbers.Real | decimal.Decimal) and not isinstance(value, bool):
        with contextlib.suppress(ValueError, OverflowError):  # a signaling NaN; an int too large for a float
            seconds = float(value)
            if (seconds >= 0 if zero else seconds > 0) and (infinite or seconds < math.inf):
                return seconds
    bound = "0 or more" if zero else "greater than 0"
    raise ValueError(f"{name} must be a number {bound}" + (" (math.inf for no limit)" if infinite else ", and finite"))


def check_signal(signal: object) -> None:
    """A signal is matched against text labels (`signal_read`'s, a read's `in wake.signals`): an Enum never matches."""
    if not isinstance(signal, str):
        raise TypeError(f"pg_sse: a signal must be a str, got {type(signal).__name__}; pass its text")


def _owner_text(owner: object) -> str | None:
    """
    Streams are matched by owner with `==`, so an int (a user id) is turned into text wherever an owner is passed:
    `close(owner=7)` then finds a stream opened with `owner=7` or `owner="7"`. Any other type is refused, since a
    stream opened with it could silently never be matched.
    """
    if owner is None or isinstance(owner, str):
        return owner
    if isinstance(owner, int) and not isinstance(owner, bool):
        return str(owner)
    raise TypeError(f"pg_sse: owner must be a str, an int or None, got {type(owner).__name__}; pass str(owner)")


def _discard(index: dict[Any, set[Subscription]], name: object, subscription: Subscription) -> None:
    if (found := index.get(name)) is not None:
        found.discard(subscription)
        if not found:
            del index[name]


class _Goodbye(Exception):
    """Raised inside a stream at a planned close: the stream sends a goodbye with `reason` and ends."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _cancelling() -> bool:
    """Whether this task has been asked to stop, as `_forget` does a stream's watch."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _stop_if_closed(subscription: Subscription) -> None:
    """Ended by `hub.close` or shutdown, so what a running read found is not sent. A stream dropped for falling behind
    is not: it sends what it read before its goodbye."""
    if subscription.end_marker not in (None, DROPPED):
        raise _Goodbye(_end_reason(subscription))


def _end_reason(subscription: Subscription) -> str:
    return _ENDS[subscription.end_marker][1]


def _running_loop() -> asyncio.AbstractEventLoop | None:
    """The running loop, or None. As asyncio's `_get_running_loop`, which is private."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def _cancel(timer: asyncio.TimerHandle | None) -> asyncio.TimerHandle | None:
    """Cancels a timer, if there is one. Always None, for `x.timer = _cancel(x.timer)`."""
    if timer is not None:
        timer.cancel()
    return None


def _goodbye(event_name: str | None, reason: str) -> str:
    retry_ms = _RETRY_MS[reason] or random.randint(*_SHUTDOWN_RETRY_MS)  # noqa: S311
    return encode_goodbye(event_name, reason, retry_ms)


def _wake_from(items: list[QueueItem], *, reread: bool = False) -> Wake:
    keys = frozenset(item[1] for item in items if item[0] == "notify")
    signal_keys: dict[str, set[str]] = {}
    for item in items:
        if item[0] == "wake":
            signal_keys.setdefault(item[2], set()).add(item[1])
    return Wake(
        keys=keys,
        signals=frozenset(signal_keys),
        resync=("marker", RESYNC) in items,
        reread=reread,
        signal_keys={label: frozenset(found) for label, found in signal_keys.items()},
    )


def _check_read(read: object) -> None:
    if not callable(read):
        raise TypeError(f"pg_sse: read must be a function of the cursor (and the wake); got {read!r}")


def _signature(fn: Callable[..., Any]) -> inspect.Signature | None:
    """`fn`'s signature, or None if it has none (some builtins)."""
    try:
        if sys.version_info >= (3, 14):
            # An annotation naming something imported only for type checking is a name, not an error: only the
            # parameters matter here.
            import annotationlib

            return inspect.signature(fn, annotation_format=annotationlib.Format.FORWARDREF)
        return inspect.signature(fn)
    except (TypeError, ValueError):  # no signature
        return None


def _can_be_called(fn: Callable[..., Any], *argument_lists: tuple[None, ...]) -> bool:
    """Whether `fn` can be called with one of the argument lists. Without a signature the first call tells."""
    signature = _signature(fn)
    if signature is None:
        return True
    for arguments in argument_lists:
        try:
            signature.bind(*arguments)
        except TypeError:
            continue
        return True
    return False


def _check_revalidate(revalidate: object) -> None:
    """
    A `revalidate` that is not async would block the loop, and one that needs arguments would fail at its first check,
    long after the route: refused where the stream is made.
    """
    if not (callable(revalidate) and _is_async(revalidate) and _can_be_called(revalidate, ())):
        raise TypeError(
            "pg_sse: revalidate must be an async function of no arguments (an `async def`, or a `functools.partial` "
            f"of one): it runs on the event loop; got {revalidate!r}"
        )


def _check_on_error(handler: object) -> None:
    """An `on_error` that takes neither `(key, exc)` nor `(key, exc, info)` would fail at its first report."""
    if not (callable(handler) and _can_be_called(handler, (None, None), (None, None, None))):
        raise TypeError(f"on_error must be a function of (key, exc) or (key, exc, info); got {handler!r}")


def _takes_argument(fn: Callable[..., Any], index: int, name: str) -> bool:
    """
    Whether `fn` takes a positional parameter number `index` (from 0) that is named `name` or has no default. A
    defaulted one of another name is the app's own (`limit=500`), and `*args` is not one. Handles lambdas, partials
    (their bound arguments are gone from the signature) and methods; without a signature it says no.
    """
    signature = _signature(fn)
    if signature is None:
        return False
    kinds = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    positional = [p for p in signature.parameters.values() if p.kind in kinds]
    if len(positional) <= index:
        return False
    parameter = positional[index]
    return parameter.name == name or parameter.default is parameter.empty


def _accepts_wake(fn: Callable[..., Any]) -> bool:
    """Whether `read` is read(cursor, wake). Without a signature it is assumed to be read(cursor)."""
    return _takes_argument(fn, 1, "wake")


def _accepts_info(fn: Callable[..., Any]) -> bool:
    """Whether `on_error` is on_error(key, exc, info)."""
    return _takes_argument(fn, 2, "info")


def _is_async(fn: Callable[..., Any]) -> bool:
    """Whether calling `fn` gives a coroutine: an `async def`, a partial or bound method of one, or an object whose
    class has an `async def __call__`. A class with one is not: calling the class makes an instance."""
    while isinstance(fn, functools.partial):  # iscoroutinefunction unwraps a partial, but not to an object's __call__
        fn = fn.func
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(type(fn).__call__)


async def _call(
    fn: Callable[..., Any],
    *args: Any,
    executor: concurrent.futures.Executor | None = None,
    is_async: bool | None = None,
) -> Any:
    """
    Awaits an async callable on the loop; runs a plain one in `executor` (the default pool when None), with the
    caller's context variables. A lambda returning a coroutine works too. `is_async` is `_is_async(fn)`, for a caller
    that worked it out once rather than on every call.
    """
    if _is_async(fn) if is_async is None else is_async:
        return await fn(*args)
    context = contextvars.copy_context()
    result = await asyncio.get_running_loop().run_in_executor(executor, context.run, fn, *args)
    if inspect.isawaitable(result):
        return await result
    return result
