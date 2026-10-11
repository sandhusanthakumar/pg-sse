import json
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

# The only line endings the SSE parser knows; str.splitlines would also split on U+2028 and friends.
_LINE_BREAK = re.compile(r"\r\n|\r|\n")
_FORBIDDEN_IN_FIELD = re.compile(r"[\r\n\x00]")


@dataclass(frozen=True, slots=True)
class Event:
    """
    One server-sent event.

    - `data`: a str is sent as is; anything else is sent as JSON.
    - `id`: the cursor after this event. It is sent as the SSE id (so a browser reconnects with it as Last-Event-ID)
      and handed back to your `read` as the cursor, unchanged.
    - `event`: the SSE event name; None means the browser's default, "message".
    - `final`: end the stream after this event, e.g. when the conversation it follows is closed.
    """

    data: Any
    id: Any = None
    event: str | None = None
    final: bool = False


def encode(event: Event, default: Callable[[Any], Any] | None = None) -> str:
    """`default` is `json.dumps`'s: what to do with data that is not JSON-native (e.g. `str` for a datetime)."""
    lines = []
    if event.id is not None:
        lines.append(f"id: {id_text(event.id)}")
    if event.event is not None:
        lines.append(f"event: {_field('event', event.event)}")
    data = event.data if isinstance(event.data, str) else json.dumps(event.data, separators=(",", ":"), default=default)
    lines.extend(f"data: {line}" for line in _LINE_BREAK.split(data))
    return "\n".join(lines) + "\n\n"


def encode_cursor(cursor: Any, what: str = "a cursor") -> str:
    """An id with no data: the browser records it as its Last-Event-ID and dispatches nothing. `what`: see `id_text`."""
    return f"id: {id_text(cursor, what)}\n\n"


def encode_goodbye(event_name: str | None, reason: str, retry_ms: int) -> str:
    """
    The last frame before a planned close. `retry:` sets the client's reconnect delay and dispatches nothing by
    itself; the named event, with the reason as data, is sent only when asked for. No `id:`, so the client's
    Last-Event-ID stays at the last real event.
    """
    lines = [f"retry: {retry_ms}"]
    if event_name is not None:
        lines += [
            f"event: {_field('event', event_name)}",
            f"data: {json.dumps({'reason': reason}, separators=(',', ':'))}",
        ]
    return "\n".join(lines) + "\n\n"


def id_text(value: Any, what: str = "a cursor or Event id") -> str:
    """
    The SSE id for a cursor. Raw bytes would be sent as `b'42'`, and a collection (a fetched row, what
    `IntCursor.parse` returns, a set) as `(12, 3)` or `{'id': 12}`: the browser hands that back as its Last-Event-ID,
    and `IntCursor` then refuses it. `what` names where the value came from, for the error.
    """
    if type(value) is int or type(value) is str:  # nearly every id: no further checks on the hot path
        return _field("id", str(value))
    if isinstance(value, bool):  # sent as "True", which no reconnect can parse
        raise TypeError(f"pg_sse: {what} cannot be a bool")
    if isinstance(value, bytes | bytearray | memoryview):
        raise TypeError(f"pg_sse: {what} cannot be bytes: pass IntCursor.parse_one(header), which reads them")
    if isinstance(value, Iterable) and not isinstance(value, str):
        raise TypeError(
            f"pg_sse: {what} cannot be a {type(value).__name__}: pass the value itself (row[0], not the row), "
            "IntCursor.parse_one's int, or IntCursor.format(*values)"
        )
    return _field("id", str(value))


# Log lines that many callers set off at once (every client, every stream) are said once a minute.
LOG_EVERY_SECONDS = 60.0


class Throttle:
    """For a log line many callers can set off at once: says yes at most once every `seconds`, to whoever asks first."""

    def __init__(self, seconds: float = LOG_EVERY_SECONDS) -> None:
        self.seconds = seconds
        self._last = -seconds

    def ready(self) -> bool:
        now = time.monotonic()
        if now - self._last < self.seconds:
            return False
        self._last = now
        return True


def check_event_name(cause: str, name: object) -> None:
    """An event name the encoder accepts, checked where it is given rather than in every stream; None is no event."""
    if name is not None and not isinstance(name, str):
        raise TypeError(f"{cause}: an event name must be a str or None; got {name!r}")
    if name == "":  # the browser would dispatch it as a plain `message`
        raise ValueError(f"{cause}: an event name must not be empty; pass None to send nothing")
    if name is not None:
        _field("event", name)  # a line break or NUL


def _field(name: str, value: str) -> str:
    if _FORBIDDEN_IN_FIELD.search(value):
        raise ValueError(f"an SSE {name} cannot contain a line break or NUL: {value!r}")
    return value


@dataclass(frozen=True, slots=True)
class Frame:
    """
    One decoded SSE frame, for tests: what a client would parse out of a string `hub.stream` yields.

    - `data`: the raw text, `data:` lines joined with a newline; None for a frame with no `data:` line.
    - `comment`: a `:` line without the colon (`": ping"` is `"ping"`).
    - `raw`: the frame as it was sent.
    """

    id: str | None
    event: str | None
    data: str | None
    retry: int | None
    comment: str | None
    raw: str

    def json(self) -> Any:
        """`data` parsed as JSON."""
        if self.data is None:
            raise ValueError("this frame has no data")
        return json.loads(self.data)

    @property
    def is_ping(self) -> bool:
        return self.comment == "ping"

    @property
    def is_cursor(self) -> bool:
        """An id and nothing else: sent first when a stream starts from a cursor, so the client records it."""
        return self.id is not None and self.data is None and self.event is None and self.retry is None

    @property
    def is_goodbye(self) -> bool:
        """The last frame before a planned close: a `retry:` and no id (so the client's Last-Event-ID stands)."""
        return self.retry is not None and self.id is None


def decode(frame: str) -> Frame:
    """Parses one frame the way an SSE client does: any line ending, one optional space after the colon."""
    id_: str | None = None
    event: str | None = None
    retry: int | None = None
    data: list[str] = []
    comments: list[str] = []
    for line in _LINE_BREAK.split(frame):
        if not line:
            continue
        if line.startswith(":"):
            comments.append(line[2:] if line.startswith(": ") else line[1:])
            continue
        name, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if name == "id":
            id_ = value
        elif name == "event":
            event = value
        elif name == "data":
            data.append(value)
        elif name == "retry" and value.isascii() and value.isdigit():
            retry = int(value)
    return Frame(
        id=id_,
        event=event,
        data="\n".join(data) if data else None,
        retry=retry,
        comment="\n".join(comments) if comments else None,
        raw=frame,
    )
