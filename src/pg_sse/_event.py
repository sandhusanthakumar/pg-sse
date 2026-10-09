import json
import re
from collections.abc import Callable
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
        lines.append(f"id: {_field('id', str(event.id))}")
    if event.event is not None:
        lines.append(f"event: {_field('event', event.event)}")
    data = event.data if isinstance(event.data, str) else json.dumps(event.data, separators=(",", ":"), default=default)
    lines.extend(f"data: {line}" for line in _LINE_BREAK.split(data))
    return "\n".join(lines) + "\n\n"


def encode_cursor(cursor: Any) -> str:
    """An id with no data: the browser records it as its Last-Event-ID and dispatches nothing."""
    return f"id: {_field('id', str(cursor))}\n\n"


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


def _field(name: str, value: str) -> str:
    if _FORBIDDEN_IN_FIELD.search(value):
        raise ValueError(f"an SSE {name} cannot contain a line break or NUL: {value!r}")
    return value
