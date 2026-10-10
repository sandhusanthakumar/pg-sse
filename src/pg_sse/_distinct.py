"""`distinct`: a `read` that stays quiet while what it would send has not changed."""

from collections.abc import Callable, Hashable
from typing import Any

from ._event import Event
from ._hub import Read, _accepts_wake, _call

_NOTHING_SENT = object()


def distinct(read: Read, key: Callable[[Event], Hashable | None]) -> Read:
    """
    Wraps `read` so a snapshot is sent only when it changed.

    `key(event)` summarises what the event shows. An event whose key equals the key of the last one sent is dropped.
    `None` means "always send" (and the next event is compared with nothing, so it is sent too), and so does a
    `final` event.

    **The state is per stream:** call `distinct` inside the route, once per request, as in
    `hub.sse(room, read=distinct(read, key=...))`. One wrapper shared by two streams would drop what the second has
    never been sent.

    **A dropped event's `id` is never applied.** The hub advances the cursor only on events it yields, and the client
    records only ids it receives. Drop only events whose id would not have advanced the cursor (a snapshot with no id,
    or a repeat of the same one), never an event that carries a new row.

    `read` may be `read(cursor)` or `read(cursor, wake)`, sync or async; the wrapper keeps that shape.
    """
    last: Any = _NOTHING_SENT

    async def filtered(*args: Any) -> list[Event]:
        nonlocal last
        sent = []
        for event in await _call(read, *args):
            summary = key(event)
            if event.final or summary is None:
                last = _NOTHING_SENT
            elif summary == last:
                continue
            else:
                last = summary
            sent.append(event)
        return sent

    if _accepts_wake(read):

        async def with_wake(cursor: Any, wake: Any) -> list[Event]:
            return await filtered(cursor, wake)

        return with_wake

    async def with_cursor(cursor: Any) -> list[Event]:
        return await filtered(cursor)

    return with_cursor
