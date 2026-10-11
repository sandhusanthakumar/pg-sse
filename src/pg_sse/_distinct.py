"""`distinct`: a `read` that stays quiet while what it would send has not changed."""

import functools
from collections.abc import Callable, Hashable, Iterable
from typing import Any

from ._event import Event
from ._hub import Read, _call, _check_read, _is_async, current_read_executor

_NOTHING_SENT = object()


def distinct(read: Read, key: Callable[[Event], Hashable | None]) -> Read:
    """
    Wraps `read` so a snapshot is sent only when it changed.

    `key(event)` summarises what the event shows. An event whose key equals the key of the last one sent is dropped.
    `None` means "always send" (and the next event is compared with nothing, so it is sent too), and so does a
    `final` event.

    Prefer a key that covers everything the client is shown, including the newest row or message id: a new row then
    always changes it, with no `None`. After a `None` the next unchanged snapshot is sent again.

    **The state is per stream:** call `distinct` inside the route, once per request, as in
    `hub.sse(room, read=distinct(read, key=...))`. One wrapper shared by two streams would drop what the second has
    never been sent.

    **A dropped event's `id` is never applied.** The hub advances the cursor only on events it yields, and the client
    records only ids it receives. Drop only events whose id would not have advanced the cursor (a snapshot with no id,
    or a repeat of the same one), never an event that carries a new row.

    `read` may be `read(cursor)` or `read(cursor, wake)`, sync or async; the wrapper takes the same arguments and is
    async. A sync `read` runs in the hub's `read_executor` inside a stream, and in asyncio's default pool when the
    wrapper is called outside one, so it never blocks the loop. `key` runs on the event loop either way, once the
    events are back from `read`, so it may touch loop-bound state, and must be cheap.
    """
    last: Any = _NOTHING_SENT

    def filtered(events: Iterable[Event]) -> list[Event]:
        nonlocal last
        sent = []
        for event in events:
            summary = key(event)
            if event.final or summary is None:
                last = _NOTHING_SENT
            elif summary == last:
                continue
            else:
                last = summary
            sent.append(event)
        return sent

    _check_read(read)
    read_is_async = _is_async(read)

    async def wrapper(*args: Any) -> list[Event]:
        return filtered(await _call(read, *args, executor=current_read_executor.get(), is_async=read_is_async))

    # The hub reads the signature to decide whether to pass a `Wake`; `inspect.signature` follows `__wrapped__`.
    return functools.wraps(read, updated=())(wrapper)
