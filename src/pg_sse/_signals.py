"""`signal_read`: a `read` for streams that tell the page what to refetch instead of sending rows."""

from collections.abc import Callable, Mapping
from typing import Any

from ._event import Event, check_event_name
from ._hub import Read, Wake, check_signal

SignalName = str | tuple[str | None, Callable[[frozenset[str]], Any]] | None


def signal_read(
    *,
    start: str | None = "ready",
    resync: str | None = "resync",
    change: str | None = "change",
    change_data: Callable[[frozenset[str]], Any] | None = None,
    signals: Mapping[str, SignalName] | None = None,
) -> Read:
    """
    A `read(cursor, wake)` that answers each cause with an event naming what to refetch. Async, so it runs on the
    event loop with no thread hop.

    - `start`: the event name for the stream's first read.
    - `resync`: the name for "anything may have changed": the listener (re)connected, or `reread_seconds` passed
      quietly.
    - `change`: the name when a database notification reached the stream's key.
    - `change_data`: `fn(keys)` gives the `change` event's data from the keys notified since the last read. The data
      is `{}` without it; a per-key stream might pass `lambda keys: {"conversationId": ...}`.
    - `signals`: `{label: event name}` for `hub.wake` and `Ephemeral` signals, such as `{"presence": "presence"}`.
      The event's data is `{}`. A value of `None` ignores that signal. A value may instead be `(name, data_fn)`:
      `data_fn(keys)` gives the data from the keys the signal was sent for (`wake.signal_keys`), so one `signal_read`
      can serve every stream and each event still names its own key, as `change_data` does for notified keys.

    A name of `None` sends nothing for that cause. The causes are answered in this order, because the first two tell
    the page to refetch everything and anything after them would repeat it:

    1. `start`: only the start event.
    2. `resync` or a quiet re-read: only the resync event. With `resync=None`, go on to 3.
    3. Otherwise the `change` event if keys were notified, then one event per mapped signal that arrived, in the order
       of `signals`. Both are sent when they arrive together: a write that coincides with a signal must not hide it.

    A cause with no name contributes nothing, so the result may be `[]`.

    Raises `TypeError` for a `signals` value of any other shape, a name that is not a `str`, or `change_data` with no
    `change` event to carry it, and `ValueError` for an empty event name (the browser would dispatch it as a plain
    `message`) or one with a line break or NUL, here rather than in every stream's first read.
    """
    if change is None and change_data is not None:
        raise TypeError("change_data needs a change event name; with change=None nothing would carry its data")
    if change_data is not None and not callable(change_data):
        raise TypeError(f"change_data must be a function of the notified keys; got {change_data!r}")
    for cause, name in (("start", start), ("resync", resync), ("change", change)):
        check_event_name(cause, name)
    # label -> (event name, data_fn or None); a signal that sends nothing is left out.
    named: dict[str, tuple[str, Callable[[frozenset[str]], Any] | None]] = {}
    for label, target in (signals or {}).items():
        check_signal(label)  # what `hub.wake` accepts, so the two always match
        if target is None or isinstance(target, str):
            name, data = target, None
        elif isinstance(target, tuple) and len(target) == 2 and callable(target[1]):
            name, data = target
        else:
            raise TypeError(f"signals[{label!r}] must be an event name, None, or (event name, data_fn); got {target!r}")
        check_event_name(f"signals[{label!r}]", name)
        if name is not None:
            named[label] = (name, data)

    async def read(cursor: Any, wake: Wake) -> list[Event]:
        if wake.start:
            return [Event({}, event=start)] if start is not None else []
        if (wake.resync or wake.reread) and resync is not None:
            return [Event({}, event=resync)]
        events = []  # with resync=None, the keys and signals that came with a resync are still answered
        if wake.keys and change is not None:
            events.append(Event(change_data(wake.keys) if change_data else {}, event=change))
        for label, (name, data) in named.items():
            if label in wake.signals:
                events.append(Event(data(wake.signal_keys.get(label, frozenset())) if data else {}, event=name))
        return events

    return read
