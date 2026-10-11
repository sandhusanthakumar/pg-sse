"""
Per-key signals that lapse: state that is true only while a client keeps renewing it (someone is typing, someone
has a room open). When it changes or lapses, the key's streams are woken, so a closed tab never leaves a stale
"typing" on another screen.

State is kept in this process's memory, like `hub.wake`: with several workers, a signal set on one is invisible to
the streams on the others.
"""

import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from ._hub import _check_key, _seconds, check_signal

if TYPE_CHECKING:
    from ._hub import Hub


class Ephemeral:
    """
    Signals per key that lapse unless renewed, announced to the key's streams with `hub.wake`.

    - `name`: what this is for ("typing", "viewing"). It is the default `signal`.
    - `ttl`: seconds a signal lasts without renewal.
    - `signal`: the label `hub.wake` carries, so `wake.signals` contains it. Two instances may share one, to wake
      one stream for either.
    - `grace`: the lapse check runs this long after expiry, so it never finds a signal a hair from expiring.
    - `all_keys`: passed to `hub.wake`. False by default: ephemeral state is per key, and a view of every key has
      nothing to show for it.
    - `clock`: seconds, monotonic. Replace it in tests.

    Lapses are noticed by `hub.call_later`, which does nothing while the hub is stopped: entries then linger until the
    next `set` or `clear` for their key, and `live` never lists an expired one.
    """

    def __init__(
        self,
        hub: "Hub",
        name: str,
        *,
        ttl: float,
        signal: str | None = None,
        grace: float = 0.25,
        all_keys: bool = False,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.hub = hub
        self.name = name
        self.ttl = _seconds("ttl", ttl)
        self.signal = name if signal is None else signal
        check_signal(self.signal)  # at construction, not at the first `set`
        self.grace = _seconds("grace", grace, zero=True)
        self.all_keys = all_keys
        self._clock = clock
        self._lock = threading.Lock()
        # key -> who -> when it lapses, in insertion order. A key with nobody has no entry.
        self._entries: dict[str, dict[str, float]] = {}
        # Bumped by `reset`: a lapse check scheduled before it belongs to a state that no longer exists.
        self._generation = 0

    def set(self, key: str, who: str, on: bool = True) -> bool:
        """
        Renews or starts (`on`) or removes (`not on`) `who`'s signal on `key`. Safe from any thread.

        Returns whether what a reader sees changed, and wakes the key's streams only then. A renewal changes nothing.
        Removing wakes if there was an entry, even one that lapsed a moment ago: someone may have read it before it
        lapsed.
        """
        _check_key(key, allow_none=False)
        if not on:
            return self._remove(key, who)
        now = self._clock()
        with self._lock:
            users = self._entries.setdefault(key, {})
            previous = users.get(who)
            changed = previous is None or previous <= now
            users[who] = now + self.ttl
            generation = self._generation
        if changed:
            self._wake(key)
        # One timer per renewal, not per key: `call_later` does nothing while the hub is stopped, so a "pending"
        # flag set then would never be cleared. An earlier timer finds the signal renewed and leaves it alone.
        self.hub.call_later(self.ttl + self.grace, self._check, key, generation)
        return changed

    def clear(self, key: str, who: str | None = None) -> bool:
        """Removes `who`'s signal on `key`, or everyone's. Returns and wakes as `set(..., on=False)` does."""
        _check_key(key, allow_none=False)
        if who is not None:
            return self._remove(key, who)
        with self._lock:
            removed = self._entries.pop(key, None)
        if removed:
            self._wake(key)
        return bool(removed)

    def reset(self) -> None:
        """
        Forgets every key and signal without waking any stream, and makes the lapse checks already scheduled do
        nothing. For tests: a fixture's clean slate.
        """
        with self._lock:
            self._entries.clear()
            self._generation += 1

    def live(self, key: str) -> list[str]:
        """Whoever has not lapsed on `key` right now, in the order they started, even if a lapse check has not run."""
        now = self._clock()
        with self._lock:
            return [who for who, until in self._entries.get(key, {}).items() if until > now]

    def _remove(self, key: str, who: str) -> bool:
        with self._lock:
            users = self._entries.get(key)
            if users is None or who not in users:
                return False
            del users[who]
            if not users:
                del self._entries[key]
        self._wake(key)
        return True

    def _check(self, key: str, generation: int) -> None:
        """Runs on the hub's loop `ttl + grace` after a renewal: drops what lapsed, and says so only if it dropped."""
        now = self._clock()
        with self._lock:
            if generation != self._generation:
                return  # scheduled before a `reset`
            users = self._entries.get(key)
            if not users:
                return
            lapsed = [who for who, until in users.items() if until <= now]
            for who in lapsed:
                del users[who]
            if not users:
                del self._entries[key]
        if lapsed:
            self._wake(key)

    def _wake(self, key: str) -> None:
        self.hub.wake(key, self.signal, all_keys=self.all_keys)  # outside the lock: it only posts to the loop
