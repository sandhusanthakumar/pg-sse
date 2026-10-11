"""`IntCursor`: a client-supplied cursor made of integers, checked before it reaches `read`."""

import logging
import re
from typing import cast

from ._event import Throttle, _field

logger = logging.getLogger("pg_sse")

# For the whole process, not one `IntCursor`: one made per request must not log on every one.
_too_long = Throttle(60.0)

# A cursor is an id in Postgres, and bigint is its widest integer: a larger part would fail the query (a 500 on client
# input) or, sent as numeric, compare without the index.
BIGINT_MAX = 2**63 - 1


class IntCursor:
    """
    Parses and formats a cursor of `parts` non-negative integers joined by `sep`: `42`, or `12.3` for a feed that
    spans two tables. A `Last-Event-ID` is client input, so only ASCII digits are accepted (`str.isdigit` also
    accepts `²`, which `int` then rejects).

        cursors = IntCursor()
        hub.sse(key, read, cursor=cursors.parse_one(last_event_id), ...)  # 42, or None: start from now
        Event(row, id=row.id)  # an int, like the cursor: `read` gets the same type every time

    For several parts, `parse` gives a tuple, which the hub refuses as a cursor (`TypeError`: it has no id form
    `parse` would accept back). Pass `cursors.format(*parsed)`, set each id with `cursors.format(...)`, and have
    `read` parse its cursor.

    - `parts`: how many integers the cursor has.
    - `sep`: what joins them. Not empty, with no digit, line break or NUL.
    - `max_digits`: the most digits one part may have, leading zeros aside, so a cursor cannot ask for an absurdly large
      number. 15 digits are exact in a JavaScript number and in a JSON consumer's double. Ids that are longer (snowflake
      ids, a bigint with a high sequence start) need `max_digits=19`, the most allowed; `format` raises `ValueError` for
      a longer one. Whatever `max_digits` is, a part above the bigint maximum (9223372036854775807) is refused.
    """

    def __init__(self, parts: int = 1, *, sep: str = ".", max_digits: int = 15) -> None:
        # `type(...) is int`, as in `format`: True would pass for 1.
        if type(parts) is not int or parts < 1:
            raise ValueError("parts must be an int, at least 1")
        if type(max_digits) is not int or not 1 <= max_digits <= 19:
            raise ValueError("max_digits must be 1 to 19 (a bigint has at most 19 digits)")
        if not isinstance(sep, str) or not sep or any(char.isdigit() for char in sep):
            raise ValueError("sep must be text, not empty, and must not contain a digit")
        _field("id", sep)  # ValueError for a line break or NUL: every event formatted with it would fail the stream
        self.parts = parts
        self.sep = sep
        self.max_digits = max_digits
        # Digit runs joined by a separator that holds no digit: nothing to backtrack over, however long a client's
        # header is. `max_digits` is checked after, on each part without its zero padding (`0042` is 42).
        self._pattern = re.compile(re.escape(sep).join(["[0-9]+"] * parts), re.ASCII)

    def parse(self, text: str | bytes | None) -> tuple[int, ...] | None:
        """
        `None` for no cursor (`None` or empty); the integers otherwise; `ValueError` for any other text. Bytes are a raw
        header, read as HTTP does (Latin-1). `TypeError` for anything else, an int say: it was parsed already, by the
        code, so it is not the client's input to forgive.
        """
        text = _header_text(text)
        if text is None or text == "":
            return None
        if not isinstance(text, str):
            raise TypeError(f"pg_sse: a cursor to parse is the header's text, got {type(text).__name__}")
        digits = self._digits(text)
        if digits is None or any(len(part) > self.max_digits for part in digits):
            example = self.sep.join(str(number) for number in range(1, self.parts + 1))
            raise ValueError(f"cursor must look like {example}")
        values = tuple(int(part) for part in digits)
        if any(value > BIGINT_MAX for value in values):
            raise ValueError(f"each part of the cursor must be at most {BIGINT_MAX}")
        return values

    def _digits(self, text: str) -> list[str] | None:
        """Each part's digits without zero padding, or None if `text` is not digit runs joined by `sep`."""
        if self._pattern.fullmatch(text) is None:
            return None
        return [part.lstrip("0") or "0" for part in text.split(self.sep)]

    def parse_one(self, text: str | bytes | None) -> int | None:
        """
        A one-part cursor straight from a header (text, or the raw bytes), for `hub.sse(cursor=...)`: the integer, or
        None both for no cursor and for anything that is not one, so the stream starts from now. Not a 422:
        `EventSource` stops for good on any response that is not a 200, so one stale id would end live updates in that
        tab. `ValueError` if `parts` is not 1, and `TypeError` for an int, as for `parse`: mistakes in the code, not in
        the client's input.
        """
        if self.parts != 1:
            raise ValueError("parse_one is for a one-part cursor (parts=1); use parse")
        text = _header_text(text)  # so the warning below can read it
        try:
            parsed = self.parse(text)
        except ValueError:
            found = self._digits(cast(str, text))  # a str by now: anything else made `parse` raise TypeError
            digits = found[0] if found else ""
            # An id this app may well have sent: ids outgrew `max_digits`, and every reconnect skips what it missed.
            # Only when a larger `max_digits` would read it, and at most once a minute: any client can send the header.
            if self.max_digits < len(digits) <= 19 and int(digits) <= BIGINT_MAX and _too_long.ready():
                logger.warning(
                    "pg_sse: cursor %s has more than %d digits; pass a larger max_digits", digits, self.max_digits
                )
            return None
        return parsed[0] if parsed else None

    def format(self, *values: int) -> str:
        """The cursor text for `Event(id=...)`. `ValueError` unless there are `parts` values that `parse` accepts."""
        if len(values) != self.parts:
            raise ValueError(f"expected {self.parts} values, got {len(values)}")
        # `type(...) is int`, not isinstance: True is an int and would be written as "True"; a float as "1.5".
        for value in values:
            if type(value) is int and value > BIGINT_MAX:  # before `str`, which refuses ints over 4300 digits
                raise ValueError(f"each value must be at most {BIGINT_MAX}, the bigint maximum")
            if type(value) is not int or value < 0 or len(str(value)) > self.max_digits:
                raise ValueError(
                    f"each value must be a non-negative int of at most {self.max_digits} digits "
                    "(pass a larger max_digits for ids that long; a bigint has up to 19)"
                )
        return self.sep.join(str(value) for value in values)


def _header_text(text: str | bytes | None) -> str | None:
    """A raw header (bytes), read as HTTP does: Latin-1. Anything else as it is."""
    return text.decode("latin-1") if isinstance(text, bytes) else text
