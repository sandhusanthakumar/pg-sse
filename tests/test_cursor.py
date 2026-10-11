import logging
import time

import pytest

from pg_sse import IntCursor


def test_a_single_cursor_round_trips_as_a_tuple():
    cursor = IntCursor()
    assert cursor.format(42) == "42"
    assert cursor.parse("42") == (42,)
    assert cursor.parse(cursor.format(0)) == (0,)


@pytest.mark.parametrize(
    ("text", "expected"), [("42", 42), ("0", 0), (None, None), ("", None), ("x", None), ("²", None), ("1.2", None)]
)
def test_parse_one_gives_the_int_or_none_and_never_raises_for_client_input(text, expected):
    assert IntCursor().parse_one(text) == expected


def test_parse_one_warns_about_an_id_longer_than_max_digits(caplog):
    cursors = IntCursor()
    with caplog.at_level(logging.WARNING, logger="pg_sse"):
        assert cursors.parse_one("x" * 16) is None  # just not a cursor: nothing to say
        assert cursors.parse_one("1" * 16) is None  # an id the app may have sent: catching up would be lost
        assert cursors.parse_one("2" * 16) is None  # said once: a client could send it on every request
        assert IntCursor(max_digits=19).parse_one(str(2**63)) is None  # above the bigint maximum: no larger setting
    assert caplog.messages == ["pg_sse: cursor 1111111111111111 has more than 15 digits; pass a larger max_digits"]


def test_zero_padding_does_not_count_against_max_digits():
    assert IntCursor().parse_one("0000000000000042") == 42
    assert IntCursor(2, max_digits=3).parse("0001.0002") == (1, 2)


def test_a_cursor_already_parsed_is_the_codes_mistake_not_the_clients():
    with pytest.raises(TypeError, match="header's text, got int"):
        IntCursor().parse_one(42)  # type: ignore[arg-type]  # returning None would start from now, silently
    assert IntCursor().parse_one(b"42") == 42  # a raw header, read as HTTP does
    assert IntCursor().parse_one(b"\xb2") is None  # "²" in Latin-1: not a cursor


def test_any_amount_of_zero_padding_parses_to_its_value():
    assert IntCursor().parse("0" * 5000 + "42") == (42,)  # `int` alone refuses strings over 4300 digits


def test_a_long_crafted_header_is_refused_quickly():
    # Long zero runs ending in a mismatch: a pattern that can backtrack over them takes seconds, on the event loop.
    cursors = IntCursor(4, max_digits=19)
    started = time.perf_counter()
    with pytest.raises(ValueError, match="cursor must look like"):
        cursors.parse(".".join(["0" * 2000] * 4) + "x")
    assert time.perf_counter() - started < 0.1


def test_parse_one_does_not_warn_about_an_id_no_setting_could_read(caplog):
    with caplog.at_level(logging.WARNING, logger="pg_sse"):
        assert IntCursor().parse_one("9" * 26) is None  # longer than a bigint: a client made it up
        assert IntCursor().parse_one(str(2**63)) is None  # 19 digits, above the bigint maximum
    assert caplog.messages == []


def test_format_says_the_bigint_limit_for_a_huge_int():
    with pytest.raises(ValueError, match="the bigint maximum"):
        IntCursor().format(10**5000)  # not Python's own limit on turning it into text


def test_parse_one_is_only_for_a_one_part_cursor():
    with pytest.raises(ValueError, match="parts=1"):
        IntCursor(2).parse_one("1.2")


def test_a_composite_cursor_round_trips():
    cursor = IntCursor(2)
    assert cursor.format(12, 3) == "12.3"
    assert cursor.parse("12.3") == (12, 3)
    dashed = IntCursor(3, sep="-")
    assert dashed.parse(dashed.format(1, 22, 333)) == (1, 22, 333)


def test_no_cursor_is_none():
    assert IntCursor().parse(None) is None
    assert IntCursor(2).parse("") is None
    assert IntCursor().parse(b"") is None


def test_a_raw_header_is_read_as_its_text():
    assert IntCursor().parse(b"42") == (42,)  # bytes from a raw ASGI header, read as Latin-1 as HTTP does


@pytest.mark.parametrize(
    ("parts", "text"),
    [
        (2, "12"),  # too few parts
        (1, "1.2"),  # too many
        (2, "1.2.3"),
        (2, "1."),  # an empty part
        (2, ".2"),
        (2, "."),
        (1, ".1"),  # a leading or trailing separator
        (1, "1."),
        (1, " 1"),  # whitespace
        (1, "1 "),
        (1, "1\n"),
        (2, "1. 2"),
        (1, "-1"),
        (1, "+1"),
        (1, "1e3"),
        (1, "0x10"),
        (1, "1" * 16),  # more than max_digits
        (2, "1." + "2" * 16),
        (1, "abc"),
        (1, "\u00b2"),  # superscript two: isdigit() is True, int() raises
        (1, "\u0661\u0662"),  # Arabic-Indic digits: int() reads them, but a cursor is ASCII only
        (1, "\uff11\uff12"),  # fullwidth digits, likewise
        (2, "1.\u00b2"),
    ],
)
def test_input_that_is_not_digits_in_the_right_shape_is_refused(parts, text):
    with pytest.raises(ValueError, match="cursor must look like"):
        IntCursor(parts).parse(text)


def test_the_message_shows_the_shape_and_the_longest_part_is_accepted():
    with pytest.raises(ValueError, match=r"cursor must look like 1\.2$"):
        IntCursor(2).parse("x")
    with pytest.raises(ValueError, match=r"cursor must look like 1$"):
        IntCursor().parse("x")
    assert IntCursor().parse("9" * 15) == (10**15 - 1,)
    assert IntCursor(max_digits=3).parse("999") == (999,)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"parts": 0},
        {"parts": -1},
        {"max_digits": 0},
        {"max_digits": 20},
        {"max_digits": True},  # an int, but it would mean max_digits=1
        {"parts": 1.0},
        {"sep": ""},
        {"sep": "1"},
        {"sep": "²"},
        {"sep": "\n"},
        {"sep": "\r"},
        {"sep": "\x00"},  # an SSE id cannot hold a NUL, so every formatted cursor would fail the stream
        {"sep": "-\x00"},
        {"sep": 1},
    ],
)
def test_bad_settings_are_refused(kwargs):
    with pytest.raises(ValueError):
        IntCursor(**kwargs)


def test_format_refuses_what_parse_would_not_read_back():
    cursor = IntCursor(2, max_digits=3)
    for values in ((1,), (1, 2, 3), (), (-1, 2), (1, 1000), (True, 1), (1, False), (1.5, 2), (1.0, 2), ("1", 2)):
        with pytest.raises(ValueError):
            cursor.format(*values)
    assert cursor.parse(cursor.format(999, 0)) == (999, 0)


def test_a_value_too_long_for_max_digits_says_how_to_allow_it():
    with pytest.raises(ValueError, match="max_digits"):
        IntCursor().format(10**15)
    assert IntCursor(max_digits=19).format(2**62) == str(2**62)


def test_a_part_above_the_bigint_maximum_is_refused_both_ways():
    cursors = IntCursor(2, max_digits=19)
    largest = 2**63 - 1
    assert cursors.parse(f"{largest}.0") == (largest, 0)
    assert cursors.format(largest, 0) == f"{largest}.0"
    for text in (f"{largest + 1}.0", "0.9999999999999999999"):
        with pytest.raises(ValueError, match="at most 9223372036854775807"):
            cursors.parse(text)
    with pytest.raises(ValueError, match="bigint maximum"):
        cursors.format(0, largest + 1)
