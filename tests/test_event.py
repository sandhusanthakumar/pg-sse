from datetime import datetime

import pytest

import pg_sse
from pg_sse import Event
from pg_sse._event import decode, encode, encode_cursor, encode_goodbye


def test_a_string_is_sent_as_is():
    assert encode(Event("hello")) == "data: hello\n\n"


def test_anything_else_is_json():
    assert encode(Event({"a": [1, 2]})) == 'data: {"a":[1,2]}\n\n'


def test_id_and_event_name_come_first():
    assert encode(Event("x", id=7, event="message")) == "id: 7\nevent: message\ndata: x\n\n"


def test_every_line_ending_starts_a_new_data_line():
    assert encode(Event("a\nb\r\nc\rd")) == "data: a\ndata: b\ndata: c\ndata: d\n\n"


def test_other_unicode_breaks_stay_in_one_line():
    line_separator = "\u2028"
    assert encode(Event(f"a{line_separator}b")) == f"data: a{line_separator}b\n\n"


def test_an_empty_string_is_one_empty_data_line():
    assert encode(Event("")) == "data: \n\n"


@pytest.mark.parametrize("bad", ["1\n2", "1\r", "a\x00"])
def test_an_id_cannot_break_the_frame(bad):
    with pytest.raises(ValueError, match="id"):
        encode(Event("x", id=bad))


def test_an_event_name_cannot_break_the_frame():
    with pytest.raises(ValueError, match="event"):
        encode(Event("x", event="a\nb"))


def test_a_cursor_frame_has_no_data():
    assert encode_cursor(42) == "id: 42\n\n"


def test_data_that_is_not_json_native_needs_a_default():
    event = Event({"at": datetime(2026, 1, 2, 3, 4, 5)})
    assert encode(event, default=str) == 'data: {"at":"2026-01-02 03:04:05"}\n\n'
    with pytest.raises(TypeError, match="datetime"):
        encode(event)


def test_a_goodbye_sets_the_retry_and_names_the_reason_only_when_asked():
    assert encode_goodbye(None, "max_age", 1000) == "retry: 1000\n\n"
    assert encode_goodbye("end", "max_age", 1000) == 'retry: 1000\nevent: end\ndata: {"reason":"max_age"}\n\n'


def test_the_read_and_start_aliases_are_public():
    assert {"Read", "Start"} <= set(pg_sse.__all__)
    assert pg_sse.Read is not None
    assert pg_sse.Start is not None


def test_decode_names_the_frames_a_stream_yields():
    ping = decode(": ping\n\n")
    assert (ping.comment, ping.is_ping, ping.is_cursor, ping.is_goodbye, ping.data) == (
        "ping",
        True,
        False,
        False,
        None,
    )

    cursor = decode(encode_cursor(7))
    assert (cursor.id, cursor.is_cursor, cursor.is_ping, cursor.data) == ("7", True, False, None)

    retry = decode(encode_goodbye(None, "max_age", 1000))
    assert (retry.retry, retry.is_goodbye, retry.event, retry.data, retry.id) == (1000, True, None, None, None)

    named = decode(encode_goodbye("end", "shutdown", 30000))
    assert (named.retry, named.event, named.is_goodbye) == (30000, "end", True)
    assert named.json() == {"reason": "shutdown"}

    event = decode(encode(Event({"a": 1}, id=3, event="change")))
    assert (event.id, event.event, event.json(), event.is_goodbye, event.is_cursor) == (
        "3",
        "change",
        {"a": 1},
        False,
        False,
    )
    assert event.raw == 'id: 3\nevent: change\ndata: {"a":1}\n\n'


@pytest.mark.parametrize(
    "data",
    ["plain", "a\nb\nc", "", "caf\u00e9 \u2603 \U0001f600", "a\u2028b", " leading space", "x: y", "tail "],
)
def test_decode_round_trips_what_encode_writes(data):
    frame = decode(encode(Event(data, id="1", event="m")))
    assert (frame.id, frame.event, frame.data) == ("1", "m", data)


@pytest.mark.parametrize("data", ["a\r\nb", "a\rb", "a\nb\r\nc\rd"])
def test_decode_joins_data_lines_with_a_newline_whatever_the_line_ending(data):
    assert decode(encode(Event(data))).data == data.replace("\r\n", "\n").replace("\r", "\n")


def test_decode_round_trips_data_that_json_cannot_encode_by_itself():
    when = datetime(2026, 1, 2, 3, 4, 5)
    frame = decode(encode(Event({"at": when, "n": [1, 2]}), default=str))
    assert frame.json() == {"at": str(when), "n": [1, 2]}


def test_decode_follows_the_sse_parsing_rules():
    frame = decode("id:1\r\nevent:e\rdata:a\ndata: b\r\ndata:  c\n:\n: one\n:two\nretry: 12\nbogus\nretry: x\n\n")
    assert (frame.id, frame.event, frame.data, frame.retry) == ("1", "e", "a\nb\n c", 12)
    assert frame.comment == "\none\ntwo"
    assert decode("data\n\n").data == ""  # a field with no colon has an empty value
    assert decode("\n\n").data is None
    assert decode("retry: 1x\n\n").retry is None  # digits only
    with pytest.raises(ValueError, match="no data"):
        decode(": ping\n\n").json()
