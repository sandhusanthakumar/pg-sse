from datetime import datetime

import pytest

from pg_sse import Event
from pg_sse._event import encode, encode_cursor, encode_goodbye


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
