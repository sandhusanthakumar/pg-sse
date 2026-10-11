import asyncio

import pytest
from helpers import execute, make_hub

from pg_sse import Event, Wake, signal_read
from pg_sse._hub import _accepts_wake
from pg_sse.testing import next_event, next_frame

pytestmark = pytest.mark.anyio


def names(events: list[Event]) -> list[str | None]:
    return [event.event for event in events]


async def test_each_cause_alone_yields_its_event():
    read = signal_read(signals={"presence": "presence"})
    assert names(await read(None, Wake(start=True))) == ["ready"]
    assert names(await read(None, Wake(resync=True))) == ["resync"]
    assert names(await read(None, Wake(reread=True))) == ["resync"]
    assert names(await read(None, Wake(keys=frozenset({"a"})))) == ["change"]
    assert names(await read(None, Wake(signals=frozenset({"presence"})))) == ["presence"]


async def test_start_with_anything_else_yields_only_the_start_event():
    read = signal_read(signals={"presence": "presence"})
    everything = Wake(start=True, resync=True, keys=frozenset({"a"}), signals=frozenset({"presence"}))
    assert names(await read(None, everything)) == ["ready"]


async def test_a_resync_or_a_reread_hides_the_keys_and_signals_it_arrived_with():
    read = signal_read(signals={"presence": "presence"})
    for cause in ({"resync": True}, {"reread": True}):
        wake = Wake(keys=frozenset({"a"}), signals=frozenset({"presence"}), **cause)
        assert names(await read(None, wake)) == ["resync"]


async def test_keys_and_a_signal_together_yield_the_change_then_the_signal():
    read = signal_read(signals={"presence": "presence"})
    wake = Wake(keys=frozenset({"a"}), signals=frozenset({"presence"}))
    assert names(await read(None, wake)) == ["change", "presence"]


async def test_signals_come_out_in_the_order_of_the_mapping_not_the_order_they_arrived():
    read = signal_read(signals={"typing": "typing", "presence": "presence", "seen": "seen"})
    assert names(await read(None, Wake(signals=frozenset({"seen", "typing", "presence"})))) == [
        "typing",
        "presence",
        "seen",
    ]
    assert names(await read(None, Wake(signals=frozenset({"seen", "typing", "other"})))) == ["typing", "seen"]


async def test_change_data_gets_the_notified_keys_and_the_default_is_an_empty_object():
    seen: list[frozenset[str]] = []

    def data(keys):
        seen.append(keys)
        return {"rooms": sorted(keys)}

    with_data = signal_read(change_data=data)
    [event] = await with_data(None, Wake(keys=frozenset({"b", "a"})))
    assert event.data == {"rooms": ["a", "b"]}
    assert seen == [frozenset({"a", "b"})]
    [plain] = await signal_read()(None, Wake(keys=frozenset({"a"})))
    assert plain.data == {}
    assert [e.data for e in await signal_read(signals={"x": "x"})(None, Wake(signals=frozenset({"x"})))] == [{}]


async def test_a_signal_may_carry_data_made_from_the_keys_it_was_sent_for():
    seen: list[frozenset[str]] = []

    def data(keys):
        seen.append(keys)
        return {"conversationIds": sorted(keys)}

    read = signal_read(signals={"presence": ("presence", data), "typing": "typing"})
    wake = Wake(
        signals=frozenset({"presence", "typing"}),
        signal_keys={"presence": frozenset({"b", "a"}), "typing": frozenset({"a"})},
    )
    presence, typing = await read(None, wake)
    assert (presence.event, presence.data) == ("presence", {"conversationIds": ["a", "b"]})
    assert (typing.event, typing.data) == ("typing", {})  # a plain name still means empty data
    assert seen == [frozenset({"a", "b"})]


async def test_a_signal_with_data_but_no_recorded_keys_gets_an_empty_set():
    read = signal_read(signals={"presence": ("presence", lambda keys: {"n": len(keys)})})
    [event] = await read(None, Wake(signals=frozenset({"presence"})))  # a Wake written by hand has no keys
    assert event.data == {"n": 0}


async def test_a_signal_with_data_keeps_its_place_in_the_mapping_order():
    read = signal_read(signals={"typing": "typing", "presence": ("presence", lambda keys: {}), "seen": "seen"})
    wake = Wake(signals=frozenset({"seen", "presence", "typing"}))
    assert names(await read(None, wake)) == ["typing", "presence", "seen"]


async def test_a_cause_with_no_name_or_no_mapping_yields_nothing():
    silent = signal_read(start=None, resync=None, change=None)
    assert await silent(None, Wake(start=True)) == []
    assert await silent(None, Wake(resync=True)) == []
    assert await silent(None, Wake(reread=True)) == []
    assert await silent(None, Wake(keys=frozenset({"a"}))) == []
    assert await signal_read()(None, Wake(signals=frozenset({"unmapped"}))) == []
    assert await signal_read()(None, Wake()) == []


async def test_with_no_resync_event_a_resync_still_answers_its_keys_and_signals():
    read = signal_read(resync=None, change="messages", signals={"typing": "typing"})
    wake = Wake(keys=frozenset({"a"}), signals=frozenset({"typing"}), resync=True)
    assert names(await read(None, wake)) == ["messages", "typing"]
    assert await read(None, Wake(resync=True)) == []


async def test_a_signal_mapped_to_none_or_to_a_none_name_sends_nothing():
    read = signal_read(signals={"typing": None, "presence": (None, lambda keys: {}), "seen": "seen"})
    everything = Wake(signals=frozenset({"typing", "presence", "seen"}))
    assert names(await read(None, everything)) == ["seen"]


@pytest.mark.parametrize(
    "target",
    [("typing",), ("typing", lambda keys: {}, "extra"), ["typing", lambda keys: {}], ("typing", "not callable"), 1],
)
def test_a_malformed_signal_is_refused_when_the_read_is_built_not_in_a_stream(target):
    with pytest.raises(TypeError, match=r"signals\['typing'\]"):
        signal_read(signals={"typing": target})


@pytest.mark.parametrize(
    "kwargs",
    [{"start": ""}, {"resync": ""}, {"change": ""}, {"signals": {"typing": ""}}, {"signals": {"typing": ("", str)}}],
)
def test_an_empty_event_name_is_refused(kwargs):
    with pytest.raises(ValueError, match="must not be empty"):
        signal_read(**kwargs)


@pytest.mark.parametrize("kwargs", [{"change": "a\nb"}, {"signals": {"typing": "typ\x00ing"}}])
def test_an_event_name_the_encoder_would_refuse_is_refused_up_front(kwargs):
    with pytest.raises(ValueError, match="line break or NUL"):
        signal_read(**kwargs)


@pytest.mark.parametrize("kwargs", [{"start": 1}, {"resync": b"resync"}, {"change": ["change"]}])
def test_an_event_name_that_is_not_text_is_refused(kwargs):
    with pytest.raises(TypeError, match="must be a str or None"):
        signal_read(**kwargs)


def test_a_signal_label_that_is_not_text_is_refused():
    with pytest.raises(TypeError, match="a signal must be a str"):
        signal_read(signals={1: "typing"})  # type: ignore[dict-item]


def test_change_data_that_is_not_a_function_is_refused():
    with pytest.raises(TypeError, match="change_data must be a function"):
        signal_read(change_data={"rooms": []})  # type: ignore[arg-type]


def test_change_data_with_no_change_event_is_refused():
    with pytest.raises(TypeError, match="change_data needs a change event name"):
        signal_read(change=None, change_data=lambda keys: {"keys": sorted(keys)})


async def test_custom_event_names_replace_the_defaults():
    read = signal_read(start="hello", resync="refetch-all", change="messages")
    assert names(await read(None, Wake(start=True))) == ["hello"]
    assert names(await read(None, Wake(resync=True))) == ["refetch-all"]
    assert names(await read(None, Wake(keys=frozenset({"a"})))) == ["messages"]


async def test_changing_the_signals_dict_afterwards_changes_nothing():
    mapping = {"presence": "presence"}
    read = signal_read(signals=mapping)
    mapping["typing"] = "typing"
    assert names(await read(None, Wake(signals=frozenset({"presence", "typing"})))) == ["presence"]


async def test_the_hub_hands_it_the_wake_and_it_streams_what_to_refetch(db):
    hub = make_hub(db)
    read = signal_read(change_data=lambda keys: {"rooms": sorted(keys)}, signals={"presence": "presence"})
    assert _accepts_wake(read)
    async with hub.lifespan():
        frames = hub.stream("room", read=read)
        assert (await next_event(frames)).event == "ready"
        hub.wake("room", "presence")
        assert (await next_event(frames)).event == "presence"
        execute(db.dsn, "SELECT pg_notify(%s, %s)", (db.channel, "room"))
        changed = await next_event(frames)
        assert (changed.event, changed.json()) == ("change", {"rooms": ["room"]})
        await frames.aclose()


async def test_one_read_shared_by_every_stream_names_each_streams_own_key(db):
    hub = make_hub(db)
    read = signal_read(signals={"presence": ("presence", lambda keys: {"conversationId": sorted(keys)[0]})})
    async with hub.lifespan():
        first, second = hub.stream("a", read=read), hub.stream("b", read=read)
        assert (await next_event(first)).event == "ready"
        assert (await next_event(second)).event == "ready"
        hub.wake("b", "presence")
        assert (await next_event(second)).json() == {"conversationId": "b"}
        hub.wake("a", "presence")
        assert (await next_event(first)).json() == {"conversationId": "a"}
        await first.aclose()
        await second.aclose()


async def test_an_all_keys_stream_learns_which_conversation_signalled(db):
    hub = make_hub(db)
    read = signal_read(signals={"presence": ("presence", lambda keys: {"conversationIds": sorted(keys)})})
    async with hub.lifespan():
        frames = hub.stream(None, read=read, cursor=0)
        assert (await next_frame(frames)).is_cursor
        assert (await next_event(frames)).event == "ready"
        waiting = asyncio.ensure_future(next_event(frames))
        await asyncio.sleep(0.05)  # the stream waits for a wake-up
        for key in ("a", "b"):
            hub._dispatch(("wake", key, "presence"), key)  # both before the stream wakes: one wake carries both
        assert (await waiting).json() == {"conversationIds": ["a", "b"]}
        await frames.aclose()
