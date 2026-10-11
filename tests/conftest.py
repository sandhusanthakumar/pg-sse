import os
import signal
import uuid
from types import SimpleNamespace

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from pg_sse import _cursor as cursor_module
from pg_sse import _hub as hub_module
from pg_sse._event import Throttle

# A scratch database the tests may create schemas in; every test gets its own schema and channel.
DSN = os.environ.get("PG_SSE_TEST_DSN")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def fast_listener(monkeypatch):
    """The listener polls its stop flag and retries on shorter clocks, so every lifespan ends quickly."""
    monkeypatch.setattr(hub_module, "_LISTEN_POLL_SECONDS", 0.1)
    monkeypatch.setattr(hub_module, "_BACKOFF_START_SECONDS", 0.05)


@pytest.fixture(autouse=True)
def fresh_throttles(monkeypatch):
    """The warning a process limits to one a minute is said again for every test."""
    monkeypatch.setattr(hub_module, "_bad_cursor", Throttle())
    monkeypatch.setattr(cursor_module, "_too_long", Throttle())


@pytest.fixture
def restore_signals():
    saved = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    yield
    for sig, handler in saved.items():
        signal.signal(sig, handler)


@pytest.fixture
def db():
    if not DSN:
        pytest.skip("set PG_SSE_TEST_DSN to a scratch database to run the database tests")
    schema = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
    dsn = make_conninfo(DSN, options=f"-csearch_path={schema}")
    try:
        yield SimpleNamespace(dsn=dsn, schema=schema, channel=schema)
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
