import psycopg

from pg_sse import Hub, trigger_sql
from pg_sse.testing import until  # noqa: F401 -- re-exported for the tests


def execute(dsn: str, query: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.execute(query, params)
        return cur.fetchall() if cur.description else []


def make_hub(db, **kwargs) -> Hub:
    return Hub(db.dsn, db.channel, handle_signals=False, **kwargs)


def make_table(db, *, order_column: str | None = None) -> None:
    """A `messages` table wired to the test's channel."""
    execute(db.dsn, "CREATE TABLE messages (id bigserial PRIMARY KEY, room text NOT NULL, body text)")
    execute(db.dsn, trigger_sql("messages", "room", db.channel, order_column=order_column))


def terminate_listener(db) -> None:
    execute(
        db.dsn,
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE application_name = 'pg-sse-listener' AND query LIKE %s",
        (f"%{db.channel}%",),
    )
