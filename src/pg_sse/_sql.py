import re

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def trigger_sql(table: str, key_column: str, channel: str, *, order_column: str | None = None) -> str:
    """
    The SQL that wires a table to a channel: run it once, in a migration.

    - A notify trigger sends `pg_notify(channel, key)` for every inserted, updated or deleted row (both keys when an
      update moves a row). A trigger, not a call in each writer, so a cron job or a psql session cannot forget it.
      NOTIFY is delivered on commit: a rolled-back write announces nothing.
    - With `order_column` (a serial, bigserial or identity column, usually `id`), a second trigger makes that column
      follow commit order within a key: an insert locks its key until it commits, then draws the next value. Without
      it, a reader at cursor 11 can skip id 10 committing late. The trigger replaces any value an INSERT supplies.

    The order trigger takes an advisory lock on the key until the inserting transaction ends, so other inserts to
    that key wait, and two transactions inserting into keys in opposite order can deadlock: keep that transaction
    short. The lock is `pg_advisory_xact_lock(hashtext(channel), hashtext(key))`, and advisory locks share one
    namespace per database, so an application taking the same two-int lock would serialise against these inserts.
    `hashtext` is 32-bit, so two different keys can collide and briefly make each other's inserts wait: a delay,
    never wrong ids.

    Names are plain lower-case identifiers; `table` may be `schema.table`. Postgres 14 or later. The objects it creates
    are named `"pg_sse$notify$<table>$<key_column>"` and `"pg_sse$order$<table>$<order_column>"`: a trigger and a
    function each, in the table's schema.
    """
    name, qualified, prefix = _parse(table, key_column, channel, order_column)
    notify_fn = _name("pg_sse", "notify", name, key_column)
    statements = []
    if order_column:
        # Without a sequence the first insert would fail on a null id; fail when the trigger is installed instead.
        statements.append(f"""DO $pg_sse$
BEGIN
    IF pg_get_serial_sequence('{qualified}', '{order_column}') IS NULL THEN
        RAISE EXCEPTION 'pg_sse: {qualified}."{order_column}" has no sequence; '
            'order_column must be serial, bigserial or identity';
    END IF;
END
$pg_sse$;""")
    statements += [
        f"""CREATE OR REPLACE FUNCTION {prefix}"{notify_fn}"() RETURNS trigger LANGUAGE plpgsql AS $pg_sse$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('{channel}', OLD."{key_column}"::text);
        RETURN OLD;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD."{key_column}" IS DISTINCT FROM NEW."{key_column}" THEN
        PERFORM pg_notify('{channel}', OLD."{key_column}"::text);
    END IF;
    PERFORM pg_notify('{channel}', NEW."{key_column}"::text);
    RETURN NEW;
END;
$pg_sse$;""",
        f'CREATE OR REPLACE TRIGGER "{notify_fn}" AFTER INSERT OR UPDATE OR DELETE ON {qualified} '
        f'FOR EACH ROW EXECUTE FUNCTION {prefix}"{notify_fn}"();',
    ]
    if order_column:
        order_fn = _name("pg_sse", "order", name, order_column)
        statements += [
            f"""CREATE OR REPLACE FUNCTION {prefix}"{order_fn}"() RETURNS trigger LANGUAGE plpgsql AS $pg_sse$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('{channel}'), hashtext(NEW."{key_column}"::text));
    NEW."{order_column}" := nextval(pg_get_serial_sequence('{qualified}', '{order_column}'));
    RETURN NEW;
END;
$pg_sse$;""",
            f'CREATE OR REPLACE TRIGGER "{order_fn}" BEFORE INSERT ON {qualified} '
            f'FOR EACH ROW EXECUTE FUNCTION {prefix}"{order_fn}"();',
        ]
    return "\n\n".join(statements) + "\n"


def drop_trigger_sql(table: str, key_column: str, *, order_column: str | None = None) -> str:
    """The SQL that undoes `trigger_sql` (same arguments, no channel needed): for a downgrade migration."""
    name, qualified, prefix = _parse(table, key_column, None, order_column)
    names = [_name("pg_sse", "notify", name, key_column)]
    if order_column:
        names.append(_name("pg_sse", "order", name, order_column))
    statements = []
    for fn in names:
        statements += [f'DROP TRIGGER IF EXISTS "{fn}" ON {qualified};', f'DROP FUNCTION IF EXISTS {prefix}"{fn}"();']
    return "\n".join(statements) + "\n"


def _parse(table: str, key_column: str, channel: str | None, order_column: str | None) -> tuple[str, str, str]:
    """Validates the identifiers; returns the table name, the quoted table and the quoted schema prefix."""
    schema, _, name = table.rpartition(".")
    parts = [name, key_column, *([schema] if schema else [])]
    parts += [part for part in (channel, order_column) if part is not None]
    for part in parts:
        _check(part)
    qualified = f'"{schema}"."{name}"' if schema else f'"{name}"'
    prefix = f'"{schema}".' if schema else ""
    return name, qualified, prefix


def _check(identifier: str) -> None:
    if not _IDENTIFIER.match(identifier):
        raise ValueError(f"pg_sse: {identifier!r} is not a plain lower-case identifier (a-z, 0-9, _; at most 63 bytes)")


def _name(*parts: str) -> str:
    # `$` is legal in a PostgreSQL identifier and cannot appear in a validated one, so no two tables share a name.
    name = "$".join(parts)
    if len(name) > 63:
        raise ValueError(f"pg_sse: the generated name {name!r} is longer than 63 bytes; shorten the table or column")
    return name
