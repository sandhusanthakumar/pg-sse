# Contributing

The library is small (a few hundred lines in `src/pg_sse/`). Read `README.md` and `src/pg_sse/_hub.py` first.

## Set up

The database tests need a real Postgres.

```sh
docker run -d --name pg-sse-dev -e POSTGRES_USER=sse -e POSTGRES_PASSWORD=sse -e POSTGRES_DB=pg_sse_test \
  -p 127.0.0.1:55432:5432 postgres:17
export PG_SSE_TEST_DSN=postgresql://sse:sse@127.0.0.1:55432/pg_sse_test
uv sync
```

Each test creates and drops its own schema in that database, so use a scratch one. Without `PG_SSE_TEST_DSN` the
database tests are skipped.

## Check your change

```sh
uv run pytest -q --cov --cov-report=term-missing   # coverage must stay at or above 95%
uv run ruff check . && uv run ruff format --check .
uv run mypy                                        # strict
```

CI runs the same on Python 3.11-3.14, Postgres 14 and 18, and the lowest supported dependencies.

## Style

- Short modules, and comments that explain *why*, not what.
- **Tests are sentences**: `test_a_stream_ends_at_its_age_limit`, not `test_age_limit`. A bug fix comes with a test
  that fails without it.
- Keep `hub.stream` free of any web framework; Starlette is imported lazily, only inside `hub.sse`.
- Update `README.md`, `llms.txt` and `CHANGELOG.md` when the API changes.
