---
name: Bug report
about: Something does not work as the README says
labels: bug
---

**What happened, and what did you expect?**

**Environment**

- pg-sse version:
- Postgres version:
- Pooler in use between the app and Postgres (none, PgBouncer session/transaction mode, Supabase, Neon, ...), and whether the hub's connection string is direct:
- Framework and version (FastAPI, Starlette, Django, Litestar, ...):
- ASGI server and version (uvicorn, hypercorn, granian, ...), and number of workers:
- Python version:

**Minimal code to reproduce** (the `Hub(...)` setup, the route and `read`):

**Logs** (the `pg_sse` logger, with tracebacks):
