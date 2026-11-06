# Test infrastructure

Every test run in this repository **shares two long-lived containers**. No agent,
and no test, starts or stops a container.

| Server | Port | Memory limit | Purpose |
|---|---|---|---|
| MySQL 8.4 | 13307 | 4 GB, 256 MB buffer pool | Rust and Python suites |
| Postgres 17 | 13309 | 2 GB, 512 MB shm | Rust and Python suites |

Ports 13308 and 13310 belong to the crash harness, which is paused. Never use
them for ordinary tests.

## Isolation comes from a database per test, not a container per test

Both suites create one throwaway database per test and drop it at the end:

- Rust: a fresh test gets `dbos_t_<uuid>` created from a migrated template
  (`dbos_tpl_mysql_*` / `dbos_tpl_pg_*`), and `impl Drop for TestDb` sends a
  removal request to a single admin thread. `drop_stale_templates` clears
  template databases left by a killed run at the start of the next one.
- Python: `tests/conftest.py` creates `dbos_t_<uuid>` and drops it in teardown.
  The bridge tests share a small fixed set of names (`dbos_bridge_<worker>`),
  one per xdist worker, so parallel workers do not collide.

Creating from a template is what keeps this fast: the schema is migrated once
per run, then every test gets a byte-for-byte copy. Creating a database is
milliseconds; migrating one is not.

## Why orphans matter

A killed run never reaches `Drop`, so its `dbos_t_*` database survives. Those
accumulate and each one holds a little memory and file handles in the shared
server, which is what eventually produced `timed out creating a test database`
on a server whose own accounting showed far less memory in use.

If a run is interrupted, or after a long day of several agents working in
parallel, reap them:

```sh
.venv/bin/python scripts/reap_test_databases.py --dry-run   # see what would go
.venv/bin/python scripts/reap_test_databases.py             # drop them
```

The reaper only touches `dbos_t_*` databases with no live connection, so it can
run while other agents are testing. It never touches `dbos_test`, the templates,
or the `dbos_bridge_*` databases.

## Keeping the machine alive

Several agents run in parallel worktrees. Each has its own `target/`, and cargo
defaults to one job per core, so five agents meant up to 80 rustc processes and
drove the box into swap. `.cargo/config.toml` now sets `jobs = 2` for every
worktree; raise it with `CARGO_BUILD_JOBS=8` when running the full suite alone.

Also:

- Remove a finished worktree's `target/` when its branch is merged. Each one is
  1 to 5 GB.
- Prefer `-n 4` for pytest and default cargo threads. More parallelism than that
  just adds memory pressure, because the bottleneck is the shared database
  server, not the cores.
- Never `docker compose up` from a worktree. Use the two servers that already
  exist.
