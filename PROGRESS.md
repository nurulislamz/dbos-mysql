# Progress

Now: first version complete and on `main`. Next: not decided. Queues and durable sleep are the next features in `docs/PLAN.md`.

Status: todo, in progress, blocked, done. A task is done only when its proof passes and it has merged. Update this file only on the main line, after a merge.

| # | Task | Status | Group | Needs | Proof |
|---|---|---|---|---|---|
| 0 | Archive old work, rename, new docs, Postgres in compose, try one store on all 3 | done | alone, first | | `Any` test passed on all 3 |
| 1 | One shared store and Postgres; remove test helpers from the store | done | A | 0 | 223 Rust tests pass, clippy clean |
| 2 | Shutdown not stored as error, flaky test, child step ID decided in Rust | done | A | 0 | Python suite passes; flaky test 72 of 72 under load |
| 3 | JSON-only stored values, remove pickle | done | A | 0 | Python suite passes; no pickle in `python/` |
| 6 | Read-only web viewer | done | A | 0 | `tests/test_ui.py` on all 3; viewed in headless Chromium on SQLite |
| 4 | Python tests and crash harness run on Postgres too | done | B | 1, 3 | 309 Python tests pass on all 3 |
| 5 | Pass `kill -9` and every fault on all 3 | done | alone, last | all | 6 crash tests clean, plus 6 database restart runs clean |
| 7 | Pickle by default, with `"serializer": "json"` to opt out | done | alone | 3 | `tests/test_pickle_option.py` on all 3; its gate tests fail when the gate is removed; crash tests run both formats |

## Databases

| Database | Rust tests | Python tests | Crash tests |
|---|---|---|---|
| SQLite | pass | pass | pass: kill, pause, term, crash window. No database restart, because SQLite has no server |
| MySQL 8.4 | pass | pass | pass: kill, pause, term, crash window, database restart (3 seeds) |
| Postgres 17 | pass | pass | pass: kill, pause, term, crash window, database restart (3 seeds) |

## Running the crash tests

```sh
DBOS_TEST_DB_PORT=13308 DBOS_TEST_PG_PORT=13310 docker compose -p rust-dwos-chaos up -d --wait
pytest -m chaos tests/chaos
python -m tests.chaos.run --backend postgres --seed 11 --workflows 600 --fault-seconds 20 --faults db_restart
```

The fixed seed in `test_chaos.py` never picks `db_restart`, so that fault needs the `run` command above.

## Baseline before this work

`main` at 53c228c on 2026-11-04: Rust 145 passed, 1 ignored. Python 140 passed. Crash tests not run.

## Decisions

- Keep this code and reshape it. Do not restart from scratch.
- Do not fork `dbos-transact-rust`. Reuse its design, schema and tests.
- One store through `sqlx` `Any`. See `docs/PLAN.md`.
- Leases for crash detection.
- Stored values use pickle by default, because only admins and the program write to the database. JSON is opt-out through config. The config decides, never a row's tag.
- All work lives on `main`. Old branches and worktrees were deleted on 2026-11-04.

## Known problems

- The bar "Python contains no step, result, or lease decisions" is not fully met. Still in `dbos.py`: a step called inside a step or outside a workflow just runs; a value that cannot be encoded becomes a failure; the retry loop sleeps and picks the exception type, with delays and limits from Rust; after a lost lease Python cancels the body and waits for the stored result.
- `sys.exit()` inside a background workflow probably kills the `dbos-workflows` thread. Not tested or fixed.
- Postgres rejects NUL characters in text, so a workflow ID or name containing one fails only there. Not handled.
- Background recovery now waits one interval first. With the default one second, a restart recovers up to 1.5s later, and a backlog recovers at most one batch of 50 per interval.
- A real error raised during the shutdown window is no longer final. The workflow runs again after restart.
- JSON values follow the DBOS `portable_json` shape, but reading rows across SDKs is untested.
- If every running copy is set to JSON, pickled workflows wait unclaimed with no warning.
- With the default pickle, the web viewer shows stored values as base64, so it is less useful for debugging.
- MySQL counters became `BIGINT`. Nothing was released, so there is no migration, and old test databases need recreating.
- A stored error can name any `Exception` class already loaded in the process. It cannot import anything or run a constructor, but that class's `__str__` can still run later.
- The viewer can write to a SQLite file: connecting sets WAL mode in the Rust core. A read-only connection would fix it.
- The viewer's browser checks used SQLite and Chromium only. Its API tests ran on all 3.
- The stopped container `dbos-mysql-chaos-mysql-1` can be removed.
- Ports: 13307 test MySQL, 13308 crash MySQL, 13309 test Postgres, 13310 crash Postgres.
