# Progress

Now: tasks 1 and 2 still running. Next: task 4 once task 1 merges.

Status: todo, in progress, blocked, done. A task is done only when its proof passes and it has merged into `rewrite`. Only update this file on `rewrite`, after a merge.

| # | Task | Status | Group | Needs | Branch | Proof |
|---|---|---|---|---|---|---|
| 0 | Archive old work, rename, new docs, Postgres in compose, try one store on all 3 | done | alone, first | | rewrite | 145 Rust tests pass after the rename; `Any` test passed on all 3 |
| 1 | One shared store and Postgres; remove test helpers from the store | in progress | A | 0 | store-shared | `cargo test -p dwos-core` on all 3 |
| 2 | Shutdown not stored as error, flaky test, child step ID decided in Rust | in progress | A | 0 | shutdown-fixes | `pytest tests --ignore=tests/chaos`, flaky test passes 25 of 25 |
| 3 | JSON-only stored values, remove pickle | done | A | 0 | json-only | `pytest tests --ignore=tests/chaos`, no `pickle` left in `python/` |
| 6 | Read-only web viewer | done | A | 0 | web-viewer | `pytest tests/test_ui.py`, and viewed in a browser |
| 4 | Python tests and crash harness run on Postgres too | todo | B | 1, 3 | | `pytest tests --ignore=tests/chaos` on all 3 |
| 5 | Pass `kill -9` and every fault on all 3 | todo | alone, last | all | | `pytest -m chaos tests/chaos` on all 3 |

## Databases

| Database | Rust tests | Python tests | Crash tests |
|---|---|---|---|
| SQLite | pass | pass | seed 2 known failure (shutdown stored as error) |
| MySQL 8.4 | pass | pass | not run on this branch |
| Postgres 17 | not started | not started | not started |

## Baseline

`main` at 53c228c on 2026-11-04: Rust 145 passed, 1 ignored. Python 140 passed. Crash tests not run.

## Decisions

- Keep this code and reshape it. Do not restart from scratch.
- Do not fork `dbos-transact-rust`. Reuse its design, schema and tests.
- One store through `sqlx` `Any`. See `docs/PLAN.md`.
- Leases for crash detection. Stored values are `portable_json`.
- Old work and the old README and plan are on the local branch `archive/pre-rewrite`.

## Known problems

- `test_dead_letter_queue_scenario[mysql]` failed once in the full suite on `rewrite` at afbf427 while other tasks loaded MySQL, then passed 48 of 48 alone and under load. Probable cause, not proven: the first background recovery tick claims the workflow before the test's manual recovery. Only one caller can win a claim, so this looks like a test race. Watch for it in task 5.
- The viewer can write to a SQLite file: connecting sets WAL mode in the Rust core. A read-only connection would fix it.
- The viewer is not tested on Postgres yet (task 4), and browser checks used SQLite and Chromium only.
- Stored values follow the DBOS `portable_json` shape, but the `serialization` column is never read and reading rows across SDKs is untested.
- Rows written with `py_pickle` by older builds fail with a clear error. Nothing was released, so there is no migration.
- A stored error can name any `Exception` class already loaded in the process. It cannot import anything or run a constructor, but that class's `__str__` can still run later.
- `test_cancelling_an_inline_workflow_hands_it_over_without_using_an_attempt` is flaky: 3 of 25 runs failed earlier. Task 2.
- Stale worktrees remain in `.worktrees/` (`engine`, `tests`, `ts-port`). Not removed. `ts-port` holds `main`.
- Ports: 13307 is test MySQL, 13308 is the crash harness MySQL, 13309 is test Postgres.
