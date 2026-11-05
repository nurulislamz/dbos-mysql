# Progress

Now: core hardening is merged on `main`: expired-lease refusal, keeper timing, numbered schema migrations and queue race fixes. The Python SDK was restarted on 2026-11-05. See `.worktrees/HANDOFF.md` for the next steps. Two things block readiness: the Postgres late-start incident is still unexplained, and `stress_owners_stay_within_the_limit_and_every_workflow_ends::sqlite` hung once in 3 suite runs, past its own 90 s limit.

Status: todo, in progress, blocked, done. A task is done only when its proof passes and it has merged. Update this file only on the main line, after a merge.

| # | Task | Status | Group | Needs | Proof |
|---|---|---|---|---|---|
| 0 | Archive old work, rename, new docs, Postgres in compose, try one store on all 3 | done | alone, first | | `Any` test passed on all 3 |
| 1 | One shared store and Postgres; remove test helpers from the store | done | A | 0 | 223 Rust tests pass, clippy clean |
| 2 | Shutdown not stored as error, flaky test, child step ID decided in Rust | done | A | 0 | Python suite passes; flaky test 72 of 72 under load |
| 3 | JSON-only stored values, remove pickle | done | A | 0 | Python suite passes; no pickle in `python/` |
| 6 | Read-only web viewer | done | A | 0 | `tests/test_ui.py` on all 3; viewed in headless Chromium on SQLite |
| 4 | Python tests and crash harness run on Postgres too | done | B | 1, 3 | 309 Python tests pass on all 3 |
| 5 | Pass `kill -9` and every fault on all 3 | blocked | alone, last | all | Earlier runs passed; latest Postgres rerun has a real unresolved late-start report (below). Further Python crash tests are paused by the user |
| 7 | Pickle by default, with `"serializer": "json"` to opt out | done | alone | 3 | `tests/test_pickle_option.py` on all 3; its gate tests fail when the gate is removed; crash tests run both formats |
| 8 | Rust engine/store feature split and explicit outcomes | done | core | 1 | Merged `e1e6151`; Rust core checks pass |
| 9 | Durable sleep and database-clock workflow deadlines | done | core | 8 | Merged `5f91756`; cross-database Rust suite and three timeout integration checks pass |
| 10 | Workflow cancel/resume/list and owned step reads | done | core | 8 | Merged `1f16adb`; replaced owners cannot start steps/children; resume preserves live owners |
| 11 | Durable events and FIFO messages | done | core | 8 | Merged `44f3829`; Rust tests pass on all three databases |
| 12 | NUL validation, unreadable-pickle warning, read-only connections | done | core | 8 | Merged `7b753fc`, `5ee6a3e`; writes rejected by read-only sessions on all three databases |
| 13 | Durable FIFO queues and global admission | done | core | 9, 10 | Merged `5ff4d0c`, `8c5e7cc`; 21 cross-database queue checks and one committed-request replay test pass; independent code review found no blocker |
| 14 | Lease acknowledgment timing and claim retry safety | done | core | 8 | Merged `74629e6`, `bd31d2b`; slow-ack regression fails old timing; discarded claim-response retry tests pass without spending attempts |
| 15 | Recovered message/event wait deadline semantics | done | core | 11 | Merged `f7405f7`; cross-database Rust event checks pass; event revisions preserve values that arrived before the deadline |
| 16 | Reject invalid step names before executing bodies | done | core | 10 | Merged `38edcdb`; NUL/overlong names reject before Run on all three databases |
| 17 | Zero retry delay under overflowing backoff | done | core | 8 | Merged `e058ec3`; focused Rust regression passes |
| 18 | Refuse and revoke expired leases at execution decisions; keeper reports loss before expiry | done | core | 10 | `tests/step_fencing.rs` (90 tests) on all three databases |
| 19 | Numbered schema migrations that keep existing rows | done | core | 8 | `tests/migrations.rs` (45 tests) from real historic schemas on all three databases |
| 20 | Queue admission races: MySQL recovery mid-scan, oversized timeout | done | core | 13 | `tests/queue_races.rs` (74 tests). The stress test hung once on SQLite, see Known problems |
| 21 | Rust-only late-start reproduction | blocked | core | 8 | `tests/late_start_restart.rs` (ignored by default): 0 late starts in 45 restarts at real settings, cause unproven |
| 22 | Python SDK on the core APIs | in progress | sdk | 8-13 | Code written. Existing Python tests only; the new features have never run. No smoke tests yet |

## Verification

On 2026-11-05 the merged core passed **669 tests, 2 ignored, zero failures** in 2 of 3 full runs across SQLite, MySQL and Postgres. In the third run the SQLite stress test hung and was killed. The other 267 tests that had finished in that run passed. Clippy with warnings denied and formatting pass.

Python and crash results below are historical first-version results. They do not validate the new Rust APIs.

| Database | Rust tests | Python tests | Crash tests |
|---|---|---|---|
| SQLite | pass | pass | pass: kill, pause, term, crash window. No database restart, because SQLite has no server |
| MySQL 8.4 | pass | pass | pass: kill, pause, term, crash window, database restart (3 seeds) |
| Postgres 17 | pass | historical pass | unresolved: latest restart rerun was 2 clean, 1 failed |

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
- All work lives on `main`. Old branches and worktrees were deleted on 2026-11-04, and again on 2026-11-05.
- SDK code is copied from DBOS, not rewritten. Python tests of calls into Rust only check that the call connects and succeeds. Core rules are tested only in Rust.
- The user expanded core scope to durable sleep, workflow management, events/messages and queues, then paused SDK work and restricted checks to Rust. MessagePack remains deferred by the user.
- The five feature worktrees were closed after merging core changes. Branches remain. Deferred SDK/binding/UI edits were archived under `.worktrees/deferred-sdk-20261104/`; each archive was restored and byte-compared before removal.

## Postgres restart incident

The completed rerun had two clean runs and one failed run: seed `1838798794`, 153 seconds, `db_restart=1`. It reported `wf-127` step 2 and `wf-301` step 3 starting after their saves. The earlier failure had the same shape. Treat these as real until proven otherwise; load is not an explanation.

Core review found missing ownership checks before step execution decisions, a lease keeper that counted renewal time from the acknowledgment rather than the request, and claim retry handling that could lose committed descriptors after a lost acknowledgment. These are fixed independently with Rust checks. The exact incident is not reproduced or proven resolved. The supplied original log path was unavailable in this environment. A Rust reproduction now exists in `crates/dwos-core/tests/late_start_restart.rs`. It found no core bug and did not reproduce the incident at real settings. With the side-effect retry stretched to 1 s it produced 2 late stamps, both after `check_step` returned `Run` well before the save. This supports a harness artifact and does not prove it. Python chaos runs are still paused. To settle it, record the first attempt time and retry count in `_write_ledger` in `tests/chaos/worker.py`, and report them in `harness.py`.

## Known problems

- `stress_owners_stay_within_the_limit_and_every_workflow_ends::sqlite` hung in 1 of 3 full suite runs. It ran over 11 minutes, past its 90 s limit, with every thread idle. The cause is unknown and may be in the test or in the product.
- On MySQL, a queue claim can hold a live owner's row lock. `finish` waited about 10 s in one run. No over-admission was seen, but a healthy short lease could expire during the wait. The statement that takes the lock is not known. See the `shortcut:` comment in `tests/queue_races.rs`.
- The SDK's sleep, timeouts, cancel, resume, events, messages and queues have never run. The `sys.exit()` fix is also unproven.
- The bar "Python contains no step, result, or lease decisions" is not fully met. Still in `dbos.py`: a step called inside a step or outside a workflow just runs; a value that cannot be encoded becomes a failure; the retry loop sleeps and picks the exception type, with delays and limits from Rust; after a lost lease Python cancels the body and waits for the stored result.
- `sys.exit()` inside a background workflow probably kills the `dbos-workflows` thread. Not tested or fixed.
- Background recovery now waits one interval first. With the default one second, a restart recovers up to 1.5s later, and a backlog recovers at most one batch of 50 per interval.
- A real error raised during the shutdown window is no longer final. The workflow runs again after restart.
- JSON values follow the DBOS `portable_json` shape, but reading rows across SDKs is untested.
- With the default pickle, the web viewer shows stored values as base64, so it is less useful for debugging.
- A stored error can name any `Exception` class already loaded in the process. It cannot import anything or run a constructor, but that class's `__str__` can still run later.
- The viewer's browser checks used SQLite and Chromium only. Its API tests ran on all 3.
- The stopped container `dbos-mysql-chaos-mysql-1` can be removed.
- Ports: 13307 test MySQL, 13308 crash MySQL, 13309 test Postgres, 13310 crash Postgres.
