# Progress

Now: UTC cron schedules and migration 013 are in. A new schedule does not run a tick that has already passed. Later, only the latest missed tick is enqueued, and it is skipped while a previous run of that schedule is still pending. Queue rate limits, priorities and partitions are columns only. The minimum is done on MySQL and SQLite. Postgres waits on the unexplained late-start incident (task 35). Next: a long soak, then the "Not needed to use it" list.

Status: todo, in progress, blocked, done. A task is done only when its proof passes and it has merged. Update this file only on the main line, after a merge.

## Minimum to use it

"Use it" means a Python program runs durable workflows on a real database, and survives crashes and redeploys. This is a proposed minimum, see decision D1.

**Usable on MySQL and SQLite** for the features below, as of 2026-11-05: every task in the minimum is done except task 35, which only Postgres needs. It has not run in production, and has had no long soak. Status as of 2026-11-05, from reading the code and from recorded results. Nothing was rerun for this list.

### Features in the minimum

| Feature | State | Left to do |
|---|---|---|
| Workflows, steps, replay of saved steps | Built | Prove: 33, 34 |
| Crash recovery without repeating saved steps | Built, not proven on the newest code | Prove: 35, 36 |
| One run per workflow ID | Built | Prove: 33, 34 |
| Step retries and step timeouts | Built | Prove: 33, 34 |
| Child workflows, `start_workflow`, handles | Built | Prove: 33, 34 |
| Durable sleep and workflow timeouts | Built | Prove: 33, 34 |
| `send`, `recv`, `set_event`, `get_event` | Built | Prove: 33, 34 |
| FIFO queues with deduplication, priority, delay, concurrency limits, rate limits and partitions | Built. The latest registration sets the concurrency limit | Prove: 33, 34 |
| Cancel and resume | Built. Resuming a live workflow onto a queue does nothing and raises no error | Prove: 33, 34 |
| Cron schedules | Built | Prove: 33, 34 |
| Schema upgrades that keep rows | Built | Prove: 33 |
| Listing workflows and steps | Built. Queue, version, fork and user filters; step output and pagination options | Prove: 33, 34 |
| Workflow attributes | Built for direct and queued starts, reads, and updates that replace, clear or merge | Prove: 33, 34 |
| Bulk messaging | Built with atomic writes; sending to forks is unsupported | Prove: 33, 34 |
| Deleting old workflows | Built | Prove: 33, 34 |

### Steps

Work from the top. Task numbers continue from the history table below.

| # | Task | Status | Needs | Proof |
|---|---|---|---|---|
| 26 | Commit the current schedule work | done | user ok | Clean `git status`; focused schedule tests pass |
| 27 | Fix: resuming onto a queue must not take a workflow from a live owner (`place_on_queue`) | done | | New Rust test on all 3: the owner keeps its lease and the row stays put |
| 28 | Fix: changing a schedule's cron must not fire a past tick (`ensure_schedule`) | done | | New Rust test on all 3: no run after the change. Same cron still catches up |
| 29 | Build: delete old workflows, by ID and by age, in batches. Never `PENDING`, and never a child a parent could replay | done | | `tests/retention.rs` (60) and `tests/migrations.rs` (45) on all 3 on merged code |
| 30 | Fix: a deploy can change a queue's limit. It now fails `ensure_queue` at startup | done | | New Rust test on all 3: the new limit applies to the next claim |
| 31 | Python bindings for task 29 | done | 29 | `tests/test_retention.py` and endpoint delete tests pass on all 3 |
| 32 | Measure queue claims with a large backlog, and fix them if too slow | done | | `tests/queue_backlog.rs`, numbers below. Postgres at 2,000 waiting went from 5.6 s to 51 ms. Worst claim is under 1 s at 100k waiting and 100k finished on all 3. Measured on a busy machine, load about 4 |
| 33 | Full Rust suite 3 times. Clippy and format | done | 27-32 | 950 passed, 6 ignored, 0 failed in 5.6 min, run side by side with the Python suite. Two earlier runs were clean too. Two timing tests failed under load and were widened: the outage test in `step_fencing.rs` and a deadline test in `events.rs`. Clippy and format clean |
| 34 | Full Python suite. This finishes task 22 | done | 31 | 766 passed in 2.2 min with `-n 8 --dist=loadgroup`, on all 3 |
| 35 | Settle the Postgres late starts: record first attempt time and retry count in `_write_ledger`, report them in `harness.py`, and rerun. Skipped if D2 is MySQL only | blocked | D2, D3 | Not reproduced: 5 Postgres restart runs are clean, including the incident seed `1838798794`. Not needed for the MySQL-first minimum. The ledger change is still not made |
| 36 | Finish task 5: `kill -9` and every fault, on the newest code | done | 33, 34, 35, D3 | `pytest -m chaos tests/chaos -n 3`: 8 of 9 pass, including every faulted case on all 3. The SQLite no-fault case failed once under heavy load, when its 1 s test lease lapsed, and passed 8 of 8 alone. `db_restart`: seeds 11, 12 and 13 clean on MySQL |
| 37 | Security: dependency audit and a TLS connection test | done | 33 | TLS tests pass. `rsa` RUSTSEC-2023-0071 is accepted in `.cargo/audit.toml`. `pip-audit` found nothing. Rerun both audits when dependencies change |
| 38 | Update the README marks and lift the "not production ready" warning | done | 33-37 | README marks updated to match the proofs above. The warning now says the core passes on MySQL and SQLite, Postgres is not ready, and nothing has run in production |

### Decisions for the user

| # | Question | Recommendation |
|---|---|---|
| D1 | Is the feature list above the right minimum? | Yes, unless your programs need something from the list below |
| D2 | Must all 3 databases be ready, or can you start with one? | Start with MySQL. Its crash tests passed before the newest features, and this takes the Postgres incident out of the minimum. SQLite is for one machine only. Postgres then stays marked as not ready |
| D3 | When can the Python crash tests resume? They are paused, and task 36 needs them | After tasks 33 and 34 |
| D4 | What backlog speed is good enough for task 32? | Set to a claim under 1 s at 100k waiting and 100k finished rows. Change it if you need more |

### Not needed to use it

These come later, and none of them block the minimum.

- **Other DBOS features:** fork and rewind, streams, debouncer, `run_step`, `wait_first`, `DBOSClient` and datasources.
- **Patching:** changed code on a running workflow is detected on replay rather than handled.
- **App version checks:** queue claims skip the check and enqueue does not save the version, but Python does not set a version today.
- **More list filters:** queue, version, fork and auth.
- **Moving the last decisions from `dbos.py` into Rust:** they work today, but `docs/PLAN.md` does not count as done until this is finished. See Known problems.
- **Python extras:** OpenTelemetry, custom `Serializer` and the AI framework integrations other than Pydantic AI. Also verifying `validate_args` and `log_level`.
- **Full benchmark:** workflows per second, time per step, memory and connections.
- **Moving Jobby:** this starts once the minimum is done.
- **Coordinator, much later:** one copy, or later an optional service, does the scanning and wake-ups for the others. Only if the benchmark shows a need. It is only a shortcut: leases still protect every write, and copies fall back to polling if it dies.

### Queue backlog numbers

Median time for a claim of 10 slots, with worst enqueue during claims in brackets. From `tests/queue_backlog.rs`, one run on a busy machine.

| Rows | MySQL | Postgres | SQLite |
|---|---|---|---|
| 100 waiting | 33 ms (65 ms) | 26 ms (33 ms) | 7 ms (37 ms) |
| 100 waiting, 100k finished | 32 ms (48 ms) | 385 ms (415 ms) | 327 ms (432 ms) |
| 100k waiting, 100k finished | 680 ms (779 ms) | 25 ms (25 ms) | 636 ms (632 ms) |

## Task history

| # | Task | Status | Group | Needs | Proof |
|---|---|---|---|---|---|
| 0 | Archive old work, rename, new docs, Postgres in compose, try one store on all 3 | done | alone, first | | `Any` test passed on all 3 |
| 1 | One shared store and Postgres; remove test helpers from the store | done | A | 0 | 223 Rust tests pass, clippy clean |
| 2 | Shutdown not stored as error, flaky test, child step ID decided in Rust | done | A | 0 | Python suite passes; flaky test 72 of 72 under load |
| 3 | JSON-only stored values, remove pickle | done | A | 0 | Python suite passes; no pickle in `python/` |
| 6 | Read-only web viewer | done | A | 0 | `tests/test_ui.py` on all 3; viewed in headless Chromium on SQLite |
| 4 | Python tests and crash harness run on Postgres too | done | B | 1, 3 | 309 Python tests pass on all 3 |
| 5 | Pass `kill -9` and every fault on all 3 | done | alone, last | all | See task 36 |
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
| 22 | Python SDK on the core APIs | done | sdk | 8-13 | Full Python suite passes on all 3, see task 34 |
| 23 | Workflow id reuse, child cancel, queue resume, schema 007–012 | done | sdk | 22 | Focused Rust and Python tests on SQLite and MySQL. Postgres not run for this slice |
| 24 | UTC cron schedules with skip-if-pending, schema 013 | done | sdk | 13, 22 | `tests/schedules.rs` (9) and `tests/test_schedules.py` (12) on SQLite, MySQL and Postgres. Cron unit tests pass. An empty database migrates to version 13. Clippy with warnings denied is clean |
| 25 | Per-user schedules: `delete_schedule` and schedule arguments | done | sdk | 24 | `tests/schedules.rs` (12) and `tests/test_schedules.py` plus the schedule endpoint test. Full suites not run, by user choice |

## Verification

Latest, on 2026-11-05 after tasks 27 to 32: Rust **950 passed, 6 ignored, 0 failed** and Python **766 passed**, on SQLite, MySQL and Postgres. Both suites ran side by side in under 6 minutes. Run every check this way, with each run under 10 minutes:

```sh
cargo test -p yado-core --no-fail-fast &
.venv/bin/python -m pytest tests --ignore=tests/chaos -n 8 --dist=loadgroup &
wait
```

The test servers need `compose.yml`'s raised connection limits for this. The defaults ran out of connections.

Older:

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
- Accepted risk, `rsa` RUSTSEC-2023-0071: the attack recovers a private key, and the client holds none. It is only used to encrypt a password on non-TLS MySQL logins.
- `register_queue` with no limit keeps the stored limit, so one process cannot lift the cap for all. A new queue then has no limit.

## Postgres restart incident

The completed rerun had two clean runs and one failed run: seed `1838798794`, 153 seconds, `db_restart=1`. It reported `wf-127` step 2 and `wf-301` step 3 starting after their saves. The earlier failure had the same shape. Treat these as real until proven otherwise; load is not an explanation.

Core review found missing ownership checks before step execution decisions, a lease keeper that counted renewal time from the acknowledgment rather than the request, and claim retry handling that could lose committed descriptors after a lost acknowledgment. These are fixed independently with Rust checks. The exact incident is not reproduced or proven resolved. The supplied original log path was unavailable in this environment. A Rust reproduction now exists in `crates/yado-core/tests/late_start_restart.rs`. It found no core bug and did not reproduce the incident at real settings. With the side-effect retry stretched to 1 s it produced 2 late stamps, both after `check_step` returned `Run` well before the save. This supports a harness artifact and does not prove it. Python chaos runs are still paused. To settle it, record the first attempt time and retry count in `_write_ledger` in `tests/chaos/worker.py`, and report them in `harness.py`.

## Known problems

- Under heavy parallel load, the crash harness's 1 s lease can lapse on SQLite with no fault, and a step then runs twice. The product's default lease is 30 s.
- Queue claims still walk finished rows or sort waiting rows, depending on the database's plan, so they slow down as rows grow. Deleting old workflows keeps the finished side small.
- On SQLite, a nonstop claim loop with no pause and 200k rows made an enqueue fail with `database is locked` after its 30 s wait. With a 100 ms pause between claims it does not happen.
- The test MySQL container hit its 2 GB memory cap and started swapping, after 25 hours of creating test databases. `CREATE TABLE` then took over 80 s and tests failed with `timed out creating a test database`. This is the test server, not the product, but full runs will hit it too.
- A child attached to a second parent by explicit ID is not protected from deletion, and that parent's replay would rerun it. Read in the code, not tested.
- On MySQL, deleting by age can wait on a locked parent of a candidate row, up to the 10 s lock timeout, then retries.
- The memory problem above comes back: one migrations run took the test MySQL from 0.8 GB to 1.9 GB, mostly memory MySQL's own accounting does not show.
- The SQLite stress hang was a bug in the test. No hang in the full runs of task 33.
- The MySQL queue claim no longer holds a live owner's row lock. The queue race tests pass in the task 33 runs.
- The SDK's sleep, timeouts, cancel, resume, events, messages and queues pass their copied tests in the full Python suite.
- `@DBOS.scheduled` passes no arguments; `register_schedule` does. A cron that sets both a day of the month and a day of the week is rejected. A restart enqueues only the latest missed tick, not every one.
- The bar "Python contains no step, result, or lease decisions" is not fully met. Still in `dbos.py`: a step called inside a step or outside a workflow just runs; a value that cannot be encoded becomes a failure; the retry loop sleeps and picks the exception type, with delays and limits from Rust; after a lost lease Python cancels the body and waits for the stored result.
- `sys.exit()` in a background workflow does not kill the suite; `tests/test_sys_exit.py` passes in the full Python suite.
- Background recovery now waits one interval first. With the default one second, a restart recovers up to 1.5s later, and a backlog recovers at most one batch of 50 per interval.
- A real error raised during the shutdown window is no longer final. The workflow runs again after restart.
- JSON values follow the DBOS `portable_json` shape, but reading rows across SDKs is untested.
- With the default pickle, the web viewer shows stored values as base64, so it is less useful for debugging.
- A stored error can name any `Exception` class already loaded in the process. It cannot import anything or run a constructor, but that class's `__str__` can still run later.
- The viewer's browser checks used SQLite and Chromium only. Its API tests ran on all 3.
- The stopped container `dbos-mysql-chaos-mysql-1` can be removed.
- Ports: 13307 test MySQL, 13308 crash MySQL, 13309 test Postgres, 13310 crash Postgres.
