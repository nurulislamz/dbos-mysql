# Continue work on dbos-mysql

Repo: `~/dbos-mysql`, branch `main`. The goal is to port DBOS Transact from TypeScript to Rust. Read `docs/PLAN.md` first.

Upstream reference: `dbos-transact-ts`. A read-only tree may still be at `/tmp/claude-1000/-home-nurul-jobby-backend/2651a31f-1ede-4547-af2b-ef523ad88f07/scratchpad/ts`. If it's gone, clone `https://github.com/dbos-inc/dbos-transact-ts` outside this repo. Run that TypeScript with Bun. Do not use `dbos-transact-py` as the source.

Work on `main`. Do not `git fetch`. Do not push.

Use simple English and keep replies short. Don't commit or push unless I ask.

## Decisions already made
- **Goal:** port `dbos-transact-ts` to Rust. TypeScript is the original DBOS Transact. Stop adding Python features. The Python package is paused.
- **Approach:** port the TypeScript behaviour into Rust, feature by feature. Read the TypeScript, write normal Rust, and record any deliberate difference in `docs/PLAN.md`.
- **Tests:** run the original TypeScript in parallel with this Rust code. Same scenario, both implementations, compare the outcomes. Do not rewrite those checks as Python tests. Bun is the runtime for the TypeScript.
- **Crash detection:** keep leases (`claim_after`, `lease_token`), not executor IDs.
- **Stored format:** follow the TypeScript serializers. The default is `js_superjson`. The other built-in is `portable_json`. Python pickle is not the format. Replace leftover `py_pickle` code when serialization is touched. Do not extend pickle.
- **Schema:** keep the DBOS table names already in the tree (`workflow_status`, `operation_outputs`, and the renamed columns). A `serialization` column and millisecond times.
- **Not yet decided:** adding Postgres. Do not add it.

## Current state
The schema rename and paused Python pickle experiment are on `main`. Serialization is not yet the TypeScript formats. The next work is the TypeScript-to-Rust port, starting with stored format, then other engine behaviour, with Bun comparison tests.

## Known problems
1. **Already known, not a new schema-rename bug.** The SQLite chaos test `test_workflows_survive_every_fault` with seed 2 fails on this tree and on `HEAD` the same way. `SIGTERM` calls `sys.exit`, `atexit` runs `DBOS.destroy()` while a workflow is still running, and that shutdown error is stored as a final `ERROR`. Recovery does not retry `ERROR`.
2. **Already flaky at `HEAD`:** `test_cancelling_an_inline_workflow_hands_it_over_without_using_an_attempt` failed 3 of 25 runs at `HEAD` and 2 of 25 on this code. It asserts `recovery_attempts == 0` straight after a cancel, so it may be a race with the lease release.

## Work left, in order
1. Replace pickle with `js_superjson` (default) and `portable_json` in the Rust store. Do not extend pickle.
2. Run TypeScript and Rust on the same scenarios with Bun, and compare outcomes.
3. Port remaining TypeScript behaviour into Rust, feature by feature. Record deliberate differences in `docs/PLAN.md`.
4. Keep this file and `docs/PLAN.md` matched to the goal above.

## How to run things
```sh
source .venv/bin/activate && maturin develop
cargo test -p dbos-mysql-core --no-fail-fast
pytest -q tests --ignore=tests/chaos
pytest -q -m chaos tests/chaos
```
Rust and Python tests use MySQL on port 13307 (`compose.yml`). TypeScript comparison tests use Bun against the `dbos-transact-ts` tree.

When renaming SQL, check any query built in pieces. `reclaim` has an `ORDER BY` fragment that was missed once.
