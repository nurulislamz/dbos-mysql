# Copied test manifest

Tests in this directory come from [dbos-transact-py](https://github.com/dbos-inc/dbos-transact-py) and [dbos-transact-ts](https://github.com/dbos-inc/dbos-transact-ts), under the MIT licence in `LICENSE-DBOS`.

Results come from 3 full runs against SQLite and MySQL 8.4. `pass/fail` in a result column means the test gave both results across those runs. A `failing` status means the test is correct and the SDK does not support that behaviour yet; the reason names what is missing.

- **passing**: copied unchanged, and passes on both backends.
- **adapted**: copied, with the smallest change needed for 0.1 scope.
- **rewritten**: written again in pytest from the upstream scenario.
- **dropped**: not carried over.

Counts: adapted: 18, dropped: 6, passing: 16, rewritten: 11. Every test that was not dropped passes on both backends.

| Test | Source | Commit | Status | SQLite | MySQL | Reason |
|---|---|---|---|---|---|---|
| `test_async_completed_replay_no_hook_rerun` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_async_step` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_async_workflow` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_child_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_child_workflow_assigned_id` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_child_workflow_async` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_child_workflow_recovery` | dbos-transact-ts `tests/recovery.test.ts` child-workflow-recovery | 306b732 | rewritten | pass | pass | Rewritten in pytest. |
| `test_custom_names` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | Queue removed: `enqueue_workflow` replaced with `start_workflow`. |
| `test_dead_letter_queue` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | Internal status write replaced with `Database.expire_lease`. Resume section removed: resume is out of scope for 0.1. |
| `test_dead_letter_queue_scenario` | dbos-transact-ts `tests/recovery.test.ts` dead-letter-queue | 306b732 | rewritten | pass | pass | Rewritten in pytest. Resume section removed: out of scope for 0.1. |
| `test_duplicate_registration` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_exception_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_local_recovery` | dbos-transact-ts `tests/recovery.test.ts` local-recovery | 306b732 | rewritten | pass | pass | Rewritten in pytest. Authenticated user removed: out of scope for 0.1. |
| `test_nested_steps` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_nondeterministic_workflow` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | `fork_workflow` replaced with lease expiry and recovery: fork is out of scope for 0.1. Same assertion. |
| `test_nonserializable_return` | dbos-transact-py `tests/test_failures.py` | 236142b | passing | pass | pass |  |
| `test_parallel_steps_replay_from_recorded_results` | dbos-transact-py `tests/test_async.py` test_asyncio_wait_all_completed | 236142b | rewritten | pass | pass | Uses `asyncio.gather` and recovery in place of `DBOS.asyncio_wait` and fork. |
| `test_recorded_step_output` | dbos-transact-py `tests/test_async.py` test_asyncio_wait | 236142b | rewritten | pass | pass | Keeps the recorded step assertions from a dropped test. |
| `test_recovery_attempts` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | Process restarts replaced with lease expiry and recovery, because crash detection uses leases. Queue and app version removed. |
| `test_recovery_during_retries` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | Lease expired before recovery, because a live lease is not recoverable by design. Blocking event released at teardown. |
| `test_recovery_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | Internal SQL replaced with `Database.expire_lease`. |
| `test_recovery_workflow_step` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | Internal SQL replaced with `Database.expire_lease`. |
| `test_reject_name_mismatch_throws_in_use` | dbos-transact-ts `tests/oaoo.test.ts` reject-name-mismatch-throws-in-use | 306b732 | rewritten | pass | pass | Rewritten in pytest. Reuse policy is out of scope, so this checks the default conflict error, which still names the recorded workflow. |
| `test_retrieve_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | `dbos.sleep` replaced with `time.sleep`: durable sleep is out of scope for 0.1. |
| `test_retrieve_workflow_async` | dbos-transact-py `tests/test_async.py` | 236142b | adapted | pass | pass | `DBOS.sleep_async` replaced with `asyncio.sleep`: durable sleep is out of scope for 0.1. |
| `test_simple_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_simple_workflow_attempts_counter` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | Internal table read replaced with `DBOS.get_workflow_status`; same assertions. |
| `test_stale_owner_cannot_write_the_outcome` | dbos-transact-ts `tests/timinghole.test.ts` stale-owner-cannot-write-the-outcome | 306b732 | rewritten | pass | pass | Rewritten in pytest. A new owner finishes the workflow instead of `cancelWorkflow`, which is out of scope. |
| `test_start_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | Internal context check replaced with the equivalent `DBOS.workflow_id is None`. |
| `test_start_workflow_async` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_step_conc` | dbos-transact-ts `tests/timinghole.test.ts` step-conc | 306b732 | rewritten | pass | pass | Rewritten in pytest. Redispatch replaced with lease expiry and recovery. |
| `test_step_oaoo` | dbos-transact-ts `tests/oaoo.test.ts` step-oaoo | 306b732 | rewritten | pass | pass | Rewritten in pytest. |
| `test_step_recorded_twice_by_the_owner_is_nondeterminism` | dbos-transact-ts `tests/timinghole.test.ts` step-recorded-twice-by-the-owner-is-nondeterminism | 306b732 | rewritten | pass | pass | Rewritten in pytest, with `Database.plant_step`. |
| `test_step_retries` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | Queue removed: `enqueue_workflow` replaced with `start_workflow`; queue cleanup assertion removed. |
| `test_step_retries_no_final_sleep` | dbos-transact-py `tests/test_failures.py` | 236142b | passing | pass | pass |  |
| `test_step_retries_no_final_sleep_async` | dbos-transact-py `tests/test_failures.py` | 236142b | passing | pass | pass |  |
| `test_step_status` | dbos-transact-py `tests/test_failures.py` | 236142b | passing | pass | pass |  |
| `test_step_without_dbos` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_unawaited_workflow` | dbos-transact-py `tests/test_async.py` | 236142b | adapted | pass | pass | Queue removed: `enqueue_workflow` replaced with `start_workflow`. |
| `test_unawaited_workflow_exception` | dbos-transact-py `tests/test_async.py` | 236142b | adapted | pass | pass | Queue removed: `enqueue_workflow` replaced with `start_workflow`. |
| `test_workflow_error_serialization` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | `DBOSClient` check removed: the external client is out of scope for 0.1. |
| `test_workflow_oaoo` | dbos-transact-ts `tests/oaoo.test.ts` workflow-oaoo | 306b732 | rewritten | pass | pass | Rewritten in pytest. |
| `test_workflow_recovery_async` | dbos-transact-py `tests/test_async.py` | 236142b | adapted | pass | pass | Internal SQL replaced with `Database.expire_lease`. |
| `test_workflow_returns_none` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | Internal SQL replaced with `Database.expire_lease`. |
| `test_workflow_with_task_cancellation` | dbos-transact-py `tests/test_async.py` | 236142b | adapted | pass | pass | `DBOS.sleep_async` replaced with `asyncio.sleep`: durable sleep is out of scope for 0.1. |
| `test_main_loop` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Tests the event loop an enqueued workflow runs on. Queues are out of scope for 0.1. Loop choice is still checked by `test_start_workflow_async` and `test_workflow_recovery_async`. |
| `test_asyncio_wait` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork, which are not in the 0.1 API. Its step assertions moved to `test_recorded_step_output`. |
| `test_asyncio_wait_all_completed` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork. Replaced by `test_parallel_steps_replay_from_recorded_results`. |
| `test_asyncio_wait_first_exception` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork. |
| `test_asyncio_wait_timeout` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork. |
| `test_error_serialization` | dbos-transact-py `tests/test_failures.py` | 236142b | dropped | not run | not run | Tests upstream's pickle serializer. This project stores JSON and never unpickles. |
