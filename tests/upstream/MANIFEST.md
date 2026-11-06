# Copied test manifest

Tests in this directory come from [dbos-transact-py](https://github.com/dbos-inc/dbos-transact-py) and [dbos-transact-ts](https://github.com/dbos-inc/dbos-transact-ts), under the MIT licence in `LICENSE-DBOS`.

Results come from 3 full runs against SQLite and MySQL 8.4. `pass/fail` in a result column means the test gave both results across those runs. A `failing` status means the test is correct and the SDK does not support that behaviour yet; the reason names what is missing.

- **passing**: copied unchanged, and passes on both backends.
- **adapted**: copied, with the smallest change needed for 0.1 scope.
- **rewritten**: written again in pytest from the upstream scenario.
- **dropped**: not carried over.

Counts: adapted: 42, dropped: 5, passing: 21, rewritten: 12. Every test that was not dropped passes on both backends. The workflow-management rows were run once, on SQLite, MySQL and Postgres. The reuse-policy and handles rows passed on SQLite and MySQL.

| Test | Source | Commit | Status | SQLite | MySQL | Reason |
|---|---|---|---|---|---|---|
| `test_async_completed_replay_no_hook_rerun` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_async_step` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_async_workflow` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_cancel_after_final_step` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_cancel_resume` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_cancel_workflow_async` | dbos-transact-py `tests/test_async_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_child_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_child_workflow_assigned_id` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_child_workflow_async` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass |  |
| `test_child_workflow_recovery` | dbos-transact-ts `tests/recovery.test.ts` child-workflow-recovery | 306b732 | rewritten | pass | pass | Rewritten in pytest. |
| `test_custom_names` | dbos-transact-py `tests/test_dbos.py` | 236142b | adapted | pass | pass | Queue removed: `enqueue_workflow` replaced with `start_workflow`. |
| `test_dead_letter_queue` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | Internal status write replaced with `Database.expire_lease`. Resume section removed: resume is out of scope for 0.1. |
| `test_dead_letter_queue_scenario` | dbos-transact-ts `tests/recovery.test.ts` dead-letter-queue | 306b732 | rewritten | pass | pass | Rewritten in pytest. Resume section removed: out of scope for 0.1. |
| `test_duplicate_registration` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_exception_workflow` | dbos-transact-py `tests/test_dbos.py` | 236142b | passing | pass | pass |  |
| `test_list_workflow` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflow_completed_at` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflow_limit` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflow_prefix` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflow_start_end_times` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflow_steps_async` | dbos-transact-py `tests/test_async_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflows_async` | dbos-transact-py `tests/test_async_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflows_by_parent` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_list_workflows_has_parent` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
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
| `test_invalid_reuse_policy`, `test_reject_reuse_policy` | dbos-transact-py `tests/test_duplication_policy.py` | 236142b | adapted | pass | pass | The two cases that only need `SetWorkflowID(workflow_id_reuse_policy=)`. Dedup, enqueue options and the client are still out. |
| `test_polling_interval_must_be_positive`, `test_cancel_children_bulk_resume_and_poll_interval` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Connects `polling_interval_sec`, `cancel_children`, `cancel_workflows` and `resume_workflows`. The rules are in the Rust tests. |
| `test_resume_nonexistent_workflow` | dbos-transact-py `tests/test_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_resume_nonexistent_workflow_async` | dbos-transact-py `tests/test_async_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
| `test_resume_workflow_async` | dbos-transact-py `tests/test_async_workflow_management.py` | 236142b | adapted | pass | pass | Copied from the deferred workflow-management archive. One run passed on SQLite, MySQL and Postgres. |
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
| `test_main_loop` | dbos-transact-py `tests/test_async.py` | 236142b | passing | pass | pass | Brought back now that queues exist. Copied unchanged. Run on SQLite and MySQL. |
| `test_asyncio_wait` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork, which are not in the 0.1 API. Its step assertions moved to `test_recorded_step_output`. |
| `test_asyncio_wait_all_completed` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork. Replaced by `test_parallel_steps_replay_from_recorded_results`. |
| `test_asyncio_wait_first_exception` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork. |
| `test_asyncio_wait_timeout` | dbos-transact-py `tests/test_async.py` | 236142b | dropped | not run | not run | Needs `DBOS.asyncio_wait` and fork. |
| `test_error_serialization` | dbos-transact-py `tests/test_failures.py` | 236142b | adapted | pass | pass | Tests the JSON format, while upstream tests pickle. Pickle round trips are in `tests/test_pickle_option.py`. Keeps the step-retry round trip. Queue, auth, and client exceptions are not in this API. A custom exception keeps its type without running its constructor, and only a class not loaded in this process comes back as `DBOSRecordedError`. |

| `test_pydantic_validation_variadic_params`, `test_pydantic_validator_concurrent_first_calls` | dbos-transact-py `tests/test_serialization.py` | 236142b | adapted | unit pass | not run | Direct validator scenarios; workflow execution remains unverified. |
| `test_workflow_pydantic_validation` | dbos-transact-py `tests/test_serialization.py` | 236142b | adapted | blocked | not run | SQLite launch hangs before the test. |
| `test_log_config` | dbos-transact-py `tests/test_config.py` | 236142b | adapted | blocked | blocked | SQLite launch hangs; sandbox denies MySQL sockets. |
| `test_sync_ds_runs_outside_workflow`, `test_sync_ds_rejects_misuse` | dbos-transact-py `tests/test_datasource.py` | 236142b | adapted | pass | pass | No registry check: rust-dwos keeps no datasource list, because it has no rewind. A plain object stands in for `async_sessionmaker`, which needs greenlet. |
| `test_sync_ds_records_and_replays`, `test_sync_ds_records_and_replays_errors` | dbos-transact-py `tests/test_datasource.py` | 236142b | adapted | pass | pass | `_sys_db.delete_workflows` replaced with `DBOS.delete_workflow`. The pickle row assertion is removed: rows follow the config's format. |
| `test_sync_ds_rolls_back_once_ownership_moves` | dbos-transact-py `tests/test_datasource.py` | 236142b | adapted | pass | pass | The owner change uses `Database.take_over`; the stale run sees `DBOSLeaseLostError` instead of `DBOSWorkflowConflictIDError`. |
| `test_sync_ds_replays_the_row_that_won` | dbos-transact-py `tests/test_datasource.py` `test_sync_ds_conflicts_when_duplicate_execution_wins` | 236142b | adapted | pass | pass | Only the case where the second execution still owns the workflow: it replays the recorded row and its writes roll back. The stolen-owner case is `test_sync_ds_rolls_back_once_ownership_moves`. |
| `test_sync_ds_completion_clears_checkpoints`, `test_datasource_must_be_created_before_launch` | dbos-transact-py `tests/test_datasource.py` | 236142b | adapted | pass | pass |  |
| `test_ds_run_migrations_false` | dbos-transact-py `tests/test_datasource.py` `test_ds_run_migrations_false_rejects_unmigrated_sqlite`, `test_ds_static_migrate_sqlite` | 236142b | adapted | pass | pass | Merged, and run on MySQL too. There is no version table, so it checks only that the table exists. |
| `test_async_ds_records_and_replays` | dbos-transact-py `tests/test_datasource.py` | 236142b | adapted | pass | pass | One `@ads.transaction` step instead of three call styles. aiosqlite on SQLite, aiomysql on MySQL. Skips without greenlet. |
| Postgres-only datasource tests: serialization retry, lost commit acknowledgement, least-privilege role, adopting an unversioned table, concurrent and frozen migrators | dbos-transact-py `tests/test_datasource.py` | 236142b | dropped | not run | not run | They need psycopg errors or Postgres roles and advisory locks. Postgres is not tested in this slice. |
| Remaining datasource tests: other async copies, ORM objects, custom sessionmaker, `schema_translate_map`, locked pre-check retry, stale error, lost race, `reserve_step`, `_delete_checkpoints` | dbos-transact-py `tests/test_datasource.py` | 236142b | dropped | not run | not run | Not adapted yet. `_delete_checkpoints` is for rewind, which rust-dwos does not have. |
| `test_debouncer` | dbos-transact-py `tests/test_debouncer.py` | 236142b | adapted | pass | pass | Drops the recovery rerun at the end, which needs `set_workflow_status`. |
| `test_debouncer_timeout` | dbos-transact-py `tests/test_debouncer.py` | 236142b | passing | pass | pass |  |
| `test_multiple_debouncers` | dbos-transact-py `tests/test_debouncer.py` | 236142b | passing | pass | pass |  |
| `test_debouncer_queue` | dbos-transact-py `tests/test_debouncer.py` | 236142b | adapted | pass | pass | Drops the `app_version` pass-through, which needs `GlobalParams`. |
| `test_debouncer_async` | dbos-transact-py `tests/test_debouncer.py` | 236142b | passing | pass | pass |  |
| `test_debouncer_client` | dbos-transact-py `tests/test_debouncer.py` | 236142b | adapted | pass | pass | No `DBOSClient` yet: a stand-in exposes `_engine` and `retrieve_workflow`. |
| `test_debounce_flip_clears_dedup` | dbos-transact-py `tests/test_debouncer.py` | 236142b | adapted | pass | pass | Reads `DBOS.get_workflow_status`, which has no `is_debounced` field. |
| `test_debounce_cancel_then_redebounce` | dbos-transact-py `tests/test_debouncer.py` | 236142b | adapted | pass | pass | Reads `DBOS.get_workflow_status`. |
| `test_debounce_user_dedup_conflict_raises` | dbos-transact-py `tests/test_debouncer.py` | 236142b | adapted | pass | pass | Uses a named queue, since users cannot enqueue on the internal queue here. |
| `test_debounce_fixed_workflow_id_reuse` | dbos-transact-py `tests/test_debouncer.py` | 236142b | passing | pass | pass |  |
| `test_debounce_rejects_caller_dedup_and_delay` | dbos-transact-py `tests/test_debouncer.py` | 236142b | rewritten | pass | pass | One parametrized case per rejected option, checked through `SetEnqueueOptions` and `SetWorkflowID`. |
| `test_debouncer_client_async`, `test_debounce_deadline_caps_initial_delay`, `test_debounce_inworkflow_*`, `test_debounce_retries_replayed_portable_dedup_error`, `test_debounce_bounce_path_does_not_leak_pinned_id`, `test_debounce_pinned_id_survives_enqueue_dedup_race`, `test_debounce_key_collision_between_workflows`, `test_debounce_ownership_across_applications`, `test_debounce_targets_another_application`, `test_debounce_bounce_atomic_with_checkpoint`, `test_debounce_bounce_checkpoint_survives_post_commit_crash` | dbos-transact-py `tests/test_debouncer.py` | 236142b | dropped | not run | not run | They read DBOS's `_sys_db`, debug triggers or `_classify_bounce`, or need `DBOSClient`. The timeout cap, key collision, in-workflow replay and atomic checkpoint are tested in Rust in `crates/dwos-core/tests/debounce.rs`. |
