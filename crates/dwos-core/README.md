# dwos-core

An embedded durable workflow engine for SQLite, MySQL 8.4 and Postgres. Rust owns persistence, replay, step numbering, leases, recovery, retry policy and workflow management. An SDK supplies serialized values and invokes user functions; the engine never calls them.

`Engine::begin` returns a new `Run`, a stored result, or an existing workflow ID to attach to. A workflow ID identifies one execution; reuse with another function name is an error. For a run, allocate step IDs in call order and use `check_step` before invoking a step. Save its output or error, then `conclude` the workflow. Saved steps replay their recorded values. Unfinished steps can execute again after a crash, so external effects need their own idempotency or fencing.

Keep `ActiveRun` alive with `keep_lease` while executing. Monitor `ActiveRun::lost` and stop scheduling work when ownership is lost. The store fences writes and execution decisions by ownership token. A database check cannot prevent an SDK function from starting after a process pause between that check and invocation; initial lease age and this scheduling gap must be handled by the SDK and external effect boundaries.

`recover_now` and `recovery_tick` return `Claimed` descriptors filtered by registered function names and readable serialization formats. Construct a `Run` from its lease, serialization and deadline, then execute through the same path as a new run. Recovery limits, interrupted-attempt accounting and persisted deadlines apply to recovery too. Claim retries reuse request tokens to recover a committed descriptor after a lost acknowledgment.

`sleep` records an absolute database-clock wake time and returns the remaining duration. The SDK performs the actual sleep while monitoring ownership. A relative workflow timeout starts when execution starts; children inherit the absolute deadline unless they supply their own timeout or explicitly opt out. Deadlines survive restart and resume. An expired deadline cancels the workflow; resume does not silently grant a new budget.

Workflow management supports cancellation, resume and filtered listing. Events and messages use durable step records for replay, and messages are FIFO within a destination/topic. Polling APIs return serialized values or remaining wait time; SDKs perform the waits.

## Queues

Register a durable queue with `ensure_queue(name, NonZeroU32)`, submit with `enqueue(name, Begin)`, and ask for executable descriptors with `claim_queue(name, registered_names)`. Registration must match the persisted concurrency limit. Enqueue returns an ID, never an executable run. Use an explicit workflow ID or a durable parent step when submission itself must be idempotent.

Queue membership is separate from the existing workflow row. One queue transaction revokes expired tokens, counts remaining owners and admits compatible work in FIFO order. This enforces global lease-owner concurrency across processes. Ordinary recovery and direct resume claims cannot bypass admission. Unsupported functions/formats still occupy capacity while owned. Relative timeout budgets begin on first admission; inherited deadlines may expire while waiting. Cancellation, finish and release free capacity through the existing workflow state.

Upgrade or stop all older claimers before using queues: workers without membership exclusions can bypass admission. Queue concurrency bounds current owners; an old user function may continue until its SDK responds to lease loss. Rate limits, priorities, partitions and runtime reconfiguration are outside this API.

## Storage and checks

Connections apply bounded lock waits and strict settings. `EngineConfig::read_only` opens read-only database sessions, skips migrations and avoids timeout writes from status reads. It is suitable for inspection; database credentials remain the security boundary.

Schemas are prerelease. New features add companion tables; there is no general migration runner for older changes to existing columns or CHECK constraints. Upgrade those schemas offline before using the new engine. Never delete a database containing wanted workflow history to resolve a schema mismatch.

Run `cargo test -p dwos-core --no-fail-fast` and `cargo clippy -p dwos-core --all-targets -- -D warnings`. The Rust tests use disposable databases on the existing test servers (MySQL port 13307, Postgres port 13309) and temporary SQLite files. The workspace SDK is being integrated separately.
