# dwos-core

An embedded durable workflow engine for SQLite, MySQL 8.4 and Postgres. Rust owns persistence, replay, step numbering, leases, recovery, retry policy and workflow management. An SDK supplies serialized values and invokes user functions; the engine never calls them.

`Engine::begin` returns a new `Run`, a stored result, or an existing workflow ID to attach to. A workflow ID identifies one execution; reuse with another function name is an error. For a run, allocate step IDs in call order and use `check_step` before invoking a step. Save its output or error, then `conclude` the workflow. Saved steps replay their recorded values. Unfinished steps can execute again after a crash, so external effects need their own idempotency or fencing.

Keep `ActiveRun` alive with `keep_lease` while executing. Monitor `ActiveRun::lost` and stop scheduling work when ownership is lost. The store fences writes and execution decisions by ownership token. A database check cannot prevent an SDK function from starting after a process pause between that check and invocation; initial lease age and this scheduling gap must be handled by the SDK and external effect boundaries.

`recover_now` and `recovery_tick` return `Claimed` descriptors filtered by registered function names and readable serialization formats. Construct a `Run` from its lease, serialization and deadline, then execute through the same path as a new run. Recovery limits, interrupted-attempt accounting and persisted deadlines apply to recovery too. Claim retries reuse request tokens to recover a committed descriptor after a lost acknowledgment.

`sleep` records an absolute database-clock wake time and returns the remaining duration. The SDK performs the actual sleep while monitoring ownership. A relative workflow timeout starts when execution starts; children inherit the absolute deadline unless they supply their own timeout or explicitly opt out. Deadlines survive restart and resume. An expired deadline cancels the workflow; resume does not silently grant a new budget.

Workflow management supports cancellation, resume and filtered listing. Events and messages use durable step records for replay, and messages are FIFO within a destination/topic. Recovered waits use the recorded arrival deadline. Event revisions preserve earlier eligible values after overwrites and remain until workflow deletion. Older event rows without history support immediate reads, but their past arrival times cannot be reconstructed. Polling APIs return serialized values or remaining wait time; SDKs perform the waits.

## Queues

Register a durable queue with `ensure_queue(name, NonZeroU32)`, submit with `enqueue(name, Begin)`, and ask for executable descriptors with `claim_queue(name, registered_names)`. Registration must match the persisted concurrency limit. Enqueue returns an ID, never an executable run. Use an explicit workflow ID or a durable parent step when submission itself must be idempotent.

Queue membership is separate from the existing workflow row. One queue transaction revokes expired tokens, counts remaining owners and admits compatible work in FIFO order. This enforces global lease-owner concurrency across processes. Ordinary recovery and direct resume claims cannot bypass admission. Unsupported functions/formats still occupy capacity while owned. Relative timeout budgets begin on first admission; inherited deadlines may expire while waiting. Cancellation, finish and release free capacity through the existing workflow state.

Upgrade or stop all older claimers before using queues: workers without membership exclusions can bypass admission. Queue concurrency bounds current owners; an old user function may continue until its SDK responds to lease loss. Rate limits, priorities, partitions and runtime reconfiguration are outside this API.

## Storage

Connections apply bounded lock waits and strict settings. `EngineConfig::read_only` opens read-only database sessions and avoids timeout writes from status reads. It is suitable for inspection; database credentials remain the security boundary.

## Schema upgrades

`Store::migrate`, and `Engine::connect` unless `run_migrations` is false, apply the numbered files in `schema/<database>/` and record each in `dwos_schema_migrations`. An up-to-date start only reads: no lock, no DDL, no write. A database with no version record is recognised by its real shape, and each migration first checks what it would change. Empty databases get the current shape. Several processes can start at once; they take turns and each migration applies once.

**Before upgrading:** stop every older worker and take a backup. Older workers do not read the version record, so nothing stops them. Their claims can bypass queue admission, and their writes can fail or hit the rewrite below.

| Database | What a first upgrade does | Lock and downtime |
|---|---|---|
| MySQL | Rebuilds `workflow_status` for the status CHECK, again for its counters, and `operation_outputs` for its step ID, each only if needed | Each is `ALGORITHM=COPY, LOCK=SHARED`: reads continue, writes wait for the whole copy, then a short exclusive lock swaps the table. Time and free disk grow with table size, and values are `LONGTEXT`. If an `ALTER` waits 60 seconds for its lock, the start fails and the next start resumes |
| Postgres | Replaces the status CHECK | Blocks reads and writes on `workflow_status` during a scan of the table, with no rewrite. If it waits 10 seconds for the lock, the start fails and the next start retries |
| SQLite | Rebuilds `workflow_status` in one transaction, with foreign keys off so that children survive | Blocks writers for the rebuild. Readers continue in WAL mode |

Other starters wait for up to one hour for their turn. Upgrade a large MySQL database in a maintenance window: on MySQL 8.4.11 with other load running, one rebuild of an 824 MB, 400,000-row table took 59 seconds. A write made during it waited 57 seconds, and a read did not wait.

Postgres and SQLite run each migration in one transaction, so a failure leaves that migration undone. MySQL DDL commits on its own, so a crash can stop between steps; the next start checks each step by its effect and carries on. A failed upgrade returns its error, keeps the rows, and is retried by the next start.

A database newer than the binary is refused with `Error::SchemaTooNew`. Sessions that cannot migrate, meaning `read_only` and `run_migrations = false`, never write. They open only a database at exactly the current version, and otherwise return `Error::SchemaTooNew` or `Error::SchemaOutdated` from `connect`. This includes an upgrade still in progress, so retry after it ends. The exception is a database with no tables: it opens, and each query then returns a plain database error until a writer creates the schema.

Databases from before the rename have `dbos_workflows` and `dbos_steps`. Their rows are copied into the current tables in one transaction, and MySQL `DATETIME` values become milliseconds as the old code read them. The old tables stay; drop them yourself once satisfied. Those rows hold an older JSON layout, so they are tagged `dbos_mysql_json`. They stay readable as stored text and can be cancelled, but recovery and resume never claim them, and the Python SDK reports the unknown format instead of loading them. If one workflow ID is in both old and new tables with different data, the upgrade stops with `Error::Migration` and copies nothing.

To change the schema, add a new numbered file for each database and list it in `src/store/migrations.rs`. Never edit a file that has shipped, including `001_init.sql`. On MySQL give it a `pending` query and split DDL into one statement per migration, because it cannot roll back.

## Checks

Run `cargo test -p dwos-core --no-fail-fast` and `cargo clippy -p dwos-core --all-targets -- -D warnings`. The Rust tests use disposable databases on the existing test servers (MySQL port 13307, Postgres port 13309) and temporary SQLite files. The workspace SDK is being integrated separately.
