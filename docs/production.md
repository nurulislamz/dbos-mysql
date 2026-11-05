# Production

> **Not production ready yet.** Crash tests last ran before the newest features, the full Python suite has not passed on the newest code, and a Postgres restart incident is unexplained. `PROGRESS.md` has the list of what is left. This page describes how it is meant to run.

## Hosting

There is nothing extra to host. Deploy your program as you do now, give it a database URL, and run as many copies as you need. SQLite is for one machine only.

Each workflow runs in a copy that has registered its name. Register the same workflows and queues in every copy, unless you want some copies to skip some work.

## Settings that matter

| Setting | Default | Trade-off |
|---|---|---|
| `lease_seconds` | 30 | Shorter finds crashes faster, but a pause or a database blip can lose a lease and cause a replay |
| `max_run_seconds` | 3600 | Set it above your longest step |
| `recovery_interval_seconds` | 1 | How often each copy looks for dead owners, with jitter |
| `max_concurrent_recoveries` | 50 | The most workflows one tick claims. A copy already this busy claims none |
| `shutdown_timeout_seconds` | 5 | How long `destroy()` waits for work to stop |

After a crash, a workflow restarts about one lease later. Backlogs recover at most one batch per interval.

## Upgrades

### Changing your code

A workflow that is still running replays on the new code. If the steps changed, it fails on replay. Before you deploy a change to a workflow's steps, let old runs finish or cancel them.

> **Coming soon:** patching and app versions, so that old runs can keep working through a code change.

### Changing the schema

`DBOS.launch()` applies numbered migrations and keeps all rows. Several processes can start at once. They take turns, and each migration applies once.

**Before upgrading:** stop every older worker and take a backup. Older workers do not read the schema version, so nothing stops them. Their claims can bypass queue admission, and their writes can fail.

| Database | What a first upgrade does | Effect |
|---|---|---|
| MySQL | May rebuild `workflow_status` and `operation_outputs` | Reads continue. Writes wait for the whole copy. One rebuild of an 824 MB table took 59 seconds |
| Postgres | Replaces a status check | Blocks reads and writes on `workflow_status` during a scan |
| SQLite | Rebuilds `workflow_status` in one transaction | Blocks writers. Readers continue in WAL mode |

Run large MySQL upgrades in a maintenance window. Set `"run_migrations": False` if your program should never change the schema. It then starts only on a database at exactly the current version.

A failed upgrade returns its error, keeps the rows, and is retried at the next start. A database newer than the binary is refused.

## See your workflows

A small read-only page shows workflows, their status and each saved step. Your program never needs it.

```sh
python -m rust_dwos.ui --db mysql://user:password@host:3306/database --port 8000
```

It answers only on your own machine and has no login, so do not expose the port without adding one. A URL passed with `--db` is visible to other users of the machine in the process list.

Values are shown as stored and never decoded, so pickled values appear as base64. Use `"serializer": "json"` if you want them readable.

## Stored values

Values use pickle by default, so most Python values come back as they were.

- **Trust:** loading a pickle can run code. Anyone who can write to this database could run code on every process that reads it.
- **Renames:** a pickle stores the path to its class. Moving a class can break unfinished workflows.

To use JSON:

```python
DBOS(config={"name": "my-app", "system_database_url": url, "serializer": "json"})
```

A database row then cannot run code, and the row's tag cannot turn pickle on. A JSON process refuses pickled values and leaves pickled workflows unclaimed. Each workflow keeps the format it started with, so you can switch without breaking old rows.

Annotate your functions to keep rich types with JSON. Without annotations:

| Value | Comes back as |
|---|---|
| `tuple`, `set` | `list` |
| `datetime`, `UUID`, `Decimal`, `bytes` | `str` |
| Dataclass or Pydantic model | `dict` |
| Non-string dict keys | Strings |
| `NaN`, `Infinity`, non-UTF-8 `bytes`, arbitrary class instances | `TypeError`, nothing is stored |

## Security

- Database credentials are the security boundary. There are no users or roles in rust-dwos.
- Require TLS in the database URL. Without that, a connection can use plain text. TLS is tested on MySQL and Postgres.
- `dwos-core` forbids `unsafe` Rust. The Python dependencies have no lock file and have not been audited.
- There has been no independent security review.

## SQLite warning

Do not open the workflow database with Python's `sqlite3` in a process that also runs this library. The library has its own copy of SQLite, and two copies in one process can corrupt the file.

## Monitoring

Use `DBOS.list_workflows` for counts by status, and watch `recovery_attempts`. Logs use the `dbos` logger, and `log_level` and `console_log_level` configure it.

> **Coming soon:** OpenTelemetry tracing and published benchmarks. Speed, memory use and behaviour with a large backlog have not been measured.

DBOS's hosted Conductor is not planned here, nor what depends on it: alerting, autoscaling, audit logs and `dbosctl`.
