# Coming soon

These are in DBOS but not here yet. Using one today either fails with a clear error or the call does not exist. None of them block the first usable version.

## Workflows

| Feature | What it will do |
|---|---|
| Fork and rewind | Restart a workflow from a chosen step |
| `DBOS.run_step` | Run a plain function as a step, without a decorator |
| `DBOS.wait_first` | Wait for the first of several handles |
| `DBOS.asyncio_wait` | A durable version of `asyncio.wait` |
| `DBOSContextEnsure` | Make sure a DBOS context exists |
| Streams | Let a workflow publish a live series of values |
| Debouncer | Merge rapid repeated starts into one run |
| Patching and app versions | Keep old runs working through a code change |

## Queues

| Feature | What it will do |
|---|---|
| Priorities | Run some waiting work first |
| Rate limits | Cap starts per period |
| Partitions | Apply a limit per key, such as per user |
| Deduplication | Refuse a second waiting entry with the same key |
| Per-worker limits | Cap how much of a queue one process runs |
| Delayed starts | Enqueue now and start later |

## Integration

| Feature | What it will do |
|---|---|
| `DBOSClient` | Enqueue and manage workflows from a program that is not running DBOS |
| Datasources | Commit a step's app database writes together with its result |
| AI frameworks | Pydantic AI, OpenAI Agents, Google ADK and LlamaIndex |
| Custom `Serializer` | Your own stored format. Only pickle and JSON are accepted now |
| TypeScript SDK | The core is built so that this can be added, but it is not part of this version |

## Operations

| Feature | What it will do |
|---|---|
| OpenTelemetry | Traces for workflows and steps |
| More list filters | Queue, version, fork and auth fields |
| Full benchmark | Workflows per second, time per step, memory and connections |

## Much later

| Feature | What it will do |
|---|---|
| Coordinator | One copy, or later an optional service, does the scanning and wake-ups for the others, to cut database load when many copies run. It is only a shortcut: leases still protect every write, and copies fall back to polling if it dies |

## Not planned

| Feature | Why |
|---|---|
| Conductor and what depends on it | It is DBOS's own hosted service |
| Kafka, class-based workflows, external object storage | Out of scope |
| Sharding, `auto_upgrade` | Enterprise features in DBOS |
| `dbos.enqueue_workflow`, `dbos.send_message` SQL functions | They need DBOS's exact schema |
| Reading rows written by another SDK | The tables are not DBOS's `dbos` schema |
| Authentication and roles | Database credentials are the boundary |
