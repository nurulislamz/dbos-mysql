use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use dwos_core::{
    Begin, Engine, EngineConfig, Error, PORTABLE_JSON, SendFrom, Status, Store, WorkflowFilter,
};
use sqlx::any::AnyRow;
use sqlx::{AnyConnection, AnyPool, AssertSqlSafe, Connection, Row};

#[macro_use]
mod common;
use common::*;

const LEGACY_TAG: &str = "dbos_mysql_json";
const U32_MAX: i64 = 4_294_967_295;
const SECOND: i64 = 1_700_000_000_000;

macro_rules! fixture {
    ($db:literal, $commit:literal) => {
        (
            $commit,
            include_str!(concat!("fixtures/", $db, "/", $commit, ".sql")),
        )
    };
}

/// Every distinct shape in the git history, oldest first.
fn shapes(backend: Backend) -> Vec<(&'static str, &'static str)> {
    match backend {
        Backend::Sqlite => vec![
            fixture!("sqlite", "f176afd"),
            fixture!("sqlite", "526f304"),
            fixture!("sqlite", "893a6fc"),
            fixture!("sqlite", "e1e6151"),
            fixture!("sqlite", "1cbc6a5"),
            fixture!("sqlite", "44f3829"),
            fixture!("sqlite", "5f91756"),
            fixture!("sqlite", "f7405f7"),
            fixture!("sqlite", "5ff4d0c"),
        ],
        Backend::MySql => vec![
            fixture!("mysql", "f9f7144"),
            fixture!("mysql", "526f304"),
            fixture!("mysql", "893a6fc"),
            fixture!("mysql", "fddbf8f"),
            fixture!("mysql", "e1e6151"),
            fixture!("mysql", "1cbc6a5"),
            fixture!("mysql", "44f3829"),
            fixture!("mysql", "5f91756"),
            fixture!("mysql", "f7405f7"),
            fixture!("mysql", "5ff4d0c"),
        ],
        Backend::Postgres => vec![
            fixture!("postgres", "fddbf8f"),
            fixture!("postgres", "e1e6151"),
            fixture!("postgres", "1cbc6a5"),
            fixture!("postgres", "44f3829"),
            fixture!("postgres", "5f91756"),
            fixture!("postgres", "f7405f7"),
            fixture!("postgres", "5ff4d0c"),
        ],
    }
}

fn shape(backend: Backend, commit: &str) -> &'static str {
    shapes(backend)
        .into_iter()
        .find(|(name, _)| *name == commit)
        .unwrap()
        .1
}

/// The newest shape with old tables, or the oldest shape with the new ones.
fn oldest_current_named(backend: Backend) -> &'static str {
    shape(
        backend,
        match backend {
            Backend::Postgres => "fddbf8f",
            Backend::Sqlite | Backend::MySql => "893a6fc",
        },
    )
}

fn current_init(backend: Backend) -> &'static str {
    match backend {
        Backend::Sqlite => include_str!("../schema/sqlite/001_init.sql"),
        Backend::MySql => include_str!("../schema/mysql/001_init.sql"),
        Backend::Postgres => include_str!("../schema/postgres/001_init.sql"),
    }
}

fn versions(backend: Backend) -> Vec<i64> {
    match backend {
        Backend::Sqlite => vec![1, 2, 5, 6],
        Backend::MySql => vec![1, 2, 3, 4, 5, 6],
        Backend::Postgres => vec![1, 2],
    }
}

fn latest(backend: Backend) -> i64 {
    *versions(backend).last().unwrap()
}

#[derive(Clone, Debug, PartialEq)]
enum Cell {
    Text(String),
    Int(i64),
}

fn text(value: impl Into<String>) -> Cell {
    Cell::Text(value.into())
}

type Fields = Vec<(&'static str, Cell)>;

fn big_value() -> String {
    format!("\"quote's \\ {}\"", "日本語🦀 ".repeat(50_000))
}

fn workflows(serialization: &str, cancelled: bool, queued: bool) -> Vec<Fields> {
    let workflow = |id: &str, status: &str| -> Fields {
        vec![
            ("workflow_uuid", text(id)),
            ("name", text("wf")),
            ("status", text(status)),
            ("inputs", text(format!("{{\"id\":\"{id}\"}}"))),
            ("serialization", text(serialization)),
            ("recovery_attempts", Cell::Int(1)),
            ("max_recovery_attempts", Cell::Int(100)),
            ("created_at", Cell::Int(SECOND + 123)),
            ("updated_at", Cell::Int(SECOND + 124)),
        ]
    };
    let mut rows = Vec::new();
    let mut pending = workflow("wf-pending", "PENDING");
    pending[3].1 = text(big_value());
    pending.extend([
        ("claim_after", Cell::Int(SECOND + 900)),
        ("lease_token", text("00000000-0000-0000-0000-000000000001")),
    ]);
    pending[5].1 = Cell::Int(3);
    rows.push(pending);
    let mut recover = workflow("wf-recover", "PENDING");
    recover.extend([
        ("claim_after", Cell::Int(SECOND + 900)),
        ("lease_token", text("00000000-0000-0000-0000-000000000002")),
    ]);
    rows.push(recover);
    let mut success = workflow("wf-success", "SUCCESS");
    success.extend([
        ("parent_workflow_id", text("wf-pending")),
        ("output", text("\"it's done \\ 🦀\"")),
        ("completed_at", Cell::Int(SECOND + 125)),
    ]);
    rows.push(success);
    let mut error = workflow("wf-error", "ERROR");
    error.extend([
        ("error", text("{\"message\":\"boom\"}")),
        ("completed_at", Cell::Int(SECOND + 126)),
    ]);
    rows.push(error);
    let mut most = workflow("wf-max", "MAX_RECOVERY_ATTEMPTS_EXCEEDED");
    most[5].1 = Cell::Int(U32_MAX);
    most[6].1 = Cell::Int(U32_MAX);
    rows.push(most);
    if cancelled {
        rows.push(workflow("wf-cancelled", "CANCELLED"));
    }
    if queued {
        // As enqueue stores it: a token and a claim time, with no attempt used yet.
        let mut member = workflow("wf-queued", "PENDING");
        member[5].1 = Cell::Int(0);
        member.extend([
            ("claim_after", Cell::Int(SECOND + 900)),
            ("lease_token", text("00000000-0000-0000-0000-000000000003")),
        ]);
        rows.push(member);
    }
    rows
}

fn steps(serialization: &str) -> Vec<Fields> {
    let step = |id: i64, name: &str| -> Fields {
        vec![
            ("workflow_uuid", text("wf-pending")),
            ("function_id", Cell::Int(id)),
            ("function_name", text(name)),
            ("serialization", text(serialization)),
            ("completed_at_epoch_ms", Cell::Int(SECOND + 200)),
        ]
    };
    let mut first = step(1, "saved");
    first.push(("output", text(big_value())));
    let mut second = step(2, "failed");
    second.push(("error", text("{\"message\":\"step boom\"}")));
    let mut third = step(3, "child");
    third.push(("child_workflow_id", text("wf-success")));
    let mut last = step(U32_MAX, "last");
    last.push(("output", text("1")));
    vec![first, second, third, last]
}

fn notifications() -> Vec<Fields> {
    [(7, "m-7", 1), (42, "m-42", 0)]
        .into_iter()
        .map(|(id, uuid, consumed)| {
            let mut row: Fields = vec![
                ("message_id", Cell::Int(id)),
                ("destination_uuid", text("wf-pending")),
                ("topic", text("topic")),
                ("message", text(format!("\"message {id}\""))),
                ("created_at_epoch_ms", Cell::Int(SECOND + 300)),
                ("message_uuid", text(uuid)),
                ("serialization", text(PORTABLE_JSON)),
                ("consumed", Cell::Int(consumed)),
            ];
            if consumed == 1 {
                row.push(("consumed_by_function_id", Cell::Int(1)));
            }
            row
        })
        .collect()
}

fn events() -> Vec<Fields> {
    vec![vec![
        ("workflow_uuid", text("wf-pending")),
        ("key", text("k")),
        ("value", text("\"event\"")),
        ("serialization", text(PORTABLE_JSON)),
    ]]
}

fn event_history() -> Vec<Fields> {
    vec![vec![
        ("workflow_uuid", text("wf-pending")),
        ("event_key", text("k")),
        ("revision", Cell::Int(1)),
        ("created_at_epoch_ms", Cell::Int(SECOND + 400)),
        ("value", text("\"event\"")),
        ("serialization", text(PORTABLE_JSON)),
    ]]
}

fn timeouts() -> Vec<Fields> {
    vec![vec![
        ("workflow_uuid", text("wf-pending")),
        ("timeout_ms", Cell::Int(1_000)),
        ("deadline_epoch_ms", Cell::Int(4_102_444_800_000)),
    ]]
}

fn queues() -> Vec<Fields> {
    vec![vec![
        ("name", text("fixture-q")),
        ("concurrency", Cell::Int(2)),
    ]]
}

fn queue_entries() -> Vec<Fields> {
    vec![vec![
        ("sequence", Cell::Int(5)),
        ("workflow_uuid", text("wf-queued")),
        ("queue_name", text("fixture-q")),
    ]]
}

fn quoted(backend: Backend, column: &str) -> String {
    match backend {
        Backend::MySql => format!("`{column}`"),
        Backend::Sqlite | Backend::Postgres => format!("\"{column}\""),
    }
}

async fn insert(pool: &AnyPool, backend: Backend, table: &str, fields: &Fields) {
    let columns: Vec<String> = fields.iter().map(|(c, _)| quoted(backend, c)).collect();
    let marks: Vec<String> = (1..=fields.len())
        .map(|n| match backend {
            Backend::Postgres => format!("${n}"),
            Backend::Sqlite | Backend::MySql => "?".to_owned(),
        })
        .collect();
    let sql = format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        columns.join(", "),
        marks.join(", ")
    );
    let mut query = sqlx::query(AssertSqlSafe(sql));
    for (_, cell) in fields {
        query = match cell {
            Cell::Text(value) => query.bind(value.clone()),
            Cell::Int(value) => query.bind(*value),
        };
    }
    query.execute(pool).await.unwrap();
}

fn cell(row: &AnyRow, column: &str) -> Option<Cell> {
    if let Ok(value) = row.try_get::<Option<String>, _>(column) {
        return value.map(Cell::Text);
    }
    row.try_get::<Option<i64>, _>(column)
        .unwrap()
        .map(Cell::Int)
}

/// Reads back the rows, in the order given, and compares every column of the table.
async fn assert_rows(
    pool: &AnyPool,
    backend: Backend,
    table: &str,
    order: &str,
    columns: &[&'static str],
    expected: Vec<Fields>,
) {
    let list: Vec<String> = columns.iter().map(|c| quoted(backend, c)).collect();
    let sql = format!(
        "SELECT {} FROM {table} ORDER BY {}",
        list.join(", "),
        quoted(backend, order)
    );
    let rows = sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap();
    let found: Vec<Fields> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .filter_map(|column| cell(row, column).map(|value| (*column, value)))
                .collect()
        })
        .collect();
    let mut expected: Vec<Fields> = expected
        .into_iter()
        .map(|mut fields| {
            fields.sort_by_key(|(column, _)| columns.iter().position(|c| c == column).unwrap());
            fields
        })
        .collect();
    let key = |fields: &Fields| fields.iter().find(|(c, _)| *c == order).unwrap().1.clone();
    expected.sort_by_key(|fields| match key(fields) {
        Cell::Text(value) => (value, 0),
        Cell::Int(value) => (String::new(), value),
    });
    assert_eq!(found.len(), expected.len(), "row count of {table} changed");
    for (found, expected) in found.iter().zip(&expected) {
        assert_eq!(found, expected, "a row of {table} changed");
    }
}

/// What a shape's rows are: which tables and columns it has decides what can be seeded.
#[derive(Clone, Copy)]
struct Seeded {
    legacy: bool,
    parent_columns: bool,
    cancelled: bool,
    notifications: bool,
    timeouts: bool,
    history: bool,
    queues: bool,
}

impl Seeded {
    fn of(schema: &str) -> Self {
        Self {
            legacy: schema.contains("dbos_workflows"),
            parent_columns: schema.contains("parent_workflow_id"),
            cancelled: schema.contains("'CANCELLED'"),
            notifications: schema.contains("notifications"),
            timeouts: schema.contains("workflow_timeouts"),
            history: schema.contains("workflow_event_history"),
            queues: schema.contains("workflow_queues"),
        }
    }

    fn expected_workflows(self) -> Vec<Fields> {
        let serialization = if self.legacy {
            LEGACY_TAG
        } else {
            PORTABLE_JSON
        };
        let mut rows = workflows(serialization, self.cancelled, self.queues);
        if self.legacy && !self.parent_columns {
            for row in &mut rows {
                row.retain(|(c, _)| *c != "parent_workflow_id" && *c != "completed_at");
            }
        }
        rows
    }

    fn expected_steps(self) -> Vec<Fields> {
        steps(if self.legacy {
            LEGACY_TAG
        } else {
            PORTABLE_JSON
        })
    }
}

fn legacy_time(ms: i64) -> String {
    // The sub-millisecond digits prove the upgrade keeps milliseconds the way the old reader did.
    format!("2023-11-14 22:13:20.{:03}456", ms - SECOND)
}

fn legacy_fields(backend: Backend, fields: &Fields, table: &str) -> Fields {
    fields
        .iter()
        .filter(|(column, _)| *column != "serialization")
        .map(|(column, value)| {
            let renamed = match (table, *column) {
                ("dbos_workflows", "workflow_uuid") | ("dbos_steps", "workflow_uuid") => {
                    "workflow_id"
                }
                ("dbos_workflows", "inputs") => "input",
                ("dbos_steps", "function_id") => "step_id",
                ("dbos_steps", "function_name") => "step_name",
                ("dbos_steps", "completed_at_epoch_ms") => match backend {
                    Backend::MySql => "created_at",
                    _ => "created_at_ms",
                },
                ("dbos_workflows", "claim_after") if !matches!(backend, Backend::MySql) => {
                    "claim_after_ms"
                }
                ("dbos_workflows", "created_at") if !matches!(backend, Backend::MySql) => {
                    "created_at_ms"
                }
                ("dbos_workflows", "updated_at") if !matches!(backend, Backend::MySql) => {
                    "updated_at_ms"
                }
                ("dbos_workflows", "completed_at") if !matches!(backend, Backend::MySql) => {
                    "completed_at_ms"
                }
                (_, same) => same,
            };
            let value = match (backend, value, renamed) {
                (
                    Backend::MySql,
                    Cell::Int(ms),
                    "claim_after" | "created_at" | "updated_at" | "completed_at",
                ) => Cell::Text(legacy_time(*ms)),
                _ => value.clone(),
            };
            (renamed, value)
        })
        .collect()
}

/// Builds a shape on a new database and fills it with real rows, as that binary would have left them.
async fn old_database(backend: Backend, schema: &str) -> (TestDb, Store, Seeded) {
    let db = TestDb::create(backend).await;
    let store = Store::connect(&db.url).await.unwrap();
    sqlx::raw_sql(AssertSqlSafe(schema.to_owned()))
        .execute(store.pool())
        .await
        .unwrap();
    let seeded = Seeded::of(schema);
    let pool = store.pool();
    if seeded.legacy {
        for row in seeded.expected_workflows() {
            let fields = legacy_fields(backend, &row, "dbos_workflows");
            insert(pool, backend, "dbos_workflows", &fields).await;
        }
        for row in seeded.expected_steps() {
            let fields = legacy_fields(backend, &row, "dbos_steps");
            insert(pool, backend, "dbos_steps", &fields).await;
        }
        return (db, store, seeded);
    }
    for row in seeded.expected_workflows() {
        insert(pool, backend, "workflow_status", &row).await;
    }
    for row in seeded.expected_steps() {
        insert(pool, backend, "operation_outputs", &row).await;
    }
    if seeded.notifications {
        for row in notifications() {
            insert(pool, backend, "notifications", &row).await;
        }
        for row in events() {
            insert(pool, backend, "workflow_events", &row).await;
        }
    }
    if seeded.timeouts {
        for row in timeouts() {
            insert(pool, backend, "workflow_timeouts", &row).await;
        }
    }
    if seeded.history {
        for row in event_history() {
            insert(pool, backend, "workflow_event_history", &row).await;
        }
    }
    if seeded.queues {
        for row in queues() {
            insert(pool, backend, "workflow_queues", &row).await;
        }
        for row in queue_entries() {
            insert(pool, backend, "workflow_queue_entries", &row).await;
        }
    }
    // Explicit IDs do not move a Postgres sequence, and real rows would have.
    if matches!(backend, Backend::Postgres) {
        if seeded.notifications {
            sqlx::raw_sql(
                "SELECT setval(pg_get_serial_sequence('notifications', 'message_id'), 42)",
            )
            .execute(pool)
            .await
            .unwrap();
        }
        if seeded.queues {
            sqlx::raw_sql(
                "SELECT setval(pg_get_serial_sequence('workflow_queue_entries', 'sequence'), 5)",
            )
            .execute(pool)
            .await
            .unwrap();
        }
    }
    (db, store, seeded)
}

async fn assert_rows_survived(pool: &AnyPool, backend: Backend, seeded: Seeded) {
    assert_rows(
        pool,
        backend,
        "workflow_status",
        "workflow_uuid",
        &[
            "workflow_uuid",
            "name",
            "status",
            "inputs",
            "parent_workflow_id",
            "output",
            "error",
            "serialization",
            "claim_after",
            "lease_token",
            "recovery_attempts",
            "max_recovery_attempts",
            "created_at",
            "updated_at",
            "completed_at",
        ],
        seeded.expected_workflows(),
    )
    .await;
    assert_rows(
        pool,
        backend,
        "operation_outputs",
        "function_id",
        &[
            "workflow_uuid",
            "function_id",
            "function_name",
            "output",
            "error",
            "child_workflow_id",
            "serialization",
            "completed_at_epoch_ms",
        ],
        seeded.expected_steps(),
    )
    .await;
    let either = |present: bool, rows: Vec<Fields>| if present { rows } else { Vec::new() };
    assert_rows(
        pool,
        backend,
        "notifications",
        "message_id",
        &[
            "message_id",
            "destination_uuid",
            "topic",
            "message",
            "created_at_epoch_ms",
            "message_uuid",
            "serialization",
            "consumed",
            "consumed_by_function_id",
        ],
        either(seeded.notifications, notifications()),
    )
    .await;
    assert_rows(
        pool,
        backend,
        "workflow_events",
        "workflow_uuid",
        &["workflow_uuid", "key", "value", "serialization"],
        either(seeded.notifications, events()),
    )
    .await;
    assert_rows(
        pool,
        backend,
        "workflow_event_history",
        "workflow_uuid",
        &[
            "workflow_uuid",
            "event_key",
            "revision",
            "created_at_epoch_ms",
            "value",
            "serialization",
        ],
        either(seeded.history, event_history()),
    )
    .await;
    assert_rows(
        pool,
        backend,
        "workflow_timeouts",
        "workflow_uuid",
        &["workflow_uuid", "timeout_ms", "deadline_epoch_ms"],
        either(seeded.timeouts, timeouts()),
    )
    .await;
    assert_rows(
        pool,
        backend,
        "workflow_queues",
        "name",
        &["name", "concurrency"],
        either(seeded.queues, queues()),
    )
    .await;
    assert_rows(
        pool,
        backend,
        "workflow_queue_entries",
        "sequence",
        &[
            "sequence",
            "workflow_uuid",
            "queue_name",
            "start_timeout_ms",
        ],
        either(seeded.queues, queue_entries()),
    )
    .await;
}

async fn recorded(pool: &AnyPool) -> Vec<i64> {
    sqlx::query_scalar("SELECT version FROM dwos_schema_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .unwrap()
}

/// The structure of the engine's tables, in a form a fresh database and an upgraded one can share.
async fn fingerprint(pool: &AnyPool, backend: Backend) -> Vec<String> {
    const TABLES: [&str; 8] = [
        "workflow_status",
        "operation_outputs",
        "notifications",
        "workflow_events",
        "workflow_timeouts",
        "workflow_event_history",
        "workflow_queues",
        "workflow_queue_entries",
    ];
    let mut lines = Vec::new();
    match backend {
        Backend::Sqlite => {
            let rows: Vec<String> = sqlx::query_scalar(
                "SELECT type || ' ' || name || ' ' || sql FROM sqlite_master \
                 WHERE sql IS NOT NULL AND name NOT LIKE 'dbos%' AND tbl_name NOT LIKE 'dbos%' \
                 AND name NOT LIKE 'dwos%' AND name <> 'sqlite_sequence' ORDER BY type, name",
            )
            .fetch_all(pool)
            .await
            .unwrap();
            // The rebuild's rename quotes the name, and SQLite keeps it that way.
            lines.extend(rows.iter().map(|sql| {
                sql.replace("\"workflow_status\"", "workflow_status")
                    .replace("workflow_status_new", "workflow_status")
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            }));
        }
        Backend::MySql => {
            for table in TABLES {
                let row = sqlx::query(AssertSqlSafe(format!("SHOW CREATE TABLE {table}")))
                    .fetch_one(pool)
                    .await
                    .unwrap();
                let sql: String = row.try_get(1).unwrap();
                // A literal's charset prefix follows how the CHECK was created and means nothing for ASCII statuses.
                let sql = sql.replace("_ascii'", "'").replace("_utf8mb4'", "'");
                lines.extend(sql.lines().map(|line| {
                    line.split_whitespace()
                        .filter(|word| !word.starts_with("AUTO_INCREMENT="))
                        .collect::<Vec<_>>()
                        .join(" ")
                }));
            }
        }
        Backend::Postgres => {
            let queries = [
                "SELECT table_name || '.' || column_name || ' ' || ordinal_position || ' ' || data_type || ' ' || is_nullable || ' ' || COALESCE(column_default, '') \
                 FROM information_schema.columns WHERE table_schema = current_schema() \
                 AND table_name NOT LIKE 'dwos%' ORDER BY 1",
                "SELECT conrelid::regclass::text || ' ' || conname || ' ' || pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE connamespace = current_schema()::regnamespace AND conrelid <> 'dwos_schema_migrations'::regclass ORDER BY 1",
                "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename NOT LIKE 'dwos%' ORDER BY 1",
            ];
            for query in queries {
                let rows: Vec<String> = sqlx::query_scalar(query).fetch_all(pool).await.unwrap();
                lines.extend(rows);
            }
        }
    }
    lines
}

fn config() -> EngineConfig {
    EngineConfig {
        lease: LONG,
        pickle: false,
        ..EngineConfig::default()
    }
}

fn begin(id: &str) -> Begin<'_> {
    Begin {
        name: "wf",
        input: "{}",
        workflow_id: Some(id),
        parent: None,
        max_recovery_attempts: 100,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    }
}

async fn assert_new_engine_works(db: &TestDb, seeded: Seeded) {
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    let listed = engine
        .list_workflows(&WorkflowFilter::default())
        .await
        .unwrap();
    assert_eq!(listed.len(), seeded.expected_workflows().len());

    // Recovery claims what it can read, and never runs the old JSON layout with the wrong arguments.
    let recovered = engine.recover_now(&names()).await.unwrap();
    let recovered: Vec<&str> = recovered
        .iter()
        .map(|claimed| claimed.lease.workflow_id.as_str())
        .collect();
    if seeded.legacy {
        assert!(recovered.is_empty(), "{recovered:?}");
    } else {
        assert!(recovered.contains(&"wf-recover"), "{recovered:?}");
    }

    engine.cancel_workflow("wf-pending").await.unwrap();
    assert_eq!(
        engine.get_status("wf-pending").await.unwrap().status,
        Status::Cancelled
    );

    engine
        .ensure_queue("new-q", NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    engine.enqueue("new-q", begin("in-queue")).await.unwrap();
    let claimed = engine.claim_queue("new-q", &names()).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].lease.workflow_id, "in-queue");

    if seeded.queues {
        let claimed = engine.claim_queue("fixture-q", &names()).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].lease.workflow_id, "wf-queued");
    }

    // The sequence carried on: a new message does not reuse the fixture's IDs.
    if seeded.notifications {
        engine
            .store()
            .send(
                "wf-pending",
                Some("topic"),
                "\"later\"",
                PORTABLE_JSON,
                Some("m-later"),
                SendFrom::Outside,
            )
            .await
            .unwrap();
        let newest: i64 = sqlx::query_scalar("SELECT MAX(message_id) FROM notifications")
            .fetch_one(engine.store().pool())
            .await
            .unwrap();
        assert!(newest > 42, "{newest}");
    }
}

async fn assert_fresh_shape(pool: &AnyPool, backend: Backend) {
    let fresh = TestDb::create(backend).await;
    let store = fresh.open().await;
    let upgraded = fingerprint(pool, backend).await;
    let wanted = fingerprint(store.pool(), backend).await;
    store.pool().close().await;
    let differs: Vec<_> = upgraded
        .iter()
        .filter(|line| !wanted.contains(line))
        .chain(wanted.iter().filter(|line| !upgraded.contains(line)))
        .collect();
    assert!(
        differs.is_empty(),
        "differs from a fresh database: {differs:#?}"
    );
    assert_eq!(upgraded, wanted, "same lines in another order");
}

all_dbs!(
    every_historic_shape_upgrades_with_its_rows_exact,
    backend,
    {
        for (commit, schema) in shapes(backend) {
            for mixed in [false, true] {
                let (db, store, seeded) = old_database(backend, schema).await;
                // Before the runner, an old database that met a new binary gained new tables and kept its old columns.
                if mixed {
                    sqlx::raw_sql(AssertSqlSafe(current_init(backend).to_owned()))
                        .execute(store.pool())
                        .await
                        .unwrap();
                }
                store.migrate().await.unwrap_or_else(|error| {
                    panic!("{commit} (mixed {mixed}): {error}");
                });
                assert_eq!(recorded(store.pool()).await, versions(backend), "{commit}");
                assert_rows_survived(store.pool(), backend, seeded).await;
                assert_fresh_shape(store.pool(), backend).await;
                assert_new_engine_works(&db, seeded).await;
                store.pool().close().await;
            }
        }
    }
);

all_dbs!(an_empty_database_gets_the_current_shape, backend, {
    let db = TestDb::create(backend).await;
    let store = db.open().await;
    assert_eq!(recorded(store.pool()).await, versions(backend));
    let raw = TestDb::create(backend).await;
    let bare = Store::connect(&raw.url).await.unwrap();
    sqlx::raw_sql(AssertSqlSafe(current_init(backend).to_owned()))
        .execute(bare.pool())
        .await
        .unwrap();
    // The bare database has no version table, which fingerprints ignore except on Postgres.
    sqlx::raw_sql("CREATE TABLE dwos_schema_migrations (version BIGINT NOT NULL PRIMARY KEY)")
        .execute(bare.pool())
        .await
        .unwrap();
    assert_eq!(
        fingerprint(store.pool(), backend).await,
        fingerprint(bare.pool(), backend).await
    );
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    engine
        .ensure_queue("q", NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    engine.enqueue("q", begin("a")).await.unwrap();
    assert_eq!(engine.claim_queue("q", &names()).await.unwrap().len(), 1);
    engine.cancel_workflow("a").await.unwrap();
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::Cancelled
    );
});

async fn hold_migration_lock(backend: Backend, url: &str) -> AnyConnection {
    let mut conn = AnyConnection::connect(url).await.unwrap();
    let sql = match backend {
        Backend::Sqlite => "BEGIN IMMEDIATE",
        Backend::MySql => "SELECT GET_LOCK(SHA1(CONCAT('dwos_migrate:', DATABASE())), 0)",
        Backend::Postgres => "SELECT pg_advisory_lock(7240319)",
    };
    sqlx::raw_sql(sql).execute(&mut conn).await.unwrap();
    conn
}

/// SQLite shows no waiters, so there the proof is every starter succeeding without a busy error.
async fn wait_for_waiters(pool: &AnyPool, backend: Backend, waiters: i64) {
    let sql = match backend {
        Backend::Sqlite => return,
        Backend::MySql => {
            "SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_TYPE = 'USER LEVEL LOCK' \
             AND LOCK_STATUS = 'PENDING' AND OBJECT_NAME = SHA1(CONCAT('dwos_migrate:', DATABASE()))"
        }
        Backend::Postgres => {
            "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted \
             AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
        }
    };
    for _ in 0..600 {
        let count: i64 = sqlx::query_scalar(sql).fetch_one(pool).await.unwrap();
        if count == waiters {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{waiters} starters never waited for the migration lock");
}

async fn race_starters(backend: Backend, url: &str, pool: &AnyPool) {
    const STARTERS: usize = 8;
    let holder = hold_migration_lock(backend, url).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(STARTERS));
    let mut tasks = Vec::new();
    for _ in 0..STARTERS {
        let url = url.to_owned();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let store = Store::connect(&url).await.unwrap();
            barrier.wait().await;
            let result = store.migrate().await;
            store.pool().close().await;
            result
        }));
    }
    wait_for_waiters(pool, backend, STARTERS as i64).await;
    assert!(
        tasks.iter().all(|task| !task.is_finished()),
        "a starter finished while another held the lock"
    );
    // Closing also proves that a holder that dies frees the lock, not only a polite release.
    holder.close().await.unwrap();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
}

all_dbs!(concurrent_starts_upgrade_an_old_database_once, backend, {
    let (db, store, seeded) = old_database(backend, oldest_current_named(backend)).await;
    race_starters(backend, &db.url, store.pool()).await;
    assert_eq!(recorded(store.pool()).await, versions(backend));
    assert_rows_survived(store.pool(), backend, seeded).await;
    assert_fresh_shape(store.pool(), backend).await;
});

all_dbs!(concurrent_starts_create_an_empty_database_once, backend, {
    let db = TestDb::create(backend).await;
    let store = Store::connect(&db.url).await.unwrap();
    race_starters(backend, &db.url, store.pool()).await;
    assert_eq!(recorded(store.pool()).await, versions(backend));
    assert_fresh_shape(store.pool(), backend).await;
});

all_dbs!(
    up_to_date_start_takes_no_lock_and_writes_nothing,
    backend,
    {
        let db = TestDb::create(backend).await;
        let store = db.open().await;
        let before = fingerprint(store.pool(), backend).await;
        let holder = hold_migration_lock(backend, &db.url).await;
        let again = Store::connect(&db.url).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), again.migrate())
            .await
            .expect("an up-to-date start waited for the migration lock")
            .unwrap();
        holder.close().await.unwrap();

        // A user who can only read proves there is not even one write to fail.
        let reader = format!("ro_{}", &uuid::Uuid::new_v4().simple().to_string()[..16]);
        let admin = store.pool();
        match backend {
            Backend::Sqlite => {
                let version: i64 = sqlx::query_scalar("PRAGMA schema_version")
                    .fetch_one(admin)
                    .await
                    .unwrap();
                again.migrate().await.unwrap();
                let after: i64 = sqlx::query_scalar("PRAGMA schema_version")
                    .fetch_one(admin)
                    .await
                    .unwrap();
                assert_eq!(version, after);
            }
            Backend::MySql => {
                let database = db.mysql_database.as_deref().unwrap();
                sqlx::raw_sql(AssertSqlSafe(format!(
                "CREATE USER '{reader}'@'%' IDENTIFIED BY 'pw'; GRANT SELECT ON {database}.* TO '{reader}'@'%'"
            )))
            .execute(admin)
            .await
            .unwrap();
                let (_, host) = db.url.split_once('@').unwrap();
                let limited = Store::connect(&format!("mysql://{reader}:pw@{host}"))
                    .await
                    .unwrap();
                let result = limited.migrate().await;
                limited.pool().close().await;
                sqlx::raw_sql(AssertSqlSafe(format!("DROP USER '{reader}'@'%'")))
                    .execute(admin)
                    .await
                    .unwrap();
                result.unwrap();
            }
            Backend::Postgres => {
                sqlx::raw_sql(AssertSqlSafe(format!(
                "CREATE ROLE {reader} LOGIN PASSWORD 'pw'; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {reader}"
            )))
            .execute(admin)
            .await
            .unwrap();
                let (_, host) = db.url.split_once('@').unwrap();
                let limited = Store::connect(&format!("postgres://{reader}:pw@{host}"))
                    .await
                    .unwrap();
                let result = limited.migrate().await;
                limited.pool().close().await;
                sqlx::raw_sql(AssertSqlSafe(format!(
                    "DROP OWNED BY {reader}; DROP ROLE {reader}"
                )))
                .execute(admin)
                .await
                .unwrap();
                result.unwrap();
            }
        }
        assert_eq!(before, fingerprint(store.pool(), backend).await);
        assert_eq!(recorded(store.pool()).await, versions(backend));
    }
);

fn stopped(error: &Error) -> bool {
    matches!(error, Error::Migration(message) if message == "stopped on request")
}

async fn resume_shapes(backend: Backend) -> Vec<&'static str> {
    match backend {
        Backend::Sqlite => vec![shape(backend, "f176afd"), shape(backend, "893a6fc")],
        Backend::MySql => vec![shape(backend, "f9f7144"), shape(backend, "893a6fc")],
        Backend::Postgres => vec![shape(backend, "fddbf8f")],
    }
}

all_dbs!(an_upgrade_stopped_at_any_step_resumes, backend, {
    for schema in resume_shapes(backend).await {
        let mut stop_after = 0;
        loop {
            let (db, store, seeded) = old_database(backend, schema).await;
            let result = store.migrate_stopping_after(stop_after).await;
            let finished = match result {
                Ok(()) => true,
                Err(error) => {
                    assert!(stopped(&error), "stopped after {stop_after}: {error}");
                    false
                }
            };
            let next = Store::connect(&db.url).await.unwrap();
            next.migrate().await.unwrap_or_else(|error| {
                panic!("resuming after {stop_after} steps: {error}");
            });
            assert_eq!(recorded(next.pool()).await, versions(backend));
            assert_rows_survived(next.pool(), backend, seeded).await;
            assert_fresh_shape(next.pool(), backend).await;
            assert_new_engine_works(&db, seeded).await;
            next.pool().close().await;
            store.pool().close().await;
            if finished {
                break;
            }
            stop_after += 1;
        }
        assert!(stop_after > 1, "the stopping point never reached a step");
    }
});

all_dbs!(an_upgrade_stopped_again_and_again_finishes, backend, {
    for schema in resume_shapes(backend).await {
        let (db, store, seeded) = old_database(backend, schema).await;
        let mut runs = 0;
        loop {
            runs += 1;
            assert!(runs < 40, "the upgrade made no progress");
            let next = Store::connect(&db.url).await.unwrap();
            let result = next.migrate_stopping_after(3).await;
            next.pool().close().await;
            match result {
                Ok(()) => break,
                Err(error) => assert!(stopped(&error), "{error}"),
            }
        }
        assert_eq!(recorded(store.pool()).await, versions(backend));
        assert_rows_survived(store.pool(), backend, seeded).await;
        assert_fresh_shape(store.pool(), backend).await;
        store.pool().close().await;
    }
});

all_dbs!(
    a_failed_upgrade_changes_nothing_and_can_be_retried,
    backend,
    {
        let (db, store, seeded) = old_database(backend, oldest_current_named(backend)).await;
        // A row the new CHECK rejects makes the status migration fail part way.
        let pool = store.pool();
        let mut weird = workflows(PORTABLE_JSON, false, false).remove(1);
        weird[0].1 = text("wf-weird");
        weird[2].1 = text("WEIRD");
        match backend {
            Backend::Sqlite => {
                let mut conn = pool.acquire().await.unwrap();
                sqlx::raw_sql("PRAGMA ignore_check_constraints = ON")
                    .execute(&mut *conn)
                    .await
                    .unwrap();
                sqlx::query("INSERT INTO workflow_status (workflow_uuid, name, status, inputs, serialization, max_recovery_attempts, created_at, updated_at) VALUES ('wf-weird', 'wf', 'WEIRD', '{}', 'portable_json', 1, 0, 0)")
                .execute(&mut *conn)
                .await
                .unwrap();
                sqlx::raw_sql("PRAGMA ignore_check_constraints = OFF")
                    .execute(&mut *conn)
                    .await
                    .unwrap();
            }
            Backend::MySql => {
                sqlx::raw_sql("ALTER TABLE workflow_status ALTER CHECK ck_workflow_status_status NOT ENFORCED")
                .execute(pool)
                .await
                .unwrap();
                insert(pool, backend, "workflow_status", &weird).await;
            }
            Backend::Postgres => {
                sqlx::raw_sql(
                    "ALTER TABLE workflow_status DROP CONSTRAINT workflow_status_status_check",
                )
                .execute(pool)
                .await
                .unwrap();
                insert(pool, backend, "workflow_status", &weird).await;
                sqlx::raw_sql("ALTER TABLE workflow_status ADD CONSTRAINT workflow_status_status_check CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')) NOT VALID")
                .execute(pool)
                .await
                .unwrap();
            }
        }
        let error = store.migrate().await.unwrap_err();
        assert!(matches!(error, Error::Database(_)), "{error}");
        assert_eq!(recorded(pool).await, vec![1], "only 001 finished");

        // The old shape still works, and still refuses what it refused before.
        let cancelled =
            "UPDATE workflow_status SET status = 'CANCELLED' WHERE workflow_uuid = 'wf-recover'";
        if !matches!(backend, Backend::MySql) {
            assert!(sqlx::raw_sql(cancelled).execute(pool).await.is_err());
        }
        let weird_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM workflow_status WHERE status = 'WEIRD'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(weird_rows, 1);

        sqlx::raw_sql("DELETE FROM workflow_status WHERE status = 'WEIRD'")
            .execute(pool)
            .await
            .unwrap();
        store.migrate().await.unwrap();
        assert_eq!(recorded(pool).await, versions(backend));
        assert_rows_survived(pool, backend, seeded).await;
        assert_new_engine_works(&db, seeded).await;
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_rebuild_keeps_cascade_children_and_their_cascade() {
    let backend = Backend::Sqlite;
    // Old CHECK, and every child table in place: the shape that loses rows if the rebuild is careless.
    let (db, store, _) = old_database(backend, shape(backend, "893a6fc")).await;
    let pool = store.pool();
    sqlx::raw_sql(AssertSqlSafe(current_init(backend).to_owned()))
        .execute(pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE INDEX ix_user_added ON workflow_status (name); \
         CREATE TABLE audit (id TEXT); \
         CREATE TRIGGER tr_user_added AFTER DELETE ON workflow_status BEGIN INSERT INTO audit VALUES (old.workflow_uuid); END;",
    )
    .execute(pool)
    .await
    .unwrap();

    let child_tables = [
        "operation_outputs",
        "notifications",
        "workflow_events",
        "workflow_timeouts",
        "workflow_event_history",
        "workflow_queue_entries",
    ];
    for row in notifications() {
        insert(pool, backend, "notifications", &row).await;
    }
    for row in events() {
        insert(pool, backend, "workflow_events", &row).await;
    }
    for row in timeouts() {
        insert(pool, backend, "workflow_timeouts", &row).await;
    }
    for row in event_history() {
        insert(pool, backend, "workflow_event_history", &row).await;
    }
    for row in queues() {
        insert(pool, backend, "workflow_queues", &row).await;
    }
    let mut entry = queue_entries().remove(0);
    entry[1].1 = text("wf-pending");
    insert(pool, backend, "workflow_queue_entries", &entry).await;

    async fn counts(pool: &AnyPool, tables: &[&str]) -> Vec<i64> {
        let mut counts = Vec::new();
        for table in tables {
            let count: i64 =
                sqlx::query_scalar(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                    .fetch_one(pool)
                    .await
                    .unwrap();
            counts.push(count);
        }
        counts
    }
    let rowids = "SELECT workflow_uuid || ':' || rowid FROM workflow_status ORDER BY workflow_uuid";
    let rowids_before: Vec<String> = sqlx::query_scalar(rowids).fetch_all(pool).await.unwrap();
    let before = counts(pool, &child_tables).await;
    assert_eq!(before, vec![4, 2, 1, 1, 1, 1]);

    store.migrate().await.unwrap();
    assert_eq!(
        counts(pool, &child_tables).await,
        before,
        "children were emptied"
    );
    let rowids_after: Vec<String> = sqlx::query_scalar(rowids).fetch_all(pool).await.unwrap();
    assert_eq!(rowids_before, rowids_after);
    let check: Vec<String> = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE name IN ('workflow_status', 'ix_user_added', 'tr_user_added', 'ix_workflow_status_claim') ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        check.len(),
        4,
        "the rebuild lost an index or trigger: {check:?}"
    );
    assert!(check.iter().any(|sql| sql.contains("CANCELLED")));

    let enabled: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(enabled, 1, "foreign keys stayed off on the pool");
    sqlx::raw_sql("DELETE FROM workflow_status WHERE workflow_uuid = 'wf-pending'")
        .execute(pool)
        .await
        .unwrap();
    assert_eq!(
        counts(pool, &child_tables).await,
        vec![0, 0, 0, 0, 0, 0],
        "the children no longer cascade from the rebuilt table"
    );
    drop(db);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_refuses_rows_with_a_missing_parent_and_rolls_back() {
    let backend = Backend::Sqlite;
    let (_db, store, seeded) = old_database(backend, shape(backend, "893a6fc")).await;
    let mut conn = store.pool().acquire().await.unwrap();
    sqlx::raw_sql(
        "PRAGMA foreign_keys = OFF; \
         INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, serialization, completed_at_epoch_ms) VALUES ('gone', 1, 's', 'portable_json', 0); \
         PRAGMA foreign_keys = ON",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    drop(conn);
    let error = store.migrate().await.unwrap_err();
    assert!(matches!(error, Error::Migration(_)), "{error}");
    let version_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE name = 'dwos_schema_migrations'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(version_tables, 0, "the first migration was not rolled back");
    let check: String =
        sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE name = 'workflow_status'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert!(!check.contains("CANCELLED"));
    sqlx::raw_sql("DELETE FROM operation_outputs WHERE workflow_uuid = 'gone'")
        .execute(store.pool())
        .await
        .unwrap();
    store.migrate().await.unwrap();
    assert_rows_survived(store.pool(), backend, seeded).await;
}

all_dbs!(
    a_current_database_with_no_version_record_is_not_rewritten,
    backend,
    {
        let (db, store, seeded) = old_database(backend, shape(backend, "5ff4d0c")).await;
        let identity = match backend {
            Backend::Sqlite => {
                "SELECT rootpage || ':' || sql FROM sqlite_master WHERE name = 'workflow_status'"
            }
            Backend::MySql => {
                "SELECT CAST(GROUP_CONCAT(TABLE_ID ORDER BY NAME) AS CHAR) FROM information_schema.INNODB_TABLES \
             WHERE NAME IN (CONCAT(DATABASE(), '/workflow_status'), CONCAT(DATABASE(), '/operation_outputs'))"
            }
            Backend::Postgres => {
                "SELECT oid::text FROM pg_constraint WHERE conname = 'workflow_status_status_check'"
            }
        };
        let before: String = sqlx::query_scalar(identity)
            .fetch_one(store.pool())
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let after: String = sqlx::query_scalar(identity)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(before, after, "a table that needed nothing was changed");
        assert_eq!(recorded(store.pool()).await, versions(backend));
        assert_rows_survived(store.pool(), backend, seeded).await;
        drop(db);
    }
);

mod failed_legacy_copy {
    use super::*;

    async fn check(backend: Backend, trigger: &str) {
        let (db, store, seeded) = old_database(backend, shapes(backend)[1].1).await;
        let pool = store.pool();
        sqlx::raw_sql(AssertSqlSafe(current_init(backend).to_owned()))
            .execute(pool)
            .await
            .unwrap();
        // Fails the second of the copy's two inserts, after the first has run.
        sqlx::raw_sql(AssertSqlSafe(trigger.to_owned()))
            .execute(pool)
            .await
            .unwrap();
        store.migrate().await.unwrap_err();
        let copied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_status")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(copied, 0, "half of the copy stayed");
        sqlx::raw_sql("DROP TRIGGER refuse_steps")
            .execute(pool)
            .await
            .unwrap();
        store.migrate().await.unwrap();
        assert_rows_survived(pool, backend, seeded).await;
        assert_new_engine_works(&db, seeded).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_copies_nothing() {
        check(
            Backend::Sqlite,
            "CREATE TRIGGER refuse_steps BEFORE INSERT ON operation_outputs BEGIN SELECT RAISE(ABORT, 'no'); END",
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mysql_copies_nothing() {
        check(
            Backend::MySql,
            "CREATE TRIGGER refuse_steps BEFORE INSERT ON operation_outputs FOR EACH ROW SIGNAL SQLSTATE '45000'",
        )
        .await;
    }
}

mod legacy_conflict {
    use super::*;

    async fn check(backend: Backend) {
        let (_db, store, _) = old_database(backend, shapes(backend)[1].1).await;
        sqlx::raw_sql(AssertSqlSafe(current_init(backend).to_owned()))
            .execute(store.pool())
            .await
            .unwrap();
        let mut other = workflows(PORTABLE_JSON, false, false).remove(1);
        other[1].1 = text("other_function");
        insert(store.pool(), backend, "workflow_status", &other).await;

        let error = store.migrate().await.unwrap_err();
        assert!(matches!(error, Error::Migration(_)), "{error}");
        let copied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_status")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(copied, 1, "nothing is copied when IDs conflict");
        let old: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dbos_workflows")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(old, 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_refuses_it() {
        check(Backend::Sqlite).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mysql_refuses_it() {
        check(Backend::MySql).await;
    }
}

async fn assert_refused(db: &TestDb, check: impl Fn(&Error) -> bool) {
    let writer = Engine::connect(
        &db.url,
        EngineConfig {
            run_migrations: false,
            ..config()
        },
    )
    .await
    .err()
    .unwrap();
    assert!(check(&writer), "run_migrations = false: {writer}");
    let reader = Engine::connect(
        &db.url,
        EngineConfig {
            read_only: true,
            run_migrations: false,
            ..config()
        },
    )
    .await
    .err()
    .unwrap();
    assert!(check(&reader), "read_only: {reader}");
    let reader = Engine::connect(
        &db.url,
        EngineConfig {
            read_only: true,
            run_migrations: true,
            ..config()
        },
    )
    .await
    .err()
    .unwrap();
    assert!(check(&reader), "read_only with migrations on: {reader}");
    let store = Store::connect_read_only(&db.url).await.err().unwrap();
    assert!(check(&store), "Store::connect_read_only: {store}");
}

all_dbs!(a_newer_schema_is_refused_by_everyone, backend, {
    let (db, store) = setup(backend).await;
    start(&store, &workflow("a", LONG)).await;
    sqlx::raw_sql("INSERT INTO dwos_schema_migrations (version) VALUES (999)")
        .execute(store.pool())
        .await
        .unwrap();
    let structure = fingerprint(store.pool(), backend).await;
    let is_too_new = |error: &Error| matches!(error, Error::SchemaTooNew { found: 999, supported } if *supported == latest(backend));
    let error = store.migrate().await.unwrap_err();
    assert!(is_too_new(&error), "{error}");
    assert!(error.to_string().contains("999"), "{error}");
    let error = Engine::connect(&db.url, config()).await.err().unwrap();
    assert!(is_too_new(&error), "{error}");
    assert_refused(&db, is_too_new).await;
    assert_eq!(fingerprint(store.pool(), backend).await, structure);
    let mut expected = versions(backend);
    expected.push(999);
    assert_eq!(recorded(store.pool()).await, expected);
    assert_eq!(
        store.get_workflow("a").await.unwrap().unwrap().status,
        Status::Pending
    );
});

all_dbs!(
    sessions_that_cannot_migrate_refuse_an_old_schema_and_write_nothing,
    backend,
    {
        let (db, store, seeded) = old_database(backend, oldest_current_named(backend)).await;
        let tables = |pool: AnyPool| async move {
            let sql = match backend {
                Backend::Sqlite => "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
                Backend::MySql => {
                    "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()"
                }
                Backend::Postgres => {
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema()"
                }
            };
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(&pool)
                .await
                .unwrap()
        };
        let before = tables(store.pool().clone()).await;
        let required = latest(backend);
        assert_refused(&db, |error| {
        matches!(error, Error::SchemaOutdated { found: 0, required: r } if *r == required)
    })
    .await;
        assert_eq!(
            tables(store.pool().clone()).await,
            before,
            "a session created a table"
        );

        // A half-finished upgrade reports how far it got, and the readers still do not touch it.
        store.migrate_stopping_after(2).await.unwrap_err();
        assert_refused(&db, |error| {
        matches!(error, Error::SchemaOutdated { found: 1, required: r } if *r == required)
    })
    .await;
        assert_eq!(recorded(store.pool()).await, vec![1]);

        store.migrate().await.unwrap();
        assert_rows_survived(store.pool(), backend, seeded).await;
        let reader = Engine::connect(
            &db.url,
            EngineConfig {
                read_only: true,
                ..config()
            },
        )
        .await
        .unwrap();
        let rows = reader
            .list_workflows(&WorkflowFilter::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), seeded.expected_workflows().len());
    }
);

all_dbs!(a_read_only_store_never_migrates, backend, {
    let (db, store) = setup(backend).await;
    let reader = Store::connect_read_only(&db.url).await.unwrap();
    reader.migrate().await.unwrap();
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DELETE FROM dwos_schema_migrations WHERE version = {}",
        latest(backend)
    )))
    .execute(store.pool())
    .await
    .unwrap();
    let error = reader.migrate().await.unwrap_err();
    assert!(matches!(error, Error::SchemaOutdated { .. }), "{error}");
    let mut expected = versions(backend);
    expected.pop();
    assert_eq!(recorded(store.pool()).await, expected);
});

all_dbs!(
    sessions_that_cannot_migrate_may_open_an_empty_database,
    backend,
    {
        let db = TestDb::create(backend).await;
        let writer = Store::connect(&db.url).await.unwrap();
        let reader = Engine::connect(
            &db.url,
            EngineConfig {
                read_only: true,
                ..config()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            reader.get_status("a").await,
            Err(Error::Database(_))
        ));
        let tables: Vec<String> = match backend {
        Backend::Sqlite => sqlx::query_scalar("SELECT name FROM sqlite_master"),
        Backend::MySql => sqlx::query_scalar(
            "SELECT CAST(TABLE_NAME AS CHAR) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()",
        ),
        Backend::Postgres => sqlx::query_scalar(
            "SELECT table_name::text FROM information_schema.tables WHERE table_schema = current_schema()",
        ),
    }
    .fetch_all(writer.pool())
    .await
    .unwrap();
        assert!(tables.is_empty(), "{tables:?}");
    }
);
