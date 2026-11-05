use std::time::Duration;

use sqlx::{AnyConnection, AssertSqlSafe, Connection};

use super::{Dialect, Store};
use crate::Error;

// A big table rewrite can take this long while other starters wait for their turn.
const LOCK_WAIT: Duration = Duration::from_secs(3600);
const POSTGRES_LOCK: &str = "SET LOCAL lock_timeout = '1h'; \
     SELECT 1 FROM (SELECT pg_advisory_xact_lock(7240319)) AS locked; \
     SET LOCAL lock_timeout = '10s'";
// GET_LOCK names are per server and at most 64 characters, so hash the database name into it.
const MYSQL_LOCK: &str = "SELECT GET_LOCK(SHA1(CONCAT('dwos_migrate:', DATABASE())), ?)";
const MYSQL_UNLOCK: &str = "SELECT RELEASE_LOCK(SHA1(CONCAT('dwos_migrate:', DATABASE())))";
// An ALTER waiting on a long transaction queues every later query behind it, so give up early and retry at the next start.
const MYSQL_DDL_WAIT: &str = "SET SESSION lock_wait_timeout = 60";
const CREATE_VERSIONS: &str =
    "CREATE TABLE IF NOT EXISTS dwos_schema_migrations (version BIGINT NOT NULL PRIMARY KEY)";

/// `pending` counts what the migration would change, so a version-less or half-upgraded database
/// runs only what it needs. `conflict` counts rows that must stop the upgrade. `rebuilds` names a
/// SQLite table whose indexes and triggers the SQL drops, for the runner to restore.
struct Migration {
    version: i64,
    sql: &'static str,
    pending: Option<&'static str>,
    conflict: Option<&'static str>,
    rebuilds: Option<&'static str>,
}

const fn plain(version: i64, sql: &'static str, pending: &'static str) -> Migration {
    Migration {
        version,
        sql,
        pending: Some(pending),
        conflict: None,
        rebuilds: None,
    }
}

const fn init(sql: &'static str) -> Migration {
    Migration {
        version: 1,
        sql,
        pending: None,
        conflict: None,
        rebuilds: None,
    }
}

// A number means the same change on every database. A database that never had a shape skips its number.
const POSTGRES: &[Migration] = &[
    init(include_str!("../../schema/postgres/001_init.sql")),
    plain(
        2,
        include_str!("../../schema/postgres/002_status_cancelled.sql"),
        "SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'workflow_status'::regclass \
         AND conname = 'workflow_status_status_check' AND pg_get_constraintdef(oid) NOT LIKE '%CANCELLED%'",
    ),
    plain(
        7,
        include_str!("../../schema/postgres/007_workflow_columns.sql"),
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'workflow_status' AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'workflow_status' AND column_name = 'started_at_epoch_ms')",
    ),
    plain(
        8,
        include_str!("../../schema/postgres/008_step_started_at.sql"),
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'operation_outputs' AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'operation_outputs' AND column_name = 'started_at_epoch_ms')",
    ),
    plain(
        9,
        include_str!("../../schema/postgres/009_queue_entry_columns.sql"),
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'workflow_queue_entries' AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'workflow_queue_entries' AND column_name = 'priority')",
    ),
    plain(
        10,
        include_str!("../../schema/postgres/010_queue_columns.sql"),
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'workflow_queues' AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'workflow_queues' AND column_name = 'worker_concurrency')",
    ),
    plain(
        11,
        include_str!("../../schema/postgres/011_workflow_attributes.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'workflow_attributes')",
    ),
    plain(
        12,
        include_str!("../../schema/postgres/012_workflow_indexes.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'ix_workflow_status_parent')",
    ),
    plain(
        13,
        include_str!("../../schema/postgres/013_workflow_schedules.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'workflow_schedules')",
    ),
    plain(
        14,
        include_str!("../../schema/postgres/014_workflow_completed_at.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'ix_workflow_status_completed_at')",
    ),
    plain(
        15,
        include_str!("../../schema/postgres/015_workflow_lease_token.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'ix_workflow_status_lease_token')",
    ),
    plain(
        20,
        include_str!("../../schema/postgres/020_queue_admissions.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'workflow_queue_admissions')",
    ),
    plain(
        25,
        include_str!("../../schema/postgres/025_workflow_attributes_json.sql"),
        "SELECT CASE WHEN (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'workflow_status' AND column_name = 'attributes') = 0 THEN 1 ELSE 0 END",
    ),
];

const MYSQL: &[Migration] = &[
    init(include_str!("../../schema/mysql/001_init.sql")),
    plain(
        2,
        include_str!("../../schema/mysql/002_status_cancelled.sql"),
        "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() \
         AND CONSTRAINT_NAME = 'ck_workflow_status_status' AND CHECK_CLAUSE NOT LIKE '%CANCELLED%'",
    ),
    plain(
        3,
        include_str!("../../schema/mysql/003_workflow_counters_bigint.sql"),
        "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_NAME = 'workflow_status' AND COLUMN_NAME IN ('recovery_attempts', 'max_recovery_attempts') \
         AND COLUMN_TYPE <> 'bigint'",
    ),
    plain(
        4,
        include_str!("../../schema/mysql/004_step_id_bigint.sql"),
        "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_NAME = 'operation_outputs' AND COLUMN_NAME = 'function_id' AND COLUMN_TYPE <> 'bigint'",
    ),
    plain(
        5,
        include_str!("../../schema/mysql/005_legacy_workflow_columns.sql"),
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() \
         AND TABLE_NAME = 'dbos_workflows' AND NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dbos_workflows' AND COLUMN_NAME = 'parent_workflow_id')",
    ),
    Migration {
        version: 6,
        sql: include_str!("../../schema/mysql/006_copy_legacy_tables.sql"),
        pending: Some(
            "SELECT COUNT(*) FROM information_schema.TABLES \
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dbos_workflows'",
        ),
        conflict: Some(
            "SELECT COUNT(*) FROM dbos_workflows old JOIN workflow_status new_row ON new_row.workflow_uuid = old.workflow_id \
             WHERE new_row.name <> old.name OR new_row.inputs <> old.input \
             OR new_row.created_at <> CAST(UNIX_TIMESTAMP(old.created_at) * 1000 AS SIGNED)",
        ),
        rebuilds: None,
    },
    plain(
        7,
        include_str!("../../schema/mysql/007_workflow_columns.sql"),
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_status' AND NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_status' AND COLUMN_NAME = 'started_at_epoch_ms')",
    ),
    plain(
        8,
        include_str!("../../schema/mysql/008_step_started_at.sql"),
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'operation_outputs' AND NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'operation_outputs' AND COLUMN_NAME = 'started_at_epoch_ms')",
    ),
    plain(
        9,
        include_str!("../../schema/mysql/009_queue_entry_columns.sql"),
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_queue_entries' AND NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_queue_entries' AND COLUMN_NAME = 'priority')",
    ),
    plain(
        10,
        include_str!("../../schema/mysql/010_queue_columns.sql"),
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_queues' AND NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_queues' AND COLUMN_NAME = 'worker_concurrency')",
    ),
    plain(
        11,
        include_str!("../../schema/mysql/011_workflow_attributes.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_attributes')",
    ),
    plain(
        12,
        include_str!("../../schema/mysql/012_workflow_indexes.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_status' AND INDEX_NAME = 'ix_workflow_status_parent')",
    ),
    plain(
        13,
        include_str!("../../schema/mysql/013_workflow_schedules.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_schedules')",
    ),
    plain(
        14,
        include_str!("../../schema/mysql/014_workflow_completed_at.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_status' AND INDEX_NAME = 'ix_workflow_status_completed_at')",
    ),
    plain(
        15,
        include_str!("../../schema/mysql/015_workflow_lease_token.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_status' AND INDEX_NAME = 'ix_workflow_status_lease_token')",
    ),
    plain(
        20,
        include_str!("../../schema/mysql/020_queue_admissions.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'workflow_queue_admissions')",
    ),
    plain(
        25,
        include_str!("../../schema/mysql/025_workflow_attributes_json.sql"),
        "SELECT CASE WHEN (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'workflow_status' AND column_name = 'attributes') = 0 THEN 1 ELSE 0 END",
    ),
];

const SQLITE: &[Migration] = &[
    init(include_str!("../../schema/sqlite/001_init.sql")),
    Migration {
        version: 2,
        sql: include_str!("../../schema/sqlite/002_status_cancelled.sql"),
        pending: Some(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'workflow_status' \
             AND sql NOT LIKE '%CANCELLED%'",
        ),
        conflict: None,
        rebuilds: Some("workflow_status"),
    },
    plain(
        5,
        include_str!("../../schema/sqlite/005_legacy_workflow_columns.sql"),
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'dbos_workflows' \
         AND sql NOT LIKE '%parent_workflow_id%'",
    ),
    Migration {
        version: 6,
        sql: include_str!("../../schema/sqlite/006_copy_legacy_tables.sql"),
        pending: Some(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'dbos_workflows'",
        ),
        conflict: Some(
            "SELECT COUNT(*) FROM dbos_workflows AS old JOIN workflow_status AS new_row ON new_row.workflow_uuid = old.workflow_id \
             WHERE new_row.name <> old.name OR new_row.inputs <> old.input OR new_row.created_at <> old.created_at_ms",
        ),
        rebuilds: None,
    },
    plain(
        7,
        include_str!("../../schema/sqlite/007_workflow_columns.sql"),
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'workflow_status' AND sql NOT LIKE '%started_at_epoch_ms%'",
    ),
    plain(
        8,
        include_str!("../../schema/sqlite/008_step_started_at.sql"),
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'operation_outputs' AND sql NOT LIKE '%started_at_epoch_ms%'",
    ),
    plain(
        9,
        include_str!("../../schema/sqlite/009_queue_entry_columns.sql"),
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'workflow_queue_entries' AND sql NOT LIKE '%priority%'",
    ),
    plain(
        10,
        include_str!("../../schema/sqlite/010_queue_columns.sql"),
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'workflow_queues' AND sql NOT LIKE '%worker_concurrency%'",
    ),
    plain(
        11,
        include_str!("../../schema/sqlite/011_workflow_attributes.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'workflow_attributes')",
    ),
    plain(
        12,
        include_str!("../../schema/sqlite/012_workflow_indexes.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'ix_workflow_status_parent')",
    ),
    plain(
        13,
        include_str!("../../schema/sqlite/013_workflow_schedules.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'workflow_schedules')",
    ),
    plain(
        14,
        include_str!("../../schema/sqlite/014_workflow_completed_at.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'ix_workflow_status_completed_at')",
    ),
    plain(
        15,
        include_str!("../../schema/sqlite/015_workflow_lease_token.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'ix_workflow_status_lease_token')",
    ),
    plain(
        20,
        include_str!("../../schema/sqlite/020_queue_admissions.sql"),
        "SELECT COUNT(*) FROM (SELECT 1) AS one WHERE NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'workflow_queue_admissions')",
    ),
    plain(
        25,
        include_str!("../../schema/sqlite/025_workflow_attributes_json.sql"),
        "SELECT CASE WHEN (SELECT COUNT(*) FROM pragma_table_info('workflow_status') WHERE name = 'attributes') = 0 THEN 1 ELSE 0 END",
    ),
];

fn migrations(dialect: Dialect) -> &'static [Migration] {
    match dialect {
        Dialect::Postgres => POSTGRES,
        Dialect::MySql => MYSQL,
        Dialect::Sqlite => SQLITE,
    }
}

fn latest(dialect: Dialect) -> i64 {
    migrations(dialect).last().map_or(0, |last| last.version)
}

async fn count(conn: &mut AnyConnection, sql: &str) -> Result<i64, Error> {
    Ok(sqlx::query_scalar(AssertSqlSafe(sql.to_owned()))
        .fetch_one(conn)
        .await?)
}

async fn table_count(
    conn: &mut AnyConnection,
    dialect: Dialect,
    names: &str,
) -> Result<i64, Error> {
    let sql = match dialect {
        Dialect::Sqlite => {
            format!("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ({names})")
        }
        Dialect::MySql => format!(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ({names})"
        ),
        Dialect::Postgres => format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name IN ({names})"
        ),
    };
    count(conn, &sql).await
}

// None means no version record yet, including a table left empty by a crash.
async fn recorded_version(
    conn: &mut AnyConnection,
    dialect: Dialect,
) -> Result<Option<i64>, Error> {
    if table_count(conn, dialect, "'dwos_schema_migrations'").await? == 0 {
        return Ok(None);
    }
    Ok(
        sqlx::query_scalar("SELECT MAX(version) FROM dwos_schema_migrations")
            .fetch_one(conn)
            .await?,
    )
}

fn refuse_newer(found: i64, dialect: Dialect) -> Result<(), Error> {
    let supported = latest(dialect);
    if found > supported {
        return Err(Error::SchemaTooNew { found, supported });
    }
    Ok(())
}

// Counts the actions a test lets through, to stop where a crash could.
fn spend(budget: &mut usize) -> Result<(), Error> {
    if *budget == 0 {
        return Err(Error::Migration("stopped on request".to_owned()));
    }
    *budget -= 1;
    Ok(())
}

async fn apply(
    conn: &mut AnyConnection,
    dialect: Dialect,
    migration: &Migration,
    budget: &mut usize,
) -> Result<(), Error> {
    // Checked again under the lock: another process may have got here first.
    let found = recorded_version(conn, dialect).await?;
    if let Some(found) = found {
        refuse_newer(found, dialect)?;
        if found >= migration.version {
            return Ok(());
        }
    } else {
        sqlx::raw_sql(CREATE_VERSIONS).execute(&mut *conn).await?;
    }
    let needed = match migration.pending {
        Some(sql) => count(conn, sql).await? > 0,
        None => true,
    };
    if needed {
        if let Some(sql) = migration.conflict {
            let conflicts = count(conn, sql).await?;
            if conflicts > 0 {
                return Err(Error::Migration(format!(
                    "{conflicts} workflow IDs are in both dbos_workflows and workflow_status with different data. \
                     Nothing was copied. Resolve them by hand and start again"
                )));
            }
        }
        spend(budget)?;
        run_sql(conn, migration).await?;
    }
    if dialect == Dialect::Sqlite {
        // Foreign keys are off here, so check what they would have enforced.
        let broken = sqlx::query("PRAGMA foreign_key_check")
            .fetch_optional(&mut *conn)
            .await?;
        if broken.is_some() {
            return Err(Error::Migration(format!(
                "migration {} left rows with a missing parent. It was rolled back",
                migration.version
            )));
        }
    }
    spend(budget)?;
    let record = match dialect {
        Dialect::Postgres => "INSERT INTO dwos_schema_migrations (version) VALUES ($1)",
        Dialect::MySql | Dialect::Sqlite => {
            "INSERT INTO dwos_schema_migrations (version) VALUES (?)"
        }
    };
    sqlx::query(record)
        .bind(migration.version)
        .execute(conn)
        .await?;
    Ok(())
}

async fn run_sql(conn: &mut AnyConnection, migration: &Migration) -> Result<(), Error> {
    let Some(table) = migration.rebuilds else {
        sqlx::raw_sql(AssertSqlSafe(migration.sql))
            .execute(conn)
            .await?;
        return Ok(());
    };
    let restore: Vec<String> = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL",
    )
    .bind(table)
    .fetch_all(&mut *conn)
    .await?;
    sqlx::raw_sql(AssertSqlSafe(migration.sql))
        .execute(&mut *conn)
        .await?;
    for sql in restore {
        sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

impl Store {
    /// Brings the database to the shape this binary needs. Does nothing, and writes nothing, when it is already there.
    pub async fn migrate(&self) -> Result<(), Error> {
        self.run_migrations(usize::MAX).await
    }

    /// Stops after this many migration actions, where a crash could stop. For tests only.
    #[doc(hidden)]
    pub async fn migrate_stopping_after(&self, actions: usize) -> Result<(), Error> {
        self.run_migrations(actions).await
    }

    /// The checks a session that must not migrate runs: only the exact schema, or an empty database, passes.
    pub async fn check_schema(&self) -> Result<(), Error> {
        let required = latest(self.dialect);
        let mut conn = self.pool.acquire().await?;
        let found = recorded_version(&mut conn, self.dialect).await?;
        let found = match found {
            Some(found) => found,
            None if table_count(
                &mut conn,
                self.dialect,
                "'workflow_status', 'dbos_workflows'",
            )
            .await?
                == 0 =>
            {
                return Ok(());
            }
            None => 0,
        };
        refuse_newer(found, self.dialect)?;
        if found < required {
            return Err(Error::SchemaOutdated { found, required });
        }
        Ok(())
    }

    async fn run_migrations(&self, mut budget: usize) -> Result<(), Error> {
        if self.read_only {
            return self.check_schema().await;
        }
        {
            let mut conn = self.pool.acquire().await?;
            if let Some(found) = recorded_version(&mut conn, self.dialect).await? {
                refuse_newer(found, self.dialect)?;
                if found == latest(self.dialect) {
                    return Ok(());
                }
            }
        }
        // Its own connection: it holds the MySQL lock, and turning SQLite's foreign keys off must not reach the pool.
        let mut conn = AnyConnection::connect_with(&self.pool.connect_options()).await?;
        let result = self.upgrade(&mut conn, &mut budget).await;
        // A failed upgrade may have left a transaction or a lock open: closing ends both.
        let closed = conn.close().await;
        result?;
        Ok(closed?)
    }

    async fn upgrade(&self, conn: &mut AnyConnection, budget: &mut usize) -> Result<(), Error> {
        sqlx::raw_sql(self.dialect.session_settings(false))
            .execute(&mut *conn)
            .await?;
        match self.dialect {
            Dialect::Sqlite => {
                let wait = format!(
                    "PRAGMA busy_timeout = {}; PRAGMA foreign_keys = OFF",
                    LOCK_WAIT.as_millis()
                );
                sqlx::raw_sql(AssertSqlSafe(wait))
                    .execute(&mut *conn)
                    .await?;
            }
            Dialect::MySql => {
                sqlx::raw_sql(MYSQL_DDL_WAIT).execute(&mut *conn).await?;
                let locked: Option<i64> = sqlx::query_scalar(MYSQL_LOCK)
                    .bind(i64::try_from(LOCK_WAIT.as_secs()).unwrap_or(i64::MAX))
                    .fetch_one(&mut *conn)
                    .await?;
                if locked != Some(1) {
                    return Err(Error::Timeout(LOCK_WAIT));
                }
            }
            Dialect::Postgres => {}
        }
        for migration in migrations(self.dialect) {
            match self.dialect {
                // DDL commits on its own here. Each migration checks what it would change, so a crash can resume.
                Dialect::MySql => apply(conn, self.dialect, migration, budget).await?,
                Dialect::Sqlite => {
                    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;
                    apply(&mut tx, self.dialect, migration, budget).await?;
                    tx.commit().await?;
                }
                Dialect::Postgres => {
                    let mut tx = conn.begin().await?;
                    sqlx::raw_sql(POSTGRES_LOCK).execute(&mut *tx).await?;
                    apply(&mut tx, self.dialect, migration, budget).await?;
                    tx.commit().await?;
                }
            }
        }
        if self.dialect == Dialect::MySql {
            sqlx::query(MYSQL_UNLOCK).execute(&mut *conn).await?;
        }
        Ok(())
    }
}
