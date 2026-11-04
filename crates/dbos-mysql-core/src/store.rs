use std::str::FromStr;
use std::time::Duration;

use sqlx::Row;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};

use crate::mysql::MySqlStore;
use crate::workflow::{Claimed, Lease, NewWorkflow, Outcome, Started, StepRecord, Workflow};
use crate::{Error, sqlite};

const MAX_NAME_CHARS: usize = 255;

fn check_name(field: &'static str, value: &str) -> Result<(), Error> {
    if value.chars().count() > MAX_NAME_CHARS {
        return Err(Error::NameTooLong { field });
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub enum Store {
    MySql(MySqlStore),
    Sqlite(SqlitePool),
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        if url.starts_with("mysql://") {
            return Ok(Self::MySql(MySqlStore::connect(url).await?));
        }
        if url.starts_with("sqlite://") {
            let options = SqliteConnectOptions::from_str(url)
                .map_err(|_| Error::InvalidUrl)?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Full)
                .foreign_keys(true)
                .busy_timeout(Duration::from_secs(30));
            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .acquire_timeout(Duration::from_secs(40))
                .connect_with(options)
                .await?;
            log::info!("connected to sqlite");
            return Ok(Self::Sqlite(pool));
        }
        Err(Error::UnsupportedUrl)
    }

    pub fn backend(&self) -> &'static str {
        match self {
            Self::MySql(_) => "mysql",
            Self::Sqlite(_) => "sqlite",
        }
    }

    pub fn transient_retries(&self) -> u64 {
        match self {
            Self::MySql(store) => store.transient_retries(),
            Self::Sqlite(_) => 0,
        }
    }

    pub async fn migrate(&self) -> Result<(), Error> {
        match self {
            Self::MySql(store) => store.migrate().await,
            Self::Sqlite(pool) => sqlite::migrate(pool).await,
        }
    }

    pub async fn start_workflow(&self, new: &NewWorkflow<'_>) -> Result<Started, Error> {
        check_name("workflow ID", new.workflow_id)?;
        check_name("workflow name", new.name)?;
        match self {
            Self::MySql(store) => store.start_workflow(new).await,
            Self::Sqlite(pool) => sqlite::start_workflow(pool, new).await,
        }
    }

    pub async fn get_workflow(&self, workflow_id: &str) -> Result<Option<Workflow>, Error> {
        match self {
            Self::MySql(store) => store.get_workflow(workflow_id).await,
            Self::Sqlite(pool) => sqlite::get_workflow(pool, workflow_id).await,
        }
    }

    pub async fn load_step(
        &self,
        workflow_id: &str,
        step_id: u32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        match self {
            Self::MySql(store) => store.load_step(workflow_id, step_id, step_name).await,
            Self::Sqlite(pool) => sqlite::load_step(pool, workflow_id, step_id, step_name).await,
        }
    }

    pub async fn save_step(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        check_name("step name", step_name)?;
        if let Some(child) = &record.child_workflow_id {
            check_name("child workflow ID", child)?;
        }
        match self {
            Self::MySql(store) => store.save_step(lease, step_id, step_name, record).await,
            Self::Sqlite(pool) => sqlite::save_step(pool, lease, step_id, step_name, record).await,
        }
    }

    pub async fn finish_workflow(&self, lease: &Lease, outcome: &Outcome) -> Result<(), Error> {
        match self {
            Self::MySql(store) => store.finish_workflow(lease, outcome).await,
            Self::Sqlite(pool) => sqlite::finish_workflow(pool, lease, outcome).await,
        }
    }

    pub async fn renew_lease(&self, lease: &Lease, duration: Duration) -> Result<(), Error> {
        match self {
            Self::MySql(store) => store.renew_lease(lease, duration).await,
            Self::Sqlite(pool) => sqlite::renew_lease(pool, lease, duration).await,
        }
    }

    pub async fn release_lease(&self, lease: &Lease) -> Result<(), Error> {
        match self {
            Self::MySql(store) => store.release_lease(lease).await,
            Self::Sqlite(pool) => sqlite::release_lease(pool, lease).await,
        }
    }

    pub async fn reclaim(
        &self,
        names: &[String],
        limit: u32,
        duration: Duration,
    ) -> Result<Vec<Claimed>, Error> {
        match self {
            Self::MySql(store) => store.reclaim(names, limit, duration).await,
            Self::Sqlite(pool) => sqlite::reclaim(pool, names, limit, duration).await,
        }
    }

    pub async fn ping(&self) -> Result<i64, Error> {
        let value = match self {
            Self::MySql(store) => sqlx::query("SELECT 1")
                .fetch_one(store.pool())
                .await?
                .try_get::<i64, _>(0)?,
            Self::Sqlite(pool) => sqlx::query("SELECT 1")
                .fetch_one(pool)
                .await?
                .try_get::<i64, _>(0)?,
        };
        Ok(value)
    }

    pub async fn session_settings(&self) -> Result<Vec<String>, Error> {
        match self {
            Self::MySql(store) => {
                let row = sqlx::query(
                    "SELECT @@transaction_isolation, @@session.time_zone, @@session.sql_mode",
                )
                .fetch_one(store.pool())
                .await?;
                Ok(vec![row.try_get(0)?, row.try_get(1)?, row.try_get(2)?])
            }
            Self::Sqlite(pool) => {
                let mut conn = pool.acquire().await?;
                let journal: String = sqlx::query("PRAGMA journal_mode")
                    .fetch_one(&mut *conn)
                    .await?
                    .try_get(0)?;
                let synchronous: i64 = sqlx::query("PRAGMA synchronous")
                    .fetch_one(&mut *conn)
                    .await?
                    .try_get(0)?;
                let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
                    .fetch_one(&mut *conn)
                    .await?
                    .try_get(0)?;
                Ok(vec![
                    journal,
                    synchronous.to_string(),
                    foreign_keys.to_string(),
                ])
            }
        }
    }

    pub async fn spike_reset(&self) -> Result<(), Error> {
        const DROP: &str = "DROP TABLE IF EXISTS dbos_spike";
        const CREATE: &str = "CREATE TABLE dbos_spike (id INT NOT NULL)";
        match self {
            Self::MySql(store) => {
                sqlx::query(DROP).execute(store.pool()).await?;
                sqlx::query(CREATE).execute(store.pool()).await?;
            }
            Self::Sqlite(pool) => {
                sqlx::query(DROP).execute(pool).await?;
                sqlx::query(CREATE).execute(pool).await?;
            }
        }
        Ok(())
    }

    pub async fn spike_hold_transaction(&self, seconds: f64) -> Result<(), Error> {
        const INSERT: &str = "INSERT INTO dbos_spike (id) VALUES (1)";
        let hold = Duration::from_secs_f64(seconds);
        match self {
            Self::MySql(store) => {
                let mut tx = store.pool().begin().await?;
                sqlx::query(INSERT).execute(&mut *tx).await?;
                tokio::time::sleep(hold).await;
                tx.commit().await?;
            }
            Self::Sqlite(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
                sqlx::query(INSERT).execute(&mut *tx).await?;
                tokio::time::sleep(hold).await;
                tx.commit().await?;
            }
        }
        Ok(())
    }

    pub async fn spike_sleep(&self, seconds: f64) -> Result<(), Error> {
        if let Self::MySql(store) = self {
            sqlx::query("SELECT SLEEP(?)")
                .bind(seconds)
                .execute(store.pool())
                .await?;
        }
        Ok(())
    }

    pub async fn spike_rows(&self) -> Result<i64, Error> {
        const COUNT: &str = "SELECT COUNT(*) FROM dbos_spike";
        let value = match self {
            Self::MySql(store) => sqlx::query(COUNT)
                .fetch_one(store.pool())
                .await?
                .try_get::<i64, _>(0)?,
            Self::Sqlite(pool) => sqlx::query(COUNT)
                .fetch_one(pool)
                .await?
                .try_get::<i64, _>(0)?,
        };
        Ok(value)
    }

    pub async fn spike_open_transactions(&self) -> Result<Option<i64>, Error> {
        match self {
            Self::MySql(store) => {
                let row = sqlx::query("SELECT COUNT(*) FROM information_schema.innodb_trx")
                    .fetch_one(store.pool())
                    .await?;
                Ok(Some(row.try_get(0)?))
            }
            Self::Sqlite(_) => Ok(None),
        }
    }

    pub fn connections_in_use(&self) -> u32 {
        match self {
            Self::MySql(store) => store.pool().size() - store.pool().num_idle() as u32,
            Self::Sqlite(pool) => pool.size() - pool.num_idle() as u32,
        }
    }
}
