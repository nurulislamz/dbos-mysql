use std::str::FromStr;
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{Executor, Row};

use crate::workflow::{Claimed, Lease, NewWorkflow, Outcome, Started, StepRecord, Workflow};
use crate::{Error, sqlite};

#[derive(Clone, Debug)]
pub enum Store {
    MySql(MySqlPool),
    Sqlite(SqlitePool),
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        if url.starts_with("mysql://") {
            let options = MySqlConnectOptions::from_str(url).map_err(|_| Error::InvalidUrl)?;
            let pool = MySqlPoolOptions::new()
                .max_connections(5)
                .acquire_timeout(Duration::from_secs(10))
                .after_connect(|conn, _| {
                    Box::pin(async move {
                        conn.execute("SET SESSION transaction_isolation = 'READ-COMMITTED', time_zone = '+00:00'")
                            .await?;
                        Ok(())
                    })
                })
                .connect_with(options)
                .await?;
            log::info!("connected to mysql");
            return Ok(Self::MySql(pool));
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
                .acquire_timeout(Duration::from_secs(10))
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

    fn sqlite_pool(&self, operation: &'static str) -> Result<&SqlitePool, Error> {
        match self {
            Self::Sqlite(pool) => Ok(pool),
            Self::MySql(_) => Err(Error::NotImplemented(operation, "mysql")),
        }
    }

    pub async fn migrate(&self) -> Result<(), Error> {
        sqlite::migrate(self.sqlite_pool("migrate")?).await
    }

    pub async fn start_workflow(&self, new: &NewWorkflow<'_>) -> Result<Started, Error> {
        sqlite::start_workflow(self.sqlite_pool("start_workflow")?, new).await
    }

    pub async fn get_workflow(&self, workflow_id: &str) -> Result<Option<Workflow>, Error> {
        sqlite::get_workflow(self.sqlite_pool("get_workflow")?, workflow_id).await
    }

    pub async fn load_step(
        &self,
        workflow_id: &str,
        step_id: u32,
        step_name: &str,
    ) -> Result<Option<StepRecord>, Error> {
        sqlite::load_step(
            self.sqlite_pool("load_step")?,
            workflow_id,
            step_id,
            step_name,
        )
        .await
    }

    pub async fn save_step(
        &self,
        lease: &Lease,
        step_id: u32,
        step_name: &str,
        record: &StepRecord,
    ) -> Result<(), Error> {
        sqlite::save_step(
            self.sqlite_pool("save_step")?,
            lease,
            step_id,
            step_name,
            record,
        )
        .await
    }

    pub async fn finish_workflow(&self, lease: &Lease, outcome: &Outcome) -> Result<(), Error> {
        sqlite::finish_workflow(self.sqlite_pool("finish_workflow")?, lease, outcome).await
    }

    pub async fn renew_lease(&self, lease: &Lease, duration: Duration) -> Result<(), Error> {
        sqlite::renew_lease(self.sqlite_pool("renew_lease")?, lease, duration).await
    }

    pub async fn release_lease(&self, lease: &Lease) -> Result<(), Error> {
        sqlite::release_lease(self.sqlite_pool("release_lease")?, lease).await
    }

    pub async fn reclaim(
        &self,
        names: &[String],
        limit: u32,
        duration: Duration,
    ) -> Result<Vec<Claimed>, Error> {
        sqlite::reclaim(self.sqlite_pool("reclaim")?, names, limit, duration).await
    }

    pub async fn ping(&self) -> Result<i64, Error> {
        let value = match self {
            Self::MySql(pool) => sqlx::query("SELECT 1")
                .fetch_one(pool)
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
            Self::MySql(pool) => {
                let row = sqlx::query("SELECT @@transaction_isolation, @@session.time_zone")
                    .fetch_one(pool)
                    .await?;
                Ok(vec![row.try_get(0)?, row.try_get(1)?])
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
            Self::MySql(pool) => {
                sqlx::query(DROP).execute(pool).await?;
                sqlx::query(CREATE).execute(pool).await?;
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
            Self::MySql(pool) => {
                let mut tx = pool.begin().await?;
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

    pub async fn spike_rows(&self) -> Result<i64, Error> {
        const COUNT: &str = "SELECT COUNT(*) FROM dbos_spike";
        let value = match self {
            Self::MySql(pool) => sqlx::query(COUNT)
                .fetch_one(pool)
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
            Self::MySql(pool) => {
                let row = sqlx::query("SELECT COUNT(*) FROM information_schema.innodb_trx")
                    .fetch_one(pool)
                    .await?;
                Ok(Some(row.try_get(0)?))
            }
            Self::Sqlite(_) => Ok(None),
        }
    }

    pub fn connections_in_use(&self) -> u32 {
        match self {
            Self::MySql(pool) => pool.size() - pool.num_idle() as u32,
            Self::Sqlite(pool) => pool.size() - pool.num_idle() as u32,
        }
    }
}
