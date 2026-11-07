use sqlx::{Any, Row, Transaction};

use super::{Dialect, Store, check_name};
use crate::Error;
use crate::workflow::new_token;

/// The newest version, as a scalar subquery. A version-less row belongs to it.
pub(super) const LATEST_VERSION: &str = "(SELECT version_name FROM application_versions \
     ORDER BY version_timestamp DESC, version_name DESC LIMIT 1)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionRow {
    pub version_id: String,
    pub version_name: String,
    pub version_timestamp: i64,
    pub created_at: i64,
}

impl Store {
    /// Records a version the first time it starts, as the newest. A known version keeps its place.
    pub async fn register_version(&self, name: &str) -> Result<(), Error> {
        check_name("application version", name)?;
        let conflict = match self.dialect {
            Dialect::MySql => "ON DUPLICATE KEY UPDATE version_name = version_name",
            Dialect::Postgres | Dialect::Sqlite => "ON CONFLICT (version_name) DO NOTHING",
        };
        self.attempt(|| async {
            let mut tx = self.begin().await?;
            let newest = newest_timestamp(self, &mut tx).await?;
            sqlx::query(self.sql(&format!(
                "INSERT INTO application_versions (version_name, version_id, version_timestamp, created_at) \
                 VALUES (?, ?, ?, {{NOW}}) {conflict}"
            )))
            .bind(name)
            .bind(new_token().as_str())
            .bind(newest)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    /// Makes a known version the newest. A missing name is an error, so a typo is not silent.
    pub async fn set_latest_version(&self, name: &str) -> Result<(), Error> {
        self.attempt(|| async {
            let mut tx = self.begin().await?;
            let newest = newest_timestamp(self, &mut tx).await?;
            let changed = sqlx::query(self.sql(
                "UPDATE application_versions SET version_timestamp = ? WHERE version_name = ?",
            ))
            .bind(newest)
            .bind(name)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if changed == 0 {
                return Err(Error::InvalidConfig(format!(
                    "application version {name} is not registered"
                )));
            }
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    /// Newest first.
    pub async fn list_versions(&self) -> Result<Vec<VersionRow>, Error> {
        self.attempt(|| async {
            let rows = sqlx::query(self.sql(
                "SELECT version_id, version_name, version_timestamp, created_at FROM application_versions \
                 ORDER BY version_timestamp DESC, version_name DESC",
            ))
            .fetch_all(&self.pool)
            .await?;
            rows.iter()
                .map(|row| {
                    Ok(VersionRow {
                        version_id: row.try_get("version_id")?,
                        version_name: row.try_get("version_name")?,
                        version_timestamp: row.try_get("version_timestamp")?,
                        created_at: row.try_get("created_at")?,
                    })
                })
                .collect()
        })
        .await
    }
}

/// The database clock, or one past the newest version when the clock has not passed it, so the
/// version written now is strictly the newest even within one millisecond.
async fn newest_timestamp(store: &Store, tx: &mut Transaction<'static, Any>) -> Result<i64, Error> {
    // Serialize version ordering across workers using the registry's migration row.
    sqlx::query("UPDATE dwos_schema_migrations SET version = version WHERE version = 34")
        .execute(&mut **tx)
        .await?;
    let row = sqlx::query(store.sql(
        "SELECT {NOW} AS now_ms, (SELECT MAX(version_timestamp) FROM application_versions) AS newest",
    ))
    .fetch_one(&mut **tx)
    .await?;
    let now: i64 = row.try_get("now_ms")?;
    let newest: Option<i64> = row.try_get("newest")?;
    Ok(newest.map_or(now, |newest| now.max(newest.saturating_add(1))))
}
