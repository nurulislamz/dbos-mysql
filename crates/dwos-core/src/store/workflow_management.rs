use std::time::Duration;

use sqlx::Row;

use super::{Store, workflow_from_row};
use crate::Error;
use crate::workflow::{Claimed, Workflow, WorkflowFilter, new_token};

const LIST_COLUMNS: &str = "workflow_uuid, name, status, inputs, output, error, recovery_attempts, \
     max_recovery_attempts, parent_workflow_id, created_at, updated_at, completed_at, serialization, (SELECT timeout_ms FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid) AS workflow_timeout_ms, (SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid) AS workflow_deadline_epoch_ms";

fn like_prefix(prefix: &str) -> String {
    let escaped = prefix
        .replace('/', "//")
        .replace('%', "/%")
        .replace('_', "/_");
    format!("{escaped}%")
}

fn in_clause(column: &str, count: usize) -> String {
    if count == 0 {
        return " AND 1 = 0".to_owned();
    }
    format!(" AND {column} IN ({})", vec!["?"; count].join(", "))
}

impl Store {
    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<(), Error> {
        self.attempt(|| async {
            sqlx::query(self.sql(
                "UPDATE workflow_status SET status = 'CANCELLED', lease_token = NULL, claim_after = NULL, \
                 completed_at = {NOW}, updated_at = {NOW} \
                 WHERE workflow_uuid = ? AND status IN ('PENDING', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')",
            ))
            .bind(workflow_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await
    }

    pub async fn resume_workflow(&self, workflow_id: &str) -> Result<(), Error> {
        self.attempt(|| async {
            let mut tx = self.begin().await?;
            let row = sqlx::query(self.sql(&format!(
                "SELECT status FROM workflow_status WHERE workflow_uuid = ? {}",
                self.dialect.lock_row(),
            )))
            .bind(workflow_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| Error::WorkflowNotFound(workflow_id.to_owned()))?;
            let status: String = row.try_get("status")?;
            if status == "CANCELLED" || status == "MAX_RECOVERY_ATTEMPTS_EXCEEDED" {
                sqlx::query(self.sql(
                    "UPDATE workflow_status SET status = 'PENDING', lease_token = NULL, claim_after = {NOW}, \
                     recovery_attempts = 0, completed_at = NULL, updated_at = {NOW} WHERE workflow_uuid = ?",
                ))
                .bind(workflow_id)
                .execute(&mut *tx)
                .await?;
            }
            self.cancel_if_expired_on(&mut tx, workflow_id).await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    /// Takes this pending, due workflow if this process can run it.
    pub async fn claim_one(
        &self,
        workflow_id: &str,
        names: &[String],
        duration: Duration,
    ) -> Result<Option<Claimed>, Error> {
        if names.is_empty() {
            return Ok(None);
        }
        let request_token = new_token();
        let mut retry = false;
        self.attempt(|| {
            let replay = retry;
            retry = true;
            self.claim_one_once(workflow_id, names, duration, &request_token, replay)
        })
        .await
    }

    async fn claim_one_once(
        &self,
        workflow_id: &str,
        names: &[String],
        duration: Duration,
        request_token: &str,
        retry: bool,
    ) -> Result<Option<Claimed>, Error> {
        let mut tx = self.begin().await?;
        if retry
            && let Some(claimed) = self
                .replay_claim_request(&mut tx, request_token, duration)
                .await?
        {
            tx.commit().await?;
            return Ok(claimed.into_iter().next());
        }
        let select_sql = self.sql(&format!(
            "SELECT workflow_uuid, name, inputs, recovery_attempts, max_recovery_attempts, serialization, (SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid) AS workflow_deadline_epoch_ms FROM workflow_status \
             WHERE workflow_uuid = ? AND claim_after <= {{NOW}} AND status = 'PENDING' AND NOT EXISTS \
             (SELECT 1 FROM workflow_queue_entries WHERE workflow_queue_entries.workflow_uuid = workflow_status.workflow_uuid) \
             AND serialization IN ({}) AND name IN ({}) {}",
            self.readable_formats(),
            vec!["?"; names.len()].join(", "),
            self.dialect.lock_free_rows()
        ));
        let mut select = sqlx::query(select_sql).bind(workflow_id);
        for name in names {
            select = select.bind(name.as_str());
        }
        let Some(row) = select.fetch_optional(&mut *tx).await? else {
            tx.commit().await?;
            return Ok(None);
        };
        let claimed = self
            .claim_locked_row(&mut tx, &row, request_token, duration)
            .await?;
        tx.commit().await?;
        Ok(claimed)
    }

    pub async fn list_workflows(&self, filter: &WorkflowFilter) -> Result<Vec<Workflow>, Error> {
        if filter.limit.is_some_and(|value| value < 0)
            || filter.offset.is_some_and(|value| value < 0)
        {
            return Err(Error::InvalidConfig(
                "workflow list limit and offset must be zero or more".to_owned(),
            ));
        }
        self.attempt(|| async { self.list_workflows_once(filter).await })
            .await
    }

    async fn list_workflows_once(&self, filter: &WorkflowFilter) -> Result<Vec<Workflow>, Error> {
        let order = if filter.sort_desc { "DESC" } else { "ASC" };
        let mut sql = format!("SELECT {LIST_COLUMNS} FROM workflow_status WHERE 1 = 1");
        if let Some(ids) = &filter.workflow_ids {
            sql.push_str(&in_clause("workflow_uuid", ids.len()));
        }
        if let Some(statuses) = &filter.statuses {
            sql.push_str(&in_clause("status", statuses.len()));
        }
        if let Some(names) = &filter.names {
            sql.push_str(&in_clause("name", names.len()));
        }
        if let Some(parents) = &filter.parent_workflow_ids {
            sql.push_str(&in_clause("parent_workflow_id", parents.len()));
        }
        if let Some(prefixes) = &filter.workflow_id_prefixes {
            if prefixes.is_empty() {
                sql.push_str(" AND 1 = 0");
            } else {
                sql.push_str(" AND (");
                sql.push_str(&vec!["workflow_uuid LIKE ? ESCAPE '/'"; prefixes.len()].join(" OR "));
                sql.push(')');
            }
        }
        if filter.start_time_ms.is_some() {
            sql.push_str(" AND created_at >= ?");
        }
        if filter.end_time_ms.is_some() {
            sql.push_str(" AND created_at <= ?");
        }
        if filter.completed_after_ms.is_some() {
            sql.push_str(" AND completed_at >= ?");
        }
        if filter.completed_before_ms.is_some() {
            sql.push_str(" AND completed_at <= ?");
        }
        match filter.has_parent {
            Some(true) => sql.push_str(" AND parent_workflow_id IS NOT NULL"),
            Some(false) => sql.push_str(" AND parent_workflow_id IS NULL"),
            None => {}
        }
        sql.push_str(&format!(
            " ORDER BY created_at {order}, workflow_uuid {order}"
        ));
        if filter.limit.is_some() || filter.offset.is_some() {
            sql.push_str(" LIMIT ? OFFSET ?");
        }

        let mut query = sqlx::query(self.sql(&sql));
        if let Some(ids) = &filter.workflow_ids {
            for id in ids {
                query = query.bind(id.as_str());
            }
        }
        if let Some(statuses) = &filter.statuses {
            for status in statuses {
                query = query.bind(status.as_str());
            }
        }
        if let Some(names) = &filter.names {
            for name in names {
                query = query.bind(name.as_str());
            }
        }
        if let Some(parents) = &filter.parent_workflow_ids {
            for parent in parents {
                query = query.bind(parent.as_str());
            }
        }
        let prefix_patterns: Vec<String> = filter
            .workflow_id_prefixes
            .iter()
            .flatten()
            .map(|prefix| like_prefix(prefix))
            .collect();
        for pattern in &prefix_patterns {
            query = query.bind(pattern.as_str());
        }
        if let Some(ms) = filter.start_time_ms {
            query = query.bind(ms);
        }
        if let Some(ms) = filter.end_time_ms {
            query = query.bind(ms);
        }
        if let Some(ms) = filter.completed_after_ms {
            query = query.bind(ms);
        }
        if let Some(ms) = filter.completed_before_ms {
            query = query.bind(ms);
        }
        if filter.limit.is_some() || filter.offset.is_some() {
            query = query.bind(filter.limit.unwrap_or(i64::MAX));
            query = query.bind(filter.offset.unwrap_or(0));
        }

        let rows = query.fetch_all(&self.pool).await?;
        rows.iter().map(workflow_from_row).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{NewWorkflow, PORTABLE_JSON};

    #[tokio::test]
    async fn direct_claim_retries_a_committed_request_without_spending_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::connect(&format!(
            "sqlite://{}",
            directory.path().join("test.sqlite").display()
        ))
        .await
        .unwrap();
        store.migrate().await.unwrap();
        store
            .start_workflow(&NewWorkflow {
                workflow_id: "a",
                name: "wf",
                input: "{}",
                max_recovery_attempts: 1,
                lease: Duration::ZERO,
                parent_workflow_id: None,
                serialization: PORTABLE_JSON,
                timeout_ms: None,
                deadline_epoch_ms: None,
            })
            .await
            .unwrap();
        let names = vec!["wf".to_owned()];
        let request = new_token();
        let duration = Duration::from_secs(60);
        let committed = store
            .claim_one_once("a", &names, duration, &request, false)
            .await
            .unwrap()
            .unwrap();
        let replayed = store
            .claim_one_once("a", &names, duration, &request, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed, committed);
        assert_eq!(replayed.recovery_attempts, 2);
        assert_eq!(
            store
                .get_workflow("a")
                .await
                .unwrap()
                .unwrap()
                .recovery_attempts,
            2
        );
    }
}
