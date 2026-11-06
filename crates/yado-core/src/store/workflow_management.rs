use std::collections::HashSet;
use std::time::Duration;

use sqlx::Row;

use super::{SELECT_WORKFLOWS, STATUS_EXPR, Store, workflow_from_row};
use crate::Error;
use crate::workflow::{Claimed, Workflow, WorkflowFilter, new_token};

fn like_prefix(prefix: &str) -> String {
    let escaped = prefix
        .replace('/', "//")
        .replace('%', "/%")
        .replace('_', "/_");
    format!("{escaped}%")
}

pub(super) fn in_clause(column: &str, count: usize) -> String {
    if count == 0 {
        return " AND 1 = 0".to_owned();
    }
    format!(" AND {column} IN ({})", vec!["?"; count].join(", "))
}

impl Store {
    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<(), Error> {
        self.cancel_workflows(&[workflow_id.to_owned()], false)
            .await
            .map(|_| ())
    }

    /// Cancels pending and dead-lettered rows. With `cancel_children`, walks `parent_workflow_id`
    /// until every descendant has been visited. A finished row is left as it is. The returned ids
    /// are the ones the caller should wake.
    pub async fn cancel_workflows(
        &self,
        workflow_ids: &[String],
        cancel_children: bool,
    ) -> Result<Vec<String>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.attempt(|| self.cancel_workflows_once(workflow_ids, cancel_children))
            .await
    }

    async fn cancel_workflows_once(
        &self,
        workflow_ids: &[String],
        cancel_children: bool,
    ) -> Result<Vec<String>, Error> {
        let mut seen = HashSet::new();
        let mut frontier: Vec<String> = workflow_ids.to_vec();
        let mut woke = Vec::new();
        loop {
            frontier.retain(|id| seen.insert(id.clone()));
            if frontier.is_empty() {
                break;
            }
            let sql = format!(
                "UPDATE workflow_status SET status = 'CANCELLED', lease_token = NULL, claim_after = NULL, \
                 completed_at = {{NOW}}, updated_at = {{NOW}} \
                 WHERE status IN ('PENDING', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED'){}",
                in_clause("workflow_uuid", frontier.len())
            );
            let mut update = sqlx::query(self.sql(&sql));
            for id in &frontier {
                update = update.bind(id.as_str());
            }
            update.execute(&self.pool).await?;
            woke.extend(frontier.iter().cloned());
            if !cancel_children {
                break;
            }
            let sql = format!(
                "SELECT workflow_uuid FROM workflow_status WHERE 1 = 1{}",
                in_clause("parent_workflow_id", frontier.len())
            );
            let mut children = sqlx::query_scalar(self.sql(&sql));
            for id in &frontier {
                children = children.bind(id.as_str());
            }
            frontier = children.fetch_all(&self.pool).await?;
        }
        Ok(woke)
    }

    pub async fn require_workflows(&self, workflow_ids: &[String]) -> Result<(), Error> {
        if workflow_ids.is_empty() {
            return Ok(());
        }
        self.attempt(|| async {
            let sql = format!(
                "SELECT workflow_uuid FROM workflow_status WHERE 1 = 1{}",
                in_clause("workflow_uuid", workflow_ids.len())
            );
            let mut query = sqlx::query_scalar(self.sql(&sql));
            for id in workflow_ids {
                query = query.bind(id.as_str());
            }
            let found: HashSet<String> = query.fetch_all(&self.pool).await?.into_iter().collect();
            let missing: Vec<&str> = workflow_ids
                .iter()
                .map(String::as_str)
                .filter(|id| !found.contains(*id))
                .collect();
            if missing.is_empty() {
                Ok(())
            } else {
                Err(Error::WorkflowNotFound(missing.join(", ")))
            }
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
        let (compatible, binds) = self.compatible_clause();
        let select_sql = self.sql(&format!(
            "SELECT workflow_uuid, name, inputs, recovery_attempts, max_recovery_attempts, serialization, (SELECT deadline_epoch_ms FROM workflow_timeouts WHERE workflow_timeouts.workflow_uuid = workflow_status.workflow_uuid) AS workflow_deadline_epoch_ms FROM workflow_status \
             WHERE workflow_uuid = ? AND claim_after <= {{NOW}} AND status = 'PENDING' AND NOT EXISTS \
             (SELECT 1 FROM workflow_queue_entries WHERE workflow_queue_entries.workflow_uuid = workflow_status.workflow_uuid) \
             AND serialization IN ({}) AND name IN ({}){compatible} {}",
            self.readable_formats(),
            vec!["?"; names.len()].join(", "),
            self.dialect.lock_free_rows()
        ));
        let mut select = sqlx::query(select_sql).bind(workflow_id);
        for name in names {
            select = select.bind(name.as_str());
        }
        for bind in binds {
            select = select.bind(bind);
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
        let mut sql = format!("{SELECT_WORKFLOWS} WHERE 1 = 1");
        if let Some(ids) = &filter.workflow_ids {
            sql.push_str(&in_clause("workflow_status.workflow_uuid", ids.len()));
        }
        for (column, values) in [
            ("workflow_queue_entries.queue_name", &filter.queue_names),
            ("application_version", &filter.app_versions),
            ("forked_from", &filter.forked_from),
            ("authenticated_user", &filter.users),
        ] {
            if let Some(values) = values {
                sql.push_str(&in_clause(column, values.len()));
            }
        }
        if filter.queues_only {
            sql.push_str(" AND workflow_queue_entries.queue_name IS NOT NULL");
        }
        if let Some(statuses) = &filter.statuses {
            sql.push_str(&in_clause(&format!("({STATUS_EXPR})"), statuses.len()));
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
                sql.push_str(
                    &vec!["workflow_status.workflow_uuid LIKE ? ESCAPE '/'"; prefixes.len()]
                        .join(" OR "),
                );
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
        match filter.is_fork {
            Some(true) => sql.push_str(" AND forked_from IS NOT NULL"),
            Some(false) => sql.push_str(" AND forked_from IS NULL"),
            None => {}
        }
        match filter.was_forked_from {
            Some(true) => sql.push_str(" AND was_forked_from != 0"),
            Some(false) => sql.push_str(" AND was_forked_from = 0"),
            None => {}
        }
        match filter.has_parent {
            Some(true) => sql.push_str(" AND parent_workflow_id IS NOT NULL"),
            Some(false) => sql.push_str(" AND parent_workflow_id IS NULL"),
            None => {}
        }
        sql.push_str(&format!(
            " ORDER BY created_at {order}, workflow_status.workflow_uuid {order}"
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
        for values in [
            &filter.queue_names,
            &filter.app_versions,
            &filter.forked_from,
            &filter.users,
        ]
        .into_iter()
        .flatten()
        {
            for value in values {
                query = query.bind(value.as_str());
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
