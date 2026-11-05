use std::collections::HashSet;

use super::timeouts::RECHECK_CHUNK;
use super::workflow_management::in_clause;
use super::{Store, check_name};
use crate::Error;

// A parent's replay starts a child again if the child's row is gone.
// shortcut: only `parent_workflow_id` protects a child, so one attached to a second parent by explicit ID can be deleted and rerun; upgrade by indexing and checking `operation_outputs.child_workflow_id`.
const RUNNING_PARENT: &str = "'PENDING'";
// By age, also keep a child whose parent can still be resumed.
const RESUMABLE_PARENT: &str = "'PENDING', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED'";

/// For a read of `workflow_status`. MySQL refuses this form in a DELETE (error 1093).
fn parent_not_in(statuses: &str) -> String {
    format!(
        " AND NOT EXISTS (SELECT 1 FROM workflow_status AS parent \
         WHERE parent.workflow_uuid = workflow_status.parent_workflow_id AND parent.status IN ({statuses}))"
    )
}

/// Binds `count` ids twice. The DISTINCT makes MySQL materialize the derived table, which 1093 allows.
fn delete_stopped_sql(age_check: &str, statuses: &str, count: usize) -> String {
    format!(
        "DELETE FROM workflow_status WHERE {age_check}status <> 'PENDING'{} AND workflow_uuid NOT IN \
         (SELECT kept FROM (SELECT DISTINCT child.workflow_uuid AS kept FROM workflow_status AS child \
         JOIN workflow_status AS parent ON parent.workflow_uuid = child.parent_workflow_id \
         WHERE parent.status IN ({statuses}){}) AS kept_rows)",
        in_clause("workflow_uuid", count),
        in_clause("child.workflow_uuid", count),
    )
}

impl Store {
    /// Deletes these rows and returns the ids actually deleted. A PENDING row, a row whose parent
    /// is PENDING and a missing id are left out. With `delete_children`, also deletes finished
    /// descendants, and does not walk below a PENDING row. Deleting a workflow lets its ID run again.
    /// A child of a CANCELLED or dead-lettered parent can still be deleted, and resuming that parent reruns it.
    pub async fn delete_workflows(
        &self,
        workflow_ids: &[String],
        delete_children: bool,
    ) -> Result<Vec<String>, Error> {
        for id in workflow_ids {
            check_name("workflow ID", id)?;
        }
        let targets = if delete_children {
            self.attempt(|| self.with_descendants(workflow_ids)).await?
        } else {
            workflow_ids.to_vec()
        };
        let mut deleted = Vec::new();
        for chunk in targets.chunks(RECHECK_CHUNK) {
            deleted.extend(self.attempt(|| self.delete_finished(chunk)).await?);
        }
        Ok(deleted)
    }

    /// Deepest first: a call that fails midway can run again and still reach what is left.
    async fn with_descendants(&self, roots: &[String]) -> Result<Vec<String>, Error> {
        let mut pending: HashSet<String> = HashSet::new();
        for chunk in roots.chunks(RECHECK_CHUNK) {
            let sql = format!(
                "SELECT workflow_uuid FROM workflow_status WHERE status = 'PENDING'{}",
                in_clause("workflow_uuid", chunk.len())
            );
            let mut query = sqlx::query_scalar(self.sql(&sql));
            for id in chunk {
                query = query.bind(id.as_str());
            }
            pending.extend(query.fetch_all(&self.pool).await?);
        }
        let mut seen: HashSet<String> = roots.iter().cloned().collect();
        let mut all = roots.to_vec();
        let mut frontier: Vec<String> = roots
            .iter()
            .filter(|id| !pending.contains(*id))
            .cloned()
            .collect();
        while !frontier.is_empty() {
            let mut children = Vec::new();
            for chunk in frontier.chunks(RECHECK_CHUNK) {
                let sql = format!(
                    "SELECT workflow_uuid FROM workflow_status WHERE status <> 'PENDING'{}",
                    in_clause("parent_workflow_id", chunk.len())
                );
                let mut query = sqlx::query_scalar(self.sql(&sql));
                for id in chunk {
                    query = query.bind(id.as_str());
                }
                children.extend(query.fetch_all(&self.pool).await?);
            }
            children.retain(|id: &String| seen.insert(id.clone()));
            all.extend(children.iter().cloned());
            frontier = children;
        }
        all.reverse();
        Ok(all)
    }

    async fn delete_finished(&self, ids: &[String]) -> Result<Vec<String>, Error> {
        let mut tx = self.begin().await?;
        // The lock clause covers these rows, not the parents the subquery reads.
        let sql = format!(
            "SELECT workflow_uuid FROM workflow_status WHERE status <> 'PENDING'{}{} {}",
            parent_not_in(RUNNING_PARENT),
            in_clause("workflow_uuid", ids.len()),
            self.dialect.lock_row(),
        );
        let mut select = sqlx::query_scalar(self.sql(&sql));
        for id in ids {
            select = select.bind(id.as_str());
        }
        let finished: Vec<String> = select.fetch_all(&mut *tx).await?;
        if !finished.is_empty() {
            let sql = delete_stopped_sql("", RUNNING_PARENT, finished.len());
            let mut delete = sqlx::query(self.sql(&sql));
            for id in finished.iter().chain(&finished) {
                delete = delete.bind(id.as_str());
            }
            delete.execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(finished)
    }

    /// Deletes rows that finished before the cutoff, at most `batch` per transaction, and returns
    /// the total. A batch above 200 uses 200, to bound the binds in one statement. A row whose
    /// parent is PENDING, CANCELLED or dead-lettered stays until that parent changes or is gone.
    /// Deleting a workflow lets its ID run again.
    pub async fn delete_finished_before(
        &self,
        cutoff_epoch_ms: i64,
        batch: u32,
    ) -> Result<u64, Error> {
        if batch == 0 {
            return Err(Error::InvalidConfig(
                "delete batch must be at least 1".to_owned(),
            ));
        }
        let limit = (batch as usize).min(RECHECK_CHUNK);
        let mut total = 0;
        loop {
            let ids = self
                .attempt(|| self.finished_before(cutoff_epoch_ms, limit))
                .await?;
            if !ids.is_empty() {
                total += self
                    .attempt(|| self.delete_candidates(&ids, cutoff_epoch_ms))
                    .await?;
            }
            // By candidates read, not rows deleted: a resume that wins a race must not end the loop.
            if ids.len() < limit {
                return Ok(total);
            }
        }
    }

    // A plain read: MySQL's locking scan could keep unrelated live rows locked.
    // Kept rows are filtered here, so they cannot fill a batch and hide the rows behind them.
    async fn finished_before(&self, cutoff: i64, limit: usize) -> Result<Vec<String>, Error> {
        let sql = format!(
            "SELECT workflow_uuid FROM workflow_status WHERE completed_at < ? AND status <> 'PENDING'{} \
             ORDER BY completed_at LIMIT ?",
            parent_not_in(RESUMABLE_PARENT)
        );
        Ok(sqlx::query_scalar(self.sql(&sql))
            .bind(cutoff)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?)
    }

    /// Checks each row and its parent again: a resume can change either after the read. For tests, public.
    #[doc(hidden)]
    pub async fn delete_candidates(&self, ids: &[String], cutoff: i64) -> Result<u64, Error> {
        let sql = delete_stopped_sql("completed_at < ? AND ", RESUMABLE_PARENT, ids.len());
        let mut delete = sqlx::query(self.sql(&sql)).bind(cutoff);
        for id in ids.iter().chain(ids) {
            delete = delete.bind(id.as_str());
        }
        Ok(delete.execute(&self.pool).await?.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{NewWorkflow, PORTABLE_JSON};
    use std::time::Duration;

    #[tokio::test]
    async fn a_candidate_resumed_after_the_read_stays() {
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
        // The row is PENDING, with a stale completed_at, as if the read saw it just before a resume.
        sqlx::query("UPDATE workflow_status SET completed_at = 1")
            .execute(&store.pool)
            .await
            .unwrap();
        let ids = vec!["a".to_owned()];
        assert_eq!(store.delete_candidates(&ids, i64::MAX).await.unwrap(), 0);
        assert!(store.delete_finished(&ids).await.unwrap().is_empty());
        assert!(store.get_workflow("a").await.unwrap().is_some());
    }
}
