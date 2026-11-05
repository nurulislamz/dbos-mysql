use super::Engine;
use crate::Error;

impl Engine {
    /// See [`crate::Store::delete_workflows`]. Deleting a workflow lets its ID run again.
    pub async fn delete_workflows(
        &self,
        workflow_ids: &[String],
        delete_children: bool,
    ) -> Result<Vec<String>, Error> {
        self.store
            .delete_workflows(workflow_ids, delete_children)
            .await
    }

    /// Deletes finished workflows older than `max_age_hours` (by the database clock) or beyond the
    /// `keep_rows` newest. A limit of None is off, and with both off nothing is deleted. Pending rows
    /// and protected children stay. Returns the rows deleted. Deleting a workflow lets its ID run again.
    pub async fn apply_retention(
        &self,
        max_age_hours: Option<f64>,
        keep_rows: Option<u32>,
        batch: u32,
    ) -> Result<u64, Error> {
        let mut cutoff: Option<i64> = None;
        if let Some(hours) = max_age_hours {
            if !(hours.is_finite() && hours > 0.0) {
                return Err(Error::InvalidConfig(
                    "retention hours must be a positive number".to_owned(),
                ));
            }
            let age_ms = (hours * 3_600_000.0) as i64;
            cutoff = Some(self.store.db_now_ms().await?.saturating_sub(age_ms));
        }
        if let Some(rows) = keep_rows {
            if rows == 0 {
                return Err(Error::InvalidConfig(
                    "retention rows must be at least 1".to_owned(),
                ));
            }
            // Rows older than the `rows`-th newest are past the limit. The later cutoff decides.
            if let Some(completed) = self.store.completed_at_of_row(rows).await? {
                cutoff = Some(cutoff.map_or(completed, |c| c.max(completed)));
            }
        }
        match cutoff {
            Some(cutoff) => self.delete_finished_before(cutoff, batch).await,
            None => Ok(0),
        }
    }

    /// See [`crate::Store::delete_finished_before`]. Deleting a workflow lets its ID run again.
    pub async fn delete_finished_before(
        &self,
        cutoff_epoch_ms: i64,
        batch: u32,
    ) -> Result<u64, Error> {
        self.store
            .delete_finished_before(cutoff_epoch_ms, batch)
            .await
    }
}
