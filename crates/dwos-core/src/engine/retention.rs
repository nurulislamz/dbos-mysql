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
