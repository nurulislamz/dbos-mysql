use super::Engine;
use crate::Error;
use crate::workflow::Claimed;

impl Engine {
    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<(), Error> {
        self.store.cancel_workflow(workflow_id).await
    }

    /// Makes a cancelled or dead-lettered workflow claimable again. SUCCESS and ERROR are left alone.
    pub async fn resume_workflow(
        &self,
        workflow_id: &str,
        names: &[String],
    ) -> Result<Option<Claimed>, Error> {
        self.store.resume_workflow(workflow_id).await?;
        self.store
            .claim_one(workflow_id, names, self.config.lease)
            .await
    }
}
