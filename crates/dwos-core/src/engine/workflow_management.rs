use super::Engine;
use super::wakeup::WakeKey;
use crate::Error;
use crate::workflow::Claimed;

impl Engine {
    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<(), Error> {
        let cancelled = self.store.cancel_workflow(workflow_id).await;
        // Even on an error, because the commit may have happened. A needless renewal does no harm.
        self.local_runs.poke(workflow_id);
        cancelled?;
        self.wakeups
            .wake(&WakeKey::Workflow(workflow_id.to_owned()));
        Ok(())
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
