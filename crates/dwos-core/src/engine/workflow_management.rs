use super::Engine;
use super::wakeup::WakeKey;
use crate::Error;
use crate::workflow::Claimed;

impl Engine {
    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<(), Error> {
        self.cancel_workflows(&[workflow_id.to_owned()], false)
            .await
    }

    pub async fn cancel_workflows(
        &self,
        workflow_ids: &[String],
        cancel_children: bool,
    ) -> Result<(), Error> {
        let cancelled = self
            .store
            .cancel_workflows(workflow_ids, cancel_children)
            .await;
        // Even on an error, because the update may have committed. A needless wake does no harm.
        let ids = match &cancelled {
            Ok(ids) => ids.clone(),
            Err(_) => workflow_ids.to_vec(),
        };
        for id in &ids {
            self.local_runs.poke(id);
            self.wakeups.wake(&WakeKey::Workflow(id.clone()));
        }
        cancelled.map(|_| ())
    }

    /// Makes cancelled or dead-lettered workflows claimable again. SUCCESS and ERROR are left alone.
    /// `queue` parks every resumable row on that queue instead of claiming it here. A live PENDING
    /// owner is not resumable: it keeps its lease and its queue.
    pub async fn resume_workflow(
        &self,
        workflow_id: &str,
        names: &[String],
    ) -> Result<Option<Claimed>, Error> {
        Ok(self
            .resume_workflows(&[workflow_id.to_owned()], names, None)
            .await?
            .into_iter()
            .next())
    }

    pub async fn resume_workflows(
        &self,
        workflow_ids: &[String],
        names: &[String],
        queue: Option<&str>,
    ) -> Result<Vec<Claimed>, Error> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(queue) = queue {
            // A renewal before the placement keeps a local owner. One after it reports the loss.
            for id in workflow_ids {
                self.local_runs.poke(id);
            }
            self.store.place_on_queue(workflow_ids, queue).await?;
            self.wake_queue(queue);
            return Ok(Vec::new());
        }
        self.store.require_workflows(workflow_ids).await?;
        let mut claims = Vec::new();
        for id in workflow_ids {
            self.store.resume_workflow(id).await?;
            if let Some(claimed) = self.store.claim_one(id, names, self.config.lease).await? {
                claims.push(claimed);
            }
        }
        Ok(claims)
    }
}
