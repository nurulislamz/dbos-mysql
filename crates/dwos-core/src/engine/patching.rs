use std::sync::atomic::Ordering;

use super::{Engine, Run};
use crate::Error;
use crate::workflow::StepRecord;

/// DBOS's marker prefix. A step name can carry it too, but then it only matches the same marker.
const PATCH_PREFIX: &str = "DBOS.patch-";

impl Engine {
    /// True for a run that has not passed this point, or passed it with this patch. The marker
    /// then takes the step ID. False when another checkpoint is there, and the next step keeps the ID.
    pub async fn patch(&self, run: &Run, name: &str) -> Result<bool, Error> {
        self.take_patch(run, name, true).await
    }

    /// Steps over this patch's marker in a run that recorded one. Other runs keep the step ID.
    pub async fn deprecate_patch(&self, run: &Run, name: &str) -> Result<(), Error> {
        self.take_patch(run, name, false).await.map(drop)
    }

    async fn take_patch(&self, run: &Run, name: &str, place: bool) -> Result<bool, Error> {
        let marker = format!("{PATCH_PREFIX}{name}");
        let step_id = run.next_step_id();
        let found = match self
            .store
            .load_step_owned(&run.lease, step_id, &marker)
            .await
        {
            Ok(Some(_)) => true,
            Ok(None) if place => {
                self.store
                    .save_step(&run.lease, step_id, &marker, &StepRecord::default())
                    .await?;
                true
            }
            Ok(None) | Err(Error::UnexpectedStep { .. }) => false,
            Err(error) => return Err(error),
        };
        if !found && !run.give_back_step_id(step_id) {
            return Err(Error::PatchRace(run.workflow_id().to_owned()));
        }
        Ok(found)
    }
}

impl Run {
    /// Fails when a later ID was drawn meanwhile: giving this one back would shift replay.
    fn give_back_step_id(&self, step_id: u32) -> bool {
        self.last_step_id
            .compare_exchange(step_id, step_id - 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::Lease;

    #[test]
    fn a_step_id_goes_back_only_while_it_is_the_last_one_drawn() {
        let lease = Lease {
            workflow_id: "a".into(),
            token: "t".into(),
        };
        let run = Run::new(lease, String::new(), None);
        assert!(run.give_back_step_id(run.next_step_id()));
        assert_eq!(run.last_step_id(), 0);
        let first = run.next_step_id();
        run.next_step_id();
        assert!(!run.give_back_step_id(first));
        assert_eq!(run.last_step_id(), 2);
    }
}
