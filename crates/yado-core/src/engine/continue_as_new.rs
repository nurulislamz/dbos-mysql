use super::{Engine, Run};
use crate::workflow::new_token;
use crate::{Error, Lease};

impl Engine {
    pub async fn continue_as_new(&self, run: &Run, input: &str) -> Result<Run, Error> {
        let step_base = run.last_step_id();
        if step_base == u32::MAX {
            return Err(Error::InvalidConfig(
                "workflow has exhausted its step IDs".into(),
            ));
        }
        let token = new_token();
        let step_base = self
            .store
            .continue_as_new(&run.lease, &token, input, step_base)
            .await?;
        Ok(Run::new_at(
            Lease {
                workflow_id: run.workflow_id().to_owned(),
                token,
            },
            run.serialization().to_owned(),
            run.deadline_epoch_ms(),
            step_base,
        ))
    }
}
