use super::{Engine, Run};
use crate::{Error, SendFrom, SendOutcome};

impl Engine {
    pub async fn update_workflow_attributes(
        &self,
        workflow_id: &str,
        attributes: Option<&str>,
        merge: bool,
        run: Option<&Run>,
        step_id: Option<u32>,
    ) -> Result<SendOutcome, Error> {
        let from = match (run, step_id) {
            (Some(run), Some(step_id)) => SendFrom::Workflow {
                lease: &run.lease,
                step_id,
            },
            (Some(run), None) => SendFrom::Step(&run.lease),
            (None, None) => SendFrom::Outside,
            (None, Some(_)) => {
                return Err(Error::InvalidConfig(
                    "a step ID requires a workflow run".into(),
                ));
            }
        };
        let serialization = run.map_or(self.serialization(), |run| run.serialization.as_str());
        self.store
            .update_workflow_attributes(workflow_id, attributes, merge, from, serialization)
            .await
    }
}
