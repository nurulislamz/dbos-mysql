use super::{Engine, Run};
use crate::Error;

impl Engine {
    /// The fence an app database transaction checks just before it commits a step's result:
    /// `Error::LeaseLost` means another execution owns the workflow, so the caller must roll back.
    pub async fn check_owner(&self, run: &Run) -> Result<(), Error> {
        self.store.check_owner(&run.lease).await
    }
}
