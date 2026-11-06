use super::Engine;
use crate::Error;
use crate::workflow::{WorkflowOptions, is_known_format};

impl Engine {
    /// The format tag new rows get, unless a workflow chooses its own.
    pub fn serialization(&self) -> &str {
        &self.format
    }

    /// The format a new row gets: the workflow's own choice, else the engine's.
    pub(crate) fn format_for(&self, options: &WorkflowOptions) -> Result<String, Error> {
        match &options.serialization {
            None => Ok(self.format.clone()),
            Some(tag) if is_known_format(tag) => Ok(tag.clone()),
            Some(tag) => Err(Error::InvalidConfig(format!(
                "serialization {tag:?} is unknown"
            ))),
        }
    }
}
