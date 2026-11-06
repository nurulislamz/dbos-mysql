use super::{Begin, Engine};
use crate::Error;

impl Engine {
    pub(crate) fn timeout_deadline(
        &self,
        begin: &Begin<'_>,
    ) -> Result<(Option<i64>, Option<i64>), Error> {
        if let Some(timeout_ms) = begin.timeout_ms {
            if timeout_ms <= 0 {
                return Err(Error::InvalidConfig(
                    "workflow timeout must be positive".to_owned(),
                ));
            }
            return Ok((Some(timeout_ms), None));
        }
        if begin.ignore_propagated_deadline {
            return Ok((None, None));
        }
        Ok((
            None,
            begin
                .parent
                .and_then(|parent| parent.run.deadline_epoch_ms()),
        ))
    }
}
