#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported database URL scheme, expected postgres://, mysql:// or sqlite://")]
    UnsupportedUrl,
    #[error("invalid database URL")]
    InvalidUrl,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Non-existent target workflow ID: {0}")]
    WorkflowNotFound(String),
    #[error(
        "workflow {workflow_id} already exists with function {recorded_name}, but {requested_name} was requested"
    )]
    ConflictingWorkflow {
        workflow_id: String,
        recorded_name: String,
        requested_name: String,
    },
    #[error(
        "during workflow {workflow_id} step {step_id}, {recorded_name} was recorded when {expected_name} was expected. Check that your workflow is deterministic"
    )]
    UnexpectedStep {
        workflow_id: String,
        step_id: u32,
        expected_name: String,
        recorded_name: String,
    },
    #[error("workflow {workflow_id} step {step_id} was already recorded with a different result")]
    StepNondeterminism { workflow_id: String, step_id: u32 },
    #[error(
        "workflow {workflow_id} step {step_id} is recorded as something other than the expected {expected}. Check that your workflow is deterministic"
    )]
    StepKindMismatch {
        workflow_id: String,
        step_id: u32,
        expected: &'static str,
    },
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("this process no longer owns workflow {0}")]
    LeaseLost(String),
    #[error("DBOS was destroyed while this work was running")]
    ShuttingDown,
    #[error("{field} is longer than the 255 character limit")]
    NameTooLong { field: &'static str },
    #[error("{field} contains a NUL character")]
    ContainsNul { field: &'static str },
    #[error("value of {size} bytes is over the database limit of {limit} bytes")]
    ValueTooLarge { size: u64, limit: u64 },
    #[error("database operation timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("stored data is invalid: {0}")]
    Corrupt(String),
}
