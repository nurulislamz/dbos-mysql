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
    #[error("queue {0} does not exist")]
    QueueNotFound(String),
    #[error(
        "Queue {queue} already has an active workflow with deduplication ID {deduplication_id}"
    )]
    QueueDeduplicated {
        queue: String,
        deduplication_id: String,
    },
    #[error("Workflow {0} is cancelled. Aborting function.")]
    WorkflowCancelled(String),
    #[error("Non-existent awakeable ID: {0}")]
    AwakeableNotFound(String),
    #[error("Non-existent `send` destination workflow ID: {0}")]
    SendDestinationNotFound(String),
    #[error(
        "workflow {workflow_id} already exists with function {recorded_name}, but {requested_name} was requested"
    )]
    ConflictingWorkflow {
        workflow_id: String,
        recorded_name: String,
        requested_name: String,
    },
    #[error("Workflow ID {workflow_id} is already in use by workflow {name} with status {status}.")]
    WorkflowIdInUse {
        workflow_id: String,
        status: String,
        name: String,
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
    #[error("step reservation {0} can only be run once, by the workflow run that reserved it")]
    StepReservationUsed(u32),
    #[error("workflow {0} started a step while a patch was being checked; await the patch first")]
    PatchRace(String),
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
    #[error(
        "database schema version {found} is newer than {supported}, the newest this program knows. Upgrade this program"
    )]
    SchemaTooNew { found: i64, supported: i64 },
    #[error(
        "database schema version {found} is older than the required {required}. Start a writer with migrations on to upgrade it"
    )]
    SchemaOutdated { found: i64, required: i64 },
    #[error("schema upgrade stopped: {0}")]
    Migration(String),
    #[error("stored data is invalid: {0}")]
    Corrupt(String),
}
