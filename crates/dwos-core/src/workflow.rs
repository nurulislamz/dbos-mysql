use std::time::Duration;

use crate::Error;

pub const DEFAULT_MAX_RECOVERY_ATTEMPTS: u32 = 100;
const UNLIMITED_RECOVERY_ATTEMPTS: u32 = u32::MAX;
pub const PORTABLE_JSON: &str = "portable_json";
pub const PY_PICKLE: &str = "py_pickle";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Pending,
    /// Waiting on a queue and never admitted. Derived from the row, which stays `PENDING`.
    Enqueued,
    /// Like `Enqueued`, but its start time has not come.
    Delayed,
    Success,
    Error,
    MaxRecoveryAttemptsExceeded,
    Cancelled,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Enqueued => "ENQUEUED",
            Self::Delayed => "DELAYED",
            Self::Success => "SUCCESS",
            Self::Error => "ERROR",
            Self::MaxRecoveryAttemptsExceeded => "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "ENQUEUED" => Ok(Self::Enqueued),
            "DELAYED" => Ok(Self::Delayed),
            "SUCCESS" => Ok(Self::Success),
            "ERROR" => Ok(Self::Error),
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => Ok(Self::MaxRecoveryAttemptsExceeded),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(Error::Corrupt(format!("unknown workflow status {other}"))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NewWorkflow<'a> {
    pub workflow_id: &'a str,
    pub name: &'a str,
    pub input: &'a str,
    pub max_recovery_attempts: u32,
    pub lease: Duration,
    pub parent_workflow_id: Option<&'a str>,
    pub serialization: &'a str,
    pub timeout_ms: Option<i64>,
    pub deadline_epoch_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    pub workflow_id: String,
    pub token: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Workflow {
    pub workflow_id: String,
    pub name: String,
    pub status: Status,
    pub input: String,
    pub output: Option<String>,
    pub error: Option<String>,
    pub recovery_attempts: u32,
    pub max_recovery_attempts: u32,
    pub parent_workflow_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub serialization: String,
    pub timeout_ms: Option<i64>,
    pub deadline_epoch_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub application_version: Option<String>,
    pub application_name: Option<String>,
    pub executor_id: Option<String>,
    pub forked_from: Option<String>,
    pub was_forked_from: bool,
    pub schedule_name: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<String>,
    pub attributes: Option<String>,
    pub queue_name: Option<String>,
    pub priority: Option<i64>,
    pub queue_partition_key: Option<String>,
    pub deduplication_id: Option<String>,
    pub dequeued_at_ms: Option<i64>,
    pub delay_until_ms: Option<i64>,
}

/// Row fields a start can set. A version or application left `None` comes from the store's identity.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkflowOptions {
    pub application_version: Option<String>,
    pub application_name: Option<String>,
    pub schedule_name: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<String>,
    pub attributes: Option<String>,
    /// What to do when the workflow id already has a row.
    pub reuse: ReusePolicy,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReusePolicy {
    /// An existing workflow id returns the stored row.
    #[default]
    ReturnExisting,
    /// An existing workflow id is an error. A new id still starts.
    Reject,
}

impl ReusePolicy {
    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "return-existing" => Ok(Self::ReturnExisting),
            "reject" => Ok(Self::Reject),
            other => Err(Error::InvalidConfig(format!(
                "Invalid workflow_id_reuse_policy {other}. Must be either 'return-existing' or 'reject'."
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Started {
    Owned(Lease, Option<i64>),
    // `Workflow` is much larger than `Lease`. Boxing keeps the two arms the same size.
    Existing(Box<Workflow>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Claimed {
    pub lease: Lease,
    pub name: String,
    pub input: String,
    pub recovery_attempts: u32,
    pub serialization: String,
    pub deadline_epoch_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StepRecord {
    pub output: Option<String>,
    pub error: Option<String>,
    pub child_workflow_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedStep {
    pub step_id: u32,
    pub step_name: String,
    pub started_at_epoch_ms: Option<i64>,
    pub completed_at_epoch_ms: i64,
    pub record: StepRecord,
    pub serialization: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Success(String),
    Error(String),
}

/// Filters for `list_workflows`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkflowFilter {
    pub workflow_ids: Option<Vec<String>>,
    pub queue_names: Option<Vec<String>>,
    pub app_versions: Option<Vec<String>>,
    pub forked_from: Option<Vec<String>>,
    pub users: Option<Vec<String>>,
    pub queues_only: bool,
    pub statuses: Option<Vec<Status>>,
    pub names: Option<Vec<String>>,
    pub parent_workflow_ids: Option<Vec<String>>,
    pub workflow_id_prefixes: Option<Vec<String>>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
    pub completed_after_ms: Option<i64>,
    pub completed_before_ms: Option<i64>,
    pub has_parent: Option<bool>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub sort_desc: bool,
}

/// The limit to store: `None` means no limit.
pub fn recovery_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(UNLIMITED_RECOVERY_ATTEMPTS)
}

pub fn recovery_limit_of(stored: u32) -> Option<u32> {
    (stored != UNLIMITED_RECOVERY_ATTEMPTS).then_some(stored)
}

pub fn pickle_from_serializer(name: &str) -> Result<bool, Error> {
    match name {
        "json" => Ok(false),
        "pickle" => Ok(true),
        other => Err(Error::InvalidConfig(format!(
            "serializer must be json or pickle, not {other}"
        ))),
    }
}

pub(crate) fn new_token() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub(crate) fn attempts_exhausted(attempts: u32, max_attempts: u32) -> bool {
    attempts > max_attempts.saturating_add(1)
}

pub(crate) fn id_in_use(existing: &Workflow) -> Error {
    Error::WorkflowIdInUse {
        workflow_id: existing.workflow_id.clone(),
        status: existing.status.as_str().to_owned(),
        name: existing.name.clone(),
    }
}

pub(crate) fn check_workflow_name(new: &NewWorkflow<'_>, recorded_name: &str) -> Result<(), Error> {
    if recorded_name == new.name {
        return Ok(());
    }
    Err(Error::ConflictingWorkflow {
        workflow_id: new.workflow_id.to_owned(),
        recorded_name: recorded_name.to_owned(),
        requested_name: new.name.to_owned(),
    })
}

pub(crate) fn check_step_name(
    workflow_id: &str,
    step_id: u32,
    expected: &str,
    recorded_name: String,
) -> Result<(), Error> {
    if recorded_name == expected {
        return Ok(());
    }
    Err(Error::UnexpectedStep {
        workflow_id: workflow_id.to_owned(),
        step_id,
        expected_name: expected.to_owned(),
        recorded_name,
    })
}

pub(crate) fn check_recorded_step(
    workflow_id: &str,
    step_id: u32,
    step_name: &str,
    record: &StepRecord,
    recorded: &(String, StepRecord),
) -> Result<(), Error> {
    if recorded.0 == step_name && &recorded.1 == record {
        return Ok(());
    }
    Err(Error::StepNondeterminism {
        workflow_id: workflow_id.to_owned(),
        step_id,
    })
}
