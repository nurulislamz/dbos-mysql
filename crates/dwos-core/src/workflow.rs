use std::time::Duration;

use crate::Error;

pub const DEFAULT_MAX_RECOVERY_ATTEMPTS: u32 = 100;
pub const SERIALIZATION: &str = "py_pickle";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Pending,
    Success,
    Error,
    MaxRecoveryAttemptsExceeded,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Success => "SUCCESS",
            Self::Error => "ERROR",
            Self::MaxRecoveryAttemptsExceeded => "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        }
    }

    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "SUCCESS" => Ok(Self::Success),
            "ERROR" => Ok(Self::Error),
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => Ok(Self::MaxRecoveryAttemptsExceeded),
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Started {
    Owned(Lease),
    Existing(Workflow),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Claimed {
    pub lease: Lease,
    pub name: String,
    pub input: String,
    pub recovery_attempts: u32,
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
    pub record: StepRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Success(String),
    Error(String),
}

pub(crate) fn new_token() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub(crate) fn attempts_exhausted(attempts: u32, max_attempts: u32) -> bool {
    attempts > max_attempts.saturating_add(1)
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
