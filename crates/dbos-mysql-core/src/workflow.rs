use std::time::Duration;

use crate::Error;

pub const DEFAULT_MAX_RECOVERY_ATTEMPTS: u32 = 100;

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
pub enum Outcome {
    Success(String),
    Error(String),
}
