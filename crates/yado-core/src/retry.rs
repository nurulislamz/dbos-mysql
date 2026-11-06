use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sqlx::mysql::MySqlDatabaseError;

use crate::Error;

const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONTENTION_RETRIES: u32 = 5;
const CONNECTION_RETRY_WINDOW: Duration = Duration::from_secs(30);

const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
const ER_LOCK_NOWAIT: u16 = 3572;
const ER_LOCK_DEADLOCK: u16 = 1213;
// Postgres: serialization failure, deadlock, lock timeout.
const PG_CONTENTION: [&str; 3] = ["40001", "40P01", "55P03"];
// Postgres: server shutting down, crashing or starting. It sends these as ordinary errors.
const PG_CONNECTION: [&str; 3] = ["57P01", "57P02", "57P03"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Contention,
    Connection,
    Permanent,
}

fn classify(error: &Error) -> Kind {
    let Error::Database(error) = error else {
        return Kind::Permanent;
    };
    match error {
        // Any passes the driver's own error through, so these checks still work.
        sqlx::Error::Database(database) => {
            if let Some(mysql) = database.try_downcast_ref::<MySqlDatabaseError>() {
                return match mysql.number() {
                    ER_LOCK_DEADLOCK | ER_LOCK_WAIT_TIMEOUT => Kind::Contention,
                    _ => Kind::Permanent,
                };
            }
            let code = database.code();
            let code = code.as_deref().unwrap_or_default();
            if PG_CONTENTION.contains(&code) {
                Kind::Contention
            } else if PG_CONNECTION.contains(&code) {
                Kind::Connection
            } else {
                Kind::Permanent
            }
        }
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolTimedOut => Kind::Connection,
        _ => Kind::Permanent,
    }
}

/// MySQL 1205 (lock wait timeout) and 3572 (a NOWAIT lock was busy). A deadlock (1213) is still retried by `with_retry`.
pub(crate) fn is_lock_busy(error: &Error) -> bool {
    let Error::Database(sqlx::Error::Database(database)) = error else {
        return false;
    };
    database
        .try_downcast_ref::<MySqlDatabaseError>()
        .is_some_and(|mysql| matches!(mysql.number(), ER_LOCK_WAIT_TIMEOUT | ER_LOCK_NOWAIT))
}

fn backoff(attempt: u32) -> Duration {
    let jitter = u64::from(uuid::Uuid::new_v4().as_bytes()[0]);
    Duration::from_millis(20u64 * u64::from(attempt.min(6)).pow(2) + jitter)
}

pub async fn with_retry<T, F, Fut>(retries: &AtomicU64, mut operation: F) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let started = Instant::now();
    let mut attempt = 0;
    loop {
        attempt += 1;
        let error = match tokio::time::timeout(ATTEMPT_TIMEOUT, operation()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => error,
            Err(_) => return Err(Error::Timeout(ATTEMPT_TIMEOUT)),
        };
        let retry = match classify(&error) {
            Kind::Contention => attempt <= MAX_CONTENTION_RETRIES,
            Kind::Connection => started.elapsed() < CONNECTION_RETRY_WINDOW,
            Kind::Permanent => false,
        };
        if !retry {
            return Err(error);
        }
        retries.fetch_add(1, Ordering::Relaxed);
        log::warn!("retrying after a transient database error: {error}");
        tokio::time::sleep(backoff(attempt)).await;
    }
}
