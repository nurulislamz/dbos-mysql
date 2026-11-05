//! Times queue claims and enqueues with a large backlog. It only prints numbers.
//!
//! BACKLOG_ROWS=100000 cargo test --release -p dwos-core --test queue_backlog -- --ignored --nocapture --test-threads=1

mod common;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::*;
use dwos_core::{Begin, Engine, EngineConfig};

const QUEUE: &str = "backlog";
const SLOTS: u32 = 10;
const ROUNDS: usize = 5;
const CHUNK: usize = 1000;
const CLAIM_PAUSE: Duration = Duration::from_millis(100);

async fn seed(engine: &Engine, prefix: &str, rows: usize, finished: bool) {
    let (status, claim_after, completed) = if finished {
        ("SUCCESS", "NULL", "1")
    } else {
        ("PENDING", "1", "NULL")
    };
    let pool = engine.store().pool();
    for start in (0..rows).step_by(CHUNK) {
        let ids: Vec<String> = (start..rows.min(start + CHUNK))
            .map(|row| format!("{prefix}-{row}"))
            .collect();
        let values = |line: &dyn Fn(&str) -> String| {
            ids.iter().map(|id| line(id)).collect::<Vec<_>>().join(", ")
        };
        let workflows = values(&|id| {
            format!(
                "('{id}', 'wf', '{status}', '{{}}', {claim_after}, 0, 100, 'portable_json', 1, 1, {completed})"
            )
        });
        let entries = values(&|id| format!("('{id}', '{QUEUE}')"));
        let mut tx = pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO workflow_status (workflow_uuid, name, status, inputs, claim_after, recovery_attempts, \
             max_recovery_attempts, serialization, created_at, updated_at, completed_at) VALUES {workflows}"
        )))
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO workflow_queue_entries (workflow_uuid, queue_name) VALUES {entries}"
        )))
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
}

async fn claim_and_release(engine: &Engine, names: &[String]) -> Duration {
    let started = Instant::now();
    let claims = engine.claim_queue(QUEUE, names).await.unwrap();
    let took = started.elapsed();
    assert_eq!(claims.len(), SLOTS as usize);
    for claim in &claims {
        engine.store().release_lease(&claim.lease).await.unwrap();
    }
    took
}

async fn enqueue(engine: &Engine, id: &str) -> Option<Duration> {
    let started = Instant::now();
    engine
        .enqueue(
            QUEUE,
            Begin {
                name: "wf",
                input: "{}",
                max_recovery_attempts: 100,
                workflow_id: Some(id),
                parent: None,
                timeout_ms: None,
                ignore_propagated_deadline: false,
            },
        )
        .await
        .inspect_err(|error| println!("enqueue failed: {error}"))
        .ok()?;
    Some(started.elapsed())
}

fn summary(mut times: Vec<Duration>) -> String {
    times.sort();
    format!(
        "median {:?}, worst {:?}",
        times[times.len() / 2],
        times[times.len() - 1]
    )
}

async fn report(engine: Arc<Engine>, names: &[String], label: &str, run: &str) {
    let mut claims = Vec::new();
    for _ in 0..ROUNDS {
        claims.push(claim_and_release(&engine, names).await);
    }
    println!("{label}: claim of {SLOTS} slots: {}", summary(claims));
    let stop = Arc::new(AtomicBool::new(false));
    let claimer = tokio::spawn({
        let (engine, stop, names) = (engine.clone(), stop.clone(), names.to_vec());
        async move {
            while !stop.load(Ordering::Relaxed) {
                claim_and_release(&engine, &names).await;
                // Real workers pause between polls. With no pause, SQLite can starve a writer past its busy timeout.
                tokio::time::sleep(CLAIM_PAUSE).await;
            }
        }
    });
    let mut enqueues = Vec::new();
    for round in 0..20 {
        enqueues.extend(enqueue(&engine, &format!("{run}-{round}")).await);
    }
    stop.store(true, Ordering::Relaxed);
    claimer.await.unwrap();
    println!("{label}: enqueue during claims: {}", summary(enqueues));
}

async fn measure(backend: Backend, name: &str) {
    let rows = std::env::var("BACKLOG_ROWS")
        .map(|value| value.parse().unwrap())
        .unwrap_or(100_000);
    let db = TestDb::migrated(backend).await;
    let engine = Arc::new(
        Engine::connect(
            &db.url,
            EngineConfig {
                lease: Duration::from_secs(600),
                pickle: false,
                ..EngineConfig::default()
            },
        )
        .await
        .unwrap(),
    );
    engine
        .ensure_queue(QUEUE, NonZeroU32::new(SLOTS).unwrap())
        .await
        .unwrap();
    let names = vec!["wf".to_owned()];
    seed(&engine, "small", 100, false).await;
    report(engine.clone(), &names, &format!("{name} 100 waiting"), "a").await;
    seed(&engine, "done", rows, true).await;
    report(
        engine.clone(),
        &names,
        &format!("{name} 100 waiting, {rows} finished"),
        "b",
    )
    .await;
    seed(&engine, "wait", rows, false).await;
    report(
        engine.clone(),
        &names,
        &format!("{name} {rows} waiting, {rows} finished"),
        "c",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "seeds a large backlog and only prints numbers"]
async fn sqlite() {
    measure(Backend::Sqlite, "sqlite").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "seeds a large backlog and only prints numbers"]
async fn mysql() {
    measure(Backend::MySql, "mysql").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "seeds a large backlog and only prints numbers"]
async fn postgres() {
    measure(Backend::Postgres, "postgres").await;
}
