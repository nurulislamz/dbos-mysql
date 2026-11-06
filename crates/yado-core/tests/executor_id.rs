use std::time::Duration;

use yado_core::{Begin, Begun, Engine, EngineConfig};

#[macro_use]
mod common;
use common::*;

fn begin(id: &str) -> Begin<'_> {
    Begin {
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        workflow_id: Some(id),
        parent: None,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    }
}

all_dbs!(a_takeover_rewrites_the_executor_id, backend, {
    let db = TestDb::migrated(backend).await;
    let first = Engine::connect(
        &db.url,
        EngineConfig {
            executor_id: Some("worker-a".into()),
            lease: EXPIRED,
            ..EngineConfig::default()
        },
    )
    .await
    .unwrap();
    let Begun::Run(_stale) = first.begin(begin("a")).await.unwrap() else {
        panic!("expected a new run")
    };
    let started = first.get_status("a").await.unwrap();
    assert_eq!(started.executor_id.as_deref(), Some("worker-a"));

    let second = Engine::connect(
        &db.url,
        EngineConfig {
            executor_id: Some("worker-b".into()),
            recovery_interval: Duration::from_millis(20),
            ..EngineConfig::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(second.recover_now(&names()).await.unwrap().len(), 1);
    let taken_over = second.get_status("a").await.unwrap();
    assert_eq!(taken_over.executor_id.as_deref(), Some("worker-b"));
});
