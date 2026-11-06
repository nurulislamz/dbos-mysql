//! The ownership fence a datasource transaction checks before it commits a step's result.

use dwos_core::{Begin, Begun, Engine, EngineConfig, Error, Run, Store};

#[macro_use]
mod common;
use common::*;

async fn begin_run(engine: &Engine, id: &str) -> Run {
    let begun = engine
        .begin(Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some(id),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap();
    match begun {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

async fn expire(store: &Store, id: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = '{id}'"
    )))
    .execute(store.pool())
    .await
    .unwrap();
}

#[track_caller]
fn assert_lost(result: Result<(), Error>) {
    assert!(matches!(result, Err(Error::LeaseLost(_))), "{result:?}");
}

all_dbs!(only_the_current_owner_passes_the_fence, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    let old = begin_run(&engine, "a").await;
    engine.check_owner(&old).await.unwrap();

    // An expired lease fails the fence and is revoked, so it cannot pass later either.
    expire(engine.store(), "a").await;
    assert_lost(engine.check_owner(&old).await);
    assert_lost(engine.check_owner(&old).await);

    // The execution that takes over passes; the one it replaced still does not.
    let claimed = engine.recover_now(&names()).await.unwrap();
    assert_eq!(claimed.len(), 1, "{claimed:?}");
    let claim = claimed.into_iter().next().unwrap();
    let new = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);
    engine.check_owner(&new).await.unwrap();
    assert_lost(engine.check_owner(&old).await);
});
