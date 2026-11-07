use sqlx::AssertSqlSafe;
use yado_core::{Begin, Begun, Engine, EngineConfig, Error, Run};

#[macro_use]
mod common;
use common::*;

async fn engine(db: &TestDb, version: Option<&str>) -> Engine {
    let config = EngineConfig {
        lease: LONG,
        pickle: false,
        application_version: version.map(str::to_owned),
        ..EngineConfig::default()
    };
    Engine::connect(&db.url, config).await.unwrap()
}

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

async fn start(engine: &Engine, id: &str) -> Run {
    match engine.begin(begin(id)).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected a new run, got {other:?}"),
    }
}

/// Makes the row claimable, as if its owner had died.
async fn expire(engine: &Engine, id: &str) {
    sqlx::query(AssertSqlSafe(format!(
        "UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = '{id}'"
    )))
    .execute(engine.store().pool())
    .await
    .unwrap();
}

async fn recover(engine: &Engine) -> Vec<String> {
    let mut ids: Vec<String> = engine
        .recover_now(&names())
        .await
        .unwrap()
        .into_iter()
        .map(|claim| claim.lease.workflow_id)
        .collect();
    ids.sort();
    ids
}

fn names_of(rows: &[yado_core::VersionRow]) -> Vec<&str> {
    rows.iter().map(|row| row.version_name.as_str()).collect()
}

all_dbs!(rows_go_only_to_their_own_version, backend, {
    let db = TestDb::migrated(backend).await;
    let v1 = engine(&db, Some("v1")).await;
    start(&v1, "a").await;
    expire(&v1, "a").await;

    let v2 = engine(&db, Some("v2")).await;
    assert!(recover(&v2).await.is_empty());
    assert_eq!(recover(&v1).await, ["a"]);
    let row = v1.get_status("a").await.unwrap();
    assert_eq!(row.application_version.as_deref(), Some("v1"));
});

all_dbs!(versionless_rows_go_only_to_the_latest_version, backend, {
    let db = TestDb::migrated(backend).await;
    let unversioned = engine(&db, None).await;
    start(&unversioned, "old").await;
    expire(&unversioned, "old").await;

    let v1 = engine(&db, Some("v1")).await;
    let v2 = engine(&db, Some("v2")).await;
    assert!(recover(&v1).await.is_empty(), "v1 is not the latest");
    v2.set_latest_version("v1").await.unwrap();
    assert!(recover(&v2).await.is_empty(), "v2 is no longer the latest");
    assert_eq!(recover(&v1).await, ["old"]);
});

all_dbs!(
    versionless_rows_go_to_anyone_until_a_version_is_registered,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let unversioned = engine(&db, None).await;
        start(&unversioned, "old").await;
        expire(&unversioned, "old").await;
        assert_eq!(recover(&unversioned).await, ["old"]);
    }
);

all_dbs!(the_newest_registration_is_the_latest, backend, {
    let db = TestDb::migrated(backend).await;
    let v1 = engine(&db, Some("v1")).await;
    let v2 = engine(&db, Some("v2")).await;
    assert_eq!(names_of(&v1.list_versions().await.unwrap()), ["v2", "v1"]);

    // Starting a known version again keeps its place.
    engine(&db, Some("v1")).await;
    assert_eq!(names_of(&v1.list_versions().await.unwrap()), ["v2", "v1"]);

    v2.set_latest_version("v1").await.unwrap();
    let rows = v1.list_versions().await.unwrap();
    assert_eq!(names_of(&rows), ["v1", "v2"]);
    assert!(rows[0].version_timestamp > rows[1].version_timestamp);
    assert!(matches!(
        v1.set_latest_version("missing").await,
        Err(Error::InvalidConfig(_))
    ));
});

all_dbs!(concurrent_versions_are_ordered, backend, {
    let db = TestDb::migrated(backend).await;
    let (v1, v2) = tokio::join!(engine(&db, Some("first")), engine(&db, Some("second")));
    let rows = v1.list_versions().await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].version_timestamp > rows[1].version_timestamp);
    v2.set_latest_version(&rows[1].version_name).await.unwrap();
    assert_eq!(
        v1.list_versions().await.unwrap()[0].version_name,
        rows[1].version_name
    );
});
