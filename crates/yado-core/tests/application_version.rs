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

async fn engine(db: &TestDb, version: Option<&str>) -> Engine {
    Engine::connect(
        &db.url,
        EngineConfig {
            lease: if version == Some("2.0") {
                LONG
            } else {
                EXPIRED
            },
            application_version: version.map(str::to_owned),
            ..EngineConfig::default()
        },
    )
    .await
    .unwrap()
}

all_dbs!(recovery_takes_only_its_version_or_none, backend, {
    let db = TestDb::migrated(backend).await;
    for (id, version) in [("v1", Some("1.0")), ("none", None)] {
        let Begun::Run(_) = engine(&db, version).await.begin(begin(id)).await.unwrap() else {
            panic!("expected a new run")
        };
        let stamped = engine(&db, None).await.get_status(id).await.unwrap();
        assert_eq!(stamped.application_version.as_deref(), version);
    }

    let v2 = engine(&db, Some("2.0")).await;
    let claimed = v2.recover_now(&names()).await.unwrap();
    let ids: Vec<_> = claimed
        .iter()
        .map(|claim| claim.lease.workflow_id.as_str())
        .collect();
    assert_eq!(ids, ["none"]);

    let v1 = engine(&db, Some("1.0")).await;
    let claimed = v1.recover_now(&names()).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].lease.workflow_id, "v1");
});

all_dbs!(
    unversioned_process_does_not_recover_versioned_rows,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        started_versioned(&db).await;
        assert!(
            engine(&db, None)
                .await
                .recover_now(&names())
                .await
                .unwrap()
                .is_empty()
        );
    }
);

async fn started_versioned(db: &TestDb) {
    assert!(matches!(
        engine(db, Some("1.0"))
            .await
            .begin(begin("v1"))
            .await
            .unwrap(),
        Begun::Run(_)
    ));
}
