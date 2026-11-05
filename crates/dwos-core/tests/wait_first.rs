use std::time::Duration;

use dwos_core::{Begin, Begun, Engine, EngineConfig, Outcome};

#[macro_use]
mod common;
use common::*;

all_dbs!(waits_for_a_finished_workflow, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    let mut runs = Vec::new();
    for id in ["first", "second"] {
        let begun = engine
            .begin(Begin {
                name: "wf",
                input: "{}",
                max_recovery_attempts: 10,
                workflow_id: Some(id),
                parent: None,
                timeout_ms: None,
                ignore_propagated_deadline: false,
            })
            .await
            .unwrap();
        match begun {
            Begun::Run(run) => runs.push(run),
            other => panic!("{other:?}"),
        }
    }
    let ids = vec!["first".to_owned(), "second".to_owned()];
    let waiting = engine.await_first(&ids, Some(Duration::from_millis(50)));
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err()
    );
    engine
        .finish(&runs[1], Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap(),
        "second"
    );
    assert!(engine.await_first(&[], None).await.is_err());
    assert!(
        engine
            .await_first(&["first".into(), "first".into()], None)
            .await
            .is_err()
    );
});
