use std::time::Duration;
use yado_core::{Begin, Begun, Engine, EngineConfig, SendOutcome, WorkflowFilter, WorkflowOptions};
#[macro_use]
mod common;
use common::*;

all_dbs!(attributes_update_replay, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(
        &db.url,
        EngineConfig {
            lease: Duration::from_secs(60),
            ..EngineConfig::default()
        },
    )
    .await
    .unwrap();
    let begin = |id| Begin {
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        workflow_id: Some(id),
        parent: None,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    };
    let opts = WorkflowOptions {
        attributes: Some(r#"{"old":1,"nested":{"a":1}}"#.into()),
        ..Default::default()
    };
    let Begun::Run(sender) = engine.begin_with(begin("sender"), &opts).await.unwrap() else {
        panic!("not started")
    };
    assert_eq!(
        engine
            .get_status("sender")
            .await
            .unwrap()
            .attributes
            .as_deref(),
        opts.attributes.as_deref()
    );
    assert_eq!(
        engine
            .update_workflow_attributes(
                "sender",
                Some(r#"{"nested":{"b":2},"null":null}"#),
                true,
                Some(&sender),
                Some(1)
            )
            .await
            .unwrap(),
        SendOutcome::Sent
    );
    let row = engine
        .list_workflows(&WorkflowFilter::default())
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.attributes.as_deref().unwrap()).unwrap(),
        serde_json::json!({"old":1,"nested":{"b":2},"null":null})
    );
    engine
        .update_workflow_attributes("sender", Some(r#"{"changed":3}"#), false, None, None)
        .await
        .unwrap();
    assert_eq!(
        engine
            .update_workflow_attributes(
                "sender",
                Some(r#"{"nested":0}"#),
                true,
                Some(&sender),
                Some(1)
            )
            .await
            .unwrap(),
        SendOutcome::Replayed
    );
    assert_eq!(
        engine
            .get_status("sender")
            .await
            .unwrap()
            .attributes
            .as_deref(),
        Some(r#"{"changed":3}"#)
    );
    let (first, second) = tokio::join!(
        engine.update_workflow_attributes("sender", Some(r#"{"first":1}"#), true, None, None),
        engine.update_workflow_attributes("sender", Some(r#"{"second":2}"#), true, None, None),
    );
    first.unwrap();
    second.unwrap();
    let merged = engine
        .get_status("sender")
        .await
        .unwrap()
        .attributes
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&merged).unwrap(),
        serde_json::json!({"changed":3,"first":1,"second":2})
    );
    engine
        .update_workflow_attributes("sender", None, false, None, None)
        .await
        .unwrap();
    assert_eq!(engine.get_status("sender").await.unwrap().attributes, None);
    assert!(
        engine
            .update_workflow_attributes("sender", None, true, None, None)
            .await
            .is_err()
    );
});
