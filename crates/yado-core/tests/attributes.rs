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

all_dbs!(attribute_list_index, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
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
    let options = WorkflowOptions {
        attributes: Some(r#"{"customer":"acme","nested":{"a":1},"flag":true}"#.into()),
        ..Default::default()
    };
    engine.begin_with(begin("start"), &options).await.unwrap();
    engine
        .ensure_queue("q", std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    engine
        .enqueue_with("q", begin("queued"), &options)
        .await
        .unwrap();
    engine
        .fork_workflow("start", 1, Some("fork"), &[], &Default::default())
        .await
        .unwrap();
    let matching = |text: &str| WorkflowFilter {
        attributes: Some(text.to_owned()),
        ..Default::default()
    };
    let ids = |rows: Vec<yado_core::Workflow>| {
        let mut ids: Vec<_> = rows.into_iter().map(|r| r.workflow_id).collect();
        ids.sort();
        ids
    };
    assert_eq!(
        ids(engine
            .list_workflows(&matching(r#"{"customer":"acme","nested":{"a":1}}"#))
            .await
            .unwrap()),
        ["fork", "queued", "start"]
    );
    assert!(
        engine
            .list_workflows(&matching(r#"{"nested":{"a":2}}"#))
            .await
            .unwrap()
            .is_empty()
    );
    engine
        .update_workflow_attributes("start", Some(r#"{"customer":"other"}"#), false, None, None)
        .await
        .unwrap();
    engine
        .update_workflow_attributes("queued", Some(r#"{"extra":null}"#), true, None, None)
        .await
        .unwrap();
    engine
        .update_workflow_attributes("fork", None, false, None, None)
        .await
        .unwrap();
    assert_eq!(
        ids(engine
            .list_workflows(&matching(r#"{"customer":"acme","extra":null}"#))
            .await
            .unwrap()),
        ["queued"]
    );
    assert_eq!(
        ids(engine
            .list_workflows(&matching(r#"{"customer":"other"}"#))
            .await
            .unwrap()),
        ["start"]
    );
    assert!(engine.list_workflows(&matching("[]")).await.is_err());
    assert!(
        engine
            .list_workflows(&matching(&format!("{{\"{}\":1}}", "x".repeat(256))))
            .await
            .is_err()
    );

    let store = db.open().await;
    let plan = match backend {
        Backend::Sqlite => {
            "EXPLAIN QUERY PLAN SELECT workflow_uuid FROM workflow_attribute_lookup WHERE attr_key = 'customer' AND substr(attr_value, 1, 255) = substr('\"acme\"', 1, 255) AND attr_value = '\"acme\"'"
        }
        Backend::Postgres => {
            "EXPLAIN SELECT workflow_uuid FROM workflow_attribute_lookup WHERE attr_key = 'customer' AND substr(attr_value, 1, 255) = substr('\"acme\"', 1, 255) AND attr_value = '\"acme\"'"
        }
        Backend::MySql => {
            "EXPLAIN SELECT workflow_uuid FROM workflow_attribute_lookup WHERE attr_key = 'customer' AND attr_value = '\"acme\"'"
        }
    };
    use sqlx::Row;
    if backend == Backend::Postgres {
        sqlx::query("SET enable_seqscan = off")
            .execute(store.pool())
            .await
            .unwrap();
    }
    let rows = sqlx::query(plan).fetch_all(store.pool()).await.unwrap();
    let uses_index = rows.iter().any(|row| {
        (0..row.len()).any(|i| {
            row.try_get::<String, _>(i)
                .is_ok_and(|s| s.contains("ix_workflow_attributes_lookup"))
        })
    });
    assert!(uses_index, "lookup index missing from plan");
});
