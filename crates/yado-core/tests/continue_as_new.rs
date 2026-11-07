use yado_core::{Begin, Begun, Engine, EngineConfig, PORTABLE_JSON, Run};

#[macro_use]
mod common;
use common::*;

all_dbs!(recovery_restores_step_offset, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    let started = engine
        .begin(Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some("continued"),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap();
    assert!(matches!(started, Begun::Run(_)));
    sqlx::query("UPDATE workflow_status SET step_base = 42, claim_after = 0 WHERE workflow_uuid = 'continued'")
        .execute(engine.store().pool()).await.unwrap();
    let claim = engine
        .recover_now(&names())
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.lease.workflow_id == "continued")
        .unwrap();
    let run = Run::new_at(
        claim.lease,
        claim.serialization,
        claim.deadline_epoch_ms,
        claim.step_base,
    );
    assert_eq!(run.next_step_id(), 43);
});

all_dbs!(
    continuation_rotates_lease_and_clears_checkpoints,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = Engine::connect(&db.url, EngineConfig::default())
            .await
            .unwrap();
        let Begun::Run(run) = engine
            .begin(Begin {
                name: "wf",
                input: "{}",
                max_recovery_attempts: 100,
                workflow_id: Some("continued"),
                parent: None,
                timeout_ms: None,
                ignore_propagated_deadline: false,
            })
            .await
            .unwrap()
        else {
            panic!("expected a run")
        };
        let first = run.next_step_id();
        engine
            .save_step_output(&run, first, "old", "1".into())
            .await
            .unwrap();
        engine
            .save_step_output(&run, first + 5, "parallel", "2".into())
            .await
            .unwrap();
        engine
            .send(
                "continued",
                None,
                "\"message\"",
                PORTABLE_JSON,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let next = engine
            .continue_as_new(&run, "{\"new\":true}")
            .await
            .unwrap();
        assert!(engine.list_steps("continued").await.unwrap().is_empty());
        assert_eq!(
            engine
                .store()
                .list_messages("continued")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(next.next_step_id(), first + 6);
        assert!(
            engine
                .save_step_output(&run, first + 1, "stale", "0".into())
                .await
                .is_err()
        );
        assert_eq!(
            engine.get_status("continued").await.unwrap().input,
            "{\"new\":true}"
        );
        sqlx::query("UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = 'continued'")
            .execute(engine.store().pool())
            .await
            .unwrap();
        let claim = engine
            .recover_now(&names())
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.lease.workflow_id == "continued")
            .unwrap();
        assert_eq!(claim.input, "{\"new\":true}");
        assert_eq!(claim.step_base, first + 5);
    }
);

all_dbs!(fork_keeps_continued_step_offset, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    let Begun::Run(run) = engine
        .begin(Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some("source"),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    else {
        panic!("expected a run")
    };
    run.next_step_id();
    engine.continue_as_new(&run, "{}").await.unwrap();
    let (_, claim) = engine
        .fork_workflow("source", 2, Some("copy"), &names(), &Default::default())
        .await
        .unwrap();
    let claim = claim.unwrap();
    assert_eq!(claim.step_base, 1);
    let replay = Run::new_at(
        claim.lease,
        claim.serialization,
        claim.deadline_epoch_ms,
        claim.step_base,
    );
    assert_eq!(replay.next_step_id(), 2);
});

all_dbs!(queued_claim_restores_step_offset, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, EngineConfig::default())
        .await
        .unwrap();
    engine
        .ensure_queue("q", std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    engine
        .enqueue(
            "q",
            Begin {
                name: "wf",
                input: "{}",
                max_recovery_attempts: 100,
                workflow_id: Some("queued"),
                parent: None,
                timeout_ms: None,
                ignore_propagated_deadline: false,
            },
        )
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_status SET step_base = 42 WHERE workflow_uuid = 'queued'")
        .execute(engine.store().pool())
        .await
        .unwrap();
    let claim = engine
        .claim_queue("q", &names())
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claim.step_base, 42);
});
