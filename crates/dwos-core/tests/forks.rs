use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::Duration;

use dwos_core::{
    Begin, Begun, Claimed, Engine, EngineConfig, Error, ForkOptions, Outcome, PORTABLE_JSON, Run,
    Status, StepDecision, StepRecord, WorkflowFilter,
};

#[macro_use]
mod common;
use common::*;

// Events are matched to steps by database time, so ordered writes need a clear gap.
const GAP: Duration = Duration::from_millis(20);

async fn engine(db: &TestDb) -> Engine {
    let config = EngineConfig {
        lease: LONG,
        recovery_interval: Duration::from_secs(3600),
        ..EngineConfig::default()
    };
    Engine::connect(&db.url, config).await.unwrap()
}

async fn begin_run(engine: &Engine, id: &str) -> Run {
    let begin = Begin {
        name: "wf",
        input: "{\"n\": 1}",
        max_recovery_attempts: 100,
        workflow_id: Some(id),
        parent: None,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    };
    match engine.begin(begin).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

fn run_of(claim: Claimed) -> Run {
    Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms)
}

async fn save(engine: &Engine, run: &Run, step: u32, value: &str) {
    engine
        .save_step_output(run, step, &format!("step{step}"), value.to_owned())
        .await
        .unwrap();
}

/// Steps 1 and 2 saved with event k=1, then after a gap event k=2 at step 3, step 4, success.
async fn finished_original(engine: &Engine, id: &str) {
    let run = begin_run(engine, id).await;
    save(engine, &run, 1, "\"one\"").await;
    engine
        .set_event(&run, None, "k", "1", PORTABLE_JSON)
        .await
        .unwrap();
    save(engine, &run, 2, "\"two\"").await;
    tokio::time::sleep(GAP).await;
    engine
        .set_event(&run, Some(3), "k", "2", PORTABLE_JSON)
        .await
        .unwrap();
    save(engine, &run, 4, "\"four\"").await;
    engine
        .finish(&run, Outcome::Success("\"done\"".to_owned()))
        .await
        .unwrap();
}

async fn event(engine: &Engine, id: &str) -> Vec<String> {
    engine
        .list_events(id)
        .await
        .unwrap()
        .into_iter()
        .map(|row| format!("{}={}", row.key, row.value.text))
        .collect()
}

all_dbs!(fork_replays_steps_before_the_start_step, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    finished_original(&engine, "a").await;

    let (fork_id, claim) = engine
        .fork_workflow("a", 3, Some("f"), &names(), &ForkOptions::default())
        .await
        .unwrap();
    assert_eq!(fork_id, "f");
    let claim = claim.expect("a direct fork is claimed here");
    assert_eq!(claim.input, "{\"n\": 1}");
    let run = run_of(claim);
    assert_eq!(
        engine.check_step(&run, 1, "step1").await.unwrap(),
        StepDecision::ReplayOutput("\"one\"".to_owned())
    );
    assert_eq!(
        engine.check_step(&run, 2, "step2").await.unwrap(),
        StepDecision::ReplayOutput("\"two\"".to_owned())
    );
    assert_eq!(
        engine.check_step(&run, 3, "DBOS.setEvent").await.unwrap(),
        StepDecision::Run
    );
    assert_eq!(event(&engine, "f").await, ["k=1"]);

    let fork = engine.get_status("f").await.unwrap();
    assert_eq!(fork.status, Status::Pending);
    assert_eq!(fork.forked_from.as_deref(), Some("a"));
    assert!(!fork.was_forked_from);
    let original = engine.get_status("a").await.unwrap();
    assert!(original.was_forked_from);
    assert_eq!(original.status, Status::Success);
    assert_eq!(event(&engine, "a").await, ["k=2"]);

    let listed = engine
        .list_workflows(&WorkflowFilter {
            forked_from: Some(vec!["a".to_owned()]),
            ..WorkflowFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].workflow_id, "f");
});

all_dbs!(fork_ids_options_and_errors, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "p").await;
    let child = StepRecord {
        child_workflow_id: Some("old-child".to_owned()),
        ..StepRecord::default()
    };
    engine
        .store()
        .save_step(
            &dwos_core::Lease {
                workflow_id: "p".into(),
                token: run.token().into(),
            },
            1,
            "child",
            &child,
        )
        .await
        .unwrap();
    engine
        .finish(&run, Outcome::Error("\"boom\"".to_owned()))
        .await
        .unwrap();

    assert!(matches!(
        engine
            .fork_workflow("missing", 1, None, &names(), &ForkOptions::default())
            .await,
        Err(Error::WorkflowNotFound(_))
    ));

    let options = ForkOptions {
        application_version: Some("v2".to_owned()),
        timeout_ms: Some(60_000),
        replacement_children: HashMap::from([("old-child".to_owned(), "new-child".to_owned())]),
        ..ForkOptions::default()
    };
    let (random_id, _) = engine
        .fork_workflow("p", 2, None, &[], &options)
        .await
        .unwrap();
    assert_ne!(random_id, "p");
    let fork = engine.get_status(&random_id).await.unwrap();
    assert_eq!(fork.application_version.as_deref(), Some("v2"));
    assert_eq!(fork.timeout_ms, Some(60_000));
    let steps = engine.list_steps(&random_id).await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(
        steps[0].record.child_workflow_id.as_deref(),
        Some("new-child")
    );

    // A repeat of the same fork is kept; another workflow's ID is in use.
    engine
        .fork_workflow("p", 2, Some(&random_id), &[], &options)
        .await
        .unwrap();
    assert!(matches!(
        engine.fork_workflow("p", 2, Some("p"), &[], &options).await,
        Err(Error::WorkflowIdInUse { .. })
    ));
});

all_dbs!(fork_onto_a_queue_waits_for_admission, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    finished_original(&engine, "a").await;
    engine
        .ensure_queue("q", NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    let options = ForkOptions {
        queue_name: Some("q".to_owned()),
        ..ForkOptions::default()
    };
    let (fork_id, claim) = engine
        .fork_workflow("a", 1, None, &names(), &options)
        .await
        .unwrap();
    assert!(claim.is_none());
    let fork = engine.get_status(&fork_id).await.unwrap();
    assert_eq!(fork.status, Status::Enqueued);
    assert_eq!(fork.queue_name.as_deref(), Some("q"));
    let claims = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].lease.workflow_id, fork_id);
    assert!(matches!(
        engine
            .fork_workflow(
                "a",
                1,
                None,
                &names(),
                &ForkOptions {
                    queue_name: Some("nope".into()),
                    ..ForkOptions::default()
                }
            )
            .await,
        Err(Error::QueueNotFound(_))
    ));
});

all_dbs!(rewind_discards_from_the_start_step, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let _pending = begin_run(&engine, "live").await;
    assert!(matches!(
        engine
            .rewind_workflow("live", None, &names(), None, None, None)
            .await,
        Err(Error::InvalidConfig(_))
    ));

    finished_original(&engine, "a").await;
    // A message never consumed: rewind drops it.
    engine
        .send("a", None, "\"m1\"", PORTABLE_JSON, None, None, None)
        .await
        .unwrap();
    let rows = engine.store().list_messages("a").await.unwrap();
    assert_eq!(rows.len(), 1);
    let claim = engine
        .rewind_workflow("a", Some(3), &names(), Some("v9"), None, None)
        .await
        .unwrap()
        .expect("a direct rewind is claimed here");
    let row = engine.get_status("a").await.unwrap();
    assert_eq!(row.status, Status::Pending);
    assert_eq!(row.output, None);
    assert_eq!(row.application_version.as_deref(), Some("v9"));
    let steps: Vec<u32> = engine
        .list_steps("a")
        .await
        .unwrap()
        .iter()
        .map(|s| s.step_id)
        .collect();
    assert_eq!(steps, [1, 2]);
    assert_eq!(event(&engine, "a").await, ["k=1"]);
    assert!(engine.store().list_messages("a").await.unwrap().is_empty());

    let run = run_of(claim);
    assert_eq!(
        engine.check_step(&run, 2, "step2").await.unwrap(),
        StepDecision::ReplayOutput("\"two\"".to_owned())
    );
    engine
        .finish(&run, Outcome::Success("\"again\"".to_owned()))
        .await
        .unwrap();

    // The default start step discards everything.
    engine
        .rewind_workflow("a", None, &names(), None, None, None)
        .await
        .unwrap();
    assert!(engine.list_steps("a").await.unwrap().is_empty());
    assert!(event(&engine, "a").await.is_empty());
});

all_dbs!(send_to_forks_reaches_forks_of_forks, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    finished_original(&engine, "a").await;
    engine
        .fork_workflow("a", 1, Some("f"), &[], &ForkOptions::default())
        .await
        .unwrap();
    engine
        .fork_workflow("f", 1, Some("g"), &[], &ForkOptions::default())
        .await
        .unwrap();
    let message = (
        "a".to_owned(),
        Some("t".to_owned()),
        "\"hi\"".to_owned(),
        Some("once".to_owned()),
    );
    engine
        .send_bulk_to_forks(std::slice::from_ref(&message), PORTABLE_JSON, None, None)
        .await
        .unwrap();
    engine
        .send_bulk_to_forks(&[message], PORTABLE_JSON, None, None)
        .await
        .unwrap();
    for id in ["a", "f", "g"] {
        let rows = engine.store().list_messages(id).await.unwrap();
        assert_eq!(rows.len(), 1, "{id}");
        assert_eq!(rows[0].message.text, "\"hi\"");
    }
});
