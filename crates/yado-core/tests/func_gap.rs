use std::time::Duration;

use yado_core::{Begin, Begun, Engine, EngineConfig, Error, LeaseLoss, Run, StepDecision};

#[macro_use]
mod common;
use common::*;

fn begin<'a>(id: &'a str) -> Begin<'a> {
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

// The keeper reports max run before the database lease ends. The workflow has to be
// claimable then: stopping renewals alone leaves it owned until `claim_after`.
all_dbs!(
    max_run_gives_the_workflow_back_before_the_lease_ends,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = Engine::connect(
            &db.url,
            EngineConfig {
                lease: Duration::from_secs(30),
                max_run: Duration::from_millis(200),
                pickle: false,
                ..EngineConfig::default()
            },
        )
        .await
        .unwrap();
        let Begun::Run(run) = engine.begin(begin("a")).await.unwrap() else {
            panic!("expected a new run");
        };
        let active = engine.keep_lease(&run);
        let loss = tokio::time::timeout(Duration::from_secs(5), active.lost())
            .await
            .expect("max run should be reported");
        assert_eq!(loss, LeaseLoss::MaxRunExceeded);

        let claimed = engine.recover_now(&names()).await.unwrap();
        assert_eq!(claimed.len(), 1, "the workflow stayed leased after max run");
        assert_ne!(claimed[0].lease.token, run.token());
        let error = engine.check_step(&run, 1, "step").await.unwrap_err();
        assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
        let owner = Run::new(
            claimed[0].lease.clone(),
            claimed[0].serialization.clone(),
            claimed[0].deadline_epoch_ms,
        );
        assert_eq!(
            engine.check_step(&owner, 1, "step").await.unwrap(),
            StepDecision::Run
        );
    }
);
