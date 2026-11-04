use std::num::NonZeroU32;
use std::time::Duration;

use dwos_core::{
    Begin, Begun, Claimed, Engine, EngineConfig, Error, Outcome, ParentStep, Run, Status,
    StepDecision,
};
use sqlx::Connection;

#[macro_use]
mod common;
use common::*;

fn begin(id: &str) -> Begin<'_> {
    Begin {
        name: "wf",
        input: "{}",
        workflow_id: Some(id),
        parent: None,
        max_recovery_attempts: 100,
        timeout_ms: None,
        ignore_propagated_deadline: false,
    }
}

fn config() -> EngineConfig {
    EngineConfig {
        lease: LONG,
        pickle: false,
        ..EngineConfig::default()
    }
}

async fn engine(db: &TestDb, config: EngineConfig) -> Engine {
    Engine::connect(&db.url, config).await.unwrap()
}

fn run(claimed: Claimed) -> Run {
    Run::new(
        claimed.lease,
        claimed.serialization,
        claimed.deadline_epoch_ms,
    )
}

async fn queue(engine: &Engine, concurrency: u32) {
    engine
        .ensure_queue("q", NonZeroU32::new(concurrency).unwrap())
        .await
        .unwrap();
}

all_dbs!(fifo_and_identity_survive_duplicate_enqueue, backend, {
    let db = TestDb::create(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 3).await;
    queue(&engine, 3).await;
    assert!(
        engine
            .ensure_queue("q", NonZeroU32::new(4).unwrap())
            .await
            .is_err()
    );
    assert!(engine.enqueue("missing", begin("missing")).await.is_err());
    for id in ["z-first", "a-second", "m-third"] {
        assert_eq!(engine.enqueue("q", begin(id)).await.unwrap(), id);
    }
    let mut connection = sqlx::AnyConnection::connect(&db.url).await.unwrap();
    sqlx::query("UPDATE workflow_status SET created_at = 1")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let Begun::Run(ordinary) = engine.begin(begin("ordinary")).await.unwrap() else {
        panic!("ordinary workflow should run");
    };
    assert_eq!(
        engine.enqueue("q", begin("ordinary")).await.unwrap(),
        "ordinary"
    );
    assert_eq!(
        engine.check_step(&ordinary, 1, "step").await.unwrap(),
        StepDecision::Run
    );
    assert_eq!(
        engine.enqueue("q", begin("z-first")).await.unwrap(),
        "z-first"
    );
    assert!(
        matches!(engine.begin(begin("z-first")).await.unwrap(), Begun::Attach(id) if id == "z-first")
    );
    let mut conflicting = begin("z-first");
    conflicting.name = "different";
    assert!(matches!(
        engine.enqueue("q", conflicting).await,
        Err(Error::ConflictingWorkflow { .. })
    ));
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(
        claimed
            .iter()
            .map(|row| row.lease.workflow_id.as_str())
            .collect::<Vec<_>>(),
        ["z-first", "a-second", "m-third"]
    );
    assert!(claimed.iter().all(|row| row.recovery_attempts == 1));
});

all_dbs!(
    ordinary_recovery_and_resume_cannot_bypass_admission,
    backend,
    {
        let db = TestDb::create(backend).await;
        let engine = engine(&db, config()).await;
        queue(&engine, 1).await;
        engine.enqueue("q", begin("queued")).await.unwrap();
        assert!(engine.recover_now(&names()).await.unwrap().is_empty());
        assert!(
            engine
                .store()
                .claim_one("queued", &names(), LONG)
                .await
                .unwrap()
                .is_none()
        );
        engine.cancel_workflow("queued").await.unwrap();
        assert!(
            engine
                .resume_workflow("queued", &names())
                .await
                .unwrap()
                .is_none()
        );
        assert!(engine.recover_now(&names()).await.unwrap().is_empty());
        let first = run(engine
            .claim_queue("q", &names())
            .await
            .unwrap()
            .pop()
            .unwrap());
        engine
            .save_step_output(&first, 1, "step", "42".to_owned())
            .await
            .unwrap();
        engine.release(&first).await.unwrap();
        assert!(engine.recover_now(&names()).await.unwrap().is_empty());
        let recovered = run(engine
            .claim_queue("q", &names())
            .await
            .unwrap()
            .pop()
            .unwrap());
        assert_ne!(first.token(), recovered.token());
        assert_eq!(
            engine.check_step(&recovered, 1, "step").await.unwrap(),
            StepDecision::ReplayOutput("42".to_owned())
        );
        assert!(matches!(
            engine.check_step(&first, 2, "later").await,
            Err(Error::LeaseLost(_))
        ));
    }
);

all_dbs!(
    global_capacity_counts_unsupported_owners_and_serializes_workers,
    backend,
    {
        let db = TestDb::create(backend).await;
        let producer = engine(
            &db,
            EngineConfig {
                pickle: true,
                ..config()
            },
        )
        .await;
        let first = engine(&db, config()).await;
        let second = engine(&db, config()).await;
        queue(&producer, 1).await;
        let mut unsupported = begin("unsupported");
        unsupported.name = "other";
        producer.enqueue("q", unsupported).await.unwrap();
        first.enqueue("q", begin("second")).await.unwrap();
        first.enqueue("q", begin("third")).await.unwrap();
        let owner = run(producer
            .claim_queue("q", &["other".to_owned()])
            .await
            .unwrap()
            .pop()
            .unwrap());
        assert!(first.claim_queue("q", &names()).await.unwrap().is_empty());
        producer
            .finish(&owner, Outcome::Success("null".to_owned()))
            .await
            .unwrap();
        let supported = names();
        let (a, b) = tokio::join!(
            first.claim_queue("q", &supported),
            second.claim_queue("q", &supported)
        );
        let admitted = a.unwrap().into_iter().chain(b.unwrap()).collect::<Vec<_>>();
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].lease.workflow_id, "second");
        first.cancel_workflow("second").await.unwrap();
        let next = second.claim_queue("q", &names()).await.unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].lease.workflow_id, "third");
    }
);

all_dbs!(
    expired_owners_are_fenced_and_dead_letters_do_not_block_fifo,
    backend,
    {
        let db = TestDb::create(backend).await;
        let engine = engine(
            &db,
            EngineConfig {
                lease: EXPIRED,
                ..config()
            },
        )
        .await;
        queue(&engine, 1).await;
        let mut limited = begin("first");
        limited.max_recovery_attempts = 1;
        engine.enqueue("q", limited).await.unwrap();
        engine.enqueue("q", begin("second")).await.unwrap();
        let old = run(engine
            .claim_queue("q", &names())
            .await
            .unwrap()
            .pop()
            .unwrap());
        let recovered = engine
            .claim_queue("q", &names())
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(recovered.lease.workflow_id, "first");
        assert_eq!(recovered.recovery_attempts, 2);
        assert_ne!(old.token(), recovered.lease.token);
        assert!(matches!(
            engine.check_step(&old, 1, "step").await,
            Err(Error::LeaseLost(_))
        ));
        let next = engine.claim_queue("q", &names()).await.unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].lease.workflow_id, "second");
        assert_eq!(
            engine.get_status("first").await.unwrap().status,
            Status::MaxRecoveryAttemptsExceeded
        );
    }
);

all_dbs!(relative_timeout_starts_on_first_admission, backend, {
    let db = TestDb::create(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 1).await;
    let mut timed = begin("timed");
    timed.timeout_ms = Some(100);
    engine.enqueue("q", timed).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let waiting = engine.get_status("timed").await.unwrap();
    assert_eq!(waiting.status, Status::Pending);
    assert_eq!(waiting.deadline_epoch_ms, None);
    let before_admission = engine.store().now_ms().await.unwrap();
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(claimed[0].deadline_epoch_ms.unwrap() >= before_admission + 100);
});

all_dbs!(
    queued_children_replay_ids_and_follow_deadline_policy,
    backend,
    {
        let db = TestDb::create(backend).await;
        let engine = engine(&db, config()).await;
        queue(&engine, 2).await;
        let mut parent_begin = begin("parent");
        parent_begin.timeout_ms = Some(500);
        let Begun::Run(parent) = engine.begin(parent_begin).await.unwrap() else {
            panic!("parent should run");
        };
        let mut inherited = begin("explicit-child");
        inherited.parent = Some(ParentStep {
            run: &parent,
            step_id: 1,
        });
        assert_eq!(
            engine.enqueue("q", inherited).await.unwrap(),
            "explicit-child"
        );
        inherited.workflow_id = Some("different-requested-id");
        assert_eq!(
            engine.enqueue("q", inherited).await.unwrap(),
            "explicit-child"
        );
        let mut overridden = begin("independent-child");
        overridden.parent = Some(ParentStep {
            run: &parent,
            step_id: 2,
        });
        overridden.timeout_ms = Some(2000);
        engine.enqueue("q", overridden).await.unwrap();
        assert_eq!(
            engine
                .get_status("explicit-child")
                .await
                .unwrap()
                .deadline_epoch_ms,
            parent.deadline_epoch_ms()
        );
        assert_eq!(
            engine
                .get_status("independent-child")
                .await
                .unwrap()
                .deadline_epoch_ms,
            None
        );
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert_eq!(
            engine.get_status("explicit-child").await.unwrap().status,
            Status::Cancelled
        );
        let claimed = engine.claim_queue("q", &names()).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].lease.workflow_id, "independent-child");
    }
);

all_dbs!(local_active_runs_bound_queue_batches, backend, {
    let db = TestDb::create(backend).await;
    let engine = engine(
        &db,
        EngineConfig {
            max_concurrent_recoveries: 1,
            ..config()
        },
    )
    .await;
    queue(&engine, 2).await;
    let Begun::Run(ordinary) = engine.begin(begin("ordinary")).await.unwrap() else {
        panic!("ordinary should run");
    };
    let active = engine.keep_lease(&ordinary);
    engine.enqueue("q", begin("queued")).await.unwrap();
    assert!(engine.claim_queue("q", &names()).await.unwrap().is_empty());
    drop(active);
    assert_eq!(engine.claim_queue("q", &names()).await.unwrap().len(), 1);
});
