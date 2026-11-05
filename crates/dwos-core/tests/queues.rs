use std::num::NonZeroU32;
use std::time::Duration;

use dwos_core::{
    Begin, Begun, Claimed, Engine, EngineConfig, Error, Outcome, ParentStep, ReusePolicy, Run,
    Status, StepDecision, WorkflowOptions,
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
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 3).await;
    queue(&engine, 3).await;
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

fn claimed_ids(claimed: &[Claimed]) -> Vec<&str> {
    claimed
        .iter()
        .map(|row| row.lease.workflow_id.as_str())
        .collect()
}

all_dbs!(raising_the_limit_admits_more_on_the_next_claim, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 1).await;
    for id in ["a", "b", "c", "d"] {
        engine.enqueue("q", begin(id)).await.unwrap();
    }
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(claimed_ids(&claimed), ["a"]);
    assert!(engine.claim_queue("q", &names()).await.unwrap().is_empty());
    queue(&engine, 3).await;
    assert_eq!(engine.get_queue("q").await.unwrap().unwrap().get(), 3);
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(claimed_ids(&claimed), ["b", "c"]);
});

all_dbs!(lowering_the_limit_waits_for_running_owners, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 3).await;
    for id in ["a", "b", "c", "d"] {
        engine.enqueue("q", begin(id)).await.unwrap();
    }
    let owners: Vec<Run> = engine
        .claim_queue("q", &names())
        .await
        .unwrap()
        .into_iter()
        .map(run)
        .collect();
    assert_eq!(owners.len(), 3);
    queue(&engine, 1).await;
    assert_eq!(engine.get_queue("q").await.unwrap().unwrap().get(), 1);
    assert!(engine.claim_queue("q", &names()).await.unwrap().is_empty());
    for owner in &owners {
        assert_eq!(
            engine.check_step(owner, 1, "step").await.unwrap(),
            StepDecision::Run
        );
    }
    // Two finishes leave one owner, which is still at the limit of one.
    for owner in &owners[..2] {
        engine
            .finish(owner, Outcome::Success("null".to_owned()))
            .await
            .unwrap();
        assert!(engine.claim_queue("q", &names()).await.unwrap().is_empty());
    }
    engine
        .finish(&owners[2], Outcome::Success("null".to_owned()))
        .await
        .unwrap();
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(claimed_ids(&claimed), ["d"]);
});

all_dbs!(registering_the_same_limit_changes_nothing, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 1).await;
    engine.enqueue("q", begin("a")).await.unwrap();
    engine.enqueue("q", begin("b")).await.unwrap();
    let owner = run(engine
        .claim_queue("q", &names())
        .await
        .unwrap()
        .pop()
        .unwrap());
    queue(&engine, 1).await;
    assert_eq!(engine.get_queue("q").await.unwrap().unwrap().get(), 1);
    assert!(engine.claim_queue("q", &names()).await.unwrap().is_empty());
    assert_eq!(
        engine.check_step(&owner, 1, "step").await.unwrap(),
        StepDecision::Run
    );
});

all_dbs!(reject_reuse_policy_refuses_a_queued_id, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 3).await;
    let reject = WorkflowOptions {
        reuse: ReusePolicy::Reject,
        ..WorkflowOptions::default()
    };
    assert_eq!(
        engine.enqueue_with("q", begin("a"), &reject).await.unwrap(),
        "a"
    );
    let error = engine
        .enqueue_with("q", begin("a"), &reject)
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::WorkflowIdInUse { ref status, .. } if status == "ENQUEUED"),
        "{error:?}"
    );
    assert_eq!(
        engine.get_status("a").await.unwrap().status,
        Status::Enqueued
    );
    assert_eq!(engine.get_status("a").await.unwrap().input, "{}");
});

all_dbs!(
    ordinary_recovery_and_resume_cannot_bypass_admission,
    backend,
    {
        let db = TestDb::migrated(backend).await;
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
        let db = TestDb::migrated(backend).await;
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
        let db = TestDb::migrated(backend).await;
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
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 1).await;
    let mut timed = begin("timed");
    timed.timeout_ms = Some(100);
    engine.enqueue("q", timed).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let waiting = engine.get_status("timed").await.unwrap();
    assert_eq!(waiting.status, Status::Enqueued);
    assert_eq!(waiting.queue_name.as_deref(), Some("q"));
    assert_eq!(waiting.deadline_epoch_ms, None);
    let before_admission = engine.store().now_ms().await.unwrap();
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(claimed[0].deadline_epoch_ms.unwrap() >= before_admission + 100);
    let admitted = engine.get_status("timed").await.unwrap();
    assert_eq!(admitted.status, Status::Pending);
    assert!(admitted.started_at_ms.is_some());
});

all_dbs!(
    queued_children_replay_ids_and_follow_deadline_policy,
    backend,
    {
        let db = TestDb::migrated(backend).await;
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
        let deadline = parent.deadline_epoch_ms().expect("parent has a deadline");
        let now = engine.store().now_ms().await.unwrap();
        let remaining = deadline.saturating_sub(now).saturating_add(50) as u64;
        tokio::time::sleep(Duration::from_millis(remaining)).await;
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
    let db = TestDb::migrated(backend).await;
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

// Far longer than any test, so only a wake can end a wait with this interval.
const NO_TIMER: Duration = Duration::from_secs(3600);
const BOUND: Duration = Duration::from_secs(30);

fn spawn_claims(engine: &Engine, interval: Duration) -> tokio::task::JoinHandle<Vec<Claimed>> {
    let engine = engine.clone();
    tokio::spawn(async move {
        loop {
            let claims = engine.wait_queue_claims("q", &names(), interval).await;
            if !claims.is_empty() {
                return claims;
            }
        }
    })
}

async fn claims_in_time(waiter: tokio::task::JoinHandle<Vec<Claimed>>) -> Vec<Claimed> {
    tokio::time::timeout(BOUND, waiter)
        .await
        .expect("the queue wait did not end in time")
        .unwrap()
}

all_dbs!(every_enqueue_wakes_a_queue_wait, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 1000).await;
    // Each round races the enqueue against the claim. A lost wakeup hangs on NO_TIMER.
    for round in 0..30 {
        let id = format!("queued-{round}");
        let waiter = spawn_claims(&engine, NO_TIMER);
        engine.enqueue("q", begin(&id)).await.unwrap();
        let claims = claims_in_time(waiter).await;
        assert_eq!(claims[0].lease.workflow_id, id);
    }
    assert_eq!(engine.waiting_keys(), 0);
});

all_dbs!(a_freed_slot_wakes_a_queue_wait, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 1).await;
    engine.enqueue("q", begin("first")).await.unwrap();
    engine.enqueue("q", begin("second")).await.unwrap();
    let claimed = engine.claim_queue("q", &names()).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let waiter = spawn_claims(&engine, NO_TIMER);
    let first = run(claimed.into_iter().next().unwrap());
    engine
        .finish(&first, Outcome::Success("1".to_owned()))
        .await
        .unwrap();
    engine.wake_queue("q");
    let claims = claims_in_time(waiter).await;
    assert_eq!(claims[0].lease.workflow_id, "second");
});

all_dbs!(a_second_engine_enqueue_is_seen_at_the_interval, backend, {
    let db = TestDb::migrated(backend).await;
    let (waiting, enqueuing) = (engine(&db, config()).await, engine(&db, config()).await);
    queue(&waiting, 1000).await;
    let waiter = spawn_claims(&waiting, Duration::from_millis(20));
    enqueuing.enqueue("q", begin("queued")).await.unwrap();
    let claims = claims_in_time(waiter).await;
    assert_eq!(claims[0].lease.workflow_id, "queued");
    assert_eq!(waiting.wakeups_received(), 0);
});

all_dbs!(an_empty_or_unreadable_queue_waits_its_interval, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db, config()).await;
    queue(&engine, 1).await;
    let interval = Duration::from_millis(50);
    for name in ["q", ""] {
        assert_eq!(
            engine.claim_queue(name, &names()).await.is_err(),
            name.is_empty()
        );
        let started = std::time::Instant::now();
        assert!(
            engine
                .wait_queue_claims(name, &names(), interval)
                .await
                .is_empty()
        );
        assert!(started.elapsed() >= interval);
    }
    assert_eq!(engine.waiting_keys(), 0);
});

all_dbs!(registering_with_no_limit_keeps_the_stored_limit, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    engine.ensure_queue_exists("open").await.unwrap();
    assert_eq!(
        engine.get_queue("open").await.unwrap(),
        Some(NonZeroU32::MAX)
    );
    let limit = NonZeroU32::new(3).unwrap();
    engine.ensure_queue("capped", limit).await.unwrap();
    engine.ensure_queue_exists("capped").await.unwrap();
    assert_eq!(engine.get_queue("capped").await.unwrap(), Some(limit));
});
