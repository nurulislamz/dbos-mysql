//! Forced interleavings for step fencing. Leases expire by writing `claim_after`, not by waiting.
//! Only the keeper tests use real lease timers, because the keeper's own clock is what they check.

use std::fmt::Debug;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dwos_core::{
    Begin, Begun, Claimed, Ended, Engine, EngineConfig, Error, FinalOutcome, Finished, Lease,
    LeaseKeeper, LeaseLoss, ParentStep, Run, Status, StepDecision, Store,
};
use sqlx::{Any, Transaction};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

#[macro_use]
mod common;
use common::*;

// SQLite has no connection to lose and does not retry, so lost-acknowledgment tests need a server.
macro_rules! networked {
    ($name:ident, $backend:ident, $body:block) => {
        mod $name {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn mysql() {
                let $backend = Backend::MySql;
                $body
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn postgres() {
                let $backend = Backend::Postgres;
                $body
            }
        }
    };
}

const REAL_LEASE: Duration = Duration::from_millis(400);
const PAST_EXPIRY: Duration = Duration::from_millis(50);

fn config(lease: Duration) -> EngineConfig {
    EngineConfig {
        lease,
        pickle: false,
        ..EngineConfig::default()
    }
}

async fn engine_at(url: &str, lease: Duration) -> Engine {
    Engine::connect(url, config(lease)).await.unwrap()
}

async fn engine(db: &TestDb) -> Engine {
    engine_at(&db.url, LONG).await
}

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

async fn begin_run(engine: &Engine, id: &str) -> Run {
    match engine.begin(begin(id)).await.unwrap() {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

fn run_of(claim: Claimed) -> Run {
    Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms)
}

fn lease_of(run: &Run) -> Lease {
    Lease {
        workflow_id: run.workflow_id().to_owned(),
        token: run.token().to_owned(),
    }
}

fn run_with(workflow_id: &str, token: &str) -> Run {
    let lease = Lease {
        workflow_id: workflow_id.to_owned(),
        token: token.to_owned(),
    };
    Run::new(lease, dwos_core::PORTABLE_JSON.to_owned(), None)
}

async fn expire(store: &Store, id: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = '{id}'"
    )))
    .execute(store.pool())
    .await
    .unwrap();
}

async fn take_over(engine: &Engine, id: &str) -> Run {
    expire(engine.store(), id).await;
    let claimed = engine.recover_now(&names()).await.unwrap();
    assert_eq!(claimed.len(), 1, "{claimed:?}");
    assert_eq!(claimed[0].lease.workflow_id, id);
    run_of(claimed.into_iter().next().unwrap())
}

fn replay(value: &str) -> StepDecision {
    StepDecision::ReplayOutput(value.to_owned())
}

#[track_caller]
fn assert_lost<T: Debug>(result: Result<T, Error>) {
    assert!(matches!(result, Err(Error::LeaseLost(_))), "{result:?}");
}

async fn saved_values(engine: &Engine, id: &str) -> Vec<Option<String>> {
    let steps = engine.list_steps(id).await.unwrap();
    steps.into_iter().map(|step| step.record.output).collect()
}

async fn hold_write_lock(store: &Store) -> Transaction<'static, Any> {
    let begin = if store.backend() == "sqlite" {
        "BEGIN IMMEDIATE"
    } else {
        "BEGIN"
    };
    store.pool().begin_with(begin).await.unwrap()
}

async fn execute(tx: &mut Transaction<'static, Any>, sql: &'static str) {
    sqlx::query(sql).execute(&mut **tx).await.unwrap();
}

// A takeover and the new owner's save, not committed yet.
async fn takeover_in_progress(store: &Store) -> Transaction<'static, Any> {
    let mut tx = hold_write_lock(store).await;
    execute(
        &mut tx,
        "UPDATE workflow_status SET lease_token = 'new-owner', claim_after = 9000000000000000 WHERE workflow_uuid = 'a'",
    )
    .await;
    execute(
        &mut tx,
        "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, serialization, completed_at_epoch_ms) \
         VALUES ('a', 1, 'step', 'new', 'portable_json', 0)",
    )
    .await;
    tx
}

// Waits until another session is blocked on a lock in this test's own database.
async fn wait_until_blocked(store: &Store) {
    let sql = match store.backend() {
        "postgres" => {
            "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'"
        }
        "mysql" => {
            "SELECT COUNT(*) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l \
             ON w.REQUESTING_ENGINE_LOCK_ID = l.ENGINE_LOCK_ID WHERE l.OBJECT_SCHEMA = DATABASE()"
        }
        // shortcut: SQLite does not show its busy waiters, so this only gives the other task time to reach the lock.
        _ => {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return;
        }
    };
    let deadline = Instant::now() + Duration::from_secs(9);
    loop {
        let waiting: i64 = sqlx::query_scalar(sql)
            .fetch_one(store.pool())
            .await
            .unwrap();
        if waiting > 0 {
            return;
        }
        assert!(Instant::now() < deadline, "nothing blocked on the lock");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[derive(Clone, Copy)]
enum Fault {
    // Forward the request, wait for the server's reply, drop that reply and close the client.
    LoseReply,
    // Close the client without forwarding the request.
    Cut,
}

struct WireState {
    armed: Mutex<Option<(&'static str, Fault)>>,
    tripped: Notify,
    open: watch::Sender<bool>,
}

impl WireState {
    // Traffic is held before the client can see the failure, so its retry cannot run until `open`.
    fn trip(&self) {
        self.open.send_replace(false);
        self.tripped.notify_one();
    }

    fn fault_for(&self, request: &[u8]) -> Option<Fault> {
        let mut armed = self.armed.lock().unwrap();
        let (pattern, fault) = (*armed)?;
        let found = request
            .windows(pattern.len())
            .any(|window| window == pattern.as_bytes());
        found.then(|| {
            *armed = None;
            fault
        })
    }
}

/// A TCP proxy between one store and its server. It fails one chosen request, and holds traffic on demand.
struct WireFault {
    url: String,
    state: Arc<WireState>,
    accepting: JoinHandle<()>,
}

impl Drop for WireFault {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

impl WireFault {
    async fn start(db_url: &str) -> Self {
        let (credentials, rest) = db_url.rsplit_once('@').unwrap();
        let (server, database) = rest.split_once('/').unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // Without TLS, so that the proxy can read the requests.
        let plain = if db_url.starts_with("mysql") {
            "ssl-mode=DISABLED"
        } else {
            "sslmode=disable"
        };
        let url = format!(
            "{credentials}@{}/{database}?{plain}",
            listener.local_addr().unwrap()
        );
        let state = Arc::new(WireState {
            armed: Mutex::new(None),
            tripped: Notify::new(),
            open: watch::channel(true).0,
        });
        let server = server.to_owned();
        let shared = state.clone();
        let accepting = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(relay(client, server.clone(), shared.clone()));
            }
        });
        Self {
            url,
            state,
            accepting,
        }
    }

    fn arm(&self, pattern: &'static str, fault: Fault) {
        *self.state.armed.lock().unwrap() = Some((pattern, fault));
    }

    async fn tripped(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.tripped.notified())
            .await
            .expect("the armed request never arrived");
    }

    fn hold(&self) {
        self.state.open.send_replace(false);
    }

    fn open(&self) {
        self.state.open.send_replace(true);
    }
}

async fn relay(client: TcpStream, server: String, state: Arc<WireState>) {
    let Ok(server) = TcpStream::connect(server).await else {
        return;
    };
    let (mut from_client, mut to_client) = client.into_split();
    let (mut from_server, mut to_server) = server.into_split();
    let lose_reply = Arc::new(AtomicBool::new(false));
    let replies = {
        let (state, lose_reply) = (state.clone(), lose_reply.clone());
        tokio::spawn(async move {
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let size = match from_server.read(&mut buffer).await {
                    Ok(size) if size > 0 => size,
                    _ => return,
                };
                if lose_reply.load(Ordering::SeqCst) {
                    state.trip();
                    return;
                }
                if to_client.write_all(&buffer[..size]).await.is_err() {
                    return;
                }
            }
        })
    };
    let mut open = state.open.subscribe();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let size = match from_client.read(&mut buffer).await {
            Ok(size) if size > 0 => size,
            _ => break,
        };
        if open.wait_for(|open| *open).await.is_err() {
            break;
        }
        match state.fault_for(&buffer[..size]) {
            Some(Fault::LoseReply) => lose_reply.store(true, Ordering::SeqCst),
            Some(Fault::Cut) => {
                state.trip();
                break;
            }
            None => {}
        }
        if to_server.write_all(&buffer[..size]).await.is_err() {
            break;
        }
    }
    replies.abort();
}

// Question 1: can check_step return Run when the step is already saved?

all_dbs!(
    q1_after_takeover_only_the_new_owner_sees_the_step,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let old = begin_run(&engine, "a").await;
        let current = take_over(&engine, "a").await;
        engine
            .save_step_output(&current, 1, "step", "new".to_owned())
            .await
            .unwrap();

        assert_lost(engine.check_step(&old, 1, "step").await);
        assert_lost(engine.check_step(&old, 2, "step").await);
        assert_eq!(
            engine.check_step(&current, 1, "step").await.unwrap(),
            replay("new")
        );
        let next = take_over(&engine, "a").await;
        assert_eq!(
            engine.check_step(&next, 1, "step").await.unwrap(),
            replay("new")
        );
    }
);

all_dbs!(q1_repeated_checks_say_run_only_until_the_save, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "a").await;
    for _ in 0..2 {
        assert_eq!(
            engine.check_step(&run, 1, "step").await.unwrap(),
            StepDecision::Run
        );
    }
    assert!(engine.list_steps("a").await.unwrap().is_empty());
    engine
        .save_step_error(&run, 1, "step", "failed".to_owned())
        .await
        .unwrap();
    assert_eq!(
        engine.check_step(&run, 1, "step").await.unwrap(),
        StepDecision::ReplayError("failed".to_owned())
    );
    let current = take_over(&engine, "a").await;
    assert_eq!(
        engine.check_step(&current, 1, "step").await.unwrap(),
        StepDecision::ReplayError("failed".to_owned())
    );
});

networked!(
    q1_a_save_abandoned_after_a_lost_ack_still_replays,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let proxy = WireFault::start(&db.url).await;
        let engine = engine_at(&proxy.url, LONG).await;
        let run = Arc::new(begin_run(&engine, "a").await);

        proxy.arm("COMMIT", Fault::LoseReply);
        let (saver, saving_run) = (engine.clone(), run.clone());
        let saving = tokio::spawn(async move {
            saver
                .save_step_output(&saving_run, 1, "step", "v".to_owned())
                .await
        });
        proxy.tripped().await;
        // The caller gives up, so it never learns that the commit landed.
        saving.abort();
        proxy.open();

        assert_eq!(
            engine.check_step(&run, 1, "step").await.unwrap(),
            replay("v")
        );
    }
);

// Question 2: execution decisions on an expired lease that nobody has replaced.

all_dbs!(q2_an_expired_lease_gets_no_run_and_is_revoked, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let old = begin_run(&engine, "a").await;
    expire(engine.store(), "a").await;

    let retries = engine.store().transient_retries();
    for _ in 0..3 {
        assert_lost(engine.check_step(&old, 1, "step").await);
    }
    assert_eq!(engine.store().transient_retries(), retries, "retry storm");

    // The refused owner cannot return: the next owner claims it once, spending one attempt.
    assert_lost(
        engine
            .store()
            .renew_lease(&lease_of(&old), Duration::from_secs(60))
            .await,
    );
    let claimed = engine.recover_now(&names()).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].recovery_attempts, 2);
    assert!(engine.recover_now(&names()).await.unwrap().is_empty());
    let current = run_of(claimed.into_iter().next().unwrap());
    assert_eq!(
        engine.check_step(&current, 1, "step").await.unwrap(),
        StepDecision::Run
    );
});

all_dbs!(q2_a_child_cannot_start_from_an_expired_parent, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let parent = begin_run(&engine, "a").await;
    expire(engine.store(), "a").await;
    let mut child = begin("child");
    child.parent = Some(ParentStep {
        run: &parent,
        step_id: 1,
    });
    assert_lost(engine.begin(child).await);
    assert!(matches!(
        engine.get_status("child").await,
        Err(Error::WorkflowNotFound(_))
    ));
});

all_dbs!(q2_a_refused_queue_member_is_admitted_again, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    engine
        .ensure_queue("q", NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    engine.enqueue("q", begin("a")).await.unwrap();
    let mut claimed = engine.claim_queue("q", &names()).await.unwrap();
    let old = run_of(claimed.pop().unwrap());
    expire(engine.store(), "a").await;
    assert_lost(engine.check_step(&old, 1, "step").await);
    let mut claimed = engine.claim_queue("q", &names()).await.unwrap();
    let current = run_of(claimed.pop().unwrap());
    assert_ne!(old.token(), current.token());
    assert_eq!(
        engine.check_step(&current, 1, "step").await.unwrap(),
        StepDecision::Run
    );
});

all_dbs!(q2_an_expired_lease_still_replays_saved_steps, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "a").await;
    engine
        .save_step_output(&run, 1, "step", "v".to_owned())
        .await
        .unwrap();
    expire(engine.store(), "a").await;
    // Replay starts no body, so it does not revoke, and the finished work can still be kept.
    assert_eq!(
        engine.check_step(&run, 1, "step").await.unwrap(),
        replay("v")
    );
    assert_eq!(
        engine
            .conclude(&run, Ended::Success("done".to_owned()))
            .await
            .unwrap(),
        Finished::Accepted
    );
});

all_dbs!(
    q2_save_and_finish_after_expiry_without_takeover_stay_valid,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let run = begin_run(&engine, "a").await;
        expire(engine.store(), "a").await;
        engine
            .save_step_output(&run, 1, "step", "v".to_owned())
            .await
            .unwrap();
        assert_eq!(
            engine
                .conclude(&run, Ended::Success("done".to_owned()))
                .await
                .unwrap(),
            Finished::Accepted
        );
        let row = engine.get_status("a").await.unwrap();
        assert_eq!(row.status, Status::Success);
        assert_eq!(row.output.as_deref(), Some("done"));
    }
);

// An SDK raises LeaseLost through the workflow body, and the body's failure reaches `conclude`.
all_dbs!(
    q2_a_refused_owner_cannot_store_its_refusal_as_final,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let old = Arc::new(begin_run(&engine, "a").await);
        expire(engine.store(), "a").await;
        assert_lost(engine.check_step(&old, 1, "step").await);
        assert_lost(
            engine
                .save_step_output(&old, 1, "step", "stale".to_owned())
                .await,
        );

        let (stale, stale_run) = (engine.clone(), old.clone());
        let concluding = tokio::spawn(async move {
            stale
                .conclude(&stale_run, Ended::Error("lease lost".to_owned()))
                .await
        });

        let claimed = engine.recover_now(&names()).await.unwrap();
        let current = run_of(claimed.into_iter().next().unwrap());
        assert_eq!(
            engine.check_step(&current, 1, "step").await.unwrap(),
            StepDecision::Run
        );
        assert_eq!(
            engine
                .conclude(&current, Ended::Success("done".to_owned()))
                .await
                .unwrap(),
            Finished::Accepted
        );
        let Finished::Superseded(result) = concluding.await.unwrap().unwrap() else {
            panic!("the refused owner's error must not be accepted");
        };
        assert_eq!(result.outcome, FinalOutcome::Success("done".to_owned()));
        assert_eq!(engine.get_status("a").await.unwrap().recovery_attempts, 2);
    }
);

// renew_lease is not refused on expiry. Renewal and reclaim serialize on the row, and the token
// fences them, so a renewal that lands first restores real ownership. Refusing it would only cost
// work when a claim sat unstarted for most of a lease.
all_dbs!(
    q2_a_renewal_rescues_an_expired_lease_before_any_refusal,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let run = begin_run(&engine, "a").await;
        expire(engine.store(), "a").await;
        engine
            .store()
            .renew_lease(&lease_of(&run), LONG)
            .await
            .unwrap();
        assert!(engine.recover_now(&names()).await.unwrap().is_empty());
        assert_eq!(
            engine.check_step(&run, 1, "step").await.unwrap(),
            StepDecision::Run
        );
    }
);

all_dbs!(
    q2_a_renewal_cannot_undo_a_revocation_in_progress,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let run = begin_run(&engine, "a").await;
        let mut revoking = hold_write_lock(engine.store()).await;
        execute(
        &mut revoking,
        "UPDATE workflow_status SET lease_token = NULL, claim_after = 0 WHERE workflow_uuid = 'a'",
    )
    .await;
        let (store, lease) = (engine.store().clone(), lease_of(&run));
        let renewing = tokio::spawn(async move { store.renew_lease(&lease, LONG).await });
        wait_until_blocked(engine.store()).await;
        assert!(!renewing.is_finished());
        revoking.commit().await.unwrap();
        assert_lost(renewing.await.unwrap());
    }
);

all_dbs!(q2_a_check_blocked_by_a_renewal_sees_the_renewal, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = Arc::new(begin_run(&engine, "a").await);
    expire(engine.store(), "a").await;
    let mut renewing = hold_write_lock(engine.store()).await;
    execute(
        &mut renewing,
        "UPDATE workflow_status SET claim_after = 9000000000000000 WHERE workflow_uuid = 'a'",
    )
    .await;
    let (checker, checking_run) = (engine.clone(), run.clone());
    let checking = tokio::spawn(async move { checker.check_step(&checking_run, 1, "step").await });
    wait_until_blocked(engine.store()).await;
    assert!(!checking.is_finished());
    renewing.commit().await.unwrap();
    assert_eq!(checking.await.unwrap().unwrap(), StepDecision::Run);
});

all_dbs!(q2_one_failed_renewal_causes_no_loss, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine_at(&db.url, REAL_LEASE).await;
    let run = begin_run(&engine, "a").await;
    let calls = AtomicU32::new(0);
    let (store, lease) = (engine.store().clone(), lease_of(&run));
    let keeper = LeaseKeeper::spawn(REAL_LEASE, LONG, move || {
        let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
        let (store, lease) = (store.clone(), lease.clone());
        async move {
            if first {
                return Err(Error::Timeout(Duration::ZERO));
            }
            store.renew_lease(&lease, REAL_LEASE).await
        }
    });
    // Past the original expiry, which only the second renewal can have moved.
    tokio::time::sleep(REAL_LEASE + PAST_EXPIRY).await;
    assert_eq!(
        engine.check_step(&run, 1, "step").await.unwrap(),
        StepDecision::Run
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), keeper.lost())
            .await
            .is_err()
    );
});

// The keeper gives up at 5/6 of the lease, so an owner hears of the loss before any refusal.
all_dbs!(
    q2_the_keeper_reports_loss_before_the_lease_expires,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine_at(&db.url, REAL_LEASE).await;
        let run = begin_run(&engine, "a").await;
        let started = Instant::now();
        let keeper = LeaseKeeper::spawn(REAL_LEASE, LONG, || async {
            Err(Error::Timeout(Duration::ZERO))
        });
        assert_eq!(keeper.lost().await, LeaseLoss::Unrenewable);
        assert!(started.elapsed() < REAL_LEASE, "{:?}", started.elapsed());
        tokio::time::sleep_until((started + REAL_LEASE + PAST_EXPIRY).into()).await;
        assert_lost(engine.check_step(&run, 1, "step").await);
    }
);

networked!(q2_a_brief_outage_causes_no_loss, backend, {
    let db = TestDb::migrated(backend).await;
    let proxy = WireFault::start(&db.url).await;
    let engine = engine_at(&proxy.url, REAL_LEASE).await;
    let run = Arc::new(begin_run(&engine, "a").await);
    let active = engine.keep_lease(&run);

    // Long enough to catch the first renewal, which starts at a third of the lease.
    proxy.hold();
    let (checker, checking_run) = (engine.clone(), run.clone());
    let checking = tokio::spawn(async move { checker.check_step(&checking_run, 1, "step").await });
    tokio::time::sleep(REAL_LEASE / 2).await;
    assert!(!checking.is_finished());
    proxy.open();

    assert_eq!(checking.await.unwrap().unwrap(), StepDecision::Run);
    assert!(
        tokio::time::timeout(REAL_LEASE, active.lost())
            .await
            .is_err()
    );
    assert_eq!(
        engine.check_step(&run, 2, "step").await.unwrap(),
        StepDecision::Run
    );
});

// The limit the core keeps: a check can be honest, and the body can still start much later.
all_dbs!(
    q2_the_gap_after_a_check_is_fenced_only_at_the_save,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let old = begin_run(&engine, "a").await;
        assert_eq!(
            engine.check_step(&old, 1, "step").await.unwrap(),
            StepDecision::Run
        );
        // The old owner pauses here, holding a valid Run decision. The core cannot see this pause.
        let current = take_over(&engine, "a").await;
        assert_eq!(
            engine.check_step(&current, 1, "step").await.unwrap(),
            StepDecision::Run
        );
        engine
            .save_step_output(&current, 1, "step", "new".to_owned())
            .await
            .unwrap();
        // The old body now starts after that save. Only its result can be stopped.
        assert_lost(
            engine
                .save_step_output(&old, 1, "step", "old".to_owned())
                .await,
        );
        assert_eq!(saved_values(&engine, "a").await, [Some("new".to_owned())]);
    }
);

// Question 3: retries across an outage.

all_dbs!(q3_a_blocked_check_sees_the_committed_takeover, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = Arc::new(begin_run(&engine, "a").await);
    let takeover = takeover_in_progress(engine.store()).await;
    let (checker, checking_run) = (engine.clone(), run.clone());
    let checking = tokio::spawn(async move { checker.check_step(&checking_run, 1, "step").await });
    wait_until_blocked(engine.store()).await;
    assert!(!checking.is_finished());
    takeover.commit().await.unwrap();

    assert_lost(checking.await.unwrap());
    assert_eq!(
        engine
            .check_step(&run_with("a", "new-owner"), 1, "step")
            .await
            .unwrap(),
        replay("new")
    );
});

all_dbs!(
    q3_a_blocked_check_survives_a_rolled_back_takeover,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let run = Arc::new(begin_run(&engine, "a").await);
        let takeover = takeover_in_progress(engine.store()).await;
        let (checker, checking_run) = (engine.clone(), run.clone());
        let checking =
            tokio::spawn(async move { checker.check_step(&checking_run, 1, "step").await });
        wait_until_blocked(engine.store()).await;
        assert!(!checking.is_finished());
        takeover.rollback().await.unwrap();
        assert_eq!(checking.await.unwrap().unwrap(), StepDecision::Run);
    }
);

networked!(
    q3_a_check_retried_across_a_takeover_sees_fresh_state,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let direct = { engine(&db).await };
        let proxy = WireFault::start(&db.url).await;
        let engine = engine_at(&proxy.url, LONG).await;
        let run = Arc::new(begin_run(&engine, "a").await);

        proxy.arm("lease_token, serialization", Fault::Cut);
        let (checker, checking_run) = (engine.clone(), run.clone());
        let checking =
            tokio::spawn(async move { checker.check_step(&checking_run, 1, "step").await });
        proxy.tripped().await;
        let current = take_over(&direct, "a").await;
        direct
            .save_step_output(&current, 1, "step", "new".to_owned())
            .await
            .unwrap();
        proxy.open();

        assert_lost(checking.await.unwrap());
        assert!(engine.store().transient_retries() >= 1, "no retry happened");
    }
);

networked!(q3_a_retried_check_by_the_owner_sees_a_new_save, backend, {
    let db = TestDb::migrated(backend).await;
    let direct = { engine(&db).await };
    let proxy = WireFault::start(&db.url).await;
    let engine = engine_at(&proxy.url, LONG).await;
    let run = Arc::new(begin_run(&engine, "a").await);

    proxy.arm("lease_token, serialization", Fault::Cut);
    let (checker, checking_run) = (engine.clone(), run.clone());
    let checking = tokio::spawn(async move { checker.check_step(&checking_run, 1, "step").await });
    proxy.tripped().await;
    direct
        .save_step_output(&run, 1, "step", "saved".to_owned())
        .await
        .unwrap();
    proxy.open();

    assert_eq!(checking.await.unwrap().unwrap(), replay("saved"));
});

networked!(q3_a_save_retried_after_a_lost_ack_is_idempotent, backend, {
    let db = TestDb::migrated(backend).await;
    let direct = { engine(&db).await };
    let proxy = WireFault::start(&db.url).await;
    let engine = engine_at(&proxy.url, LONG).await;
    let run = Arc::new(begin_run(&engine, "a").await);

    proxy.arm("COMMIT", Fault::LoseReply);
    let (saver, saving_run) = (engine.clone(), run.clone());
    let saving = tokio::spawn(async move {
        saver
            .save_step_output(&saving_run, 1, "step", "v".to_owned())
            .await
    });
    proxy.tripped().await;
    // The commit landed, and the client has not heard.
    assert_eq!(saved_values(&direct, "a").await, [Some("v".to_owned())]);
    proxy.open();

    saving.await.unwrap().unwrap();
    assert!(engine.store().transient_retries() >= 1, "no retry happened");
    assert_eq!(saved_values(&direct, "a").await, [Some("v".to_owned())]);
    assert_eq!(
        engine.check_step(&run, 1, "step").await.unwrap(),
        replay("v")
    );
});

networked!(q3_a_retried_save_is_refused_after_a_takeover, backend, {
    let db = TestDb::migrated(backend).await;
    let direct = { engine(&db).await };
    let proxy = WireFault::start(&db.url).await;
    let engine = engine_at(&proxy.url, LONG).await;
    let run = Arc::new(begin_run(&engine, "a").await);

    proxy.arm("COMMIT", Fault::LoseReply);
    let (saver, saving_run) = (engine.clone(), run.clone());
    let saving = tokio::spawn(async move {
        saver
            .save_step_output(&saving_run, 1, "step", "v".to_owned())
            .await
    });
    proxy.tripped().await;
    let current = take_over(&direct, "a").await;
    proxy.open();

    // Its own row is there, but it no longer owns the workflow, so it is told so.
    assert_lost(saving.await.unwrap());
    assert!(engine.store().transient_retries() >= 1, "no retry happened");
    // The commit stands: the row was written while it was still the owner.
    assert_eq!(
        direct.check_step(&current, 1, "step").await.unwrap(),
        replay("v")
    );
});

// The same two calls as a retry, for SQLite too.
all_dbs!(
    q3_repeating_a_save_is_idempotent_only_for_the_owner,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let old = begin_run(&engine, "a").await;
        for _ in 0..2 {
            engine
                .save_step_output(&old, 1, "step", "v".to_owned())
                .await
                .unwrap();
        }
        assert_eq!(saved_values(&engine, "a").await, [Some("v".to_owned())]);
        take_over(&engine, "a").await;
        assert_lost(
            engine
                .save_step_output(&old, 1, "step", "v".to_owned())
                .await,
        );
    }
);

// Question 4: stale owners, and values that never change.

all_dbs!(q4_a_recorded_value_never_changes, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let old = begin_run(&engine, "a").await;
    let current = take_over(&engine, "a").await;

    assert_lost(
        engine
            .save_step_output(&old, 1, "step", "old".to_owned())
            .await,
    );
    assert_lost(
        engine
            .save_step_error(&old, 1, "step", "old".to_owned())
            .await,
    );
    assert!(engine.list_steps("a").await.unwrap().is_empty());

    engine
        .save_step_output(&current, 1, "step", "new".to_owned())
        .await
        .unwrap();
    assert_lost(
        engine
            .save_step_output(&old, 1, "step", "new".to_owned())
            .await,
    );
    assert!(matches!(
        engine
            .save_step_output(&current, 1, "step", "other".to_owned())
            .await,
        Err(Error::StepNondeterminism { .. })
    ));
    assert!(matches!(
        engine
            .save_step_error(&current, 1, "step", "other".to_owned())
            .await,
        Err(Error::StepNondeterminism { .. })
    ));
    assert_eq!(saved_values(&engine, "a").await, [Some("new".to_owned())]);
});

all_dbs!(q4_the_new_owner_replays_only_what_was_committed, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let old = begin_run(&engine, "a").await;
    engine
        .save_step_output(&old, 1, "one", "1".to_owned())
        .await
        .unwrap();
    let current = take_over(&engine, "a").await;
    assert_lost(
        engine
            .save_step_output(&old, 2, "two", "2".to_owned())
            .await,
    );

    // A write that never commits leaves nothing to replay.
    let mut uncommitted = hold_write_lock(engine.store()).await;
    execute(
        &mut uncommitted,
        "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, serialization, completed_at_epoch_ms) \
         VALUES ('a', 3, 'three', '3', 'portable_json', 0)",
    )
    .await;
    uncommitted.rollback().await.unwrap();

    assert_eq!(
        engine.check_step(&current, 1, "one").await.unwrap(),
        replay("1")
    );
    for (step, name) in [(2, "two"), (3, "three")] {
        assert_eq!(
            engine.check_step(&current, step, name).await.unwrap(),
            StepDecision::Run
        );
    }
});

all_dbs!(
    q4_a_stale_conclude_cannot_overwrite_or_free_the_workflow,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let old = Arc::new(begin_run(&engine, "a").await);
        let current = take_over(&engine, "a").await;

        // An interrupted stale owner changes nothing.
        assert_eq!(
            engine.conclude(&old, Ended::Interrupted).await.unwrap(),
            Finished::Accepted
        );
        let row = engine.get_status("a").await.unwrap();
        assert_eq!((row.status, row.recovery_attempts), (Status::Pending, 2));
        assert!(engine.recover_now(&names()).await.unwrap().is_empty());

        // Stale results wait for the real one, and do not become it.
        let mut stale = Vec::new();
        for ended in [
            Ended::Success("stale".to_owned()),
            Ended::Error("stale".to_owned()),
        ] {
            let (engine, old) = (engine.clone(), old.clone());
            stale.push(tokio::spawn(
                async move { engine.conclude(&old, ended).await },
            ));
        }
        engine
            .store()
            .renew_lease(&lease_of(&current), LONG)
            .await
            .unwrap();
        assert_eq!(
            engine
                .conclude(&current, Ended::Success("real".to_owned()))
                .await
                .unwrap(),
            Finished::Accepted
        );
        for concluding in stale {
            let Finished::Superseded(result) = concluding.await.unwrap().unwrap() else {
                panic!("a stale result was accepted");
            };
            assert_eq!(result.outcome, FinalOutcome::Success("real".to_owned()));
        }
        let row = engine.get_status("a").await.unwrap();
        assert_eq!(row.output.as_deref(), Some("real"));
        assert_eq!(row.error, None);
    }
);

all_dbs!(q4_a_stale_error_during_shutdown_changes_nothing, backend, {
    let db = TestDb::migrated(backend).await;
    let engine_open = engine(&db).await;
    let old = begin_run(&engine_open, "a").await;
    let current = take_over(&engine_open, "a").await;

    let closing = self::engine(&db).await;
    closing.close();
    assert!(matches!(
        closing
            .conclude(&old, Ended::Error("stale".to_owned()))
            .await,
        Err(Error::ShuttingDown)
    ));
    assert_eq!(
        engine_open.get_status("a").await.unwrap().status,
        Status::Pending
    );
    engine_open
        .store()
        .renew_lease(&lease_of(&current), LONG)
        .await
        .unwrap();
});

// A finish is one statement, so its retry after a lost acknowledgment is this second call.
all_dbs!(q4_a_repeated_conclude_returns_the_stored_result, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "a").await;
    assert_eq!(
        engine
            .conclude(&run, Ended::Success("first".to_owned()))
            .await
            .unwrap(),
        Finished::Accepted
    );
    let Finished::Superseded(result) = engine
        .conclude(&run, Ended::Success("second".to_owned()))
        .await
        .unwrap()
    else {
        panic!("a second result was accepted");
    };
    assert_eq!(result.outcome, FinalOutcome::Success("first".to_owned()));
});

// Question 5: one request token can own several workflows.

all_dbs!(q5_one_token_is_fenced_per_workflow, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    for id in ["a", "b", "c"] {
        begin_run(&engine, id).await;
        expire(engine.store(), id).await;
    }
    let claimed = engine.recover_now(&names()).await.unwrap();
    assert_eq!(claimed.len(), 3);
    let token = claimed[0].lease.token.clone();
    assert!(claimed.iter().all(|claim| claim.lease.token == token));
    let mut runs: Vec<Run> = claimed.into_iter().map(run_of).collect();
    let run_c = runs.pop().unwrap();
    let run_b = runs.pop().unwrap();
    let run_a = runs.pop().unwrap();

    let current_a = take_over(&engine, "a").await;
    engine.release(&run_c).await.unwrap();
    let current_c = run_of(engine.recover_now(&names()).await.unwrap().pop().unwrap());
    assert_ne!(current_a.token(), token);
    assert_ne!(current_c.token(), token);

    // Replaced and released workflows refuse the shared token. Its other workflow keeps it.
    for run in [&run_a, &run_c] {
        assert_lost(engine.check_step(run, 1, "step").await);
        assert_lost(
            engine
                .save_step_output(run, 1, "step", "old".to_owned())
                .await,
        );
        assert_lost(engine.store().renew_lease(&lease_of(run), LONG).await);
    }
    assert_eq!(
        engine.check_step(&run_b, 1, "step").await.unwrap(),
        StepDecision::Run
    );
    engine
        .store()
        .renew_lease(&lease_of(&run_b), LONG)
        .await
        .unwrap();
    assert_eq!(
        engine
            .conclude(&run_b, Ended::Success("done".to_owned()))
            .await
            .unwrap(),
        Finished::Accepted
    );
    for current in [&current_a, &current_c] {
        assert_eq!(
            engine.check_step(current, 1, "step").await.unwrap(),
            StepDecision::Run
        );
    }
});

all_dbs!(q5_expiry_revokes_only_the_workflow_that_expired, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    for id in ["a", "b"] {
        begin_run(&engine, id).await;
        expire(engine.store(), id).await;
    }
    let mut claimed = engine.recover_now(&names()).await.unwrap();
    let run_b = run_of(claimed.pop().unwrap());
    let run_a = run_of(claimed.pop().unwrap());
    expire(engine.store(), "a").await;
    assert_lost(engine.check_step(&run_a, 1, "step").await);
    assert_eq!(
        engine.check_step(&run_b, 1, "step").await.unwrap(),
        StepDecision::Run
    );
});

// Starts two due workflows, then loses the acknowledgment of a claim on both.
async fn claim_with_lost_ack(
    direct: &Engine,
    proxied: &Engine,
    proxy: &WireFault,
    ids: &[&str],
) -> JoinHandle<Result<Vec<Claimed>, Error>> {
    for id in ids {
        begin_run(direct, id).await;
        expire(direct.store(), id).await;
    }
    proxy.arm("COMMIT", Fault::LoseReply);
    let store = proxied.store().clone();
    let claiming = tokio::spawn(async move { store.reclaim(&names(), 10, LONG).await });
    proxy.tripped().await;
    claiming
}

networked!(
    q5_a_retried_reclaim_returns_only_what_it_still_owns,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let direct = { engine(&db).await };
        let proxy = WireFault::start(&db.url).await;
        let proxied = engine_at(&proxy.url, LONG).await;
        let claiming = claim_with_lost_ack(&direct, &proxied, &proxy, &["a", "b"]).await;
        let current = take_over(&direct, "a").await;
        proxy.open();

        let claimed = claiming.await.unwrap().unwrap();
        assert!(
            proxied.store().transient_retries() >= 1,
            "no retry happened"
        );
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        assert_eq!(claimed[0].lease.workflow_id, "b");
        // The retry repeats the committed claim, so it spends no second attempt.
        assert_eq!(claimed[0].recovery_attempts, 2);
        let token = claimed[0].lease.token.clone();
        assert_ne!(current.token(), token);
        assert_lost(direct.check_step(&run_with("a", &token), 1, "step").await);
        assert_eq!(
            direct
                .check_step(&run_with("b", &token), 1, "step")
                .await
                .unwrap(),
            StepDecision::Run
        );
        assert_eq!(
            direct.check_step(&current, 1, "step").await.unwrap(),
            StepDecision::Run
        );
    }
);

networked!(
    q5_a_retried_reclaim_cannot_take_back_a_replaced_workflow,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let direct = { engine(&db).await };
        let proxy = WireFault::start(&db.url).await;
        let proxied = engine_at(&proxy.url, LONG).await;
        let claiming = claim_with_lost_ack(&direct, &proxied, &proxy, &["a"]).await;
        let current = take_over(&direct, "a").await;
        proxy.open();

        assert_eq!(claiming.await.unwrap().unwrap(), []);
        assert_eq!(
            direct.check_step(&current, 1, "step").await.unwrap(),
            StepDecision::Run
        );
    }
);

// The retry may claim a released workflow again under its request token. That is safe: the lost
// reply means nobody ever held that token, so the retry's caller is its only holder.
networked!(
    q5_a_retried_reclaim_of_a_released_workflow_is_a_new_claim,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let direct = { engine(&db).await };
        let proxy = WireFault::start(&db.url).await;
        let proxied = engine_at(&proxy.url, LONG).await;
        let claiming = claim_with_lost_ack(&direct, &proxied, &proxy, &["a"]).await;
        let replaced = take_over(&direct, "a").await;
        direct.release(&replaced).await.unwrap();
        proxy.open();

        let claimed = claiming.await.unwrap().unwrap();
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        assert_ne!(claimed[0].lease.token, replaced.token());
        // Start 1, lost claim 2, takeover 3, release 2, this claim 3.
        assert_eq!(claimed[0].recovery_attempts, 3);
        assert_lost(direct.check_step(&replaced, 1, "step").await);
        assert_eq!(
            direct
                .check_step(&run_of(claimed.into_iter().next().unwrap()), 1, "step")
                .await
                .unwrap(),
            StepDecision::Run
        );
    }
);
