//! Reproduces the crash test's "started after its save" report with real Postgres restarts.
//! Only for the crash server on port 13310, which it restarts. Postgres only, so no `all_dbs!`.
//!
//! Use --release: a debug build spends about 600 ms on each new connection's SCRAM login.
//!
//! LATE_START_ITERATIONS=30 LATE_START_SEED=1 cargo test --release -p dwos-core \
//!     --test late_start_restart -- --ignored --nocapture

use std::collections::{BTreeMap, HashMap};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dwos_core::{
    Begin, Begun, DEFAULT_MAX_RECOVERY_ATTEMPTS, Ended, Engine, EngineConfig, Error, Run,
    StepDecision,
};
use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection, Row};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const SERVER: &str = "postgres://postgres:dbos-test-root@127.0.0.1:13310";
const CONTAINER: &str = "rust-dwos-chaos-postgres-1";
const LEASE: Duration = Duration::from_secs(1);
const RECOVERY: Duration = Duration::from_millis(200);
const ENGINES: usize = 4;
const SLOTS: usize = 8;
const STEPS: u32 = 3;
const STEP: &str = "late_step";
const WORKFLOW: &str = "late_workflow";
const LATE_MARGIN_MS: i64 = 50;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CLOCK_TIMEOUT: Duration = Duration::from_millis(100);
const ITERATION_DEADLINE: Duration = Duration::from_secs(180);
const SETTLE_DEADLINE: Duration = Duration::from_secs(60);
const SHM_SEGMENT_LIMIT: u32 = 40;
const NOW: &str = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT";

/// Records what the crash harness cannot see. Triggers commit with the write they describe.
const AUDIT: &str = r"
CREATE TABLE ledger (workflow_id TEXT, step_id BIGINT, exec_id BIGINT, started_at_ms BIGINT);
CREATE TABLE audit_steps (workflow_uuid TEXT, function_id BIGINT, op TEXT, output TEXT, owner TEXT);
CREATE TABLE audit_finish (workflow_uuid TEXT, old_token TEXT, status TEXT, output TEXT);
CREATE FUNCTION audit_steps_fn() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE row_data operation_outputs;
BEGIN
  IF TG_OP = 'DELETE' THEN row_data := OLD; ELSE row_data := NEW; END IF;
  INSERT INTO audit_steps SELECT row_data.workflow_uuid, row_data.function_id, TG_OP, row_data.output,
    (SELECT lease_token FROM workflow_status WHERE workflow_uuid = row_data.workflow_uuid);
  RETURN NULL;
END $$;
CREATE TRIGGER audit_steps_tr AFTER INSERT OR UPDATE OR DELETE ON operation_outputs
  FOR EACH ROW EXECUTE FUNCTION audit_steps_fn();
CREATE FUNCTION audit_finish_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO audit_finish VALUES (NEW.workflow_uuid, OLD.lease_token, NEW.status, NEW.output);
  RETURN NULL;
END $$;
CREATE TRIGGER audit_finish_tr AFTER UPDATE ON workflow_status
  FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION audit_finish_fn();
";

#[derive(Clone, Debug, Default)]
struct Exec {
    id: usize,
    workflow: String,
    step: i64,
    token: String,
    engine: usize,
    sync: bool,
    owned_at: f64,
    db_before: Option<i64>,
    check_begin: f64,
    check_end: f64,
    db_after: Option<i64>,
    first_attempt: Option<f64>,
    write_ok: Option<f64>,
    retries: u32,
    connect_ms: f64,
    first_error: Option<String>,
    save_begin: Option<f64>,
    save_end: Option<f64>,
    save_ok: Option<bool>,
}

type Slot = Arc<Mutex<Exec>>;

struct Ctx {
    url: String,
    write_retry: Duration,
    t0: Instant,
    clock: PgPool,
    execs: Mutex<Vec<Slot>>,
    errors: Mutex<BTreeMap<String, u64>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    lost_async: Mutex<u64>,
    claims: Mutex<Vec<f64>>,
}

impl Ctx {
    fn now(&self) -> f64 {
        self.t0.elapsed().as_secs_f64() * 1000.0
    }

    fn error(&self, place: &str, error: &Error) {
        let kind = match error {
            Error::LeaseLost(_) => "lease lost".to_owned(),
            Error::Database(sqlx::Error::Database(database)) => {
                format!("database {:?}", database.code())
            }
            other => {
                let text = other.to_string();
                text.split(':').take(2).collect::<Vec<_>>().join(":")
            }
        };
        *self
            .errors
            .lock()
            .unwrap()
            .entry(format!("{place}: {kind}"))
            .or_default() += 1;
    }

    fn track(&self, task: JoinHandle<()>) {
        self.tasks.lock().unwrap().push(task);
    }

    async fn db_clock(&self) -> Option<i64> {
        let read =
            sqlx::query_scalar(AssertSqlSafe(format!("SELECT {NOW}"))).fetch_one(&self.clock);
        tokio::time::timeout(CLOCK_TIMEOUT, read).await.ok()?.ok()
    }
}

fn is_sync(workflow: &str) -> bool {
    workflow.bytes().last().is_some_and(|byte| byte % 2 == 1)
}

fn jitter() -> Duration {
    Duration::from_millis(u64::from(uuid::Uuid::new_v4().as_bytes()[0]) * 30 / 255)
}

/// The same side effect as `_write_ledger`: a new connection each try, and a sleep after failure.
async fn write_ledger(ctx: Arc<Ctx>, slot: Slot) {
    let (workflow, step, id) = {
        let mut exec = slot.lock().unwrap();
        exec.first_attempt = Some(ctx.now());
        (exec.workflow.clone(), exec.step, exec.id)
    };
    let sql = format!(
        "INSERT INTO ledger (workflow_id, step_id, exec_id, started_at_ms) VALUES ($1, $2, $3, {NOW})"
    );
    loop {
        let begun = ctx.now();
        let result = async {
            let mut conn = tokio::time::timeout(CONNECT_TIMEOUT, PgConnection::connect(&ctx.url))
                .await
                .map_err(|_| "connect timed out".to_owned())?
                .map_err(|error| error.to_string())?;
            sqlx::query(AssertSqlSafe(sql.clone()))
                .bind(workflow.as_str())
                .bind(step)
                .bind(i64::try_from(id).unwrap())
                .execute(&mut conn)
                .await
                .map_err(|error| error.to_string())?;
            let _ = conn.close().await;
            Ok::<_, String>(())
        }
        .await;
        match result {
            Ok(()) => {
                let mut exec = slot.lock().unwrap();
                exec.write_ok = Some(ctx.now());
                exec.connect_ms = ctx.now() - begun;
                return;
            }
            Err(error) => {
                {
                    let mut exec = slot.lock().unwrap();
                    exec.retries += 1;
                    exec.first_error.get_or_insert(error);
                }
                tokio::time::sleep(ctx.write_retry).await;
            }
        }
    }
}

async fn run_steps(
    ctx: &Arc<Ctx>,
    engine_index: usize,
    engine: &Engine,
    run: &Run,
    owned_at: f64,
) -> Result<String, Error> {
    let workflow = run.workflow_id().to_owned();
    let mut values = vec![run.token().to_owned()];
    for _ in 0..STEPS {
        let step = run.next_step_id();
        let db_before = ctx.db_clock().await;
        let check_begin = ctx.now();
        let decision = engine.check_step(run, step, STEP).await?;
        let check_end = ctx.now();
        let value = match decision {
            StepDecision::ReplayOutput(value) => value,
            StepDecision::ReplayError(error) => panic!("no step stores an error: {error}"),
            StepDecision::Run => {
                let slot = {
                    let mut execs = ctx.execs.lock().unwrap();
                    let slot = Arc::new(Mutex::new(Exec {
                        id: execs.len(),
                        workflow: workflow.clone(),
                        step: i64::from(step),
                        token: run.token().to_owned(),
                        engine: engine_index,
                        sync: is_sync(&workflow),
                        owned_at,
                        db_before,
                        check_begin,
                        check_end,
                        ..Exec::default()
                    }));
                    execs.push(slot.clone());
                    slot
                };
                // Read beside the write, so the read does not delay the side effect.
                let (read_ctx, read_slot) = (ctx.clone(), slot.clone());
                ctx.track(tokio::spawn(async move {
                    let after = read_ctx.db_clock().await;
                    read_slot.lock().unwrap().db_after = after;
                }));
                // Spawned: losing the lease drops this body, but like the Python thread the write goes on.
                let write = tokio::spawn(write_ledger(ctx.clone(), slot.clone()));
                write.await.unwrap();
                tokio::time::sleep(jitter()).await;
                let value = format!("{workflow}:{step}:{}", slot.lock().unwrap().id);
                slot.lock().unwrap().save_begin = Some(ctx.now());
                let saved = engine
                    .save_step_output(run, step, STEP, value.clone())
                    .await;
                {
                    let mut exec = slot.lock().unwrap();
                    exec.save_end = Some(ctx.now());
                    exec.save_ok = Some(saved.is_ok());
                }
                saved?;
                value
            }
        };
        values.push(value);
    }
    Ok(values.join("|"))
}

async fn conclude(ctx: &Ctx, engine: &Engine, run: &Run, body: Result<String, Error>) {
    let ended = match body {
        Ok(output) => Ended::Success(output),
        Err(error) => {
            ctx.error("body", &error);
            Ended::Error(error.to_string())
        }
    };
    if let Err(error) = engine.conclude(run, ended).await {
        ctx.error("conclude", &error);
    }
}

/// Follows the SDK's `_run_sync` and `_run_async`: sync bodies ignore loss until the next store call.
async fn execute(ctx: Arc<Ctx>, engine_index: usize, engine: Engine, run: Run, owned_at: f64) {
    let active = engine.keep_lease(&run);
    if is_sync(run.workflow_id()) {
        let body = run_steps(&ctx, engine_index, &engine, &run, owned_at).await;
        active.stop();
        conclude(&ctx, &engine, &run, body).await;
        return;
    }
    let outcome = tokio::select! {
        body = run_steps(&ctx, engine_index, &engine, &run, owned_at) => Some(body),
        _ = active.lost() => None,
    };
    active.stop();
    match outcome {
        Some(body) => conclude(&ctx, &engine, &run, body).await,
        None => {
            *ctx.lost_async.lock().unwrap() += 1;
            if let Err(error) = engine.await_result(run.workflow_id()).await {
                ctx.error("await after loss", &error);
            }
        }
    }
}

/// Like `_run_one`: any failure waits and asks again, and an attached caller waits for the result.
async fn run_one(ctx: Arc<Ctx>, engine_index: usize, engine: Engine, id: String) {
    loop {
        let begun = engine
            .begin(Begin {
                name: WORKFLOW,
                input: "{}",
                max_recovery_attempts: DEFAULT_MAX_RECOVERY_ATTEMPTS,
                workflow_id: Some(&id),
                parent: None,
                timeout_ms: None,
                ignore_propagated_deadline: false,
            })
            .await;
        match begun {
            Ok(Begun::Run(run)) => {
                let owned_at = ctx.now();
                execute(ctx.clone(), engine_index, engine.clone(), run, owned_at).await;
                // Asks again at once, which reads the stored result instead of holding this slot.
                continue;
            }
            Ok(Begun::Stored(_)) => return,
            Ok(Begun::Attach(_)) => match engine.await_result(&id).await {
                Ok(_) => return,
                Err(error) => ctx.error("await", &error),
            },
            Err(error) => ctx.error("begin", &error),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn recovery_loop(ctx: Arc<Ctx>, engine_index: usize, engine: Engine, stop: Arc<AtomicBool>) {
    let names = vec![WORKFLOW.to_owned()];
    while !stop.load(Ordering::SeqCst) {
        for claim in engine.recovery_tick(&names).await {
            let owned_at = ctx.now();
            ctx.claims.lock().unwrap().push(owned_at);
            let run = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);
            ctx.track(tokio::spawn(execute(
                ctx.clone(),
                engine_index,
                engine.clone(),
                run,
                owned_at,
            )));
        }
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut mixed = *state;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

async fn admin(database: &str) -> PgConnection {
    let url = format!("{SERVER}/{database}");
    loop {
        match PgConnection::connect(&url).await {
            Ok(conn) => return conn,
            Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
}

async fn postmaster_start(database: &str) -> String {
    let mut conn = admin(database).await;
    loop {
        let started = sqlx::query_scalar("SELECT pg_postmaster_start_time()::TEXT")
            .fetch_one(&mut conn)
            .await;
        match started {
            Ok(started) => return started,
            Err(_) => conn = admin(database).await,
        }
    }
}

/// `docker restart` kills Postgres uncleanly, so each one leaves segments in the container's 64 MB
/// /dev/shm until the server crash-loops. A full stop and start clears them; it runs between iterations.
fn clear_leaked_shared_memory() -> bool {
    let listing = Command::new("docker")
        .args(["exec", CONTAINER, "sh", "-c", "ls /dev/shm | wc -l"])
        .output()
        .expect("docker runs");
    let segments: u32 = String::from_utf8_lossy(&listing.stdout)
        .trim()
        .parse()
        .unwrap_or(u32::MAX);
    if segments < SHM_SEGMENT_LIMIT {
        return false;
    }
    for action in ["stop", "start"] {
        let done = Command::new("docker")
            .args([action, CONTAINER])
            .output()
            .expect("docker runs");
        assert!(done.status.success(), "docker {action} failed: {done:?}");
    }
    true
}

async fn drop_old_databases() {
    let mut root = admin("postgres").await;
    let names: Vec<String> =
        sqlx::query_scalar("SELECT datname FROM pg_database WHERE datname LIKE 'late\\_%'")
            .fetch_all(&mut root)
            .await
            .unwrap();
    for name in names {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
        )))
        .execute(&mut root)
        .await
        .unwrap();
    }
}

fn restart_container() {
    let status = Command::new("docker")
        .args(["restart", CONTAINER])
        .output()
        .expect("docker runs");
    assert!(status.status.success(), "docker restart failed: {status:?}");
}

async fn wait_for_clock_offset(database: &str, t0: Instant) -> f64 {
    let mut conn = admin(database).await;
    let mut best = (f64::MAX, 0.0);
    for _ in 0..20 {
        let before = t0.elapsed().as_secs_f64() * 1000.0;
        let stamp: f64 =
            sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::FLOAT8")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        let after = t0.elapsed().as_secs_f64() * 1000.0;
        if after - before < best.0 {
            best = (after - before, stamp - (before + after) / 2.0);
        }
    }
    best.1
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Class {
    /// (a) `Run` came for a step whose save or replacing claim was already visible when the check began.
    CoreRunAfterSave,
    /// (a?) The check began before the new owner's claim but returned after the save was acknowledged.
    AckDelayedOverlap,
    /// (b) The write began before the save, failed and retried. The crash test's stamp shows the retry.
    StampRetry,
    /// (b) `Run` came before the save, but the first write attempt began after it. The documented gap.
    InvocationGap,
    /// (b) The write began before the save and had no retry, so one slow attempt made the stamp late.
    StampSlow,
}

#[derive(Default)]
struct Report {
    workflows: usize,
    execs: usize,
    repeated: usize,
    late_any: usize,
    /// Largest stamp minus save time on a repeated step. Above +50 is what the crash test reports.
    closest_ms: Option<i64>,
    /// Stale writes that landed after the step's new owner already held the workflow.
    landed_after_takeover: usize,
    classes: BTreeMap<Class, usize>,
    late_by_mode: [usize; 2],
    write_retries: usize,
    no_db_clock: usize,
    /// Mean milliseconds in check, write and save, to compare the shape with the crash test.
    phases: [f64; 3],
    violations: Vec<String>,
    lines: Vec<String>,
}

fn classify(late: &Exec, saver: &Exec, saved_at: f64, completed_ms: i64) -> Class {
    let check_after_replaced = late.token != saver.token && late.check_begin >= saver.owned_at;
    let visible_by_clock = late.db_before.is_some_and(|before| before > completed_ms);
    if check_after_replaced || visible_by_clock || late.check_begin >= saved_at {
        return Class::CoreRunAfterSave;
    }
    if late.check_end >= saved_at {
        return Class::AckDelayedOverlap;
    }
    if late.first_attempt.is_some_and(|first| first >= saved_at) {
        return Class::InvocationGap;
    }
    if late.retries > 0 {
        Class::StampRetry
    } else {
        Class::StampSlow
    }
}

fn value_exec(value: &str) -> Option<usize> {
    value.rsplit(':').next()?.parse().ok()
}

fn phase_means(execs: &[Exec]) -> [f64; 3] {
    let mean = |pick: &dyn Fn(&Exec) -> Option<f64>| {
        let values: Vec<f64> = execs.iter().filter_map(pick).collect();
        values.iter().sum::<f64>() / values.len().max(1) as f64
    };
    [
        mean(&|exec| Some(exec.check_end - exec.check_begin)),
        mean(&|exec| Some(exec.write_ok? - exec.first_attempt?)),
        mean(&|exec| Some(exec.save_end? - exec.save_begin?)),
    ]
}

async fn analyse(database: &str, ctx: &Ctx, offset: f64, expected: usize) -> Report {
    let mut conn = admin(database).await;
    let execs: Vec<Exec> = ctx
        .execs
        .lock()
        .unwrap()
        .iter()
        .map(|slot| slot.lock().unwrap().clone())
        .collect();
    let mut report = Report {
        workflows: expected,
        execs: execs.len(),
        write_retries: execs.iter().filter(|exec| exec.retries > 0).count(),
        no_db_clock: execs
            .iter()
            .filter(|exec| exec.db_before.is_none() || exec.db_after.is_none())
            .count(),
        phases: phase_means(&execs),
        ..Report::default()
    };
    let by_id: HashMap<usize, &Exec> = execs.iter().map(|exec| (exec.id, exec)).collect();

    let saved: HashMap<(String, i64), (String, i64)> = sqlx::query(
        "SELECT workflow_uuid, function_id, output, completed_at_epoch_ms FROM operation_outputs",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap()
    .iter()
    .map(|row| {
        (
            (row.get("workflow_uuid"), row.get("function_id")),
            (row.get("output"), row.get("completed_at_epoch_ms")),
        )
    })
    .collect();

    let statuses = sqlx::query("SELECT workflow_uuid, status, output, error FROM workflow_status")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    if statuses.len() != expected {
        report.violations.push(format!(
            "expected {expected} workflows, found {}",
            statuses.len()
        ));
    }
    let finishes = sqlx::query("SELECT workflow_uuid, old_token, status, output FROM audit_finish")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let mut finish_count: HashMap<String, usize> = HashMap::new();
    let mut finished_by: HashMap<String, Option<String>> = HashMap::new();
    for row in &finishes {
        let workflow: String = row.get("workflow_uuid");
        *finish_count.entry(workflow.clone()).or_default() += 1;
        finished_by.insert(workflow, row.get("old_token"));
    }
    for row in &statuses {
        let workflow: String = row.get("workflow_uuid");
        let status: String = row.get("status");
        if status != "SUCCESS" {
            let error: Option<String> = row.get("error");
            report
                .violations
                .push(format!("{workflow} ended as {status}: {error:?}"));
            continue;
        }
        if finish_count.get(&workflow) != Some(&1) {
            report.violations.push(format!(
                "(c) {workflow} changed final status {:?} times",
                finish_count.get(&workflow)
            ));
        }
        let output: String = row.get("output");
        let mut parts = output.split('|');
        let result_token = parts.next().unwrap_or_default().to_owned();
        let used: Vec<&str> = parts.collect();
        let expected_values: Vec<&str> = (1..=i64::from(STEPS))
            .map(|step| {
                saved
                    .get(&(workflow.clone(), step))
                    .map_or("<missing>", |(value, _)| value.as_str())
            })
            .collect();
        if used != expected_values {
            report.violations.push(format!(
                "{workflow} used {used:?}, but its saved steps are {expected_values:?}"
            ));
        }
        if finished_by
            .get(&workflow)
            .and_then(|token| token.as_deref())
            != Some(result_token.as_str())
        {
            report.violations.push(format!(
                "(c) {workflow} finished by {result_token} while the owner was {:?}",
                finished_by.get(&workflow)
            ));
        }
    }

    // Every saved step, late or not: who wrote it, whether that writer owned it, and whether it changed.
    let audit =
        sqlx::query("SELECT workflow_uuid, function_id, op, output, owner FROM audit_steps")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    for row in &audit {
        let workflow: String = row.get("workflow_uuid");
        let step: i64 = row.get("function_id");
        let op: String = row.get("op");
        let output: String = row.get("output");
        let owner: Option<String> = row.get("owner");
        if op != "INSERT" {
            report
                .violations
                .push(format!("(c) {workflow} step {step} had {op} to {output}"));
        }
        let writer = value_exec(&output).and_then(|id| by_id.get(&id));
        match writer {
            Some(writer) if owner.as_deref() == Some(writer.token.as_str()) => {}
            _ => report.violations.push(format!(
                "(c) {workflow} step {step} saved by exec {:?} while owner was {owner:?}",
                writer.map(|exec| &exec.token)
            )),
        }
    }

    // Applies to every execution: a fenced save must lose, and an acknowledged save must remain.
    let mut repeated: HashMap<(String, i64), usize> = HashMap::new();
    for exec in &execs {
        let key = (exec.workflow.clone(), exec.step);
        *repeated.entry(key.clone()).or_default() += 1;
        let Some((value, completed_ms)) = saved.get(&key) else {
            report
                .violations
                .push(format!("{} step {} has no saved row", key.0, key.1));
            continue;
        };
        let Some(saver) = value_exec(value).and_then(|id| by_id.get(&id)) else {
            report
                .violations
                .push(format!("{} step {} saved {value}", key.0, key.1));
            continue;
        };
        if exec.save_ok == Some(true) && exec.id != saver.id {
            report.violations.push(format!(
                "(c) exec {} save was acknowledged, but {} step {} holds {value}",
                exec.id, key.0, key.1
            ));
        }
        if exec.id != saver.id {
            let saved_at = saver
                .save_end
                .unwrap_or(f64::MAX)
                .min(*completed_ms as f64 - offset + 5.0);
            let class = classify(exec, saver, saved_at, *completed_ms);
            if class == Class::CoreRunAfterSave || class == Class::AckDelayedOverlap {
                report.classes.entry(class).or_default();
                report.violations.push(format!(
                    "{class:?}: exec {exec:?} ran beside saver {saver:?}, completed_ms {completed_ms}"
                ));
            }
        }
    }
    report.repeated = repeated.values().filter(|count| **count > 1).count();

    // The crash harness's own rule, on the same database-clock stamps.
    let ledger = sqlx::query("SELECT workflow_id, step_id, exec_id, started_at_ms FROM ledger")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    for row in &ledger {
        let key: (String, i64) = (row.get("workflow_id"), row.get("step_id"));
        let exec_id: i64 = row.get("exec_id");
        let stamp: i64 = row.get("started_at_ms");
        let Some((value, completed_ms)) = saved.get(&key) else {
            continue;
        };
        if repeated.get(&key).is_some_and(|count| *count > 1) {
            report.closest_ms = report.closest_ms.max(Some(stamp - completed_ms));
            let writer = by_id[&usize::try_from(exec_id).unwrap()];
            let winner = value_exec(value).and_then(|id| by_id.get(&id));
            if winner.is_some_and(|winner| {
                winner.token != writer.token
                    && writer.write_ok.is_some_and(|ok| ok > winner.owned_at)
            }) {
                report.landed_after_takeover += 1;
            }
        }
        if stamp <= *completed_ms {
            continue;
        }
        report.late_any += 1;
        if stamp <= completed_ms + LATE_MARGIN_MS {
            continue;
        }
        let exec = by_id[&usize::try_from(exec_id).unwrap()];
        let Some(saver) = value_exec(value).and_then(|id| by_id.get(&id)) else {
            continue;
        };
        let saved_at = saver
            .save_end
            .unwrap_or(f64::MAX)
            .min(*completed_ms as f64 - offset + 5.0);
        let class = classify(exec, saver, saved_at, *completed_ms);
        *report.classes.entry(class).or_default() += 1;
        report.late_by_mode[usize::from(exec.sync)] += 1;
        report.lines.push(format!(
            "  LATE {} step {} by +{}ms class={class:?} sync={} | late owner engine={} token={} check=[{:.0},{:.0}] first_write={:.0} write_ok={:.0} retries={} last_try_ms={:.0} err={:?} | saver engine={} token={} owned={:.0} save_ack={:.0} | db_before={:?} db_after={:?} completed={completed_ms} stamp={stamp} clock_offset={offset:.0}",
            key.0,
            key.1,
            stamp - completed_ms,
            exec.sync,
            exec.engine,
            &exec.token[..8.min(exec.token.len())],
            exec.check_begin,
            exec.check_end,
            exec.first_attempt.unwrap_or(-1.0),
            exec.write_ok.unwrap_or(-1.0),
            exec.retries,
            exec.connect_ms,
            exec.first_error.as_deref().map(|text| &text[..60.min(text.len())]),
            saver.engine,
            &saver.token[..8.min(saver.token.len())],
            saver.owned_at,
            saver.save_end.unwrap_or(-1.0),
            exec.db_before,
            exec.db_after,
        ));
    }
    report
}

struct Iteration {
    delay_ms: u64,
    restart_begin: f64,
    restart_end: f64,
    db_up: f64,
    finished: f64,
    claim_lag: f64,
    report: Report,
    errors: BTreeMap<String, u64>,
    lost_async: u64,
}

async fn iteration(seed: u64, workflows: usize, max_delay_ms: u64) -> Iteration {
    let mut random = seed;
    let delay_ms = next_random(&mut random) % max_delay_ms;
    let database = format!("late_{}", uuid::Uuid::new_v4().simple());
    let mut root = admin("postgres").await;
    sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE {database}")))
        .execute(&mut root)
        .await
        .unwrap();
    let url = format!("{SERVER}/{database}");
    let config = EngineConfig {
        lease: LEASE,
        recovery_interval: RECOVERY,
        pickle: false,
        ..EngineConfig::default()
    };
    let mut engines = vec![Engine::connect(&url, config.clone()).await.unwrap()];
    sqlx::raw_sql(AUDIT)
        .execute(engines[0].store().pool())
        .await
        .unwrap();
    for _ in 1..ENGINES {
        engines.push(Engine::connect(&url, config.clone()).await.unwrap());
    }
    let clock = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(CLOCK_TIMEOUT)
        .connect_lazy(&url)
        .unwrap();
    let t0 = Instant::now();
    let offset = wait_for_clock_offset(&database, t0).await;
    let start_time = postmaster_start("postgres").await;
    let ctx = Arc::new(Ctx {
        url: url.clone(),
        write_retry: Duration::from_millis(env_number("LATE_START_WRITE_RETRY_MS", 200)),
        t0,
        clock,
        execs: Mutex::new(Vec::new()),
        errors: Mutex::new(BTreeMap::new()),
        tasks: Mutex::new(Vec::new()),
        lost_async: Mutex::new(0),
        claims: Mutex::new(Vec::new()),
    });

    let stop = Arc::new(AtomicBool::new(false));
    let recoveries: Vec<_> = engines
        .iter()
        .enumerate()
        .map(|(index, engine)| {
            tokio::spawn(recovery_loop(
                ctx.clone(),
                index,
                engine.clone(),
                stop.clone(),
            ))
        })
        .collect();

    let mut workers = Vec::new();
    for (index, engine) in engines.iter().enumerate() {
        let slots = Arc::new(Semaphore::new(SLOTS));
        for number in (0..workflows).filter(|number| number % ENGINES == index) {
            let (ctx, engine, slots) = (ctx.clone(), engine.clone(), slots.clone());
            workers.push(tokio::spawn(async move {
                let _slot = slots.acquire_owned().await.unwrap();
                run_one(ctx, index, engine, format!("wf-{number}")).await;
            }));
        }
    }

    let restart_ctx = ctx.clone();
    let restart = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let begin = restart_ctx.now();
        tokio::task::spawn_blocking(restart_container)
            .await
            .unwrap();
        let end = restart_ctx.now();
        let _ = admin("postgres").await;
        (begin, end, restart_ctx.now())
    });

    let finished = tokio::time::timeout(ITERATION_DEADLINE, async {
        for worker in &mut workers {
            let _ = worker.await;
        }
    })
    .await;
    for worker in &workers {
        worker.abort();
    }
    let (restart_begin, restart_end, db_up) = restart.await.unwrap();
    let finished_at = ctx.now();
    let new_start_time = postmaster_start("postgres").await;
    assert_ne!(start_time, new_start_time, "the server did not restart");

    stop.store(true, Ordering::SeqCst);
    for recovery in recoveries {
        recovery.abort();
    }
    let settle_by = tokio::time::Instant::now() + SETTLE_DEADLINE;
    let mut leftover = false;
    loop {
        let Some(task) = ctx.tasks.lock().unwrap().pop() else {
            break;
        };
        let abort = task.abort_handle();
        if tokio::time::timeout_at(settle_by, task).await.is_err() {
            leftover = true;
            abort.abort();
        }
    }

    let mut report = analyse(&database, &ctx, offset, workflows).await;
    let claim_lag = ctx
        .claims
        .lock()
        .unwrap()
        .iter()
        .filter(|claimed| **claimed > restart_begin)
        .fold(f64::MAX, |first, claimed| first.min(*claimed))
        - db_up;
    if finished.is_err() {
        report.violations.push(format!(
            "workflows did not finish in {ITERATION_DEADLINE:?}"
        ));
    }
    if leftover {
        report
            .violations
            .push("an owner task never ended".to_owned());
    }
    for engine in &engines {
        engine.store().pool().close().await;
    }
    ctx.clock.close().await;
    let mut root = admin("postgres").await;
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {database} WITH (FORCE)"
    )))
    .execute(&mut root)
    .await
    .unwrap();

    let errors = ctx.errors.lock().unwrap().clone();
    let lost_async = *ctx.lost_async.lock().unwrap();
    Iteration {
        delay_ms,
        restart_begin,
        restart_end,
        db_up,
        finished: finished_at,
        claim_lag,
        report,
        errors,
        lost_async,
    }
}

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "restarts the crash Postgres container"]
async fn late_starts_are_classified_across_real_restarts() {
    let iterations = env_number("LATE_START_ITERATIONS", 30);
    let seed = env_number("LATE_START_SEED", 1);
    let workflows = usize::try_from(env_number("LATE_START_WORKFLOWS", 400)).unwrap();
    let max_delay_ms = env_number("LATE_START_MAX_DELAY_MS", 1500);
    let retry = env_number("LATE_START_WRITE_RETRY_MS", 200);
    println!(
        "seed={seed} iterations={iterations} workflows={workflows} max_delay_ms={max_delay_ms} write_retry_ms={retry}"
    );

    let mut totals: BTreeMap<Class, usize> = BTreeMap::new();
    let mut violations = Vec::new();
    let (mut late, mut late_any, mut retries, mut repeated, mut execs) = (0, 0, 0, 0, 0);
    let mut closest = None;
    let mut stale_after_takeover = 0;
    let mut claim_lags = Vec::new();
    let mut modes = [0, 0];
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    let mut hit = 0;
    let mut shm_clears = 0;
    drop_old_databases().await;
    for index in 0..iterations {
        if tokio::task::spawn_blocking(clear_leaked_shared_memory)
            .await
            .unwrap()
        {
            shm_clears += 1;
        }
        let run = iteration(
            seed.wrapping_mul(1_000_003).wrapping_add(index),
            workflows,
            max_delay_ms,
        )
        .await;
        let report = run.report;
        println!(
            "iter={index} delay={} restart=[{:.0},{:.0}] db_up={:.0} claim_lag={:.0} finished={:.0} wf={} execs={} repeated={} write_retries={} no_db_clock={} phases={:.0?} lost_async={} closest={:?} stale_after_takeover={} late_any={} late50={:?} modes(async,sync)={:?} violations={}",
            run.delay_ms,
            run.restart_begin,
            run.restart_end,
            run.db_up,
            run.claim_lag,
            run.finished,
            report.workflows,
            report.execs,
            report.repeated,
            report.write_retries,
            report.no_db_clock,
            report.phases,
            run.lost_async,
            report.closest_ms,
            report.landed_after_takeover,
            report.late_any,
            report.classes,
            report.late_by_mode,
            report.violations.len(),
        );
        for line in &report.lines {
            println!("{line}");
        }
        for violation in &report.violations {
            println!("  VIOLATION {violation}");
        }
        hit += usize::from(report.write_retries > 0);
        closest = closest.max(report.closest_ms);
        stale_after_takeover += report.landed_after_takeover;
        claim_lags.push(run.claim_lag.round());
        late += report.lines.len();
        late_any += report.late_any;
        retries += report.write_retries;
        repeated += report.repeated;
        execs += report.execs;
        modes[0] += report.late_by_mode[0];
        modes[1] += report.late_by_mode[1];
        for (class, count) in report.classes {
            *totals.entry(class).or_default() += count;
        }
        for (key, count) in run.errors {
            *errors.entry(key).or_default() += count;
        }
        violations.extend(report.violations);
    }
    println!(
        "TOTAL iterations={iterations} shm_clears={shm_clears} restarts_with_blocked_writes={hit} execs={execs} repeated_steps={repeated} writes_retried={retries} closest={closest:?} stale_after_takeover={stale_after_takeover} claim_lags={claim_lags:?} late_any={late_any} late50={late} by_class={totals:?} modes(async,sync)={modes:?} errors={errors:#?}"
    );
    assert!(violations.is_empty(), "{violations:#?}");
}
