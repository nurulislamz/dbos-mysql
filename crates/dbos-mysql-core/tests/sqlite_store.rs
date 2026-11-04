use std::path::Path;
use std::time::Duration;

use dbos_mysql_core::{Error, Lease, NewWorkflow, Outcome, Started, Status, StepRecord, Store};
use tempfile::TempDir;

const LONG: Duration = Duration::from_secs(600);
const EXPIRED: Duration = Duration::ZERO;

async fn open(path: &Path) -> Store {
    let store = Store::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    store
}

async fn setup() -> (TempDir, Store) {
    let dir = TempDir::new().unwrap();
    let store = open(&dir.path().join("test.sqlite")).await;
    (dir, store)
}

fn workflow<'a>(id: &'a str, lease: Duration) -> NewWorkflow<'a> {
    NewWorkflow {
        workflow_id: id,
        name: "wf",
        input: "{}",
        max_recovery_attempts: 100,
        lease,
    }
}

async fn start(store: &Store, new: &NewWorkflow<'_>) -> Lease {
    match store.start_workflow(new).await.unwrap() {
        Started::Owned(lease) => lease,
        Started::Existing(existing) => panic!("expected a new workflow, got {existing:?}"),
    }
}

fn output(value: &str) -> StepRecord {
    StepRecord {
        output: Some(value.to_owned()),
        ..StepRecord::default()
    }
}

fn names() -> Vec<String> {
    vec!["wf".to_owned()]
}

#[tokio::test]
async fn new_workflow_is_owned_and_pending() {
    let (_dir, store) = setup().await;
    start(&store, &workflow("a", LONG)).await;
    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.status, Status::Pending);
    assert_eq!(state.input, "{}");
    assert_eq!(state.recovery_attempts, 1);
}

#[tokio::test]
async fn missing_workflow_is_none() {
    let (_dir, store) = setup().await;
    assert!(store.get_workflow("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn same_id_attaches_to_existing_workflow() {
    let (_dir, store) = setup().await;
    start(&store, &workflow("a", LONG)).await;
    let mut other_input = workflow("a", LONG);
    other_input.input = "{\"changed\":true}";
    let Started::Existing(existing) = store.start_workflow(&other_input).await.unwrap() else {
        panic!("expected the existing workflow");
    };
    assert_eq!(existing.input, "{}");
}

#[tokio::test]
async fn same_id_with_another_name_conflicts() {
    let (_dir, store) = setup().await;
    start(&store, &workflow("a", LONG)).await;
    let mut other = workflow("a", LONG);
    other.name = "other";
    let error = store.start_workflow(&other).await.unwrap_err();
    assert!(
        matches!(error, Error::ConflictingWorkflow { .. }),
        "{error:?}"
    );
    assert_eq!(store.get_workflow("a").await.unwrap().unwrap().name, "wf");
}

#[tokio::test]
async fn ids_are_case_sensitive() {
    let (_dir, store) = setup().await;
    start(&store, &workflow("Job-1", LONG)).await;
    start(&store, &workflow("job-1", LONG)).await;
}

#[tokio::test]
async fn saved_step_replays_its_output() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    assert_eq!(store.load_step("a", 1, "step").await.unwrap(), None);
    store
        .save_step(&lease, 1, "step", &output("\"x\""))
        .await
        .unwrap();
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("\"x\""))
    );
}

#[tokio::test]
async fn saved_step_error_replays() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let failed = StepRecord {
        error: Some("{\"type\":\"ValueError\"}".to_owned()),
        ..StepRecord::default()
    };
    store.save_step(&lease, 1, "step", &failed).await.unwrap();
    assert_eq!(store.load_step("a", 1, "step").await.unwrap(), Some(failed));
}

#[tokio::test]
async fn child_workflow_id_is_stored_with_the_step() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let child = StepRecord {
        child_workflow_id: Some("a-1".to_owned()),
        ..StepRecord::default()
    };
    store.save_step(&lease, 1, "child", &child).await.unwrap();
    assert_eq!(store.load_step("a", 1, "child").await.unwrap(), Some(child));
}

#[tokio::test]
async fn load_step_on_missing_workflow_fails() {
    let (_dir, store) = setup().await;
    let error = store.load_step("nope", 1, "step").await.unwrap_err();
    assert!(matches!(error, Error::WorkflowNotFound(_)), "{error:?}");
}

#[tokio::test]
async fn nondeterministic_workflow_step_name_is_rejected() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
    let error = store.load_step("a", 1, "renamed").await.unwrap_err();
    assert!(matches!(error, Error::UnexpectedStep { .. }), "{error:?}");
}

#[tokio::test]
async fn retried_identical_save_is_accepted() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
}

#[tokio::test]
async fn step_recorded_twice_by_the_owner_is_nondeterminism() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
    let error = store
        .save_step(&lease, 1, "step", &output("2"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::StepNondeterminism { .. }),
        "{error:?}"
    );
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("1"))
    );
}

#[tokio::test]
async fn stale_owner_cannot_save_a_step() {
    let (_dir, store) = setup().await;
    let stale = start(&store, &workflow("a", EXPIRED)).await;
    assert_eq!(store.reclaim(&names(), 10, LONG).await.unwrap().len(), 1);
    let error = store
        .save_step(&stale, 1, "step", &output("1"))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
    assert_eq!(store.load_step("a", 1, "step").await.unwrap(), None);
}

#[tokio::test]
async fn stale_owner_cannot_write_the_outcome() {
    let (_dir, store) = setup().await;
    let stale = start(&store, &workflow("a", EXPIRED)).await;
    let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);

    let late = Outcome::Success("\"stale\"".to_owned());
    let error = store.finish_workflow(&stale, &late).await.unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");

    store
        .finish_workflow(&claimed.lease, &Outcome::Success("\"owner\"".to_owned()))
        .await
        .unwrap();
    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.output.as_deref(), Some("\"owner\""));
}

#[tokio::test]
async fn finished_workflow_returns_stored_result_and_is_final() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store
        .finish_workflow(&lease, &Outcome::Success("1".to_owned()))
        .await
        .unwrap();

    let Started::Existing(existing) = store.start_workflow(&workflow("a", LONG)).await.unwrap()
    else {
        panic!("expected the finished workflow");
    };
    assert_eq!(existing.status, Status::Success);
    assert_eq!(existing.output.as_deref(), Some("1"));

    let again = store
        .finish_workflow(&lease, &Outcome::Error("late".to_owned()))
        .await;
    assert!(matches!(again, Err(Error::LeaseLost(_))), "{again:?}");
    assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
}

#[tokio::test]
async fn workflow_error_is_stored_and_not_recovered() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", EXPIRED)).await;
    store
        .finish_workflow(&lease, &Outcome::Error("boom".to_owned()))
        .await
        .unwrap();
    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.status, Status::Error);
    assert_eq!(state.error.as_deref(), Some("boom"));
    assert_eq!(state.output, None);
    assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
}

#[tokio::test]
async fn reclaim_skips_live_leases() {
    let (_dir, store) = setup().await;
    start(&store, &workflow("live", LONG)).await;
    assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
}

#[tokio::test]
async fn renewing_keeps_a_lease_alive() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", EXPIRED)).await;
    store.renew_lease(&lease, LONG).await.unwrap();
    assert!(store.reclaim(&names(), 10, LONG).await.unwrap().is_empty());
}

#[tokio::test]
async fn renewing_after_takeover_reports_lease_lost() {
    let (_dir, store) = setup().await;
    let stale = start(&store, &workflow("a", EXPIRED)).await;
    store.reclaim(&names(), 10, LONG).await.unwrap();
    let error = store.renew_lease(&stale, LONG).await.unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
}

#[tokio::test]
async fn reclaim_only_takes_registered_names() {
    let (_dir, store) = setup().await;
    start(&store, &workflow("a", EXPIRED)).await;
    assert!(
        store
            .reclaim(&["other".to_owned()], 10, LONG)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.reclaim(&[], 10, LONG).await.unwrap().is_empty());
    assert_eq!(store.reclaim(&names(), 10, LONG).await.unwrap().len(), 1);
}

#[tokio::test]
async fn reclaim_respects_its_limit() {
    let (_dir, store) = setup().await;
    for id in ["a", "b", "c"] {
        start(&store, &workflow(id, EXPIRED)).await;
    }
    assert_eq!(store.reclaim(&names(), 2, LONG).await.unwrap().len(), 2);
    assert_eq!(store.reclaim(&names(), 2, LONG).await.unwrap().len(), 1);
}

#[tokio::test]
async fn recovery_keeps_saved_steps_and_counts_the_attempt() {
    let (_dir, store) = setup().await;
    let first = start(&store, &workflow("a", EXPIRED)).await;
    store
        .save_step(&first, 1, "step", &output("1"))
        .await
        .unwrap();

    let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);
    assert_ne!(claimed.lease.token, first.token);
    assert_eq!(claimed.recovery_attempts, 2);
    assert_eq!(claimed.input, "{}");
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("1"))
    );
    store
        .save_step(&claimed.lease, 2, "next", &output("2"))
        .await
        .unwrap();
}

#[tokio::test]
async fn too_many_recoveries_end_the_workflow() {
    let (_dir, store) = setup().await;
    let mut new = workflow("a", EXPIRED);
    new.max_recovery_attempts = 1;
    start(&store, &new).await;

    let claimed = store.reclaim(&names(), 10, EXPIRED).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(
        store
            .reclaim(&names(), 10, EXPIRED)
            .await
            .unwrap()
            .is_empty()
    );

    let state = store.get_workflow("a").await.unwrap().unwrap();
    assert_eq!(state.status, Status::MaxRecoveryAttemptsExceeded);
    assert!(
        store
            .reclaim(&names(), 10, EXPIRED)
            .await
            .unwrap()
            .is_empty()
    );
    let late = store
        .save_step(&claimed[0].lease, 1, "step", &output("1"))
        .await;
    assert!(matches!(late, Err(Error::LeaseLost(_))), "{late:?}");
}

#[tokio::test]
async fn release_hands_over_without_using_an_attempt() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    store.release_lease(&lease).await.unwrap();

    let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);
    assert_eq!(claimed.recovery_attempts, 1);
    let late = store.release_lease(&lease).await;
    assert!(matches!(late, Err(Error::LeaseLost(_))), "{late:?}");
}

#[tokio::test]
async fn racing_processes_never_claim_the_same_workflow() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("race.sqlite");
    let store = open(&path).await;
    for index in 0..50 {
        let id = format!("wf-{index}");
        start(&store, &workflow(&id, EXPIRED)).await;
    }

    let mut racers = Vec::new();
    for _ in 0..8 {
        let racer = open(&path).await;
        racers.push(tokio::spawn(async move {
            let mut won = Vec::new();
            loop {
                let claimed = racer.reclaim(&names(), 3, LONG).await.unwrap();
                if claimed.is_empty() {
                    return won;
                }
                won.extend(claimed.into_iter().map(|claim| claim.lease.workflow_id));
            }
        }));
    }

    let mut all = Vec::new();
    for racer in racers {
        all.extend(racer.await.unwrap());
    }
    let total = all.len();
    all.sort();
    all.dedup();
    assert_eq!(total, 50, "a workflow was claimed twice or missed");
    assert_eq!(all.len(), 50);
}

#[tokio::test]
async fn state_survives_reopening_the_database() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("reopen.sqlite");
    {
        let store = open(&path).await;
        let lease = start(&store, &workflow("a", EXPIRED)).await;
        store
            .save_step(&lease, 1, "step", &output("1"))
            .await
            .unwrap();
    }
    let store = open(&path).await;
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("1"))
    );
    assert_eq!(store.reclaim(&names(), 10, LONG).await.unwrap().len(), 1);
}

#[tokio::test]
async fn migrate_can_run_twice() {
    let (_dir, store) = setup().await;
    store.migrate().await.unwrap();
}

#[tokio::test]
async fn large_and_unicode_values_round_trip_exactly() {
    let (_dir, store) = setup().await;
    let lease = start(&store, &workflow("a", LONG)).await;
    let large = format!("\"{}日本語🦀\"", "x".repeat(10 * 1024 * 1024));
    store
        .save_step(&lease, 1, "step", &output(&large))
        .await
        .unwrap();
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output(&large))
    );
}

#[tokio::test]
async fn mysql_reports_not_implemented_until_phase_two() {
    let url = std::env::var("DBOS_TEST_MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root:dbos-test-root@127.0.0.1:13307/dbos_test".to_owned());
    let store = Store::connect(&url).await.unwrap();
    let error = store.get_workflow("a").await.unwrap_err();
    assert!(
        matches!(error, Error::NotImplemented("get_workflow", "mysql")),
        "{error:?}"
    );
}

#[tokio::test]
#[ignore = "run by crash_keeps_committed_state_and_drops_the_rest"]
async fn crash_child() {
    let path = std::env::var("DBOS_CRASH_DB").unwrap();
    let store = open(Path::new(&path)).await;
    let lease = start(&store, &workflow("a", EXPIRED)).await;
    store
        .save_step(&lease, 1, "step", &output("1"))
        .await
        .unwrap();
    store.spike_reset().await.unwrap();
    let holder = store.clone();
    tokio::spawn(async move { holder.spike_hold_transaction(60.0).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    std::process::abort();
}

#[tokio::test]
async fn crash_keeps_committed_state_and_drops_the_rest() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("crash.sqlite");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_child", "--exact", "--ignored", "--nocapture"])
        .env("DBOS_CRASH_DB", &path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child should have aborted");

    let store = open(&path).await;
    assert_eq!(
        store.load_step("a", 1, "step").await.unwrap(),
        Some(output("1"))
    );
    assert_eq!(store.spike_rows().await.unwrap(), 0);
    let claimed = store.reclaim(&names(), 10, LONG).await.unwrap().remove(0);
    assert_eq!(claimed.recovery_attempts, 2);
    store
        .save_step(&claimed.lease, 2, "next", &output("2"))
        .await
        .unwrap();
    store
        .finish_workflow(&claimed.lease, &Outcome::Success("2".to_owned()))
        .await
        .unwrap();
}
