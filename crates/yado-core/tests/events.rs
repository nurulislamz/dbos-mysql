use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use yado_core::{
    Begin, Begun, Engine, EngineConfig, Error, FinalOutcome, Finished, Lease, Outcome,
    PORTABLE_JSON, Poll, Run, SendOutcome, StoredValue,
};

#[macro_use]
mod common;
use common::*;

fn config() -> EngineConfig {
    EngineConfig {
        lease: LONG,
        recovery_interval: Duration::from_millis(20),
        pickle: false,
        ..EngineConfig::default()
    }
}

async fn engine(db: &TestDb) -> Engine {
    Engine::connect(&db.url, config()).await.unwrap()
}

async fn begin_run(engine: &Engine, id: &str) -> Run {
    match engine
        .begin(Begin {
            name: "wf",
            input: "{}",
            max_recovery_attempts: 100,
            workflow_id: Some(id),
            parent: None,
            timeout_ms: None,
            ignore_propagated_deadline: false,
        })
        .await
        .unwrap()
    {
        Begun::Run(run) => run,
        other => panic!("expected to run, got {other:?}"),
    }
}

fn json(text: &str) -> StoredValue {
    StoredValue {
        text: text.to_owned(),
        serialization: PORTABLE_JSON.to_owned(),
    }
}

all_dbs!(send_to_a_missing_workflow_fails, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let error = engine
        .send("missing", None, "\"hi\"", PORTABLE_JSON, None, None, None)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::SendDestinationNotFound(id) if id == "missing"),
        "{error:?}"
    );
});

all_dbs!(send_from_outside_a_workflow_inserts_a_message, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    begin_run(&engine, "dest").await;
    engine
        .send(
            "dest",
            Some("t"),
            "\"hello\"",
            PORTABLE_JSON,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let messages = engine.store().list_messages("dest").await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].topic, "t");
    assert_eq!(messages[0].message, json("\"hello\""));
    assert!(!messages[0].consumed);
});

all_dbs!(
    send_inside_a_workflow_is_recorded_and_not_sent_twice,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        begin_run(&engine, "dest").await;
        let sender = begin_run(&engine, "sender").await;
        assert_eq!(
            engine
                .send(
                    "dest",
                    None,
                    "\"once\"",
                    PORTABLE_JSON,
                    None,
                    Some(&sender),
                    Some(1),
                )
                .await
                .unwrap(),
            SendOutcome::Sent
        );
        assert_eq!(
            engine
                .send(
                    "dest",
                    None,
                    "\"once\"",
                    PORTABLE_JSON,
                    None,
                    Some(&sender),
                    Some(1),
                )
                .await
                .unwrap(),
            SendOutcome::Replayed
        );
        let messages = engine.store().list_messages("dest").await.unwrap();
        assert_eq!(messages.len(), 1);
        let step = engine
            .store()
            .load_step("sender", 1, "DBOS.send")
            .await
            .unwrap();
        assert!(step.is_some());
    }
);

all_dbs!(an_idempotency_key_delivers_a_message_once, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    begin_run(&engine, "dest").await;
    engine
        .send(
            "dest",
            None,
            "\"a\"",
            PORTABLE_JSON,
            Some("key-1"),
            None,
            None,
        )
        .await
        .unwrap();
    engine
        .send(
            "dest",
            None,
            "\"b\"",
            PORTABLE_JSON,
            Some("key-1"),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(engine.store().list_messages("dest").await.unwrap().len(), 1);
});

all_dbs!(
    idempotency_keys_are_scoped_to_a_destination_without_collisions,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        begin_run(&engine, "dest").await;
        begin_run(&engine, "b::dest").await;
        for (destination, key) in [("dest", "a::b"), ("b::dest", "a"), ("dest", "a")] {
            engine
                .send(destination, None, "1", PORTABLE_JSON, Some(key), None, None)
                .await
                .unwrap();
        }
        assert_eq!(engine.store().list_messages("dest").await.unwrap().len(), 2);
        assert_eq!(
            engine.store().list_messages("b::dest").await.unwrap().len(),
            1
        );
    }
);

all_dbs!(
    oversized_idempotency_keys_are_rejected_before_inserting,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        begin_run(&engine, "dest").await;
        let error = engine
            .send(
                "dest",
                None,
                "1",
                PORTABLE_JSON,
                Some(&"a".repeat(256)),
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::NameTooLong {
                field: "idempotency key"
            }
        ));
        assert!(
            engine
                .store()
                .list_messages("dest")
                .await
                .unwrap()
                .is_empty()
        );
    }
);

all_dbs!(
    messages_with_equal_timestamps_are_consumed_in_send_order,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let recv = begin_run(&engine, "dest").await;
        for key in ["z", "a", "m"] {
            engine
                .send("dest", None, key, PORTABLE_JSON, Some(key), None, None)
                .await
                .unwrap();
        }
        sqlx::query(sqlx::AssertSqlSafe(
            "UPDATE notifications SET created_at_epoch_ms = 0",
        ))
        .execute(engine.store().pool())
        .await
        .unwrap();
        for (i, value) in ["z", "a", "m"].into_iter().enumerate() {
            let step = (i as u32) * 2 + 1;
            assert_eq!(
                engine
                    .poll_recv(&recv, step, step + 1, None, 0)
                    .await
                    .unwrap(),
                Poll::Ready(json(value))
            );
        }
    }
);

all_dbs!(wait_and_timeout_step_ids_must_differ, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let recv = begin_run(&engine, "dest").await;
    assert!(matches!(
        engine.poll_recv(&recv, 1, 1, None, 0).await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        engine
            .poll_event("dest", "key", 0, None, Some((&recv, 1, 1)))
            .await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(engine.list_steps("dest").await.unwrap().is_empty());
});

all_dbs!(
    recv_consumes_a_message_and_records_the_step_together,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let recv = begin_run(&engine, "dest").await;
        engine
            .send("dest", None, "\"one\"", PORTABLE_JSON, None, None, None)
            .await
            .unwrap();
        engine
            .send("dest", None, "\"two\"", PORTABLE_JSON, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            engine.poll_recv(&recv, 1, 2, None, 0).await.unwrap(),
            Poll::Ready(json("\"one\""))
        );
        let messages = engine.store().list_messages("dest").await.unwrap();
        assert!(messages[0].consumed);
        assert_eq!(messages[0].consumed_by_function_id, Some(1));
        assert!(!messages[1].consumed);
        assert_eq!(
            engine.poll_recv(&recv, 1, 2, None, 0).await.unwrap(),
            Poll::Ready(json("\"one\""))
        );
        let messages = engine.store().list_messages("dest").await.unwrap();
        assert!(!messages[1].consumed);
    }
);

all_dbs!(
    a_timeout_recv_records_null_and_does_not_consume_a_later_message,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let recv = begin_run(&engine, "dest").await;
        assert_eq!(
            engine.poll_recv(&recv, 1, 2, None, 0).await.unwrap(),
            Poll::Ready(json("null"))
        );
        engine
            .send("dest", None, "\"late\"", PORTABLE_JSON, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            engine.poll_recv(&recv, 1, 2, None, 0).await.unwrap(),
            Poll::Ready(json("null"))
        );
        let messages = engine.store().list_messages("dest").await.unwrap();
        assert_eq!(messages.len(), 1);
        assert!(!messages[0].consumed);
    }
);

all_dbs!(recv_waits_until_the_recorded_deadline, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let recv = begin_run(&engine, "dest").await;
    let first = engine.poll_recv(&recv, 1, 2, None, 5_000).await.unwrap();
    let Poll::Waiting {
        remaining_ms,
        deadline_ms,
    } = first
    else {
        panic!("expected to wait, got {first:?}");
    };
    assert!(remaining_ms > 1_000, "{remaining_ms}");
    let second = engine.poll_recv(&recv, 1, 2, None, 5_000).await.unwrap();
    let Poll::Waiting {
        remaining_ms: again,
        deadline_ms: same_deadline,
    } = second
    else {
        panic!("expected to keep waiting, got {second:?}");
    };
    assert_eq!(deadline_ms, same_deadline);
    assert!(again <= remaining_ms, "{again} > {remaining_ms}");
    let sleep = engine
        .store()
        .load_step("dest", 2, "DBOS.sleep")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sleep.output.as_deref(),
        Some(deadline_ms.to_string()).as_deref()
    );
});

all_dbs!(a_message_arriving_during_a_wait_is_consumed, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let recv = begin_run(&engine, "dest").await;
    let waiting = engine.poll_recv(&recv, 1, 2, None, 5_000).await.unwrap();
    assert!(matches!(waiting, Poll::Waiting { .. }), "{waiting:?}");
    engine
        .send("dest", None, "\"hi\"", PORTABLE_JSON, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        engine.poll_recv(&recv, 1, 2, None, 5_000).await.unwrap(),
        Poll::Ready(json("\"hi\""))
    );
});

all_dbs!(topics_are_separate_queues, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let recv = begin_run(&engine, "dest").await;
    engine
        .send("dest", None, "\"default\"", PORTABLE_JSON, None, None, None)
        .await
        .unwrap();
    engine
        .send(
            "dest",
            Some("other"),
            "\"topic\"",
            PORTABLE_JSON,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .poll_recv(&recv, 1, 2, Some("other"), 0)
            .await
            .unwrap(),
        Poll::Ready(json("\"topic\""))
    );
    assert_eq!(
        engine.poll_recv(&recv, 3, 4, None, 0).await.unwrap(),
        Poll::Ready(json("\"default\""))
    );
});

all_dbs!(stale_owner_cannot_send_or_recv, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    begin_run(&engine, "dest").await;
    let stale = Engine::connect(
        &db.url,
        EngineConfig {
            lease: EXPIRED,
            pickle: false,
            ..config()
        },
    )
    .await
    .unwrap();
    let sender = begin_run(&stale, "sender").await;
    stale.recover_now(&names()).await.unwrap();
    let error = stale
        .send(
            "dest",
            None,
            "\"no\"",
            PORTABLE_JSON,
            None,
            Some(&sender),
            Some(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
    assert!(
        stale
            .store()
            .list_messages("dest")
            .await
            .unwrap()
            .is_empty()
    );
});

all_dbs!(set_event_upserts_and_is_recorded_as_a_step, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "w").await;
    engine
        .set_event(&run, Some(1), "k", "\"first\"", PORTABLE_JSON)
        .await
        .unwrap();
    engine
        .set_event(&run, Some(2), "k", "\"second\"", PORTABLE_JSON)
        .await
        .unwrap();
    let events = engine.list_events("w").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, "k");
    assert_eq!(events[0].value, json("\"second\""));
    assert_eq!(
        engine
            .set_event(&run, Some(1), "k", "\"first\"", PORTABLE_JSON)
            .await
            .unwrap(),
        SendOutcome::Replayed
    );
    let events = engine.list_events("w").await.unwrap();
    assert_eq!(events[0].value, json("\"second\""));
});

all_dbs!(
    set_event_from_a_step_overwrites_without_a_new_step,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let run = begin_run(&engine, "w").await;
        engine
            .set_event(&run, None, "k", "\"bad\"", PORTABLE_JSON)
            .await
            .unwrap();
        engine
            .set_event(&run, None, "k", "\"good\"", PORTABLE_JSON)
            .await
            .unwrap();
        let events = engine.list_events("w").await.unwrap();
        assert_eq!(events[0].value, json("\"good\""));
        assert!(
            engine
                .store()
                .load_step("w", 1, "DBOS.setEvent")
                .await
                .unwrap()
                .is_none()
        );
    }
);

all_dbs!(get_event_returns_a_value_without_waiting, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let setter = begin_run(&engine, "src").await;
    engine
        .set_event(&setter, Some(1), "k", "\"v\"", PORTABLE_JSON)
        .await
        .unwrap();
    let getter = begin_run(&engine, "dst").await;
    assert_eq!(
        engine
            .poll_event("src", "k", 5_000, None, Some((&getter, 1, 2)))
            .await
            .unwrap(),
        Poll::Ready(json("\"v\""))
    );
    assert_eq!(
        engine
            .poll_event("src", "k", 5_000, None, Some((&getter, 1, 2)))
            .await
            .unwrap(),
        Poll::Ready(json("\"v\""))
    );
});

all_dbs!(get_event_of_a_missing_key_times_out, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let getter = begin_run(&engine, "dst").await;
    assert_eq!(
        engine
            .poll_event("missing", "k", 0, None, Some((&getter, 1, 2)))
            .await
            .unwrap(),
        Poll::Ready(json("null"))
    );
});

all_dbs!(get_event_outside_a_workflow_is_not_recorded, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let waiting = engine
        .poll_event("missing", "k", 5_000, None, None)
        .await
        .unwrap();
    assert!(matches!(waiting, Poll::Waiting { .. }), "{waiting:?}");
    let setter = begin_run(&engine, "src").await;
    engine
        .set_event(&setter, Some(1), "k", "\"v\"", PORTABLE_JSON)
        .await
        .unwrap();
    assert_eq!(
        engine
            .poll_event("src", "k", 5_000, None, None)
            .await
            .unwrap(),
        Poll::Ready(json("\"v\""))
    );
});

all_dbs!(a_none_event_is_present_and_does_not_wait, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let setter = begin_run(&engine, "src").await;
    engine
        .set_event(&setter, Some(1), "k", "null", PORTABLE_JSON)
        .await
        .unwrap();
    assert_eq!(
        engine
            .poll_event("src", "k", 5_000, None, None)
            .await
            .unwrap(),
        Poll::Ready(json("null"))
    );
});

all_dbs!(
    recovered_waits_use_arrivals_before_the_recorded_deadline,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let recv = begin_run(&engine, "receiver").await;
        let target = begin_run(&engine, "target").await;
        // Deadlines stay ahead of the "before" sends, then the sleep passes every one of them.
        // Wide enough that a loaded suite still sends the "before" values ahead of the deadline.
        const WAIT_MS: i64 = 1500;
        assert!(matches!(
            engine.poll_recv(&recv, 1, 2, None, WAIT_MS).await.unwrap(),
            Poll::Waiting { .. }
        ));
        assert!(matches!(
            engine
                .poll_recv(&recv, 3, 4, Some("early"), WAIT_MS)
                .await
                .unwrap(),
            Poll::Waiting { .. }
        ));
        assert!(matches!(
            engine
                .poll_event("target", "overwritten", WAIT_MS, None, Some((&recv, 5, 6)))
                .await
                .unwrap(),
            Poll::Waiting { .. }
        ));
        assert!(matches!(
            engine
                .poll_event("target", "late", WAIT_MS, None, Some((&recv, 7, 8)))
                .await
                .unwrap(),
            Poll::Waiting { .. }
        ));
        engine
            .send(
                "receiver",
                Some("early"),
                "\"before\"",
                PORTABLE_JSON,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        engine
            .set_event(&target, None, "overwritten", "\"before\"", PORTABLE_JSON)
            .await
            .unwrap();
        let recorded = Instant::now();
        tokio::time::sleep(Duration::from_millis(u64::try_from(WAIT_MS).unwrap() + 40)).await;
        assert!(
            recorded.elapsed() > Duration::from_millis(u64::try_from(WAIT_MS).unwrap()),
            "the sleep did not pass the recorded deadlines"
        );
        engine
            .send(
                "receiver",
                None,
                "\"after\"",
                PORTABLE_JSON,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        engine
            .set_event(&target, None, "overwritten", "\"after\"", PORTABLE_JSON)
            .await
            .unwrap();
        engine
            .set_event(&target, None, "late", "\"after\"", PORTABLE_JSON)
            .await
            .unwrap();
        engine.release(&recv).await.unwrap();
        let claim = engine.recover_now(&names()).await.unwrap().remove(0);
        let recv = Run::new(claim.lease, claim.serialization, claim.deadline_epoch_ms);
        assert_eq!(
            engine.poll_recv(&recv, 1, 2, None, 500).await.unwrap(),
            Poll::Ready(json("null"))
        );
        assert_eq!(
            engine
                .poll_recv(&recv, 3, 4, Some("early"), 500)
                .await
                .unwrap(),
            Poll::Ready(json("\"before\""))
        );
        assert_eq!(
            engine
                .poll_event("target", "overwritten", 500, None, Some((&recv, 5, 6)))
                .await
                .unwrap(),
            Poll::Ready(json("\"before\""))
        );
        assert_eq!(
            engine
                .poll_event("target", "late", 500, None, Some((&recv, 7, 8)))
                .await
                .unwrap(),
            Poll::Ready(json("null"))
        );
        let messages = engine.store().list_messages("receiver").await.unwrap();
        assert_eq!(
            messages.iter().filter(|message| !message.consumed).count(),
            1
        );
        assert_eq!(
            engine.poll_recv(&recv, 9, 10, None, 0).await.unwrap(),
            Poll::Ready(json("\"after\""))
        );
        assert_eq!(
            engine
                .poll_event("target", "overwritten", 0, None, None)
                .await
                .unwrap(),
            Poll::Ready(json("\"after\""))
        );
    }
);

all_dbs!(
    legacy_events_are_read_immediately_but_have_no_recovered_arrival_time,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = engine(&db).await;
        let recv = begin_run(&engine, "receiver").await;
        begin_run(&engine, "target").await;
        assert!(matches!(
            engine
                .poll_event("target", "legacy", 80, None, Some((&recv, 1, 2)))
                .await
                .unwrap(),
            Poll::Waiting { .. }
        ));
        tokio::time::sleep(Duration::from_millis(130)).await;
        let sql = match backend {
            Backend::MySql => {
                "INSERT INTO workflow_events (workflow_uuid, `key`, value, serialization) VALUES ('target', 'legacy', '1', 'portable_json')"
            }
            _ => {
                "INSERT INTO workflow_events (workflow_uuid, \"key\", value, serialization) VALUES ('target', 'legacy', '1', 'portable_json')"
            }
        };
        sqlx::query(sql)
            .execute(engine.store().pool())
            .await
            .unwrap();
        assert_eq!(
            engine
                .poll_event("target", "legacy", 500, None, Some((&recv, 1, 2)))
                .await
                .unwrap(),
            Poll::Ready(json("null"))
        );
        assert_eq!(
            engine
                .poll_event("target", "legacy", 0, None, Some((&recv, 3, 4)))
                .await
                .unwrap(),
            Poll::Ready(json("1"))
        );
        assert_eq!(
            engine
                .poll_event("target", "legacy", 0, None, None)
                .await
                .unwrap(),
            Poll::Ready(json("1"))
        );
        assert_eq!(
            engine
                .poll_event("target", "legacy", 5_000, None, None)
                .await
                .unwrap(),
            Poll::Ready(json("1"))
        );
        let expired = engine.store().now_ms().await.unwrap() - 1;
        assert_eq!(
            engine
                .poll_event("target", "legacy", 5_000, Some(expired), None)
                .await
                .unwrap(),
            Poll::Ready(json("null"))
        );
    }
);

// Far above any timeout a test expects to reach, so a pass cannot come from the timeout itself.
const LONG_WAIT_MS: i64 = 60_000;
const BOUND: Duration = Duration::from_secs(30);
// A write can land inside a re-read that then sees it, which says nothing about the wakeup. So a
// wakeup test passes when one round of these shows a wake. With no wakeup, no round ever does.
const ROUNDS: u32 = 10;

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, future)
        .await
        .expect("the wait did not end in time")
}

/// Returns once a wait is registered, with time for it to reach its timer.
async fn until_waiting(engine: &Engine) {
    within(async {
        while engine.waiting_keys() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
}

fn spawn_recv(
    engine: &Engine,
    run: &Arc<Run>,
) -> tokio::task::JoinHandle<Result<StoredValue, Error>> {
    let (engine, run) = (engine.clone(), run.clone());
    tokio::spawn(async move { engine.wait_recv(&run, 1, 2, None, LONG_WAIT_MS).await })
}

fn spawn_event(
    engine: &Engine,
    target: &str,
) -> tokio::task::JoinHandle<Result<StoredValue, Error>> {
    let (engine, target) = (engine.clone(), target.to_owned());
    tokio::spawn(async move { engine.wait_event(&target, "k", LONG_WAIT_MS, None).await })
}

async fn send(engine: &Engine, destination: &str, message: &str) {
    engine
        .send(destination, None, message, PORTABLE_JSON, None, None, None)
        .await
        .unwrap();
}

all_dbs!(same_engine_send_wakes_a_waiter, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    for round in 0..ROUNDS {
        let id = format!("dest-{round}");
        let run = Arc::new(begin_run(&engine, &id).await);
        let before = engine.wakeups_received();
        let waiter = spawn_recv(&engine, &run);
        until_waiting(&engine).await;
        send(&engine, &id, "\"hi\"").await;
        assert_eq!(within(waiter).await.unwrap().unwrap(), json("\"hi\""));
        assert_eq!(engine.waiting_keys(), 0);
        if engine.wakeups_received() > before {
            return;
        }
    }
    panic!("no send reached its waiter through the wakeup");
});

all_dbs!(same_engine_set_event_wakes_a_waiter, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    for round in 0..ROUNDS {
        let id = format!("target-{round}");
        let run = begin_run(&engine, &id).await;
        let before = engine.wakeups_received();
        let waiter = spawn_event(&engine, &id);
        until_waiting(&engine).await;
        engine
            .set_event(&run, None, "k", "1", PORTABLE_JSON)
            .await
            .unwrap();
        assert_eq!(within(waiter).await.unwrap().unwrap(), json("1"));
        assert_eq!(engine.waiting_keys(), 0);
        if engine.wakeups_received() > before {
            return;
        }
    }
    panic!("no set_event reached its waiter through the wakeup");
});

all_dbs!(a_second_engine_send_is_seen_by_re_reading, backend, {
    let db = TestDb::migrated(backend).await;
    let (waiting, sending) = (engine(&db).await, engine(&db).await);
    let run = Arc::new(begin_run(&waiting, "dest").await);
    let waiter = spawn_recv(&waiting, &run);
    until_waiting(&waiting).await;
    send(&sending, "dest", "\"hi\"").await;
    assert_eq!(within(waiter).await.unwrap().unwrap(), json("\"hi\""));
    assert_eq!(waiting.wakeups_received(), 0);
});

all_dbs!(a_second_engine_set_event_is_seen_by_re_reading, backend, {
    let db = TestDb::migrated(backend).await;
    let (waiting, writing) = (engine(&db).await, engine(&db).await);
    let run = begin_run(&writing, "target").await;
    let waiter = spawn_event(&waiting, "target");
    until_waiting(&waiting).await;
    writing
        .set_event(&run, None, "k", "1", PORTABLE_JSON)
        .await
        .unwrap();
    assert_eq!(within(waiter).await.unwrap().unwrap(), json("1"));
    assert_eq!(waiting.wakeups_received(), 0);
});

all_dbs!(wait_recv_timeout_fires_and_replays, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "dest").await;
    assert_eq!(
        within(engine.wait_recv(&run, 1, 2, None, 50))
            .await
            .unwrap(),
        json("null")
    );
    let steps = engine.list_steps("dest").await.unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|step| step.step_name.as_str())
            .collect::<Vec<_>>(),
        ["DBOS.recv", "DBOS.sleep"]
    );
    send(&engine, "dest", "\"late\"").await;
    assert_eq!(
        within(engine.wait_recv(&run, 1, 2, None, LONG_WAIT_MS))
            .await
            .unwrap(),
        json("null")
    );
    assert!(!engine.store().list_messages("dest").await.unwrap()[0].consumed);
    assert_eq!(
        within(engine.wait_recv(&run, 3, 4, None, LONG_WAIT_MS))
            .await
            .unwrap(),
        json("\"late\"")
    );
});

all_dbs!(wait_event_timeout_fires_and_replays, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let target = begin_run(&engine, "target").await;
    let caller = begin_run(&engine, "caller").await;
    assert_eq!(
        within(engine.wait_event("target", "k", 50, None))
            .await
            .unwrap(),
        json("null")
    );
    assert_eq!(
        within(engine.wait_event("target", "k", 50, Some((&caller, 1, 2))))
            .await
            .unwrap(),
        json("null")
    );
    let steps = engine.list_steps("caller").await.unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|step| step.step_name.as_str())
            .collect::<Vec<_>>(),
        ["DBOS.getEvent", "DBOS.sleep"]
    );
    engine
        .set_event(&target, None, "k", "1", PORTABLE_JSON)
        .await
        .unwrap();
    assert_eq!(
        within(engine.wait_event("target", "k", LONG_WAIT_MS, Some((&caller, 1, 2))))
            .await
            .unwrap(),
        json("null")
    );
    assert_eq!(
        within(engine.wait_event("target", "k", LONG_WAIT_MS, Some((&caller, 3, 4))))
            .await
            .unwrap(),
        json("1")
    );
});

all_dbs!(zero_and_negative_timeouts_do_not_wait, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "dest").await;
    for (step_id, timeout_ms) in [(1, 0), (3, -5)] {
        assert_eq!(
            within(engine.wait_recv(&run, step_id, step_id + 1, None, timeout_ms))
                .await
                .unwrap(),
            json("null")
        );
    }
    for timeout_ms in [0, -5] {
        assert_eq!(
            within(engine.wait_event("dest", "k", timeout_ms, None))
                .await
                .unwrap(),
            json("null")
        );
    }
    engine
        .set_event(&run, None, "k", "1", PORTABLE_JSON)
        .await
        .unwrap();
    assert_eq!(
        within(engine.wait_event("dest", "k", 0, None))
            .await
            .unwrap(),
        json("1")
    );
});

all_dbs!(same_engine_cancel_wakes_a_waiter, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    for round in 0..ROUNDS {
        let id = format!("dest-{round}");
        let run = Arc::new(begin_run(&engine, &id).await);
        let before = engine.wakeups_received();
        let waiter = spawn_recv(&engine, &run);
        until_waiting(&engine).await;
        engine.cancel_workflow(&id).await.unwrap();
        let error = within(waiter).await.unwrap().unwrap_err();
        assert!(
            matches!(&error, Error::WorkflowCancelled(found) if *found == id),
            "{error:?}"
        );
        if engine.wakeups_received() > before {
            return;
        }
    }
    panic!("no cancel reached its waiter through the wakeup");
});

all_dbs!(cancel_of_the_caller_stops_a_get_event_wait, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    begin_run(&engine, "target").await;
    let caller = Arc::new(begin_run(&engine, "caller").await);
    let waiter = {
        let (engine, caller) = (engine.clone(), caller.clone());
        tokio::spawn(async move {
            engine
                .wait_event("target", "k", LONG_WAIT_MS, Some((&caller, 1, 2)))
                .await
        })
    };
    until_waiting(&engine).await;
    engine.cancel_workflow("caller").await.unwrap();
    let error = within(waiter).await.unwrap().unwrap_err();
    assert!(matches!(error, Error::WorkflowCancelled(_)), "{error:?}");
});

all_dbs!(a_second_engine_cancel_stops_a_waiter, backend, {
    let db = TestDb::migrated(backend).await;
    let (waiting, cancelling) = (engine(&db).await, engine(&db).await);
    let run = Arc::new(begin_run(&waiting, "dest").await);
    let waiter = spawn_recv(&waiting, &run);
    until_waiting(&waiting).await;
    cancelling.cancel_workflow("dest").await.unwrap();
    let error = within(waiter).await.unwrap().unwrap_err();
    assert!(matches!(error, Error::WorkflowCancelled(_)), "{error:?}");
});

all_dbs!(losing_the_lease_stops_a_waiter, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = Arc::new(begin_run(&engine, "dest").await);
    let waiter = spawn_recv(&engine, &run);
    until_waiting(&engine).await;
    let lease = Lease {
        workflow_id: run.workflow_id().to_owned(),
        token: run.token().to_owned(),
    };
    engine.store().release_lease(&lease).await.unwrap();
    let error = within(waiter).await.unwrap().unwrap_err();
    assert!(matches!(error, Error::LeaseLost(_)), "{error:?}");
});

all_dbs!(closing_the_engine_stops_waiters, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = Arc::new(begin_run(&engine, "dest").await);
    let (recv, event) = (spawn_recv(&engine, &run), spawn_event(&engine, "dest"));
    until_waiting(&engine).await;
    engine.close();
    for waiter in [recv, event] {
        let error = within(waiter).await.unwrap().unwrap_err();
        assert!(matches!(error, Error::ShuttingDown), "{error:?}");
    }
});

all_dbs!(no_send_is_lost_in_a_tight_loop, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = Arc::new(begin_run(&engine, "dest").await);
    let count = 40_u32;
    let receiver = {
        let (engine, run) = (engine.clone(), run.clone());
        tokio::spawn(async move {
            let mut received = Vec::new();
            for index in 0..count {
                let step_id = index * 2 + 1;
                let value = engine
                    .wait_recv(&run, step_id, step_id + 1, None, LONG_WAIT_MS)
                    .await
                    .unwrap();
                received.push(value.text);
            }
            received
        })
    };
    for index in 0..count {
        send(&engine, "dest", &index.to_string()).await;
    }
    let expected: Vec<String> = (0..count).map(|index| index.to_string()).collect();
    assert_eq!(within(receiver).await.unwrap(), expected);
    assert_eq!(engine.waiting_keys(), 0);
});

all_dbs!(more_waiters_than_connections_all_finish, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let extra = engine.store().pool().options().get_max_connections() as usize + 5;
    let mut waiters = Vec::new();
    for index in 0..extra {
        let run = Arc::new(begin_run(&engine, &format!("dest-{index}")).await);
        waiters.push((spawn_recv(&engine, &run), run));
    }
    within(async {
        while engine.waiting_keys() < extra {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    for index in 0..extra {
        send(&engine, &format!("dest-{index}"), "1").await;
    }
    for (waiter, _run) in waiters {
        assert_eq!(within(waiter).await.unwrap().unwrap(), json("1"));
    }
    assert_eq!(engine.waiting_keys(), 0);
});

all_dbs!(a_dropped_wait_leaves_no_waiter_behind, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    let run = begin_run(&engine, "dest").await;
    let wait = engine.wait_recv(&run, 1, 2, None, LONG_WAIT_MS);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), wait)
            .await
            .is_err()
    );
    assert_eq!(engine.waiting_keys(), 0);
});

all_dbs!(finishing_a_workflow_wakes_await_result, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    for round in 0..ROUNDS {
        let id = format!("done-{round}");
        let run = begin_run(&engine, &id).await;
        let before = engine.wakeups_received();
        let waiter = {
            let (engine, id) = (engine.clone(), id.clone());
            tokio::spawn(async move { engine.await_result(&id).await })
        };
        until_waiting(&engine).await;
        assert_eq!(
            engine
                .finish(&run, Outcome::Success("1".to_owned()))
                .await
                .unwrap(),
            Finished::Accepted
        );
        let result = within(waiter).await.unwrap().unwrap();
        assert_eq!(result.outcome, FinalOutcome::Success("1".to_owned()));
        if engine.wakeups_received() > before {
            return;
        }
    }
    panic!("no finish reached await_result through the wakeup");
});

all_dbs!(cancelling_a_workflow_wakes_await_result, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    for round in 0..ROUNDS {
        let id = format!("cancelled-{round}");
        begin_run(&engine, &id).await;
        let before = engine.wakeups_received();
        let waiter = {
            let (engine, id) = (engine.clone(), id.clone());
            tokio::spawn(async move { engine.await_result(&id).await })
        };
        until_waiting(&engine).await;
        engine.cancel_workflow(&id).await.unwrap();
        let result = within(waiter).await.unwrap().unwrap();
        assert_eq!(result.outcome, FinalOutcome::Cancelled);
        if engine.wakeups_received() > before {
            return;
        }
    }
    panic!("no cancel reached await_result through the wakeup");
});

all_dbs!(bulk_send_is_atomic_and_replays, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = engine(&db).await;
    begin_run(&engine, "dest").await;
    let run = begin_run(&engine, "sender").await;
    let bad = vec![
        ("dest".into(), None, "1".into(), None),
        ("missing".into(), None, "2".into(), None),
    ];
    assert!(
        engine
            .send_bulk(&bad, PORTABLE_JSON, None, None)
            .await
            .is_err()
    );
    assert!(
        engine
            .store()
            .list_messages("dest")
            .await
            .unwrap()
            .is_empty()
    );
    let messages = vec![
        ("dest".into(), None, "1".into(), None),
        ("dest".into(), None, "2".into(), None),
    ];
    assert_eq!(
        engine
            .send_bulk(&messages, PORTABLE_JSON, Some(&run), Some(1))
            .await
            .unwrap(),
        SendOutcome::Sent
    );
    assert_eq!(
        engine
            .send_bulk(&messages, PORTABLE_JSON, Some(&run), Some(1))
            .await
            .unwrap(),
        SendOutcome::Replayed
    );
    assert_eq!(engine.store().list_messages("dest").await.unwrap().len(), 2);
});
