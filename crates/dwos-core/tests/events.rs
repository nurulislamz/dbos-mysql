use std::time::Duration;

use dwos_core::{
    Begin, Begun, Engine, EngineConfig, Error, PORTABLE_JSON, Poll, Run, SendOutcome, StoredValue,
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
    let db = TestDb::create(backend).await;
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
        let db = TestDb::create(backend).await;
        let engine = engine(&db).await;
        let recv = begin_run(&engine, "receiver").await;
        let target = begin_run(&engine, "target").await;
        assert!(matches!(
            engine.poll_recv(&recv, 1, 2, None, 500).await.unwrap(),
            Poll::Waiting { .. }
        ));
        assert!(matches!(
            engine
                .poll_recv(&recv, 3, 4, Some("early"), 500)
                .await
                .unwrap(),
            Poll::Waiting { .. }
        ));
        assert!(matches!(
            engine
                .poll_event("target", "overwritten", 500, None, Some((&recv, 5, 6)))
                .await
                .unwrap(),
            Poll::Waiting { .. }
        ));
        assert!(matches!(
            engine
                .poll_event("target", "late", 500, None, Some((&recv, 7, 8)))
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
        tokio::time::sleep(Duration::from_millis(600)).await;
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
        let db = TestDb::create(backend).await;
        let engine = engine(&db).await;
        let recv = begin_run(&engine, "receiver").await;
        begin_run(&engine, "target").await;
        assert!(matches!(
            engine
                .poll_event("target", "legacy", 500, None, Some((&recv, 1, 2)))
                .await
                .unwrap(),
            Poll::Waiting { .. }
        ));
        tokio::time::sleep(Duration::from_millis(550)).await;
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
