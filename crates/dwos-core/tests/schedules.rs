use std::num::NonZeroU32;

use dwos_core::{Engine, EngineConfig, NewSchedule, PORTABLE_JSON, ScheduleFire};
use sqlx::Row;

#[macro_use]
mod common;
use common::*;

fn schedule<'a>(
    name: &'a str,
    workflow_name: &'a str,
    cron: &'a str,
    queue: &'a str,
    inputs: &'a str,
    max_recovery_attempts: u32,
) -> NewSchedule<'a> {
    NewSchedule {
        name,
        workflow_name,
        cron,
        queue,
        inputs,
        serialization: PORTABLE_JSON,
        max_recovery_attempts,
    }
}

fn config() -> EngineConfig {
    EngineConfig {
        lease: LONG,
        pickle: false,
        ..EngineConfig::default()
    }
}

fn placeholders(backend: &str, sql: &str) -> String {
    if backend != "postgres" {
        return sql.to_owned();
    }
    let mut number = 1;
    let mut out = String::new();
    for character in sql.chars() {
        if character == '?' {
            out.push('$');
            out.push_str(&number.to_string());
            number += 1;
        } else {
            out.push(character);
        }
    }
    out
}

async fn last_fired(engine: &Engine, name: &str) -> Option<i64> {
    let sql = placeholders(
        engine.store().backend(),
        "SELECT last_fired_epoch_ms FROM workflow_schedules WHERE schedule_name = ?",
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(name)
        .fetch_one(engine.store().pool())
        .await
        .unwrap()
        .try_get("last_fired_epoch_ms")
        .unwrap()
}

async fn rewind(engine: &Engine, name: &str) {
    let sql = placeholders(
        engine.store().backend(),
        "UPDATE workflow_schedules SET last_fired_epoch_ms = 0 WHERE schedule_name = ?",
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(name)
        .execute(engine.store().pool())
        .await
        .unwrap();
}

async fn scheduled_rows(engine: &Engine) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM workflow_status WHERE workflow_uuid LIKE 'sched-%'")
        .fetch_one(engine.store().pool())
        .await
        .unwrap()
}

all_dbs!(
    a_schedule_fires_the_latest_tick_and_skips_while_one_is_pending,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = Engine::connect(&db.url, config()).await.unwrap();
        engine
            .ensure_queue("notifications", NonZeroU32::new(2).unwrap())
            .await
            .unwrap();
        engine
            .ensure_schedule(schedule(
                "daily-job-digest",
                "DispatchDailyDigestWorkflow",
                "* * * * *",
                "notifications",
                "{}",
                100,
            ))
            .await
            .unwrap();
        assert!(engine.fire_schedules().await.unwrap().is_empty());
        assert!(last_fired(&engine, "daily-job-digest").await.is_some());

        rewind(&engine, "daily-job-digest").await;
        let fired = engine.fire_schedules().await.unwrap();
        assert_eq!(fired.len(), 1, "{fired:?}");
        assert!(fired[0].started);
        assert_eq!(fired[0].queue_name, "notifications");
        assert_eq!(fired[0].schedule_name, "daily-job-digest");
        assert!(fired[0].workflow_id.starts_with("sched-daily-job-digest-"));
        assert_eq!(scheduled_rows(&engine).await, 1);

        rewind(&engine, "daily-job-digest").await;
        let skipped = engine.fire_schedules().await.unwrap();
        assert_eq!(
            skipped,
            vec![ScheduleFire {
                schedule_name: "daily-job-digest".to_owned(),
                queue_name: "notifications".to_owned(),
                workflow_id: fired[0].workflow_id.clone(),
                scheduled_at_ms: fired[0].scheduled_at_ms,
                started: false,
            }]
        );
        assert_eq!(scheduled_rows(&engine).await, 1);

        let sql = placeholders(
            engine.store().backend(),
            "UPDATE workflow_status SET status = 'SUCCESS', lease_token = NULL WHERE workflow_uuid = ?",
        );
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(&fired[0].workflow_id)
            .execute(engine.store().pool())
            .await
            .unwrap();
        rewind(&engine, "daily-job-digest").await;
        let again = engine.fire_schedules().await.unwrap();
        assert_eq!(again.len(), 1);
        assert!(!again[0].started);
        assert_eq!(scheduled_rows(&engine).await, 1);

        engine
            .ensure_schedule(schedule(
                "daily-job-digest",
                "DispatchDeadlineRemindersWorkflow",
                "0 * * * *",
                "notifications",
                "{\"kept\":true}",
                7,
            ))
            .await
            .unwrap();
        let stored: (String, String, i64) = sqlx::query_as(
        "SELECT workflow_name, inputs, max_recovery_attempts FROM workflow_schedules WHERE schedule_name = 'daily-job-digest'",
    )
    .fetch_one(engine.store().pool())
    .await
    .unwrap();
        assert_eq!(stored.0, "DispatchDeadlineRemindersWorkflow");
        assert_eq!(stored.1, "{\"kept\":true}");
        assert_eq!(stored.2, 7);
        assert!(last_fired(&engine, "daily-job-digest").await.is_some());
    }
);

all_dbs!(a_missing_queue_does_not_consume_the_tick, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    engine
        .ensure_schedule(schedule(
            "temp-cv-cleaner",
            "TempCvCleanerWorkflow",
            "0 3 * * *",
            "system-ops",
            "{}",
            100,
        ))
        .await
        .unwrap();
    assert!(engine.fire_schedules().await.unwrap().is_empty());
    rewind(&engine, "temp-cv-cleaner").await;
    assert!(engine.fire_schedules().await.unwrap().is_empty());
    assert_eq!(last_fired(&engine, "temp-cv-cleaner").await, Some(0));
    assert_eq!(scheduled_rows(&engine).await, 0);
});

all_dbs!(an_invalid_cron_is_rejected_before_it_is_stored, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(&db.url, config()).await.unwrap();
    assert!(
        engine
            .ensure_schedule(schedule("bad", "wf", "not a cron", "q", "{}", 100))
            .await
            .is_err()
    );
    assert!(
        engine
            .ensure_schedule(schedule("both-days", "wf", "0 0 1 * 1", "q", "{}", 100))
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_schedules")
        .fetch_one(engine.store().pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
});
