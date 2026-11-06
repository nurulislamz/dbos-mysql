use yado_core::{Begin, Begun, Engine, EngineConfig, Error, PORTABLE_JSON, Run};

#[macro_use]
mod common;
use common::*;

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
        other => panic!("expected a new run, got {other:?}"),
    }
}

async fn expire(engine: &Engine, backend: Backend, id: &str) {
    let query = match backend {
        Backend::Postgres => "UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = $1",
        _ => "UPDATE workflow_status SET claim_after = 0 WHERE workflow_uuid = ?",
    };
    sqlx::query(query)
        .bind(id)
        .execute(engine.store().pool())
        .await
        .unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Send,
    SendFromStep,
    SetEvent,
    SetEventFromStep,
    Recv,
    GetEvent,
    Sleep,
}

impl Operation {
    async fn execute(self, engine: &Engine, run: &Run) -> Result<(), Error> {
        match self {
            Self::Send | Self::SendFromStep => engine
                .send(
                    "target",
                    None,
                    "1",
                    PORTABLE_JSON,
                    None,
                    Some(run),
                    matches!(self, Self::Send).then_some(1),
                )
                .await
                .map(|_| ()),
            Self::SetEvent | Self::SetEventFromStep => engine
                .set_event(
                    run,
                    matches!(self, Self::SetEvent).then_some(1),
                    "key",
                    "1",
                    PORTABLE_JSON,
                )
                .await
                .map(|_| ()),
            Self::Recv => engine.poll_recv(run, 1, 2, None, 0).await.map(|_| ()),
            Self::GetEvent => engine
                .poll_event("target", "key", 0, None, Some((run, 1, 2)))
                .await
                .map(|_| ()),
            Self::Sleep => engine.sleep(run, 1, 1.0).await.map(|_| ()),
        }
    }
}

all_dbs!(expired_owners_cannot_start_durable_operations, backend, {
    let db = TestDb::migrated(backend).await;
    let engine = Engine::connect(
        &db.url,
        EngineConfig {
            lease: LONG,
            pickle: false,
            ..EngineConfig::default()
        },
    )
    .await
    .unwrap();
    begin_run(&engine, "target").await;
    for operation in [
        Operation::Send,
        Operation::SendFromStep,
        Operation::SetEvent,
        Operation::SetEventFromStep,
        Operation::Recv,
        Operation::GetEvent,
        Operation::Sleep,
    ] {
        let id = format!("{operation:?}");
        let run = begin_run(&engine, &id).await;
        expire(&engine, backend, &id).await;
        let result = operation.execute(&engine, &run).await;
        assert!(
            matches!(result, Err(Error::LeaseLost(_))),
            "{operation:?}: {result:?}"
        );
        assert!(engine.list_steps(&id).await.unwrap().is_empty());
        assert!(engine.list_events(&id).await.unwrap().is_empty());
        let error = engine
            .save_step_output(&run, 9, "after_refusal", "1".to_owned())
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::LeaseLost(_)),
            "{operation:?}: {error:?}"
        );
    }
    assert!(
        engine
            .store()
            .list_messages("target")
            .await
            .unwrap()
            .is_empty()
    );
});

all_dbs!(
    expired_owners_can_replay_saved_durable_operations,
    backend,
    {
        let db = TestDb::migrated(backend).await;
        let engine = Engine::connect(
            &db.url,
            EngineConfig {
                lease: LONG,
                pickle: false,
                ..EngineConfig::default()
            },
        )
        .await
        .unwrap();
        begin_run(&engine, "target").await;
        for operation in [
            Operation::Send,
            Operation::SetEvent,
            Operation::Recv,
            Operation::GetEvent,
            Operation::Sleep,
        ] {
            let id = format!("{operation:?}");
            let run = begin_run(&engine, &id).await;
            operation.execute(&engine, &run).await.unwrap();
            let steps = engine.list_steps(&id).await.unwrap();
            expire(&engine, backend, &id).await;
            operation.execute(&engine, &run).await.unwrap();
            assert_eq!(engine.list_steps(&id).await.unwrap(), steps);
        }
        assert_eq!(
            engine.store().list_messages("target").await.unwrap().len(),
            1
        );
    }
);
