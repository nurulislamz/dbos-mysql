"""Adapted from dbos-transact-py tests/test_datasource.py at 236142b. See MANIFEST.md."""

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

sa = pytest.importorskip("sqlalchemy")

from yado import DBOS, DBOSConfig, DBOSException, DBOSLeaseLostError, SetWorkflowID  # noqa: E402
from yado import datasources  # noqa: E402
from yado.datasources import AsyncSQLAlchemyDatasource, DBOSInitializationError, SQLAlchemyDatasource  # noqa: E402
from tests.conftest import Database  # noqa: E402

text = sa.text


def _app_url(database: Database, tmp_path: Path, driver: str = "") -> str:
    if database.backend == "sqlite":
        return f"sqlite{driver or ''}:///{tmp_path}/app.sqlite"
    return database.url.replace("mysql://", f"mysql{driver or '+pymysql'}://", 1)


@pytest.fixture
def app_db(database: Database, tmp_path: Path) -> Iterator[Database]:
    """A second database for the app, apart from the system database."""
    app = Database(database.backend, tmp_path)
    yield app
    app.drop()


def _launch(config: DBOSConfig) -> None:
    DBOS(config=config)
    DBOS.launch()


@pytest.fixture
def sync_ds(app_db: Database, config: DBOSConfig, tmp_path: Path) -> Iterator[SQLAlchemyDatasource]:
    DBOS.destroy(destroy_registry=True)
    ds = SQLAlchemyDatasource.create(_app_url(app_db, tmp_path))
    _launch(config)
    yield ds
    DBOS.destroy(destroy_registry=True)
    ds.engine.dispose()


@pytest.fixture
def keep_checkpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    """Most tests read checkpoints after the workflow finished."""
    monkeypatch.setattr(datasources, "drop_checkpoints", lambda run, used: None)


def _rows(ds: SQLAlchemyDatasource, wfid: str) -> list[Any]:
    t = ds._outputs_table
    with ds.engine.connect() as conn:
        return list(conn.execute(sa.select(t).where(t.c.workflow_id == wfid).order_by(t.c.step_id)))


def _race_table(ds: SQLAlchemyDatasource) -> Any:
    table = sa.Table("race_side_effects", sa.MetaData(), sa.Column("tag", sa.String(64)))
    table.create(ds.engine, checkfirst=True)
    return table


def _tags(ds: SQLAlchemyDatasource, table: Any) -> list[str]:
    with ds.engine.connect() as conn:
        return [row.tag for row in conn.execute(sa.select(table.c.tag))]


def test_sync_ds_runs_outside_workflow(sync_ds: SQLAlchemyDatasource) -> None:
    """Outside a workflow, every call style runs transactionally and checkpoints nothing."""
    counter = {"n": 0}

    def increment(amount: int) -> int:
        sync_ds.sql_session().execute(text("SELECT 1"))
        counter["n"] += amount
        return counter["n"]

    assert sync_ds.run_tx_step(None, increment, 5) == 5
    assert sync_ds.transaction(increment)(3) == 8
    assert sync_ds.transaction(isolation_level="SERIALIZABLE", name="my_step")(increment)(2) == 10
    with sync_ds.engine.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(sync_ds._outputs_table)).scalar() == 0


def test_sync_ds_rejects_misuse(tmp_path: Path) -> None:
    from sqlalchemy.orm import sessionmaker

    DBOS.destroy(destroy_registry=True)
    with pytest.raises(DBOSException, match="sessionmaker"):
        SQLAlchemyDatasource.create(f"sqlite:///{tmp_path}/bad.sqlite", sessionmaker=object())
    with pytest.raises(DBOSException, match="binds="):
        SQLAlchemyDatasource.create(f"sqlite:///{tmp_path}/bad.sqlite", sessionmaker=sessionmaker(binds={object: sa.create_engine("sqlite://")}))
    ds = SQLAlchemyDatasource.create(f"sqlite:///{tmp_path}/ds.sqlite", sessionmaker=sessionmaker(binds={}))
    try:
        with pytest.raises(AssertionError):
            ds.sql_session()

        async def coro() -> str:
            return "nope"

        with pytest.raises(DBOSException, match="coroutine"):
            ds.run_tx_step(None, coro)
        with pytest.raises(DBOSException, match="coroutine"):
            ds.transaction(coro)
    finally:
        ds.engine.dispose()


def test_sync_ds_records_and_replays(sync_ds: SQLAlchemyDatasource, keep_checkpoints: None) -> None:
    """Every call style checkpoints to both databases in step order, and the datasource
    checkpoint alone replays the steps once the system database records are lost."""
    calls = {"plain": 0, "unnamed": 0, "named": 0}

    def plain(value: str) -> str:
        calls["plain"] += 1
        sync_ds.sql_session().execute(text("SELECT 1"))
        return f"plain:{value}"

    @sync_ds.transaction
    def unnamed(value: str) -> str:
        calls["unnamed"] += 1
        return f"unnamed:{value}"

    @sync_ds.transaction(name="my_named_step", isolation_level="SERIALIZABLE")
    def named(value: str) -> str:
        calls["named"] += 1
        return f"named:{value}"

    @DBOS.workflow()
    def my_workflow(value: str) -> tuple[str, str, str]:
        return sync_ds.run_tx_step(None, plain, value), unnamed(value), named(value)

    expected = ("plain:x", "unnamed:x", "named:x")
    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow("x") == expected

    steps = DBOS.list_workflow_steps(wfid)
    assert [s["function_id"] for s in steps] == [1, 2, 3]
    assert steps[0]["function_name"].endswith("plain")
    assert steps[1]["function_name"].endswith("unnamed")
    assert steps[2]["function_name"] == "my_named_step"
    assert tuple(s["output"] for s in steps) == expected
    assert [r.step_id for r in _rows(sync_ds, wfid)] == [1, 2, 3]

    # The crash window: the system database records are lost, the datasource rows are not.
    DBOS.delete_workflow(wfid)
    with SetWorkflowID(wfid):
        assert my_workflow("x") == expected
    assert calls == {"plain": 1, "unnamed": 1, "named": 1}


def test_sync_ds_records_and_replays_errors(sync_ds: SQLAlchemyDatasource, keep_checkpoints: None) -> None:
    """A failing body's error, from Python or the database, is recorded once and replayed
    from the datasource checkpoint once the system database records are lost."""
    calls = {"n": 0}

    def fail(kind: str) -> str:
        calls["n"] += 1
        if kind == "sql":
            sync_ds.sql_session().execute(text("selct abc from c"))
        raise ValueError("ds step failed")

    @DBOS.workflow()
    def my_workflow(kind: str) -> str:
        return sync_ds.run_tx_step(None, fail, kind)

    cases = [("python", ValueError, "ds step failed"), ("sql", Exception, "selct")]
    for kind, exc_type, match in cases:
        wfid = str(uuid.uuid4())
        with SetWorkflowID(wfid), pytest.raises(exc_type, match=match):
            my_workflow(kind)
        [row] = _rows(sync_ds, wfid)
        assert row.output is None and row.error is not None
        DBOS.delete_workflow(wfid)
        with SetWorkflowID(wfid), pytest.raises(exc_type, match=match):
            my_workflow(kind)
    assert calls["n"] == len(cases)


def test_sync_ds_rolls_back_once_ownership_moves(sync_ds: SQLAlchemyDatasource, database: Database) -> None:
    """An execution that loses the workflow mid-transaction rolls back rather than
    commit, so a stale execution cannot apply a step the new owner also runs."""
    race = _race_table(sync_ds)
    held: dict[str, Any] = {}

    def step_fn() -> str:
        sync_ds.sql_session().execute(race.insert().values(tag="stale"))
        [(held["token"],)] = database.execute("SELECT lease_token FROM workflow_status WHERE workflow_uuid = %s", wfid)
        database.take_over(wfid)
        return "stale"

    @DBOS.workflow()
    def my_workflow() -> str:
        try:
            return sync_ds.run_tx_step(None, step_fn)
        except DBOSLeaseLostError:
            # A real stale owner stops here. Handed back, so this workflow can finish.
            database.execute("UPDATE workflow_status SET lease_token = %s WHERE workflow_uuid = %s", held["token"], wfid)
            return "conflicted"

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "conflicted"
    assert _tags(sync_ds, race) == []
    assert _rows(sync_ds, wfid) == [], "the stale execution left a checkpoint or an error row"
    assert DBOS.list_workflow_steps(wfid) == []


def test_sync_ds_replays_the_row_that_won(sync_ds: SQLAlchemyDatasource, keep_checkpoints: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """An execution that misses an existing row, and still owns the workflow, rolls back
    its own writes and returns the recorded result instead of a primary key error."""
    race = _race_table(sync_ds)
    calls = {"n": 0}

    def step_fn() -> str:
        calls["n"] += 1
        sync_ds.sql_session().execute(race.insert().values(tag=f"run-{calls['n']}"))
        return f"result-{calls['n']}"

    @DBOS.workflow()
    def my_workflow() -> str:
        return sync_ds.run_tx_step(None, step_fn)

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "result-1"

    # Lose the system database record and blind the next pre-check, as in a real race.
    DBOS.delete_workflow(wfid)
    real_read = sync_ds._read
    blind = {"next": True}

    def blind_read(workflow_id: str, step_id: int) -> Any:
        if blind["next"]:
            blind["next"] = False
            return None
        return real_read(workflow_id, step_id)

    monkeypatch.setattr(sync_ds, "_read", blind_read)
    with SetWorkflowID(wfid):
        assert my_workflow() == "result-1"
    assert calls["n"] == 2  # the second execution did run its body
    assert _tags(sync_ds, race) == ["run-1"]  # and its writes were rolled back


def test_sync_ds_completion_clears_checkpoints(sync_ds: SQLAlchemyDatasource) -> None:
    before_cleanup: list[list[int]] = []

    @DBOS.workflow()
    def my_workflow() -> str:
        result = sync_ds.run_tx_step(None, lambda: "done")
        before_cleanup.append([r.step_id for r in _rows(sync_ds, DBOS.workflow_id or "")])
        return result

    wfid = str(uuid.uuid4())
    with SetWorkflowID(wfid):
        assert my_workflow() == "done"
    assert before_cleanup == [[1]]
    assert _rows(sync_ds, wfid) == []


def test_datasource_must_be_created_before_launch(dbos: DBOS, tmp_path: Path) -> None:
    with pytest.raises(DBOSException, match="before DBOS.launch"):
        SQLAlchemyDatasource.create(f"sqlite:///{tmp_path}/late.sqlite")


def test_ds_run_migrations_false(app_db: Database, tmp_path: Path) -> None:
    """Without migrations a datasource only checks its table, and migrate() creates it."""
    DBOS.destroy(destroy_registry=True)
    url = _app_url(app_db, tmp_path)
    with pytest.raises(DBOSInitializationError, match="run_migrations"):
        SQLAlchemyDatasource.create(url, run_migrations=False)
    SQLAlchemyDatasource.migrate(url)
    SQLAlchemyDatasource.migrate(url)  # a second migration is a no-op
    SQLAlchemyDatasource.create(url, run_migrations=False).engine.dispose()


async def test_async_ds_records_and_replays(app_db: Database, config: DBOSConfig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("greenlet")
    pytest.importorskip("aiosqlite" if app_db.backend == "sqlite" else "aiomysql")
    DBOS.destroy(destroy_registry=True)
    ads = await AsyncSQLAlchemyDatasource.create(_app_url(app_db, tmp_path, "+aiosqlite" if app_db.backend == "sqlite" else "+aiomysql"))
    _launch(config)
    calls = {"n": 0}

    @ads.transaction(name="insert_step")
    async def step(value: str) -> str:
        calls["n"] += 1
        await ads.sql_session().execute(text("SELECT 1"))
        return f"done:{value}"

    @DBOS.workflow()
    async def my_workflow(value: str) -> str:
        return await step(value)

    try:
        wfid = str(uuid.uuid4())
        monkeypatch.setattr(datasources, "drop_checkpoints_async", _no_drop)
        with SetWorkflowID(wfid):
            assert await my_workflow("x") == "done:x"
        DBOS.delete_workflow(wfid)
        with SetWorkflowID(wfid):
            assert await my_workflow("x") == "done:x"
        assert calls["n"] == 1
    finally:
        DBOS.destroy(destroy_registry=True)
        await ads.engine.dispose()


async def _no_drop(run: Any, used: Any) -> None:
    return None
