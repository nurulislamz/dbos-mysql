"""Datasources: steps that run a transaction on the app's own database through SQLAlchemy.

Adapted from dbos-transact-py `dbos/_datasource.py` at 236142b (MIT). The step's result is
written to `datasource_outputs` in the same transaction as the user's SQL, and the
transaction commits only after Rust confirms this run still owns the workflow. A retry
reads that row first, so a transaction that committed before a crash is never run twice.

Rust decides the step ID, the system database replay, and ownership. This file holds only
the app database transaction, because that is the user's SQLAlchemy engine.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import logging
import time
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session, sessionmaker as SyncSessionmaker

from yado import _native
from yado.dbos import _current_run, _current_step, _running
from yado.errors import DBOSException, DBOSLeaseLostError, DBOSWorkflowCancelledError
from yado.sdk_calls import _internal_step
from yado.serialization import Codec, Serializer, decode_exception, encode_exception

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

logger = logging.getLogger("dbos")

_INITIAL_RETRY_WAIT_SECONDS = 0.001
_RETRY_BACKOFF_FACTOR = 1.5
_MAX_RETRY_WAIT_SECONDS = 2.0
_NOT_OWNER = (DBOSLeaseLostError, DBOSWorkflowCancelledError)

IsolationLevel = Literal["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"]


class DatasourceOptions(TypedDict, total=False):
    name: str | None
    isolation_level: IsolationLevel | None


class DBOSInitializationError(DBOSException):
    pass


class _StepAlreadyRecorded(Exception):
    """Another execution recorded this step first. Raised by the insert, so the transaction rolls back."""


def _outputs_table(schema: str | None) -> sa.Table:
    long_text = sa.Text().with_variant(mysql.LONGTEXT(), "mysql")
    return sa.Table(
        "datasource_outputs",
        sa.MetaData(schema=schema),
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("step_id", sa.Integer, nullable=False),
        sa.Column("output", long_text),
        sa.Column("error", long_text),
        sa.Column("serialization", sa.Text),
        sa.Column("created_at", sa.BigInteger, nullable=False),
        sa.PrimaryKeyConstraint("workflow_id", "step_id"),
    )


def _url(database_url: str) -> sa.URL:
    url = sa.make_url(database_url.replace("postgres://", "postgresql://", 1))
    # DBOS connects to Postgres with psycopg. Other drivers are kept as the URL names them.
    return url.set(drivername="postgresql+psycopg") if url.drivername == "postgresql" else url


def _resolve_schema(url: sa.URL, schema: str | None) -> str | None:
    # DBOS defaults to the "dbos" schema on Postgres. On MySQL a schema is a database, so the URL's own is used.
    if url.get_backend_name() == "sqlite":
        return None
    if url.get_backend_name() == "postgresql":
        return schema or "dbos"
    return schema


def _parse_ds_options(ds_options: DatasourceOptions | None, func: Callable[..., Any]) -> tuple[str, str]:
    name = (ds_options.get("name") if ds_options else None) or func.__qualname__
    isolation_level = (ds_options.get("isolation_level") if ds_options else None) or "SERIALIZABLE"
    return name, isolation_level


def _retriable(error: Exception) -> bool:
    """Transient errors worth running the whole transaction again for."""
    if not isinstance(error, DBAPIError):
        return False
    if error.connection_invalidated:
        return True
    orig = error.orig
    if getattr(orig, "sqlstate", None) in ("40001", "40P01"):  # Postgres serialization failure, deadlock
        return True
    args = getattr(orig, "args", ())
    if args and args[0] in (1205, 1213):  # MySQL lock wait timeout, deadlock
        return True
    message = str(orig).lower()
    return "database is locked" in message or "database table is locked" in message


def _set_sqlite_pragmas(dbapi_conn: Any, _record: Any) -> None:
    # Serialize writers and wait out lock contention, as the system database does.
    dbapi_conn.isolation_level = "IMMEDIATE"
    dbapi_conn.execute("PRAGMA busy_timeout=30000")
    dbapi_conn.execute("PRAGMA foreign_keys=ON")


def _reject_session_binds(sessionmaker: Any) -> None:
    # Per-mapper binds outrank bind=self.engine, which would split user writes from the checkpoint.
    if sessionmaker.kw.get("binds"):
        raise DBOSException(
            "A datasource sessionmaker must not set binds=: every statement in a datasource transaction "
            "must use the datasource's engine so it commits atomically with the step checkpoint"
        )


def _check_launch_order() -> None:
    from yado import dbos

    if dbos._instance is not None and dbos._instance._engine is not None:
        raise DBOSException("Datasources must be created before DBOS.launch()")


def _migrate(conn: sa.Connection, table: sa.Table) -> None:
    # shortcut: one schema version, so the table's existence is the version; add a version table with the second migration.
    if table.schema is not None:
        conn.execute(sa.schema.CreateSchema(table.schema, if_not_exists=True))
    conn.execute(sa.schema.CreateTable(table, if_not_exists=True))


def _verify(conn: sa.Connection, table: sa.Table) -> None:
    if not sa.inspect(conn).has_table(table.name, schema=table.schema):
        url = conn.engine.url.render_as_string(hide_password=True)
        raise DBOSInitializationError(
            f"Datasource database {url} has no datasource_outputs table. This datasource was created with "
            "run_migrations disabled, so it will not create it: either migrate it out of band (the "
            "datasource class's `migrate`) or create it with run_migrations enabled."
        )


def _migrate_database(database_url: str, schema: str | None, application_role: str | None) -> None:
    url = _url(database_url)
    if application_role and url.get_backend_name() != "postgresql":
        raise DBOSException("application_role is only supported for Postgres datasources")
    table = _outputs_table(_resolve_schema(url, schema))
    # A bare engine, not a datasource, which would register itself with DBOS.
    engine = sa.create_engine(url)
    try:
        with engine.begin() as conn:
            _migrate(conn, table)
            if application_role:
                role = conn.dialect.identifier_preparer.quote(application_role)
                quoted = conn.dialect.identifier_preparer.quote(table.schema)
                for grant in (f"GRANT USAGE ON SCHEMA {quoted} TO {role}", f"GRANT SELECT, INSERT, DELETE ON {quoted}.datasource_outputs TO {role}"):
                    conn.execute(sa.text(grant))
    finally:
        engine.dispose()


class _Datasource:
    """What the sync and async datasources share: the table, the stored format and the statements."""

    def _setup(self, url: sa.URL, schema: str | None, serializer: Serializer | None) -> None:
        _check_launch_order()
        self.schema = _resolve_schema(url, schema)
        self._outputs_table = _outputs_table(self.schema)
        # Pinned on DBOS's own statements, so a caller engine's schema_translate_map can't move them.
        self._pin: dict[str, Any] = {"schema_translate_map": {self.schema: self.schema}}
        self.serializer = serializer

    def _select(self, workflow_id: str, step_id: int) -> Any:
        t = self._outputs_table
        return sa.select(t.c.output, t.c.error, t.c.serialization).where(t.c.workflow_id == workflow_id, t.c.step_id == step_id)

    def _insert(self, workflow_id: str, step_id: int, output: str | None, error: str | None, serialization: str) -> Any:
        # created_at is informational, so the app process clock is enough.
        return sa.insert(self._outputs_table).values(
            workflow_id=workflow_id, step_id=step_id, output=output, error=error, serialization=serialization, created_at=int(time.time() * 1000)
        )

    def _delete(self, workflow_id: str) -> Any:
        return sa.delete(self._outputs_table).where(self._outputs_table.c.workflow_id == workflow_id)

    def _encode(self, value: Any, codec: Codec, run: _native.Run) -> tuple[str, str]:
        # DBOS stores pickle here whatever the config says; this follows the config, so a JSON process never loads a pickle.
        if self.serializer is not None:
            return self.serializer.serialize(value), self.serializer.name()
        return codec.encode_output(value, run.serialization), run.serialization

    def _replay(self, recorded: Any, codec: Codec) -> Any:
        output, error, serialization = recorded
        if error:
            raise decode_exception(error, serialization)
        if output is None:
            raise DBOSException("Datasource recorded output and error are both None")
        if self.serializer is not None and serialization == self.serializer.name():
            return self.serializer.deserialize(output)
        return codec.decode_output(output, serialization)

    def _track(self) -> _native.Run | None:
        """The run to checkpoint for, or None outside a workflow or inside a step, where nothing is recorded."""
        from yado.dbos import _used_datasources

        run = _current_run.get()
        if run is None or _current_step.get() is not None:
            return None
        used = _used_datasources.get()
        # Tracked even on replay, so completion also clears an earlier execution's rows.
        if used is not None and self not in used:
            used.append(self)
        return run


def _wait(seconds: float) -> float:
    return min(seconds * _RETRY_BACKOFF_FACTOR, _MAX_RETRY_WAIT_SECONDS)


def _check_owner(run: _native.Run) -> None:
    instance = _running()
    instance._blocking(instance._engine.check_owner, run)


class SQLAlchemyDatasource(_Datasource):
    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: dict[str, Any],
        engine: sa.Engine | None,
        schema: str | None,
        serializer: Serializer | None,
        sessionmaker: SyncSessionmaker[Any] | None = None,
    ) -> None:
        if sessionmaker is not None and not isinstance(sessionmaker, SyncSessionmaker):
            raise DBOSException("SQLAlchemyDatasource requires a sqlalchemy.orm.sessionmaker")
        if sessionmaker is not None:
            _reject_session_binds(sessionmaker)
        url = engine.url if engine is not None else _url(database_url)
        self._setup(url, schema, serializer)
        self.created_engine = engine is None
        if engine is None:
            engine = sa.create_engine(url, **engine_kwargs)
            if url.get_backend_name() == "sqlite":
                sa.event.listens_for(engine, "connect")(_set_sqlite_pragmas)
        self.engine = engine
        # No expiry by default: returned ORM objects outlive the session and are checkpointed after commit.
        self.sessionmaker: SyncSessionmaker[Any] = sessionmaker if sessionmaker is not None else SyncSessionmaker(expire_on_commit=False)
        self._session: contextvars.ContextVar[Session | None] = contextvars.ContextVar("dbos_ds_session", default=None)

    @staticmethod
    def migrate(database_url: str, *, schema: str | None = None, application_role: str | None = None) -> None:
        """Create the datasource's table with a privileged role, optionally granting application_role what it needs."""
        _migrate_database(database_url, schema, application_role)

    @staticmethod
    def create(
        database_url: str,
        engine_kwargs: dict[str, Any] | None = None,
        engine: sa.Engine | None = None,
        schema: str | None = None,
        serializer: Serializer | None = None,
        sessionmaker: SyncSessionmaker[Any] | None = None,
        run_migrations: bool = True,
    ) -> SQLAlchemyDatasource:
        instance = SQLAlchemyDatasource(
            database_url=database_url, engine_kwargs=engine_kwargs or {}, engine=engine, schema=schema, serializer=serializer, sessionmaker=sessionmaker
        )
        if run_migrations:
            with instance.engine.begin() as conn:
                _migrate(conn, instance._outputs_table)
        else:
            with instance.engine.connect() as conn:
                _verify(conn, instance._outputs_table)
        return instance

    def sql_session(self) -> Session:
        session = self._session.get()
        assert session is not None, "sql_session() must be called within a sync datasource transaction"
        return session

    def _read(self, workflow_id: str, step_id: int) -> Any:
        wait = _INITIAL_RETRY_WAIT_SECONDS
        while True:
            try:
                with self.engine.connect() as conn:
                    return conn.execute(self._select(workflow_id, step_id), execution_options=self._pin).first()
            except DBAPIError as error:
                if not _retriable(error):
                    raise
                time.sleep(wait)
                wait = _wait(wait)

    def _record(self, conn: Any, run: _native.Run, step_id: int, output: str | None, error: str | None, serialization: str) -> None:
        try:
            conn.execute(self._insert(run.workflow_id, step_id, output, error, serialization), execution_options=self._pin)
        except IntegrityError:
            raise _StepAlreadyRecorded() from None
        # Holding this step's row, so a later owner's insert waits on our commit.
        _check_owner(run)

    def _record_error(self, run: _native.Run, step_id: int, error: Exception) -> None:
        with self.engine.begin() as conn:
            self._record(conn, run, step_id, None, encode_exception(error, run.serialization), run.serialization)

    def _transact(self, run: _native.Run | None, isolation_level: str, func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        step_id = -1
        codec = Codec(func)
        if run is not None:
            step = _current_step.get()
            assert step is not None
            step_id = step.step_id
            recorded = self._read(run.workflow_id, step_id)
            if recorded is not None:
                return self._replay(recorded, codec)
        wait = _INITIAL_RETRY_WAIT_SECONDS
        try:
            while True:
                with self.sessionmaker(bind=self.engine) as session:
                    token = self._session.set(session)
                    try:
                        with session.begin():
                            session.connection(execution_options={"isolation_level": isolation_level})
                            output = func(*args, **kwargs)
                            if run is not None:
                                # Flush first, so the checkpoint holds generated keys and defaults, as the caller sees them.
                                session.flush()
                                text, serialization = self._encode(output, codec, run)
                                self._record(session, run, step_id, text, None, serialization)
                        return output
                    except (_StepAlreadyRecorded, *_NOT_OWNER):
                        raise  # the recorded result wins, and a stale owner records nothing
                    except Exception as error:
                        if _retriable(error):
                            time.sleep(wait)
                            wait = _wait(wait)
                            continue
                        if run is not None:
                            self._record_error(run, step_id, error)
                        raise
                    finally:
                        self._session.reset(token)
        except _StepAlreadyRecorded:
            pass
        # Outside the except block, so the internal signal stays out of the traceback chain.
        assert run is not None
        _check_owner(run)  # another owner: stop, as an ordinary step's loser does
        # Still the owner: the row is our own ambiguous commit or a stale execution's, and either is this step's result.
        recorded = self._read(run.workflow_id, step_id)
        if recorded is None:
            raise DBOSException(f"Datasource step {step_id} of workflow {run.workflow_id} conflicted with a concurrent execution, but no recorded result was found")
        return self._replay(recorded, codec)

    def _drop_checkpoints(self, run: _native.Run) -> bool:
        """Delete the workflow's checkpoints; commit only while this run owns it."""
        with self.engine.connect() as conn, conn.begin() as txn:
            conn.execute(self._delete(run.workflow_id), execution_options=self._pin)
            # Delete, then check: every removed row predates any later owner's.
            try:
                _check_owner(run)
            except _NOT_OWNER:
                txn.rollback()
                return False
        return True

    def run_tx_step(self, ds_options: DatasourceOptions | None, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        name, isolation_level = _parse_ds_options(ds_options, func)
        if inspect.iscoroutinefunction(func):
            raise DBOSException(f"Function {name} is a coroutine; use AsyncSQLAlchemyDatasource.run_tx_step_async instead")
        run = self._track()
        if run is None:
            return self._transact(None, isolation_level, func, args, kwargs)

        @functools.wraps(func)  # keeps func's annotations, which the step codec reads
        def body(*call_args: Any, **call_kwargs: Any) -> Any:
            return self._transact(run, isolation_level, func, call_args, call_kwargs)

        return _internal_step({"name": name}, body)(*args, **kwargs)

    def transaction(self, func: Callable[..., Any] | None = None, *, name: str | None = None, isolation_level: IsolationLevel = "SERIALIZABLE") -> Any:
        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            if inspect.iscoroutinefunction(f):
                raise DBOSException(f"SQLAlchemyDatasource.transaction requires a non-coroutine function, but {f.__qualname__} is a coroutine")
            ds_options: DatasourceOptions = {"isolation_level": isolation_level}
            if name is not None:
                ds_options["name"] = name

            @functools.wraps(f)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return self.run_tx_step(ds_options, f, *args, **kwargs)

            return wrapper

        return decorator(func) if func is not None else decorator


class AsyncSQLAlchemyDatasource(_Datasource):
    def __init__(
        self,
        *,
        database_url: str,
        engine_kwargs: dict[str, Any],
        engine: AsyncEngine | None,
        schema: str | None,
        serializer: Serializer | None,
        sessionmaker: async_sessionmaker[Any] | None = None,
    ) -> None:
        # Imported here: SQLAlchemy's asyncio needs greenlet, which a sync-only app may not have.
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        if sessionmaker is not None and not isinstance(sessionmaker, async_sessionmaker):
            raise DBOSException("AsyncSQLAlchemyDatasource requires an async_sessionmaker")
        if sessionmaker is not None:
            _reject_session_binds(sessionmaker)
        url = engine.url if engine is not None else _url(database_url)
        self._setup(url, schema, serializer)
        self.created_engine = engine is None
        if engine is None:
            engine = create_async_engine(url, **engine_kwargs)
            if url.get_backend_name() == "sqlite":
                # AsyncEngine events must be attached to the underlying sync engine.
                sa.event.listens_for(engine.sync_engine, "connect")(_set_sqlite_pragmas)
        self.engine = engine
        self.sessionmaker: async_sessionmaker[Any] = sessionmaker if sessionmaker is not None else async_sessionmaker(expire_on_commit=False)
        self._session: contextvars.ContextVar[AsyncSession | None] = contextvars.ContextVar("dbos_async_ds_session", default=None)

    @staticmethod
    async def migrate(database_url: str, *, schema: str | None = None, application_role: str | None = None) -> None:
        """Coroutine version of SQLAlchemyDatasource.migrate."""
        await asyncio.to_thread(_migrate_database, database_url, schema, application_role)

    @staticmethod
    async def create(
        database_url: str,
        engine_kwargs: dict[str, Any] | None = None,
        engine: AsyncEngine | None = None,
        schema: str | None = None,
        serializer: Serializer | None = None,
        sessionmaker: async_sessionmaker[Any] | None = None,
        run_migrations: bool = True,
    ) -> AsyncSQLAlchemyDatasource:
        instance = AsyncSQLAlchemyDatasource(
            database_url=database_url, engine_kwargs=engine_kwargs or {}, engine=engine, schema=schema, serializer=serializer, sessionmaker=sessionmaker
        )
        if run_migrations:
            async with instance.engine.begin() as conn:
                await conn.run_sync(_migrate, instance._outputs_table)
        else:
            async with instance.engine.connect() as conn:
                await conn.run_sync(_verify, instance._outputs_table)
        return instance

    def sql_session(self) -> AsyncSession:
        session = self._session.get()
        assert session is not None, "sql_session() must be called within an async datasource transaction"
        return session

    async def _read(self, workflow_id: str, step_id: int) -> Any:
        wait = _INITIAL_RETRY_WAIT_SECONDS
        while True:
            try:
                async with self.engine.connect() as conn:
                    return (await conn.execute(self._select(workflow_id, step_id), execution_options=self._pin)).first()
            except DBAPIError as error:
                if not _retriable(error):
                    raise
                await asyncio.sleep(wait)
                wait = _wait(wait)

    async def _record(self, conn: Any, run: _native.Run, step_id: int, output: str | None, error: str | None, serialization: str) -> None:
        try:
            await conn.execute(self._insert(run.workflow_id, step_id, output, error, serialization), execution_options=self._pin)
        except IntegrityError:
            raise _StepAlreadyRecorded() from None
        # Holding this step's row, so a later owner's insert waits on our commit.
        await _running()._engine.check_owner(run)

    async def _record_error(self, run: _native.Run, step_id: int, error: Exception) -> None:
        async with self.engine.begin() as conn:
            await self._record(conn, run, step_id, None, encode_exception(error, run.serialization), run.serialization)

    async def _transact(self, run: _native.Run | None, isolation_level: str, func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        step_id = -1
        codec = Codec(func)
        if run is not None:
            step = _current_step.get()
            assert step is not None
            step_id = step.step_id
            recorded = await self._read(run.workflow_id, step_id)
            if recorded is not None:
                return self._replay(recorded, codec)
        wait = _INITIAL_RETRY_WAIT_SECONDS
        try:
            while True:
                async with self.sessionmaker(bind=self.engine) as session:
                    token = self._session.set(session)
                    try:
                        async with session.begin():
                            await session.connection(execution_options={"isolation_level": isolation_level})
                            output = await func(*args, **kwargs)
                            if run is not None:
                                await session.flush()
                                text, serialization = self._encode(output, codec, run)
                                await self._record(session, run, step_id, text, None, serialization)
                        return output
                    except (_StepAlreadyRecorded, *_NOT_OWNER):
                        raise
                    except Exception as error:
                        if _retriable(error):
                            await asyncio.sleep(wait)
                            wait = _wait(wait)
                            continue
                        if run is not None:
                            await self._record_error(run, step_id, error)
                        raise
                    finally:
                        self._session.reset(token)
        except _StepAlreadyRecorded:
            pass
        assert run is not None
        await _running()._engine.check_owner(run)
        recorded = await self._read(run.workflow_id, step_id)
        if recorded is None:
            raise DBOSException(f"Datasource step {step_id} of workflow {run.workflow_id} conflicted with a concurrent execution, but no recorded result was found")
        return self._replay(recorded, codec)

    async def _drop_checkpoints(self, run: _native.Run) -> bool:
        async with self.engine.connect() as conn:
            async with conn.begin() as txn:
                await conn.execute(self._delete(run.workflow_id), execution_options=self._pin)
                try:
                    await _running()._engine.check_owner(run)
                except _NOT_OWNER:
                    await txn.rollback()
                    return False
        return True

    async def run_tx_step_async(self, ds_options: DatasourceOptions | None, func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any) -> Any:
        name, isolation_level = _parse_ds_options(ds_options, func)
        if not inspect.iscoroutinefunction(func):
            raise DBOSException(f"Function {name} must be a coroutine function for AsyncSQLAlchemyDatasource")
        run = self._track()
        if run is None:
            return await self._transact(None, isolation_level, func, args, kwargs)

        @functools.wraps(func)
        async def body(*call_args: Any, **call_kwargs: Any) -> Any:
            return await self._transact(run, isolation_level, func, call_args, call_kwargs)

        return await _internal_step({"name": name}, body)(*args, **kwargs)

    def transaction(self, func: Callable[..., Any] | None = None, *, name: str | None = None, isolation_level: IsolationLevel = "SERIALIZABLE") -> Any:
        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            if not inspect.iscoroutinefunction(f):
                raise DBOSException(f"AsyncSQLAlchemyDatasource.transaction requires a coroutine function, but {f.__qualname__} is not")
            ds_options: DatasourceOptions = {"isolation_level": isolation_level}
            if name is not None:
                ds_options["name"] = name

            @functools.wraps(f)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self.run_tx_step_async(ds_options, f, *args, **kwargs)

            return wrapper

        return decorator(func) if func is not None else decorator


def drop_checkpoints(run: _native.Run, used: list[Any]) -> None:
    """Drop a finishing sync workflow's checkpoints while it still owns the workflow. Best effort: a leftover row is harmless."""
    for ds in used:
        if isinstance(ds, AsyncSQLAlchemyDatasource):
            continue  # Its connections belong to an event loop this workflow does not run on.
        try:
            owned = ds._drop_checkpoints(run)
        except Exception as error:  # noqa: BLE001
            logger.warning(f"Failed to delete datasource checkpoints of workflow {run.workflow_id}: {error}")
            continue
        if not owned:
            return  # Another execution owns the workflow now, and its checkpoints are its own.


async def drop_checkpoints_async(run: _native.Run, used: list[Any]) -> None:
    for ds in used:
        try:
            if isinstance(ds, AsyncSQLAlchemyDatasource):
                owned = await ds._drop_checkpoints(run)
            else:
                owned = await asyncio.to_thread(ds._drop_checkpoints, run)
        except Exception as error:  # noqa: BLE001
            logger.warning(f"Failed to delete datasource checkpoints of workflow {run.workflow_id}: {error}")
            continue
        if not owned:
            return
