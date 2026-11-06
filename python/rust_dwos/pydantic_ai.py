"""Pydantic AI durability on rust-dwos. Inside a workflow, each model request and each tool call is a step."""

from __future__ import annotations

import copy
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from contextlib import nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, TypedDict

from pydantic_ai._agent_graph import set_agent_graph_sleep
from pydantic_ai.agent import EventStreamHandler
from pydantic_ai.agent.abstract import AbstractAgent
from pydantic_ai.capabilities.abstract import AbstractCapability, WrapRunHandler
from pydantic_ai.durable_exec import (
    JSON_CODEC,
    BaseDurabilityCapability,
    DurabilityEngineSpec,
    DurableOperationId,
    EventStreamHandlerId,
    JournalCallableOperationBackend,
    RoleBasedOperationConfig,
    ToolsetCallToolId,
    conversation_id_from_run_id,
)
from pydantic_ai.messages import AgentStreamEvent, ToolCallPart
from pydantic_ai.models import Model
from pydantic_ai.run import AgentRunResult
from pydantic_ai.tools import AgentDepsT, RunContext, ToolDefinition

from rust_dwos.dbos import DBOS
from rust_dwos.errors import DBOSWorkflowCancelledError
from rust_dwos.step_reservation import StepReservation

__all__ = ["DwosDurability", "StepConfig"]

DwosParallelExecutionMode = Literal["sequential", "parallel_ordered_events"]
"""'parallel' is left out: its events reach the handler in completion order, which a replay cannot repeat."""

# Step names are labels in rust-dwos, not registrations, so an unnamed agent can share this one.
_DEFAULT_NAME = "agent"

# The reservation an event-handler step runs on, set only around that one dispatch.
_event_reservation: ContextVar[StepReservation | None] = ContextVar("rust_dwos_pydantic_ai_event_reservation", default=None)


class StepConfig(TypedDict, total=False):
    """Step options, as `@DBOS.step()` takes them."""

    retries_allowed: bool
    interval_seconds: float
    max_attempts: int
    backoff_rate: float


class _StepBackend(JournalCallableOperationBackend[StepConfig]):
    def __init__(self, durability: DwosDurability[Any], config: RoleBasedOperationConfig[StepConfig]) -> None:
        super().__init__(agent_name=durability.name, default_model_id=durability.default_model_id, config=config)
        self._durability = durability

    async def execute(self, *, operation_id: DurableOperationId, name: str, body: Callable[[], Awaitable[object]], cache_key: tuple[object, ...], config: StepConfig) -> object:
        options = {"name": name, **(config or {})}
        reservation = self._durability._take_reservation(operation_id, cache_key)
        if reservation is None:
            return await DBOS.run_step_async(options, body)
        return await reservation.run_async(options, body)


@dataclass(init=False, kw_only=True)
class DwosDurability(BaseDurabilityCapability[AgentDepsT]):
    """Makes an agent durable when `agent.run()` is called inside a `@DBOS.workflow()`.

    Each model request, tool call and event-handler call is a step. Outside a workflow the agent runs
    normally and nothing is recorded.
    """

    engine_spec = DurabilityEngineSpec(
        engine_name="rust-dwos",
        durable_unit_noun="step",
        durable_unit_plural="steps",
        durable_container_noun="workflow",
        # JSON-ready payloads store under both the pickle and the JSON serializer.
        codec=JSON_CODEC,
        # A per-run toolset was never wrapped, so its calls would run outside a step.
        unsupported_runtime_toolset_kinds=frozenset({"function", "mcp", "dynamic"}),
        wrapped_toolset_kinds=frozenset({"function", "mcp", "dynamic"}),
        toolset_lifecycles={"function": "enter-always", "mcp": "enter-in-durable-unit", "dynamic": "enter-never"},
        tool_call_result_upgrade_lenient=False,
        journal_discovery=True,
        sequential_tools_in_durable_context=False,
        tool_config_key=None,
        cancellation_error_types=(DBOSWorkflowCancelledError,),
    )

    def __init__(
        self,
        *,
        models: Mapping[str, Model] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        name: str | None = None,
        model_step_config: StepConfig | None = None,
        tool_step_config: StepConfig | None = None,
        mcp_step_config: StepConfig | None = None,
        event_stream_handler_step_config: StepConfig | None = None,
        parallel_execution_mode: DwosParallelExecutionMode = "parallel_ordered_events",
    ) -> None:
        """Create the capability. The agent's model, name and toolsets are found when it is attached.

        Args:
            models: Extra models by ID, for switching models at run time. A model crosses into a
                step as its ID, so a `Model` instance used at run time must be registered here.
            event_stream_handler: Model events reach it live inside the model-request step. Every
                other event is handled in its own step.
            name: Prefix of the step names. Defaults to the agent's name, then to `"agent"`.
            model_step_config: Step options for model requests.
            tool_step_config: Step options for function and dynamic tool calls.
            mcp_step_config: Step options for MCP server calls.
            event_stream_handler_step_config: Step options for event-handler steps.
            parallel_execution_mode: How tool calls run. Events keep their order in both modes.
                A run with a workspace always runs its tool calls one at a time.
        """
        super().__init__(models=models, event_stream_handler=event_stream_handler, name=name)
        self._model_step_config = model_step_config or {}
        self._tool_step_config = tool_step_config or {}
        self._mcp_step_config = mcp_step_config or {}
        self._event_stream_handler_step_config = event_stream_handler_step_config or {}
        self._parallel_execution_mode: DwosParallelExecutionMode = parallel_execution_mode
        self._backend: _StepBackend | None = None
        self._tool_steps: dict[tuple[str | None, str | None], dict[str, StepReservation]] = {}
        self._event_steps: dict[tuple[str | None, str | None], dict[int, deque[StepReservation | None]]] = {}

    def for_agent(self, agent: AbstractAgent[AgentDepsT, Any]) -> AbstractCapability[AgentDepsT]:
        if self.name or agent.name:
            return super().for_agent(agent)
        named = copy.copy(self)
        named.name = _DEFAULT_NAME
        return super(DwosDurability, named).for_agent(agent)

    def _bind_to_agent(self, agent: AbstractAgent[AgentDepsT, Any]) -> None:
        def tool_config(operation_id: DurableOperationId, tool: object | None, tool_name: str) -> StepConfig:
            return self._mcp_step_config if getattr(operation_id, "toolset_kind", None) == "mcp" else self._tool_step_config

        config = RoleBasedOperationConfig[StepConfig](
            model=self._model_step_config,
            event=self._event_stream_handler_step_config,
            capability={},
            tool=self._tool_step_config,
            resolve_tool=tool_config,
        )
        self._backend = _StepBackend(self, config)
        self._tool_steps = {}
        self._event_steps = {}
        super()._bind_to_agent(agent)

    def get_durable_operation_backend(self) -> _StepBackend:
        assert self._backend is not None
        return self._backend

    @property
    def in_durable_context(self) -> bool:
        """True in workflow code; false inside a step and outside any workflow."""
        return DBOS.workflow_id is not None and DBOS.step_id is None

    def get_default_run_id(self) -> str | None:
        if not self.in_durable_context:
            return None
        # A reservation replays in the same order on recovery, and runs started together get different IDs.
        return f"{DBOS.workflow_id}:{DBOS.reserve_step().step_id}"

    def get_default_conversation_id(self, run_id: str) -> str | None:
        return conversation_id_from_run_id(run_id) if self.in_durable_context else None

    async def get_run_start_time(self, run_id: str) -> datetime | None:
        if not self.in_durable_context:
            return None

        # A step, so a recovered run counts its `timeout=` from when it first started.
        async def run_start_time() -> datetime:
            return datetime.now(UTC)

        return await DBOS.run_step_async({"name": f"{self.name}.run_start_time"}, run_start_time)

    async def after_tool_validate(self, ctx: RunContext[AgentDepsT], *, call: ToolCallPart, tool_def: ToolDefinition, args: Any) -> Any:
        # Calls are validated one by one in call order, before any runs, so parallel calls take their steps in that order.
        steps = self._tool_steps.get((DBOS.workflow_id, ctx.run_id))
        if steps is not None and self.in_durable_context:
            steps[call.tool_call_id] = DBOS.reserve_step()
        return args

    def _prepare_event_dispatch(self, ctx: RunContext[AgentDepsT], event: AgentStreamEvent) -> None:
        events = self._event_steps.get((DBOS.workflow_id, ctx.run_id))
        if events is None:
            return
        # A tool's event is reserved where it is emitted; its handler step runs later, at stream position.
        reservation = DBOS.reserve_step() if ctx.tool_call_id is not None and self.in_durable_context else None
        events.setdefault(id(event), deque()).append(reservation)

    async def _dispatch_event_stream_event(self, ctx: RunContext[AgentDepsT], event: AgentStreamEvent) -> None:
        reservation = None
        events = self._event_steps.get((DBOS.workflow_id, ctx.run_id))
        if events is not None and (pending := events.get(id(event))):
            reservation = pending.popleft()
            if not pending:
                del events[id(event)]
        token = _event_reservation.set(reservation)
        try:
            await super()._dispatch_event_stream_event(ctx, event)
        finally:
            _event_reservation.reset(token)

    def _take_reservation(self, operation_id: DurableOperationId, cache_key: tuple[object, ...]) -> StepReservation | None:
        if isinstance(operation_id, EventStreamHandlerId):
            reservation = _event_reservation.get()
            _event_reservation.set(None)
            return reservation
        if not isinstance(operation_id, ToolsetCallToolId):
            return None
        ctx = next((value for value in cache_key if isinstance(value, RunContext)), None)
        if ctx is None or ctx.tool_call_id is None:
            return None
        return self._tool_steps.get((DBOS.workflow_id, ctx.run_id), {}).pop(ctx.tool_call_id, None)

    async def wrap_run(self, ctx: RunContext[AgentDepsT], *, handler: WrapRunHandler) -> AgentRunResult[Any]:
        agent = self._agent
        if agent is None:
            return await handler()
        durable = self.in_durable_context
        # One step per workspace call: parallel tools could reach them in a different order on replay.
        mode = "sequential" if durable and ctx.workspace.attached else self._parallel_execution_mode
        key = (DBOS.workflow_id, ctx.run_id)
        if durable:
            self._tool_steps[key] = {}
            if self._event_stream_handler is not None:
                self._event_steps[key] = {}
        try:
            with agent.parallel_tool_call_execution_mode(mode), set_agent_graph_sleep(DBOS.sleep_async) if durable else nullcontext():
                return await handler()
        finally:
            if durable:
                self._tool_steps.pop(key, None)
                self._event_steps.pop(key, None)
