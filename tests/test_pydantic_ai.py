import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent  # noqa: E402
from pydantic_ai._agent_graph import _agent_graph_sleep  # noqa: E402
from pydantic_ai.capabilities import AbstractCapability  # noqa: E402
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart, ToolReturnPart  # noqa: E402
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, FunctionModel  # noqa: E402

from yado import DBOS, DBOSAwaitedWorkflowCancelledError, DBOSWorkflowCancelledError  # noqa: E402
from yado.pydantic_ai import YadoDurability  # noqa: E402
from tests.conftest import Database, retry_until_success  # noqa: E402


def _tool_returns(messages: list[ModelMessage]) -> list[Any]:
    return [part.content for message in messages for part in message.parts if isinstance(part, ToolReturnPart)]


def _two_tools_then_text(calls: dict[str, int]) -> FunctionModel:
    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls["model"] += 1
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart("slow", {"x": 1}, tool_call_id="a"), ToolCallPart("fast", {"x": 2}, tool_call_id="b")])
        return ModelResponse(parts=[TextPart(f"done {_tool_returns(messages)}")])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[Any]:
        calls["model"] += 1
        if len(messages) == 1:
            yield {0: DeltaToolCall("slow", json_args='{"x": 1}', tool_call_id="a"), 1: DeltaToolCall("fast", json_args='{"x": 2}', tool_call_id="b")}
        else:
            yield f"done {_tool_returns(messages)}"

    return FunctionModel(respond, stream_function=stream)


class _SleepThenDelaySlowTool(AbstractCapability[Any]):
    """Waits through the agent graph's sleep, and holds back the first tool so the second one starts first."""

    async def before_model_request(self, ctx: Any, request_context: Any) -> Any:
        if ctx.run_step == 1:
            await _agent_graph_sleep(0.01)
        return request_context

    async def before_tool_execute(self, ctx: Any, *, call: ToolCallPart, tool_def: Any, args: Any) -> Any:
        if call.tool_name == "slow":
            await asyncio.sleep(0.3)
        return args


def _agent(calls: dict[str, int], **durability: Any) -> Agent[None, str]:
    agent = Agent(_two_tools_then_text(calls), name="writer", capabilities=[_SleepThenDelaySlowTool(), YadoDurability(**durability)])

    @agent.tool_plain
    async def slow(x: int) -> int:
        calls["slow"] += 1
        return x * 10

    @agent.tool_plain
    async def fast(x: int) -> int:
        calls["fast"] += 1
        return x * 100

    return agent


def test_agent_run_is_steps_and_recovery_reruns_none(dbos: DBOS, database: Database) -> None:
    calls = {"model": 0, "slow": 0, "fast": 0, "events": 0}

    async def handler(ctx: Any, events: AsyncIterator[Any]) -> None:
        async for _ in events:
            calls["events"] += 1

    agent = _agent(calls, event_stream_handler=handler)

    @DBOS.workflow()
    async def answer(question: str) -> str:
        return (await agent.run(question)).output

    handle = DBOS.start_workflow(answer, "hi")
    assert handle.get_result() == "done [10, 200]"
    recorded = DBOS.list_workflow_steps(handle.workflow_id)
    steps = {step["function_name"]: step["function_id"] for step in recorded}
    names = [step["function_name"] for step in recorded]
    assert names[:2] == ["DBOS.sleep", "writer__model.request_stream"]
    assert names[-1] == "writer__model.request_stream"
    assert names.count("writer__event_stream_handler") == 4
    # The fast tool runs first, but each call keeps the step reserved for it in call order.
    assert steps["writer__function_toolset__<agent>.call_tool:slow"] < steps["writer__function_toolset__<agent>.call_tool:fast"]
    assert calls["model"] == 2 and calls["slow"] == 1 and calls["fast"] == 1 and calls["events"] > 4

    before = dict(calls)
    database.expire_lease(handle.workflow_id)
    recovered = DBOS._recover_pending_workflows()
    assert [item.get_result() for item in recovered] == ["done [10, 200]"]
    assert calls == before


async def test_outside_a_workflow_nothing_is_recorded() -> None:
    DBOS.destroy(destroy_registry=True)
    calls = {"model": 0, "slow": 0, "fast": 0}
    # With no DBOS configured, any recording call would raise.
    assert (await _agent(calls).run("hi")).output == "done [10, 200]"
    assert calls == {"model": 2, "slow": 1, "fast": 1}


def test_cancel_reaches_the_workflow_as_dbos_workflow_cancelled_error(dbos: DBOS) -> None:
    seen: list[type[BaseException]] = []

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart("stop", {})])
        return ModelResponse(parts=[TextPart("not reached")])

    agent = Agent(FunctionModel(respond), name="stopper", capabilities=[YadoDurability()])

    @agent.tool_plain
    async def stop() -> str:
        workflow_id = DBOS.workflow_id
        assert workflow_id is not None
        await DBOS.cancel_workflow_async(workflow_id)
        return "stopped"

    # A sync workflow cannot be interrupted, so the cancel surfaces when the next step begins.
    @DBOS.workflow()
    def answer(question: str) -> str:
        try:
            return agent.run_sync(question).output
        except BaseException as error:
            seen.append(type(error))
            raise

    handle = DBOS.start_workflow(answer, "hi")
    with pytest.raises(DBOSAwaitedWorkflowCancelledError):
        handle.get_result()

    def cancelled_in_the_agent_run() -> None:
        assert seen == [DBOSWorkflowCancelledError]

    retry_until_success(cancelled_in_the_agent_run, interval=0.1, max_attempts=100)
