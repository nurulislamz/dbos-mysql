# Pydantic AI

`YadoDurability` makes a [Pydantic AI](https://ai.pydantic.dev) agent durable. Call `agent.run()` inside your own workflow. Each model request and each tool call becomes a step, so a crash resumes the agent without paying for the same model call twice.

```sh
uv pip install "yado[pydantic-ai]"
```

```python
from pydantic_ai import Agent
import yado
from yado.pydantic_ai import YadoDurability

agent = Agent("openai:gpt-5", capabilities=[YadoDurability()])


@yado.workflow()
async def answer(question: str) -> str:
    result = await agent.run(question)
    return result.output
```

Outside a workflow the same agent runs normally, and nothing is saved. `agent.run_sync()` works inside a sync workflow.

Pydantic AI's own `DBOSDurability` does not work with this package, because it imports the official `dbos` library. `YadoDurability` has the same shape and uses only the public yado API.

## What becomes a step

| Part of the run | Step name |
|---|---|
| Model request | `<agent>__model.request`, or `<agent>__model.request_stream` with an event handler |
| Function tool call | `<agent>__function_toolset__<toolset id>.call_tool:<tool>` |
| MCP or dynamic toolset call | `<agent>__mcp_server__<id>.call_tool` or `<agent>__dynamic_toolset__<id>.call_tool:<tool>` |
| Event handler, for each event | `<agent>__event_stream_handler` |
| A wait in the agent loop | `yado.sleep` |

`<agent>` is the agent's `name`, then the capability's `name=`, then `agent`. The names are only labels, so two agents can share one. A replay that reaches a different step name stops with an error rather than handing one tool another tool's result.

Parallel tool calls each reserve a step ID when they are checked, in call order, before any of them runs. A tool that finishes first still saves under its own ID, so recovery gives every call its own result.

## Options

```python
YadoDurability(
    model_step_config={"retries_allowed": True, "max_attempts": 3},
    tool_step_config={"retries_allowed": True},
    event_stream_handler=handler,
    parallel_execution_mode="sequential",
)
```

| Option | What it does |
|---|---|
| `models` | Extra models by ID, for switching models at run time. A `Model` object used at run time must be listed here |
| `event_stream_handler` | Model events reach it live inside the model step. Every other event runs it in its own step |
| `name` | Prefix of the step names |
| `model_step_config`, `tool_step_config`, `mcp_step_config`, `event_stream_handler_step_config` | Step options, as `@yado.step()` takes them |
| `parallel_execution_mode` | `"parallel_ordered_events"` (the default) or `"sequential"`. A run with a workspace always runs tools one at a time |

## Things to know

| Topic | Behaviour |
|---|---|
| At least once | A model request or tool call can run again if the process dies before its step is saved |
| Cancel | A step that begins after a cancel raises `DBOSWorkflowCancelledError`, and the agent run passes it through. An async workflow on the same process is usually stopped sooner, with `asyncio.CancelledError` |
| Stored results | Model responses and tool results are saved as JSON values, under either serializer. A tool that returns a dataclass gets a `dict` back in the message history, on the first run and on replay alike |
| Events from inside a tool | They are not sent again when the tool's step replays |
| Toolsets passed to `agent.run(toolsets=...)` | Refused inside a workflow, because their calls would not be steps |
| Tested | Function tools, the event handler, durable waits, recovery and cancel, on SQLite, MySQL and Postgres. MCP and dynamic toolsets use the same path but are not tested here |
