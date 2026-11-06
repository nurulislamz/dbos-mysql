import sys

import pytest

from yado import DBOS, DBOSConfig, PropagateOtelContext


@pytest.fixture(scope="module")
def exporter() -> object:
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    memory = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(memory))
    trace.set_tracer_provider(provider)  # only the first provider set in a process takes effect
    return memory


def test_tracing_is_a_no_op_when_opentelemetry_is_missing(config: DBOSConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "opentelemetry", None)  # makes every import of it fail
    DBOS.destroy(destroy_registry=True)
    DBOS(config={**config, "enable_otlp": True})
    DBOS.launch()
    try:

        @DBOS.step()
        def step() -> bool:
            return DBOS.span is None

        @DBOS.workflow()
        def flow() -> bool:
            with PropagateOtelContext():
                return step() and DBOS.span is None

        assert flow() is True
    finally:
        DBOS.destroy(destroy_registry=True)


def test_workflow_and_step_spans_join_the_callers_trace(config: DBOSConfig, exporter: object) -> None:
    from opentelemetry import trace

    DBOS.destroy(destroy_registry=True)
    DBOS(config={**config, "enable_otlp": True})
    DBOS.launch()
    try:

        @DBOS.step()
        def step(value: int) -> int:
            assert DBOS.span is not None
            return value + 1

        @DBOS.workflow()
        def child(value: int) -> int:
            return step(value)

        exporter.clear()  # type: ignore[attr-defined]
        with trace.get_tracer("test").start_as_current_span("request") as request:
            # Started on another thread, so only the stored carrier can give the workflow its parent.
            with PropagateOtelContext():
                handle = DBOS.start_workflow(child, 1)
            assert handle.get_result() == 2
        spans = {span.name: span for span in exporter.get_finished_spans()}  # type: ignore[attr-defined]
        workflow_span = spans[child.__qualname__]
        step_span = spans[step.__qualname__]
        assert workflow_span.parent is not None and workflow_span.parent.span_id == request.get_span_context().span_id
        assert workflow_span.context.trace_id == request.get_span_context().trace_id
        assert step_span.parent is not None and step_span.parent.span_id == workflow_span.context.span_id
        assert "dbos.operation.workflow_id" in workflow_span.attributes
    finally:
        DBOS.destroy(destroy_registry=True)
