"""OpenTelemetry spans for workflows and steps.

Nothing here runs unless enable_otlp is set and opentelemetry imports. Otherwise every call is a no-op.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
from collections.abc import Iterator
from typing import Any

from rust_dwos.attributes import _attributes

logger = logging.getLogger("dbos")

# Reserved workflow attribute that carries the W3C trace context from PropagateOtelContext.
CARRIER_KEY = "dbos.otelContext"
_tracer: Any = None
_span: contextvars.ContextVar[Any] = contextvars.ContextVar("dbos_otel_span", default=None)


def _api() -> Any:
    try:
        from opentelemetry import trace
    except ImportError:
        return None
    return trace


def setup(enabled: bool, endpoints: list[str], resource: dict[str, str]) -> None:
    global _tracer
    _tracer = None
    if not enabled:
        return
    trace = _api()
    if trace is None:
        logger.warning("enable_otlp is set, but OpenTelemetry is not installed, so no spans are created. Install rust-dwos[otel]")
        return
    if not endpoints:
        _tracer = trace.get_tracer("dbos")
        return
    provider = _own_provider(endpoints, resource)
    _tracer = provider.get_tracer("dbos") if provider is not None else None


def _own_provider(endpoints: list[str], resource: dict[str, str]) -> Any:
    # DBOS's own provider, so spans reach these endpoints even when the app set no global provider.
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.warning("otlp_traces_endpoints is set, but the OTLP exporter is not installed, so no spans are sent. Install rust-dwos[otel]")
        return None
    provider = TracerProvider(resource=Resource.create(resource))
    for endpoint in endpoints:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    return provider


def current_span() -> Any:
    return _span.get()


def wants_parent() -> bool:
    # Only a run with no span of its own needs the trace context stored with its row.
    trace = _api()
    return _tracer is not None and trace is not None and not trace.get_current_span().get_span_context().is_valid


def parent_from(attributes: str | None) -> Any:
    # Reads the stored carrier. Anything malformed means no parent, never an error.
    trace = _api()
    if trace is None or not attributes:
        return None
    try:
        carrier = json.loads(attributes).get(CARRIER_KEY)
    except (AttributeError, ValueError):
        return None
    if not isinstance(carrier, dict):
        return None
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    context = TraceContextTextMapPropagator().extract(carrier)
    return context if trace.get_current_span(context).get_span_context().is_valid else None


@contextlib.contextmanager
def span(name: str, attributes: dict[str, str], parent: Any = None) -> Iterator[None]:
    if _tracer is None:
        yield
        return
    with _tracer.start_as_current_span(name, context=parent, attributes=attributes) as current:
        token = _span.set(current)
        try:
            yield
        finally:
            _span.reset(token)


class PropagateOtelContext:
    """Starts the workflows and steps in this block on the current trace. Nothing is recorded without an active span."""

    def __init__(self, context: Any = None) -> None:
        self._context = context
        self._token: contextvars.Token[str | None] | None = None

    def __enter__(self) -> PropagateOtelContext:
        trace = _api()
        if trace is None:
            return self
        from opentelemetry.context import get_current
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        context = self._context if self._context is not None else get_current()
        if not trace.get_current_span(context).get_span_context().is_valid:
            return self
        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier, context=context)
        stored = json.loads(_attributes.get() or "{}")
        self._token = _attributes.set(json.dumps({**stored, CARRIER_KEY: carrier}))
        return self

    def __exit__(self, *_: object) -> None:
        if self._token is not None:
            _attributes.reset(self._token)
            self._token = None
