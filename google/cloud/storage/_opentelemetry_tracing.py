"""Manages OpenTelemetry tracing span creation and handling."""

import logging
import os
from contextlib import contextmanager
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.storage import __version__

DISABLE_OTEL_TRACES_ENV_VAR = "DISABLE_GCS_PYTHON_CLIENT_OTEL_TRACES"
_DEFAULT_DISABLE_OTEL_TRACES_VALUE = False

disable_otel_traces = os.environ.get(
    DISABLE_OTEL_TRACES_ENV_VAR, _DEFAULT_DISABLE_OTEL_TRACES_VALUE
)
logger = logging.getLogger(__name__)

try:
    from opentelemetry import trace

    HAS_OPENTELEMETRY = True

    logger.info(f"HAS_OPENTELEMETRY is {HAS_OPENTELEMETRY}")
except ImportError:
    logger.debug(
        "This service is instrumented using OpenTelemetry. "
        "OpenTelemetry or one of its components could not be imported; "
        "please add compatible versions of opentelemetry-api and "
        "opentelemetry-instrumentation packages in order to get Storage "
        "Tracing data."
    )
    HAS_OPENTELEMETRY = False

_default_attributes = {
    "rpc.service": "CloudStorage",
    "rpc.system": "http",
    "user_agent.original": f"gcloud-python/{__version__}",
    "curr.stat": "prototyping",  # TODO: remove prototype
}


@contextmanager
def create_span(name, attributes=None, client=None, **kwargs):
    """Creates a context manager for a new span and set it as the current span
    in the configured tracer. If no configuration exists yields None."""
    if not HAS_OPENTELEMETRY or disable_otel_traces:
        print(f"HAS_OPENTELEMETRY is {HAS_OPENTELEMETRY}")
        print(f"disable_otel_traces is {disable_otel_traces}")
        yield None
        return

    tracer = trace.get_tracer(__name__)
    final_attributes = _get_final_attributes(attributes, client)
    # Yield new span.
    with tracer.start_as_current_span(
        name=name, kind=trace.SpanKind.CLIENT, attributes=final_attributes
    ) as span:
        try:
            yield span
        except GoogleAPICallError as error:
            span.set_status(trace.Status(trace.StatusCode.ERROR))
            span.record_exception(error)
            raise


def _get_final_attributes(attributes=None, client=None):
    collected_attr = _default_attributes.copy()
    if client:
        collected_attr.update(_set_client_attributes(client))
    if attributes:
        collected_attr.update(attributes)
    final_attributes = {k: v for k, v in collected_attr.items() if v is not None}
    return final_attributes


def _set_client_attributes(client):
    attr = {
        "url": client._connection.API_BASE_URL,
        "user_agent.original": client._connection._client_info.user_agent,
    }
    if client.project is not None:
        attr["project"] = client.project
    return attr


# Alternatives Considered
# Alt 1 - explores the possibility of using a tracer provider instance set on the Client.
# @contextmanager
# def create_span(name, attributes=None, client=None, **kwargs):
#     """Creates a context manager for a new span and set it as the current span
#     in the configured tracer. If no configuration exists yields None."""
#     if not HAS_OPENTELEMETRY or disable_otel_traces:
#         yield None
#         return

#     # (1) A tracer provider instance explicity set on a client, if any.
#     # (2) Fall back to the global tracer provider.
#     tracer_provider = client.get_opentelemetry_tracer_provider()
#     print(f"!!!! check 1 tracer provider is {tracer_provider}")
#     if tracer_provider is None:
#         tracer = trace.get_tracer(__name__)
#     else:
#         tracer = tracer_provider.get_tracer(__name__)
#     print(f"!!!! check 2 tracer is {tracer}")
#     final_attributes = _get_final_attributes(attributes, client)

#     # Yield new span.
#     with tracer.start_as_current_span(
#         name=name, kind=trace.SpanKind.CLIENT, attributes=final_attributes
#     ) as span:
#         try:
#             span.set_status(trace.Status(trace.StatusCode.OK))
#             yield span
#         except GoogleAPICallError as error:
#             span.set_status(trace.Status(trace.StatusCode.ERROR))
#             span.record_exception(error)
#             raise

# # Alt 2 - BaseInstrumentor pattern that allows a tracer provider to be
# # passed in upon instrument call.
# class OtelTraceInstrumentor(ABC):
#     def init(self, tracer):
#         self.tracer = tracer

#     def instrument(tracer):
#         # Use the tracer provider instance passed in.
