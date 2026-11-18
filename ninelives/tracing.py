"""OpenTelemetry spans, if ``opentelemetry-api`` is installed (``pip install 'ninelives[otel]'``).

Ninelives only creates spans; where they go is decided by your application's OpenTelemetry setup
(an SDK ``TracerProvider`` with an exporter). Without one, spans are no-ops.

* ``ninelives.run <workflow>`` - one execution of a run by a worker (a run that sleeps or crashes
  has several), with the outcome: succeeded, failed, cancelled, waiting or abandoned.
* ``ninelives.step <step>`` - one attempt of a step, with its number and any error. Steps answered
  from the journal during replay do not execute and produce no span.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

try:
    from opentelemetry import trace as _trace
    from opentelemetry.trace import Status as _Status
    from opentelemetry.trace import StatusCode as _StatusCode
except ImportError:  # pragma: no cover - exercised only without the extra
    _trace = None  # type: ignore[assignment]

ENABLED = _trace is not None


class _NoSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def record_exception(self, exc: BaseException) -> None:
        pass

    def set_status(self, *args: Any, **kwargs: Any) -> None:
        pass


@contextlib.contextmanager
def span(name: str, attributes: dict[str, Any]) -> Iterator[Any]:
    """A span named ``name``; ``set_error`` / ``set_attribute`` work whether or not OTel is present."""
    if _trace is None:
        yield _NoSpan()
        return
    tracer = _trace.get_tracer("ninelives")
    with tracer.start_as_current_span(name, attributes=attributes, record_exception=False) as current:
        yield current


def set_error(current: Any, error: BaseException | str) -> None:
    if _trace is None:
        return
    if isinstance(error, BaseException):
        current.record_exception(error)
        message = f"{type(error).__name__}: {error}"
    else:
        message = error
    current.set_status(_Status(_StatusCode.ERROR, message))
