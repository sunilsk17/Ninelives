"""Public exceptions."""

from __future__ import annotations

from typing import Any


class NinelivesError(Exception):
    """Base class for Ninelives errors."""


class PermanentError(NinelivesError):
    """Raise from a step to fail it immediately, without retries."""


class StepFailed(NinelivesError):
    """A step exhausted its retries (or raised PermanentError). Raised inside the workflow."""

    def __init__(self, step: str, error: dict[str, Any]):
        self.step = step
        self.error = error
        super().__init__(f"step {step!r} failed: {error.get('type')}: {error.get('message')}")


class SignalTimeout(NinelivesError):
    """wait_for_signal timed out. Raised inside the workflow."""


class Cancelled(NinelivesError):
    """The workflow was cancelled. Raised inside the workflow at the next durable operation."""


class NonDeterminismError(NinelivesError):
    """Workflow code took a different path than the journal recorded."""


class LeaseLost(NinelivesError):
    """This worker no longer owns the workflow (another worker took over). Internal."""


class WorkflowNotFound(NinelivesError):
    pass


class UnknownWorkflow(NinelivesError):
    """No workflow with this name is registered in this process."""


class WorkflowFailed(NinelivesError):
    """Raised by Ninelives.result() when the workflow failed or was cancelled."""

    def __init__(self, workflow_id: str, status: str, error: dict[str, Any] | None):
        self.workflow_id = workflow_id
        self.status = status
        self.error = error
        detail = f"{error.get('type')}: {error.get('message')}" if error else status
        super().__init__(f"workflow {workflow_id} {status.lower()}: {detail}")


class SerializationError(NinelivesError):
    pass
