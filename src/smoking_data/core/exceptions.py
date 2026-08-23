from __future__ import annotations

from typing import Any


class SmokingDataError(Exception):
    """Base exception for public runtime failures."""

    code = "smoking_data_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code or type(self).code
        self.context = dict(context or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type": type(self).__name__,
            "error_code": self.code,
            "error_message": str(self),
            "context": self.context,
        }


class ValidationError(SmokingDataError):
    """Raised when a YAML contract or runtime argument is invalid."""

    code = "validation_error"


class ConfigError(SmokingDataError):
    """Raised when runtime config cannot be loaded."""

    code = "config_error"


class TaskExecutionError(SmokingDataError, RuntimeError):
    """Raised by the parent when a child task reports or suffers a failure."""

    code = "task_execution_error"
