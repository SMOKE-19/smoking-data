"""Read-only pipeline workload-quality recommendations."""

from .advisor import PwqHandle, advise_pipeline

__all__ = ["PwqHandle", "advise_pipeline"]
