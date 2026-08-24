"""Transport adapter protocol for SOURCE 0101.

The core only discovers an installed adapter by identifier. Adapter implementation,
credentials, and vendor-specific execution remain outside the product package.
"""

from __future__ import annotations

from collections.abc import Mapping
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Protocol

from .pipeline.task import SourceTask


class SourceAdapter(Protocol):
    def prepare_run(
        self,
        *,
        options: Mapping[str, Any],
        project_root: Path,
        temp_root: Path,
    ) -> None: ...

    def execute(
        self,
        *,
        sql_text: str,
        output_file: Path,
        task: SourceTask,
        writer_options: Mapping[str, Any],
    ) -> Any: ...


def load_source_adapter(name: str) -> SourceAdapter:
    adapter_name = str(name or "").strip()
    if not adapter_name:
        raise ValueError("SOURCE 0101 adapter name is required.")
    matches = entry_points(group="smoking_data.source_adapters", name=adapter_name)
    match = next(iter(matches), None)
    if match is None:
        raise RuntimeError(
            f"SOURCE 0101 adapter {adapter_name!r} is not installed. "
            f"Install the package providing the '{adapter_name}' source adapter."
        )
    loaded = match.load()
    adapter = loaded() if callable(loaded) else loaded
    if not hasattr(adapter, "prepare_run") or not hasattr(adapter, "execute"):
        raise TypeError(
            f"SOURCE 0101 adapter {adapter_name!r} does not implement the source adapter protocol."
        )
    return adapter
