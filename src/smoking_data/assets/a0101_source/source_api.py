"""SOURCE 0101 backend dispatch."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from .adapter_registry import load_source_adapter
from .http_source import call_http_source
from .pipeline.task import SourceTask


def call_data_api(
    sql_text: str,
    *,
    output_dir: str | Path,
    task: SourceTask,
) -> list[tuple[str, int]]:
    """Dispatch a Source task to built-in HTTP or an installed source adapter."""

    if task.query_mode in {"http_json", "http_ndjson", "http_xml"}:
        return call_http_source(output_dir=output_dir, task=task)

    adapter = load_source_adapter(task.adapter)
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    sql_revision = task.sql_revision.strip()
    if not sql_revision:
        raise ValueError("SOURCE task.sql_revision must be populated before backend execution.")
    output_file = resolved_output_dir / f"data_0001_{sql_revision}.parquet"
    adapter.execute(
        sql_text=sql_text,
        output_file=output_file,
        task=task,
        writer_options=dict(task.parquet_writer_options or {}),
    )
    # SPI decorators may emit one or more parts (for example,
    # ``part1-<filename>.parquet``) instead of using the requested name verbatim.
    # The staging orchestrator normalizes all reported parts before publication.
    parquet_parts = sorted(resolved_output_dir.glob("*.parquet"))
    if not parquet_parts:
        raise RuntimeError(
            f"SOURCE adapter did not create a parquet part in the staging directory: "
            f"{resolved_output_dir}"
        )
    return [
        (str(part), pq.ParquetFile(part).metadata.num_rows)
        for part in parquet_parts
    ]
