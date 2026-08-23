from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from smoking_data.core.dataset_compare import compare_parquet_datasets
from smoking_data.core.results import to_json_safe, utc_now_iso
from smoking_data.runtime.paths import ensure_dir


def write_parity_report(
    *,
    left_path: str | Path,
    right_path: str | Path,
    report_path: str | Path,
    label: str,
    sample_rows: int = 1000,
    left_metadata_path: str | Path | None = None,
    right_metadata_path: str | Path | None = None,
) -> Path:
    comparison = compare_parquet_datasets(left_path, right_path, sample_rows=sample_rows)
    path = Path(report_path).expanduser().resolve()
    ensure_dir(path.parent)
    payload: dict[str, Any] = {
        "created_at": utc_now_iso(),
        "label": label,
        "comparison": comparison.to_dict(),
    }
    if left_metadata_path and right_metadata_path:
        payload["metadata_comparison"] = compare_metadata_files(
            left_metadata_path,
            right_metadata_path,
        )
    path.write_text(
        json.dumps(to_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return path


def read_parity_report_ok(report_path: str | Path) -> bool:
    payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
    comparison = payload.get("comparison")
    metadata_comparison = payload.get("metadata_comparison")
    metadata_ok = not isinstance(metadata_comparison, dict) or metadata_comparison.get("ok") is True
    return bool(isinstance(comparison, dict) and comparison.get("ok") is True and metadata_ok)


def compare_metadata_files(left_path: str | Path, right_path: str | Path) -> dict[str, Any]:
    left = summarize_metadata_file(left_path)
    right = summarize_metadata_file(right_path)
    matches = {
        "preset_match": left.get("preset") == right.get("preset"),
        "job_name_match": left.get("job_name") == right.get("job_name"),
        "ok_match": left.get("ok") == right.get("ok"),
        "counter_keys_match": left.get("counter_keys") == right.get("counter_keys"),
    }
    return {
        "ok": all(matches.values()),
        **matches,
        "left": left,
        "right": right,
    }


def summarize_metadata_file(path: str | Path) -> dict[str, Any]:
    metadata_path = Path(path).expanduser().resolve()
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    result = payload.get("result") if isinstance(payload, dict) else None
    result = result if isinstance(result, dict) else {}
    counters = result.get("counters")
    counters = counters if isinstance(counters, dict) else {}
    return {
        "path": str(metadata_path),
        "preset": payload.get("preset") if isinstance(payload, dict) else None,
        "job_name": payload.get("job_name") if isinstance(payload, dict) else None,
        "yaml_hash": payload.get("yaml_hash") if isinstance(payload, dict) else None,
        "ok": result.get("ok"),
        "counter_keys": sorted(str(key) for key in counters),
        "output_path_count": len(result.get("output_paths") or []),
    }
