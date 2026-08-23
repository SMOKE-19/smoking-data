from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from smoking_sbdf import SbdfConversionResult


@dataclass(frozen=True, slots=True)
class SbdfExportRequest:
    parquet_files: list[Path] = field(default_factory=list)
    dataset_path: Path | None = None
    sbdf_path: Path | None = None
    batch_size: int = 50_000
    column_types: dict[str, str] | None = None
    encoding_rle: bool = True
    adaptive_encoding: bool = False
    workers: int = 1
    adaptive_workers: bool = False
    recursive: bool = True
    row_key_columns: list[str] | None = None
    sidecar_path: Path | None = None
    table_id: str | None = None


def export_sbdf(request: SbdfExportRequest) -> Path:
    return export_sbdf_with_result(request).output_path


def export_sbdf_with_result(request: SbdfExportRequest) -> SbdfConversionResult:
    if request.sbdf_path is None:
        raise ValueError("sbdf_path is required.")
    from smoking_sbdf import convert_with_result

    options = {
        "batch_size": request.batch_size,
        "column_types": request.column_types,
        "encoding_rle": request.encoding_rle,
        "adaptive_encoding": request.adaptive_encoding,
        "workers": request.workers,
        "adaptive_workers": request.adaptive_workers,
        "row_key_columns": request.row_key_columns,
        "sidecar_path": request.sidecar_path,
        "table_id": request.table_id,
    }

    if request.dataset_path is not None:
        return convert_with_result(
            request.dataset_path,
            request.sbdf_path,
            input_format="parquet-dataset",
            recursive=request.recursive,
            **options,
        )
    if request.parquet_files:
        output_parent = request.sbdf_path.parent
        output_parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".smoking-sbdf-manifest-", dir=output_parent) as temp_dir:
            manifest_path = Path(temp_dir) / "inputs.manifest"
            manifest_path.write_text(
                "".join(f"{_manifest_entry(path)}\n" for path in request.parquet_files),
                encoding="utf-8",
            )
            return convert_with_result(
                manifest_path,
                request.sbdf_path,
                input_format="parquet-manifest",
                **options,
            )
    raise ValueError("Either dataset_path or parquet_files is required.")


def _manifest_entry(path: Path) -> str:
    value = str(path.resolve())
    if "\n" in value or "\r" in value:
        raise ValueError(f"Parquet path cannot contain a newline: {path}")
    return value
