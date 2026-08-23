"""프로젝트 로컬 설정으로 구성되는 source query backend.

이 모듈은 통합 패키지의 Source backend 진입점이다. SQL mode는 설치 후 생성한
``.smoking-data/adapters.yaml``에서 query·decorator 연결을 구성하고, HTTP
JSON/NDJSON/XML mode는 내장 GET transport를 사용한다.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .adapter_config import AdapterConfig, load_adapter_config
from .http_source import call_http_source
from .pipeline.task import SourceTask


# Adapter 연결 정보와 Parquet 물리 정책은 변경 이유가 달라 별도 설정으로 유지한다.
# 외부에 보이는 작성 진입점은 ``pandas.DataFrame.to_parquet``이며 이 함수의 최종
# writer engine은 아래 ``engine`` 옵션이 결정한다. pandas 자체는 명시적인
# ``engine: pyarrow`` 호출에서 fastparquet를 import하지 않는다. 다만 운영 환경의
# decorator 내부 호출이나 별도 Parquet 경로에서 fastparquet 누락 오류가 확인돼
# 패키지 런타임 의존성에는 fastparquet도 포함한다.
def _resolve_parquet_writer_options(task: SourceTask) -> dict[str, Any]:
    if not isinstance(task.parquet_writer_options, dict):
        raise TypeError("task.parquet_writer_options must be a dict.")
    return dict(task.parquet_writer_options)


def _load_spi_backend(
    adapter: AdapterConfig,
) -> tuple[Callable[..., Any], Callable[..., Any]]:
    module_name = adapter.module
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise
        raise RuntimeError(
            f"The configured SOURCE 0101 adapter module is unavailable: {module_name!r}."
        ) from exc

    query_function = _require_callable(
        module,
        adapter.query_function,
        setting=f"adapters.{adapter.name}.query_function",
    )
    decorator_function = _require_callable(
        module,
        adapter.decorator_function,
        setting=f"adapters.{adapter.name}.decorator_function",
    )
    return query_function, decorator_function


def _require_callable(
    module: ModuleType,
    function_name: str,
    *,
    setting: str,
) -> Callable[..., Any]:
    value = getattr(module, function_name, None)
    if not callable(value):
        raise TypeError(
            f"Configured adapter callable {module.__name__}.{function_name} is missing or not "
            f"callable; check {setting}."
        )
    return value


def call_data_api(
    sql_text: str,
    *,
    output_dir: str | Path,
    task: SourceTask,
) -> list[tuple[str, int]]:
    """설정된 query와 decorator를 결합해 task 전용 Parquet dataset에 기록한다.

    이 callback에 전달하는 기본 옵션은 PyArrow, Zstd, 1,000 rows/row-group이며
    페이지 인덱스와 통계를 기록한다. dictionary encoding은 기본적으로 끈다.

    ``task.parquet_writer_options``에는 YAML의 ``output.artifact.parquet_writer``가 전달된다.
    YAML 값을 지정하지 않으면 ``PARQUET_WRITER_OPTIONS`` 기본값을 사용한다.
    """

    if task.query_mode in {"http_json", "http_ndjson", "http_xml"}:
        return call_http_source(output_dir=output_dir, task=task)

    adapter_config_path = task.adapter_config_path.strip()
    if not adapter_config_path:
        raise RuntimeError(
            "SOURCE 0101 adapter config path is missing. Run 'smoking-data init .' first."
        )
    adapter = load_adapter_config(adapter_config_path)
    query_function, decorator_function = _load_spi_backend(adapter)
    parquet_writer_options = _resolve_parquet_writer_options(task)

    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    sql_revision = task.sql_revision.strip()
    if not sql_revision:
        raise ValueError("SOURCE task.sql_revision must be populated before backend execution.")
    output_file = resolved_output_dir / f"data_0001_{sql_revision}.parquet"
    decorated_call = decorator_function(
        query_function,
        pd.DataFrame.to_parquet,
        str(output_file),
        **parquet_writer_options,
    )
    decorated_call(sql_text, **adapter.call_options)
    if not output_file.is_file():
        raise RuntimeError(f"SOURCE adapter did not create the declared parquet file: {output_file}")
    return [(str(output_file), pq.ParquetFile(output_file).metadata.num_rows)]
