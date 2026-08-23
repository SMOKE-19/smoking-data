from collections.abc import Mapping

__version__: str

def execute_join_task(task_json: str) -> dict[str, float]: ...
def join_backend_capabilities() -> list[str]: ...
def validate_dataset(
    parquet_paths: list[str], assertion_config_json: str, spill_dir: str
) -> str: ...
def validate_expression_ir(ir_json: str) -> str: ...
def supported_expression_functions() -> list[str]: ...
def plan_coordinates(
    input_parquet_paths: list[str],
    coord_output_dir: str,
    filter_config: Mapping[str, object] | None = ...,
    planner_config: Mapping[str, object] | None = ...,
) -> dict[str, float]: ...
def restore_parquet_to_parquet(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
    drop_cache_hint: bool = ...,
    print_timing: bool = ...,
) -> dict[str, float]: ...
def restore_parquet_to_parquet_profiled(
    input_parquet_path: str,
    output_parquet_path: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
    drop_cache_hint: bool = ...,
) -> dict[str, float]: ...
def execute_curated_task(
    coord_path: str,
    output_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    writer_config: Mapping[str, object] | None = ...,
    batch_size: int | None = ...,
    drop_cache_hint: bool = ...,
    print_timing: bool = ...,
) -> dict[str, float]: ...
def restore_dataset_to_dataset(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
    max_workers: int = ...,
    drop_cache_hint: bool = ...,
    print_timing: bool = ...,
) -> dict[str, float]: ...
def restore_dataset_to_dataset_profiled(
    input_dataset_dir: str,
    output_dataset_dir: str,
    lookup_path: str,
    schema: dict[str, str],
    config: Mapping[str, object],
    batch_size: int | None = ...,
    max_workers: int = ...,
    drop_cache_hint: bool = ...,
) -> dict[str, object]: ...
