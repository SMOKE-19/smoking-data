"""0102 Calculated Fact Asset contracts and execution."""

from .binding import BindingPlan, build_binding_plan
from .contract import (
    LONG_FACT_CONTRACT_VERSION,
    VALUE_LANES,
    long_fact_schema,
    validate_long_fact_batch,
)
from .coordinates import (
    CoordinateBatch,
    SourceCoordinate,
    load_coordinate_batch,
)
from .external_files import compile_expression_file, load_column_alias_registry
from .fingerprint import ExpressionFingerprintSpec, derive_expression_fingerprint_specs
from .invalidation import (
    InvalidationTaskGroup,
    select_projected_rows,
    subset_expression_ir,
    write_coordinate_subset,
)
from .planner import (
    ExpressionExecutionPlan,
    ListExecutionStrategy,
    plan_expression_execution,
)
from .planning import (
    CALCULATED_FACT_PLAN_VERSION,
    CalculatedFactRunPlan,
    build_calculated_fact_plan,
    preflight_calculated_fact_yaml,
)
from .segment_append import SegmentAppendedGeneration, SegmentAppendTransaction
from .segment_runner import run_yaml
from .spec import CalculatedFactSpec, load_calculated_fact_spec
from .task import build_calculated_fact_task_request, build_long_fact_writer_config

__all__ = [
    "BindingPlan",
    "CalculatedFactSpec",
    "CalculatedFactRunPlan",
    "CoordinateBatch",
    "SourceCoordinate",
    "SegmentAppendTransaction",
    "SegmentAppendedGeneration",
    "CALCULATED_FACT_PLAN_VERSION",
    "ExpressionExecutionPlan",
    "ExpressionFingerprintSpec",
    "InvalidationTaskGroup",
    "ListExecutionStrategy",
    "LONG_FACT_CONTRACT_VERSION",
    "VALUE_LANES",
    "build_binding_plan",
    "build_calculated_fact_plan",
    "build_long_fact_writer_config",
    "build_calculated_fact_task_request",
    "derive_expression_fingerprint_specs",
    "compile_expression_file",
    "load_calculated_fact_spec",
    "load_column_alias_registry",
    "load_coordinate_batch",
    "long_fact_schema",
    "plan_expression_execution",
    "preflight_calculated_fact_yaml",
    "run_yaml",
    "select_projected_rows",
    "subset_expression_ir",
    "write_coordinate_subset",
    "validate_long_fact_batch",
]
