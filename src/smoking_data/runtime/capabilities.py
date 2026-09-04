from __future__ import annotations

from dataclasses import asdict
from typing import Any

from smoking_data import __version__
from smoking_data.core.engine_contract import engine_metadata
from smoking_data.core.operations import OPERATION_PROPERTIES
from smoking_data.core.pipeline_dag import (
    CURATED_PIPELINE_SCHEMA_VERSION,
    PIPELINE_SCHEMA_VERSION,
    SNAPSHOT_PIPELINE_SCHEMA_VERSION,
)
from spotfire_expr_normalizer.semantics import canonical_function_semantics

CAPABILITIES_SCHEMA_VERSION = "smoking-data.capabilities.v1"


def get_capabilities() -> dict[str, Any]:
    """Return the installed engine's machine-readable capability contract.

    This is introspection only: it does not load a Definition, inspect a dataset,
    write registry state, or execute a pipeline.
    """

    operations = {
        operation.value: {
            **asdict(properties),
            "backend_support": list(properties.backend_support),
            "pushdown_capabilities": list(properties.pushdown_capabilities),
        }
        for operation, properties in OPERATION_PROPERTIES.items()
    }
    expressions = {
        name: asdict(semantics)
        for name, semantics in canonical_function_semantics().items()
    }
    return {
        "schema_version": CAPABILITIES_SCHEMA_VERSION,
        "package_version": __version__,
        "engine": engine_metadata(),
        "asset_schemas": {
            "0101": "smoking-data.source.v5",
            "0102": "smoking-data.calculated-fact.v4",
            "0103": "smoking-data.csv-source.v1",
            "0201": CURATED_PIPELINE_SCHEMA_VERSION,
            "0301": PIPELINE_SCHEMA_VERSION,
            "0401": SNAPSHOT_PIPELINE_SCHEMA_VERSION,
            "chain": "smoking-data.asset-chain.v2",
        },
        "pipeline_schemas": [
            PIPELINE_SCHEMA_VERSION,
            CURATED_PIPELINE_SCHEMA_VERSION,
            SNAPSHOT_PIPELINE_SCHEMA_VERSION,
        ],
        "operations": operations,
        "execution_primitives": {
            "bounded_writer_pipeline": {
                "status": "supported",
                "backpressure": "bounded_batch_queue",
                "writer_state_ownership": "dedicated_writer_thread",
                "atomic_finalize": True,
                "python_api": "smoking_data.runtime.bounded_writer.BoundedWriterPipeline",
                "native_coordinate_api": (
                    "smoking_data_engine_rs.execute_coordinate_materialize_task"
                ),
                "asset_paths": {
                    "0102": "coordinate_materialize",
                    "0103": "route_parquet_writer",
                    "0201": "coordinate_materialize",
                    "0301": "bounded_join_writer",
                    "0401": "snapshot_compaction_writer",
                },
            }
        },
        "expression_features": expressions,
        "baseline_contracts": {
            "group_aggregate": {
                "status": "supported",
                "surface": "add_calc.expression_ir.window",
                "semantics": "partition aggregate without an explicit frame",
            },
            "partition_baseline": {
                "status": "supported",
                "surface": "add_calc.expression_ir.window",
                "semantics": "complete partition baseline",
            },
            "ordered_lag": {
                "status": "supported",
                "surface": "add_calc.expression_ir.window",
                "semantics": "partition-local lag/lead ordered by explicit ORDER BY",
            },
            "rolling_baseline": {
                "status": "supported",
                "surface": "add_calc.expression_ir.window",
                "functions": ["rollingavg", "rollingmean", "rollingsum", "rollingmin", "rollingmax"],
                "semantics": "bounded preceding row frame with minimum_periods and explicit ORDER BY",
            },
        },
        "result_schemas": {
            "stage": "smoking-data.stage-result.v1",
            "asset_chain": "smoking-data.asset-chain-result.v1",
        },
        "metadata_schemas": {
            "artifact": "smoking-data.artifact-metadata.v1",
            "dataset_manifest": "smoking-data.dataset-manifest.v1",
        },
    }
