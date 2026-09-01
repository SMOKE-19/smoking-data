from datetime import datetime
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

import smoking_data_engine_rs as engine
from smoking_data import __version__ as package_version


def test_public_package_version_matches_distribution_metadata() -> None:
    assert package_version == version("smoking-data")


def test_native_engine_exposes_its_component_version() -> None:
    assert engine.__version__ == "2.1.0"


def test_validate_expression_ir_accepts_compiler_v1_contract() -> None:
    ir_json = """
    {
      "version": "spotfire-expression-ir.v1",
      "layers": [{
        "expressions": [{
          "name": "amount_x2",
          "source": "[amount] * 2",
          "dependencies": [],
          "dtype": "unknown",
          "nullable": true,
          "expr": {
            "kind": "alias",
            "name": "amount_x2",
            "expression": {
              "kind": "binary",
              "operator": "*",
              "left": {"kind": "column", "name": "amount"},
              "right": {"kind": "literal", "dtype": "int64", "value": 2}
            }
          }
        }]
      }]
    }
    """

    normalized = engine.validate_expression_ir(ir_json)

    assert '"version":"spotfire-expression-ir.v1"' in normalized


def _write_coord_file(path: Path, source: Path) -> None:
    table = pa.table(
        {
            "source_file": [str(source), str(source)],
            "row_group_id": pa.array([0, 1], type=pa.int32()),
            "row_index": pa.array([1, 3], type=pa.int64()),
            "row_offset_in_group": pa.array([1, 1], type=pa.int32()),
            "planner_chunk_id": pa.array([0, 0], type=pa.int32()),
        }
    )
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)


def test_restore_parquet_accepts_large_binary_json_columns(tmp_path: Path) -> None:
    source = tmp_path / "source_large_binary.parquet"
    lookup = tmp_path / "lookup.parquet"
    output = tmp_path / "restored.parquet"

    table = pa.table(
        {
            "group": pa.array(["A", "B"], type=pa.string()),
            "value": pa.array([b"[1.5, 2.5]", b"[3.5]"], type=pa.large_binary()),
            "coord_a": pa.array([b"[0, 0]", b"[1]"], type=pa.large_binary()),
            "coord_b": pa.array([b"[0, 1]", b"[0]"], type=pa.large_binary()),
        }
    )
    pq.write_table(table, source)
    pl.DataFrame(
        {
            "group": ["A", "A", "B"],
            "ord": [0, 1, 0],
            "coord_a": [0, 0, 1],
            "coord_b": [0, 1, 0],
        }
    ).write_parquet(lookup)

    engine.restore_parquet_to_parquet(
        str(source),
        str(output),
        str(lookup),
        {
            "group": "TEXT",
            "value": "DOUBLE[]",
            "coord_a": "INTEGER[]",
            "coord_b": "INTEGER[]",
        },
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "source_coord_columns": ["coord_a", "coord_b"],
            "lookup_coord_columns": ["coord_a", "coord_b"],
        },
        batch_size=16,
    )

    restored = pl.read_parquet(output).sort("group")
    assert restored.get_column("value").to_list() == [[1.5, 2.5], [3.5]]
    assert restored.get_column("coord_a").to_list() == [[0, 0], [1]]
    assert restored.get_column("coord_b").to_list() == [[0, 1], [0]]


def test_restore_parquet_restores_multiple_value_column_types(tmp_path: Path) -> None:
    source = tmp_path / "source_multi_value.parquet"
    lookup = tmp_path / "lookup.parquet"
    output = tmp_path / "restored.parquet"

    pl.DataFrame(
        {
            "group": ["A"],
            "value_float": ["[1.5, 3.5]"],
            "value_int": ["[10, 30]"],
            "value_text": ['["hot", "cold"]'],
            "coord_a": ["[0, 1]"],
            "coord_b": ["[0, 0]"],
        }
    ).write_parquet(source)
    pl.DataFrame(
        {
            "group": ["A", "A", "A"],
            "ord": [0, 1, 2],
            "coord_a": [0, 1, 2],
            "coord_b": [0, 0, 0],
        }
    ).write_parquet(lookup)

    engine.restore_parquet_to_parquet(
        str(source),
        str(output),
        str(lookup),
        {
            "value_float": "DOUBLE[]",
            "value_int": "INTEGER[]",
            "value_text": "TEXT[]",
            "coord_a": "INTEGER[]",
            "coord_b": "INTEGER[]",
        },
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value_float", "value_int", "value_text"],
            "source_coord_columns": ["coord_a", "coord_b"],
            "lookup_coord_columns": ["coord_a", "coord_b"],
        },
    )

    restored = pl.read_parquet(output)
    assert restored.get_column("value_float").to_list() == [[1.5, 3.5, None]]
    assert restored.get_column("value_int").to_list() == [[10, 30, None]]
    assert restored.get_column("value_text").to_list() == [["hot", "cold", None]]


def test_restore_rejects_legacy_coord_columns_key(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    output = tmp_path / "restored.parquet"

    pl.DataFrame(
        {
            "group": ["A"],
            "value": ["[10]"],
            "coord_a": ["[0]"],
            "coord_b": ["[0]"],
        }
    ).write_parquet(source)
    pl.DataFrame(
        {
            "group": ["A"],
            "ord": [0],
            "coord_a": [0],
            "coord_b": [0],
        }
    ).write_parquet(lookup)

    try:
        engine.restore_parquet_to_parquet(
            str(source),
            str(output),
            str(lookup),
            {"value": "INTEGER[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
            {
                "key_column": "group",
                "order_column": "ord",
                "value_columns": ["value"],
                "coord_columns": ["coord_a", "coord_b"],
            },
        )
    except ValueError as exc:
        assert "source_coord_columns must contain exactly 2 items" in str(exc)
    else:
        raise AssertionError("legacy coord_columns key must be rejected")


def test_curated_task_selects_only_coord_rows(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5, 3.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0, 1]", "[0]", "[1]", "[1]"],
            "coord_b": ["[0, 0]", "[1]", "[1]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A", "A", "A"],
            "ord": [0, 1, 2, 3],
            "coord_a": [0, 0, 1, 1],
            "coord_b": [0, 1, 0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    stats = engine.execute_curated_task(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "source_coord_columns": ["coord_a", "coord_b"],
            "lookup_coord_columns": ["coord_a", "coord_b"],
        },
        {"output_file_name": "part-test.parquet"},
        batch_size=16,
    )

    restored = pl.read_parquet(output_dir / "part-test.parquet").sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert restored.get_column("value").to_list() == [
        [None, 2.5, None, None],
        [None, None, 4.5, None],
    ]
    assert stats["rows_written"] == 2.0
    assert stats["source_file_count"] == 1.0
    assert stats["row_group_count"] == 2.0


def test_curated_task_executes_expression_ir(tmp_path: Path) -> None:
    source = tmp_path / "source_expression.parquet"
    coord = tmp_path / "coord_expression.arrow"
    output_dir = tmp_path / "out_expression"
    pl.DataFrame(
        {"id": ["r0", "r1", "r2", "r3"], "amount": [1, 2, 3, 7]}
    ).write_parquet(source, row_group_size=2)
    _write_coord_file(coord, source)
    expression_ir = {
        "version": "spotfire-expression-ir.v1",
        "layers": [
            {
                "expressions": [
                    {
                        "name": "amount_x2",
                        "source": "[amount] * 2",
                        "dependencies": [],
                        "dtype": "unknown",
                        "nullable": True,
                        "expr": {
                            "kind": "alias",
                            "name": "amount_x2",
                            "expression": {
                                "kind": "binary",
                                "operator": "*",
                                "left": {"kind": "column", "name": "amount"},
                                "right": {
                                    "kind": "literal",
                                    "dtype": "int64",
                                    "value": 2,
                                },
                            },
                        },
                    }
                ]
            }
        ],
    }

    engine.execute_curated_task(
        str(coord),
        str(output_dir),
        "",
        {},
        {"enabled": False},
        {
            "output_file_name": "part-expression.parquet",
            "expression_ir": expression_ir,
        },
    )

    output = pl.read_parquet(output_dir / "part-expression.parquet").sort("id")
    assert output.get_column("id").to_list() == ["r1", "r3"]
    assert output.get_column("amount_x2").to_list() == [4, 14]


def test_wheel_round_trips_timestamp_list_and_decimal(tmp_path: Path) -> None:
    source = tmp_path / "source_types.parquet"
    coord = tmp_path / "coord_types.arrow"
    output_dir = tmp_path / "out_types"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "event_at": pa.array(
                [datetime(2024, 1, 2, 3, 4, 5, 6000), datetime(2025, 2, 3, 4, 5, 6)],
                type=pa.timestamp("ns"),
            ),
            "amount": pa.array(
                [Decimal("12.34"), Decimal("-5.60")], type=pa.decimal128(12, 2)
            ),
            "values": pa.array([[1, 2], [3, None]], type=pa.list_(pa.int64())),
        }
    )
    pq.write_table(table, source, row_group_size=2)
    coord_table = pa.table(
        {
            "source_file": [str(source), str(source)],
            "row_group_id": pa.array([0, 0], type=pa.int32()),
            "row_index": pa.array([0, 1], type=pa.int64()),
            "row_offset_in_group": pa.array([0, 1], type=pa.int32()),
            "planner_chunk_id": pa.array([0, 0], type=pa.int32()),
            "active_order": pa.array([0, 1], type=pa.int64()),
        }
    )
    with ipc.new_file(coord, coord_table.schema) as writer:
        writer.write_table(coord_table)

    stats = engine.execute_curated_task(
        str(coord),
        str(output_dir),
        "",
        {
            "event_at": "TIMESTAMP",
            "amount": "DECIMAL(12,2)",
            "values": "INTEGER[]",
        },
        {"enabled": False},
        {
            "output_file_name": "part-types.parquet",
            "projection_columns": [
                {"name": "id", "source": "id"},
                {"name": "event_at", "source": "event_at"},
                {"name": "amount", "source": "amount"},
                {"name": "values", "source": "values"},
            ],
            "output_columns": ["id", "event_at", "amount", "values"],
        },
    )

    output = pq.read_table(output_dir / "part-types.parquet")
    assert output.schema.field("event_at").type == pa.timestamp("us")
    assert output.schema.field("amount").type == pa.decimal128(12, 2)
    assert output.schema.field("values").type == pa.list_(pa.int32())
    assert output.column("amount").to_pylist() == [Decimal("12.34"), Decimal("-5.60")]
    assert output.column("values").to_pylist() == [[1, 2], [3, None]]
    assert stats["rows_written"] == 2.0


def test_curated_task_separates_source_and_lookup_coord_columns(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "id": ["r0", "r1"],
            "group": ["A", "A"],
            "value": ["[10]", "[30]"],
            "src_x": ["[0]", "[1]"],
            "src_y": ["[0]", "[0]"],
        }
    ).write_parquet(source, row_group_size=1)
    pl.DataFrame(
        {
            "group": ["A", "A"],
            "ord": [0, 1],
            "lk_x": [0, 1],
            "lk_y": [0, 0],
        }
    ).write_parquet(lookup)
    coord_table = pa.table(
        {
            "source_file": [str(source), str(source)],
            "row_group_id": pa.array([0, 1], type=pa.int32()),
            "row_index": pa.array([0, 1], type=pa.int64()),
            "row_offset_in_group": pa.array([0, 0], type=pa.int32()),
            "planner_chunk_id": pa.array([0, 0], type=pa.int32()),
        }
    )
    with ipc.new_file(coord, coord_table.schema) as writer:
        writer.write_table(coord_table)

    engine.execute_curated_task(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "INTEGER[]", "src_x": "INTEGER[]", "src_y": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "source_coord_columns": ["src_x", "src_y"],
            "lookup_coord_columns": ["lk_x", "lk_y"],
        },
        {"output_file_name": "part-test.parquet"},
        batch_size=16,
    )

    restored = pl.read_parquet(output_dir / "part-test.parquet").sort("id")
    assert restored.get_column("value").to_list() == [[10, None], [None, 30]]
    assert restored.get_column("src_x").to_list() == [[0, 1], [0, 1]]
    assert restored.get_column("src_y").to_list() == [[0, 0], [0, 0]]


def test_curated_task_can_disable_list_restore_and_only_project_cast(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "raw_id": ["r0", "r1", "r2", "r3"],
            "bucket": ["A", "A", "B", "B"],
            "amount": [10, 20, 30, 40],
        }
    ).write_parquet(source, row_group_size=2)
    _write_coord_file(coord, source)

    stats = engine.execute_curated_task(
        str(coord),
        str(output_dir),
        "",
        {"id": "TEXT", "bucket": "TEXT", "amount": "DOUBLE"},
        {"enabled": False},
        {
            "output_file_name": "part-projected.parquet",
            "projection_columns": [
                {"name": "id", "source": "raw_id"},
                {"name": "bucket", "source": "bucket"},
                {"name": "amount", "source": "amount"},
            ],
        },
        batch_size=16,
    )

    projected = pl.read_parquet(output_dir / "part-projected.parquet").sort("id")
    assert projected.columns == ["id", "bucket", "amount"]
    assert projected.get_column("id").to_list() == ["r1", "r3"]
    assert projected.get_column("amount").to_list() == [20.0, 40.0]
    assert stats["rows_written"] == 2.0
    assert stats["value_column_count"] == 0.0


def test_curated_task_can_partition_output(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "partitioned"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "bucket": ["drop", "B", "drop", "C"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5, 3.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0, 1]", "[0]", "[1]", "[1]"],
            "coord_b": ["[0, 0]", "[1]", "[1]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A", "A", "A"],
            "ord": [0, 1, 2, 3],
            "coord_a": [0, 0, 1, 1],
            "coord_b": [0, 1, 0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    stats = engine.execute_curated_task(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "source_coord_columns": ["coord_a", "coord_b"],
            "lookup_coord_columns": ["coord_a", "coord_b"],
        },
        {"partition_columns": ["bucket"], "file_name_prefix": "part-test"},
    )

    assert (output_dir / "bucket=B" / "part-test-00000.parquet").exists()
    assert (output_dir / "bucket=C" / "part-test-00000.parquet").exists()
    restored = pl.concat(
        [
            pl.read_parquet(output_dir / "bucket=B" / "part-test-00000.parquet"),
            pl.read_parquet(output_dir / "bucket=C" / "part-test-00000.parquet"),
        ]
    ).sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert stats["rows_written"] == 2.0
    assert stats["output_file_count"] == 2.0


def test_curated_task_sanitizes_partition_values_and_nulls(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "partitioned"

    table = pa.table(
        {
            "id": pa.array(["r0", "r1", "r2", "r3"]),
            "bucket": pa.array(["drop", "B/1:bad", "drop", None], type=pa.string()),
            "group": pa.array(["A", "A", "A", "A"]),
            "value": pa.array(["[1.5]", "[2.5]", "[9.5]", "[4.5]"]),
            "coord_a": pa.array(["[0]", "[0]", "[0]", "[0]"]),
            "coord_b": pa.array(["[0]", "[1]", "[0]", "[0]"]),
        }
    )
    pq.write_table(table, source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A"],
            "ord": [0, 1],
            "coord_a": [0, 0],
            "coord_b": [0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    engine.execute_curated_task(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "source_coord_columns": ["coord_a", "coord_b"],
            "lookup_coord_columns": ["coord_a", "coord_b"],
        },
        {"partition_columns": ["bucket"], "file_name_prefix": "part-test"},
    )

    assert (output_dir / "bucket=B_1_bad" / "part-test-00000.parquet").exists()
    assert (output_dir / "bucket=__null__" / "part-test-00000.parquet").exists()


def test_curated_task_applies_reference_replace(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    replace_ref = tmp_path / "replace_ref.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "status": ["old", "old", "keep", "keep"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5, 3.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0, 1]", "[0]", "[1]", "[1]"],
            "coord_b": ["[0, 0]", "[1]", "[1]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A", "A", "A"],
            "ord": [0, 1, 2, 3],
            "coord_a": [0, 0, 1, 1],
            "coord_b": [0, 1, 0, 1],
        }
    ).write_parquet(lookup)
    pl.DataFrame({"from_status": ["old"], "to_status": ["new"]}).write_parquet(
        replace_ref
    )
    _write_coord_file(coord, source)

    engine.execute_curated_task(
        str(coord),
        str(output_dir),
        str(lookup),
        {
            "status": "TEXT",
            "value": "DOUBLE[]",
            "coord_a": "INTEGER[]",
            "coord_b": "INTEGER[]",
        },
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "source_coord_columns": ["coord_a", "coord_b"],
            "lookup_coord_columns": ["coord_a", "coord_b"],
        },
        {
            "output_file_name": "part-test.parquet",
            "reference_replace": {
                "reference_parquet": str(replace_ref),
                "source_column": "status",
                "reference_input_column": "from_status",
                "reference_output_column": "to_status",
            },
        },
    )

    restored = pl.read_parquet(output_dir / "part-test.parquet").sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert restored.get_column("status").to_list() == ["new", "keep"]


def test_curated_task_does_not_publish_partial_parquet_on_error(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    pl.DataFrame(
        {
            "id": ["r0", "r1"],
            "group": ["A", "B"],
            "value": ["[1.5]", "[2.5]"],
            "coord_a": ["[0]", "[0]"],
            "coord_b": ["[0]", "[0]"],
        }
    ).write_parquet(source, row_group_size=1)
    pl.DataFrame(
        {
            "group": ["A"],
            "ord": [0],
            "coord_a": [0],
            "coord_b": [0],
        }
    ).write_parquet(lookup)
    table = pa.table(
        {
            "source_file": [str(source), str(source)],
            "row_group_id": pa.array([0, 1], type=pa.int32()),
            "row_index": pa.array([0, 1], type=pa.int64()),
            "row_offset_in_group": pa.array([0, 0], type=pa.int32()),
            "planner_chunk_id": pa.array([0, 0], type=pa.int32()),
        }
    )
    with ipc.new_file(coord, table.schema) as writer:
        writer.write_table(table)

    try:
        engine.execute_curated_task(
            str(coord),
            str(output_dir),
            str(lookup),
            {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
            {
                "key_column": "group",
                "order_column": "ord",
                "value_columns": ["value"],
                "source_coord_columns": ["coord_a", "coord_b"],
                "lookup_coord_columns": ["coord_a", "coord_b"],
            },
            {"output_file_name": "part-test.parquet"},
        )
    except ValueError as exc:
        assert "unknown lookup key" in str(exc)
    else:
        raise AssertionError("execute_curated_task should fail on missing lookup key")

    assert not list(output_dir.rglob("*.parquet"))
    assert all(
        path.stat().st_size > 0 for path in output_dir.rglob("*") if path.is_file()
    )


def test_curated_task_replaces_existing_output_and_cleans_temp(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    final_output = output_dir / "part-test.parquet"
    final_output.write_bytes(b"stale")

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3"],
            "group": ["A", "A", "A", "A"],
            "value": ["[1.5]", "[2.5]", "[9.5]", "[4.5]"],
            "coord_a": ["[0]", "[0]", "[0]", "[0]"],
            "coord_b": ["[0]", "[1]", "[0]", "[0]"],
        }
    ).write_parquet(source, row_group_size=2)
    pl.DataFrame(
        {
            "group": ["A", "A"],
            "ord": [0, 1],
            "coord_a": [0, 0],
            "coord_b": [0, 1],
        }
    ).write_parquet(lookup)
    _write_coord_file(coord, source)

    engine.execute_curated_task(
        str(coord),
        str(output_dir),
        str(lookup),
        {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
        {
            "key_column": "group",
            "order_column": "ord",
            "value_columns": ["value"],
            "source_coord_columns": ["coord_a", "coord_b"],
            "lookup_coord_columns": ["coord_a", "coord_b"],
        },
        {"output_file_name": final_output.name},
    )

    restored = pl.read_parquet(final_output).sort("id")
    assert restored.get_column("id").to_list() == ["r1", "r3"]
    assert not list(output_dir.glob("*.tmp"))


def test_curated_task_rejects_invalid_binary_json_without_output(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source_invalid_binary.parquet"
    lookup = tmp_path / "lookup.parquet"
    coord = tmp_path / "coord.arrow"
    output_dir = tmp_path / "out"

    table = pa.table(
        {
            "id": pa.array(["r0"]),
            "group": pa.array(["A"]),
            "value": pa.array([b"\xff\xfe"], type=pa.binary()),
            "coord_a": pa.array([b"[0]"], type=pa.binary()),
            "coord_b": pa.array([b"[0]"], type=pa.binary()),
        }
    )
    pq.write_table(table, source)
    pl.DataFrame(
        {"group": ["A"], "ord": [0], "coord_a": [0], "coord_b": [0]}
    ).write_parquet(lookup)
    coord_table = pa.table(
        {
            "source_file": [str(source)],
            "row_group_id": pa.array([0], type=pa.int32()),
            "row_index": pa.array([0], type=pa.int64()),
            "row_offset_in_group": pa.array([0], type=pa.int32()),
            "planner_chunk_id": pa.array([0], type=pa.int32()),
        }
    )
    with ipc.new_file(coord, coord_table.schema) as writer:
        writer.write_table(coord_table)

    try:
        engine.execute_curated_task(
            str(coord),
            str(output_dir),
            str(lookup),
            {"value": "DOUBLE[]", "coord_a": "INTEGER[]", "coord_b": "INTEGER[]"},
            {
                "key_column": "group",
                "order_column": "ord",
                "value_columns": ["value"],
                "source_coord_columns": ["coord_a", "coord_b"],
                "lookup_coord_columns": ["coord_a", "coord_b"],
            },
            {"output_file_name": "part-test.parquet"},
        )
    except ValueError as exc:
        assert "invalid utf-8 bytes" in str(exc)
    else:
        raise AssertionError("execute_curated_task should fail on invalid binary json")

    assert not list(output_dir.rglob("*.parquet"))


def test_plan_coordinates_writes_row_count_chunks(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    coord_dir = tmp_path / "coords"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3", "r4"],
            "group": ["A", "A", "A", "A", "A"],
        }
    ).write_parquet(source, row_group_size=2)

    stats = engine.plan_coordinates(
        [str(source)],
        str(coord_dir),
        planner_config={"row_count": 2},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    assert [path.name for path in coord_files] == [
        "chunk-000000.arrow",
        "chunk-000001.arrow",
        "chunk-000002.arrow",
    ]
    assert stats["input_file_count"] == 1.0
    assert stats["input_row_group_count"] >= 1.0
    assert stats["selected_row_count"] == 5.0
    assert stats["coord_chunk_count"] == 3.0

    first = pl.read_ipc(coord_files[0])
    assert first.get_column("row_index").to_list() == [0, 1]
    assert first.get_column("row_offset_in_group").to_list() == [0, 1]


def test_plan_coordinates_can_chunk_by_source_file_locality(tmp_path: Path) -> None:
    source_a = tmp_path / "source-000.parquet"
    source_b = tmp_path / "source-001.parquet"
    coord_dir = tmp_path / "coords"

    pl.DataFrame(
        {
            "row_key": ["A", "B"],
            "payload": [1, 2],
        }
    ).write_parquet(source_a, row_group_size=1)
    pl.DataFrame(
        {
            "row_key": ["C", "D"],
            "payload": [3, 4],
        }
    ).write_parquet(source_b, row_group_size=1)

    stats = engine.plan_coordinates(
        [str(source_a), str(source_b)],
        str(coord_dir),
        planner_config={
            "row_count": 10,
            "mode": "source_file_locality",
            "row_keys": ["row_key"],
            "max_source_files_per_chunk": 1,
        },
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    assert len(coord_files) == 2
    assert stats["planner_mode_source_file_locality"] == 1.0
    assert stats["chunk_source_file_count_max"] == 1.0
    assert stats["selected_row_count"] == 4.0
    assert [
        sorted(set(pl.read_ipc(path).get_column("source_file").to_list()))
        for path in coord_files
    ] == [[str(source_a)], [str(source_b)]]


def test_plan_coordinates_applies_thin_filters_and_dedupe(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    coord_dir = tmp_path / "coords"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3", "r4"],
            "biz": ["keep", "drop", None, "keep", "keep"],
            "group_key": ["A", "A", "B", "A", "B"],
            "updated_at": [
                "2025-01-01T00:00:00",
                "2025-01-03T00:00:00",
                "2025-01-04T00:00:00",
                "2025-01-02T00:00:00",
                "2025-01-05T00:00:00",
            ],
        }
    ).write_parquet(source, row_group_size=2)

    stats = engine.plan_coordinates(
        [str(source)],
        str(coord_dir),
        filter_config={
            "source_filters": [
                {"column": "biz", "op": "is_not_null"},
                {"column": "biz", "op": "ne", "value": "drop"},
            ],
            "dedupe": {
                "enabled": True,
                "group_keys": ["group_key"],
                "sort": [{"column": "updated_at", "direction": "desc"}],
            },
        },
        planner_config={"row_count": 10},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    assert len(coord_files) == 1
    coords = pl.read_ipc(coord_files[0]).sort("row_index")
    assert coords.get_column("row_index").to_list() == [3, 4]
    assert stats["selected_row_count"] == 2.0


def test_plan_coordinates_dedupe_supports_timestamp_nanosecond_sort(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source_timestamp_ns.parquet"
    coord_dir = tmp_path / "coords"

    table = pa.table(
        {
            "id": pa.array(["old_a", "new_a", "old_b", "new_b"]),
            "group_key": pa.array(["A", "A", "B", "B"]),
            "updated_at": pa.array(
                [
                    1_700_000_000_000_000_001,
                    1_700_000_000_000_000_010,
                    1_700_000_000_000_000_003,
                    1_700_000_000_000_000_020,
                ],
                type=pa.timestamp("ns"),
            ),
        }
    )
    pq.write_table(table, source, row_group_size=2)

    stats = engine.plan_coordinates(
        [str(source)],
        str(coord_dir),
        filter_config={
            "dedupe": {
                "enabled": True,
                "group_keys": ["group_key"],
                "sort": [{"column": "updated_at", "direction": "desc"}],
            },
        },
        planner_config={"row_count": 10},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    assert len(coord_files) == 1
    coords = pl.read_ipc(coord_files[0]).sort("row_index")
    assert coords.get_column("row_index").to_list() == [1, 3]
    assert stats["selected_row_count"] == 2.0


def test_plan_coordinates_supports_in_like_and_or_filters(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    coord_dir = tmp_path / "coords"

    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3", "r4"],
            "biz": ["A100", "A200", "B100", "C100", None],
            "status": ["keep", "drop", "hold", "keep", "keep"],
        }
    ).write_parquet(source, row_group_size=2)

    stats = engine.plan_coordinates(
        [str(source)],
        str(coord_dir),
        filter_config={
            "source_filters": [
                {
                    "op": "and",
                    "filters": [
                        {"column": "biz", "op": "like", "value": "A%"},
                        {
                            "op": "or",
                            "filters": [
                                {
                                    "column": "status",
                                    "op": "in",
                                    "values": ["keep", "hold"],
                                },
                                {"column": "biz", "op": "eq", "value": "C100"},
                            ],
                        },
                    ],
                }
            ],
        },
        planner_config={"row_count": 10},
    )

    coord_files = sorted(coord_dir.glob("*.arrow"))
    coords = pl.read_ipc(coord_files[0]).sort("row_index")
    assert coords.get_column("row_index").to_list() == [0]
    assert stats["selected_row_count"] == 1.0
