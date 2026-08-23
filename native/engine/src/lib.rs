use pyo3::prelude::*;
use std::collections::HashMap;

mod converter;
mod coord;
mod expression_executor;
mod expression_ir;
mod join;
mod list_executor;
mod long_fact;
mod object_store_reader;
mod page_index;
mod parser;
mod pivot;
mod planner;
#[cfg(feature = "polars-join-experiment")]
mod polars_bridge;
mod post_operations;
mod reference;

#[pyfunction]
fn validate_expression_ir(ir_json: String) -> PyResult<String> {
    expression_ir::validate_expression_ir_json(ir_json)
}

#[pyfunction]
fn supported_expression_functions() -> Vec<String> {
    expression_ir::SUPPORTED_SCALAR_FUNCTIONS
        .iter()
        .map(|value| (*value).to_string())
        .collect()
}

#[pyfunction]
#[pyo3(
    text_signature = "(input_parquet_path, output_parquet_path, lookup_path, schema, config_json, batch_size=None, drop_cache_hint=False, print_timing=False)"
)]
#[allow(clippy::too_many_arguments)]
fn restore_parquet_to_parquet(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> PyResult<HashMap<String, f64>> {
    converter::restore_parquet_to_parquet_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
    )
}

#[pyfunction]
#[pyo3(
    text_signature = "(input_parquet_path, output_parquet_path, lookup_path, schema, config_json, batch_size=None, drop_cache_hint=False)"
)]
fn restore_parquet_to_parquet_profiled(
    input_parquet_path: String,
    output_parquet_path: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
) -> PyResult<HashMap<String, f64>> {
    converter::restore_parquet_to_parquet_profiled_impl(
        input_parquet_path,
        output_parquet_path,
        lookup_path,
        schema,
        config_json,
        batch_size,
        drop_cache_hint,
    )
}

#[pyfunction]
#[pyo3(
    text_signature = "(coord_path, output_dir, lookup_path, schema, config_json, writer_config_json=None, batch_size=None, drop_cache_hint=False, print_timing=False)"
)]
#[allow(clippy::too_many_arguments)]
fn execute_curated_task(
    coord_path: String,
    output_dir: String,
    lookup_path: String,
    schema: HashMap<String, String>,
    config_json: String,
    writer_config_json: Option<String>,
    batch_size: Option<usize>,
    drop_cache_hint: bool,
    print_timing: bool,
) -> PyResult<HashMap<String, f64>> {
    converter::execute_curated_task_impl(
        coord_path,
        output_dir,
        lookup_path,
        schema,
        config_json,
        writer_config_json,
        batch_size,
        drop_cache_hint,
        print_timing,
    )
}

#[pyfunction]
#[pyo3(
    text_signature = "(input_parquet_paths, coord_output_dir, filter_config_json=None, planner_config_json=None)"
)]
fn plan_coordinates(
    input_parquet_paths: Vec<String>,
    coord_output_dir: String,
    filter_config_json: Option<String>,
    planner_config_json: Option<String>,
) -> PyResult<HashMap<String, f64>> {
    planner::plan_coordinates_impl(
        input_parquet_paths,
        coord_output_dir,
        filter_config_json,
        planner_config_json,
    )
}

#[pyfunction]
fn execute_join_task(task_json: String) -> PyResult<HashMap<String, f64>> {
    join::execute_join_task_json(&task_json).map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
fn join_backend_capabilities() -> Vec<String> {
    #[cfg(feature = "polars-join-experiment")]
    return vec!["arrow_native".to_string(), "polars".to_string()];
    #[cfg(not(feature = "polars-join-experiment"))]
    return vec!["arrow_native".to_string()];
}

#[pyfunction]
fn validate_dataset(
    parquet_paths: Vec<String>,
    assertion_config_json: String,
    spill_dir: String,
) -> PyResult<String> {
    post_operations::validate_dataset(&parquet_paths, &assertion_config_json, &spill_dir)
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
fn inspect_parquet_pages(path: String) -> PyResult<String> {
    page_index::inspect_parquet_pages_impl(path).map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
fn s3_get_range(request_json: String) -> PyResult<Vec<u8>> {
    object_store_reader::s3_get_range_impl(&request_json)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

#[pyfunction]
fn read_s3_parquet_to_ipc(request_json: String) -> PyResult<String> {
    object_store_reader::read_s3_parquet_to_ipc_impl(&request_json)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

#[pymodule]
fn smoking_data_engine_rs(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "__doc__",
        "Bounded-memory Rust execution engine for smoking-data pipelines",
    )?;
    module.add_function(wrap_pyfunction!(restore_parquet_to_parquet, module)?)?;
    module.add_function(wrap_pyfunction!(
        restore_parquet_to_parquet_profiled,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(execute_curated_task, module)?)?;
    module.add_function(wrap_pyfunction!(plan_coordinates, module)?)?;
    module.add_function(wrap_pyfunction!(execute_join_task, module)?)?;
    module.add_function(wrap_pyfunction!(join_backend_capabilities, module)?)?;
    module.add_function(wrap_pyfunction!(validate_dataset, module)?)?;
    module.add_function(wrap_pyfunction!(inspect_parquet_pages, module)?)?;
    module.add_function(wrap_pyfunction!(s3_get_range, module)?)?;
    module.add_function(wrap_pyfunction!(read_s3_parquet_to_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(validate_expression_ir, module)?)?;
    module.add_function(wrap_pyfunction!(supported_expression_functions, module)?)?;
    Ok(())
}
