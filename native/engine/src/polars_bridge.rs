use arrow_array::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{make_array, Array, ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};
use polars_arrow::ffi::{
    export_array_to_c, export_field_to_c, import_array_from_c, import_field_from_c,
    ArrowArray as PolarsArrowArray, ArrowSchema as PolarsArrowSchema,
};
use polars_core::prelude::{CompatLevel, DataFrame, PlSmallStr, Series};
use std::mem::{align_of, size_of, ManuallyDrop};
use std::sync::Arc;
use std::time::Instant;

use crate::join::BridgeProfile;

pub fn apache_to_polars(batch: RecordBatch) -> Result<(DataFrame, BridgeProfile), String> {
    let started = Instant::now();
    let apache_input_bytes = batch.get_array_memory_size();
    let fields = batch.schema().fields().clone();
    let columns = batch
        .columns()
        .iter()
        .zip(fields.iter())
        .map(|(array, field)| apache_array_to_polars(array, field.name()))
        .collect::<Result<Vec<_>, _>>()?;
    let frame = DataFrame::new(batch.num_rows(), columns).map_err(|error| error.to_string())?;
    let profile = BridgeProfile {
        apache_input_bytes,
        polars_input_estimated_bytes: frame.estimated_size(),
        import_sec: started.elapsed().as_secs_f64(),
        ..BridgeProfile::default()
    };
    Ok((frame, profile))
}

pub fn polars_to_apache(mut frame: DataFrame) -> Result<(RecordBatch, BridgeProfile), String> {
    let started = Instant::now();
    let polars_output_estimated_bytes = frame.estimated_size();
    // Exact downstream schema parity requires the oldest compatibility level:
    // Polars StringView/BinaryView columns are materialized as Utf8/LargeUtf8.
    // This conversion is intentionally measured because it was a historical
    // peak-memory suspect at the Apache Arrow <-> Polars boundary.
    frame.rechunk_mut();
    let batch = frame
        .iter_chunks(CompatLevel::oldest(), false)
        .next()
        .ok_or_else(|| "Polars join produced no record batch".to_string())?;
    let (schema, arrays) = batch.into_schema_and_arrays();
    let fields = schema
        .iter()
        .map(|(_, field)| polars_field_to_apache(field))
        .collect::<Result<Vec<_>, _>>()?;
    let arrays = arrays
        .into_iter()
        .zip(schema.iter())
        .map(|(array, (_, field))| polars_array_to_apache(array, field))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| error.to_string())?;
    let profile = BridgeProfile {
        polars_output_estimated_bytes,
        apache_output_bytes: batch.get_array_memory_size(),
        export_sec: started.elapsed().as_secs_f64(),
        ..BridgeProfile::default()
    };
    Ok((batch, profile))
}

fn apache_array_to_polars(
    array: &ArrayRef,
    name: &str,
) -> Result<polars_core::prelude::Column, String> {
    let (array, schema) = to_ffi(&array.to_data()).map_err(|error| error.to_string())?;
    let polars_array = unsafe { move_ffi_array_to_polars(array) };
    let polars_schema = unsafe { move_ffi_schema_to_polars(schema) };
    let field =
        unsafe { import_field_from_c(&polars_schema) }.map_err(|error| error.to_string())?;
    let array = unsafe { import_array_from_c(polars_array, field.dtype().clone()) }
        .map_err(|error| error.to_string())?;
    Series::from_arrow(PlSmallStr::from_str(name), array)
        .map(Into::into)
        .map_err(|error| error.to_string())
}

fn polars_field_to_apache(field: &polars_arrow::datatypes::Field) -> Result<Arc<Field>, String> {
    let schema = export_field_to_c(field);
    let schema = unsafe { move_ffi_schema_to_apache(schema) };
    let field = Field::try_from(&schema).map_err(|error| error.to_string())?;
    Ok(Arc::new(field))
}

fn polars_array_to_apache(
    array: Box<dyn polars_arrow::array::Array>,
    field: &polars_arrow::datatypes::Field,
) -> Result<ArrayRef, String> {
    let array = export_array_to_c(array);
    let schema = export_field_to_c(field);
    let array = unsafe { move_ffi_array_to_apache(array) };
    let schema = unsafe { move_ffi_schema_to_apache(schema) };
    let data = unsafe { from_ffi(array, &schema) }.map_err(|error| error.to_string())?;
    Ok(make_array(data))
}

unsafe fn move_ffi_array_to_polars(array: FFI_ArrowArray) -> PolarsArrowArray {
    debug_assert_eq!(size_of::<FFI_ArrowArray>(), size_of::<PolarsArrowArray>());
    debug_assert_eq!(align_of::<FFI_ArrowArray>(), align_of::<PolarsArrowArray>());
    let array = ManuallyDrop::new(array);
    unsafe { std::ptr::read((&*array as *const FFI_ArrowArray).cast::<PolarsArrowArray>()) }
}

unsafe fn move_ffi_schema_to_polars(schema: FFI_ArrowSchema) -> PolarsArrowSchema {
    debug_assert_eq!(size_of::<FFI_ArrowSchema>(), size_of::<PolarsArrowSchema>());
    debug_assert_eq!(
        align_of::<FFI_ArrowSchema>(),
        align_of::<PolarsArrowSchema>()
    );
    let schema = ManuallyDrop::new(schema);
    unsafe { std::ptr::read((&*schema as *const FFI_ArrowSchema).cast::<PolarsArrowSchema>()) }
}

unsafe fn move_ffi_array_to_apache(array: PolarsArrowArray) -> FFI_ArrowArray {
    debug_assert_eq!(size_of::<PolarsArrowArray>(), size_of::<FFI_ArrowArray>());
    debug_assert_eq!(align_of::<PolarsArrowArray>(), align_of::<FFI_ArrowArray>());
    let array = ManuallyDrop::new(array);
    unsafe { std::ptr::read((&*array as *const PolarsArrowArray).cast::<FFI_ArrowArray>()) }
}

unsafe fn move_ffi_schema_to_apache(schema: PolarsArrowSchema) -> FFI_ArrowSchema {
    debug_assert_eq!(size_of::<PolarsArrowSchema>(), size_of::<FFI_ArrowSchema>());
    debug_assert_eq!(
        align_of::<PolarsArrowSchema>(),
        align_of::<FFI_ArrowSchema>()
    );
    let schema = ManuallyDrop::new(schema);
    unsafe { std::ptr::read((&*schema as *const PolarsArrowSchema).cast::<FFI_ArrowSchema>()) }
}
