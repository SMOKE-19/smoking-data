use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use serde::Serialize;
use std::fs::File;

#[derive(Serialize)]
struct PageIndexDocument {
    schema_version: &'static str,
    page_index_available: bool,
    pages: Vec<PageEntry>,
}

#[derive(Serialize)]
struct PageEntry {
    row_group_id: usize,
    column_path: String,
    page_ordinal: usize,
    byte_offset: i64,
    compressed_length: i32,
    first_row_index: i64,
    row_count: i64,
    dictionary_page_offset: Option<i64>,
    dictionary_page_length: Option<i64>,
}

pub fn inspect_parquet_pages_impl(path: String) -> Result<String, String> {
    let file = File::open(&path).map_err(|error| format!("failed to open {path}: {error}"))?;
    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(file, options)
        .map_err(|error| format!("failed to read parquet page index {path}: {error}"))?;
    let metadata = reader.metadata();
    let Some(offset_indexes) = metadata.offset_index() else {
        return serde_json::to_string(&PageIndexDocument {
            schema_version: "smoking-data.parquet-pages.v1",
            page_index_available: false,
            pages: Vec::new(),
        })
        .map_err(|error| error.to_string());
    };

    let mut pages = Vec::new();
    for (row_group_id, row_group_indexes) in offset_indexes.iter().enumerate() {
        let row_group = metadata.row_group(row_group_id);
        for (column_index, index) in row_group_indexes.iter().enumerate() {
            let column = row_group.column(column_index);
            let locations = index.page_locations();
            let dictionary_page_offset = column.dictionary_page_offset();
            let dictionary_page_length =
                dictionary_page_offset.map(|offset| (column.data_page_offset() - offset).max(0));
            for (page_ordinal, location) in locations.iter().enumerate() {
                let next_first_row = locations
                    .get(page_ordinal + 1)
                    .map(|next| next.first_row_index)
                    .unwrap_or_else(|| row_group.num_rows());
                pages.push(PageEntry {
                    row_group_id,
                    column_path: column.column_path().string(),
                    page_ordinal,
                    byte_offset: location.offset,
                    compressed_length: location.compressed_page_size,
                    first_row_index: location.first_row_index,
                    row_count: (next_first_row - location.first_row_index).max(0),
                    dictionary_page_offset,
                    dictionary_page_length,
                });
            }
        }
    }
    serde_json::to_string(&PageIndexDocument {
        schema_version: "smoking-data.parquet-pages.v1",
        page_index_available: true,
        pages,
    })
    .map_err(|error| error.to_string())
}
