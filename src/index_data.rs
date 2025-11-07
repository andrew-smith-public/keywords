use hashbrown::HashMap;
use std::collections::HashMap as StdHashMap;
use indexmap::IndexSet;
use crate::utils::column_pool::ColumnPool;
use crate::index_structure::column_filter::ColumnFilter;
use rkyv::{Archive, Serialize as RkyvSerialize, Deserialize as RkyvDeserialize, to_bytes};
use rkyv::rancor::Error as RkyvError;
use crate::keyword_shred::SPLIT_CHARS_INCLUSIVE;
use crate::utils::file_interaction_local_and_cloud::get_object_store;
use crate::{KeywordOneFile, ParquetSource, ProcessResult, METADATA_CHUNK_SIZE};
use crate::index_structure::index_files::{index_filename, IndexFile};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
pub struct IndexFilters {
    // Version & validation
    pub version: u32,
    pub parquet_etag: String,
    pub parquet_size: u64,
    pub parquet_last_modified: u64,

    // Configuration
    pub error_rate: f64,
    pub split_chars_inclusive: Vec<Vec<char>>,

    // Data structures
    pub column_pool: ColumnPool,
    pub column_filters: StdHashMap<String, ColumnFilter>,
    pub global_filter: ColumnFilter,
    pub chunk_index: Vec<ChunkInfo>,
}

/// Information about a chunk in the metadata file
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct ChunkInfo {
    pub start_keyword: String,
    pub offset: u64,
    pub length: u32,
    pub count: u16,
}

/// Metadata entry for a single keyword in the index
/// Each chunk in metadata.rkyv contains Vec<MetadataEntry>
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct MetadataEntry {
    pub keyword: String,
    pub offset: u64,
    pub length: u32,
    pub column_ids: Vec<u32>,
    pub num_occurrences: u32,
}

/// Flattened keyword data (stored in data.bin)
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct KeywordDataFlat {
    pub columns: Vec<ColumnDataFlat>,
    pub splits_matched: u16,
}

/// Per-column data for a keyword
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct ColumnDataFlat {
    pub column_id: u32,
    pub row_groups: Vec<RowGroupDataFlat>,
}

/// Per-row-group data for a column
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct RowGroupDataFlat {
    pub row_group_id: u16,
    pub rows: Vec<FlatRow>,
}

/// Flattened row information
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlatRow {
    pub row: u32,
    pub additional_rows: u32,
    pub splits_matched: u16,

    // Parent keyword offset for phrase search optimization
    // After serialization, parent keyword string is converted to u32 offset
    // pointing to parent keyword in the sorted keyword list
    // None indicates this is a root token from the original parquet string
    //
    // Considered but deferred: token_position (u16/u32) for phrase search optimization
    // Trade-off: Adding position would cost 2-4 bytes per row but enable early rejection
    // of non-adjacent tokens during phrase matching. Current approach favors memory
    // efficiency over search speed. If profiling shows phrase search is a bottleneck,
    // this could be revisited. For now, parent tracking provides sufficient optimization.
    pub parent_offset: Option<u32>,
}

/// Column keywords file (stored in column_keywords.rkyv)
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
pub struct ColumnKeywordsFile {
    pub all_keywords: Vec<String>,
    pub columns: StdHashMap<String, Vec<u32>>,
    pub num_columns: usize,
    pub num_unique_keywords: usize,
}

/// Converts a KeywordOneFile to a flattened KeywordDataFlat structure for serialization.
///
/// This function transforms the hierarchical keyword data structure into a flat, serializable
/// format. It processes each column (skipping the global aggregate at index 0), extracts
/// row group and row information, and converts parent keyword string references to numeric
/// offsets using the provided keyword-to-offset mapping.
///
/// The flattening process:
/// 1. Iterates through all columns except the aggregate bucket
/// 2. For each column, processes all row groups
/// 3. For each row group, converts all rows to FlatRow format
/// 4. Converts parent keyword Rc<str> references to u32 offsets
///
/// # Arguments
///
/// * `keyword_data` - Reference to the KeywordOneFile containing the hierarchical keyword data
/// * `keyword_to_offset` - HashMap mapping keyword strings to their numeric offsets in the
///   sorted keyword list, used for converting parent keyword references
///
/// # Returns
///
/// Returns a `KeywordDataFlat` structure containing:
/// - Flattened column data with row groups and rows
/// - The splits_matched bitmask from the original keyword data
///
/// # Notes
///
/// - Skips column index 0 (the aggregate bucket) during iteration
/// - Parent keyword references are converted from Rc<str> to u32 offsets
/// - Handles missing data gracefully (uses Option for parent_offset)
fn convert_to_flat(
    keyword_data: &KeywordOneFile,
    keyword_to_offset: &HashMap<&str, u32>,
) -> KeywordDataFlat {
    let mut columns = Vec::new();

    // Iterate through columns (skip index 0 which is the aggregate)
    for (col_idx, &column_id) in keyword_data.column_references.iter().enumerate() {
        let mut row_groups = Vec::new();

        // Get row groups for this column
        if let Some(rg_set) = keyword_data.row_groups.get(col_idx) {
            for (rg_idx, &row_group_id) in rg_set.iter().enumerate() {
                let mut rows = Vec::new();

                // Get rows for this row group
                if let Some(row_data) = keyword_data.row_group_to_rows
                    .get(col_idx)
                    .and_then(|rgs| rgs.get(rg_idx))
                {
                    for row in row_data {
                        // Convert parent keyword string to offset
                        let parent_offset = row.parent_keyword
                            .as_ref()
                            .and_then(|parent_str| keyword_to_offset.get(parent_str.as_ref()).copied());

                        rows.push(FlatRow {
                            row: row.row,
                            additional_rows: row.additional_rows as u32,
                            splits_matched: row.splits_matched,
                            parent_offset,
                        });
                    }
                }

                row_groups.push(RowGroupDataFlat {
                    row_group_id,
                    rows,
                });
            }
        }

        columns.push(ColumnDataFlat {
            column_id,
            row_groups,
        });
    }

    KeywordDataFlat {
        columns,
        splits_matched: keyword_data.splits_matched,
    }
}

/// Builds distributed index files from a ProcessResult.
///
/// This function creates a complete distributed index structure from the processed keyword data.
/// It performs several key operations:
/// 1. Retrieves and stores Parquet metadata for validation
/// 2. Sorts keywords deterministically for consistent layout
/// 3. Creates a keyword-to-offset mapping for parent reference conversion
/// 4. Chunks keywords and builds metadata entries
/// 5. Serializes keyword data to a binary format
/// 6. Constructs filter, metadata, data, and column keyword files
///
/// The function creates four main output files:
/// - **Filters file**: Contains Parquet metadata, bloom filters, column pool, and chunk index
/// - **Metadata file**: Contains keyword metadata entries (offset, length, columns, occurrence count)
/// - **Data file**: Contains the actual serialized keyword data
/// - **Column keywords file**: Maps column names to their associated keywords
///
/// # Arguments
///
/// * `result` - Reference to the ProcessResult containing keyword maps, filters, and column data
/// * `parquet_path` - Path to the source Parquet file for metadata retrieval
/// * `error_rate` - Bloom filter error rate used during index creation
///
/// # Returns
///
/// Returns `Result<DistributedIndexFiles, Box<dyn std::error::Error + Send + Sync>>`:
/// - `Ok(DistributedIndexFiles)` - Container with all serialized index file data
/// - `Err` - If object store access fails, serialization fails, or metadata retrieval fails
///
/// # Errors
///
/// This function will return an error if:
/// - The Parquet file cannot be accessed or metadata cannot be retrieved
/// - Serialization of any index component fails
/// - Memory allocation fails during data structure construction
///
/// # Performance Considerations
///
/// - Keywords are sorted once for deterministic layout
/// - Metadata is chunked (size defined by METADATA_CHUNK_SIZE) to enable efficient partial loading
/// - Uses rkyv for zero-copy deserialization support
/// - Parent keyword references are converted to numeric offsets to reduce serialized size
///
/// # Examples
///
/// ```no_run
/// # use keywords::index_data::build_distributed_index;
/// use keywords::ParquetSource;
/// use keywords::column_parquet_reader::process_parquet_file;
/// use keywords::index_data::DistributedIndexFiles;
/// # async fn example() -> () {
///     // Generate test parquet data in memory
///     let parquet_bytes = vec![/* generated parquet data */];
///     let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, None).await.unwrap();
///     build_distributed_index(
///         &result,
///         &ParquetSource::Bytes(parquet_bytes),
///         0.01
///     ).await.unwrap();
/// # }
/// // index_files now contains all serialized data ready to be written
/// ```
pub async fn build_distributed_index(
    result: &ProcessResult,
    source: &ParquetSource,
    error_rate: f64,
) -> Result<DistributedIndexFiles, Box<dyn std::error::Error + Send + Sync>> {
    // Get parquet metadata for validation
    let (parquet_etag, parquet_size, parquet_last_modified) = match source {
        ParquetSource::Path(path) => {
            let (store, obj_path) = get_object_store(path).await?;
            let head = store.head(&obj_path).await?;
            (
                head.e_tag.unwrap_or_else(|| "unknown".to_string()),
                head.size,
                head.last_modified.timestamp() as u64,
            )
        }
        ParquetSource::Bytes(vec) => {
            ("".to_string(), vec.len() as u64, 0)
        }
    };

    // Sort keywords for deterministic layout
    let mut sorted_keywords: Vec<_> = result.keyword_map.iter().collect();
    sorted_keywords.sort_by(|a, b| a.0.cmp(b.0));

    // Build keyword → offset mapping for parent reference conversion
    let keyword_to_offset: HashMap<&str, u32> = sorted_keywords.iter()
        .enumerate()
        .map(|(idx, (keyword, _))| (keyword.as_ref(), idx as u32))
        .collect();

    // Build metadata chunks and data file
    let mut data_file = Vec::new();
    let mut chunk_index = Vec::new();
    let mut all_metadata_chunks = Vec::new();

    for chunk in sorted_keywords.chunks(METADATA_CHUNK_SIZE) {
        let chunk_start_keyword = chunk[0].0;
        let mut chunk_entries = Vec::new();

        for (keyword, keyword_data) in chunk {
            let data_offset = data_file.len() as u64;

            // Convert to flat structure with parent offset mapping
            let flat_data = convert_to_flat(keyword_data, &keyword_to_offset);

            // Count occurrences
            let num_occurrences: u32 = flat_data.columns.iter()
                .flat_map(|col| &col.row_groups)
                .map(|rg| rg.rows.len() as u32)
                .sum();

            // Get column IDs
            let column_ids: Vec<u32> = flat_data.columns.iter()
                .map(|col| col.column_id)
                .collect();

            // Serialize keyword data
            let data_bytes = to_bytes::<RkyvError>(&flat_data)
                .map_err(|e| format!("Failed to serialize keyword data: {}", e))?;

            let data_length = data_bytes.len() as u32;

            // Append to data file
            data_file.extend_from_slice(&data_bytes);

            // Create metadata entry
            chunk_entries.push(MetadataEntry {
                keyword: keyword.to_string(),
                offset: data_offset,
                length: data_length,
                column_ids,
                num_occurrences,
            });
        }

        // Serialize this chunk
        let chunk_bytes = to_bytes::<RkyvError>(&chunk_entries)
            .map_err(|e| format!("Failed to serialize metadata chunk: {}", e))?;

        let chunk_offset = all_metadata_chunks.len() as u64;
        let chunk_length = chunk_bytes.len() as u32;
        let chunk_count = chunk_entries.len() as u16;

        all_metadata_chunks.extend_from_slice(&chunk_bytes);

        // Add to chunk index
        chunk_index.push(ChunkInfo {
            start_keyword: chunk_start_keyword.to_string(),
            offset: chunk_offset,
            length: chunk_length,
            count: chunk_count,
        });
    }

    // Build filters file
    let split_chars_vec: Vec<Vec<char>> = SPLIT_CHARS_INCLUSIVE.iter()
        .map(|&chars| chars.to_vec())
        .collect();

    let index_filters = IndexFilters {
        version: 1,
        parquet_etag,
        parquet_size,
        parquet_last_modified,
        error_rate,
        split_chars_inclusive: split_chars_vec,
        column_pool: result.column_pool.clone(),
        column_filters: result.column_filters.iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
        global_filter: result.global_filter.clone(),
        chunk_index,
    };

    let filters_bytes = to_bytes::<RkyvError>(&index_filters)
        .map_err(|e| format!("Failed to serialize filters: {}", e))?;

    // Build column keywords file
    let mut all_keywords = IndexSet::new();
    for keywords in result.column_keywords_map.values() {
        all_keywords.extend(keywords.iter().map(|k| k.to_string()));
    }

    let all_keywords_vec: Vec<String> = all_keywords.into_iter().collect();
    let mut keyword_to_idx: StdHashMap<String, u32> = StdHashMap::new();
    for (idx, keyword) in all_keywords_vec.iter().enumerate() {
        keyword_to_idx.insert(keyword.clone(), idx as u32);
    }

    let mut columns_map: StdHashMap<String, Vec<u32>> = StdHashMap::new();
    for (column_name, keywords) in &result.column_keywords_map {
        let indices: Vec<u32> = keywords.iter()
            .filter_map(|k| keyword_to_idx.get(k.as_ref()).copied())
            .collect();
        columns_map.insert(column_name.to_string(), indices);
    }

    let column_keywords = ColumnKeywordsFile {
        all_keywords: all_keywords_vec,
        columns: columns_map,
        num_columns: result.column_keywords_map.len(),
        num_unique_keywords: keyword_to_idx.len(),
    };

    let column_keywords_bytes = to_bytes::<RkyvError>(&column_keywords)
        .map_err(|e| format!("Failed to serialize column keywords: {}", e))?;

    Ok(DistributedIndexFiles {
        filters: filters_bytes.to_vec(),
        metadata: all_metadata_chunks,
        data: data_file,
        column_keywords: column_keywords_bytes.to_vec(),
    })
}

/// Container for the distributed index files
pub struct DistributedIndexFiles {
    pub filters: Vec<u8>,
    pub metadata: Vec<u8>,
    pub data: Vec<u8>,
    pub column_keywords: Vec<u8>,
}

/// Saves distributed index files to a directory structure.
///
/// This function writes all four components of the distributed index to disk in a structured
/// format. It creates an index directory (with `.index` extension) next to the Parquet file
/// and writes the filters, metadata, data, and column keywords files.
///
/// The directory structure created:
/// ```text
/// <base_path>.index/
/// ├── filters.rkyv (or <prefix>_filters.rkyv)
/// ├── metadata.rkyv (or <prefix>_metadata.rkyv)
/// ├── data.bin (or <prefix>_data.bin)
/// └── column_keywords.rkyv (or <prefix>_column_keywords.rkyv)
/// ```
///
/// # Arguments
///
/// * `files` - Reference to DistributedIndexFiles containing all serialized index data
/// * `base_path` - Base path of the Parquet file (e.g., "/data/my_file.parquet")
///   The index directory will be created as `<base_path>.index`
/// * `prefix` - Optional prefix for the index files, useful for creating multiple
///   index versions or variants (e.g., Some("v2") creates "v2_filters.rkyv")
///
/// # Returns
///
/// Returns `Result<(), Box<dyn std::error::Error + Send + Sync>>`:
/// - `Ok(())` - All files were successfully written
/// - `Err` - If directory creation fails or any file write operation fails
///
/// # Errors
///
/// This function will return an error if:
/// - The index directory cannot be created (permissions, disk space, etc.)
/// - Any of the four index files cannot be written
/// - I/O errors occur during the write operations
///
/// # Examples
///
/// ```no_run
/// # use keywords::index_data::save_distributed_index;
/// use keywords::ParquetSource;
/// use keywords::index_data::build_distributed_index;
/// use keywords::column_parquet_reader::process_parquet_file;
///
/// # async fn example() -> () {
///     // Generate test parquet data in memory
///     let parquet_bytes = vec![/* generated parquet data */];
///     let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, None).await.unwrap();
///
///     let index_files = build_distributed_index(&result, &ParquetSource::Bytes(parquet_bytes), 0.01).await.unwrap();
///
///     // Save without prefix (path can be arbitrary for in-memory sources)
///     save_distributed_index(&index_files, "my_data.parquet", None).await.unwrap();
///     // Creates: my_data.parquet.index/filters.rkyv, metadata.rkyv, data.bin, column_keywords.rkyv
///
///     // Save with prefix for versioning
///     save_distributed_index(&index_files, "my_data.parquet", Some("v2")).await.unwrap();
///     // Creates: my_data.parquet.index/v2_filters.rkyv, v2_metadata.rkyv, v2_data.bin, v2_column_keywords.rkyv
/// # }
/// ```
///
/// # Platform Compatibility
///
/// Uses Tokio's async filesystem operations, compatible with all platforms supported by Tokio.
pub async fn save_distributed_index(
    files: &DistributedIndexFiles,
    base_path: &str,
    prefix: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::path::Path;
    use tokio::fs;

    // Create index directory
    let index_dir = format!("{}.index", base_path);
    fs::create_dir_all(&index_dir).await?;

    // Write files with optional prefix
    fs::write(
        Path::new(&index_dir).join(index_filename(IndexFile::Filters, prefix)),
        &files.filters
    ).await?;
    fs::write(
        Path::new(&index_dir).join(index_filename(IndexFile::Metadata, prefix)),
        &files.metadata
    ).await?;
    fs::write(
        Path::new(&index_dir).join(index_filename(IndexFile::Data, prefix)),
        &files.data
    ).await?;
    fs::write(
        Path::new(&index_dir).join(index_filename(IndexFile::ColumnKeywords, prefix)),
        &files.column_keywords
    ).await?;

    Ok(())
}