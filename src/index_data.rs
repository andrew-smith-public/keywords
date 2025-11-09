use hashbrown::HashMap;
use std::collections::HashMap as StdHashMap;
use crate::utils::column_pool::ColumnPool;
use crate::index_structure::column_filter::ColumnFilter;
use rkyv::{Archive, Serialize as RkyvSerialize, Deserialize as RkyvDeserialize, to_bytes};
use rkyv::rancor::Error as RkyvError;
use crate::keyword_shred::SPLIT_CHARS_INCLUSIVE;
use crate::utils::file_interaction_local_and_cloud::get_object_store;
use crate::{KeywordOneFile, ParquetSource, ProcessResult, MAX_CHUNK_SIZE_BYTES};
use crate::index_structure::index_files::{index_filename, IndexFile};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug)]
pub struct IndexFilters {
    // Version & validation
    pub version: u32,
    pub parquet_etag: String,
    pub parquet_size: u64,
    pub parquet_last_modified: u64,

    // Parquet metadata caching for efficient reads
    // Store the offset and length of Parquet metadata (footer) in the file
    // This allows reading metadata once and reusing it for all row groups
    pub parquet_metadata_offset: u64,
    pub parquet_metadata_length: u64,

    // Configuration
    pub error_rate: f64,
    pub split_chars_inclusive: Vec<Vec<char>>,

    // Data structures
    pub column_pool: ColumnPool,
    pub column_filters: StdHashMap<String, ColumnFilter>,
    pub global_filter: ColumnFilter,
    pub chunk_index: Vec<ChunkInfo>,
}

/// Information about a chunk in the data file.
///
/// Each chunk contains both a keyword list section and a data section.
/// The keyword list can be read independently for parent lookups without
/// loading the full occurrence data.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct ChunkInfo {
    /// First keyword in this chunk (for binary search)
    pub start_keyword: String,

    /// Byte offset in data.bin where this chunk starts
    pub offset: u64,

    /// Length in bytes of just the keyword list section
    /// Reading `[offset, offset + keyword_list_length]` gives `Vec<String>`
    pub keyword_list_length: u32,

    /// Total length in bytes of keyword list + data section
    /// Reading `[offset, offset + total_length]` gives complete chunk
    pub total_length: u32,

    /// Number of keywords in this chunk (dynamic, based on ~1MB serialized size target)
    pub count: u16,
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

/// Flattened row information with chunk-based parent tracking.
///
/// Parent tracking uses chunk number + position within chunk instead of global offset.
/// This eliminates the need for a separate global keyword array and supports the
/// 2-file index structure where keywords are organized in chunks.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlatRow {
    pub row: u32,
    pub additional_rows: u32,
    pub splits_matched: u16,

    /// Parent keyword chunk number (which chunk contains the parent)
    /// For parent lookup: chunk_index[parent_chunk] gives the chunk location
    pub parent_chunk: Option<u16>,

    /// Position within the parent chunk (0 to chunk size-1)
    /// Combined with parent_chunk: keywords[parent_position] in that chunk
    pub parent_position: Option<u16>,
}

/// Location of a keyword within the chunked structure.
/// Used during index building to track where each keyword ends up.
#[derive(Debug, Clone, Copy)]
struct KeywordLocation {
    chunk_number: u16,
    position_in_chunk: u16,
}

/// Converts a KeywordOneFile to a flattened KeywordDataFlat structure for serialization.
///
/// This function transforms the hierarchical keyword data structure into a flat, serializable
/// format. It processes each column (skipping the global aggregate at index 0), extracts
/// row group and row information, and converts parent keyword string references to
/// chunk+position pairs using the provided keyword location mapping.
///
/// The flattening process:
/// 1. Iterates through all columns except the aggregate bucket
/// 2. For each column, processes all row groups
/// 3. For each row group, converts all rows to FlatRow format
/// 4. Converts parent keyword Rc<str> references to (chunk, position) pairs
///
/// # Arguments
///
/// * `keyword_data` - Reference to the KeywordOneFile containing the hierarchical keyword data
/// * `keyword_to_location` - HashMap mapping keyword strings to their chunk locations,
///   used for converting parent keyword references to chunk+position pairs
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
/// - Parent keyword references are converted from Rc<str> to (chunk, position) pairs
/// - Handles missing data gracefully (uses Option for parent tracking)
fn convert_to_flat(
    keyword_data: &KeywordOneFile,
    keyword_to_location: &HashMap<&str, KeywordLocation>,
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
                        // Convert parent keyword string to chunk+position
                        let (parent_chunk, parent_position) = row.parent_keyword
                            .as_ref()
                            .and_then(|parent_str| keyword_to_location.get(parent_str.as_ref()))
                            .map(|loc| (Some(loc.chunk_number), Some(loc.position_in_chunk)))
                            .unwrap_or((None, None));

                        rows.push(FlatRow {
                            row: row.row,
                            additional_rows: row.additional_rows as u32,
                            splits_matched: row.splits_matched,
                            parent_chunk,
                            parent_position,
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
/// This function creates a 2-file distributed index structure from the processed keyword data.
/// It performs several key operations:
/// 1. Retrieves and stores Parquet metadata for validation
/// 2. Sorts keywords deterministically for consistent layout
/// 3. Assigns keywords to chunks and creates keyword location mapping
/// 4. For each chunk:
///    - Serializes keyword list (Vec<String>)
///    - Serializes keyword data (Vec<KeywordDataFlat>)
///    - Tracks both section lengths for independent access
/// 5. Constructs filters file with chunk index pointing to data.bin locations
///
/// The function creates two output files:
/// - **Filters file**: Contains Parquet metadata, bloom filters, column pool, and chunk index
/// - **Data file**: Contains chunked keyword lists + occurrence data, enabling range reads
///
/// # Chunk Structure in data.bin
///
/// Each chunk consists of two consecutive sections:
/// ```text
/// [Keyword List: Vec<String>]    ← keyword_list_length bytes
/// [Data: Vec<KeywordDataFlat>]   ← (total_length - keyword_list_length) bytes
/// ```
///
/// This allows reading just keyword strings for parent lookups without loading full data.
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
/// - `Ok(DistributedIndexFiles)` - Container with both serialized index files
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
/// - Data is chunked (size defined by METADATA_CHUNK_SIZE) to enable efficient partial loading
/// - Uses rkyv for zero-copy deserialization support
/// - Parent keyword references converted to chunk+position pairs for efficient lookup
/// - Keyword lists stored separately from data for lightweight parent resolution
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
///     let result = process_parquet_file(ParquetSource::from(parquet_bytes.clone()), None, None).await.unwrap();
///     build_distributed_index(
///         &result,
///         &ParquetSource::from(parquet_bytes),
///         0.01
///     ).await.unwrap();
/// # }
/// // index_files now contains filters and data ready to be written
/// ```
pub async fn build_distributed_index(
    result: &ProcessResult,
    source: &ParquetSource,
    error_rate: f64,
) -> Result<DistributedIndexFiles, Box<dyn std::error::Error + Send + Sync>> {
    // Get parquet metadata for validation and to cache metadata location
    let (parquet_etag, parquet_size, parquet_last_modified, parquet_metadata_offset, parquet_metadata_length) = match source {
        ParquetSource::Path(path) => {
            let (store, obj_path) = get_object_store(path).await?;
            let head = store.head(&obj_path).await?;

            // Read the last 8 bytes to get footer length
            // Parquet file structure: [...data...][FileMetaData][4-byte footer length][4-byte "PAR1"]
            let file_size = head.size;
            let footer_range = (file_size - 8)..file_size;
            let footer_bytes = store.get_range(&obj_path, footer_range).await?;
            let footer_slice = footer_bytes.to_vec();

            // Last 4 bytes are "PAR1", 4 bytes before that are footer length (little endian)
            let footer_len = u32::from_le_bytes([
                footer_slice[0],
                footer_slice[1],
                footer_slice[2],
                footer_slice[3],
            ]) as u64;

            // Metadata includes: FileMetaData + 4 bytes footer_len + 4 bytes "PAR1"
            let metadata_total_length = footer_len + 8;
            let metadata_offset = file_size - metadata_total_length;

            (
                head.e_tag.unwrap_or_else(|| "unknown".to_string()),
                file_size,
                head.last_modified.timestamp() as u64,
                metadata_offset,
                metadata_total_length,
            )
        }
        ParquetSource::Bytes(vec) => {
            // For in-memory bytes, calculate metadata location
            let file_size = vec.len() as u64;

            // Read the last 8 bytes to get footer length
            if file_size < 8 {
                return Err("Parquet file too small".into());
            }

            let footer_slice = &vec[(file_size - 8) as usize..];
            let footer_len = u32::from_le_bytes([
                footer_slice[0],
                footer_slice[1],
                footer_slice[2],
                footer_slice[3],
            ]) as u64;

            let metadata_total_length = footer_len + 8;
            let metadata_offset = file_size - metadata_total_length;

            ("".to_string(), file_size, 0, metadata_offset, metadata_total_length)
        }
    };

    // Sort keywords for deterministic layout
    let mut sorted_keywords: Vec<_> = result.keyword_map.iter().collect();
    sorted_keywords.sort_by(|a, b| a.0.cmp(b.0));

    // =========================================================================
    // Pass 1: Determine chunk boundaries based on ~1MB serialized size
    // =========================================================================

    // First, we need to estimate sizes and determine chunk boundaries
    let mut chunk_boundaries = Vec::new(); // Stores (start_idx, end_idx) for each chunk
    let mut current_chunk_start = 0;
    let mut current_chunk_estimated_size = 0;

    for (idx, (_keyword, keyword_data)) in sorted_keywords.iter().enumerate() {
        // Rough size estimation without full serialization
        // KeywordOneFile structure: column_references, row_groups, row_group_to_rows
        let mut estimated_size = 100; // Base overhead per keyword

        estimated_size += keyword_data.column_references.len() * 20; // Column references

        // Estimate row group data: row_group_to_rows is Vec<Vec<Vec<Row>>>
        // Outer Vec = columns, Middle Vec = row groups, Inner Vec = rows
        for col_rgs in &keyword_data.row_group_to_rows {
            for rg_rows in col_rgs {
                estimated_size += 10; // Row group overhead
                estimated_size += rg_rows.len() * 40; // ~40 bytes per row entry
            }
        }

        // If adding this keyword would exceed limit and we have at least one keyword, finalize chunk
        if current_chunk_estimated_size + estimated_size > MAX_CHUNK_SIZE_BYTES && idx > current_chunk_start {
            chunk_boundaries.push((current_chunk_start, idx));
            current_chunk_start = idx;
            current_chunk_estimated_size = 0;
        }

        current_chunk_estimated_size += estimated_size;
    }

    // Add final chunk
    chunk_boundaries.push((current_chunk_start, sorted_keywords.len()));

    println!("  Creating {} dynamic chunks (target: ~1MB per chunk)", chunk_boundaries.len());

    // =========================================================================
    // Pass 2: Build keyword → location mapping based on determined chunks
    // =========================================================================

    let keyword_to_location: HashMap<&str, KeywordLocation> = sorted_keywords.iter()
        .enumerate()
        .map(|(idx, (keyword, _))| {
            // Find which chunk this keyword belongs to
            let chunk_number = chunk_boundaries.iter()
                .position(|(start, end)| idx >= *start && idx < *end)
                .unwrap() as u16;

            // Position within that chunk
            let chunk_start = chunk_boundaries[chunk_number as usize].0;
            let position_in_chunk = (idx - chunk_start) as u16;

            (
                keyword.as_ref(),
                KeywordLocation { chunk_number, position_in_chunk }
            )
        })
        .collect();

    // =========================================================================
    // Pass 3: Build data file with dynamically-sized chunks
    // =========================================================================

    let mut data_file = Vec::new();
    let mut chunk_index = Vec::new();

    for (_chunk_idx, (start_idx, end_idx)) in chunk_boundaries.iter().enumerate() {
        let chunk = &sorted_keywords[*start_idx..*end_idx];
        let chunk_start_offset = data_file.len() as u64;
        let chunk_start_keyword = chunk[0].0;

        // Build keyword list for this chunk
        let keywords_in_chunk: Vec<String> = chunk.iter()
            .map(|(keyword, _)| keyword.to_string())
            .collect();

        // Build data list for this chunk
        let mut data_in_chunk = Vec::new();

        for (_keyword, keyword_data) in chunk {
            // Convert to flat structure with parent chunk+position mapping
            let flat_data = convert_to_flat(keyword_data, &keyword_to_location);
            data_in_chunk.push(flat_data);
        }

        // Serialize keyword list section
        let keyword_list_bytes = to_bytes::<RkyvError>(&keywords_in_chunk)
            .map_err(|e| format!("Failed to serialize keyword list: {}", e))?;
        let keyword_list_length = keyword_list_bytes.len() as u32;
        data_file.extend_from_slice(&keyword_list_bytes);

        // Serialize data section
        let data_bytes = to_bytes::<RkyvError>(&data_in_chunk)
            .map_err(|e| format!("Failed to serialize chunk data: {}", e))?;
        let data_length = data_bytes.len() as u32;
        data_file.extend_from_slice(&data_bytes);

        let total_length = keyword_list_length + data_length;
        let chunk_count = keywords_in_chunk.len() as u16;

        // Add to chunk index
        chunk_index.push(ChunkInfo {
            start_keyword: chunk_start_keyword.to_string(),
            offset: chunk_start_offset,
            keyword_list_length,
            total_length,
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
        parquet_metadata_offset,
        parquet_metadata_length,
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

    Ok(DistributedIndexFiles {
        filters: filters_bytes.to_vec(),
        data: data_file,
    })
}

/// Container for the distributed index files.
///
/// Contains the two components of the 2-file index structure:
/// - filters: Bloom filters, metadata, column pool, and chunk index
/// - data: Chunked keyword lists and occurrence data
pub struct DistributedIndexFiles {
    pub filters: Vec<u8>,
    pub data: Vec<u8>,
}

/// Saves distributed index files to a directory structure.
///
/// This function writes both components of the distributed index to disk in a structured
/// format. It creates an index directory (with `.index` extension) next to the Parquet file
/// and writes the filters and data files.
///
/// The directory structure created:
/// ```text
/// <base_path>.index/
/// ├── filters.rkyv (or <prefix>_filters.rkyv)
/// └── data.bin (or <prefix>_data.bin)
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
/// - Either of the two index files cannot be written
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
///     let result = process_parquet_file(ParquetSource::from(parquet_bytes.clone()), None, None).await.unwrap();
///
///     let index_files = build_distributed_index(&result, &ParquetSource::from(parquet_bytes), 0.01).await.unwrap();
///
///     // Save without prefix (path can be arbitrary for in-memory sources)
///     save_distributed_index(&index_files, "my_data.parquet", None).await.unwrap();
///     // Creates: my_data.parquet.index/filters.rkyv, data.bin
///
///     // Save with prefix for versioning
///     save_distributed_index(&index_files, "my_data.parquet", Some("v2")).await.unwrap();
///     // Creates: my_data.parquet.index/v2_filters.rkyv, v2_data.bin
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
    use bytes::Bytes;
    use object_store::PutPayload;

    // Build paths for index files
    let filters_path = format!("{}.index/{}", base_path, index_filename(IndexFile::Filters, prefix));
    let data_path = format!("{}.index/{}", base_path, index_filename(IndexFile::Data, prefix));

    // Write filters file using object store abstraction
    let (store, filters_obj_path) = get_object_store(&filters_path).await?;
    store.put(&filters_obj_path, PutPayload::from_bytes(Bytes::from(files.filters.clone()))).await?;

    // Write data file using object store abstraction
    let (store, data_obj_path) = get_object_store(&data_path).await?;
    store.put(&data_obj_path, PutPayload::from_bytes(Bytes::from(files.data.clone()))).await?;

    Ok(())
}