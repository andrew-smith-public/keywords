use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;
use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::*;
use bytes::{Buf, Bytes, BytesMut};
use futures::StreamExt;
use hashbrown::HashMap;
use indexmap::IndexSet;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, FileReader, Length, SerializedFileReader};
use tokio::sync::mpsc;
use crate::index_structure::column_filter::ColumnFilter;
use crate::keyword_shred::{build_column_keywords_map, perform_split, KeywordOneFile};
use crate::{ParquetSource, ProcessResult};
use crate::utils::column_pool::ColumnPool;
use crate::utils::file_interaction_local_and_cloud::get_object_store;

/// Files under this size are read entirely in a single request.
pub const TWO_MB: u64 = 2 * 1024 * 1024;

/// Amount of data to read for larger files that should normally cover parquet metadata without over reading
pub const ONE_MB: u64 = 1024 * 1024;

/// Parquet footer size: 4 bytes metadata length + 4 bytes "PAR1" magic number.
pub const FOOTER_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct CachedData {
    pub cached_file_data: Bytes,
    pub cached_range_start: u64   // Cache is ALWAYS the end of the file
}

/// Metadata with optional cached file data
#[derive(Debug, Clone)]
pub struct MetadataWithCache {
    pub parquet_source: ParquetSource,
    pub metadata: ParquetMetaData,
    pub cached_file_data: CachedData,
    pub file_size: u64
}

/// Information about a column chunk extracted from metadata
#[derive(Debug, Clone)]
struct ColumnChunkInfo {
    column_name: String,
    row_group: u16,
    column_index: usize,
    start_offset: u64,
    size: u64,
}

/// Column chunk with raw parquet bytes ready for processing
struct ColumnChunk {
    bytes: Bytes,
    column_name: String,
    row_group: u16,
    column_index: usize,
    start_offset: u64,
}

/// Extracts metadata length from Parquet footer bytes.
///
/// The Parquet file format stores metadata at the end of the file with a footer structure:
/// - Last 4 bytes: Magic number "PAR1" (validates this is a Parquet file)
/// - Previous 4 bytes: Metadata length as little-endian u32
///
/// This function validates the magic number and extracts the metadata length needed to
/// read the complete metadata section.
///
/// # Arguments
///
/// * `footer_bytes` - Byte slice containing at least the last 8 bytes of the Parquet file
///
/// # Returns
///
/// Returns `Result<usize, Box<dyn std::error::Error + Send + Sync>>`:
/// - `Ok(usize)` - The metadata length in bytes
/// - `Err` - If footer is invalid or too short
///
/// # Errors
///
/// Returns error if:
/// - Footer is shorter than 8 bytes (FOOTER_SIZE)
/// - Magic number is not "PAR1" (invalid Parquet file)
///
/// # Parquet Footer Format
///
/// ```text
/// [... file content ...]
/// [metadata bytes]
/// [4 bytes: metadata length (little-endian u32)]
/// [4 bytes: "PAR1" magic number]
/// ```
fn read_metadata_length(footer_bytes: &[u8]) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    if footer_bytes.len() < FOOTER_SIZE {
        return Err("Footer too short".into());
    }

    let magic = &footer_bytes[footer_bytes.len() - 4..];
    if magic != b"PAR1" {
        return Err("Invalid Parquet file - missing PAR1 magic number".into());
    }

    let length_bytes = &footer_bytes[footer_bytes.len() - 8..footer_bytes.len() - 4];
    let metadata_length = u32::from_le_bytes([
        length_bytes[0],
        length_bytes[1],
        length_bytes[2],
        length_bytes[3],
    ]) as usize;

    Ok(metadata_length)
}

/// Reads Parquet metadata using optimized range requests.
///
/// For files <= 2MB we read the whole file and cache the data.
///
/// For files > 2MB we use an efficient two-phase metadata reading strategy:
/// 1. **Initial read**: Fetch the last 1MB of the file (contains footer + likely all metadata)
/// 2. **Parse footer**: Determine the actual metadata size from the footer
/// 3. **Conditional fetch**: If 1MB was insufficient, fetch only the missing bytes
///
/// This approach minimizes network requests and data transfer for large Parquet files while
/// ensuring complete metadata retrieval.
///
/// # Arguments
///
/// * `parquet_source` - Path to the Parquet file within the object store or Bytes of the file
/// * `file_size` - Total size of the Parquet file in bytes (if available, will head the file if not
/// provided, or take the length of the bytes if the source is bytes)
///
/// # Returns
///
/// Returns `Result<MetadataWithCache, Box<dyn std::error::Error + Send + Sync>>`:
/// - `Ok(MetadataWithCache)` - Parsed Parquet metadata with cache of some or all data from the file
/// - `Err` - If range requests fail, parsing fails, or arithmetic underflow occurs
///
/// # Strategy Details
///
/// **Phase 1 - Initial Read:**
/// - Reads last 1MB (or entire file if <= 2MB)
/// - Typical Parquet metadata is 10-500KB, so 1MB covers most cases
///
/// **Phase 2 - Conditional Fetch:**
/// - Only if metadata > 1MB (rare)
/// - Fetches only the missing bytes before the initial read
/// - Combines with initial read to form complete metadata
///
/// # Performance
///
/// - **Typical case**: 1 range request (metadata < 1MB)
/// - **Worst case**: 2 range requests (very large metadata)
/// - Saves significant bandwidth compared to reading entire large files
///
/// # Errors
///
/// Returns error if:
/// - Range requests to object store fail
/// - Integer underflow in offset calculations
/// - Parquet metadata parsing fails
/// - File is not a valid Parquet file
async fn read_metadata(
    parquet_source: ParquetSource,
    file_size_in: Option<u64>,
) -> Result<MetadataWithCache, Box<dyn std::error::Error + Send + Sync>> {

    match &parquet_source {
        ParquetSource::Path(path_str) => {
            let (store, path) = get_object_store(path_str).await?;

            let file_size: u64 = if file_size_in.is_none() {
                let object_meta = store.head(&path).await?;
                object_meta.size
            } else {
                file_size_in.unwrap()
            };

            let initial_read_size :u64 = if file_size <= TWO_MB {
                file_size
            } else {
                ONE_MB.min(file_size)
            };

            let initial_start :u64 = file_size - initial_read_size;  // Subtraction is safe by definition of initial_read_size
            let initial_bytes = store.get_range(&path, initial_start..file_size).await?;

            let footer_offset :usize = initial_bytes.len().saturating_sub(FOOTER_SIZE);
            let metadata_length :usize = read_metadata_length(&initial_bytes[footer_offset..])?;
            let total_metadata_size :usize = metadata_length + FOOTER_SIZE;

            if total_metadata_size <= initial_bytes.len() {
                // Initial read contained all metadata
                let metadata_start = initial_bytes.len() - total_metadata_size;
                let metadata_bytes = initial_bytes.slice_ref(&initial_bytes[metadata_start..]);
                let reader = SerializedFileReader::new(metadata_bytes)?;
                let metadata = reader.metadata();

                Ok(MetadataWithCache {
                    metadata: metadata.clone(),
                    cached_file_data: CachedData {
                        cached_file_data: initial_bytes,
                        cached_range_start: initial_start
                    },
                    file_size,
                    parquet_source
                })
            } else {
                // Need additional bytes
                let additional_bytes_needed :usize = total_metadata_size - initial_bytes.len();
                let additional_start :u64 = initial_start.checked_sub(additional_bytes_needed as u64)
                    .ok_or("Subtraction Overflow on Parquet Metadata, Parquet file is corrupt")?;
                let additional_bytes = store.get_range(&path, additional_start..initial_start).await?;

                let mut combined_mut = BytesMut::with_capacity(initial_bytes.len() + additional_bytes.len());
                combined_mut.extend_from_slice(&initial_bytes);
                combined_mut.extend_from_slice(&additional_bytes);
                let combined: Bytes = combined_mut.freeze();

                let reader = SerializedFileReader::new(combined.clone())?;
                let metadata = reader.metadata();

                Ok(MetadataWithCache {
                    metadata: metadata.clone(),
                    cached_file_data: CachedData {
                        cached_file_data: combined,
                        cached_range_start: additional_start
                    },
                    file_size,
                    parquet_source
                })
            }
        }
        ParquetSource::Bytes(bytes) => {
            let reader = SerializedFileReader::new(bytes.clone())?;
            let metadata = reader.metadata();

            Ok(MetadataWithCache {
                metadata: metadata.clone(),
                file_size: bytes.len() as u64,
                cached_file_data: CachedData {
                    cached_file_data: bytes.clone(),
                    cached_range_start: 0
                },
                parquet_source
            })
        }
    }
}

/// Streams and processes a Parquet file with optimized single-request reading.
///
/// This internal function implements an efficient streaming architecture that:
/// 1. Streams the entire file from object store (single GET) OR uses cached bytes if available
/// 2. Extracts and processes columns in file order as bytes arrive
/// 3. Decodes all Arrow types to StringArray for keyword extraction
/// 4. Calls keyword splitting for each non-null string value
///
/// The function uses a producer-consumer pattern with channels to overlap I/O and processing.
///
/// # Arguments
///
/// * `file_path` - Path to the Parquet file (S3 or local)
/// * `excluded_columns` - Optional set of column names to skip during processing
/// * `metadata_with_cache` - Pre-loaded Parquet metadata with optional cached file data
/// * `keyword_map` - Mutable reference to the keyword map for storing extracted keywords
/// * `column_pool` - Mutable reference to the column pool for interning column names
///
/// # Returns
///
/// Returns `Result<(), Box<dyn std::error::Error + Send + Sync>>`:
/// - `Ok(())` - Processing completed successfully
/// - `Err` - If streaming fails, column processing fails, or channel communication fails
///
/// # Architecture
///
/// ```text
/// ┌─────────────┐      ┌──────────────┐      ┌────────────────┐
/// │ Object Store│─────▶│ Reader Task  │─────▶│ Processor Task │
/// │  / Cache    │      │ (spawned)    │      │  (main task)   │
/// └─────────────┘      └──────────────┘      └────────────────┘
///        │                    │                       │
///        │              Channel buffer           keyword_map
///        │              (1000 or 2 chunks)           │
///        └──────────▶ Zero-copy slicing ◀───────────┘
/// ```
///
/// # Performance Optimizations
///
/// - **Single GET request**: Entire file streamed once
/// - **Zero-copy slicing**: Direct Bytes slicing without copying
/// - **Cached path**: For small files, slices directly from cached bytes
/// - **Adaptive buffering**: 1000 chunks for small files, 2 for large files
/// - **Overlapped I/O**: Reader and processor run concurrently
pub(crate) async fn stream_and_process_parquet(
    excluded_columns: Option<HashSet<String>>,
    metadata_with_cache: &MetadataWithCache,
    keyword_map: &mut HashMap<Rc<str>, KeywordOneFile>,
    column_pool: &mut ColumnPool
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // Extract metadata
    let metadata = metadata_with_cache.metadata.clone();
    let cache = metadata_with_cache.cached_file_data.clone();

    // Extract all column chunks metadata from parquet metadata object, ordered by file position
    let column_chunks = extract_column_chunk_metadata(&metadata, &excluded_columns);

    if column_chunks.is_empty() {
        return Ok(());
    }

    // Create channel for passing column chunks from reader to processor
    //
    // Design decision: Process columns sequentially rather than in parallel
    // Current single-threaded approach simplifies memory management and avoids
    // contention on the shared keyword_map HashMap. Parallelizing would require
    // either sharded locks (DashMap) or thread-local maps with merge overhead.
    // For most files, I/O (not CPU) is a bottleneck, so parallelization wouldn't
    // improve throughput enough. Revisit if CPU-bound processing is identified.

    // Use high capacity for <=250MB files, limited for large files
    let channel_capacity = if metadata_with_cache.file_size < 250 * 1024 * 1024 {
        1000
    } else {
        2
    };
    let (tx, mut rx) = mpsc::channel::<ColumnChunk>(channel_capacity);

    let metadata_arc = Arc::new(metadata);
    let metadata_for_processor = Arc::clone(&metadata_arc);

    // Choose path based on whether we have cached bytes
    let reader_handle = if cache.cached_range_start == 0 {
        // Fast path: slice directly from cached bytes (zero-copy)
        tokio::spawn(async move {
            for chunk_info in column_chunks {
                let start = chunk_info.start_offset as usize;
                let end = start + chunk_info.size as usize;

                // Zero-copy slice from cached bytes
                let column_bytes = cache.cached_file_data.slice(start..end);

                tx.send(ColumnChunk {
                    bytes: column_bytes,
                    column_name: chunk_info.column_name,
                    row_group: chunk_info.row_group,
                    column_index: chunk_info.column_index,
                    start_offset: chunk_info.start_offset,
                })
                    .await
                    .expect("Failed to send column chunk to processor");
            }
        })
    } else {
        // Network path: stream from object store (except the end that we have already cached)
        // We should be guaranteed by design to have path information in the metadata_with_cache object
        let path_str: &str = match &metadata_with_cache.parquet_source {
            ParquetSource::Path(path) => {
                path.as_str()
            }
            ParquetSource::Bytes(_) => {
                panic!("Design error, should not be possible to get bytes parquet source in this else block")
            }
        };
        let (store, path) = get_object_store(path_str).await?;
        let get_result = store.get(&path).await?;
        let stream_file = get_result.into_stream();

        // Spawn reader task (handles streaming and buffering)
        tokio::spawn(async move {
            reader_task(stream_file, column_chunks, tx).await;
        })
    };

    // Processor task (decodes and processes columns)
    // Runs in main task to ensure sequential processing
    while let Some(column_chunk) = rx.recv().await {
        // Design decision: Rely on object_store crate's built-in retry logic for transient S3 failures
        // The object_store library already implements exponential backoff for transient errors.
        // Additional retry logic here would add complexity without clear benefit, since we can't
        // distinguish transient vs fatal errors at this layer. Fatal errors (corrupted Parquet,
        // invalid schemas) should fail fast. If more sophisticated error recovery is needed
        // (e.g., skip corrupted columns, continue with partial index), add explicit Result handling.
        process_column_chunk(column_chunk, &metadata_for_processor, keyword_map, column_pool);
    }

    // Ensure reader completed successfully
    reader_handle.await.expect("Reader task panicked");

    Ok(())
}

/// Extracts column chunk information from Parquet metadata, ordered by file position.
///
/// This function scans through all row groups and columns in the Parquet metadata, extracting
/// byte ranges for each column chunk. It filters out excluded columns and sorts the chunks by
/// file offset to enable efficient sequential reading.
///
/// # Arguments
///
/// * `metadata` - Reference to Parquet metadata containing row group and column information
/// * `excluded_columns` - Optional set of column names to skip
///
/// # Returns
///
/// Returns `Vec<ColumnChunkInfo>` containing information about each column chunk:
/// - Column name
/// - Row group index (as u16)
/// - Column index within the row group
/// - Start offset in the file
/// - Size in bytes
///
/// The vector is sorted by `start_offset` to enable sequential streaming.
///
/// # Panics
///
/// Panics if the file has more than `u16::MAX` (65,535) row groups, which exceeds the
/// supported limit.
fn extract_column_chunk_metadata(
    metadata: &ParquetMetaData,
    excluded_columns: &Option<HashSet<String>>
) -> Vec<ColumnChunkInfo> {
    let num_row_groups = metadata.num_row_groups();

    if num_row_groups > u16::MAX as usize {
        panic!(
            "Parquet file has {} row groups, but only {} row groups are supported (u16::MAX). \
             Please split the file into smaller files.",
            num_row_groups,
            u16::MAX
        );
    }

    let mut column_chunks = Vec::new();

    for rg_idx in 0..num_row_groups {
        let rg_metadata = metadata.row_group(rg_idx);

        for col_idx in 0..rg_metadata.num_columns() {
            let col_chunk = rg_metadata.column(col_idx);
            let col_desc = col_chunk.column_descr();
            let column_name = col_desc.name().to_string();

            // Skip excluded columns
            if excluded_columns.as_ref().is_some_and(|set| set.contains(&column_name)) {
                continue;
            }

            let (offset, size) = col_chunk.byte_range();

            column_chunks.push(ColumnChunkInfo {
                column_name,
                row_group: rg_idx as u16,
                column_index: col_idx,
                start_offset: offset,
                size,
            });
        }
    }

    // Sort by file position to process in order
    column_chunks.sort_by_key(|c| c.start_offset);

    column_chunks
}

/// Reader task that streams file data and sends column chunks to the processor.
///
/// This async task implements the producer side of the streaming architecture. It:
/// 1. Maintains a buffer of incoming bytes from the object store stream
/// 2. Identifies when enough bytes have arrived for the next column chunk
/// 3. Extracts column bytes using zero-copy slicing
/// 4. Sends complete column chunks to the processor via channel
///
/// The task handles gaps between columns by advancing the buffer appropriately.
///
/// # Arguments
///
/// * `stream` - Stream of byte chunks from the object store
/// * `column_chunks` - Vector of column chunk metadata, sorted by file position
/// * `tx` - Channel sender for transmitting column chunks to the processor
///
/// # Panics
///
/// This function panics if:
/// - Stream reading fails (network error, permission denied, etc.)
/// - Stream ends before all expected bytes are received
/// - Channel send fails (processor task terminated unexpectedly)
///
/// # Implementation Details
///
/// **Buffer Management:**
/// - Accumulates bytes from stream into `BytesMut` buffer
/// - Advances buffer to skip gaps between columns
/// - Uses `split_to` and `freeze` for zero-copy extraction
///
/// **Progress Tracking:**
/// - `current_file_position`: Tracks absolute position in file
/// - `bytes_needed_in_buffer`: Relative to current buffer start
/// - Handles partial reads by accumulating until sufficient bytes available
async fn reader_task(
    mut stream: impl futures::Stream<Item = Result<Bytes, object_store::Error>> + Unpin,
    column_chunks: Vec<ColumnChunkInfo>,
    tx: mpsc::Sender<ColumnChunk>,
) {
    let mut buffer = BytesMut::new();
    let mut current_file_position = 0u64;
    let mut chunk_iter = column_chunks.into_iter().peekable();

    while let Some(chunk_info) = chunk_iter.next() {
        let chunk_end_in_file = chunk_info.start_offset + chunk_info.size;
        let bytes_needed_in_buffer = chunk_end_in_file - current_file_position;

        // Read from stream until we have enough bytes for this column
        while (buffer.len() as u64) < bytes_needed_in_buffer {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    buffer.extend_from_slice(&chunk);
                }
                Some(Err(e)) => {
                    panic!("Failed to read from object store stream: {}", e);
                }
                None => {
                    panic!(
                        "Stream ended unexpectedly. Expected {} bytes but stream ended at position {}",
                        chunk_end_in_file,
                        current_file_position + buffer.len() as u64
                    );
                }
            }
        }

        // Drop any gap bytes before this column
        let chunk_start_in_buffer = (chunk_info.start_offset - current_file_position) as usize;
        if chunk_start_in_buffer > 0 {
            buffer.advance(chunk_start_in_buffer);
            current_file_position += chunk_start_in_buffer as u64;
        }

        // Extract column bytes (zero-copy via split_to + freeze)
        let column_bytes = buffer.split_to(chunk_info.size as usize).freeze();
        current_file_position += chunk_info.size;

        // Send to processor
        tx.send(ColumnChunk {
            bytes: column_bytes,
            column_name: chunk_info.column_name,
            row_group: chunk_info.row_group,
            column_index: chunk_info.column_index,
            start_offset: chunk_info.start_offset,
        })
            .await
            .expect("Failed to send column chunk to processor");
    }
}

/// Processes a single column chunk: decodes Arrow data and extracts keywords.
///
/// This function implements the consumer side of the streaming architecture. It:
/// 1. Creates a ChunkReader wrapper for the column bytes
/// 2. Builds an Arrow reader with proper projection for the single column
/// 3. Iterates through record batches (handles multiple batches per row group)
/// 4. Converts Arrow data to StringArray
/// 5. Calls keyword extraction for each string value
///
/// The function correctly handles multiple batches within a row group by tracking
/// cumulative row offsets.
///
/// # Arguments
///
/// * `column_chunk` - Column chunk with bytes and metadata
/// * `metadata` - Shared reference to Parquet metadata
/// * `keyword_map` - Mutable reference to keyword map for storing results
/// * `column_pool` - Mutable reference to column pool for name interning
///
/// # Panics
///
/// Panics if:
/// - Arrow metadata creation fails
/// - Parquet reader build fails
/// - Record batch reading fails
/// - Type casting fails (shouldn't happen with proper conversion)
///
/// # Multiple Batch Handling
///
/// Row groups can be split into multiple Arrow record batches. This function tracks
/// `cumulative_row_offset` to ensure row numbers are correct across all batches:
///
/// ```text
/// Row Group 0:
///   Batch 0: rows 0-999   (offset=0)
///   Batch 1: rows 1000-1999 (offset=1000)
///   Batch 2: rows 2000-2500 (offset=2000)
/// ```
fn process_column_chunk(
    column_chunk: ColumnChunk,
    metadata: &Arc<ParquetMetaData>,
    keyword_map: &mut HashMap<Rc<str>, KeywordOneFile>,
    column_pool: &mut ColumnPool
) {
    // Create a chunk reader for our column data
    let chunk_reader = ColumnBytesReader::new(column_chunk.bytes, column_chunk.start_offset);

    // Build projection mask to select only this column
    let schema = metadata.file_metadata().schema_descr();
    let projection = ProjectionMask::leaves(schema, vec![column_chunk.column_index]);

    // Create ArrowReaderMetadata from ParquetMetaData
    // This derives the Arrow schema from the Parquet schema
    let arrow_metadata = ArrowReaderMetadata::try_new(
        Arc::clone(metadata),
        ArrowReaderOptions::new(),
    ).expect("Failed to create arrow metadata");

    // Create arrow reader builder with the metadata
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        chunk_reader,
        arrow_metadata,
    );

    let mut record_batch_reader = builder
        .with_projection(projection)
        .with_row_groups(vec![column_chunk.row_group as usize])
        .build()
        .expect("Failed to build parquet reader");

    // Track cumulative row offset across batches within this row group
    let mut cumulative_row_offset = 0u32;

    // Decode and process batches
    while let Some(batch_result) = record_batch_reader.next() {
        let batch = batch_result.expect("Failed to read record batch");
        let batch_size = batch.num_rows() as u32;

        // Should only be one column due to projection
        for array in batch.columns() {
            // Convert Arrow array to StringArray (returns ArrayRef)
            let string_array_ref = cast(array, &DataType::Utf8).expect("Failed to cast array to string");

            // Downcast to concrete StringArray type
            let string_array = string_array_ref
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Cast to Utf8 should produce StringArray");

            // Call user callback with cumulative offset
            process_arrow_string_array(
                string_array,
                &column_chunk.column_name,
                column_chunk.row_group,
                cumulative_row_offset,
                keyword_map,
                column_pool
            );
        }

        // Increment offset for next batch
        cumulative_row_offset += batch_size;
    }
}

/// ChunkReader implementation that wraps a Bytes buffer with offset translation.
///
/// This struct adapts a `Bytes` buffer to implement Parquet's `ChunkReader` trait,
/// enabling the Parquet library to read column data from memory rather than a file.
/// It handles file-relative offsets by translating them to buffer-relative offsets.
///
/// # Fields
///
/// * `data` - The Bytes buffer containing the column chunk data
/// * `file_offset` - The absolute offset of this buffer's data in the original file
///
/// # Offset Translation
///
/// Parquet readers use file-absolute offsets. This reader translates them:
/// ```text
/// File: |------|XXXXXX|------|  (X = our data)
///              ^      ^
///              |      |
///       file_offset  file_offset + data.len()
///
/// Request: read at file_offset + 100
/// Translation: data[100..]
/// ```
struct ColumnBytesReader {
    data: Bytes,
    file_offset: u64,
}

impl ColumnBytesReader {
    /// Creates a new ColumnBytesReader.
    ///
    /// # Arguments
    ///
    /// * `data` - Bytes buffer containing the column chunk data
    /// * `file_offset` - Absolute offset of this data in the original Parquet file
    fn new(data: Bytes, file_offset: u64) -> Self {
        Self { data, file_offset }
    }
}

impl Length for ColumnBytesReader {
    /// Returns the length of the buffered data.
    ///
    /// # Returns
    ///
    /// The size of the column chunk data in bytes.
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl ChunkReader for ColumnBytesReader {
    type T = std::io::Cursor<Bytes>;

    /// Gets a reader starting at the specified file-absolute position.
    ///
    /// # Arguments
    ///
    /// * `start` - File-absolute offset to start reading from
    ///
    /// # Returns
    ///
    /// Returns `Result<Cursor<Bytes>, ParquetError>`:
    /// - `Ok` - Cursor positioned at the requested data
    /// - `Err` - If the start position is beyond the buffered data
    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let relative_start = start.saturating_sub(self.file_offset) as usize;
        if relative_start <= self.data.len() {
            Ok(std::io::Cursor::new(self.data.slice(relative_start..)))
        } else {
            Err(parquet::errors::ParquetError::General(format!(
                "Read start {} (relative: {}) is beyond data length {}",
                start,
                relative_start,
                self.data.len()
            )))
        }
    }

    /// Gets a byte range starting at the specified file-absolute position.
    ///
    /// # Arguments
    ///
    /// * `start` - File-absolute offset to start reading from
    /// * `length` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// Returns `Result<Bytes, ParquetError>`:
    /// - `Ok(Bytes)` - Zero-copy slice of the requested data
    /// - `Err` - If the requested range is beyond the buffered data
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let relative_start = start.saturating_sub(self.file_offset) as usize;
        let end = relative_start + length;

        if end <= self.data.len() {
            Ok(self.data.slice(relative_start..end))
        } else {
            Err(parquet::errors::ParquetError::General(format!(
                "Read range {}..{} (relative: {}..{}) is beyond data length {}",
                start,
                start + length as u64,
                relative_start,
                end,
                self.data.len()
            )))
        }
    }
}

/// Processes an Arrow StringArray efficiently by extracting and splitting keywords.
///
/// This function is the core keyword extraction routine. For each non-null, non-empty
/// string in the array, it:
/// 1. Gets a zero-copy string slice from the Arrow array
/// 2. Calculates the correct absolute row number using the provided offset
/// 3. Calls the keyword splitting function to extract and index keywords
///
/// The function handles multiple batches per row group correctly by adding `row_offset`
/// to the batch-local row index.
///
/// # Arguments
///
/// * `array` - The Arrow StringArray to process
/// * `column_name` - The name of the column being indexed
/// * `row_group` - The row group number (u16)
/// * `row_offset` - The cumulative row offset within the row group (for batch handling)
/// * `keyword_map` - Mutable reference to the HashMap storing indexed keywords
/// * `column_pool` - Mutable reference to the column pool for interning column names
///
/// # Performance Considerations
///
/// - **Zero-copy access**: Uses Arrow's `value()` method for direct string slice access
/// - **Efficient null handling**: Uses `is_valid()` for fast null checks
/// - **No allocations**: Passes string slices directly to avoid copies
/// - **Correct row numbers**: Adds offset to handle multiple batches within row groups
///
/// # Row Number Calculation
///
/// ```text
/// Row Group 0:
///   Batch 0 (offset=0):     rows 0-99    -> row_offset + 0, row_offset + 1, ...
///   Batch 1 (offset=100):   rows 100-199 -> row_offset + 0, row_offset + 1, ...
/// ```
///
/// Actual row number = `row_offset + row_idx`
pub(crate) fn process_arrow_string_array(
    array: &StringArray,
    column_name: &str,
    row_group: u16,
    row_offset: u32,
    keyword_map: &mut HashMap<Rc<str>, KeywordOneFile>,
    column_pool: &mut ColumnPool
) {
    let column_reference: u32 = column_pool.intern(column_name);
    for row_idx in 0..array.len() {
        // Skip null values efficiently
        if array.is_valid(row_idx) {
            // Get string slice directly from Arrow array (zero-copy)
            let value = array.value(row_idx);

            // Only process non-empty strings
            if !value.is_empty() {
                perform_split(
                    value,
                    column_reference,
                    row_group,
                    row_offset + row_idx as u32,  // Add offset to handle multiple batches
                    keyword_map
                );
            }
        }
    }
}

/// Processes a Parquet file and returns a complete keyword index with filters.
///
/// This is the main high-level entry point for building a searchable keyword index from
/// a Parquet file. It orchestrates the entire indexing pipeline:
///
/// 1. **Metadata Reading**: Loads Parquet metadata (caches small files)
/// 2. **Capacity Estimation**: Pre-allocates HashMap based on file characteristics
/// 3. **Column Processing**: Streams and extracts keywords from all (non-excluded) columns
/// 4. **Column Keywords Map**: Builds reverse index from columns to keywords
/// 5. **Bloom Filters**: Creates per-column and global bloom filters for fast lookups
///
/// # Arguments
///
/// * `source` - Parquet data source (ParquetSource::Path or ParquetSource::Bytes)
/// * `exclude_columns` - Optional set of column names to skip during indexing
/// * `error_rate` - Optional bloom filter false positive rate (default: 0.01 = 1%)
///
/// # Returns
///
/// Returns `Result<ProcessResult, Box<dyn std::error::Error + Send + Sync>>`:
/// - `Ok(ProcessResult)` - Complete index with keyword map, column pool, filters
/// - `Err` - If file access fails, processing fails, or filter creation fails
///
/// # Capacity Estimation
///
/// Uses heuristic formula to pre-allocate HashMap:
/// ```text
/// estimated_keywords = num_rows × num_columns × keywords_per_cell × dedup_factor
/// where:
///   keywords_per_cell = 2.0  (average splits per cell)
///   dedup_factor = 1.0 / (1.0 + log10(num_rows) / 2.0)  (accounts for duplication)
/// ```
///
/// # Bloom Filter Error Rate
///
/// - **Lower rate (0.001)**: More memory, fewer false positives
/// - **Higher rate (0.05)**: Less memory, more false positives
/// - **Recommended**: 0.01 (1%) balances memory and accuracy
///
/// # Errors
///
/// Returns error if:
/// - File doesn't exist or is inaccessible
/// - File is not a valid Parquet file
/// - Processing encounters invalid data
/// - Memory allocation fails
///
/// # Examples
///
/// ```no_run
/// # use std::collections::HashSet;
/// # use keywords::column_parquet_reader::process_parquet_file;
/// use keywords::ParquetSource;
/// #
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// // Process with defaults (1% error rate, all columns)
/// let result = process_parquet_file(ParquetSource::Path("data.parquet".to_string()), None, None).await?;
/// println!("Indexed {} unique keywords", result.keyword_map.len());
///
/// // Process with custom error rate and exclusions
/// let mut excluded = HashSet::new();
/// excluded.insert("id".to_string());
/// excluded.insert("timestamp".to_string());
/// let result = process_parquet_file(
///     ParquetSource::Path("s3://bucket/data.parquet".to_string()),
///     Some(excluded),
///     Some(0.001)  // 0.1% error rate
/// ).await?;
///
/// // Use bloom filters for fast lookups
/// if result.global_filter.might_contain("search_term") {
///     println!("'search_term' may exist in the file");
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Performance Notes
///
/// - Small files (<2MB) are cached, enabling very fast repeated access
/// - Large files are streamed with single GET request
/// - Pre-allocation reduces HashMap rehashing overhead
/// - Column pool interns column names to reduce memory usage
pub async fn process_parquet_file(
    source: ParquetSource,
    exclude_columns: Option<HashSet<String>>,
    error_rate: Option<f64>,
) -> Result<ProcessResult, Box<dyn std::error::Error + Send + Sync>> {
    // Default to 1% error rate if not specified
    let error_rate = error_rate.unwrap_or(0.01);

    // Read metadata and determine file path for streaming
    let metadata_with_cache: MetadataWithCache = read_metadata(source, None).await?;

    let num_rows: i64 = metadata_with_cache.metadata.file_metadata().num_rows();
    let num_cols: usize = metadata_with_cache.metadata.file_metadata().schema_descr().num_columns();
    let keywords_per_cell: f64 = 2.0;
    let dedup_factor: f64 = 1.0 / (1.0 + (num_rows as f64).log10() / 2.0);
    let estimated = (
        num_rows as f64
            * num_cols as f64
            * keywords_per_cell
            * dedup_factor
    ) as usize;

    // Create keyword map, column pool, and column keywords map
    let mut keyword_map = HashMap::with_capacity(estimated);
    let mut column_pool = ColumnPool::new();

    stream_and_process_parquet(
        exclude_columns,
        &metadata_with_cache,
        &mut keyword_map,
        &mut column_pool
    ).await?;

    // Process columns (will reuse cached data if available)
    let column_keywords_map = build_column_keywords_map(&keyword_map, &column_pool);

    // Build column filters
    let mut column_filters = HashMap::new();
    for (column_name, keywords) in &column_keywords_map {
        let filter = ColumnFilter::create_column_filter(keywords, error_rate);
        column_filters.insert(column_name.clone(), filter);
    }

    // Build global filter from all keywords across all columns
    let mut all_keywords = IndexSet::new();
    for keywords in column_keywords_map.values() {
        all_keywords.extend(keywords.iter().cloned());
    }
    let global_filter = ColumnFilter::create_column_filter(&all_keywords, error_rate);

    // Return the populated map, pool, column keywords map, filters, and global filter
    Ok(ProcessResult {
        keyword_map,
        column_pool,
        column_keywords_map,
        column_filters,
        global_filter,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    /// Helper function to write a RecordBatch to in-memory parquet bytes
    fn write_parquet_to_bytes(
        schema: Arc<Schema>,
        batch: RecordBatch,
        props: WriterProperties,
    ) -> Vec<u8> {
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buffer
    }

    /// Test that row numbers are continuous across multiple batches within a row group
    /// This test would have caught the original bug where row_idx was used directly
    /// instead of row_offset + row_idx
    #[tokio::test]
    async fn test_row_numbers_continuous_across_batches() {
        // Create schema with one column
        let schema = Arc::new(Schema::new(vec![
            Field::new("test_col", DataType::Utf8, false),
        ]));

        // Create properties with small row group to ensure multiple batches
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_write_batch_size(100) // Force small batches
            .set_max_row_group_size(1000) // Small row group
            .build();

        // Write 1000 rows - this will be split into multiple batches
        let values: Vec<String> = (0..1000)
            .map(|i| format!("row_{}", i))
            .collect();

        let array = Arc::new(StringArray::from(values.clone())) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write to in-memory bytes instead of disk
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        // Process the in-memory parquet
        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();
        let bytes = bytes::Bytes::from(parquet_bytes.clone());
        let metadata_with_cache = read_metadata(ParquetSource::Bytes(bytes.clone()), None).await.unwrap();

        stream_and_process_parquet(
            None,
            &metadata_with_cache,
            &mut keyword_map,
            &mut column_pool
        ).await.unwrap();

        // Verify that we have all 1000 unique keywords
        // Each "row_N" should appear exactly once at row N
        for i in 0..1000 {
            let keyword = format!("row_{}", i);
            let keyword_data = keyword_map.get(keyword.as_str())
                .expect(&format!("Keyword '{}' should exist in map", keyword));

            // Should have exactly one column
            assert_eq!(keyword_data.column_references.len(), 2,
                       "Should have 2 entries (aggregate + column)");

            // Check the rows for the first column (index 1, since 0 is aggregate)
            let rows = &keyword_data.row_group_to_rows[1][0]; // First row group, first entry
            assert_eq!(rows.len(), 1, "Should have exactly one row");

            // THE CRITICAL TEST: Row number should equal i
            assert_eq!(rows[0].row, i as u32,
                       "Keyword 'row_{}' should be at row {}, but was at row {}",
                       i, i, rows[0].row);
        }
    }

    /// Test with multiple row groups to ensure row numbering restarts correctly
    #[tokio::test]
    async fn test_multiple_row_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(500) // Force 2 row groups
            .build();

        // Write 1000 rows total (will create 2 row groups of 500 each)
        let values: Vec<String> = (0..1000)
            .map(|i| format!("rg{}_row{}", i / 500, i % 500))
            .collect();

        let array = Arc::new(StringArray::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write to in-memory bytes
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        // Process in-memory parquet
        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();
        let bytes = bytes::Bytes::from(parquet_bytes.clone());
        let metadata_with_cache = read_metadata(ParquetSource::Bytes(bytes.clone()), None).await.unwrap();

        stream_and_process_parquet(
            None,
            &metadata_with_cache,
            &mut keyword_map,
            &mut column_pool
        ).await.unwrap();

        // Verify row group 0, row 0
        let keyword = "rg0_row0";
        let data = keyword_map.get(keyword).unwrap();
        let rows = &data.row_group_to_rows[1][0];
        assert_eq!(rows[0].row, 0, "First row of row group 0 should be 0");

        // Verify row group 0, row 499
        let keyword = "rg0_row499";
        let data = keyword_map.get(keyword).unwrap();
        let rows = &data.row_group_to_rows[1][0];
        assert_eq!(rows[0].row, 499, "Last row of row group 0 should be 499");

        // Verify row group 1, row 0 (should restart at 0)
        let keyword = "rg1_row0";
        let data = keyword_map.get(keyword).unwrap();
        let row_groups = &data.row_groups[1]; // Column index 1
        assert_eq!(row_groups.len(), 1, "Should have one row group");
        assert_eq!(row_groups[0], 1, "Should be row group 1");
        let rows = &data.row_group_to_rows[1][0];
        assert_eq!(rows[0].row, 0, "First row of row group 1 should be 0 (restart)");

        // Verify row group 1, row 499
        let keyword = "rg1_row499";
        let data = keyword_map.get(keyword).unwrap();
        let rows = &data.row_group_to_rows[1][0];
        assert_eq!(rows[0].row, 499, "Last row of row group 1 should be 499");
    }

    /// Test that null values are skipped
    #[tokio::test]
    async fn test_null_values_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("nullable_col", DataType::Utf8, true), // nullable
        ]));

        let props = WriterProperties::builder().build();

        // Create array with some null values
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("value0"),
            None,
            Some("value2"),
            None,
            Some("value4"),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write to in-memory bytes
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        // Process in-memory parquet
        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();
        let bytes = bytes::Bytes::from(parquet_bytes.clone());
        let metadata_with_cache = read_metadata(ParquetSource::Bytes(bytes.clone()), None).await.unwrap();

        stream_and_process_parquet(
            None,
            &metadata_with_cache,
            &mut keyword_map,
            &mut column_pool
        ).await.unwrap();

        // Should only have 3 keywords (nulls skipped)
        assert!(keyword_map.contains_key("value0"));
        assert!(keyword_map.contains_key("value2"));
        assert!(keyword_map.contains_key("value4"));

        // Verify row numbers are correct (not counting nulls)
        let data = keyword_map.get("value0").unwrap();
        assert_eq!(data.row_group_to_rows[1][0][0].row, 0);

        let data = keyword_map.get("value2").unwrap();
        assert_eq!(data.row_group_to_rows[1][0][0].row, 2);

        let data = keyword_map.get("value4").unwrap();
        assert_eq!(data.row_group_to_rows[1][0][0].row, 4);
    }

    /// Test with empty strings (should be skipped)
    #[tokio::test]
    async fn test_empty_strings_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder().build();

        let array: ArrayRef = Arc::new(StringArray::from(vec![
            "value",
            "",  // empty
            "another",
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write to in-memory bytes
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        // Process in-memory parquet
        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();
        let bytes = bytes::Bytes::from(parquet_bytes.clone());
        let metadata_with_cache = read_metadata(ParquetSource::Bytes(bytes.clone()), None).await.unwrap();

        stream_and_process_parquet(
            None,
            &metadata_with_cache,
            &mut keyword_map,
            &mut column_pool
        ).await.unwrap();

        // Should only have 2 keywords (empty string skipped)
        assert!(keyword_map.contains_key("value"));
        assert!(keyword_map.contains_key("another"));
    }

    /// Test excluded columns functionality
    #[tokio::test]
    async fn test_excluded_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("include_col", DataType::Utf8, false),
            Field::new("exclude_col", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder().build();

        let col_a: ArrayRef = Arc::new(StringArray::from(vec!["included"]));
        let col_b: ArrayRef = Arc::new(StringArray::from(vec!["excluded"]));

        let batch = RecordBatch::try_new(schema.clone(), vec![col_a, col_b]).unwrap();

        // Write to in-memory bytes
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        // Process with exclusion
        let mut excluded = HashSet::new();
        excluded.insert("exclude_col".to_string());

        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();
        let bytes = bytes::Bytes::from(parquet_bytes.clone());
        let metadata_with_cache = read_metadata(ParquetSource::Bytes(bytes.clone()), None).await.unwrap();

        stream_and_process_parquet(
            Some(excluded),
            &metadata_with_cache,
            &mut keyword_map,
            &mut column_pool
        ).await.unwrap();

        // Should only have keyword from included column
        assert!(keyword_map.contains_key("included"));
        assert!(!keyword_map.contains_key("excluded"));

        // Column pool should only have included column and the default value for all columns
        assert_eq!(column_pool.strings.len(), 2);

        // Column pool should have included column, but not excluded
        assert!(column_pool.strings.contains(&"include_col".to_string()),
                "Column pool should contain 'include_col'");
        assert!(!column_pool.strings.contains(&"exclude_col".to_string()),
                "Column pool should NOT contain 'exclude_col'");
    }

    /// Test with realistic data - keywords with split characters
    #[tokio::test]
    async fn test_split_keywords() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder()
            .set_write_batch_size(2) // Force multiple batches with just 4 rows
            .build();

        // These will be split into multiple keywords
        let emails = vec![
            "user@example.com",
            "admin@test.org",
            "john-doe@company.net",
            "jane_smith@website.io",
        ];

        let array: ArrayRef = Arc::new(StringArray::from(emails.clone()));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write to in-memory bytes
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        // Process in-memory parquet
        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();
        let bytes = bytes::Bytes::from(parquet_bytes.clone());
        let metadata_with_cache = read_metadata(ParquetSource::Bytes(bytes.clone()), None).await.unwrap();

        stream_and_process_parquet(
            None,
            &metadata_with_cache,
            &mut keyword_map,
            &mut column_pool
        ).await.unwrap();

        // Verify split keywords exist
        assert!(keyword_map.contains_key("user"));
        assert!(keyword_map.contains_key("example"));
        assert!(keyword_map.contains_key("com"));
        assert!(keyword_map.contains_key("john"));
        assert!(keyword_map.contains_key("doe"));

        // Verify row numbers are correct across batches
        // "user" is in row 0 (first batch)
        let data = keyword_map.get("user").unwrap();
        assert_eq!(data.row_group_to_rows[1][0][0].row, 0);

        // "john" is in row 2 (second batch with offset 2)
        let data = keyword_map.get("john").unwrap();
        assert_eq!(data.row_group_to_rows[1][0][0].row, 2,
                   "Keyword 'john' should be at row 2");

        // "jane" is in row 3 (second batch with offset 2)
        let data = keyword_map.get("jane").unwrap();
        assert_eq!(data.row_group_to_rows[1][0][0].row, 3,
                   "Keyword 'jane' should be at row 3");
    }

    /// Regression test: verify the exact scenario that caused the bug
    #[tokio::test]
    async fn test_regression_batch_offset_bug() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder()
            .set_write_batch_size(3) // Force batches of 3 rows
            .build();

        // Create 10 rows with unique values
        let values: Vec<String> = vec![
            "unique_0".to_string(),
            "unique_1".to_string(),
            "unique_2".to_string(),
            "unique_3".to_string(),  // Start of batch 2
            "unique_4".to_string(),
            "unique_5".to_string(),
            "unique_6".to_string(),  // Start of batch 3
            "unique_7".to_string(),
            "unique_8".to_string(),
            "unique_9".to_string(),  // Start of batch 4
        ];

        let array: ArrayRef = Arc::new(StringArray::from(values));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Write to in-memory bytes
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        // Process in-memory parquet
        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();
        let bytes = bytes::Bytes::from(parquet_bytes.clone());
        let metadata_with_cache = read_metadata(ParquetSource::Bytes(bytes.clone()), None).await.unwrap();

        stream_and_process_parquet(
            None,
            &metadata_with_cache,
            &mut keyword_map,
            &mut column_pool
        ).await.unwrap();

        // THE BUG: Without the fix, all these would be at row 0, 1, or 2
        // because row_idx restarts for each batch

        // Batch 1 (rows 0-2)
        assert_eq!(keyword_map.get("unique_0").unwrap().row_group_to_rows[1][0][0].row, 0);
        assert_eq!(keyword_map.get("unique_1").unwrap().row_group_to_rows[1][0][0].row, 1);
        assert_eq!(keyword_map.get("unique_2").unwrap().row_group_to_rows[1][0][0].row, 2);

        // Batch 2 (rows 3-5) - BUG would have these at 0-2
        assert_eq!(keyword_map.get("unique_3").unwrap().row_group_to_rows[1][0][0].row, 3,
                   "BUG: unique_3 should be at row 3, not 0");
        assert_eq!(keyword_map.get("unique_4").unwrap().row_group_to_rows[1][0][0].row, 4,
                   "BUG: unique_4 should be at row 4, not 1");
        assert_eq!(keyword_map.get("unique_5").unwrap().row_group_to_rows[1][0][0].row, 5,
                   "BUG: unique_5 should be at row 5, not 2");

        // Batch 3 (rows 6-8) - BUG would have these at 0-2
        assert_eq!(keyword_map.get("unique_6").unwrap().row_group_to_rows[1][0][0].row, 6,
                   "BUG: unique_6 should be at row 6, not 0");
        assert_eq!(keyword_map.get("unique_7").unwrap().row_group_to_rows[1][0][0].row, 7,
                   "BUG: unique_7 should be at row 7, not 1");
        assert_eq!(keyword_map.get("unique_8").unwrap().row_group_to_rows[1][0][0].row, 8,
                   "BUG: unique_8 should be at row 8, not 2");

        // Batch 4 (row 9) - BUG would have this at 0
        assert_eq!(keyword_map.get("unique_9").unwrap().row_group_to_rows[1][0][0].row, 9,
                   "BUG: unique_9 should be at row 9, not 0");
    }

    /// Test metadata length extraction
    #[test]
    fn test_read_metadata_length() {
        // Valid footer
        let mut footer = vec![0u8; 8];
        footer[0..4].copy_from_slice(&100u32.to_le_bytes()); // metadata length
        footer[4..8].copy_from_slice(b"PAR1"); // magic

        let length = read_metadata_length(&footer).unwrap();
        assert_eq!(length, 100);
    }

    #[test]
    fn test_read_metadata_length_invalid_magic() {
        let mut footer = vec![0u8; 8];
        footer[0..4].copy_from_slice(&100u32.to_le_bytes());
        footer[4..8].copy_from_slice(b"WXYZ"); // wrong magic

        let result = read_metadata_length(&footer);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_metadata_length_too_short() {
        let footer = vec![0u8; 4]; // Too short
        let result = read_metadata_length(&footer);
        assert!(result.is_err());
    }
}