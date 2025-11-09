//! Keywords - Efficient Keyword Search for Parquet Files
//!
//! A high-performance library for building keyword indexes on Parquet files and performing
//! lightning-fast keyword searches. Works with both local filesystems and cloud storage (S3).
//!
//! # Overview
//!
//! This library provides:
//! - **Fast Indexing**: Build keyword indexes from Parquet files with hierarchical splitting
//! - **Fast Searches**: Keyword lookups using bloom filters, even on multi-GB files
//! - **Space Efficient**: Distributed index structure with configurable false positive rates
//! - **Cloud Support**: Seamless integration with S3 and local storage
//! - **Phrase Search**: Multi-token search with parent verification
//!
//! # Quick Start
//!
//! ```no_run
//! use keywords::{build_and_save_index, search};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Build an index
//!     build_and_save_index("data.parquet", None, None, None).await?;
//!
//!     // Search for a keyword
//!     let result = search("data.parquet", "example", None, true).await?;
//!     if let Some(data) = result.verified_matches {
//!         println!("Found in columns: {:?}", data.columns);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Performance
//!
//! - **Index building**: Optimized I/O with minimal memory allocations
//! - **Search time**: O(1) bloom filter check + O(log n) binary search
//! - **Memory**: Only index loaded during searches, not the Parquet data itself
pub mod index_data;
pub mod searching;
pub mod index_structure;
pub mod keyword_shred;
#[cfg(test)]
pub mod unit_tests;
pub mod utils;
pub mod column_parquet_reader;

use hashbrown::HashMap;
use indexmap::IndexSet;
use std::rc::Rc;
use std::collections::HashSet as StdHashSet;
use bytes::Bytes;
use crate::index_data::{build_distributed_index, save_distributed_index};
use crate::utils::column_pool::ColumnPool;
use crate::index_structure::column_filter::ColumnFilter;
use crate::column_parquet_reader::process_parquet_file;
use crate::keyword_shred::KeywordOneFile;
use crate::searching::keyword_search::KeywordSearcher;
use crate::searching::search_results::SearchResult;
use crate::utils::file_interaction_local_and_cloud::get_object_store;

// ============================================================================
// Public Types
// ============================================================================

/// Source of Parquet data for indexing.
///
/// Allows flexibility in providing Parquet data either from a file path
/// or directly from memory as bytes. The bytes variant is particularly
/// useful for testing and scenarios where the Parquet file is generated
/// in-memory.
///
/// # Examples
///
/// ```no_run
/// use keywords::ParquetSource;
/// use bytes::Bytes;
///
/// // From file path
/// let source = ParquetSource::Path("data.parquet".to_string());
///
/// // From bytes
/// let parquet_bytes = vec![/* parquet data */];
/// let source = ParquetSource::from(parquet_bytes);
/// ```
#[derive(Debug, Clone)]
pub enum ParquetSource {
    /// Path to a Parquet file (local or remote like s3://)
    Path(String),
    /// In-memory Parquet data as bytes
    Bytes(Bytes),
}

impl From<Vec<u8>> for ParquetSource {
    fn from(vec: Vec<u8>) -> Self {
        ParquetSource::Bytes(Bytes::from(vec))
    }
}

// ============================================================================
// Distributed Index Structures
// ============================================================================

/// Maximum size per metadata chunk in bytes for partial loading (1MB)
const MAX_CHUNK_SIZE_BYTES: usize = 1_000_000;

/// Result of processing a Parquet file.
/// Contains the keyword map, the column name pool, column keywords map, column filters, and global filter.
#[derive(Debug)]
pub struct ProcessResult {
    pub keyword_map: HashMap<Rc<str>, KeywordOneFile>,
    pub column_pool: ColumnPool,
    pub column_keywords_map: HashMap<Rc<str>, IndexSet<Rc<str>>>,
    pub column_filters: HashMap<Rc<str>, ColumnFilter>,
    pub global_filter: ColumnFilter,
}

/// Information about a keyword index for a Parquet file.
///
/// Contains comprehensive metadata about the index including version information,
/// validation data, column details, keyword counts, and file sizes.
///
/// # Examples
///
/// ```no_run
/// use keywords::get_index_info;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let info = get_index_info("data.parquet", None).await?;
///     println!("Index has {} keywords across {} columns",
///         info.total_keywords, info.num_columns);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index format version
    pub version: u32,

    /// Parquet file validation data
    pub parquet_etag: String,
    pub parquet_size: u64,
    pub parquet_last_modified: u64,

    /// Index configuration
    pub error_rate: f64,

    /// Column information
    pub num_columns: usize,
    pub indexed_columns: Vec<String>,

    /// Keyword statistics
    pub total_keywords: usize,

    /// Chunk information
    pub num_chunks: usize,
    pub max_chunk_size_bytes: usize,  // Target max size per chunk in bytes

    /// Index file sizes (in bytes)
    pub filters_size: u64,
    pub data_size: u64,
    pub total_size: u64,
}

/// Build and save distributed index in one step
pub async fn build_and_save_index(
    parquet_path: &str,
    exclude_columns: Option<StdHashSet<String>>,
    error_rate: Option<f64>,
    index_file_prefix: Option<&str>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let error_rate = error_rate.unwrap_or(0.01);

    // Validate error_rate is within acceptable range
    if error_rate < 0.0000000001 || error_rate > 0.5 {
        return Err(format!(
            "error_rate must be between 0.0000000001 and 0.5, got {}",
            error_rate
        ).into());
    }

    println!("Processing parquet file...");
    let source = ParquetSource::Path(parquet_path.to_string());
    let result = process_parquet_file(source.clone(), exclude_columns, Some(error_rate)).await?;

    println!("Building distributed index...");
    let files = build_distributed_index(&result, &source, error_rate).await?;

    println!("Saving index files...");
    save_distributed_index(&files, parquet_path, index_file_prefix).await?;

    println!("Index created successfully!");
    println!("  filters.rkyv: {} bytes ({:.2} KB)", files.filters.len(), files.filters.len() as f64 / 1024.0);
    println!("  data.bin: {} bytes ({:.2} KB)", files.data.len(), files.data.len() as f64 / 1024.0);

    let total_size = files.filters.len() + files.data.len();
    println!("  Total: {} bytes ({:.2} MB)", total_size, total_size as f64 / (1024.0 * 1024.0));

    Ok(())
}

/// Get comprehensive information about an index.
///
/// This function loads the index files and extracts detailed metadata including:
/// - Version and validation information
/// - Parquet file metadata
/// - Index configuration (error rate)
/// - Column information and keyword counts
/// - Chunk information
/// - File sizes for all index components
///
/// # Arguments
///
/// * `parquet_path` - Path to the Parquet file (index is at `{parquet_path}.index`)
/// * `index_file_prefix` - Optional prefix for index files (e.g., "v2_")
///
/// # Returns
///
/// An `IndexInfo` struct containing all index metadata
///
/// # Errors
///
/// Returns error if:
/// - Index files cannot be found or read
/// - Index files are corrupted or cannot be deserialized
///
/// # Examples
///
/// ```no_run
/// use keywords::get_index_info;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let info = get_index_info("data.parquet", None).await?;
///
///     println!("Index Version: {}", info.version);
///     println!("Total Keywords: {}", info.total_keywords);
///     println!("Indexed Columns: {:?}", info.indexed_columns);
///     println!("Total Index Size: {:.2} MB", info.total_size as f64 / (1024.0 * 1024.0));
///
///     Ok(())
/// }
/// ```
pub async fn get_index_info(
    parquet_path: &str,
    index_file_prefix: Option<&str>,
) -> Result<IndexInfo, Box<dyn std::error::Error + Send + Sync>> {
    use crate::index_structure::index_files::{index_filename, IndexFile};

    // Build paths for index files
    let filters_path = format!("{}.index/{}", parquet_path,
                               index_filename(IndexFile::Filters, index_file_prefix));
    let data_path = format!("{}.index/{}", parquet_path,
                            index_filename(IndexFile::Data, index_file_prefix));

    // Get file sizes using object store abstraction
    let (store, filters_obj_path) = get_object_store(&filters_path).await?;
    let filters_meta = store.head(&filters_obj_path).await?;
    let filters_size = filters_meta.size;

    let (store, data_obj_path) = get_object_store(&data_path).await?;
    let data_meta = store.head(&data_obj_path).await?;
    let data_size = data_meta.size;

    let total_size = filters_size + data_size;

    // Load the searcher to access index data
    let searcher = KeywordSearcher::load(parquet_path, index_file_prefix).await?;

    // Extract information from filters
    let version = searcher.filters.version;
    let parquet_etag = searcher.filters.parquet_etag.clone();
    let parquet_size = searcher.filters.parquet_size;
    let parquet_last_modified = searcher.filters.parquet_last_modified;
    let error_rate = searcher.filters.error_rate;
    let num_chunks = searcher.filters.chunk_index.len();

    // Get column information (skip index 0 which is reserved for "All Columns")
    let indexed_columns: Vec<String> = searcher.filters.column_pool.strings
        .iter()
        .skip(1)
        .map(|s| s.to_string())
        .collect();
    let num_columns = indexed_columns.len();

    // Get total keywords count by summing all chunks
    let total_keywords: usize = searcher.filters.chunk_index.iter()
        .map(|chunk| chunk.count as usize)
        .sum();

    Ok(IndexInfo {
        version,
        parquet_etag,
        parquet_size,
        parquet_last_modified,
        error_rate,
        num_columns,
        indexed_columns,
        total_keywords,
        num_chunks,
        max_chunk_size_bytes: MAX_CHUNK_SIZE_BYTES,
        filters_size,
        data_size,
        total_size,
    })
}


/// Search for keywords or phrases in a Parquet file.
///
/// This is a convenience function that loads the index, performs the search,
/// and returns the result. For multiple searches, use `KeywordSearcher::load()`
/// instead to avoid reloading the index each time.
///
/// # Arguments
///
/// * `parquet_path` - Path to the Parquet file (index is at `{parquet_path}.index`)
/// * `search_for` - The text to search for (keyword or phrase)
/// * `in_columns` - Optional column filter:
///   - `None` - Search all columns (uses optimized index 0)
///   - `Some("column")` - Search only specified column
/// * `keyword_only` - Search mode:
///   - `true` - Search for exact keyword only
///   - `false` - Split into tokens and do phrase search (default, more flexible)
///
/// # Returns
///
/// A `SearchResult` containing:
/// - `verified_matches` - Guaranteed correct matches (can answer directly)
/// - `needs_verification` - Potential matches requiring Parquet read
///
/// Returns `Err` if index cannot be loaded or search fails.
///
/// # Examples
///
/// **Simple keyword search:**
/// ```no_run
/// # use keywords::search;
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let result = search("data.parquet", "alice", None, true).await?;
///
/// if let Some(verified) = result.verified_matches {
///     println!("Found in columns: {:?}", verified.columns);
/// }
/// # Ok(())
/// # }
/// ```
///
/// **Phrase search (default mode):**
/// ```no_run
/// # use keywords::search;
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let result = search("data.parquet", "user@example.com", None, false).await?;
///
/// if let Some(verified) = result.verified_matches {
///     println!("Verified matches: {}", verified.total_occurrences);
/// }
///
/// if let Some(needs_check) = result.needs_verification {
///     println!("Need to verify: {}", needs_check.total_occurrences);
///     // Read Parquet file to verify these
/// }
/// # Ok(())
/// # }
/// ```
///
/// **Search specific column:**
/// ```no_run
/// # use keywords::search;
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let result = search("data.parquet", "admin", Some("username"), false).await?;
///
/// if result.found {
///     println!("Found 'admin' in username column");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn search(
    parquet_path: &str,
    search_for: &str,
    in_columns: Option<&str>,
    keyword_only: bool,
) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
    let searcher = KeywordSearcher::load(parquet_path, None).await?;
    searcher.search(search_for, in_columns, keyword_only).await
}


/// Check if the index exists and is up-to-date with the parquet file
///
/// Validates that:
/// 1. The index directory exists
/// 2. The parquet file size matches what's in the index
/// 3. The parquet file etag matches (if available)
/// 4. The parquet file last modified time matches
///
/// # Arguments
/// * `parquet_path` - Path to the parquet file
///
/// # Returns
/// * `Ok(true)` - Index exists and is valid
/// * `Ok(false)` - Index doesn't exist or is out of date
/// * `Err(...)` - Error checking the index
///
/// # Example
/// ```no_run
/// # use keywords::validate_index;
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// if validate_index("data.parquet").await? {
///     println!("Index is up to date");
/// } else {
///     println!("Index needs to be rebuilt");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn validate_index(
    parquet_path: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // Check if index exists first
    if !index_exists(parquet_path).await {
        return Ok(false);
    }

    // Load the index and validate against parquet file
    let searcher = KeywordSearcher::load(parquet_path, None).await?;
    searcher.validate_index(&ParquetSource::Path(parquet_path.to_string())).await
}

/// Check if an index exists for the parquet file
///
/// This is a quick, synchronous check that only verifies the index directory exists.
/// It does not validate that the index is up-to-date. For that, use `validate_index()`.
///
/// # Arguments
/// * `parquet_path` - Path to the parquet file
///
/// # Returns
/// * `true` - Index directory exists
/// * `false` - Index directory does not exist
///
/// # Example
/// ```no_run
/// # use keywords::index_exists;
/// # #[tokio::main]
/// # async fn main() {
/// if index_exists("data.parquet").await {
///     println!("Index found");
/// } else {
///     println!("Index not found - needs to be built");
/// }
/// # }
/// ```
pub async fn index_exists(parquet_path: &str) -> bool {
    // Try to check if filters file exists using object store abstraction
    let filters_path = format!("{}.index/{}", parquet_path,
                               crate::index_structure::index_files::index_filename(
                                   crate::index_structure::index_files::IndexFile::Filters, None));

    match get_object_store(&filters_path).await {
        Ok((store, path)) => store.head(&path).await.is_ok(),
        Err(_) => false,
    }
}

/// Build index in memory without writing to disk (test-only).
///
/// This function builds the index using the memory:// protocol, enabling
/// fast in-memory indexing for tests without filesystem cleanup.
///
/// # Arguments
///
/// * `source` - Parquet data source (Path or Bytes)
/// * `exclude_columns` - Optional set of column names to exclude from indexing
/// * `error_rate` - Bloom filter false positive rate (default: 0.01)
///
/// # Returns
///
/// `Ok(KeywordSearcher)` ready for searches
///
/// # Errors
///
/// Returns error if Parquet processing, index building, or serialization fails
#[cfg(test)]
pub async fn build_index_in_memory(
    source: ParquetSource,
    exclude_columns: Option<StdHashSet<String>>,
    error_rate: Option<f64>,
) -> Result<KeywordSearcher, Box<dyn std::error::Error + Send + Sync>> {
    use crate::utils::file_interaction_local_and_cloud::register_memory_file;

    let error_rate = error_rate.unwrap_or(0.01);

    if error_rate < 0.0000000001 || error_rate > 0.5 {
        return Err(format!(
            "error_rate must be between 0.0000000001 and 0.5, got {}",
            error_rate
        ).into());
    }

    // Convert source to a memory:// path
    let memory_path = match source {
        ParquetSource::Path(ref p) if p.starts_with("memory://") => {
            // Already a memory path
            p.clone()
        }
        ParquetSource::Path(ref p) => {
            // Regular path, use as-is (will use local filesystem)
            p.clone()
        }
        ParquetSource::Bytes(ref bytes) => {
            // Auto-register bytes to memory:// with timestamp-based ID
            use std::time::{SystemTime, UNIX_EPOCH};
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("memory://temp-{}.parquet", timestamp);
            register_memory_file(&path, bytes.clone()).await?;
            path
        }
    };

    // Build index using memory path
    let result = process_parquet_file(source, exclude_columns, Some(error_rate)).await?;
    let files = build_distributed_index(&result, &ParquetSource::Path(memory_path.clone()), error_rate).await?;

    // Save to memory using the abstraction
    save_distributed_index(&files, &memory_path, None).await?;

    // Load and return searcher
    KeywordSearcher::load(&memory_path, None).await
}