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
use std::collections::{HashSet as StdHashSet, HashMap as StdHashMap};
use std::path::Path;
use crate::index_data::{build_distributed_index, save_distributed_index, ColumnKeywordsFile};
use crate::utils::column_pool::ColumnPool;
use crate::index_structure::column_filter::ColumnFilter;
use crate::column_parquet_reader::process_parquet_file;
use crate::keyword_shred::KeywordOneFile;
use crate::searching::keyword_search::KeywordSearcher;
use crate::searching::search_results::SearchResult;

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
///
/// // From file path
/// let source = ParquetSource::Path("data.parquet".to_string());
///
/// // From bytes
/// let parquet_bytes = vec![/* parquet data */];
/// let source = ParquetSource::Bytes(parquet_bytes);
/// ```
#[derive(Debug, Clone)]
pub enum ParquetSource {
    /// Path to a Parquet file (local or remote like s3://)
    Path(String),
    /// In-memory Parquet data as bytes
    Bytes(Vec<u8>),
}

// ============================================================================
// Distributed Index Structures
// ============================================================================

/// Number of keywords per metadata chunk for partial loading
const METADATA_CHUNK_SIZE: usize = 1000;

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
    pub keywords_per_column: StdHashMap<String, usize>,

    /// Chunk information
    pub num_chunks: usize,
    pub chunk_size: usize,

    /// Index file sizes (in bytes)
    pub filters_size: u64,
    pub metadata_size: u64,
    pub data_size: u64,
    pub column_keywords_size: u64,
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
    println!("  metadata.rkyv: {} bytes ({:.2} KB)", files.metadata.len(), files.metadata.len() as f64 / 1024.0);
    println!("  data.bin: {} bytes ({:.2} KB)", files.data.len(), files.data.len() as f64 / 1024.0);
    println!("  column_keywords.rkyv: {} bytes ({:.2} KB)", files.column_keywords.len(), files.column_keywords.len() as f64 / 1024.0);

    let total_size = files.filters.len() + files.metadata.len() + files.data.len() + files.column_keywords.len();
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
#[must_use = "index information should be used"]
pub async fn get_index_info(
    parquet_path: &str,
    index_file_prefix: Option<&str>,
) -> Result<IndexInfo, Box<dyn std::error::Error + Send + Sync>> {
    use tokio::fs;
    use crate::index_structure::index_files::{index_filename, IndexFile};

    // Construct index directory path
    let index_dir = format!("{}.index", parquet_path);

    // Check if index exists
    if !Path::new(&index_dir).exists() {
        return Err(format!("Index directory not found: {}", index_dir).into());
    }

    // Get file sizes
    let filters_path = Path::new(&index_dir).join(index_filename(IndexFile::Filters, index_file_prefix));
    let metadata_path = Path::new(&index_dir).join(index_filename(IndexFile::Metadata, index_file_prefix));
    let data_path = Path::new(&index_dir).join(index_filename(IndexFile::Data, index_file_prefix));
    let column_keywords_path = Path::new(&index_dir).join(index_filename(IndexFile::ColumnKeywords, index_file_prefix));

    let filters_size = fs::metadata(&filters_path).await?.len();
    let metadata_size = fs::metadata(&metadata_path).await?.len();
    let data_size = fs::metadata(&data_path).await?.len();
    let column_keywords_size = fs::metadata(&column_keywords_path).await?.len();
    let total_size = filters_size + metadata_size + data_size + column_keywords_size;

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

    // Get total keywords count
    let total_keywords = searcher.all_keywords.len();

    // Load column keywords file to get keywords per column
    let column_keywords_bytes = fs::read(&column_keywords_path).await?;
    let mut column_keywords_aligned = rkyv::util::AlignedVec::<16>::new();
    column_keywords_aligned.extend_from_slice(&column_keywords_bytes);

    use rkyv::Archived;
    let column_keywords_archived: &Archived<ColumnKeywordsFile> = rkyv::access(&column_keywords_aligned)
        .map_err(|e: rkyv::rancor::Error| format!("Failed to access archived column keywords: {}", e))?;

    // Build keywords per column map
    let mut keywords_per_column = StdHashMap::new();
    for (col_name, keyword_indices) in column_keywords_archived.columns.iter() {
        keywords_per_column.insert(col_name.to_string(), keyword_indices.len());
    }

    Ok(IndexInfo {
        version,
        parquet_etag,
        parquet_size,
        parquet_last_modified,
        error_rate,
        num_columns,
        indexed_columns,
        total_keywords,
        keywords_per_column,
        num_chunks,
        chunk_size: METADATA_CHUNK_SIZE,
        filters_size,
        metadata_size,
        data_size,
        column_keywords_size,
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
#[must_use = "search results should be examined"]
pub async fn search(
    parquet_path: &str,
    search_for: &str,
    in_columns: Option<&str>,
    keyword_only: bool,
) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
    let searcher = KeywordSearcher::load(parquet_path, None).await?;
    searcher.search(search_for, in_columns, keyword_only)
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
#[must_use = "validation result should be checked"]
pub async fn validate_index(
    parquet_path: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // Check if index exists first
    if !index_exists(parquet_path) {
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
/// if index_exists("data.parquet") {
///     println!("Index found");
/// } else {
///     println!("Index not found - needs to be built");
/// }
/// ```
pub fn index_exists(parquet_path: &str) -> bool {
    let index_dir = format!("{}.index", parquet_path);
    Path::new(&index_dir).exists()
}
/// Build index in memory without writing to disk (test-only).
///
/// This function builds the index, serializes it, and deserializes it back
/// (testing the full serialization round-trip) but skips disk I/O.
/// Useful for fast tests that don't need to manage filesystem cleanup.
///
/// # Arguments
///
/// * `parquet_path` - Path to the Parquet file
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
    let error_rate = error_rate.unwrap_or(0.01);

    if error_rate < 0.0000000001 || error_rate > 0.5 {
        return Err(format!(
            "error_rate must be between 0.0000000001 and 0.5, got {}",
            error_rate
        ).into());
    }

    // Build index
    let result = process_parquet_file(source.clone(), exclude_columns, Some(error_rate)).await?;
    let files = build_distributed_index(&result, &source, error_rate).await?;

    // Deserialize (tests full serialization round-trip)
    KeywordSearcher::from_serialized(&files)
}