//! Keyword search functionality for Parquet file indexes.
//!
//! This module provides efficient keyword search capabilities using bloom filter-based indexes.
//! The searcher loads a pre-built index and performs fast lookups to find which rows and columns
//! contain specific keywords, without reading the actual Parquet file.
//!
//! # Architecture
//!
//! The search process works in multiple stages:
//!
//! 1. **Global Filter Check**: Fast rejection using bloom filter (~1% false positive rate)
//! 2. **Chunk Location**: Binary search to find the data chunk containing the keyword
//! 3. **Exact Match**: Precise lookup within the chunk
//! 4. **Data Retrieval**: Load row group and row range information
//!
//! # Performance
//!
//! - **Search time**: O(1) for bloom filter check + O(log n) for chunk lookup
//! - **Memory**: Only filters loaded in memory, data chunks read on-demand
//! - **False positives**: <1% (configurable during index build)
//!
//! # Basic Usage
//!
//! ```no_run
//! use keywords::searching::keyword_search::KeywordSearcher;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Load the index
//!     let searcher = KeywordSearcher::load("data.parquet", None).await?;
//!
//!     // Search for a keyword
//!     let result = searcher.search("john.doe@example.com", None, true).await?;
//!
//!     if result.found {
//!         println!("Found in {} columns", result.verified_matches.unwrap().columns.len());
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use rkyv::Archived;
use rkyv::util::AlignedVec;
use rkyv::rancor::Error as RkyvError;
use crate::index_data::{IndexFilters, KeywordDataFlat, ChunkInfo};
use crate::index_structure::column_filter::ColumnFilter;
use crate::index_structure::index_files::{index_filename, IndexFile};
use crate::ParquetSource;
use crate::searching::search_results::*;
use crate::utils::file_interaction_local_and_cloud::get_object_store;

/// Helper function to convert sorted row numbers into ranges
fn rows_to_ranges(sorted_rows: &[u32]) -> Vec<CombinedRowRange> {
    if sorted_rows.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut start = sorted_rows[0];
    let mut end = sorted_rows[0];

    for &row in &sorted_rows[1..] {
        if row == end + 1 {
            end = row;
        } else {
            ranges.push(CombinedRowRange { start_row: start, end_row: end });
            start = row;
            end = row;
        }
    }

    ranges.push(CombinedRowRange { start_row: start, end_row: end });
    ranges
}

/// Handle for searching keywords in a distributed index.
///
/// The searcher is created by loading a pre-built index from disk. Once loaded,
/// it can perform multiple keyword searches efficiently. The searcher keeps filters
/// in memory and reads data chunks on-demand from the data.bin file.
///
/// # Index Structure
///
/// The index consists of:
/// - **Bloom filters**: For fast negative lookups (global and per-column)
/// - **Chunk index**: Maps keyword ranges to byte locations in data.bin
/// - **Data file**: Chunked keyword lists + occurrence data
///
/// # Thread Safety
///
/// `KeywordSearcher` is not `Sync` due to internal buffer management. Create one
/// instance per thread or wrap in appropriate synchronization primitives.
///
/// # Examples
///
/// ```no_run
/// use keywords::searching::keyword_search::KeywordSearcher;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     // Load once, search many times
///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
///
///     // Perform multiple searches
///     for keyword in &["alice", "bob", "charlie"] {
///         let result = searcher.search(keyword, None, true).await?;
///         println!("{}: {}", keyword, result.found);
///     }
///
///     Ok(())
/// }
/// ```
pub struct KeywordSearcher {
    /// Filters containing bloom filters, metadata, column pool, and chunk index
    pub filters: IndexFilters,
    /// Directory path for index files
    pub(super) index_dir: String,
    /// Optional prefix for index files (e.g., "v1_", "test_")
    pub(super) index_file_prefix: Option<String>,
}

impl KeywordSearcher {
    /// Create searcher from serialized index data (used for testing and after load).
    ///
    /// # Arguments
    ///
    /// * `files` - Serialized index files containing filters and data
    /// * `index_dir` - Directory path for index files
    /// * `index_file_prefix` - Optional prefix for index files
    ///
    /// # Returns
    ///
    /// `Ok(KeywordSearcher)` ready to use for searches
    ///
    /// # Errors
    ///
    /// Returns error if rkyv deserialization fails
    pub fn from_serialized(
        files: &crate::index_data::DistributedIndexFiles,
        index_dir: String,
        index_file_prefix: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Copy to aligned buffer
        let mut filters_bytes = AlignedVec::<16>::new();
        filters_bytes.extend_from_slice(&files.filters);

        // Deserialize filters
        let archived_filters: &Archived<IndexFilters> = rkyv::access(&filters_bytes)
            .map_err(|e: RkyvError| format!("Failed to access archived filters: {}", e))?;

        let filters = IndexFilters {
            version: archived_filters.version.to_native(),
            parquet_etag: archived_filters.parquet_etag.to_string(),
            parquet_size: archived_filters.parquet_size.to_native(),
            parquet_last_modified: archived_filters.parquet_last_modified.to_native(),
            parquet_metadata_offset: archived_filters.parquet_metadata_offset.to_native(),
            parquet_metadata_length: archived_filters.parquet_metadata_length.to_native(),
            error_rate: archived_filters.error_rate.to_native(),
            split_chars_inclusive: archived_filters.split_chars_inclusive.iter()
                .map(|v| v.iter().map(|c| char::from(*c)).collect())
                .collect(),
            column_pool: {
                let mut pool = crate::utils::column_pool::ColumnPool::new();
                pool.strings = archived_filters.column_pool.strings.iter()
                    .map(|s| s.to_string())
                    .collect();
                pool.rebuild_lookup();
                pool
            },
            column_filters: archived_filters.column_filters.iter()
                .map(|(k, v)| {
                    let filter = match v {
                        rkyv::Archived::<ColumnFilter>::BloomFilter { data, num_hashes, num_bits } => {
                            ColumnFilter::BloomFilter {
                                data: data.to_vec(),
                                num_hashes: num_hashes.to_native(),
                                num_bits: num_bits.to_native(),
                            }
                        }
                        rkyv::Archived::<ColumnFilter>::RkyvHashSet(bytes) => {
                            ColumnFilter::RkyvHashSet(bytes.to_vec())
                        }
                    };
                    (k.to_string(), filter)
                })
                .collect(),
            global_filter: match &archived_filters.global_filter {
                rkyv::Archived::<ColumnFilter>::BloomFilter { data, num_hashes, num_bits } => {
                    ColumnFilter::BloomFilter {
                        data: data.to_vec(),
                        num_hashes: num_hashes.to_native(),
                        num_bits: num_bits.to_native(),
                    }
                }
                rkyv::Archived::<ColumnFilter>::RkyvHashSet(bytes) => {
                    ColumnFilter::RkyvHashSet(bytes.to_vec())
                }
            },
            chunk_index: archived_filters.chunk_index.iter()
                .map(|chunk| ChunkInfo {
                    start_keyword: chunk.start_keyword.to_string(),
                    offset: chunk.offset.to_native(),
                    keyword_list_length: chunk.keyword_list_length.to_native(),
                    total_length: chunk.total_length.to_native(),
                    count: chunk.count.to_native(),
                })
                .collect(),
        };

        Ok(Self {
            filters,
            index_dir,
            index_file_prefix,
        })
    }

    /// Load the distributed index from disk.
    ///
    /// Loads all index files into memory and prepares the searcher for keyword lookups.
    /// The index directory is expected to be at `{parquet_path}.index/` unless a custom
    /// prefix is specified.
    ///
    /// # Arguments
    ///
    /// * `parquet_path` - Path to the Parquet file (not the index directory). The index
    ///   directory is automatically determined as `{parquet_path}.index/`.
    /// * `index_file_prefix` - Optional custom prefix for index files within the index directory.
    ///   Use `None` for default index files or `Some("prefix_")` for custom-prefixed files.
    ///
    /// # Returns
    ///
    /// `Ok(KeywordSearcher)` - Ready-to-use searcher with index loaded in memory.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Index directory does not exist (`{parquet_path}.index/` not found)
    /// * Any index file is missing or cannot be read (filters.rkyv, data.bin)
    /// * Index files are corrupted or have invalid rkyv serialization
    /// * Insufficient memory to load the index
    /// * File system permissions prevent reading
    ///
    /// # Examples
    ///
    /// **Load default index:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     println!("Index loaded successfully");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Load index with custom prefix:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     // Loads files like: test_filters.rkyv, test_data.bin, etc.
    ///     let searcher = KeywordSearcher::load("data.parquet", Some("test_")).await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Handle missing index:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     match KeywordSearcher::load("data.parquet", None).await {
    ///         Ok(searcher) => println!("Index loaded"),
    ///         Err(e) if e.to_string().contains("not found") => {
    ///             println!("Index needs to be built first");
    ///             // Build index using build_and_save_index()
    ///         }
    ///         Err(e) => return Err(e),
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn load(
        parquet_path: &str,
        index_file_prefix: Option<&str>
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let index_dir = format!("{}.index", parquet_path);
        let index_dir_clone = index_dir.clone(); // Clone for use in closure

        // Helper to read a file using the existing abstraction
        let read_index_file = |filename: String| async move {
            let file_path = format!("{}/{}", index_dir_clone, filename);
            let (store, path) = get_object_store(&file_path).await?;
            let bytes = store.get(&path).await?.bytes().await?;
            Ok::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(bytes.to_vec())
        };

        // Read only the filters file - data.bin is read on-demand
        let filters_bytes = read_index_file(index_filename(IndexFile::Filters, index_file_prefix)).await?;

        let files = crate::index_data::DistributedIndexFiles {
            filters: filters_bytes,
            data: Vec::new(), // Not loaded into memory
        };

        Self::from_serialized(&files, index_dir, index_file_prefix.map(|s| s.to_string()))
    }

    /// Read just the keyword list from a specific chunk.
    ///
    /// Performs a range read to fetch only the keyword strings from a chunk,
    /// without loading the occurrence data. Useful for parent keyword lookups.
    ///
    /// # Arguments
    ///
    /// * `chunk_number` - The chunk number to read (0-indexed)
    ///
    /// # Returns
    ///
    /// `Ok(Vec<String>)` containing the keywords in this chunk
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Chunk number is out of bounds
    /// - File I/O fails
    /// - Deserialization fails
    async fn read_chunk_keywords(&self, chunk_number: u16) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let chunk_info = self.filters.chunk_index.get(chunk_number as usize)
            .ok_or_else(|| format!("Chunk {} not found in index", chunk_number))?;

        let data_path = format!("{}/{}", self.index_dir,
                                index_filename(IndexFile::Data, self.index_file_prefix.as_deref()));

        // Use object store abstraction for cloud/local compatibility
        let (store, obj_path) = get_object_store(&data_path).await?;

        // Read just the keyword list section
        let start = chunk_info.offset;
        let length = chunk_info.keyword_list_length as u64;

        let range = start..(start + length);

        let result = store.get_range(&obj_path, range).await?;
        let buffer = result.to_vec();

        // Deserialize keyword list
        let mut aligned_buffer = AlignedVec::<16>::new();
        aligned_buffer.extend_from_slice(&buffer);

        let archived: &Archived<Vec<String>> = rkyv::access(&aligned_buffer)
            .map_err(|e: RkyvError| format!("Failed to deserialize keyword list: {}", e))?;

        Ok(archived.iter().map(|s| s.to_string()).collect())
    }

    /// Read a complete chunk (keywords + data) from data.bin.
    ///
    /// Performs a single range read to fetch both the keyword list and the
    /// occurrence data for a chunk. Returns both deserialized structures.
    ///
    /// # Arguments
    ///
    /// * `chunk_number` - The chunk number to read (0-indexed)
    ///
    /// # Returns
    ///
    /// `Ok((Vec<String>, Vec<KeywordDataFlat>))` containing keywords and their data
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Chunk number is out of bounds
    /// - File I/O fails
    /// - Deserialization fails
    async fn read_full_chunk(&self, chunk_number: u16) -> Result<(Vec<String>, Vec<KeywordDataFlat>), Box<dyn std::error::Error + Send + Sync>> {
        let chunk_info = self.filters.chunk_index.get(chunk_number as usize)
            .ok_or_else(|| format!("Chunk {} not found in index", chunk_number))?;

        let data_path = format!("{}/{}", self.index_dir,
                                index_filename(IndexFile::Data, self.index_file_prefix.as_deref()));

        // Use object store abstraction
        let (store, obj_path) = get_object_store(&data_path).await?;

        // Read the entire chunk (keywords + data)
        let start = chunk_info.offset;
        let length = chunk_info.total_length as u64;

        let range = start..(start + length);

        let result = store.get_range(&obj_path, range).await?;
        let buffer = result.to_vec();

        // Split buffer into keyword section and data section
        let keyword_length = chunk_info.keyword_list_length as usize;
        let keyword_bytes = &buffer[..keyword_length];
        let data_bytes = &buffer[keyword_length..];

        // Deserialize keyword list
        let mut keyword_buffer = AlignedVec::<16>::new();
        keyword_buffer.extend_from_slice(keyword_bytes);

        let archived_keywords: &Archived<Vec<String>> = rkyv::access(&keyword_buffer)
            .map_err(|e: RkyvError| format!("Failed to deserialize keyword list: {}", e))?;

        let keywords: Vec<String> = archived_keywords.iter().map(|s| s.to_string()).collect();

        // Deserialize data section
        let mut data_buffer = AlignedVec::<16>::new();
        data_buffer.extend_from_slice(data_bytes);

        let archived_data: &Archived<Vec<KeywordDataFlat>> = rkyv::access(&data_buffer)
            .map_err(|e: RkyvError| format!("Failed to deserialize chunk data: {}", e))?;

        let data: Vec<KeywordDataFlat> = archived_data.iter().map(|item| {
            KeywordDataFlat {
                columns: item.columns.iter().map(|col| {
                    crate::index_data::ColumnDataFlat {
                        column_id: col.column_id.to_native(),
                        row_groups: col.row_groups.iter().map(|rg| {
                            crate::index_data::RowGroupDataFlat {
                                row_group_id: rg.row_group_id.to_native(),
                                rows: rg.rows.iter().map(|row| {
                                    crate::index_data::FlatRow {
                                        row: row.row.to_native(),
                                        additional_rows: row.additional_rows.to_native(),
                                        splits_matched: row.splits_matched.to_native(),
                                        parent_chunk: row.parent_chunk.as_ref().map(|c| c.to_native()),
                                        parent_position: row.parent_position.as_ref().map(|p| p.to_native()),
                                    }
                                }).collect(),
                            }
                        }).collect(),
                    }
                }).collect(),
                splits_matched: item.splits_matched.to_native(),
            }
        }).collect();

        Ok((keywords, data))
    }

    /// Look up a parent keyword by its chunk and position.
    ///
    /// Reads the keyword list for the specified chunk and returns the keyword
    /// at the given position. This is used for parent verification during
    /// phrase searches.
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk number containing the parent keyword
    /// * `position` - The position within the chunk (0-999 typically)
    ///
    /// # Returns
    ///
    /// `Ok(String)` containing the parent keyword
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Chunk number is out of bounds
    /// - Position is out of bounds within the chunk
    /// - File I/O or deserialization fails
    async fn lookup_parent_keyword(&self, chunk: u16, position: u16) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let keywords = self.read_chunk_keywords(chunk).await?;

        keywords.get(position as usize)
            .cloned()
            .ok_or_else(|| format!("Position {} out of bounds in chunk {}", position, chunk).into())
    }

    /// Batch lookup multiple parent keywords efficiently.
    ///
    /// Collects all needed chunks, deduplicates them, reads each chunk once,
    /// then looks up all parent keywords. This is more efficient than multiple
    /// individual lookups when processing search results.
    ///
    /// # Arguments
    ///
    /// * `parents` - Iterator of (chunk, position) pairs to look up
    ///
    /// # Returns
    ///
    /// `Ok(HashMap)` mapping (chunk, position) to parent keyword strings
    ///
    /// # Errors
    ///
    /// Returns error if any chunk read or lookup fails
    async fn batch_lookup_parents<I>(&self, parents: I) -> Result<HashMap<(u16, u16), String>, Box<dyn std::error::Error + Send + Sync>>
    where
        I: IntoIterator<Item = (u16, u16)>
    {
        let mut result = HashMap::new();
        let mut chunks_needed: HashMap<u16, Vec<u16>> = HashMap::new();

        // Collect unique chunks and their positions
        for (chunk, position) in parents {
            chunks_needed.entry(chunk).or_insert_with(Vec::new).push(position);
        }

        // Read each chunk once and extract needed keywords
        for (chunk, positions) in chunks_needed {
            let keywords = self.read_chunk_keywords(chunk).await?;

            for position in positions {
                if let Some(keyword) = keywords.get(position as usize) {
                    result.insert((chunk, position), keyword.clone());
                }
            }
        }

        Ok(result)
    }

    /// Search for keywords or phrases in the index.
    ///
    /// This unified search method handles both exact keyword matching and phrase searches
    /// with token splitting and parent verification.
    ///
    /// Performs fast lookup using bloom filters and binary search to find all occurrences
    /// of the keyword across all columns. Returns detailed location information including
    /// columns, row groups, and specific row ranges.
    ///
    /// # Arguments
    ///
    /// * `search_for` - The text to search for (keyword or phrase)
    /// * `in_columns` - Optional column filter:
    ///   - `None` - Search all columns
    ///   - `Some("column")` - Search only specified column
    /// * `keyword_only` - Search mode:
    ///   - `true` - Search for exact keyword only (faster, exact match)
    ///   - `false` - Split into tokens and do phrase search (default, more flexible)
    ///
    /// # Returns
    ///
    /// A `SearchResult` containing:
    /// - `verified_matches` - Guaranteed correct matches (answer directly from index)
    /// - `needs_verification` - Potential matches requiring Parquet read
    ///
    /// # Examples
    ///
    /// **Keyword search:**
    /// ```no_run
    /// # use keywords::searching::keyword_search::KeywordSearcher;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let searcher = KeywordSearcher::load("data.parquet", None).await?;
    /// let result = searcher.search("alice", None, true).await?;
    ///
    /// if let Some(verified) = &result.verified_matches {
    ///     println!("Found in columns: {:?}", verified.columns);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// **Phrase search (default):**
    /// ```no_run
    /// # use keywords::searching::keyword_search::KeywordSearcher;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let searcher = KeywordSearcher::load("data.parquet", None).await?;
    /// let result = searcher.search("example.com", None, false).await?;
    ///
    /// if let Some(verified) = &result.verified_matches {
    ///     println!("Verified matches: {}", verified.total_occurrences);
    /// }
    /// if let Some(needs_check) = &result.needs_verification {
    ///     println!("Need Parquet verification: {}", needs_check.total_occurrences);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search(
        &self,
        search_for: &str,
        in_columns: Option<&str>,
        keyword_only: bool,
    ) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
        if keyword_only {
            // Exact keyword search - all matches are verified
            let old_result = self.search_keyword_internal_zerocopy(search_for, in_columns).await?;

            Ok(SearchResult {
                query: search_for.to_string(),
                found: old_result.found,
                tokens: vec![search_for.to_string()],
                verified_matches: old_result.verified_matches,
                needs_verification: None,
            })
        } else {
            // Phrase search - split tokens and verify
            self.search_phrase_internal(search_for, in_columns).await
        }
    }

    /// Internal zero-copy keyword search implementation.
    ///
    /// Performs keyword lookup with minimal memory allocations by using zero-copy
    /// deserialization. Only the specific keyword data needed is converted from
    /// the archived format to native Rust structures.
    ///
    /// This is the primary internal search method called by the public `search()` API
    /// for exact keyword matching.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The exact keyword to search for (case-sensitive). Must be a
    ///   complete token as it was indexed, not a partial match or pattern.
    /// * `column_filter` - Optional column name to restrict search to a specific column.
    ///   If `None`, searches all columns using the global bloom filter.
    ///
    /// # Returns
    ///
    /// `Ok(SearchResult)` with:
    /// * `found: false` - Keyword definitely not in index (bloom filter rejection)
    /// * `found: true` - Keyword found with detailed location data
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Index data is corrupted (rkyv deserialization failure)
    /// * Column pool is missing required column IDs
    /// * I/O error reading data chunks
    ///
    /// # Note
    ///
    /// Due to bloom filter false positives (~1%), `found: true` means the keyword
    /// is *likely* present. For absolute certainty, verify by reading the Parquet file.
    async fn search_keyword_internal_zerocopy(
        &self,
        keyword: &str,
        column_filter: Option<&str>
    ) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
        // Step 1: Check appropriate filter
        if let Some(col_name) = column_filter {
            if let Some(filter) = self.filters.column_filters.get(col_name) {
                if !filter.might_contain(keyword) {
                    return Ok(SearchResult {
                        query: keyword.to_string(),
                        found: false,
                        tokens: vec![keyword.to_string()],
                        verified_matches: None,
                        needs_verification: None,
                    });
                }
            } else {
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        } else {
            if !self.filters.global_filter.might_contain(keyword) {
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        }

        // Step 2: Find chunk
        let chunk_info = self.find_chunk_for_keyword(keyword);
        let chunk_idx = match chunk_info {
            Some((idx, _)) => idx,
            None => {
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        };

        // Step 3: Read chunk from disk directly into aligned buffer
        let chunk_info = &self.filters.chunk_index[chunk_idx as usize];
        let data_path = format!("{}/{}", self.index_dir,
                                index_filename(IndexFile::Data, self.index_file_prefix.as_deref()));

        let (store, obj_path) = get_object_store(&data_path).await?;
        let start = chunk_info.offset;
        let length = chunk_info.total_length as u64;
        let range = start..(start + length);
        let result = store.get_range(&obj_path, range).await?;

        // Copy once into aligned buffer (eliminates second copy)
        let mut buffer = AlignedVec::<16>::new();
        buffer.extend_from_slice(&result);

        // Step 4: Zero-copy access to archived data (no additional copies)
        let keyword_length = chunk_info.keyword_list_length as usize;

        // Access keyword section directly in aligned buffer
        let archived_keywords: &Archived<Vec<String>> = rkyv::access(&buffer[..keyword_length])
            .map_err(|e: RkyvError| format!("Failed to deserialize keyword list: {}", e))?;

        // Binary search (zero-copy)
        let position = match archived_keywords.binary_search_by(|k| k.as_str().cmp(keyword)) {
            Ok(pos) => pos,
            Err(_) => {
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        };

        // Access data section directly in aligned buffer (no additional copy)
        let archived_data: &Archived<Vec<KeywordDataFlat>> = rkyv::access(&buffer[keyword_length..])
            .map_err(|e: RkyvError| format!("Failed to deserialize chunk data: {}", e))?;

        // Get only the one item we need (still zero-copy)
        let archived_item = &archived_data[position];

        // Determine which column(s) to process
        let filter_to_column_id: Option<u32> = if let Some(col_name) = column_filter {
            let found_id = archived_item.columns.iter()
                .map(|col| col.column_id.to_native())
                .find(|&id| {
                    if id == 0 { return false; }
                    if let Some(name) = self.filters.column_pool.get(id) {
                        name == col_name
                    } else {
                        false
                    }
                });

            if found_id.is_none() {
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
            found_id
        } else {
            Some(0)
        };

        // Step 5: Build result - convert ONLY this one keyword's data
        let mut column_details = Vec::new();
        let mut total_occurrences = 0u64;

        let all_column_ids: Vec<u32> = archived_item.columns.iter()
            .map(|col| col.column_id.to_native())
            .filter(|&id| id != 0)
            .collect();

        for col in archived_item.columns.iter() {
            let column_id: u32 = col.column_id.to_native();

            if let Some(target_id) = filter_to_column_id {
                if column_id != target_id {
                    continue;
                }
            }

            let column_name = if column_id == 0 {
                "_all_columns_aggregate_".to_string()
            } else {
                self.filters.column_pool.get(column_id)
                    .ok_or("Column not found in pool")?
                    .to_string()
            };

            let mut row_groups = Vec::new();

            for rg in col.row_groups.iter() {
                let row_group_id: u16 = rg.row_group_id.to_native();
                let mut row_ranges = Vec::new();

                for flat_row in rg.rows.iter() {
                    let row: u32 = flat_row.row.to_native();
                    let additional_rows: u32 = flat_row.additional_rows.to_native();
                    let splits_matched: u16 = flat_row.splits_matched.to_native();
                    let parent_chunk: Option<u16> = flat_row.parent_chunk.as_ref().map(|c| c.to_native());
                    let parent_position: Option<u16> = flat_row.parent_position.as_ref().map(|p| p.to_native());

                    row_ranges.push(RowRange {
                        start_row: row,
                        end_row: row + additional_rows,
                        splits_matched,
                        parent_chunk,
                        parent_position,
                    });

                    total_occurrences = total_occurrences.saturating_add(additional_rows as u64 + 1);
                }

                row_groups.push(RowGroupLocation {
                    row_group_id,
                    row_ranges,
                });
            }

            column_details.push(ColumnLocation {
                column_name,
                row_groups,
            });
        }

        if filter_to_column_id == Some(0) && !column_details.is_empty() {
            let aggregate_row_groups = column_details[0].row_groups.clone();
            column_details.clear();

            for column_id in all_column_ids {
                if let Some(column_name) = self.filters.column_pool.get(column_id) {
                    column_details.push(ColumnLocation {
                        column_name: column_name.to_string(),
                        row_groups: aggregate_row_groups.clone(),
                    });
                }
            }
        }

        let columns: Vec<String> = if filter_to_column_id == Some(0) {
            archived_item.columns.iter()
                .map(|col| col.column_id.to_native())
                .filter(|&id| id != 0)
                .filter_map(|id| self.filters.column_pool.get(id))
                .map(|s| s.to_string())
                .collect()
        } else {
            column_details.iter()
                .map(|cd| cd.column_name.clone())
                .collect()
        };

        Ok(SearchResult {
            query: keyword.to_string(),
            found: true,
            tokens: vec![keyword.to_string()],
            verified_matches: Some(KeywordLocationData {
                columns,
                total_occurrences,
                splits_matched: archived_item.splits_matched.to_native(),
                column_details,
            }),
            needs_verification: None,
        })
    }

    /// Internal keyword search implementation with full deserialization.
    ///
    /// Alternative search implementation that fully deserializes chunk data.
    /// Currently not used by the public API, which prefers `search_keyword_internal_zerocopy`
    /// for better performance.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The exact keyword to search for (case-sensitive)
    /// * `column_filter` - Optional column name to restrict search to a specific column
    ///
    /// # Returns
    ///
    /// `Ok(SearchResult)` containing:
    /// * `found: false` - Keyword not in index (bloom filter rejection)
    /// * `found: true` - Keyword found with complete location information
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Index data cannot be read or deserialized
    /// * Column pool lookup fails
    ///
    /// # Implementation Note
    ///
    /// This method performs full deserialization of chunk data, while
    /// `search_keyword_internal_zerocopy` uses zero-copy deserialization for
    /// better performance. Kept for potential future use or debugging.
    async fn search_keyword_internal(
        &self,
        keyword: &str,
        column_filter: Option<&str>
    ) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
        // Step 1: Check appropriate filter based on whether we have a column filter
        if let Some(col_name) = column_filter {
            // When filtering to a specific column, check that column's bloom filter first
            if let Some(filter) = self.filters.column_filters.get(col_name) {
                if !filter.might_contain(keyword) {
                    return Ok(SearchResult {
                        query: keyword.to_string(),
                        found: false,
                        tokens: vec![keyword.to_string()],
                        verified_matches: None,
                        needs_verification: None,
                    });
                }
            } else {
                // Column doesn't exist in the index
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        } else {
            // When no column filter, check global filter (fast rejection)
            if !self.filters.global_filter.might_contain(keyword) {
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        }

        // Step 2: Find the chunk that might contain this keyword
        let chunk_info = self.find_chunk_for_keyword(keyword);

        let chunk_idx = match chunk_info {
            Some((idx, _)) => idx,
            None => {
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        };

        // Step 3: Load and search the chunk
        let (keywords, chunk_data) = self.read_full_chunk(chunk_idx).await?;

        // Find the keyword in the chunk
        let position = match keywords.binary_search_by(|k| k.as_str().cmp(keyword)) {
            Ok(pos) => pos,
            Err(_) => {
                // Not found (bloom filter false positive)
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
        };

        // Step 4: Get the keyword data
        let archived_data = &chunk_data[position];

        // Determine which column(s) to process
        // When column_filter is None, use column_id 0 (aggregate of all columns)
        // When column_filter is Some, process only that specific column
        let filter_to_column_id: Option<u32> = if let Some(col_name) = column_filter {
            // Find the column_id for the requested column
            let found_id = archived_data.columns.iter()
                .map(|col| col.column_id)
                .find(|&id| {
                    if id == 0 {
                        return false; // Never match column_id 0 here
                    }
                    if let Some(name) = self.filters.column_pool.get(id) {
                        name == col_name
                    } else {
                        false
                    }
                });

            if found_id.is_none() {
                // Column not found in results - return not found
                return Ok(SearchResult {
                    query: keyword.to_string(),
                    found: false,
                    tokens: vec![keyword.to_string()],
                    verified_matches: None,
                    needs_verification: None,
                });
            }
            found_id
        } else {
            // No filter - use column_id 0 (aggregate)
            Some(0)
        };

        // Step 5: Convert to owned types and build result
        let mut column_details = Vec::new();
        let mut total_occurrences = 0u64;

        // Collect all column IDs for later expansion if using aggregate
        let all_column_ids: Vec<u32> = archived_data.columns.iter()
            .map(|col| col.column_id)
            .filter(|&id| id != 0)
            .collect();

        for col in &archived_data.columns {
            let column_id: u32 = col.column_id;

            // Apply column filter
            if let Some(target_id) = filter_to_column_id {
                if column_id != target_id {
                    continue;
                }
            }

            // Get column name
            let column_name = if column_id == 0 {
                // Column 0 is the aggregate - use a special name
                "_all_columns_aggregate_".to_string()
            } else {
                self.filters.column_pool.get(column_id)
                    .ok_or("Column not found in pool")?
                    .to_string()
            };

            let mut row_groups = Vec::new();

            for rg in &col.row_groups {
                let row_group_id: u16 = rg.row_group_id;
                let mut row_ranges = Vec::new();

                for flat_row in &rg.rows {
                    let row: u32 = flat_row.row;
                    let additional_rows: u32 = flat_row.additional_rows;
                    let splits_matched: u16 = flat_row.splits_matched;
                    let parent_chunk: Option<u16> = flat_row.parent_chunk;
                    let parent_position: Option<u16> = flat_row.parent_position;

                    row_ranges.push(RowRange {
                        start_row: row,
                        end_row: row + additional_rows,
                        splits_matched,
                        parent_chunk,
                        parent_position,
                    });

                    total_occurrences = total_occurrences.saturating_add(additional_rows as u64 + 1);
                }

                row_groups.push(RowGroupLocation {
                    row_group_id,
                    row_ranges,
                });
            }

            column_details.push(ColumnLocation {
                column_name,
                row_groups,
            });
        }

        // TODO: Performance Optimization - Reduce memory duplication when expanding aggregate
        //
        // When we expand column_id 0 (aggregate) into per-column entries below, we clone
        // the row_groups Vec for each column. For keywords appearing in many columns with
        // many matches, this creates O(columns × row_ranges) memory overhead.
        //
        // Potential optimizations if profiling shows this is a bottleneck:
        // 1. Use Arc<Vec<RowGroupLocation>> to share row group data across columns
        //    - Pro: Zero-copy sharing, same memory footprint as aggregate
        //    - Con: 16 bytes Arc overhead per column, more complex serialization
        // 2. Implement copy-on-write semantics with reference counting
        // 3. Add a flag to KeywordSearcher to optionally disable expansion for users
        //    who don't need per-column details (though this hurts API ergonomics)
        //
        // Current memory overhead: ~10-200 KB per search result (worst case)
        // This is negligible compared to index I/O (ms scale) and acceptable for POC.
        // Profile before optimizing!

        // If we used column_id 0 (aggregate), expand it to actual columns
        // This ensures column_details has entries for each actual column, not just "_all_columns_aggregate_"
        if filter_to_column_id == Some(0) && !column_details.is_empty() {
            // Extract the aggregate row group data
            let aggregate_row_groups = column_details[0].row_groups.clone();
            column_details.clear();

            // Create one ColumnLocation per actual column, all sharing the same row group data
            for column_id in all_column_ids {
                if let Some(column_name) = self.filters.column_pool.get(column_id) {
                    column_details.push(ColumnLocation {
                        column_name: column_name.to_string(),
                        row_groups: aggregate_row_groups.clone(),
                    });
                }
            }
        }


        // Build column list
        // When we used column_id 0, we need to get the actual column names from archived_data.columns
        // When we used a specific column, we already have it in column_details
        let columns: Vec<String> = if filter_to_column_id == Some(0) {
            // We used the aggregate (column_id 0), so get actual column names
            archived_data.columns.iter()
                .map(|col| col.column_id)
                .filter(|&id| id != 0)  // Skip column_id 0
                .filter_map(|id| self.filters.column_pool.get(id))
                .map(|s| s.to_string())
                .collect()
        } else {
            // We used a specific column, get it from column_details
            column_details.iter()
                .map(|cd| cd.column_name.clone())
                .collect()
        };

        Ok(SearchResult {
            query: keyword.to_string(),
            found: true,
            tokens: vec![keyword.to_string()],
            verified_matches: Some(KeywordLocationData {
                columns,
                total_occurrences,
                splits_matched: archived_data.splits_matched,
                column_details,
            }),
            needs_verification: None,
        })
    }

    /// Find the chunk that potentially contains a keyword using binary search.
    ///
    /// Performs binary search on the chunk index to locate which chunk should contain
    /// the keyword based on lexicographic ordering of start keywords. The keyword might
    /// not actually exist in the chunk (requires exact match after loading), but if it
    /// exists anywhere, it must be in the returned chunk.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The keyword to find the chunk for
    ///
    /// # Returns
    ///
    /// * `Some((chunk_number, chunk_info))` - The chunk that should contain this keyword
    /// * `None` - If the chunk index is empty
    ///
    /// # Algorithm
    ///
    /// Uses binary search with the invariant:
    /// ```text
    /// chunk[i].start_keyword <= keyword < chunk[i+1].start_keyword
    /// ```
    ///
    /// Special cases:
    /// * If keyword is before all chunks: Returns first chunk (chunk 0)
    /// * If exact match on start_keyword: Returns that chunk
    /// * Otherwise: Returns the chunk immediately before the insertion point
    ///
    /// # Performance
    ///
    /// O(log n) where n is the number of chunks, typically very fast since
    /// chunk count is usually small (< 100 for most indexes)
    fn find_chunk_for_keyword(&self, keyword: &str) -> Option<(u16, &ChunkInfo)> {
        // Binary search to find the right chunk
        // We want to find the chunk where: chunk.start_keyword <= keyword < next_chunk.start_keyword

        if self.filters.chunk_index.is_empty() {
            return None;
        }

        // Find first chunk where start_keyword > keyword
        match self.filters.chunk_index.binary_search_by(|chunk| {
            chunk.start_keyword.as_str().cmp(keyword)
        }) {
            Ok(idx) => Some((idx as u16, &self.filters.chunk_index[idx])),
            Err(idx) => {
                if idx == 0 {
                    // Keyword is before first chunk - still check first chunk
                    Some((0, &self.filters.chunk_index[0]))
                } else {
                    // Keyword belongs to previous chunk
                    Some(((idx - 1) as u16, &self.filters.chunk_index[idx - 1]))
                }
            }
        }
    }

    /// Check if a keyword exists in a specific column.
    ///
    /// More efficient than full search when you only need to check a single column.
    /// Uses column-specific bloom filter for fast rejection before performing full search.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The exact keyword to search for (case-sensitive)
    /// * `column_name` - Name of the column to search in
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Keyword likely exists in the specified column
    /// * `Ok(false)` - Keyword definitely not in the column (bloom filter rejection)
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Index data is corrupted
    /// * Column pool is missing required column IDs
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("users.parquet", None).await?;
    ///
    ///     // Check if "alice" appears in the "username" column
    ///     if searcher.search_in_column("alice", "username").await? {
    ///         println!("Found 'alice' in username column");
    ///     }
    ///
    ///     // Check multiple columns
    ///     for column in &["email", "description", "notes"] {
    ///         if searcher.search_in_column("test", column).await? {
    ///             println!("Found 'test' in {} column", column);
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn search_in_column(&self, keyword: &str, column_name: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // Check column filter first
        if let Some(filter) = self.filters.column_filters.get(column_name) {
            if !filter.might_contain(keyword) {
                return Ok(false);
            }
        }

        // Do filtered search for this specific column
        let result = self.search(keyword, Some(column_name), true).await?;

        if !result.found {
            return Ok(false);
        }

        // Check if keyword exists in the specified column
        let data = result.verified_matches.as_ref().expect("No verified matches");
        Ok(data.columns.iter().any(|col| col == column_name))
    }

    /// Get information about the index
    /// Get metadata about the index.
    ///
    /// Returns information about the index including version, Parquet file validation data,
    /// bloom filter error rate, and index structure statistics.
    ///
    /// # Returns
    ///
    /// [`IndexInfo`] containing:
    /// * Version number
    /// * Source Parquet file metadata (size, etag, last modified)
    /// * Bloom filter error rate
    /// * Number of indexed columns
    /// * Number of metadata chunks
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     let info = searcher.get_index_info();
    ///
    ///     println!("Index version: {}", info.version);
    ///     println!("Parquet size: {} bytes", info.parquet_size);
    ///     println!("Bloom filter error rate: {:.2}%", info.error_rate * 100.0);
    ///     println!("Indexed columns: {}", info.num_columns);
    ///     println!("Metadata chunks: {}", info.num_chunks);
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`IndexInfo`]: IndexInfo
    pub fn get_index_info(&self) -> IndexInfo {
        IndexInfo {
            version: self.filters.version,
            parquet_etag: self.filters.parquet_etag.clone(),
            parquet_size: self.filters.parquet_size,
            parquet_last_modified: self.filters.parquet_last_modified,
            error_rate: self.filters.error_rate,
            num_columns: self.filters.column_pool.strings.len(),
            num_chunks: self.filters.chunk_index.len(),
        }
    }

    /// Combine multiple search results with AND logic.
    ///
    /// Returns rows where ALL keywords appear in the same row (not necessarily the same column).
    /// This is useful for finding rows that contain multiple related keywords.
    ///
    /// # Arguments
    ///
    /// * `results` - Slice of search results to combine. All must be from the same searcher.
    ///
    /// # Returns
    ///
    /// * `Some(CombinedSearchResult)` - Combined result with rows matching all keywords.
    ///   Returns result with empty `row_groups` if any keyword not found or no common rows.
    /// * `None` - If input slice is empty.
    ///
    /// # Examples
    ///
    /// **Find rows with multiple keywords:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("logs.parquet", None).await?;
    ///
    ///     // Find rows containing ALL of these keywords
    ///     let search1 = searcher.search("error", None, true).await?;
    ///     let search2 = searcher.search("database", None, true).await?;
    ///     let search3 = searcher.search("connection", None, true).await?;
    ///
    ///     // Convert to SearchResult for combine_and
    ///     use keywords::searching::search_results::SearchResult;
    ///     let r1 = search1;
    ///     let r2 = search2;
    ///     let r3 = search3;
    ///
    ///     let combined = KeywordSearcher::combine_and(&[r1, r2, r3]);
    ///
    ///     if let Some(result) = combined {
    ///         if !result.row_groups.is_empty() {
    ///             println!("Found rows with all keywords: {:?}", result.keywords);
    ///             println!("Matching row groups: {}", result.row_groups.len());
    ///         } else {
    ///             println!("No rows contain all keywords together");
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Use with pruned reader:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///
    ///     let search1 = searcher.search("alice", None, true).await?;
    ///     let search2 = searcher.search("admin", None, true).await?;
    ///
    ///     // Convert to SearchResult for combine_and
    ///     use keywords::searching::search_results::SearchResult;
    ///     let r1 = search1;
    ///     let r2 = search2;
    ///
    ///     if let Some(combined) = KeywordSearcher::combine_and(&[r1, r2]) {
    ///         let reader = PrunedParquetReader::from_path("data.parquet");
    ///         let batches = reader.read_combined_rows(&combined, None).await?;
    ///
    ///         let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    ///         println!("Found {} rows with both keywords", total);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn combine_and(results: &[SearchResult]) -> Option<CombinedSearchResult> {
        if results.is_empty() {
            return None;
        }

        // Check if all keywords were found
        if results.iter().any(|r| !r.found) {
            return Some(CombinedSearchResult {
                keywords: results.iter().map(|r| r.query.clone()).collect(),
                row_groups: Vec::new(),
            });
        }

        // Build per-column row sets for each result
        // Structure: result_idx -> column_name -> row_group_id -> HashSet<row>
        let mut per_result_columns: Vec<HashMap<String, HashMap<u16, std::collections::HashSet<u32>>>> = Vec::new();

        for result in results {
            let mut column_map: HashMap<String, HashMap<u16, std::collections::HashSet<u32>>> = HashMap::new();

            if let Some(data) = &result.verified_matches {
                for col in &data.column_details {
                    let rg_map = column_map.entry(col.column_name.clone()).or_insert_with(HashMap::new);

                    for rg in &col.row_groups {
                        let rows = rg_map.entry(rg.row_group_id).or_insert_with(std::collections::HashSet::new);
                        for range in &rg.row_ranges {
                            for row in range.start_row..=range.end_row {
                                rows.insert(row);
                            }
                        }
                    }
                }
            }

            per_result_columns.push(column_map);
        }

        // Find intersection: rows that appear across all results in ANY column combination
        let mut combined_row_groups: HashMap<u16, std::collections::HashSet<u32>> = HashMap::new();

        // Start with all row groups from first result
        if let Some(first_columns) = per_result_columns.first() {
            for rg_map in first_columns.values() {
                for (&rg_id, first_rows) in rg_map {
                    // For each row in this row group, check if it exists in remaining results
                    for &row in first_rows {
                        let mut found_in_all = true;

                        // Check if this row exists in ANY column of each remaining result
                        for other_columns in &per_result_columns[1..] {
                            let mut found_in_this_result = false;

                            for other_rg_map in other_columns.values() {
                                if let Some(other_rows) = other_rg_map.get(&rg_id) {
                                    if other_rows.contains(&row) {
                                        found_in_this_result = true;
                                        break;
                                    }
                                }
                            }

                            if !found_in_this_result {
                                found_in_all = false;
                                break;
                            }
                        }

                        if found_in_all {
                            combined_row_groups
                                .entry(rg_id)
                                .or_insert_with(std::collections::HashSet::new)
                                .insert(row);
                        }
                    }
                }
            }
        }

        // Convert to result format
        let mut row_groups = Vec::new();
        for (rg_id, rows) in combined_row_groups {
            let mut sorted_rows: Vec<u32> = rows.into_iter().collect();
            sorted_rows.sort_unstable();

            // Combine consecutive rows into ranges
            let ranges = rows_to_ranges(&sorted_rows);

            row_groups.push(CombinedRowGroupLocation {
                row_group_id: rg_id,
                row_ranges: ranges,
            });
        }

        row_groups.sort_by_key(|rg| rg.row_group_id);

        Some(CombinedSearchResult {
            keywords: results.iter().map(|r| r.query.clone()).collect(),
            row_groups,
        })
    }

    /// Combine multiple search results with OR logic.
    ///
    /// Returns rows where ANY of the keywords appear. This is useful for finding rows
    /// that match multiple alternative search terms.
    ///
    /// # Arguments
    ///
    /// * `results` - Slice of search results to combine. All must be from the same searcher.
    ///
    /// # Returns
    ///
    /// * `Some(CombinedSearchResult)` - Combined result with union of all matching rows.
    ///   Returns result with empty `row_groups` if all keywords not found.
    /// * `None` - If input slice is empty.
    ///
    /// # Examples
    ///
    /// **Find rows with any of several keywords:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("logs.parquet", None).await?;
    ///
    ///     // Find rows containing ANY of these severity levels
    ///     let search_error = searcher.search("error", None, true).await?;
    ///     let search_critical = searcher.search("critical", None, true).await?;
    ///     let search_fatal = searcher.search("fatal", None, true).await?;
    ///
    ///     // Convert to SearchResult for combine_or
    ///     use keywords::searching::search_results::SearchResult;
    ///     let error = search_error;
    ///     let critical = search_critical;
    ///     let fatal = search_fatal;
    ///
    ///     let combined = KeywordSearcher::combine_or(&[error, critical, fatal]);
    ///
    ///     if let Some(result) = combined {
    ///         println!("Keywords: {:?}", result.keywords);
    ///         println!("Total row groups with any keyword: {}", result.row_groups.len());
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Search for alternative terms:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("emails.parquet", None).await?;
    ///
    ///     // Search for multiple variations of a name
    ///     let search_alice = searcher.search("alice", None, true).await?;
    ///     let search_alicia = searcher.search("alicia", None, true).await?;
    ///     let search_ali = searcher.search("ali", None, true).await?;
    ///
    ///     // Convert to SearchResult for combine_or
    ///     use keywords::searching::search_results::SearchResult;
    ///     let alice = search_alice;
    ///     let alicia = search_alicia;
    ///     let ali = search_ali;
    ///
    ///     if let Some(combined) = KeywordSearcher::combine_or(&[alice, alicia, ali]) {
    ///         let reader = PrunedParquetReader::from_path("emails.parquet");
    ///         let batches = reader.read_combined_rows(&combined, None).await?;
    ///
    ///         println!("Found {} batches with any variation", batches.len());
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn combine_or(results: &[SearchResult]) -> Option<CombinedSearchResult> {
        if results.is_empty() {
            return None;
        }

        let mut combined_row_groups: HashMap<u16, std::collections::HashSet<u32>> =
            HashMap::new();

        // Union all rows from all results
        for result in results {
            if !result.found {
                continue;
            }

            let data = result.verified_matches.as_ref()?;
            for col in &data.column_details {
                for rg in &col.row_groups {
                    let rows = combined_row_groups.entry(rg.row_group_id).or_insert_with(std::collections::HashSet::new);
                    for range in &rg.row_ranges {
                        for row in range.start_row..=range.end_row {
                            rows.insert(row);
                        }
                    }
                }
            }
        }

        // Convert to result format
        let mut row_groups = Vec::new();
        for (rg_id, rows) in combined_row_groups {
            let mut sorted_rows: Vec<u32> = rows.into_iter().collect();
            sorted_rows.sort_unstable();

            // Combine consecutive rows into ranges
            let ranges = rows_to_ranges(&sorted_rows);

            row_groups.push(CombinedRowGroupLocation {
                row_group_id: rg_id,
                row_ranges: ranges,
            });
        }

        row_groups.sort_by_key(|rg| rg.row_group_id);

        Some(CombinedSearchResult {
            keywords: results.iter().map(|r| r.query.clone()).collect(),
            row_groups,
        })
    }

    /// Check if the index is valid for the current parquet file
    /// Validate that the index matches the current Parquet source.
    ///
    /// Checks if the index is still valid for the Parquet source by comparing:
    /// - File size
    /// - ETag (if available, for remote files)
    /// - Last modified timestamp (for file paths)
    ///
    /// For in-memory sources (ParquetSource::Bytes), only the size is checked.
    ///
    /// # Arguments
    ///
    /// * `source` - The Parquet source to validate against (Path or Bytes).
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Index is valid and matches the Parquet source
    /// * `Ok(false)` - Index is outdated (file size, etag, or last modified changed)
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Cannot access the Parquet file
    /// * Cannot read file metadata
    /// * Network error (for remote files)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    /// use keywords::ParquetSource;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let source = ParquetSource::Path("data.parquet".to_string());
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///
    ///     if searcher.validate_index(&source).await? {
    ///         println!("Index is up to date");
    ///     } else {
    ///         println!("Index is outdated - rebuild required");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Check before searching:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    /// use keywords::ParquetSource;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let parquet_path = "data.parquet";
    ///     let searcher = KeywordSearcher::load(parquet_path, None).await?;
    ///     let source = ParquetSource::Path(parquet_path.to_string());
    ///
    ///     // Validate before performing searches
    ///     if !searcher.validate_index(&source).await? {
    ///         eprintln!("Warning: Index may be outdated");
    ///     }
    ///
    ///     let result = searcher.search("keyword", None, true).await?;
    ///     println!("Found: {}", result.found);
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn validate_index(&self, source: &ParquetSource) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        match source {
            ParquetSource::Path(path) => {
                use crate::utils::file_interaction_local_and_cloud::get_object_store;

                let (store, obj_path) = get_object_store(path).await?;
                let head = store.head(&obj_path).await?;

                // Check size
                if head.size != self.filters.parquet_size {
                    return Ok(false);
                }

                // Check etag if available
                if let Some(etag) = head.e_tag {
                    if etag != self.filters.parquet_etag {
                        return Ok(false);
                    }
                }

                // Check last modified
                if head.last_modified.timestamp() as u64 != self.filters.parquet_last_modified {
                    return Ok(false);
                }

                Ok(true)
            }
            ParquetSource::Bytes(bytes) => {
                // For in-memory sources, only check size
                Ok(bytes.len() as u64 == self.filters.parquet_size)
            }
        }
    }

    // ========== PHRASE SEARCH METHODS ==========

    /// Search for a multi-token phrase with parent verification.
    ///
    /// Searches for phrases containing delimiters (e.g., "john-doe", "user@email.com").
    /// Uses parent keyword information to confirm or reject matches without reading the
    /// Parquet file, making this more efficient than reading all potential matches.
    ///
    /// # Arguments
    ///
    /// * `phrase` - The phrase to search for. Will be split on delimiter characters
    ///   (space, newline, `/`, `@`, `=`, `.`, `$`, `#`, `-`, `_`, etc.).
    ///
    /// # Returns
    ///
    /// `Ok(MultiTokenSearchResult)` containing:
    /// * `tokens` - How the phrase was split into tokens
    /// * `found` - Whether any matches were found (confirmed or needing verification)
    /// * `confirmed_matches` - Matches verified via parent keywords (no Parquet read needed)
    /// * `rejected_matches` - Matches rejected via parent keywords (definitely not matches)
    /// * `needs_verification` - Matches requiring Parquet file read to verify
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Index data is corrupted
    /// * Internal search operations fail
    ///
    /// # How Parent Verification Works
    ///
    /// When a phrase like "john-doe" is indexed, three entries are created:
    /// 1. "john" (child, parent offset → "john-doe")
    /// 2. "doe" (child, parent offset → "john-doe")
    /// 3. "john-doe" (parent)
    ///
    /// When searching:
    /// * If both tokens found in same row with same parent → **Confirmed** (no read needed)
    /// * If tokens have different parents → **Rejected** (not a match)
    /// * If parent info unavailable → **Needs verification** (must read Parquet)
    ///
    /// # Examples
    ///
    /// **Search with automatic verification:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("users.parquet", None).await?;
    ///     let result = searcher.search("john-doe", None, false).await?;
    ///
    ///     println!("Query: {}", result.query);
    ///     println!("Tokens: {:?}", result.tokens);
    ///
    ///     // Count verified matches
    ///     if let Some(verified) = &result.verified_matches {
    ///         println!("Verified matches: {} occurrences", verified.total_occurrences);
    ///         println!("In columns: {:?}", verified.columns);
    ///     }
    ///
    ///     // Count needs verification
    ///     if let Some(needs_check) = &result.needs_verification {
    ///         println!("Needs verification: {} occurrences", needs_check.total_occurrences);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Email address search:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("contacts.parquet", None).await?;
    ///
    ///     // Searches for "user" AND "example" AND "com"
    ///     let result = searcher.search("user@example.com", None, false).await?;
    ///
    ///     if result.found {
    ///         let verified_count = result.verified_matches.as_ref().map(|v| v.total_occurrences).unwrap_or(0);
    ///         let needs_check_count = result.needs_verification.as_ref().map(|n| n.total_occurrences).unwrap_or(0);
    ///
    ///         println!("Total potential matches: {}", verified_count + needs_check_count);
    ///
    ///         // These are guaranteed matches (parent verification passed)
    ///         if verified_count > 0 {
    ///             println!("Verified matches: {} occurrences", verified_count);
    ///         }
    ///
    ///         // These need Parquet read to verify
    ///         if needs_check_count > 0 {
    ///             println!("{} occurrences need verification by reading Parquet", needs_check_count);
    ///         }
    ///     } else {
    ///         println!("Phrase not found");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Handle verification needed:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     let result = searcher.search("test-value", None, false).await?;
    ///
    ///     // Use verified matches directly
    ///     if let Some(verified) = &result.verified_matches {
    ///         println!("Verified: {} occurrences", verified.total_occurrences);
    ///     }
    ///
    ///     // For needs_verification, you would read the Parquet file
    ///     if let Some(needs_check) = &result.needs_verification {
    ///         println!("Need to read Parquet to verify {} occurrences",
    ///             needs_check.total_occurrences);
    ///         // Use PrunedParquetReader to read and verify these rows
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn search_phrase_internal(&self, phrase: &str, column_filter: Option<&str>) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
        // Split the phrase using the same logic as the index
        let tokens = self.split_phrase(phrase);

        if tokens.is_empty() {
            return Ok(SearchResult {
                query: phrase.to_string(),
                tokens: Vec::new(),
                found: false,
                verified_matches: None,
                needs_verification: None,
            });
        }

        // If single token, use regular search
        if tokens.len() == 1 {
            let result = self.search_keyword_internal(&tokens[0], column_filter).await?;

            // Single token always returns verified matches
            return Ok(SearchResult {
                query: phrase.to_string(),
                tokens,
                found: result.found,
                verified_matches: result.verified_matches,
                needs_verification: None,
            });
        }

        // Search for each token
        // Note: We don't fail if a token isn't found - it might be an intermediate
        // parent that wasn't indexed separately. We'll find it through parent verification.
        let mut token_results = Vec::new();
        let mut found_tokens = Vec::new();

        for token in &tokens {
            let result = self.search_keyword_internal(token, column_filter).await?;
            if result.found {
                token_results.push(result);
                found_tokens.push(token.clone());
            }
        }

        // If we found no tokens at all, the phrase definitely doesn't exist
        if token_results.is_empty() {
            return Ok(SearchResult {
                query: phrase.to_string(),
                tokens: found_tokens,
                found: false,
                verified_matches: None,
                needs_verification: None,
            });
        }

        // Find rows where ALL tokens exist in the same column and check parents
        let (confirmed, needs_verification) =
            self.find_and_verify_multi_token_matches(phrase, &token_results).await?;

        let found = !confirmed.is_empty() || !needs_verification.is_empty();

        // Convert confirmed matches and needs_verification to KeywordLocationData
        let verified_data = if !confirmed.is_empty() {
            Some(self.potential_matches_to_location_data(&confirmed))
        } else {
            None
        };

        let needs_verification_data = if !needs_verification.is_empty() {
            Some(self.potential_matches_to_location_data(&needs_verification))
        } else {
            None
        };

        Ok(SearchResult {
            query: phrase.to_string(),
            tokens: found_tokens,
            found,
            verified_matches: verified_data,
            needs_verification: needs_verification_data,
        })
    }

    /// Split a phrase using the same hierarchical splitting logic as the index.
    ///
    /// This function replicates the multi-level hierarchical splitting performed during
    /// indexing. Unlike a flat split that treats all delimiters equally, this processes
    /// delimiters level-by-level, creating parent-child token relationships.
    ///
    /// # Hierarchical Splitting Process
    ///
    /// The phrase is split across 4 delimiter levels in sequence:
    /// - **Level 0**: Whitespace and structural characters (space, newline, quotes, brackets, etc.)
    /// - **Level 1**: Path/network delimiters (@, /, :, =, ?, &, etc.)
    /// - **Level 2**: Dot notation and special symbols (., $, #, +, etc.)
    /// - **Level 3**: Word separators (-, _)
    ///
    /// At each level:
    /// 1. If no split occurs, continue to the next level with the same string
    /// 2. If a split occurs, add the parent token and recursively process each child
    ///
    /// # Example
    ///
    /// For "user@example.com":
    /// - Level 0: No split → continue
    /// - Level 1: Splits on @ → adds ["user@example.com", then processes "user" and "example.com"]
    /// - Level 2: "user" no split → adds "user"
    /// - Level 2: "example.com" splits on . → adds ["example.com", then processes "example" and "com"]
    /// - Level 3: No further splits → adds "example" and "com"
    /// - **Result**: ["user@example.com", "user", "example.com", "example", "com"]
    ///
    /// # Returns
    ///
    /// A deduplicated, sorted vector of all tokens created during hierarchical splitting.
    pub(super) fn split_phrase(&self, phrase: &str) -> Vec<String> {
        let mut all_tokens = std::collections::HashSet::new();
        self.split_phrase_recursive(phrase, 0, &mut all_tokens);

        let mut tokens: Vec<String> = all_tokens.into_iter().collect();
        tokens.sort();
        tokens
    }

    /// Recursive helper for hierarchical phrase splitting.
    ///
    /// Processes one delimiter level at a time, adding both parent and child tokens
    /// to maintain the keyword hierarchy created during indexing.
    ///
    /// # Arguments
    ///
    /// * `text` - The current text segment to split
    /// * `level` - Current delimiter level (0-3)
    /// * `tokens` - Accumulator for all discovered tokens
    fn split_phrase_recursive(&self, text: &str, level: usize, tokens: &mut std::collections::HashSet<String>) {
        // Base case: reached maximum split level
        if level >= self.filters.split_chars_inclusive.len() {
            tokens.insert(text.to_string());
            return;
        }

        // Get delimiters for current level
        let split_chars = &self.filters.split_chars_inclusive[level];

        // Split on current level's delimiters
        let parts: Vec<&str> = text
            .split(|c| split_chars.contains(&c))
            .filter(|s| !s.is_empty())
            .collect();

        if parts.len() == 1 && parts[0] == text {
            // No split occurred at this level - continue to next level
            self.split_phrase_recursive(text, level + 1, tokens);
        } else if parts.len() > 1 {
            // Split occurred - add parent token and recurse on each child
            tokens.insert(text.to_string());

            for part in parts {
                self.split_phrase_recursive(part, level + 1, tokens);
            }
        } else if parts.is_empty() {
            // Text was all delimiters (edge case) - try next level
            self.split_phrase_recursive(text, level + 1, tokens);
        }
    }

    /// Convert a list of potential matches to KeywordLocationData structure.
    ///
    /// Takes a flat list of matches (column, row_group, row) and organizes them
    /// into the hierarchical structure used by KeywordLocationData.
    fn potential_matches_to_location_data(&self, matches: &[PotentialMatch]) -> KeywordLocationData {
        use std::collections::HashMap;

        // Group by column
        let mut column_map: HashMap<String, HashMap<u16, Vec<u32>>> = HashMap::new();
        let mut all_columns = std::collections::HashSet::new();
        let mut total_rows = 0u64;
        let mut splits_matched = 0u16;

        for m in matches {
            all_columns.insert(m.column_name.clone());
            splits_matched |= 1 << m.split_level;

            let rg_map = column_map.entry(m.column_name.clone())
                .or_insert_with(HashMap::new);
            rg_map.entry(m.row_group_id)
                .or_insert_with(Vec::new)
                .push(m.row);

            total_rows += 1;
        }

        // Build column_details
        let mut column_details = Vec::new();
        let mut columns: Vec<String> = all_columns.into_iter().collect();
        columns.sort();

        for column_name in &columns {
            let rg_map = &column_map[column_name];
            let mut row_groups = Vec::new();

            for (&row_group_id, rows) in rg_map {
                let mut sorted_rows = rows.clone();
                sorted_rows.sort_unstable();

                // Convert to ranges
                let mut row_ranges = Vec::new();
                if !sorted_rows.is_empty() {
                    let mut start = sorted_rows[0];
                    let mut end = sorted_rows[0];

                    for &row in &sorted_rows[1..] {
                        if row == end + 1 {
                            end = row;
                        } else {
                            row_ranges.push(RowRange {
                                start_row: start,
                                end_row: end,
                                splits_matched,
                                parent_chunk: None,
                                parent_position: None,
                            });
                            start = row;
                            end = row;
                        }
                    }

                    // Add final range
                    row_ranges.push(RowRange {
                        start_row: start,
                        end_row: end,
                        splits_matched,
                        parent_chunk: None,
                        parent_position: None,
                    });
                }

                row_groups.push(RowGroupLocation {
                    row_group_id,
                    row_ranges,
                });
            }

            row_groups.sort_by_key(|rg| rg.row_group_id);

            column_details.push(ColumnLocation {
                column_name: column_name.clone(),
                row_groups,
            });
        }

        KeywordLocationData {
            columns,
            total_occurrences: total_rows,
            splits_matched,
            column_details,
        }
    }

    /// Find rows where all tokens exist and verify phrase matches using parent keywords.
    ///
    /// This is the core phrase verification algorithm that:
    /// 1. Finds rows containing all tokens in the same column
    /// 2. Checks if tokens share the same parent keyword
    /// 3. Verifies if the parent contains the exact phrase
    /// 4. Recurses to grandparents if needed
    ///
    /// # Arguments
    ///
    /// * `phrase` - The complete phrase being searched for
    /// * `token_results` - Search results for each individual token in the phrase
    ///
    /// # Returns
    ///
    /// `Ok((confirmed, needs_verification))` tuple containing:
    /// * `confirmed` - Matches verified via parent keyword relationships
    /// * `needs_verification` - Potential matches requiring Parquet verification
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Parent keyword batch lookup fails
    /// * Token result data is malformed
    ///
    /// # Algorithm
    ///
    /// For each column where all tokens appear:
    /// 1. Find rows containing all tokens
    /// 2. For each row, collect parent references for all tokens
    /// 3. Check if tokens share the same parent (cartesian product of possibilities)
    /// 4. Batch lookup all parent keywords
    /// 5. Verify if parent contains the phrase
    /// 6. Recurse to grandparents if parent doesn't contain phrase
    ///
    /// # Performance
    ///
    /// Uses batch parent lookups to minimize I/O. Groups all parent lookups
    /// for a column together and reads each chunk only once.
    async fn find_and_verify_multi_token_matches(
        &self,
        phrase: &str,
        token_results: &[SearchResult],
    ) -> Result<(Vec<PotentialMatch>, Vec<PotentialMatch>), Box<dyn std::error::Error + Send + Sync>> {
        let mut confirmed_matches = Vec::new();
        let mut needs_verification = Vec::new();

        // Get the first token's results as the base
        let base_result = &token_results[0];
        let base_data = match &base_result.verified_matches {
            Some(d) => d,
            None => return Ok((confirmed_matches, needs_verification)),
        };

        // For each column in the base result
        for col_detail in &base_data.column_details {
            let column_name = &col_detail.column_name;

            // Check if all other tokens exist in this same column
            let mut other_token_column_data = Vec::new();
            let mut all_tokens_in_column = true;

            for token_result in &token_results[1..] {
                let token_data = match &token_result.verified_matches {
                    Some(d) => d,
                    None => {
                        all_tokens_in_column = false;
                        break;
                    }
                };

                // Find this column in the token's results
                let col_data = token_data.column_details.iter()
                    .find(|cd| &cd.column_name == column_name);

                match col_data {
                    Some(cd) => other_token_column_data.push(cd),
                    None => {
                        all_tokens_in_column = false;
                        break;
                    }
                }
            }

            if !all_tokens_in_column {
                continue; // Skip this column
            }

            // Now find rows where ALL tokens exist in the same row group and row
            for rg in &col_detail.row_groups {
                let row_group_id = rg.row_group_id;

                // Build map of row -> (split-level, parent chunk, parent position) for base token
                let base_row_info: HashMap<u32, Vec<(u16, Option<u16>, Option<u16>)>> = rg.row_ranges.iter()
                    .fold(HashMap::new(), |mut acc, range| {
                        for row in range.start_row..=range.end_row {
                            acc.entry(row)
                                .or_insert_with(Vec::new)
                                .push((range.splits_matched, range.parent_chunk, range.parent_position));
                        }
                        acc
                    });

                // For each other token, build similar maps
                let mut all_token_row_info: Vec<HashMap<u32, Vec<(u16, Option<u16>, Option<u16>)>>> = Vec::new();
                let mut all_tokens_have_rows = true;

                for other_col in &other_token_column_data {
                    // Find this row group in the other token's data
                    let other_rg = other_col.row_groups.iter()
                        .find(|r| r.row_group_id == row_group_id);

                    match other_rg {
                        Some(rg_data) => {
                            let row_info: HashMap<u32, Vec<(u16, Option<u16>, Option<u16>)>> = rg_data.row_ranges.iter()
                                .fold(HashMap::new(), |mut acc, range| {
                                    for row in range.start_row..=range.end_row {
                                        acc.entry(row)
                                            .or_insert_with(Vec::new)
                                            .push((range.splits_matched, range.parent_chunk, range.parent_position));
                                    }
                                    acc
                                });
                            all_token_row_info.push(row_info);
                        }
                        None => {
                            all_tokens_have_rows = false;
                            break;
                        }
                    }
                }

                if !all_tokens_have_rows {
                    continue;
                }

                // Find intersection: rows where ALL tokens exist
                for (&row, base_infos) in &base_row_info {
                    // Check if all other tokens exist in this row
                    let mut all_tokens_in_row = true;
                    let mut other_token_infos = Vec::new();

                    for other_rows in &all_token_row_info {
                        if let Some(infos) = other_rows.get(&row) {
                            other_token_infos.push(infos);
                        } else {
                            all_tokens_in_row = false;
                            break;
                        }
                    }

                    if !all_tokens_in_row {
                        continue;
                    }

                    // All tokens exist in this row! Now check parents
                    // Collect all parent (chunk, position) pairs needed for batch lookup
                    let mut all_parent_pairs = Vec::new();

                    for base_info in base_infos {
                        if let (Some(chunk), Some(pos)) = (base_info.1, base_info.2) {
                            all_parent_pairs.push((chunk, pos));
                        }
                    }

                    for other_info_set in &other_token_infos {
                        for other_info in *other_info_set {
                            if let (Some(chunk), Some(pos)) = (other_info.1, other_info.2) {
                                all_parent_pairs.push((chunk, pos));
                            }
                        }
                    }

                    // Batch lookup all parent keywords
                    all_parent_pairs.sort();
                    all_parent_pairs.dedup();
                    let parent_keywords = self.batch_lookup_parents(all_parent_pairs.iter().copied()).await?;

                    // Try all combinations of parent info for each token
                    for base_info in base_infos {
                        for other_info_combinations in self.cartesian_product(&other_token_infos) {
                            let all_parent_refs: Vec<(Option<u16>, Option<u16>)> = std::iter::once((base_info.1, base_info.2))
                                .chain(other_info_combinations.iter().map(|(_, chunk, pos)| (*chunk, *pos)))
                                .collect();

                            // Convert splits_matched bitmasks to actual split levels, then take minimum
                            let min_split_level = std::iter::once(base_info.0)
                                .chain(other_info_combinations.iter().map(|(split, _, _)| *split))
                                .filter_map(|splits_matched| self.get_parent_split_level(splits_matched))
                                .min()
                                .unwrap_or(0) as u16;

                            let status = self.verify_match_with_parent(
                                phrase,
                                &all_parent_refs,
                                &parent_keywords,
                            ).await;

                            let match_info = PotentialMatch {
                                column_name: column_name.clone(),
                                row_group_id,
                                row,
                                split_level: min_split_level,
                                status: status.clone(),
                            };

                            match status {
                                MatchStatus::Confirmed { .. } => {
                                    confirmed_matches.push(match_info);
                                }
                                MatchStatus::NeedsVerification { .. } => {
                                    needs_verification.push(match_info);
                                }
                                MatchStatus::Rejected { .. } => {}
                            }
                        }
                    }
                }
            }
        }

        // Deduplicate matches (same row might appear multiple times with different parent combos)
        confirmed_matches.sort_by_key(|m| (m.column_name.clone(), m.row_group_id, m.row));
        confirmed_matches.dedup_by_key(|m| (m.column_name.clone(), m.row_group_id, m.row));

        needs_verification.sort_by_key(|m| (m.column_name.clone(), m.row_group_id, m.row));
        needs_verification.dedup_by_key(|m| (m.column_name.clone(), m.row_group_id, m.row));

        Ok((confirmed_matches, needs_verification))
    }

    /// Verify a match using parent keyword information
    /// Get the minimum (highest priority) split level in the phrase
    /// Lower number = higher priority (level 0 = whitespace, level 3 = hyphens)
    pub(super) fn get_min_phrase_split_level(&self, phrase: &str) -> Option<usize> {
        for (level, split_chars) in self.filters.split_chars_inclusive.iter().enumerate() {
            if phrase.chars().any(|c| split_chars.contains(&c)) {
                return Some(level);
            }
        }
        None
    }

    /// Determine the split level of a keyword's parent from the keyword's splits_matched.
    ///
    /// The splits_matched field encodes which levels a keyword survived:
    /// - Bit 0 (1): root token
    /// - Bit 1 (2): survived level 0
    /// - Bit 2 (4): survived level 1 (started from level 1 split)
    /// - Bit 3 (8): survived level 2 (started from level 2 split)
    /// - Bit 4 (16): survived level 3 (started from level 3 split)
    ///
    /// To find parent split level: find the lowest set bit (excluding bit 0), that's level+1 where parent was split.
    ///
    /// Example: splits_matched = 28 = 4+8+16 (bits 2,3,4 set)
    /// - Lowest set bit (excluding bit 0) is bit 2
    /// - Parent was split at level 1 (bit position 2 = level 1+1)
    pub(super) fn get_parent_split_level(&self, child_splits_matched: u16) -> Option<usize> {
        // Find the lowest bit set (excluding bit 0 which is the root marker)
        for level in 1..=4 {
            if (child_splits_matched & (1 << level)) != 0 {
                // First set bit at position `level` means parent was split at level-1
                return Some(level - 1);
            }
        }
        None // Root token (only bit 0 set or no bits set)
    }

    /// Verify if a phrase match is confirmed by checking the parent keyword.
    ///
    /// Checks if all tokens in a potential match share the same parent keyword,
    /// and if so, whether that parent contains the complete phrase. If the parent
    /// doesn't contain the phrase, recursively checks grandparents.
    ///
    /// # Arguments
    ///
    /// * `phrase` - The phrase to verify
    /// * `parent_refs` - List of (chunk, position) pairs for each token's parent
    /// * `parent_keywords` - Pre-loaded map of (chunk, position) to parent keyword strings
    ///
    /// # Returns
    ///
    /// * `MatchStatus::Confirmed` - Parent contains the phrase, match is verified
    /// * `MatchStatus::Rejected` - Reached root without finding phrase (rare)
    /// * `MatchStatus::NeedsVerification` - Cannot determine without Parquet read
    ///
    /// # Verification Logic
    ///
    /// 1. **Same parent check**: All tokens must have identical parent references
    ///    - If different parents: `NeedsVerification` (tokens not adjacent)
    ///
    /// 2. **Parent contains phrase**: Check if parent keyword contains phrase substring
    ///    - If yes: `Confirmed` (phrase definitely exists)
    ///    - If no: Recurse to grandparent (phrase might be in ancestor)
    ///
    /// 3. **No parent (root)**: Tokens are roots without parent
    ///    - `NeedsVerification` (need Parquet to confirm)
    ///
    /// # Example
    ///
    /// Searching for "user-name" where tokens are ["user-name", "user", "name"]:
    /// - If parent of "user" and "name" is "user-name": Confirmed ✓
    /// - If parent is "bob-user-name": Recurse to check if grandparent contains phrase
    /// - If tokens have different parents: NeedsVerification
    async fn verify_match_with_parent(
        &self,
        phrase: &str,
        parent_refs: &[(Option<u16>, Option<u16>)],
        parent_keywords: &HashMap<(u16, u16), String>,
    ) -> MatchStatus {
        // Check if all tokens have the same parent
        let first_parent = parent_refs[0];
        let all_same_parent = parent_refs.iter().all(|&p| p == first_parent);

        if !all_same_parent {
            return MatchStatus::NeedsVerification {
                reason: "Tokens have different parents".to_string(),
            };
        }

        match first_parent {
            (Some(chunk), Some(position)) => {
                // Look up parent keyword from batch results
                match parent_keywords.get(&(chunk, position)) {
                    Some(parent_keyword) => {
                        // Check if the phrase exists as substring in parent
                        if parent_keyword.contains(phrase) {
                            MatchStatus::Confirmed {
                                parent_keyword: parent_keyword.clone(),
                            }
                        } else {
                            // Recurse to check grandparents
                            let min_phrase_level = self.get_min_phrase_split_level(phrase);
                            self.verify_match_with_grandparent(phrase, parent_keyword, min_phrase_level, 0).await
                        }
                    }
                    None => {
                        MatchStatus::NeedsVerification {
                            reason: format!("Parent at ({}, {}) not found in batch results", chunk, position),
                        }
                    }
                }
            }
            _ => {
                MatchStatus::NeedsVerification {
                    reason: "No parent information (root token)".to_string(),
                }
            }
        }
    }

    /// Recursively search up the parent chain for a keyword that contains the phrase.
    ///
    /// Optimization: Only check parents whose split level is >= min_phrase_level.
    /// This is because a phrase can only exist in parents that were split at the same
    /// or lower priority (higher number) level as the delimiters in the phrase.
    ///
    /// Example: "user-name" (level 3 delimiter) can exist in a parent split at level 3,
    /// but CANNOT exist in a parent split at level 1 (like "something@user-name"),
    /// because the hyphen would have survived the level 1 split.
    ///
    /// # Arguments
    ///
    /// * `phrase` - The phrase to find
    /// * `current_keyword` - The current keyword whose parents to check
    /// * `min_phrase_level` - Minimum split level for optimization
    /// * `depth` - Current recursion depth (to prevent stack overflow)
    async fn verify_match_with_grandparent(
        &self,
        phrase: &str,
        current_keyword: &str,
        min_phrase_level: Option<usize>,
        depth: usize,
    ) -> MatchStatus {
        // Prevent stack overflow - limit recursion depth
        const MAX_RECURSION_DEPTH: usize = 50;
        if depth >= MAX_RECURSION_DEPTH {
            return MatchStatus::NeedsVerification {
                reason: format!("Maximum recursion depth ({}) exceeded", MAX_RECURSION_DEPTH),
            };
        }

        // Search for the current keyword in the index to get its parent
        match self.search_keyword_internal(current_keyword, None).await {
            Ok(result) => {
                if !result.found {
                    return MatchStatus::NeedsVerification {
                        reason: format!("Parent keyword '{}' not found in index", current_keyword),
                    };
                }

                // Get the row data to find parent reference
                if let Some(data) = result.verified_matches {
                    for col_detail in &data.column_details {
                        for rg in &col_detail.row_groups {
                            for range in &rg.row_ranges {
                                // Optimization: Check if this parent's split-level is compatible
                                if let Some(min_level) = min_phrase_level {
                                    if let Some(parent_split_level) = self.get_parent_split_level(range.splits_matched) {
                                        if parent_split_level < min_level {
                                            continue;
                                        }
                                    }
                                }

                                return if let (Some(grandparent_chunk), Some(grandparent_position)) =
                                    (range.parent_chunk, range.parent_position)
                                {
                                    match self.lookup_parent_keyword(grandparent_chunk, grandparent_position).await {
                                        Ok(grandparent_keyword) => {
                                            if grandparent_keyword.contains(phrase) {
                                                MatchStatus::Confirmed {
                                                    parent_keyword: grandparent_keyword,
                                                }
                                            } else {
                                                // Recurse further up the chain (boxed for async recursion)
                                                Box::pin(self.verify_match_with_grandparent(
                                                    phrase,
                                                    &grandparent_keyword,
                                                    min_phrase_level,
                                                    depth + 1
                                                )).await
                                            }
                                        }
                                        Err(_) => {
                                            MatchStatus::NeedsVerification {
                                                reason: format!("Error looking up grandparent at ({}, {})", grandparent_chunk, grandparent_position),
                                            }
                                        }
                                    }
                                } else {
                                    // No grandparent (reached root) - phrase not confirmed
                                    MatchStatus::Rejected {
                                        parent_keyword: current_keyword.to_string(),
                                        reason: format!("Reached root '{}' without finding phrase '{}'", current_keyword, phrase),
                                    }
                                }
                            }
                        }
                    }
                }

                MatchStatus::NeedsVerification {
                    reason: format!("No parent information found for '{}'", current_keyword),
                }
            }
            Err(e) => {
                MatchStatus::NeedsVerification {
                    reason: format!("Error searching for parent '{}': {}", current_keyword, e),
                }
            }
        }
    }

    /// Compute the cartesian product of parent information vectors.
    ///
    /// Used during phrase search to generate all possible combinations of parent
    /// references when tokens have multiple occurrences per row. Each token might
    /// appear multiple times in a row with different parent keywords, so we need
    /// to check all combinations to find matching parent sets.
    ///
    /// # Arguments
    ///
    /// * `vecs` - Slice of vectors containing parent info tuples for each token.
    ///   Each tuple is (split_level, parent_chunk, parent_position).
    ///
    /// # Returns
    ///
    /// Vector of all possible combinations, where each combination contains one
    /// element from each input vector.
    ///
    /// # Example
    ///
    /// ```text
    /// Input:  [ [A1, A2], [B1], [C1, C2, C3] ]
    /// Output: [ [A1, B1, C1], [A1, B1, C2], [A1, B1, C3],
    ///           [A2, B1, C1], [A2, B1, C2], [A2, B1, C3] ]
    /// ```
    ///
    /// # Use Case
    ///
    /// When searching for phrase "a.b.c" and token "b" appears twice in the same row
    /// (with different parents), we need to check both possibilities to see if either
    /// parent leads to a verified match.
    ///
    /// # Performance
    ///
    /// Time complexity: O(∏ |vec_i|) - product of all vector lengths.
    /// Space complexity: Same as time (stores all combinations).
    /// Typically small since tokens rarely appear many times in same row.
    fn cartesian_product<'a>(&self, vecs: &[&'a Vec<(u16, Option<u16>, Option<u16>)>]) -> Vec<Vec<&'a (u16, Option<u16>, Option<u16>)>> {
        if vecs.is_empty() {
            return vec![Vec::new()];
        }

        let mut result = Vec::new();
        let rest = self.cartesian_product(&vecs[1..]);

        for item in vecs[0] {
            for combination in &rest {
                let mut new_combination = vec![item];
                new_combination.extend(combination);
                result.push(new_combination);
            }
        }

        result
    }
}