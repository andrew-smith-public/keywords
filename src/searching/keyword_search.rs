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
//! 2. **Chunk Location**: Binary search to find the metadata chunk containing the keyword
//! 3. **Exact Match**: Precise lookup within the chunk
//! 4. **Data Retrieval**: Load row group and row range information
//!
//! # Performance
//!
//! - **Search time**: O(1) for bloom filter check + O(log n) for chunk lookup
//! - **Memory**: Only index loaded, not the Parquet data itself
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
//!     let result = searcher.search("john.doe@example.com", None, true)?;
//!
//!     if result.found {
//!         println!("Found in {} columns", result.verified_matches.unwrap().columns.len());
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::path::Path;
use std::collections::HashMap;
use rkyv::Archived;
use rkyv::util::AlignedVec;
use rkyv::rancor::Error as RkyvError;
use tokio::fs;
use crate::index_data::{IndexFilters, MetadataEntry, KeywordDataFlat, ChunkInfo, ColumnKeywordsFile};
use crate::index_structure::column_filter::ColumnFilter;
use crate::index_structure::index_files::{index_filename, IndexFile};
use crate::ParquetSource;
use crate::searching::search_results::*;

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
/// it can perform multiple keyword searches efficiently without re-reading index files.
/// The searcher holds the entire index in memory for fast lookups.
///
/// # Index Structure
///
/// The index consists of:
/// - **Bloom filters**: For fast negative lookups (global and per-column)
/// - **Metadata chunks**: Keyword locations organized in sorted chunks
/// - **Data file**: Row group and row range information
/// - **Column keywords**: Mapping of keywords to columns
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
///         let result = searcher.search(keyword, None, true)?;
///         println!("{}: {}", keyword, result.found);
///     }
///
///     Ok(())
/// }
/// ```
pub struct KeywordSearcher {
    pub filters: IndexFilters,
    pub metadata_bytes: AlignedVec<16>,
    pub data_bytes: AlignedVec<16>,
    pub all_keywords: Vec<String>
}

impl KeywordSearcher {
    /// Create searcher from serialized index data (used for testing and after load).
    ///
    /// # Arguments
    ///
    /// * `files` - Serialized index files containing filters, metadata, data, and column keywords
    ///
    /// # Returns
    ///
    /// `Ok(KeywordSearcher)` ready to use for searches
    ///
    /// # Errors
    ///
    /// Returns error if rkyv deserialization fails
    pub fn from_serialized(
        files: &crate::index_data::DistributedIndexFiles
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Copy to aligned buffers
        let mut filters_bytes = AlignedVec::<16>::new();
        filters_bytes.extend_from_slice(&files.filters);

        let mut metadata_bytes = AlignedVec::<16>::new();
        metadata_bytes.extend_from_slice(&files.metadata);

        let mut data_bytes = AlignedVec::<16>::new();
        data_bytes.extend_from_slice(&files.data);

        let mut column_keywords_bytes = AlignedVec::<16>::new();
        column_keywords_bytes.extend_from_slice(&files.column_keywords);

        // Deserialize filters
        let archived_filters: &Archived<IndexFilters> = rkyv::access(&filters_bytes)
            .map_err(|e: RkyvError| format!("Failed to access archived filters: {}", e))?;

        let filters = IndexFilters {
            version: archived_filters.version.to_native(),
            parquet_etag: archived_filters.parquet_etag.to_string(),
            parquet_size: archived_filters.parquet_size.to_native(),
            parquet_last_modified: archived_filters.parquet_last_modified.to_native(),
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
                .map(|c| ChunkInfo {
                    start_keyword: c.start_keyword.to_string(),
                    offset: c.offset.to_native(),
                    length: c.length.to_native(),
                    count: c.count.to_native(),
                })
                .collect(),
        };

        // Deserialize column keywords
        let archived_column_keywords: &Archived<ColumnKeywordsFile> = rkyv::access(&column_keywords_bytes)
            .map_err(|e: RkyvError| format!("Failed to access archived column keywords: {}", e))?;

        let all_keywords: Vec<String> = archived_column_keywords.all_keywords.iter()
            .map(|s| s.to_string())
            .collect();

        Ok(Self {
            filters,
            metadata_bytes,
            data_bytes,
            all_keywords,
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
    /// * Any index file is missing or cannot be read (filters.rkyv, metadata.rkyv, data.bin, column_keywords.rkyv)
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
    ///     // Loads files like: test_filters.rkyv, test_metadata.rkyv, etc.
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

        // Check if index directory exists
        if !Path::new(&index_dir).exists() {
            return Err(format!("Index directory not found: {}", index_dir).into());
        }

        // Read all files from disk
        let filters_bytes = fs::read(Path::new(&index_dir).join(index_filename(IndexFile::Filters, index_file_prefix))).await?;
        let metadata_bytes = fs::read(Path::new(&index_dir).join(index_filename(IndexFile::Metadata, index_file_prefix))).await?;
        let data_bytes = fs::read(Path::new(&index_dir).join(index_filename(IndexFile::Data, index_file_prefix))).await?;
        let column_keywords_bytes = fs::read(Path::new(&index_dir).join(index_filename(IndexFile::ColumnKeywords, index_file_prefix))).await?;

        let files = crate::index_data::DistributedIndexFiles {
            filters: filters_bytes,
            metadata: metadata_bytes,
            data: data_bytes,
            column_keywords: column_keywords_bytes,
        };

        Self::from_serialized(&files)
    }

    /// Search for keywords or phrases in the index.
    ///
    /// This unified search method handles both exact keyword matching and phrase searches
    /// with token splitting and parent verification.
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
    /// let result = searcher.search("alice", None, true)?;
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
    /// let result = searcher.search("example.com", None, false)?;
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
    pub fn search(
        &self,
        search_for: &str,
        in_columns: Option<&str>,
        keyword_only: bool,
    ) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
        if keyword_only {
            // Exact keyword search - all matches are verified
            let old_result = self.search_keyword_internal(search_for, in_columns)?;

            Ok(SearchResult {
                query: search_for.to_string(),
                found: old_result.found,
                tokens: vec![search_for.to_string()],
                verified_matches: old_result.data,
                needs_verification: None,
            })
        } else {
            // Phrase search - split tokens and verify
            self.search_phrase_internal(search_for, in_columns)
        }
    }

    /// Internal keyword search implementation.
    ///
    /// Performs fast lookup using bloom filters and binary search to find all occurrences
    /// of the keyword across all columns. Returns detailed location information including
    /// columns, row groups, and specific row ranges.
    ///
    /// This is the old search() logic, now wrapped by the unified search() method.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The exact keyword to search for (case-sensitive). This should be a
    ///   complete token as it was indexed, not a partial match or pattern.
    /// * `column_filter` - Optional column name to restrict search to a specific column.
    ///
    /// # Returns
    ///
    /// `Ok(KeywordSearchResult)` - Search result with:
    /// * `found: false` - Keyword definitely not in index (bloom filter rejection)
    /// * `found: true` - Keyword found with detailed location data including columns,
    ///   row groups, row ranges, and occurrence counts
    ///
    /// Note: Due to bloom filter false positives (~1%), `found: true` means the keyword
    /// is *likely* present. Actual verification requires reading the Parquet file.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * Index data is corrupted (rkyv deserialization failure)
    /// * Column pool is missing required column IDs (corrupted index)
    ///
    /// # Examples
    ///
    /// **Basic search:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     let result = searcher.search("alice", None, true)?;
    ///
    ///     if result.found {
    ///         let data = result.verified_matches.unwrap();
    ///         println!("Found '{}' in columns: {:?}", result.query, data.columns);
    ///         println!("Total occurrences: {}", data.total_occurrences);
    ///     } else {
    ///         println!("Keyword '{}' not found", result.query);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Analyze search results:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("logs.parquet", None).await?;
    ///     let result = searcher.search("error", None, true)?;
    ///
    ///     if let Some(data) = result.verified_matches {
    ///         println!("Keyword: {}", result.query);
    ///         println!("Columns: {}", data.columns.len());
    ///
    ///         for col in &data.column_details {
    ///             println!("  Column '{}': {} row groups",
    ///                 col.column_name, col.row_groups.len());
    ///
    ///             for rg in &col.row_groups {
    ///                 let total_rows: u32 = rg.row_ranges.iter()
    ///                     .map(|r| r.end_row - r.start_row + 1)
    ///                     .sum();
    ///                 println!("    Row group {}: {} rows",
    ///                     rg.row_group_id, total_rows);
    ///             }
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Search multiple keywords:**
    ///
    /// ```no_run
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///
    ///     let keywords = vec!["alice", "bob", "charlie", "david"];
    ///     let mut found_count = 0;
    ///
    ///     for keyword in keywords {
    ///         let result = searcher.search(keyword, None, true)?;
    ///         if result.found {
    ///             found_count += 1;
    ///             println!("Found: {}", keyword);
    ///         }
    ///     }
    ///
    ///     println!("Found {} out of {} keywords", found_count, 4);
    ///     Ok(())
    /// }
    /// ```
    fn search_keyword_internal(
        &self,
        keyword: &str,
        column_filter: Option<&str>
    ) -> Result<KeywordSearchResult, Box<dyn std::error::Error + Send + Sync>> {
        // Step 1: Check appropriate filter based on whether we have a column filter
        if let Some(col_name) = column_filter {
            // When filtering to a specific column, check that column's bloom filter first
            if let Some(filter) = self.filters.column_filters.get(col_name) {
                if !filter.might_contain(keyword) {
                    return Ok(KeywordSearchResult {
                        keyword: keyword.to_string(),
                        found: false,
                        data: None,
                    });
                }
            } else {
                // Column doesn't exist in the index
                return Ok(KeywordSearchResult {
                    keyword: keyword.to_string(),
                    found: false,
                    data: None,
                });
            }
        } else {
            // When no column filter, check global filter (fast rejection)
            if !self.filters.global_filter.might_contain(keyword) {
                return Ok(KeywordSearchResult {
                    keyword: keyword.to_string(),
                    found: false,
                    data: None,
                });
            }
        }

        // Step 2: Find the chunk that might contain this keyword
        let chunk_info = self.find_chunk_for_keyword(keyword);

        let chunk_info = match chunk_info {
            Some(info) => info,
            None => {
                return Ok(KeywordSearchResult {
                    keyword: keyword.to_string(),
                    found: false,
                    data: None,
                });
            }
        };

        // Step 3: Load and search the chunk
        let chunk_bytes = &self.metadata_bytes[chunk_info.offset as usize..(chunk_info.offset + chunk_info.length as u64) as usize];

        let archived_chunk: &Archived<Vec<MetadataEntry>> = rkyv::access(chunk_bytes)
            .map_err(|e: RkyvError| format!("Failed to access archived chunk: {}", e))?;

        // Find the keyword in the chunk
        let metadata_entry = archived_chunk.iter()
            .find(|entry| entry.keyword.as_str() == keyword);

        let metadata_entry = match metadata_entry {
            Some(entry) => entry,
            None => {
                return Ok(KeywordSearchResult {
                    keyword: keyword.to_string(),
                    found: false,
                    data: None,
                });
            }
        };

        // Step 4: Load the keyword data from data.bin
        let data_offset = metadata_entry.offset.to_native();
        let data_length: usize = metadata_entry.length.to_native() as usize;
        let data_bytes = &self.data_bytes[data_offset as usize..(data_offset as usize + data_length)];

        let archived_data: &Archived<KeywordDataFlat> = rkyv::access(data_bytes)
            .map_err(|e: RkyvError| format!("Failed to access archived keyword data: {}", e))?;

        // Determine which column(s) to process
        // When column_filter is None, use column_id 0 (aggregate of all columns)
        // When column_filter is Some, process only that specific column
        let filter_to_column_id: Option<u32> = if let Some(col_name) = column_filter {
            // Find the column_id for the requested column
            let found_id = metadata_entry.column_ids.iter()
                .map(|id| id.to_native())
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
                return Ok(KeywordSearchResult {
                    keyword: keyword.to_string(),
                    found: false,
                    data: None,
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

        for col in archived_data.columns.iter() {
            let column_id: u32 = col.column_id.to_native();

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

            for rg in col.row_groups.iter() {
                let row_group_id: u16 = rg.row_group_id.to_native();
                let mut row_ranges = Vec::new();

                for flat_row in rg.rows.iter() {
                    let row: u32 = flat_row.row.to_native();
                    let additional_rows: u32 = flat_row.additional_rows.to_native();
                    let splits_matched: u16 = flat_row.splits_matched.to_native();
                    let parent_offset: Option<u32> = flat_row.parent_offset.as_ref().map(|p| p.to_native());

                    row_ranges.push(RowRange {
                        start_row: row,
                        end_row: row + additional_rows,
                        splits_matched,
                        parent_offset,
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

            // Get actual column IDs from metadata
            let actual_column_ids: Vec<u32> = metadata_entry.column_ids.iter()
                .map(|id| id.to_native())
                .filter(|&id| id != 0) // Skip column_id 0 itself
                .collect();

            // Create one ColumnLocation per actual column, all sharing the same row group data
            for column_id in actual_column_ids {
                if let Some(column_name) = self.filters.column_pool.get(column_id) {
                    column_details.push(ColumnLocation {
                        column_name: column_name.to_string(),
                        row_groups: aggregate_row_groups.clone(),
                    });
                }
            }
        }


        // Build column list
        // When we used column_id 0, we need to get the actual column names from metadata_entry.column_ids
        // When we used a specific column, we already have it in column_details
        let columns: Vec<String> = if filter_to_column_id == Some(0) {
            // We used the aggregate (column_id 0), so get actual column names from metadata
            metadata_entry.column_ids.iter()
                .map(|id| id.to_native())
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

        Ok(KeywordSearchResult {
            keyword: keyword.to_string(),
            found: true,
            data: Some(KeywordLocationData {
                columns,
                total_occurrences,
                splits_matched: archived_data.splits_matched.to_native(),
                column_details,
            }),
        })
    }

    /// Find the chunk that might contain a keyword
    fn find_chunk_for_keyword(&self, keyword: &str) -> Option<&ChunkInfo> {
        // Binary search to find the right chunk
        // We want to find the chunk where: chunk.start_keyword <= keyword < next_chunk.start_keyword

        if self.filters.chunk_index.is_empty() {
            return None;
        }

        // Find first chunk where start_keyword > keyword
        match self.filters.chunk_index.binary_search_by(|chunk| {
            chunk.start_keyword.as_str().cmp(keyword)
        }) {
            Ok(idx) => Some(&self.filters.chunk_index[idx]),
            Err(idx) => {
                if idx == 0 {
                    // Keyword is before first chunk - still check first chunk
                    Some(&self.filters.chunk_index[0])
                } else {
                    // Keyword belongs to previous chunk
                    Some(&self.filters.chunk_index[idx - 1])
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
    ///     if searcher.search_in_column("alice", "username")? {
    ///         println!("Found 'alice' in username column");
    ///     }
    ///
    ///     // Check multiple columns
    ///     for column in &["email", "description", "notes"] {
    ///         if searcher.search_in_column("test", column)? {
    ///             println!("Found 'test' in {} column", column);
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn search_in_column(&self, keyword: &str, column_name: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // Check column filter first
        if let Some(filter) = self.filters.column_filters.get(column_name) {
            if !filter.might_contain(keyword) {
                return Ok(false);
            }
        }

        // Do filtered search for this specific column
        let result = self.search(keyword, Some(column_name), true)?;

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
    ///     let search1 = searcher.search("error", None, true)?;
    ///     let search2 = searcher.search("database", None, true)?;
    ///     let search3 = searcher.search("connection", None, true)?;
    ///
    ///     // Convert to KeywordSearchResult for combine_and
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let r1 = KeywordSearchResult { keyword: search1.query.clone(), found: search1.found, data: search1.verified_matches.clone() };
    ///     let r2 = KeywordSearchResult { keyword: search2.query.clone(), found: search2.found, data: search2.verified_matches.clone() };
    ///     let r3 = KeywordSearchResult { keyword: search3.query.clone(), found: search3.found, data: search3.verified_matches.clone() };
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
    ///     let search1 = searcher.search("alice", None, true)?;
    ///     let search2 = searcher.search("admin", None, true)?;
    ///
    ///     // Convert to KeywordSearchResult for combine_and
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let r1 = KeywordSearchResult { keyword: search1.query.clone(), found: search1.found, data: search1.verified_matches.clone() };
    ///     let r2 = KeywordSearchResult { keyword: search2.query.clone(), found: search2.found, data: search2.verified_matches.clone() };
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
    pub fn combine_and(results: &[KeywordSearchResult]) -> Option<CombinedSearchResult> {
        if results.is_empty() {
            return None;
        }

        // Check if all keywords were found
        if results.iter().any(|r| !r.found) {
            return Some(CombinedSearchResult {
                keywords: results.iter().map(|r| r.keyword.clone()).collect(),
                row_groups: Vec::new(),
            });
        }

        // Build per-column row sets for each result
        // Structure: result_idx -> column_name -> row_group_id -> HashSet<row>
        let mut per_result_columns: Vec<HashMap<String, HashMap<u16, std::collections::HashSet<u32>>>> = Vec::new();

        for result in results {
            let mut column_map: HashMap<String, HashMap<u16, std::collections::HashSet<u32>>> = HashMap::new();

            if let Some(data) = &result.data {
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
            keywords: results.iter().map(|r| r.keyword.clone()).collect(),
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
    ///     let search_error = searcher.search("error", None, true)?;
    ///     let search_critical = searcher.search("critical", None, true)?;
    ///     let search_fatal = searcher.search("fatal", None, true)?;
    ///
    ///     // Convert to KeywordSearchResult for combine_or
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let error = KeywordSearchResult { keyword: search_error.query.clone(), found: search_error.found, data: search_error.verified_matches.clone() };
    ///     let critical = KeywordSearchResult { keyword: search_critical.query.clone(), found: search_critical.found, data: search_critical.verified_matches.clone() };
    ///     let fatal = KeywordSearchResult { keyword: search_fatal.query.clone(), found: search_fatal.found, data: search_fatal.verified_matches.clone() };
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
    ///     let search_alice = searcher.search("alice", None, true)?;
    ///     let search_alicia = searcher.search("alicia", None, true)?;
    ///     let search_ali = searcher.search("ali", None, true)?;
    ///
    ///     // Convert to KeywordSearchResult for combine_or
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let alice = KeywordSearchResult { keyword: search_alice.query.clone(), found: search_alice.found, data: search_alice.verified_matches.clone() };
    ///     let alicia = KeywordSearchResult { keyword: search_alicia.query.clone(), found: search_alicia.found, data: search_alicia.verified_matches.clone() };
    ///     let ali = KeywordSearchResult { keyword: search_ali.query.clone(), found: search_ali.found, data: search_ali.verified_matches.clone() };
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
    pub fn combine_or(results: &[KeywordSearchResult]) -> Option<CombinedSearchResult> {
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

            let data = result.data.as_ref()?;
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
            keywords: results.iter().map(|r| r.keyword.clone()).collect(),
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
    ///     let result = searcher.search("keyword", None, true)?;
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
    ///     let result = searcher.search("john-doe", None, false)?;
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
    ///     let result = searcher.search("user@example.com", None, false)?;
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
    ///     let result = searcher.search("test-value", None, false)?;
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
    fn search_phrase_internal(&self, phrase: &str, column_filter: Option<&str>) -> Result<SearchResult, Box<dyn std::error::Error + Send + Sync>> {
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
            let result = self.search_keyword_internal(&tokens[0], column_filter)?;

            // Single token always returns verified matches
            return Ok(SearchResult {
                query: phrase.to_string(),
                tokens,
                found: result.found,
                verified_matches: result.data,
                needs_verification: None,
            });
        }

        // Search for each token
        // Note: We don't fail if a token isn't found - it might be an intermediate
        // parent that wasn't indexed separately. We'll find it through parent verification.
        let mut token_results = Vec::new();
        let mut found_tokens = Vec::new();

        for token in &tokens {
            let result = self.search_keyword_internal(token, column_filter)?;
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
            self.find_and_verify_multi_token_matches(phrase, &token_results)?;

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
    fn split_phrase(&self, phrase: &str) -> Vec<String> {
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
                                parent_offset: None,
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
                        parent_offset: None,
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

    /// Find rows where all tokens exist and verify using parent keywords
    fn find_and_verify_multi_token_matches(
        &self,
        phrase: &str,
        token_results: &[KeywordSearchResult],
    ) -> Result<(Vec<PotentialMatch>, Vec<PotentialMatch>), Box<dyn std::error::Error + Send + Sync>> {
        let mut confirmed_matches = Vec::new();
        let mut needs_verification = Vec::new();

        // Get the first token's results as the base
        let base_result = &token_results[0];
        let base_data = match &base_result.data {
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
                let token_data = match &token_result.data {
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

                // Build map of row -> (split-level, parent offset) for base token
                let base_row_info: HashMap<u32, Vec<(u16, Option<u32>)>> = rg.row_ranges.iter()
                    .fold(HashMap::new(), |mut acc, range| {
                        for row in range.start_row..=range.end_row {
                            acc.entry(row)
                                .or_insert_with(Vec::new)
                                .push((range.splits_matched, range.parent_offset));
                        }
                        acc
                    });

                // For each other token, build similar maps
                let mut all_token_row_info: Vec<HashMap<u32, Vec<(u16, Option<u32>)>>> = Vec::new();
                let mut all_tokens_have_rows = true;

                for other_col in &other_token_column_data {
                    // Find this row group in the other token's data
                    let other_rg = other_col.row_groups.iter()
                        .find(|r| r.row_group_id == row_group_id);

                    match other_rg {
                        Some(rg_data) => {
                            let row_info: HashMap<u32, Vec<(u16, Option<u32>)>> = rg_data.row_ranges.iter()
                                .fold(HashMap::new(), |mut acc, range| {
                                    for row in range.start_row..=range.end_row {
                                        acc.entry(row)
                                            .or_insert_with(Vec::new)
                                            .push((range.splits_matched, range.parent_offset));
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
                    // Try all combinations of parent offsets for each token
                    for base_info in base_infos {
                        for other_info_combinations in self.cartesian_product(&other_token_infos) {
                            let all_parent_offsets: Vec<Option<u32>> = std::iter::once(base_info.1)
                                .chain(other_info_combinations.iter().map(|(_, parent)| *parent))
                                .collect();

                            // Convert splits_matched bitmasks to actual split levels, then take minimum
                            let min_split_level = std::iter::once(base_info.0)
                                .chain(other_info_combinations.iter().map(|(split, _)| *split))
                                .filter_map(|splits_matched| self.get_parent_split_level(splits_matched))
                                .min()
                                .unwrap_or(0) as u16;
                            let status = self.verify_match_with_parent(
                                phrase,
                                &all_parent_offsets,
                            );

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
    fn get_min_phrase_split_level(&self, phrase: &str) -> Option<usize> {
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
    fn get_parent_split_level(&self, child_splits_matched: u16) -> Option<usize> {
        // Find the lowest bit set (excluding bit 0 which is the root marker)
        for level in 1..=4 {
            if (child_splits_matched & (1 << level)) != 0 {
                // First set bit at position `level` means parent was split at level-1
                return Some(level - 1);
            }
        }
        None // Root token (only bit 0 set or no bits set)
    }

    fn verify_match_with_parent(
        &self,
        phrase: &str,
        parent_offsets: &[Option<u32>],
    ) -> MatchStatus {
        // Check if all tokens have the same parent
        let first_parent = parent_offsets[0];
        let all_same_parent = parent_offsets.iter().all(|&p| p == first_parent);

        if !all_same_parent {
            return MatchStatus::NeedsVerification {
                reason: "Tokens have different parents".to_string(),
            };
        }

        match first_parent {
            Some(parent_offset) => {
                // Look up parent keyword
                if let Some(parent_keyword) = self.all_keywords.get(parent_offset as usize) {
                    // Check if the phrase exists as substring in parent
                    if parent_keyword.contains(phrase) {
                        MatchStatus::Confirmed {
                            parent_keyword: parent_keyword.clone(),
                        }
                    } else {
                        // Recurse to check grandparents
                        // Get minimum split level in phrase to optimize traversal
                        let min_phrase_level = self.get_min_phrase_split_level(phrase);
                        self.verify_match_with_grandparent(phrase, parent_keyword, min_phrase_level, 0)
                    }
                } else {
                    MatchStatus::NeedsVerification {
                        reason: format!("Parent offset {} out of bounds", parent_offset),
                    }
                }
            }
            None => {
                MatchStatus::NeedsVerification {
                    reason: "No parent information (root token)".to_string(),
                }
            }
        }
    }

    /// Recursively search up the parent chain for a keyword that contains the phrase
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
    fn verify_match_with_grandparent(
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
        // Search for the current keyword in the index to get its parent_offset
        match self.search_keyword_internal(current_keyword, None) {
            Ok(result) => {
                if !result.found {
                    // Current keyword not found in index - can't recurse further
                    return MatchStatus::NeedsVerification {
                        reason: format!("Parent keyword '{}' not found in index", current_keyword),
                    };
                }

                // Get the row data to find parent_offset
                if let Some(data) = result.data {
                    // Look through all column_details to find a parent_offset
                    // We just need any one since they should all have the same parent
                    for col_detail in &data.column_details {
                        for rg in &col_detail.row_groups {
                            for range in &rg.row_ranges {
                                // Optimization: Check if this parent's split-level is compatible with the phrase
                                // Only check parents split at >= min_phrase_level (same or lower priority)
                                if let Some(min_level) = min_phrase_level {
                                    if let Some(parent_split_level) = self.get_parent_split_level(range.splits_matched) {
                                        // Skip if parent was split at higher priority (lower number) than phrase
                                        if parent_split_level < min_level {
                                            continue;
                                        }
                                    }
                                }

                                if let Some(grandparent_offset) = range.parent_offset {
                                    // Found a grandparent! Look it up and check
                                    if let Some(grandparent_keyword) = self.all_keywords.get(grandparent_offset as usize) {
                                        if grandparent_keyword.contains(phrase) {
                                            return MatchStatus::Confirmed {
                                                parent_keyword: grandparent_keyword.clone(),
                                            };
                                        } else {
                                            // Recurse further up the chain
                                            return self.verify_match_with_grandparent(phrase, grandparent_keyword, min_phrase_level, depth + 1);
                                        }
                                    } else {
                                        return MatchStatus::NeedsVerification {
                                            reason: format!("Grandparent offset {} out of bounds", grandparent_offset),
                                        };
                                    }
                                } else {
                                    // No grandparent (reached root) - phrase not confirmed
                                    return MatchStatus::Rejected {
                                        parent_keyword: current_keyword.to_string(),
                                        reason: format!("Reached root '{}' without finding phrase '{}'", current_keyword, phrase),
                                    };
                                }
                            }
                        }
                    }
                }

                // No parent_offset found in data
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

    /// Helper to get cartesian product of parent info combinations
    fn cartesian_product<'a>(&self, vecs: &[&'a Vec<(u16, Option<u32>)>]) -> Vec<Vec<&'a (u16, Option<u32>)>> {
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

#[cfg(test)]
mod tests {
    use crate::searching::keyword_search::KeywordSearcher;
    use crate::index_structure::column_filter::ColumnFilter;
    use crate::index_data::IndexFilters;
    use crate::utils::column_pool::ColumnPool;

    /// Creates a test searcher with standard split character configuration
    fn create_test_searcher() -> KeywordSearcher {
        let split_chars_inclusive: Vec<Vec<char>> = vec![
            vec!['\r', '\n', '\t', '\'', '"', '<', '>', '(', ')', '|', ',', '!', ';', '{', '}', '*', ' '],
            vec!['/', '@', '=', ':', '\\', '?', '&'],
            vec!['.', '$', '#', '`', '~', '^', '+'],
            vec!['-', '_'],
        ];

        let filters = IndexFilters {
            version: 1,
            parquet_etag: "test".to_string(),
            parquet_size: 0,
            parquet_last_modified: 0,
            error_rate: 0.01,
            split_chars_inclusive,
            column_pool: ColumnPool::new(),
            column_filters: std::collections::HashMap::new(),
            global_filter: ColumnFilter::RkyvHashSet(vec![]),
            chunk_index: vec![],
        };

        KeywordSearcher {
            filters,
            metadata_bytes: rkyv::util::AlignedVec::new(),
            data_bytes: rkyv::util::AlignedVec::new(),
            all_keywords: vec![],
        }
    }

    #[test]
    fn test_hierarchical_split_email() {
        let searcher = create_test_searcher();

        // Test what split_phrase produces for "user@example.com"
        let tokens = searcher.split_phrase("user@example.com");

        println!("Tokens from split_phrase('user@example.com'): {:?}", tokens);

        // EXPECTED (from hierarchical indexing):
        // ["user@example.com", "user", "example.com", "example", "com"]
        //
        // With OLD buggy code: ["user", "example", "com"] - length 3 ❌
        // With NEW fixed code: ["com", "example", "example.com", "user", "user@example.com"] - length 5 ✅

        // These assertions PASS with fixed code, FAIL with buggy code
        assert!(tokens.contains(&"user@example.com".to_string()),
                "Missing root parent token 'user@example.com'");
        assert!(tokens.contains(&"example.com".to_string()),
                "Missing intermediate parent token 'example.com'");
        assert!(tokens.contains(&"user".to_string()),
                "Missing leaf token 'user'");
        assert!(tokens.contains(&"example".to_string()),
                "Missing leaf token 'example'");
        assert!(tokens.contains(&"com".to_string()),
                "Missing leaf token 'com'");
        assert_eq!(tokens.len(), 5,
                   "Should have all 5 hierarchical tokens, got {} tokens: {:?}", tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_hyphenated_name() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("john-smith-jr");

        println!("Tokens from split_phrase('john-smith-jr'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // ["john-smith-jr", "john", "smith", "jr"]
        //
        // With OLD buggy code: ["john", "smith", "jr"] - length 3 ❌
        // With NEW fixed code: All 4 tokens present ✅

        assert!(tokens.contains(&"john-smith-jr".to_string()),
                "Missing parent token 'john-smith-jr'");
        assert!(tokens.contains(&"john".to_string()));
        assert!(tokens.contains(&"smith".to_string()));
        assert!(tokens.contains(&"jr".to_string()));
        assert_eq!(tokens.len(), 4,
                   "Should have 4 tokens, got {}: {:?}", tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_path() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("/usr/local/bin");

        println!("Tokens from split_phrase('/usr/local/bin'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // ["/usr/local/bin", "usr", "local", "bin"]
        //
        // With OLD buggy code: ["usr", "local", "bin"] - length 3 ❌
        // With NEW fixed code: All 4 tokens present ✅

        assert!(tokens.contains(&"/usr/local/bin".to_string()),
                "Missing parent token '/usr/local/bin'");
        assert!(tokens.contains(&"usr".to_string()));
        assert!(tokens.contains(&"local".to_string()));
        assert!(tokens.contains(&"bin".to_string()));
        assert_eq!(tokens.len(), 4,
                   "Should have 4 tokens, got {}: {:?}", tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_domain() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("api.example.com");

        println!("Tokens from split_phrase('api.example.com'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // Split at level 2 (dot notation):
        // ["api.example.com", "api", "example.com", "example", "com"]
        //
        // But wait - "example.com" should also be processed:
        // So we get: ["api.example.com", "api", "example.com", "example", "com"]
        //
        // With OLD buggy code: ["api", "example", "com"] - length 3 ❌
        // With NEW fixed code: At least ["api.example.com", "example.com"] parents present ✅

        assert!(tokens.contains(&"api.example.com".to_string()),
                "Missing root parent 'api.example.com'");
        assert!(tokens.contains(&"api".to_string()));
        assert!(tokens.contains(&"example".to_string()));
        assert!(tokens.contains(&"com".to_string()));

        // The exact hierarchical structure should create more than 3 tokens
        assert!(tokens.len() > 3,
                "Should have more than 3 tokens for hierarchical split, got {}: {:?}",
                tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_complex() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("user-name@test.example.com");

        println!("Tokens from split_phrase('user-name@test.example.com'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // Level 0: No split
        // Level 1: Split on @ → ["user-name@test.example.com", "user-name", "test.example.com"]
        // Level 2: "test.example.com" splits on . → ["test.example.com", "test", "example.com", "example", "com"]
        // Level 3: "user-name" splits on - → ["user-name", "user", "name"]
        //
        // Full set: ["user-name@test.example.com", "user-name", "test.example.com",
        //            "example.com", "user", "name", "test", "example", "com"]
        //
        // With OLD buggy code: Just ["user", "name", "test", "example", "com"] - length 5 ❌
        // With NEW fixed code: Many more tokens including all parents ✅

        assert!(tokens.contains(&"user-name@test.example.com".to_string()),
                "Missing root parent");
        assert!(tokens.contains(&"user-name".to_string()),
                "Missing 'user-name' parent");
        assert!(tokens.contains(&"test.example.com".to_string()),
                "Missing 'test.example.com' parent");

        // Should have significantly more than just the leaf tokens
        assert!(tokens.len() > 5,
                "Should have more than 5 tokens for complex hierarchical split, got {}: {:?}",
                tokens.len(), tokens);
    }

    #[test]
    fn test_single_token_no_split() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("simple");

        println!("Tokens from split_phrase('simple'): {:?}", tokens);

        // No delimiters - should just return the single token
        assert_eq!(tokens.len(), 1);
        assert!(tokens.contains(&"simple".to_string()));
    }

    #[test]
    fn test_empty_string() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("");

        println!("Tokens from split_phrase(''): {:?}", tokens);

        // Empty string might return empty vec or vec with empty string
        // Either is acceptable
        assert!(tokens.is_empty() || (tokens.len() == 1 && tokens[0].is_empty()),
                "Empty string should return empty vec or vec with one empty string");
    }

    // ========== Parent Verification Tests ==========

    #[test]
    fn test_get_parent_split_level_root() {
        let searcher = create_test_searcher();

        // Bit 0 only = root token
        let splits_matched = 1;
        assert_eq!(searcher.get_parent_split_level(splits_matched), None,
                   "Root token (bit 0 only) should return None");
    }

    #[test]
    fn test_get_parent_split_level_level0() {
        let searcher = create_test_searcher();

        // Bit 1 set (value 2) = started from level 0 split
        let splits_matched = 2;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(0),
                   "Token with only bit 1 set was split at level 0");

        // Bits 1,2,3,4 all set = started from level 0, survived all others
        let splits_matched = 2 | 4 | 8 | 16; // 30
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(0),
                   "Token starting with bit 1 was split at level 0");
    }

    #[test]
    fn test_get_parent_split_level_level1() {
        let searcher = create_test_searcher();

        // Bit 2 set (value 4) = started from level 1 split
        let splits_matched = 4;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(1),
                   "Token with only bit 2 set was split at level 1");

        // Bits 2,3,4 set = started from level 1
        let splits_matched = 4 | 8 | 16; // 28
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(1),
                   "Token with bits 2,3,4 set was split at level 1");
    }

    #[test]
    fn test_get_parent_split_level_level2() {
        let searcher = create_test_searcher();

        // Bit 3 set (value 8) = started from level 2 split
        let splits_matched = 8;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(2),
                   "Token with only bit 3 set was split at level 2");

        // Bits 3,4 set = started from level 2
        let splits_matched = 8 | 16; // 24
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(2),
                   "Token with bits 3,4 set (value 24) was split at level 2");
    }

    #[test]
    fn test_get_parent_split_level_level3() {
        let searcher = create_test_searcher();

        // Bit 4 set (value 16) = started from level 3 split
        let splits_matched = 16;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(3),
                   "Token with only bit 4 set was split at level 3");
    }

    #[test]
    fn test_get_min_phrase_split_level_whitespace() {
        let searcher = create_test_searcher();

        // Level 0: whitespace
        assert_eq!(searcher.get_min_phrase_split_level("hello world"), Some(0),
                   "Space is level 0 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("hello\nworld"), Some(0),
                   "Newline is level 0 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_level1() {
        let searcher = create_test_searcher();

        // Level 1: @ / : = \ ? &
        assert_eq!(searcher.get_min_phrase_split_level("user@example.com"), Some(1),
                   "@ is level 1 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("path/to/file"), Some(1),
                   "/ is level 1 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("key:value"), Some(1),
                   "Colon is level 1 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_level2() {
        let searcher = create_test_searcher();

        // Level 2: . $ # ` ~ ^ +
        assert_eq!(searcher.get_min_phrase_split_level("example.com"), Some(2),
                   "Dot is level 2 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("$variable"), Some(2),
                   "Dollar is level 2 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_level3() {
        let searcher = create_test_searcher();

        // Level 3: - _
        assert_eq!(searcher.get_min_phrase_split_level("user-name"), Some(3),
                   "Hyphen is level 3 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("file_name"), Some(3),
                   "Underscore is level 3 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_mixed() {
        let searcher = create_test_searcher();

        // Should return the MINIMUM (highest priority) level
        assert_eq!(searcher.get_min_phrase_split_level("hello world-name"), Some(0),
                   "Space (level 0) has priority over hyphen (level 3)");
        assert_eq!(searcher.get_min_phrase_split_level("user@example.com"), Some(1),
                   "@ (level 1) has priority over . (level 2)");
        assert_eq!(searcher.get_min_phrase_split_level("file.name-version"), Some(2),
                   ". (level 2) has priority over - (level 3)");
    }

    #[test]
    fn test_get_min_phrase_split_level_no_delimiters() {
        let searcher = create_test_searcher();

        assert_eq!(searcher.get_min_phrase_split_level("simple"), None,
                   "No delimiters should return None");
        assert_eq!(searcher.get_min_phrase_split_level("12345"), None,
                   "Numbers with no delimiters should return None");
    }

}