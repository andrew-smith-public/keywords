//! Efficient Parquet data reading with row group and row-level pruning.
//!
//! This module provides optimized reading of Parquet data by using search results to skip
//! unnecessary row groups and rows. Instead of reading entire Parquet files, it uses the
//! keyword index to identify exactly which row groups and rows contain matching data,
//! dramatically reducing I/O and processing time.
//!
//! # Performance Benefits
//!
//! - **Row Group Pruning**: Skip entire row groups that don't contain target data
//! - **Row-Level Filtering**: Within relevant row groups, read only matching rows
//! - **Column Projection**: Read only the columns you need
//! - **Network Optimization**: Minimize range requests for remote files (S3, Azure, etc.)
//!
//! Typical performance gains:
//!
//! # Examples
//!
//! ```no_run
//! use keywords::searching::pruned_reader::PrunedParquetReader;
//! use keywords::searching::keyword_search::KeywordSearcher;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let searcher = KeywordSearcher::load("data.parquet", None).await?;
//!     let result = searcher.search("keyword", None, true)?;
//!
//!     let reader = PrunedParquetReader::from_path("data.parquet");
//!     // Use the new read_search_result method that accepts SearchResult directly
//!     let batches = reader.read_search_result(&result, None).await?;
//!
//!     println!("Read {} batches", batches.len());
//!     Ok(())
//! }
//! ```

use arrow::array::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::array::BooleanArray;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use futures::StreamExt;
use crate::searching::search_results::{KeywordSearchResult, CombinedSearchResult, SearchResult};
use crate::utils::file_interaction_local_and_cloud::get_object_store;
use crate::ParquetSource;
use std::sync::Arc;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;

/// Efficiently read Parquet data using search results to prune row groups and rows.
///
/// This reader uses keyword search results to identify exactly which row groups and rows to read,
/// skipping all irrelevant data for optimal performance.
///
/// # Examples
///
/// ```no_run
/// use keywords::searching::pruned_reader::PrunedParquetReader;
/// use keywords::searching::keyword_search::KeywordSearcher;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
///     let result = searcher.search("keyword", None, true)?;
///
///     let reader = PrunedParquetReader::from_path("data.parquet");
///     // Use read_search_result for SearchResult or convert to KeywordSearchResult for read_matching_rows
///     let batches = reader.read_search_result(&result, None).await?;
///
///     println!("Read {} batches", batches.len());
///     Ok(())
/// }
/// ```
pub struct PrunedParquetReader {
    source: ParquetSource,
}

impl PrunedParquetReader {
    /// Create a new pruned Parquet reader for the specified source.
    ///
    /// This constructor only stores the source; no I/O is performed until a read method is called.
    ///
    /// # Arguments
    ///
    /// * `source` - Parquet source, either a file path (local or remote like S3/Azure)
    ///   or in-memory bytes. Use `ParquetSource::Path(path)` or `ParquetSource::Bytes(vec)`.
    ///
    /// # Returns
    ///
    /// A new `PrunedParquetReader` instance ready to read from the specified source.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::ParquetSource;
    ///
    /// // From file path
    /// let reader = PrunedParquetReader::new(ParquetSource::Path("/data/users.parquet".to_string()));
    ///
    /// // From S3
    /// let reader = PrunedParquetReader::new(ParquetSource::Path("s3://bucket/data.parquet".to_string()));
    ///
    /// // From in-memory bytes
    /// let parquet_bytes = vec![/* ... */];
    /// let reader = PrunedParquetReader::new(ParquetSource::from(parquet_bytes));
    /// ```
    pub fn new(source: ParquetSource) -> Self {
        Self { source }
    }

    /// Create a reader from a file path (convenience method).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the Parquet file. Can be local path, S3 URL (`s3://bucket/path`),
    ///   or Azure URL (`az://container/path`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    ///
    /// let reader = PrunedParquetReader::from_path("data.parquet");
    /// ```
    pub fn from_path(path: &str) -> Self {
        Self::new(ParquetSource::Path(path.to_string()))
    }

    /// Create a reader from in-memory parquet bytes (convenience method).
    ///
    /// # Arguments
    ///
    /// * `bytes` - Parquet file data as bytes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use bytes::Bytes;
    ///
    /// let parquet_bytes = vec![/* ... */];
    /// let reader = PrunedParquetReader::from_bytes(Bytes::from(parquet_bytes));
    /// ```
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self::new(ParquetSource::Bytes(bytes))
    }

    /// Create a ParquetObjectReader with file size optimization
    /// Providing the file size ensures bounded range requests instead of suffix range requests,
    /// which is an important optimization to avoid extra calls
    ///
    /// Performance consideration: ParquetObjectReader may issue multiple GET requests per row group
    /// This is a known trade-off in the Parquet ecosystem: small targeted range requests provide
    /// better performance for selective queries but incur more API calls. For pruned reads (our use case),
    /// this is generally optimal since we only fetch needed row groups. Alternative approaches:
    /// - Pre-fetching entire file: Would waste bandwidth for selective queries
    /// - Batched range requests: Parquet format doesn't always allow predictable batching
    /// - Caching layer: Adds complexity, most benefit comes from OS page cache already
    /// Monitor S3 request costs if this becomes a bottleneck. Current approach aligns with
    /// standard Parquet reader behavior and works well for our access patterns.
    async fn create_object_reader(&self) -> Result<ParquetObjectReader, Box<dyn std::error::Error + Send + Sync>> {
        match &self.source {
            ParquetSource::Path(path) => {
                let (store, obj_path) = get_object_store(path).await?;
                let meta = store.head(&obj_path).await?;
                Ok(ParquetObjectReader::new(store, obj_path)
                    .with_file_size(meta.size))
            }
            ParquetSource::Bytes(bytes) => {
                // Create in-memory object store
                let store = Arc::new(InMemory::new());
                let path = ObjectPath::from("in_memory.parquet");
                let bytes_copy = Bytes::copy_from_slice(bytes);
                let file_size = bytes_copy.len() as u64;
                store.put(&path, bytes_copy.into()).await?;

                Ok(ParquetObjectReader::new(store, path)
                    .with_file_size(file_size))
            }
        }
    }

    /// Read only the rows that match a single keyword search.
    ///
    /// Uses the search result to identify which row groups and rows to read, efficiently skipping
    /// all non-matching data. Returns batches of up to 8192 rows each.
    ///
    /// # Arguments
    ///
    /// * `search_result` - Result from [`KeywordSearcher::search()`]. If `found` is `false`,
    ///   returns empty vector immediately without accessing the file.
    /// * `columns` - Optional column projection:
    ///   - `None` - Read all columns
    ///   - `Some(vec)` - Read only specified columns (reduces I/O and memory)
    ///   - Column names not found in schema are silently ignored
    ///
    /// # Returns
    ///
    /// `Ok(Vec<RecordBatch>)` - Vector of Arrow RecordBatches containing matching rows.
    /// Returns empty vector if keyword not found or no matches in file.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * File cannot be accessed (not found, permission denied, network failure)
    /// * Parquet file is corrupted or has invalid metadata
    /// * Parquet format is incompatible with Arrow reader
    /// * Search result has `found=true` but `data` is `None` (invalid state)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     let search_result = searcher.search("test@example.com", None, true)?;
    ///
    ///     let reader = PrunedParquetReader::from_path("data.parquet");
    ///
    ///     // Convert to KeywordSearchResult for read_matching_rows
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let result = KeywordSearchResult {
    ///         keyword: search_result.query.clone(),
    ///         found: search_result.found,
    ///         data: search_result.verified_matches.clone(),
    ///     };
    ///
    ///     // Read all columns
    ///     let batches = reader.read_matching_rows(&result, None).await?;
    ///     println!("Found {} batches", batches.len());
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **With column projection:**
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("users.parquet", None).await?;
    ///     let search_result = searcher.search("alice", None, true)?;
    ///
    ///     let reader = PrunedParquetReader::from_path("users.parquet");
    ///
    ///     // Convert to KeywordSearchResult for read_matching_rows
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let result = KeywordSearchResult {
    ///         keyword: search_result.query.clone(),
    ///         found: search_result.found,
    ///         data: search_result.verified_matches.clone(),
    ///     };
    ///
    ///     // Only read specific columns
    ///     let columns = vec!["user_id".to_string(), "email".to_string()];
    ///     let batches = reader.read_matching_rows(&result, Some(columns)).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`KeywordSearcher::search()`]: crate::searching::keyword_search::KeywordSearcher::search
    pub async fn read_matching_rows(
        &self,
        search_result: &KeywordSearchResult,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if !search_result.found {
            return Ok(Vec::new());
        }

        let data = search_result.data.as_ref()
            .ok_or("Search result has no data")?;

        // Collect all row groups that contain the keyword
        let mut row_groups_to_read = std::collections::HashSet::new();
        let mut row_group_ranges: std::collections::HashMap<u16, Vec<(u32, u32)>> =
            std::collections::HashMap::new();

        for col_detail in &data.column_details {
            for rg in &col_detail.row_groups {
                row_groups_to_read.insert(rg.row_group_id as usize);

                let ranges = row_group_ranges.entry(rg.row_group_id).or_insert_with(Vec::new);
                for range in &rg.row_ranges {
                    ranges.push((range.start_row, range.end_row));
                }
            }
        }

        if row_groups_to_read.is_empty() {
            return Ok(Vec::new());
        }

        // Create ParquetObjectReader
        let object_reader = self.create_object_reader().await?;

        let mut all_batches = Vec::new();
        let row_groups_vec: Vec<usize> = row_groups_to_read.into_iter().collect();

        for &rg_idx in &row_groups_vec {
            // Create async stream builder
            let mut builder = ParquetRecordBatchStreamBuilder::new(object_reader.clone()).await?;

            // Apply column projection if specified
            if let Some(cols) = &columns {
                let schema = builder.schema();
                let indices: Vec<usize> = cols.iter()
                    .filter_map(|col_name| {
                        schema.fields().iter().position(|f| f.name() == col_name)
                    })
                    .collect();

                if !indices.is_empty() {
                    let mask = ProjectionMask::leaves(
                        builder.metadata().file_metadata().schema_descr(),
                        indices
                    );
                    builder = builder.with_projection(mask);
                }
            }

            let mut stream = builder
                .with_row_groups(vec![rg_idx])
                .with_batch_size(8192)
                .build()?;

            // Get the row ranges for this row group
            let ranges = row_group_ranges.get(&(rg_idx as u16));

            let mut row_offset = 0u32;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let batch_rows = batch.num_rows() as u32;

                // Filter to only matching rows if we have specific ranges
                let filtered_batch = if let Some(ranges) = ranges {
                    // Adjust ranges to be relative to this batch
                    let batch_relative_ranges: Vec<(u32, u32)> = ranges
                        .iter()
                        .filter_map(|&(start, end)| {
                            // Check if this range overlaps with current batch
                            if end < row_offset || start >= row_offset + batch_rows {
                                None // Range doesn't overlap this batch
                            } else {
                                // Adjust range to be relative to batch start
                                let batch_start = start.saturating_sub(row_offset);
                                let batch_end = (end - row_offset).min(batch_rows - 1);
                                Some((batch_start, batch_end))
                            }
                        })
                        .collect();

                    if batch_relative_ranges.is_empty() {
                        row_offset += batch_rows;
                        continue;
                    }

                    filter_batch_to_ranges(&batch, &batch_relative_ranges)?
                } else {
                    batch
                };

                if filtered_batch.num_rows() > 0 {
                    all_batches.push(filtered_batch);
                }

                row_offset += batch_rows;
            }
        }

        Ok(all_batches)
    }


    /// Read Parquet data for unified SearchResult.
    ///
    /// Reads both verified matches and matches needing verification from the SearchResult.
    /// This is the recommended method for use with the unified search API.
    ///
    /// # Arguments
    ///
    /// * `search_result` - Result from `KeywordSearcher::search()`
    /// * `columns` - Optional column projection (None = all columns)
    ///
    /// # Returns
    ///
    /// Vector of RecordBatches containing all matching rows (verified + needs verification).
    /// If you only want verified matches, filter the search_result before passing it.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use keywords::searching::pruned_reader::PrunedParquetReader;
    /// # use keywords::searching::keyword_search::KeywordSearcher;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let searcher = KeywordSearcher::load("data.parquet", None).await?;
    /// let result = searcher.search("example.com", None, false)?;
    ///
    /// let reader = PrunedParquetReader::from_path("data.parquet");
    /// let batches = reader.read_search_result(&result, None).await?;
    ///
    /// println!("Read {} batches", batches.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_search_result(
        &self,
        search_result: &SearchResult,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if !search_result.found {
            return Ok(Vec::new());
        }

        // Collect row groups and ranges from both verified and needs_verification
        let mut row_groups_to_read = std::collections::HashSet::new();
        let mut row_group_ranges: std::collections::HashMap<u16, Vec<(u32, u32)>> =
            std::collections::HashMap::new();

        // Process verified matches
        if let Some(verified) = &search_result.verified_matches {
            for col_detail in &verified.column_details {
                for rg in &col_detail.row_groups {
                    row_groups_to_read.insert(rg.row_group_id as usize);

                    let ranges = row_group_ranges.entry(rg.row_group_id).or_insert_with(Vec::new);
                    for range in &rg.row_ranges {
                        ranges.push((range.start_row, range.end_row));
                    }
                }
            }
        }

        // Process needs_verification matches
        if let Some(needs_check) = &search_result.needs_verification {
            for col_detail in &needs_check.column_details {
                for rg in &col_detail.row_groups {
                    row_groups_to_read.insert(rg.row_group_id as usize);

                    let ranges = row_group_ranges.entry(rg.row_group_id).or_insert_with(Vec::new);
                    for range in &rg.row_ranges {
                        ranges.push((range.start_row, range.end_row));
                    }
                }
            }
        }

        // Use existing read logic
        self.read_row_groups_and_ranges(row_groups_to_read, row_group_ranges, columns).await
    }


    /// Read only the rows that match combined search results (AND/OR logic).
    ///
    /// Used after combining multiple keyword searches with [`KeywordSearcher::combine_and()`]
    /// or [`KeywordSearcher::combine_or()`]. Efficiently reads only rows matching the combined criteria.
    ///
    /// # Arguments
    ///
    /// * `combined_result` - Result from combining multiple keyword searches with AND/OR logic.
    ///   If `row_groups` is empty, returns empty vector immediately.
    /// * `columns` - Optional column projection:
    ///   - `None` - Read all columns
    ///   - `Some(vec)` - Read only specified columns
    ///
    /// # Returns
    ///
    /// `Ok(Vec<RecordBatch>)` - Vector of Arrow RecordBatches containing rows that match
    /// the combined search criteria. Returns empty vector if no matches.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * File cannot be accessed (not found, permission denied, network failure)
    /// * Parquet file is corrupted or has invalid metadata
    /// * Parquet format is incompatible with Arrow reader
    ///
    /// # Examples
    ///
    /// **AND logic (all keywords must match):**
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("logs.parquet", None).await?;
    ///
    ///     // Find rows containing ALL keywords
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
    ///         let reader = PrunedParquetReader::from_path("logs.parquet");
    ///         let batches = reader.read_combined_rows(&result, None).await?;
    ///
    ///         let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    ///         println!("Found {} rows with all three keywords", total);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **OR logic (any keyword can match):**
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("logs.parquet", None).await?;
    ///
    ///     // Find rows containing ANY keyword
    ///     let search_error = searcher.search("error", None, true)?;
    ///     let search_warning = searcher.search("warning", None, true)?;
    ///     let search_critical = searcher.search("critical", None, true)?;
    ///
    ///     // Convert to KeywordSearchResult for combine_or
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let error = KeywordSearchResult { keyword: search_error.query.clone(), found: search_error.found, data: search_error.verified_matches.clone() };
    ///     let warning = KeywordSearchResult { keyword: search_warning.query.clone(), found: search_warning.found, data: search_warning.verified_matches.clone() };
    ///     let critical = KeywordSearchResult { keyword: search_critical.query.clone(), found: search_critical.found, data: search_critical.verified_matches.clone() };
    ///
    ///     let combined = KeywordSearcher::combine_or(&[error, warning, critical]);
    ///
    ///     if let Some(result) = combined {
    ///         let reader = PrunedParquetReader::from_path("logs.parquet");
    ///         let batches = reader.read_combined_rows(&result, None).await?;
    ///
    ///         let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    ///         println!("Found {} high-severity entries", total);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`KeywordSearcher::combine_and()`]: crate::searching::keyword_search::KeywordSearcher::combine_and
    /// [`KeywordSearcher::combine_or()`]: crate::searching::keyword_search::KeywordSearcher::combine_or
    pub async fn read_combined_rows(
        &self,
        combined_result: &CombinedSearchResult,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        if combined_result.row_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Collect row groups and ranges
        let mut row_groups_to_read = std::collections::HashSet::new();
        let mut row_group_ranges: std::collections::HashMap<u16, Vec<(u32, u32)>> =
            std::collections::HashMap::new();

        for rg in &combined_result.row_groups {
            row_groups_to_read.insert(rg.row_group_id as usize);

            let ranges = row_group_ranges.entry(rg.row_group_id).or_insert_with(Vec::new);
            for range in &rg.row_ranges {
                ranges.push((range.start_row, range.end_row));
            }
        }

        // Create ParquetObjectReader
        let object_reader = self.create_object_reader().await?;

        // Pre-allocate vector for better performance
        let row_groups_vec: Vec<usize> = row_groups_to_read.into_iter().collect();
        let mut all_batches = Vec::with_capacity(row_groups_vec.len());

        for &rg_idx in &row_groups_vec {
            // Create async stream builder
            let mut builder = ParquetRecordBatchStreamBuilder::new(object_reader.clone()).await?;

            // Apply column projection if specified
            if let Some(ref cols) = columns {
                let schema = builder.schema();
                let indices: Vec<usize> = cols.iter()
                    .filter_map(|col_name| {
                        schema.fields().iter().position(|f| f.name() == col_name)
                    })
                    .collect();

                if !indices.is_empty() {
                    let mask = ProjectionMask::leaves(
                        builder.metadata().file_metadata().schema_descr(),
                        indices
                    );
                    builder = builder.with_projection(mask);
                }
            }

            let mut stream = builder
                .with_row_groups(vec![rg_idx])
                .with_batch_size(8192)
                .build()?;

            // Get the row ranges for this row group
            let ranges = row_group_ranges.get(&(rg_idx as u16));

            let mut row_offset = 0u32;
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                let batch_rows = batch.num_rows() as u32;

                // Filter to only matching rows
                let filtered_batch = if let Some(ranges) = ranges {
                    // Adjust ranges to be relative to this batch
                    let batch_relative_ranges: Vec<(u32, u32)> = ranges
                        .iter()
                        .filter_map(|&(start, end)| {
                            // Check if this range overlaps with current batch
                            if end < row_offset || start >= row_offset + batch_rows {
                                None // Range doesn't overlap this batch
                            } else {
                                // Adjust range to be relative to batch start
                                let batch_start = start.saturating_sub(row_offset);
                                let batch_end = (end - row_offset).min(batch_rows - 1);
                                Some((batch_start, batch_end))
                            }
                        })
                        .collect();

                    if batch_relative_ranges.is_empty() {
                        row_offset += batch_rows;
                        continue;
                    }

                    filter_batch_to_ranges(&batch, &batch_relative_ranges)?
                } else {
                    batch
                };

                if filtered_batch.num_rows() > 0 {
                    all_batches.push(filtered_batch);
                }

                row_offset += batch_rows;
            }
        }

        Ok(all_batches)
    }

    /// Get statistics about how much data can be skipped by pruning.
    ///
    /// Analyzes the search result against Parquet metadata to calculate how many row groups
    /// and rows can be skipped. Useful for understanding query selectivity and deciding
    /// whether to use pruned reading vs full scan.
    ///
    /// # Arguments
    ///
    /// * `search_result` - Result from [`KeywordSearcher::search()`].
    ///
    /// # Returns
    ///
    /// `Ok(PruningStats)` - Statistics showing:
    /// * Total row groups and rows in file
    /// * Row groups and rows that must be read
    /// * Row groups and rows that can be skipped
    /// * Skip percentages for both metrics
    ///
    /// If `search_result.found` is `false`, returns stats showing 100% skip rate.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// * File cannot be accessed to read metadata
    /// * Parquet metadata is corrupted or invalid
    /// * Search result has `found=true` but `data` is `None` (invalid state)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use keywords::searching::pruned_reader::PrunedParquetReader;
    /// use keywords::searching::keyword_search::KeywordSearcher;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
    ///     let search_result = searcher.search("rare_value", None, true)?;
    ///
    ///     // Convert to KeywordSearchResult for get_pruning_stats and read_matching_rows
    ///     use keywords::searching::search_results::KeywordSearchResult;
    ///     let result = KeywordSearchResult {
    ///         keyword: search_result.query.clone(),
    ///         found: search_result.found,
    ///         data: search_result.verified_matches.clone(),
    ///     };
    ///
    ///     let reader = PrunedParquetReader::from_path("data.parquet");
    ///     let stats = reader.get_pruning_stats(&result).await?;
    ///
    ///     println!("Row Groups: {}/{} ({:.1}% skipped)",
    ///         stats.row_groups_to_read,
    ///         stats.total_row_groups,
    ///         stats.row_group_skip_percentage);
    ///     println!("Rows: {}/{} ({:.1}% skipped)",
    ///         stats.rows_to_read,
    ///         stats.total_rows,
    ///         stats.row_skip_percentage);
    ///
    ///     // Decide whether to proceed with pruned read
    ///     if stats.row_skip_percentage > 50.0 {
    ///         let batches = reader.read_matching_rows(&result, None).await?;
    ///         println!("High selectivity - read {} batches", batches.len());
    ///     } else {
    ///         println!("Low selectivity - consider full scan instead");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`KeywordSearcher::search()`]: crate::searching::keyword_search::KeywordSearcher::search
    pub async fn get_pruning_stats(
        &self,
        search_result: &KeywordSearchResult,
    ) -> Result<PruningStats, Box<dyn std::error::Error + Send + Sync>> {
        // Create ParquetObjectReader and builder to access metadata
        let object_reader = self.create_object_reader().await?;
        let builder = ParquetRecordBatchStreamBuilder::new(object_reader).await?;
        let metadata = builder.metadata();

        let total_row_groups = metadata.num_row_groups();
        let total_rows: i64 = metadata.file_metadata().num_rows();

        if !search_result.found {
            return Ok(PruningStats {
                total_row_groups,
                row_groups_to_read: 0,
                row_groups_skipped: total_row_groups,
                row_group_skip_percentage: 100.0,
                total_rows: total_rows as u64,
                rows_to_read: 0,
                rows_skipped: total_rows as u64,
                row_skip_percentage: 100.0,
            });
        }

        let data = search_result.data.as_ref().unwrap();

        // Count row groups to read
        let mut row_groups_to_read = std::collections::HashSet::new();
        let mut rows_to_read = 0u64;

        for col_detail in &data.column_details {
            for rg in &col_detail.row_groups {
                row_groups_to_read.insert(rg.row_group_id);

                for range in &rg.row_ranges {
                    rows_to_read += (range.end_row - range.start_row + 1) as u64;
                }
            }
        }

        let row_groups_read = row_groups_to_read.len();
        let row_groups_skipped = total_row_groups - row_groups_read;
        let rows_skipped = (total_rows as u64).saturating_sub(rows_to_read);

        Ok(PruningStats {
            total_row_groups,
            row_groups_to_read: row_groups_read,
            row_groups_skipped,
            row_group_skip_percentage: (row_groups_skipped as f64 / total_row_groups as f64) * 100.0,
            total_rows: total_rows as u64,
            rows_to_read,
            rows_skipped,
            row_skip_percentage: (rows_skipped as f64 / total_rows as f64) * 100.0,
        })
    }

    /// Internal helper to read specific row groups and ranges.
    async fn read_row_groups_and_ranges(
        &self,
        row_groups_to_read: std::collections::HashSet<usize>,
        row_group_ranges: std::collections::HashMap<u16, Vec<(u32, u32)>>,
        columns: Option<Vec<String>>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
        let reader = self.create_object_reader().await?;
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let metadata = builder.metadata().clone();
        let schema = builder.schema().clone();

        let projection_mask = if let Some(ref cols) = columns {
            let indices: Vec<usize> = cols.iter()
                .filter_map(|col_name| {
                    schema.fields().iter().position(|f| f.name() == col_name)
                })
                .collect();
            ProjectionMask::roots(metadata.file_metadata().schema_descr(), indices)
        } else {
            ProjectionMask::all()
        };

        let mut batches = Vec::new();

        for rg_idx in row_groups_to_read {
            if rg_idx >= metadata.num_row_groups() {
                continue;
            }

            // Recreate builder for each row group (builder doesn't implement Clone)
            let reader = self.create_object_reader().await?;
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
            let builder = builder.with_projection(projection_mask.clone());
            let builder = builder.with_batch_size(8192);
            let mut stream = builder.with_row_groups(vec![rg_idx]).build()?;

            // Get the row ranges for this row group
            let ranges = row_group_ranges.get(&(rg_idx as u16));

            let mut row_offset = 0u32;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let batch_rows = batch.num_rows() as u32;

                if let Some(ranges) = ranges {
                    // BUG FIX: Convert row-group-relative ranges to batch-relative ranges
                    // This is the same logic used in read_combined_rows
                    let batch_relative_ranges: Vec<(u32, u32)> = ranges
                        .iter()
                        .filter_map(|&(start, end)| {
                            // Check if this range overlaps with current batch
                            if end < row_offset || start >= row_offset + batch_rows {
                                None // Range doesn't overlap this batch
                            } else {
                                // Adjust range to be relative to batch start
                                let batch_start = start.saturating_sub(row_offset);
                                let batch_end = (end - row_offset).min(batch_rows - 1);
                                Some((batch_start, batch_end))
                            }
                        })
                        .collect();

                    if batch_relative_ranges.is_empty() {
                        row_offset += batch_rows;
                        continue;
                    }

                    let filtered = filter_batch_to_ranges(&batch, &batch_relative_ranges)?;
                    if filtered.num_rows() > 0 {
                        batches.push(filtered);
                    }
                } else {
                    batches.push(batch);
                }

                row_offset += batch_rows;
            }
        }

        Ok(batches)
    }
}

/// Statistics about how much data can be skipped during pruned reading.
///
/// Provides detailed metrics about pruning efficiency at both the row group and row level,
/// helping you understand query selectivity and potential performance benefits.
///
/// # Fields
///
/// * `total_row_groups` - Total number of row groups in the Parquet file
/// * `row_groups_to_read` - Number of row groups that must be read (contain matches)
/// * `row_groups_skipped` - Number of row groups that can be skipped entirely
/// * `row_group_skip_percentage` - Percentage of row groups skipped (0.0 to 100.0)
/// * `total_rows` - Total number of rows in the Parquet file
/// * `rows_to_read` - Number of rows that must be read (match search criteria)
/// * `rows_skipped` - Number of rows that can be skipped
/// * `row_skip_percentage` - Percentage of rows skipped (0.0 to 100.0)
///
/// # Interpreting Results
///
/// **Row skip percentage guidelines:**
/// # Examples
///
/// ```no_run
/// use keywords::searching::pruned_reader::PrunedParquetReader;
/// use keywords::searching::keyword_search::KeywordSearcher;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let searcher = KeywordSearcher::load("data.parquet", None).await?;
///     let search_result = searcher.search("keyword", None, true)?;
///
///     // Convert to KeywordSearchResult for get_pruning_stats
///     use keywords::searching::search_results::KeywordSearchResult;
///     let result = KeywordSearchResult {
///         keyword: search_result.query.clone(),
///         found: search_result.found,
///         data: search_result.verified_matches.clone(),
///     };
///
///     let reader = PrunedParquetReader::from_path("data.parquet");
///     let stats = reader.get_pruning_stats(&result).await?;
///
///     // Check row group pruning efficiency
///     if stats.row_group_skip_percentage > 80.0 {
///         println!("Excellent row group pruning: {:.1}%", stats.row_group_skip_percentage);
///     }
///
///     // Check row-level pruning efficiency
///     if stats.row_skip_percentage > 90.0 {
///         println!("Excellent row-level pruning: {:.1}%", stats.row_skip_percentage);
///     }
///
///     // Estimate I/O savings
///     let io_reduction = stats.row_skip_percentage / 100.0;
///     println!("Estimated I/O reduction: {:.1}x", 1.0 / (1.0 - io_reduction));
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PruningStats {
    pub total_row_groups: usize,
    pub row_groups_to_read: usize,
    pub row_groups_skipped: usize,
    pub row_group_skip_percentage: f64,
    pub total_rows: u64,
    pub rows_to_read: u64,
    pub rows_skipped: u64,
    pub row_skip_percentage: f64,
}

/// Filter a record batch to only include rows in the specified ranges
/// OPTIMIZATION: Use fill() for contiguous ranges and slice() for single ranges
fn filter_batch_to_ranges(
    batch: &RecordBatch,
    ranges: &[(u32, u32)],
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let num_rows = batch.num_rows();

    // Special case: single contiguous range starting at 0 - use zero-copy slice
    if ranges.len() == 1 && ranges[0].0 == 0 {
        let end = (ranges[0].1 as usize).min(num_rows - 1);
        return Ok(batch.slice(0, end + 1));
    }

    // Create boolean mask for which rows to keep
    let mut mask = vec![false; num_rows];

    for &(start, end) in ranges {
        let start = start as usize;
        let end = (end as usize).min(num_rows - 1);

        if start < num_rows && end < num_rows {
            // Use fill() which is more efficient than a loop
            mask[start..=end].fill(true);
        }
    }

    let mask_array = BooleanArray::from(mask);
    let filtered = filter_record_batch(batch, &mask_array)?;

    Ok(filtered)
}


#[cfg(test)]
mod tests {
    use tokio::sync::OnceCell;
    use std::sync::Arc;
    use arrow::array::{StringArray, Int32Array, Int64Array, Float64Array, BooleanArray};
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::basic::Compression;
    use rand::{Rng, SeedableRng};
    use crate::{build_index_in_memory, ParquetSource};
    use super::*;
    use crate::searching::keyword_search::KeywordSearcher;

    static TEST_SEARCHER: OnceCell<KeywordSearcher> = OnceCell::const_new();
    static TEST_PARQUET: OnceCell<Bytes> = OnceCell::const_new();

    /// Generate a small test parquet file with 500 distinct values, 1000 rows
    fn create_test_parquet() -> Result<Bytes, Box<dyn std::error::Error>> {
        const DISTINCT_VALUES: usize = 500;
        const TOTAL_ROWS: usize = 1000;

        let mut rng = rand::rngs::StdRng::seed_from_u64(12345);

        // Create schema with mixed types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
        ]));

        // Generate pool of distinct values
        let names: Vec<String> = (0..DISTINCT_VALUES)
            .map(|i| format!("user_{}", i))
            .collect();
        let emails: Vec<String> = (0..DISTINCT_VALUES)
            .map(|i| format!("user_{}@test{}.com", i, i % 10))
            .collect();
        let statuses = vec!["active", "inactive", "pending", "suspended"];

        // Generate row data
        let mut id_data = Vec::with_capacity(TOTAL_ROWS);
        let mut name_data = Vec::with_capacity(TOTAL_ROWS);
        let mut email_data = Vec::with_capacity(TOTAL_ROWS);
        let mut status_data = Vec::with_capacity(TOTAL_ROWS);
        let mut age_data = Vec::with_capacity(TOTAL_ROWS);
        let mut score_data = Vec::with_capacity(TOTAL_ROWS);
        let mut active_data = Vec::with_capacity(TOTAL_ROWS);

        for i in 0..TOTAL_ROWS {
            id_data.push(i as i32);
            name_data.push(names[rng.random_range(0..DISTINCT_VALUES)].clone());
            email_data.push(emails[rng.random_range(0..DISTINCT_VALUES)].clone());
            status_data.push(statuses[rng.random_range(0..statuses.len())].to_string());
            age_data.push(rng.random_range(18..80) as i64);
            score_data.push(rng.random_range(0.0..100.0));
            active_data.push(rng.random_bool(0.7));
        }

        // Create arrays
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(id_data)),
                Arc::new(StringArray::from(name_data)),
                Arc::new(StringArray::from(email_data)),
                Arc::new(StringArray::from(status_data)),
                Arc::new(Int64Array::from(age_data)),
                Arc::new(Float64Array::from(score_data)),
                Arc::new(BooleanArray::from(active_data)),
            ],
        )?;

        // Write to buffer
        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Verify file is < 1MB
        assert!(buffer.len() < 1024 * 1024,
                "Small parquet should be < 1MB, got {} bytes ({:.2} MB)",
                buffer.len(), buffer.len() as f64 / (1024.0 * 1024.0));

        Ok(Bytes::from(buffer))
    }

    async fn get_test_parquet() -> &'static Bytes {
        TEST_PARQUET.get_or_init(|| async {
            create_test_parquet().expect("Failed to create test parquet")
        }).await
    }

    async fn get_searcher() -> &'static KeywordSearcher {
        TEST_SEARCHER.get_or_init(|| async {
            println!("Building test index in memory...");
            let parquet_bytes = get_test_parquet().await;
            build_index_in_memory(ParquetSource::Bytes(parquet_bytes.clone()), None, None)
                .await
                .expect("Build Index Failed")
        }).await
    }

    #[tokio::test]
    async fn test_pruned_read() {
        let searcher = get_searcher().await;
        let parquet_bytes = get_test_parquet().await;

        // Search for a keyword that exists in the generated data
        let search_result = searcher.search("user_0", None, true).unwrap();

        if !search_result.found {
            println!("Keyword not found - skipping test");
            return;
        }

        // Convert to KeywordSearchResult for compatibility with get_pruning_stats and read_matching_rows
        let result = KeywordSearchResult {
            keyword: search_result.query.clone(),
            found: search_result.found,
            data: search_result.verified_matches.clone(),
        };

        // Create pruned reader from bytes
        let reader = PrunedParquetReader::new(ParquetSource::Bytes(parquet_bytes.clone()));

        // Get pruning stats
        let stats = reader.get_pruning_stats(&result).await.unwrap();
        println!("Pruning stats:");
        println!("  Row groups: {}/{} ({:.1}% skipped)",
                 stats.row_groups_to_read, stats.total_row_groups, stats.row_group_skip_percentage);
        println!("  Rows: {}/{} ({:.1}% skipped)",
                 stats.rows_to_read, stats.total_rows, stats.row_skip_percentage);

        // Read only matching rows
        let batches = reader.read_matching_rows(&result, None).await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Read {} rows in {} batches", total_rows, batches.len());
    }

    #[tokio::test]
    async fn test_combined_pruned_read() {
        let searcher = get_searcher().await;
        let parquet_bytes = get_test_parquet().await;

        // Search with AND logic for keywords that exist in generated data
        let search_result1 = searcher.search("user_0", None, true).unwrap();
        let search_result2 = searcher.search("active", None, true).unwrap();

        // Convert to KeywordSearchResult for combine_and
        let result1 = KeywordSearchResult {
            keyword: search_result1.query.clone(),
            found: search_result1.found,
            data: search_result1.verified_matches.clone(),
        };
        let result2 = KeywordSearchResult {
            keyword: search_result2.query.clone(),
            found: search_result2.found,
            data: search_result2.verified_matches.clone(),
        };

        let combined = KeywordSearcher::combine_and(&[result1, result2]);

        if let Some(combined_result) = combined {
            let reader = PrunedParquetReader::new(ParquetSource::Bytes(parquet_bytes.clone()));

            // Read only rows matching both conditions
            let batches = reader.read_combined_rows(&combined_result, None).await.unwrap();

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("Combined AND read {} rows in {} batches", total_rows, batches.len());
        }
    }

    #[tokio::test]
    async fn test_column_projection() {
        let searcher = get_searcher().await;
        let parquet_bytes = get_test_parquet().await;

        let search_result = searcher.search("active", None, true).unwrap();

        if !search_result.found {
            return;
        }

        // Convert to KeywordSearchResult for read_matching_rows
        let result = KeywordSearchResult {
            keyword: search_result.query.clone(),
            found: search_result.found,
            data: search_result.verified_matches.clone(),
        };

        let reader = PrunedParquetReader::new(ParquetSource::Bytes(parquet_bytes.clone()));

        // Read only specific columns from generated data
        let columns = vec!["name".to_string(), "email".to_string(), "status".to_string()];
        let batches = reader.read_matching_rows(&result, Some(columns)).await.unwrap();

        if !batches.is_empty() {
            println!("Columns read: {:?}",
                     batches[0].schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
        }
    }

}