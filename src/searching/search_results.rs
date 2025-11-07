//! Pure data structures for keyword search results.
//!
//! This module contains immutable data structures that represent the outcomes of keyword
//! searches in Parquet indexes. These types contain no logic - they are purely for data
//! representation and transfer between the search API and consumers.
//!
//! # Type Hierarchy
//!
//! The main result types follow this structure:
//!
//! ```text
//! KeywordSearchResult
//! ├── keyword: String
//! ├── found: bool
//! └── data: Option<KeywordLocationData>
//!     ├── columns: Vec<String>
//!     ├── total_occurrences: u64
//!     └── column_details: Vec<ColumnLocation>
//!         └── row_groups: Vec<RowGroupLocation>
//!             └── row_ranges: Vec<RowRange>
//! ```
//!
//! # Search Result Types
//!
//! - [`KeywordSearchResult`] - Single keyword search result
//! - [`MultiTokenSearchResult`] - Multi-token phrase search result
//! - [`CombinedSearchResult`] - Result of combining multiple keyword searches
//! - [`IndexInfo`] - Metadata about the index itself
//!
//! # Usage Example
//!
//! ```no_run
//! # use keywords::searching::search_results::KeywordSearchResult;
//! #
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let result: KeywordSearchResult = todo!();
//! // Check if keyword was found
//! if result.found {
//!     if let Some(data) = result.data {
//!         println!("Found '{}' in {} columns", result.keyword, data.columns.len());
//!         println!("Total occurrences: {}", data.total_occurrences);
//!
//!         // Iterate through each column
//!         for col in &data.column_details {
//!             println!("  Column '{}' has {} row groups",
//!                 col.column_name, col.row_groups.len());
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```

/// Result of searching for a single keyword in the index.
///
/// This is the primary return type for keyword searches. It contains the search query,
/// a boolean indicating whether the keyword was found, and if found, detailed location
/// information about where the keyword appears in the Parquet file.
///
/// # Fields
///
/// - `keyword` - The exact keyword that was searched (case-sensitive)
/// - `found` - `true` if the keyword exists in the index, `false` otherwise
/// - `data` - Detailed location data, only present when `found` is `true`
///
/// # Examples
///
/// **Keyword found:**
/// ```no_run
/// # use keywords::searching::search_results::{KeywordSearchResult, KeywordLocationData};
/// # let result = KeywordSearchResult {
/// #     keyword: "example".to_string(),
/// #     found: true,
/// #     data: Some(KeywordLocationData {
/// #         columns: vec!["email".to_string(), "description".to_string()],
/// #         total_occurrences: 42,
/// #         splits_matched: 0b0010,
/// #         column_details: vec![],
/// #     }),
/// # };
/// assert!(result.found);
/// assert_eq!(result.keyword, "example");
/// if let Some(data) = result.data {
///     println!("Found in {} columns", data.columns.len());
/// }
/// ```
///
/// **Keyword not found:**
/// ```
/// # use keywords::searching::search_results::KeywordSearchResult;
/// # let result = KeywordSearchResult {
/// #     keyword: "nonexistent".to_string(),
/// #     found: false,
/// #     data: None,
/// # };
/// assert!(!result.found);
/// assert!(result.data.is_none());
/// ```
#[derive(Debug, Clone)]
pub struct KeywordSearchResult {
    /// The keyword that was searched for (exact, case-sensitive).
    pub keyword: String,

    /// Whether the keyword was found in the index.
    ///
    /// Note: This indicates presence in the index, not necessarily in the actual Parquet data
    /// due to bloom filter false positives (typically <1% with default settings).
    pub found: bool,

    /// Detailed information about where the keyword appears.
    ///
    /// Only `Some` when `found` is `true`. Contains column names, occurrence counts,
    /// and detailed location information for retrieving the actual data from Parquet.
    pub data: Option<KeywordLocationData>,
}


/// Unified search result for both keyword and phrase searches.
///
/// This structure provides a consistent interface for all search operations,
/// whether searching for exact keywords or phrases that need token splitting.
///
/// # Fields
///
/// - `query` - The original search query
/// - `found` - Whether any matches were found
/// - `tokens` - The tokens that were searched (single token for keyword_only searches)
/// - `verified_matches` - Matches guaranteed correct, can be used directly
/// - `needs_verification` - Potential matches requiring Parquet file verification
///
/// # Verification Status
///
/// **Verified Matches** (100% certain):
/// - Exact keyword matches from index
/// - Phrase matches confirmed via parent keyword relationships
/// - Can answer queries directly without reading Parquet
///
/// **Needs Verification** (requires Parquet read):
/// - Phrase matches where parent verification is inconclusive
/// - Must read actual Parquet data to confirm
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::keyword_search::KeywordSearcher;
/// # use keywords::searching::search_results::SearchResult;
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let searcher = KeywordSearcher::load("data.parquet", None).await?;
/// let result = searcher.search("example.com", None, false)?;
///
/// if result.found {
///     // Use verified matches directly
///     if let Some(verified) = &result.verified_matches {
///         println!("Verified in {} columns", verified.columns.len());
///     }
///
///     // Read Parquet only if needed
///     if let Some(needs_check) = &result.needs_verification {
///         println!("Need to verify {} more locations", needs_check.total_occurrences);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// The original search query.
    pub query: String,

    /// Whether any matches were found (verified or needing verification).
    pub found: bool,

    /// Tokens that were searched for.
    ///
    /// For keyword_only searches: single token [query]
    /// For phrase searches: multiple tokens from hierarchical split
    pub tokens: Vec<String>,

    /// Matches that are guaranteed correct.
    ///
    /// These can be used immediately without reading the Parquet file:
    /// - Exact keyword matches from the index
    /// - Phrase matches confirmed via parent keyword verification
    pub verified_matches: Option<KeywordLocationData>,

    /// Potential matches that require Parquet file verification.
    ///
    /// These locations have all the required tokens but couldn't be
    /// verified via parent keywords. Must read actual Parquet data
    /// to determine if the phrase actually exists.
    pub needs_verification: Option<KeywordLocationData>,
}


/// Information about where a keyword appears in the Parquet file.
///
/// This structure provides a complete picture of a keyword's locations across the entire
/// Parquet file, including which columns contain it, how many times it appears, and
/// detailed row-level location information.
///
/// # Fields
///
/// - `columns` - Names of columns containing this keyword
/// - `total_occurrences` - Sum of all occurrences across all columns
/// - `splits_matched` - Bitmask indicating which delimiter levels matched
/// - `column_details` - Fine-grained location data per column
///
/// # Split Levels
///
/// The `splits_matched` field is a bitmask where each bit represents a split-level:
/// - Bit 0 (0b0001): Level 0 - No split (original string)
/// - Bit 1 (0b0010): Level 1 - Split by ` `, `\n`, etc.
/// - Bit 2 (0b0100): Level 2 - Split by `/`, `@`, `=`, etc.
/// - Bit 3 (0b1000): Level 3 - Split by `.`, `$`, `#`, etc.
/// - Bit 4 (0b10000): Level 4 - Split by `-`, `_`
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::KeywordLocationData;
/// # let data = KeywordLocationData {
/// #     columns: vec!["email".to_string(), "notes".to_string()],
/// #     total_occurrences: 127,
/// #     splits_matched: 0b0110,
/// #     column_details: vec![],
/// # };
/// // Check which columns have the keyword
/// for col in &data.columns {
///     println!("Keyword appears in column: {}", col);
/// }
///
/// // Check split levels
/// if data.splits_matched & 0b0010 != 0 {
///     println!("Matched at level 1 (whitespace/punctuation splits)");
/// }
/// ```
#[derive(Debug, Clone)]
pub struct KeywordLocationData {
    /// Names of all columns that contain this keyword.
    ///
    /// The order matches the order in `column_details`. Guaranteed to be non-empty
    /// when this struct exists.
    pub columns: Vec<String>,

    /// Total number of keyword occurrences across all columns and row groups.
    ///
    /// This is the sum of all row ranges across all columns. Useful for ranking
    /// search results by relevance.
    pub total_occurrences: u64,

    /// Bitmask indicating which split levels matched this keyword.
    ///
    /// Each bit represents whether the keyword matched at a particular delimiter level:
    /// - `0b0001` (1): Original string, no splits
    /// - `0b0010` (2): Split by primary delimiters (space, newline, etc.)
    /// - `0b0100` (4): Split by secondary delimiters (/, @, =, etc.)
    /// - `0b1000` (8): Split by tertiary delimiters (., $, #, etc.)
    /// - `0b10000` (16): Split by quaternary delimiters (-, _)
    ///
    /// Multiple bits can be set if the keyword appears at multiple split levels.
    pub splits_matched: u16,

    /// Detailed location information for each column.
    ///
    /// Contains row group and row-level location data. The length matches `columns.len()`.
    pub column_details: Vec<ColumnLocation>,
}

/// Location data for a keyword within a specific column.
///
/// Represents all occurrences of a keyword in a single column, organized by row group.
/// This structure enables efficient retrieval of keyword occurrences from the Parquet
/// file by providing row group and row number information.
///
/// # Fields
///
/// - `column_name` - Name of the column
/// - `row_groups` - List of row groups containing the keyword, with row ranges
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::ColumnLocation;
/// # let col = ColumnLocation {
/// #     column_name: "email".to_string(),
/// #     row_groups: vec![],
/// # };
/// println!("Column '{}' contains the keyword", col.column_name);
/// println!("Found in {} row group(s)", col.row_groups.len());
/// ```
#[derive(Debug, Clone)]
pub struct ColumnLocation {
    /// Name of the column containing the keyword.
    ///
    /// This corresponds to a column name in the Parquet schema.
    pub column_name: String,

    /// Row groups within this column that contain the keyword.
    ///
    /// Each row group contains multiple row ranges where the keyword appears.
    /// Guaranteed to be non-empty when this struct exists.
    pub row_groups: Vec<RowGroupLocation>,
}

/// Location data for a keyword within a specific row group of a column.
///
/// Represents all occurrences of a keyword in a single row group, organized as
/// contiguous row ranges. Row groups are Parquet's unit of physical data organization,
/// typically containing thousands to millions of rows.
///
/// # Fields
///
/// - `row_group_id` - Row group identifier (0-indexed)
/// - `row_ranges` - Contiguous ranges of rows containing the keyword
///
/// # Row Group Context
///
/// In Parquet files:
/// - Each row group is independently compressed and stored
/// - Row groups typically contain 100K - 1M rows
/// - Row numbers within a row group are 0-indexed
/// - Reading data requires specifying the row group
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::RowGroupLocation;
/// # let rg = RowGroupLocation {
/// #     row_group_id: 2,
/// #     row_ranges: vec![],
/// # };
/// println!("Row group {} contains matches", rg.row_group_id);
/// println!("Total row ranges: {}", rg.row_ranges.len());
/// ```
#[derive(Debug, Clone)]
pub struct RowGroupLocation {
    /// Identifier for the row group (0-indexed).
    ///
    /// Used to target the correct row group when reading from the Parquet file.
    pub row_group_id: u16,

    /// Contiguous ranges of rows where the keyword appears.
    ///
    /// Consecutive rows with the keyword are grouped into single ranges for efficiency.
    /// Row numbers are relative to the start of this row group (0-indexed).
    /// Guaranteed to be non-empty when this struct exists.
    pub row_ranges: Vec<RowRange>,
}

/// A contiguous range of rows where a keyword appears.
///
/// Represents a sequence of consecutive rows containing the keyword. Using ranges
/// instead of individual row numbers dramatically reduces memory usage and improves
/// query performance for frequently-occurring keywords.
///
/// # Fields
///
/// - `start_row` - First row in the range (0-indexed within row group)
/// - `end_row` - Last row in the range, inclusive
/// - `splits_matched` - Which split levels matched in this range
/// - `parent_offset` - Optional parent keyword offset for phrase search
///
/// # Row Range Compression
///
/// When keywords appear in consecutive rows (e.g., rows 100, 101, 102, 103), they
/// are stored as a single range `{start_row: 100, end_row: 103}` rather than four
/// separate entries. This provides significant space savings for common keywords.
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::RowRange;
/// # let range = RowRange {
/// #     start_row: 100,
/// #     end_row: 103,
/// #     splits_matched: 0b0010,
/// #     parent_offset: None,
/// # };
/// // A range covering 4 rows
/// let num_rows = range.end_row - range.start_row + 1;
/// assert_eq!(num_rows, 4);
///
/// // Check if a specific row is in this range
/// let row = 102;
/// if row >= range.start_row && row <= range.end_row {
///     println!("Row {} is in this range", row);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct RowRange {
    /// Starting row number (0-indexed within the row group, inclusive).
    ///
    /// This is the first row in the range that contains the keyword.
    pub start_row: u32,

    /// Ending row number (0-indexed within the row group, inclusive).
    ///
    /// This is the last row in the range that contains the keyword.
    /// For a single-row range, `end_row == start_row`.
    pub end_row: u32,

    /// Bitmask indicating which split levels matched for this range.
    ///
    /// Same encoding as `KeywordLocationData::splits_matched`. All rows in this
    /// range have the same split pattern.
    pub splits_matched: u16,

    /// Offset to the parent keyword in the sorted keyword list.
    ///
    /// Used for phrase search optimization:
    /// - `None` - This is a root token from the original Parquet string
    /// - `Some(offset)` - Points to the parent keyword that contained this token
    ///
    /// This enables efficient phrase matching by checking parent containment
    /// without reading the Parquet file.
    pub parent_offset: Option<u32>,
}

/// Result of combining multiple keyword searches with set operations.
///
/// This structure represents the result of performing logical operations (AND, OR, NOT)
/// across multiple keyword searches. It identifies row groups and row ranges that satisfy
/// the combined criteria.
///
/// # Use Cases
///
/// - **AND queries**: Find rows containing all specified keywords
/// - **OR queries**: Find rows containing any of the specified keywords
/// - **NOT queries**: Find rows containing some keywords but not others
/// - **Complex boolean queries**: Combine multiple operations
///
/// # Fields
///
/// - `keywords` - The keywords that were combined in the query
/// - `row_groups` - Row groups and ranges that satisfy the combination
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::CombinedSearchResult;
/// # let result = CombinedSearchResult {
/// #     keywords: vec!["python".to_string(), "developer".to_string()],
/// #     row_groups: vec![],
/// # };
/// // AND query: rows containing both "python" AND "developer"
/// println!("Combined search for: {:?}", result.keywords);
/// println!("Found matches in {} row group(s)", result.row_groups.len());
/// ```
#[derive(Debug, Clone)]
pub struct CombinedSearchResult {
    /// Keywords that were combined in this search operation.
    ///
    /// The order and contents depend on the type of combination performed.
    /// For AND queries, all keywords must be present. For OR queries, at least
    /// one keyword must be present.
    pub keywords: Vec<String>,

    /// Row groups that satisfy the combination criteria.
    ///
    /// Each row group contains row ranges where the combination condition is met.
    /// Empty if no matches found.
    pub row_groups: Vec<CombinedRowGroupLocation>,
}

/// Row group location for combined search results.
///
/// Similar to `RowGroupLocation` but for combined queries. Contains row ranges
/// that satisfy the combination criteria (e.g., all keywords present for AND query).
///
/// # Fields
///
/// - `row_group_id` - Row group identifier
/// - `row_ranges` - Row ranges satisfying the combination
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::CombinedRowGroupLocation;
/// # let rg = CombinedRowGroupLocation {
/// #     row_group_id: 5,
/// #     row_ranges: vec![],
/// # };
/// println!("Row group {} has combined matches", rg.row_group_id);
/// ```
#[derive(Debug, Clone)]
pub struct CombinedRowGroupLocation {
    /// Identifier for the row group (0-indexed).
    pub row_group_id: u16,

    /// Row ranges in this row group that satisfy the combination criteria.
    ///
    /// Guaranteed to be non-empty when this struct exists.
    pub row_ranges: Vec<CombinedRowRange>,
}

/// Row range for combined search results.
///
/// Similar to `RowRange` but without split information since different keywords
/// in a combination may have different split patterns.
///
/// # Fields
///
/// - `start_row` - Starting row number (inclusive)
/// - `end_row` - Ending row number (inclusive)
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::CombinedRowRange;
/// # let range = CombinedRowRange {
/// #     start_row: 50,
/// #     end_row: 75,
/// # };
/// let count = range.end_row - range.start_row + 1;
/// println!("Range contains {} rows", count);
/// ```
#[derive(Debug, Clone)]
pub struct CombinedRowRange {
    /// Starting row number (0-indexed within row group, inclusive).
    pub start_row: u32,

    /// Ending row number (0-indexed within row group, inclusive).
    pub end_row: u32,
}

/// A potential phrase match that may or may not need verification.
///
/// Represents a location where all tokens of a phrase were found, but the match
/// status (confirmed, rejected, or needs verification) depends on parent keyword
/// analysis.
///
/// # Fields
///
/// - `column_name` - Column where match was found
/// - `row_group_id` - Row group containing the match
/// - `row` - Specific row number
/// - `split_level` - How deeply nested the tokens are
/// - `status` - Verification status
///
/// # Split Level Context
///
/// The split-level indicates token depth:
/// - `0`: Tokens appear in the original string (no splits)
/// - `1`: Tokens appear after first-level splits (spaces, newlines)
/// - `2+`: Tokens appear after deeper splits (@, /, ., etc.)
///
/// Lower split levels are more likely to be true phrase matches.
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::{PotentialMatch, MatchStatus};
/// # let match_entry = PotentialMatch {
/// #     column_name: "email".to_string(),
/// #     row_group_id: 3,
/// #     row: 1523,
/// #     split_level: 1,
/// #     status: MatchStatus::Confirmed {
/// #         parent_keyword: "user@example.com".to_string()
/// #     },
/// # };
/// match &match_entry.status {
///     MatchStatus::Confirmed { parent_keyword } => {
///         println!("Confirmed in parent: {}", parent_keyword);
///     }
///     MatchStatus::Rejected { reason, .. } => {
///         println!("Rejected: {}", reason);
///     }
///     MatchStatus::NeedsVerification { reason } => {
///         println!("Must verify: {}", reason);
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PotentialMatch {
    /// Name of the column where the potential match was found.
    pub column_name: String,

    /// Row group identifier (0-indexed).
    pub row_group_id: u16,

    /// Row number within the row group (0-indexed).
    pub row: u32,

    /// Split-level where the tokens match (0 = original string, higher = deeper splits).
    ///
    /// Lower split levels generally indicate higher confidence matches.
    pub split_level: u16,

    /// Verification status of this potential match.
    ///
    /// Determines whether the match is confirmed, rejected, or needs verification.
    pub status: MatchStatus,
}

/// Status of a phrase match verification.
///
/// Indicates whether a potential phrase match has been confirmed, rejected, or
/// still needs verification by reading the Parquet file.
///
/// # Variants
///
/// - **Confirmed**: Parent keyword contains the full phrase - guaranteed match
/// - **Rejected**: Parent keyword exists but doesn't contain phrase - guaranteed non-match
/// - **NeedsVerification**: Insufficient information - must read Parquet data
///
/// # Examples
///
/// ```
/// # use keywords::searching::search_results::MatchStatus;
/// let status = MatchStatus::Confirmed {
///     parent_keyword: "john.smith@example.com".to_string()
/// };
///
/// match status {
///     MatchStatus::Confirmed { parent_keyword } => {
///         println!("Confirmed match in: {}", parent_keyword);
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum MatchStatus {
    /// Confirmed match - parent keyword contains the full phrase.
    ///
    /// No Parquet read needed. The parent keyword itself contains the complete
    /// phrase, guaranteeing this is a true match.
    Confirmed {
        /// The parent keyword that contains the phrase.
        parent_keyword: String
    },

    /// Confirmed rejection - parent keyword exists but doesn't contain phrase.
    ///
    /// No Parquet read needed. We know the parent keyword, and it doesn't contain
    /// the phrase, so this cannot be a match.
    Rejected {
        /// The parent keyword that was checked.
        parent_keyword: String,
        /// Explanation of why the match was rejected.
        reason: String
    },

    /// Needs verification - must read Parquet file to confirm.
    ///
    /// Insufficient information to determine match status without reading the
    /// actual data. This occurs when:
    /// - Tokens have different parent keywords
    /// - No parent information is available
    /// - Parent relationships are ambiguous
    NeedsVerification {
        /// Explanation of why verification is needed.
        reason: String
    },
}

/// Information about the keyword index.
///
/// Contains metadata about the index itself, including version information, Parquet
/// file validation data, configuration settings, and index statistics.
///
/// # Fields
///
/// - `version` - Index format version
/// - `parquet_etag` - ETag of the source Parquet file
/// - `parquet_size` - Size of the source Parquet file
/// - `parquet_last_modified` - Last modification timestamp
/// - `error_rate` - Bloom filter false positive rate
/// - `num_columns` - Number of indexed columns
/// - `num_chunks` - Number of metadata chunks
///
/// # Use Cases
///
/// - **Version checking**: Ensure compatibility with index format
/// - **Validation**: Verify index matches current Parquet file
/// - **Statistics**: Understand index size and structure
/// - **Configuration**: Check bloom filter error rate settings
///
/// # Examples
///
/// ```no_run
/// # use keywords::searching::search_results::IndexInfo;
/// # let info = IndexInfo {
/// #     version: 1,
/// #     parquet_etag: "abc123".to_string(),
/// #     parquet_size: 1024000000,
/// #     parquet_last_modified: 1234567890,
/// #     error_rate: 0.01,
/// #     num_columns: 25,
/// #     num_chunks: 100,
/// # };
/// println!("Index version: {}", info.version);
/// println!("Parquet size: {} bytes", info.parquet_size);
/// println!("Bloom filter error rate: {}%", info.error_rate * 100.0);
/// println!("Indexed {} columns in {} chunks", info.num_columns, info.num_chunks);
/// ```
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index format version number.
    ///
    /// Used to ensure compatibility between index and reader versions.
    pub version: u32,

    /// ETag of the source Parquet file when the index was created.
    ///
    /// Used for validation to detect if the Parquet file has changed.
    /// May be "unknown" if ETag is unavailable.
    pub parquet_etag: String,

    /// Size of the source Parquet file in bytes.
    ///
    /// Used for validation to detect if the Parquet file has changed.
    pub parquet_size: u64,

    /// Unix timestamp of when the source Parquet file was last modified.
    ///
    /// Used for validation to detect if the Parquet file has changed.
    pub parquet_last_modified: u64,

    /// Bloom filter false positive error rate (0.0 to 1.0).
    ///
    /// Typical value is 0.01 (1%). Lower values reduce false positives but
    /// increase index size. Higher values reduce index size but increase
    /// false positives.
    pub error_rate: f64,

    /// Number of columns indexed in the Parquet file.
    ///
    /// Columns with non-string types or excluded columns are not counted.
    pub num_columns: usize,

    /// Number of metadata chunks in the index.
    ///
    /// The index is split into chunks for efficient partial loading.
    /// Typical chunk size is 1000 keywords.
    pub num_chunks: usize,
}