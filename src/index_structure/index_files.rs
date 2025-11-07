//! Index file management utilities.
//!
//! This module provides utilities for managing the various files that comprise a distributed
//! index. Each index consists of multiple specialized files stored in an `.index` directory
//! alongside the source parquet file.
//!
//! # File Structure
//!
//! For a parquet file at `data/records.parquet`, the index files are stored in:
//! ```text
//! data/records.parquet.index/
//! ├── filters.rkyv          # Bloom filters and metadata
//! ├── metadata.rkyv         # Metadata entries
//! ├── data.bin              # Keyword data
//! └── column_keywords.rkyv  # Column keywords mapping
//! ```
//!
//! # File Prefixes
//!
//! Index files can have optional prefixes for versioning or testing:
//! ```text
//! data/records.parquet.index/
//! ├── v1_filters.rkyv
//! ├── v1_metadata.rkyv
//! ├── test_filters.rkyv
//! └── test_metadata.rkyv
//! ```

/// Types of files in the distributed index.
///
/// Each variant represents a specific file type with a distinct purpose in the index structure.
/// All files use efficient binary serialization formats (rkyv or raw binary) for fast I/O.
///
/// # File Purposes
///
/// - **Filters**: Bloom filters for quick keyword existence checks
/// - **Metadata**: Index metadata and configuration
/// - **Data**: Raw keyword data in binary format
/// - **ColumnKeywords**: Mapping of columns to their associated keywords
///
/// # Examples
///
/// ```ignore
/// use index_files::IndexFile;
///
/// // Iterate over all index file types
/// for file_type in IndexFile::all() {
///     println!("File: {}", index_filename(*file_type, None));
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub enum IndexFile {
    /// Bloom filters and metadata (filters.rkyv).
    ///
    /// Contains serialized Bloom filters used for probabilistic keyword lookups,
    /// along with associated metadata like filter parameters and statistics.
    Filters,

    /// Metadata entries (metadata.rkyv).
    ///
    /// Contains index configuration, version information, column schemas,
    /// and other metadata required for index operations.
    Metadata,

    /// Keyword data (data.bin).
    ///
    /// Contains the raw binary data for keywords, stored in a compact format
    /// for efficient retrieval during query operations.
    Data,

    /// Column keywords mapping (column_keywords.rkyv).
    ///
    /// Contains the mapping between column names and their associated keywords,
    /// enabling column-specific keyword lookups.
    ColumnKeywords,
}

impl IndexFile {
    /// Returns the base filename for this index file type.
    ///
    /// The base filename does not include any prefix or directory path,
    /// just the standard filename with extension.
    ///
    /// # Returns
    ///
    /// A static string slice containing the base filename for this file type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(IndexFile::Filters.base_name(), "filters.rkyv");
    /// assert_eq!(IndexFile::Data.base_name(), "data.bin");
    /// ```
    fn base_name(&self) -> &'static str {
        match self {
            IndexFile::Filters => "filters.rkyv",
            IndexFile::Metadata => "metadata.rkyv",
            IndexFile::Data => "data.bin",
            IndexFile::ColumnKeywords => "column_keywords.rkyv",
        }
    }
}

/// Creates an index filename with an optional prefix.
///
/// Constructs the full filename for an index file by combining an optional prefix
/// with the base filename. This is useful for creating versioned or test indexes.
///
/// # Arguments
///
/// * `file_type` - The type of index file to generate a name for
/// * `prefix` - Optional prefix to prepend (e.g., `"v1_"`, `"test_"`, `"backup_"`)
///
/// # Returns
///
/// A `String` containing the complete filename with prefix (if provided).
///
/// # Examples
///
/// ```ignore
/// // Generate standard filename
/// let name = index_filename(IndexFile::Filters, None);
/// assert_eq!(name, "filters.rkyv");
///
/// // Generate filename with version prefix
/// let name = index_filename(IndexFile::Filters, Some("v2_"));
/// assert_eq!(name, "v2_filters.rkyv");
///
/// // Generate filename with test prefix
/// let name = index_filename(IndexFile::Metadata, Some("test_"));
/// assert_eq!(name, "test_metadata.rkyv");
/// ```
pub(crate) fn index_filename(file_type: IndexFile, prefix: Option<&str>) -> String {
    match prefix {
        Some(p) => format!("{}{}", p, file_type.base_name()),
        None => file_type.base_name().to_string(),
    }
}