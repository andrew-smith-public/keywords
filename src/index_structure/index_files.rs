//! Index file management utilities.
//!
//! This module provides utilities for managing the files that comprise a distributed
//! index. Each index consists of specialized files stored in an `.index` directory
//! alongside the source parquet file.
//!
//! # File Structure
//!
//! For a parquet file at `data/records.parquet`, the index files are stored in:
//! ```text
//! data/records.parquet.index/
//! ├── filters.rkyv          # Bloom filters, metadata, and chunk index
//! └── data.bin              # Keyword data (keywords + occurrence data per chunk)
//! ```
//!
//! # File Prefixes
//!
//! Index files can have optional prefixes for versioning or testing:
//! ```text
//! data/records.parquet.index/
//! ├── v1_filters.rkyv
//! ├── v1_data.bin
//! ├── test_filters.rkyv
//! └── test_data.bin
//! ```

/// Types of files in the distributed index.
///
/// Each variant represents a specific file type with a distinct purpose in the index structure.
/// All files use efficient binary serialization formats (rkyv or raw binary) for fast I/O.
///
/// # File Purposes
///
/// - **Filters**: Bloom filters, metadata, column pool, and chunk index for navigation
/// - **Data**: Chunked keyword lists and occurrence data in binary format
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
    /// Bloom filters, metadata, and chunk index (filters.rkyv).
    ///
    /// Contains serialized Bloom filters used for probabilistic keyword lookups,
    /// Parquet metadata for validation, column pool for string interning,
    /// and the chunk index that maps keyword ranges to their locations in data.bin.
    Filters,

    /// Chunked keyword data (data.bin).
    ///
    /// Contains chunks of 1000 keywords each. Each chunk has two sections:
    /// 1. Keyword list (Vec<String>) - searchable keyword strings
    /// 2. Data section (Vec<KeywordDataFlat>) - occurrence information
    ///
    /// This structure allows reading just keyword strings for parent lookups
    /// or reading the full chunk for search operations.
    Data,
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
            IndexFile::Data => "data.bin",
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
/// let name = index_filename(IndexFile::Data, Some("test_"));
/// assert_eq!(name, "test_data.bin");
/// ```
pub(crate) fn index_filename(file_type: IndexFile, prefix: Option<&str>) -> String {
    match prefix {
        Some(p) => format!("{}{}", p, file_type.base_name()),
        None => file_type.base_name().to_string(),
    }
}