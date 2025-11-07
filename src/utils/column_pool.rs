use std::collections::HashMap;
use rkyv::{Archive, Serialize as RkyvSerialize, Deserialize as RkyvDeserialize};
use rkyv::with::Skip;

/// String interning pool for column names.
///
/// Stores each unique column name once and provides indexed access.
/// The first position (index 0) is reserved for the "All Columns" concept
/// and is represented by an empty String.
///
/// # Limitations
///
/// * Does not allow real columns to have blank names - panics if attempted
/// * Assumes all columns with the same name are the same column
///   - This is problematic for Parquet, which technically allows multiple columns with the same name
///   - Also a consideration for whether columns with the same name in different schemas are truly the same
/// * Current decision: treat all columns with the same name as identical
/// * Searching Parquet files with duplicate column names may produce unexpected results
///
/// # Future Considerations
///
/// Design decision: Duplicate column names currently resolve to first occurrence.
/// Parquet allows duplicate names but this creates ambiguity in search results.
/// Alternative approaches (error on duplicates, suffix numbering) would require
/// schema validation at index time. Current approach favors simplicity and matches
/// most Parquet readers' behavior. Revisit if real-world schemas commonly use duplicates.
#[derive(Debug, Default, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct ColumnPool {
    pub strings: Vec<String>,           // Storage for unique strings
    #[rkyv(with = Skip)]
    lookup: HashMap<String, u32>,   // String -> index mapping (rebuilt after deserialization)
}

impl ColumnPool {

    /// Creates a new empty `ColumnPool` with a reserved empty string at index 0.
    ///
    /// The empty string at index 0 serves as a special placeholder representing
    /// the "All Columns" aggregate concept. This reservation means that actual
    /// column names cannot be empty strings.
    ///
    /// # Returns
    ///
    /// Returns a new `ColumnPool` instance with the empty string pre-interned at index 0.
    ///
    /// # Examples
    ///
    /// ```
    /// # use keywords::utils::column_pool::ColumnPool;
    /// let pool = ColumnPool::new();
    /// assert_eq!(pool.get(0), Some(""));
    /// ```
    pub fn new() -> Self {
        let mut pool = Self {
            strings: Vec::new(),
            lookup: HashMap::new(),
        };
        // Pre-intern empty string at index 0 as placeholder for 'all_columns' aggregate
        pool.intern_inner("", true);
        pool
    }

    /// Interns a non-empty string and returns its index.
    ///
    /// If the string already exists in the pool, returns its existing index. Otherwise, adds the string
    /// to the pool and returns the new index.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The string to intern (must be non-empty)
    ///
    /// # Returns
    ///
    /// Returns the index (u32) where the string is stored in the pool.
    ///
    /// # Panics
    ///
    /// Panics if `column_name` is an empty string, since index 0 is reserved for
    /// the "All Columns" special case.
    ///
    /// # Examples
    ///
    /// ```
    /// # use keywords::utils::column_pool::ColumnPool;
    /// let mut pool = ColumnPool::new();
    /// let idx1 = pool.intern("column_name");
    /// let idx2 = pool.intern("column_name"); // Returns same index
    /// assert_eq!(idx1, idx2);
    /// assert_eq!(pool.get(idx1), Some("column_name"));
    /// ```
    pub fn intern(&mut self, column_name: &str) -> u32 {
        self.intern_inner(column_name, false)
    }

    /// Interns a string and returns its index.
    ///
    /// If the string already exists in the pool, returns the existing index.
    /// If it's a new string, adds it to the pool and returns the new index.
    /// This internal method allows control over whether blank strings are permitted.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The string to intern
    /// * `allow_blank` - Whether to allow empty strings (used only during `ColumnPool::new()`)
    ///
    /// # Returns
    ///
    /// Returns the index (u32) of the interned string.
    ///
    /// # Panics
    ///
    /// Panics if `column_name` is empty and `allow_blank` is false. Empty strings are only
    /// allowed during initialization to reserve index 0 for the "All Columns" concept.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use keywords::utils::column_pool::ColumnPool;
    /// let mut pool = ColumnPool::new();
    /// let idx = pool.intern_inner("hello", false);
    /// assert_eq!(pool.get(idx), Some("hello"));
    ///
    /// // This would panic:
    /// // pool.intern_inner("", false);
    /// ```
    fn intern_inner(&mut self, column_name: &str, allow_blank: bool) -> u32 {
        if !allow_blank && column_name.is_empty() {
            // Design decision: Panic on blank column names rather than returning Result.
            // Blank columns indicate schema corruption or API misuse, not user input errors.
            // Failing fast prevents subtle bugs from propagating through the index.
            // A Result-based approach would add error handling complexity throughout the codebase
            // without clear benefit, as blank column names should never occur in valid Parquet files.
            panic!("Cannot intern blank string as blank is a special case");
        }
        if let Some(&idx) = self.lookup.get(column_name) {
            idx
        } else {
            let idx = self.strings.len() as u32;
            self.strings.push(column_name.to_string());
            self.lookup.insert(column_name.to_string(), idx);
            idx
        }
    }

    /// Retrieves a string by its index.
    ///
    /// Returns the string associated with the given index, or `None` if
    /// the index is out of bounds.
    ///
    /// # Arguments
    ///
    /// * `idx` - The index of the string to retrieve
    ///
    /// # Returns
    ///
    /// Returns `Some(&str)` if the index is valid, or `None` if the index
    /// is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// # use keywords::utils::column_pool::ColumnPool;
    /// let mut pool = ColumnPool::new();
    /// let idx = pool.intern("my_column");
    /// assert_eq!(pool.get(idx), Some("my_column"));
    /// assert_eq!(pool.get(999), None);
    /// ```
    pub fn get(&self, idx: u32) -> Option<&str> {
        self.strings.get(idx as usize).map(|s| s.as_str())
    }

    /// Rebuilds the internal lookup HashMap after deserialization.
    ///
    /// The lookup map is marked with `#[rkyv(with = Skip)]` and is not
    /// serialized. This method must be called after deserializing a
    /// `ColumnPool` to reconstruct the string-to-index mapping.
    ///
    /// This method clears any existing lookup data and rebuilds it from
    /// the `strings` vector.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use keywords::utils::column_pool::ColumnPool;
    /// // After deserializing from rkyv:
    /// let mut pool: ColumnPool = /* deserialized */;
    /// pool.rebuild_lookup();
    ///
    /// // Now the pool is ready to use
    /// let idx = pool.intern("new_column");
    /// ```
    pub fn rebuild_lookup(&mut self) {
        self.lookup.clear();
        for (idx, s) in self.strings.iter().enumerate() {
            self.lookup.insert(s.clone(), idx as u32);
        }
    }
}