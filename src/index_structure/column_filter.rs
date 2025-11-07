//! Column filter implementation for efficient keyword lookups.
//!
//! This module provides a space-efficient filter for checking keyword membership in columns.
//! It automatically selects between a Bloom filter (for large sets) and a serialized HashSet
//! (for small sets) based on the number of keywords.

use std::rc::Rc;
use indexmap::IndexSet;
use rkyv::{Archive, Serialize as RkyvSerialize, Deserialize as RkyvDeserialize, Archived};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;


/// Minimum number of keywords before considering Bloom filter.
///
/// Below this threshold, always use [`ColumnFilter::RkyvHashSet`] for exact matches.
/// At or above this threshold, use [`ColumnFilter::BloomFilter`] for space efficiency.
pub(crate) const MIN_KEYWORDS_FOR_BLOOM: usize = 100;

/// Represents either a Bloom filter or a serialized HashSet for column keyword lookup.
///
/// This enum automatically selects the most appropriate data structure based on the number
/// of keywords. For small sets (< [`MIN_KEYWORDS_FOR_BLOOM`]), it uses a serialized HashSet
/// for exact matching. For larger sets, it uses a space-efficient Bloom filter that trades
/// memory for potential false positives.
///
/// # Trade-offs
///
/// - **HashSet**: Exact matching, no false positives, but higher memory usage
/// - **Bloom filter**: Space-efficient, but may have false positives (no false negatives)
///
/// # Examples
///
/// ```ignore
/// use indexmap::IndexSet;
/// use std::rc::Rc;
///
/// let mut keywords = IndexSet::new();
/// keywords.insert(Rc::from("rust"));
/// keywords.insert(Rc::from("programming"));
///
/// // Creates appropriate filter based on size
/// let filter = ColumnFilter::create_column_filter(&keywords, 0.01);
///
/// assert!(filter.might_contain("rust"));
/// assert!(!filter.might_contain("python"));
/// ```
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum ColumnFilter {
    /// Bloom filter with the filter bytes and parameters.
    ///
    /// Uses multiple hash functions to achieve a configurable false positive rate.
    /// May return `true` for keywords that were not inserted (false positives), but
    /// will never return `false` for keywords that were inserted (no false negatives).
    BloomFilter {
        /// The bit array storing the Bloom filter data
        data: Vec<u8>,
        /// Number of hash functions to use
        num_hashes: u32,
        /// Total number of bits in the filter
        num_bits: u64,
    },
    /// Serialized rkyv HashSet for small keyword sets.
    ///
    /// Provides exact matching with zero false positives, stored as serialized bytes.
    /// Used when the number of keywords is below [`MIN_KEYWORDS_FOR_BLOOM`].
    RkyvHashSet(Vec<u8>),
}

impl ColumnFilter {
    /// Checks if a keyword might be in the filter.
    ///
    /// # Behavior by variant
    ///
    /// - **Bloom filter**: May have false positives (returns `true` for some keywords not in the set)
    ///   but never has false negatives (always returns `true` for keywords that are in the set)
    /// - **HashSet**: Exact match, no false positives or false negatives
    ///
    /// # Arguments
    ///
    /// * `keyword` - The keyword string to check for membership
    ///
    /// # Returns
    ///
    /// Returns `true` if the keyword might be in the set, `false` if it is definitely not.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if filter.might_contain("rust") {
    ///     // Keyword might be present (definitely present for HashSet)
    ///     // Need to verify with actual data for Bloom filter
    /// }
    /// ```
    pub fn might_contain(&self, keyword: &str) -> bool {
        match self {
            ColumnFilter::BloomFilter { data, num_hashes, num_bits } => {
                ColumnFilter::bloom_filter_check(keyword, data, *num_hashes, *num_bits)
            }
            ColumnFilter::RkyvHashSet(bytes) => {
                ColumnFilter::rkyv_hashset_contains(bytes, keyword)
            }
        }
    }

    /// Checks if a keyword might be in a Bloom filter.
    ///
    /// This method is public to the crate to allow access by integration tests.
    /// It checks all hash positions for the keyword - if any bit is not set,
    /// the keyword is definitely not in the filter.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The keyword string to check
    /// * `data` - The Bloom filter bit array
    /// * `num_hashes` - Number of hash functions to use
    /// * `num_bits` - Total number of bits in the filter
    ///
    /// # Returns
    ///
    /// Returns `true` if all hash positions are set (keyword might be present),
    /// `false` if any hash position is not set (keyword is definitely not present).
    ///
    /// # False Positives
    ///
    /// This method may return `true` for keywords that were never inserted due to
    /// hash collisions. The false positive rate depends on the filter size and
    /// number of items inserted.
    pub(crate) fn bloom_filter_check(keyword: &str, data: &[u8], num_hashes: u32, num_bits: u64) -> bool {
        for i in 0..num_hashes {
            let hash = ColumnFilter::hash_keyword(keyword, i);
            let bit_pos = (hash % num_bits) as usize;
            if (data[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
        }
        true
    }

    /// Checks if a keyword is in a rkyv-serialized HashSet.
    ///
    /// This method is public to the crate to allow access by integration tests.
    /// It deserializes the HashSet and performs an exact membership check.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The serialized HashSet bytes
    /// * `keyword` - The keyword string to check
    ///
    /// # Returns
    ///
    /// Returns `true` if the keyword is in the set, `false` otherwise.
    ///
    /// # Panics
    ///
    /// Panics if the bytes cannot be deserialized, which indicates data corruption.
    ///
    /// # Performance
    ///
    /// This method copies the bytes to an aligned buffer and deserializes on each call.
    /// For repeated lookups, consider caching the deserialized structure if performance
    /// is critical.
    pub(crate) fn rkyv_hashset_contains(bytes: &[u8], keyword: &str) -> bool {
        // Copy to aligned buffer to ensure proper alignment
        let mut aligned_bytes = AlignedVec::<16>::new();
        aligned_bytes.extend_from_slice(bytes);

        // Deserialize with validation - panic on invalid data
        let archived: &Archived<Vec<String>> = rkyv::access::<_, RkyvError>(&aligned_bytes)
            .expect("Failed to deserialize rkyv HashSet - data corruption detected");

        archived.iter().any(|k| k.as_str() == keyword)
    }

    /// Hashes a keyword with a seed for Bloom filter operations.
    ///
    /// Uses Rust's [`DefaultHasher`](std::collections::hash_map::DefaultHasher) with
    /// a seed value to generate multiple independent hash functions from a single
    /// hash algorithm.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The keyword string to hash
    /// * `seed` - Seed value to generate different hash functions (typically 0 to num_hashes-1)
    ///
    /// # Returns
    ///
    /// A 64-bit hash value derived from the keyword and seed.
    ///
    /// # Implementation Note
    ///
    /// The seed is hashed first, followed by the keyword, to ensure different seeds
    /// produce different hash values for the same keyword.
    fn hash_keyword(keyword: &str, seed: u32) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        keyword.hash(&mut hasher);
        hasher.finish()
    }

    /// Inserts a keyword into a Bloom filter.
    ///
    /// Sets all hash positions for the keyword to 1 in the bit array. This operation
    /// is used during filter construction and cannot be performed on an existing
    /// [`ColumnFilter`] instance.
    ///
    /// # Arguments
    ///
    /// * `keyword` - The keyword string to insert
    /// * `data` - The mutable Bloom filter bit array
    /// * `num_hashes` - Number of hash functions to use
    /// * `num_bits` - Total number of bits in the filter
    ///
    /// # Note
    ///
    /// Once bits are set, they cannot be unset. Bloom filters do not support deletion.
    fn bloom_filter_insert(keyword: &str, data: &mut [u8], num_hashes: u32, num_bits: u64) {
        for i in 0..num_hashes {
            let hash = ColumnFilter::hash_keyword(keyword, i);
            let bit_pos = (hash % num_bits) as usize;
            data[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }

    /// Calculates optimal Bloom filter parameters for given constraints.
    ///
    /// Uses standard Bloom filter formulas to determine the optimal number of bits
    /// and hash functions needed to achieve a target false positive rate.
    ///
    /// # Arguments
    ///
    /// * `num_items` - Expected number of items to be inserted
    /// * `error_rate` - Target false positive rate (e.g., 0.01 for 1%)
    ///
    /// # Returns
    ///
    /// A tuple of `(num_bits, num_hashes)`:
    /// - `num_bits`: Optimal number of bits in the filter
    /// - `num_hashes`: Optimal number of hash functions (minimum 1)
    ///
    /// # Formulas
    ///
    /// - `m = -n * ln(p) / (ln(2)^2)` where m is num_bits, n is num_items, p is error_rate
    /// - `k = (m/n) * ln(2)` where k is num_hashes
    ///
    /// # Examples
    ///
    /// For 1000 items with 1% error rate:
    /// - Returns approximately (9585 bits, 7 hashes)
    fn calculate_bloom_params(num_items: usize, error_rate: f64) -> (u64, u32) {
        // m = -n * ln(p) / (ln(2)^2)
        let num_bits = (-(num_items as f64) * error_rate.ln() / (2_f64.ln().powi(2))).ceil() as u64;
        // k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / num_items as f64) * 2_f64.ln()).ceil() as u32;
        (num_bits, num_hashes.max(1))
    }

    /// Creates a Bloom filter from a set of keywords.
    ///
    /// Constructs a space-efficient Bloom filter with parameters optimized for
    /// the given error rate and number of keywords.
    ///
    /// # Arguments
    ///
    /// * `keywords` - Set of keywords to insert into the filter
    /// * `error_rate` - Target false positive rate (e.g., 0.01 for 1%)
    ///
    /// # Returns
    ///
    /// A [`ColumnFilter::BloomFilter`] variant containing the filter data and parameters.
    ///
    /// # Performance
    ///
    /// Time complexity: O(n * k) where n is the number of keywords and k is the number
    /// of hash functions. Space complexity: O(m) where m is the number of bits.
    fn create_bloom_filter(keywords: &IndexSet<Rc<str>>, error_rate: f64) -> ColumnFilter {
        let num_items = keywords.len();
        let (num_bits, num_hashes) = ColumnFilter::calculate_bloom_params(num_items, error_rate);

        let num_bytes = ((num_bits + 7) / 8) as usize;
        let mut data = vec![0u8; num_bytes];

        for keyword in keywords {
            ColumnFilter::bloom_filter_insert(keyword, &mut data, num_hashes, num_bits);
        }

        ColumnFilter::BloomFilter {
            data,
            num_hashes,
            num_bits,
        }
    }

    /// Creates a rkyv-serialized HashSet from a set of keywords.
    ///
    /// Constructs an exact-match filter by serializing the keywords as a vector
    /// using the rkyv zero-copy serialization library.
    ///
    /// # Arguments
    ///
    /// * `keywords` - Set of keywords to serialize
    ///
    /// # Returns
    ///
    /// A [`ColumnFilter::RkyvHashSet`] variant containing the serialized data.
    ///
    /// # Panics
    ///
    /// Panics if serialization fails, which should not occur under normal circumstances.
    ///
    /// # Note
    ///
    /// The keywords are converted from `Rc<str>` to `String` for serialization compatibility.
    fn create_rkyv_hashset(keywords: &IndexSet<Rc<str>>) -> ColumnFilter {
        // Convert IndexSet<Box<str>> to Vec<String> for serialization
        let keywords_vec: Vec<String> = keywords.iter().map(|s| s.to_string()).collect();

        // Serialize with rkyv
        let bytes = rkyv::to_bytes::<RkyvError>(&keywords_vec)
            .expect("Failed to serialize keywords");

        ColumnFilter::RkyvHashSet(bytes.to_vec())
    }

    /// Creates an appropriate filter for a column's keywords.
    ///
    /// Automatically selects between a HashSet (for small sets) and a Bloom filter
    /// (for large sets) based on [`MIN_KEYWORDS_FOR_BLOOM`] threshold.
    ///
    /// # Arguments
    ///
    /// * `keywords` - Set of keywords to create a filter for
    /// * `error_rate` - Target false positive rate for Bloom filter (e.g., 0.01 for 1%)
    ///
    /// # Returns
    ///
    /// - [`ColumnFilter::RkyvHashSet`] if keywords.len() < [`MIN_KEYWORDS_FOR_BLOOM`]
    /// - [`ColumnFilter::BloomFilter`] if keywords.len() >= [`MIN_KEYWORDS_FOR_BLOOM`]
    ///
    /// # Selection Rationale
    ///
    /// - **Small sets**: Use HashSet for exact matching without false positives
    /// - **Large sets**: Use Bloom filter for space efficiency, accepting small false positive rate
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use indexmap::IndexSet;
    /// use std::rc::Rc;
    ///
    /// let mut keywords = IndexSet::new();
    /// // Add keywords...
    ///
    /// // Create filter with 1% false positive rate
    /// let filter = ColumnFilter::create_column_filter(&keywords, 0.01);
    /// ```
    pub(crate) fn create_column_filter(keywords: &IndexSet<Rc<str>>, error_rate: f64) -> ColumnFilter {
        let num_keywords = keywords.len();

        // For small sets (< MIN_KEYWORDS_FOR_BLOOM), always use HashSet
        if num_keywords < MIN_KEYWORDS_FOR_BLOOM {
            return ColumnFilter::create_rkyv_hashset(keywords);
        }

        // For larger sets, always use Bloom filter
        ColumnFilter::create_bloom_filter(keywords, error_rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_structure::column_filter::ColumnFilter;


    #[test]
    fn test_bloom_filter_small_set() {
        // For small sets (< 25), should use HashSet
        let mut keywords = IndexSet::new();
        for i in 0..20 {
            keywords.insert(Rc::from(format!("keyword{}", i)));
        }

        let filter = ColumnFilter::create_column_filter(&keywords, 0.01);

        match filter {
            ColumnFilter::RkyvHashSet(_) => {
                // Correct - should be HashSet for < 25 items
            }
            ColumnFilter::BloomFilter { .. } => {
                panic!("Should use HashSet for < MIN_KEYWORDS_FOR_BLOOM keywords");
            }
        }

        // Verify all keywords are found
        for keyword in &keywords {
            assert!(filter.might_contain(keyword), "Should contain {}", keyword);
        }

        // Verify non-existent keywords return false
        assert!(!filter.might_contain("nonexistent"));
    }

    #[test]
    fn test_bloom_filter_large_set() {
        // For larger sets, might use Bloom filter
        let mut keywords = IndexSet::new();
        for i in 0..1000 {
            keywords.insert(Rc::from(format!("keyword{}", i)));
        }

        let filter = ColumnFilter::create_column_filter(&keywords, 0.01);

        // Verify all keywords are found (no false negatives)
        for keyword in &keywords {
            assert!(filter.might_contain(keyword), "Should contain {}", keyword);
        }

        // Check that non-existent keywords mostly return false
        // (allowing for bloom filter false positives)
        let mut false_positives = 0;
        let test_count = 1000;
        for i in 1000..1000 + test_count {
            if filter.might_contain(&format!("nonexistent{}", i)) {
                false_positives += 1;
            }
        }

        // False positive rate should be close to 1% (allow 0-5%)
        let fp_rate = false_positives as f64 / test_count as f64;
        assert!(fp_rate <= 0.05, "False positive rate {} too high", fp_rate);
    }

    #[test]
    fn test_bloom_filter_params() {
        // Test parameter calculation
        let (num_bits, num_hashes) = ColumnFilter::calculate_bloom_params(1000, 0.01);

        // For 1000 items with 1% error rate:
        // m ≈ 9585 bits
        // k ≈ 7 hashes
        assert!(num_bits > 9000 && num_bits < 10000, "num_bits = {}", num_bits);
        assert!(num_hashes >= 6 && num_hashes <= 8, "num_hashes = {}", num_hashes);
    }

    #[test]
    fn test_column_filter_switch_threshold() {
        // Test that we use HashSet for < MIN_KEYWORDS_FOR_BLOOM and Bloom for >= MIN_KEYWORDS_FOR_BLOOM

        // Small set: should be HashSet
        let mut small_keywords = IndexSet::new();
        for i in 0..10 {
            small_keywords.insert(Rc::from(format!("k{}", i)));
        }
        let small_filter = ColumnFilter::create_column_filter(&small_keywords, 0.01);
        assert!(matches!(small_filter, ColumnFilter::RkyvHashSet(_)),
                "Should use HashSet for {} keywords (< {})", small_keywords.len(), MIN_KEYWORDS_FOR_BLOOM);

        // Large set: should be Bloom filter
        let mut large_keywords = IndexSet::new();
        for i in 0..150 {
            large_keywords.insert(Rc::from(format!("keyword{}", i)));
        }
        let large_filter = ColumnFilter::create_column_filter(&large_keywords, 0.01);
        assert!(matches!(large_filter, ColumnFilter::BloomFilter { .. }),
                "Should use Bloom filter for {} keywords (>= {})", large_keywords.len(), MIN_KEYWORDS_FOR_BLOOM);

        // Just verify it works
        assert!(large_filter.might_contain("keyword50"));
        assert!(small_filter.might_contain("k5"));
    }
}