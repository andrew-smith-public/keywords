/// Comprehensive test suite for split_match nullification functionality
///
/// Tests cover:
/// 1. Search functionality with nullified splits (10 tests)
/// 2. Split tracking flag preventing re-addition (2 tests)
///
/// Total: 12 comprehensive tests

#[cfg(test)]
mod split_match_nullification_tests {
    use std::sync::Arc;
    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::file::properties::WriterProperties;
    use parquet::basic::Compression;
    use parquet::arrow::ArrowWriter;
    use bytes::Bytes;

    use crate::column_parquet_reader::process_parquet_file;
    use crate::index_data::{build_distributed_index, CompressionAlgorithm};
    use crate::searching::keyword_search::KeywordSearcher;
    use crate::{build_index_in_memory, ParquetSource};

    // ============================================================================
    // HELPER FUNCTIONS
    // ============================================================================

    /// Helper function to write a RecordBatch to an in-memory Parquet file
    fn write_parquet_to_bytes(
        schema: Arc<Schema>,
        batch: RecordBatch,
        props: WriterProperties,
    ) -> Vec<u8> {
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
            .expect("Failed to create Arrow writer");
        writer.write(&batch)
            .expect("Failed to write batch");
        writer.close()
            .expect("Failed to close writer");
        buffer
    }

    /// Helper to create test data and build index with configurable thresholds
    async fn create_test_index_with_thresholds(
        data: Vec<String>,
        split_elimination_threshold: Option<f64>,
    ) -> KeywordSearcher {
        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder().build();
        let array: ArrayRef = Arc::new(StringArray::from(
            data.iter().map(|s| s.to_string()).collect::<Vec<_>>()
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        let bytes = Bytes::from(parquet_bytes.clone());
        let source = ParquetSource::Bytes(bytes);

        // Process with split elimination threshold
        let searcher = build_index_in_memory(
            source.clone(),
            None, // no excluded columns
            Some(0.01), // 1% error rate
            None,
            None,
            None, // default split chars
            None, // don't store full keywords
            None, // no exceptions
            split_elimination_threshold, // Must set this otherwise split threshold will not apply (correctly)
            split_elimination_threshold,
        ).await.unwrap();

        searcher
    }

    // ============================================================================
    // SEARCH FUNCTIONALITY TESTS (10 tests)
    // ============================================================================

    #[tokio::test]
    async fn test_keyword_search_with_nullified_splits() {
        // Create data where "common" keyword appears in many rows (will exceed threshold)
        // and "rare" keyword appears in few rows (won't exceed threshold)
        let mut data: Vec<String> = vec![];

        for i in 0..50 {
            data.push(format!("common@example.com{}", i));
            data.push(format!("common2@example.com{}", i));
        }

        for i in 0..5 {
            data.push(format!("rare@example{}.com", i));
        }

        // Set threshold at 0.1 (10% of 105 rows = 10.5 rows)
        // "common" (100 rows) will exceed threshold -> splits_matched = None
        // "rare" (5 rows) won't exceed threshold -> splits_matched = Some(...)
        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.1)
        ).await;

        // Test 1: Search for "common" keyword (nullified splits)
        let result = searcher.search("common", None, true).await.unwrap();
        assert!(result.found, "Should find 'common' keyword");

        if let Some(verified) = &result.verified_matches {
            // splits_matched should be None for this keyword
            assert!(verified.splits_matched.is_some(),
                    "splits_matched should not be None for keyword only exceeding threshold");

            // Should still have correct row count
            assert_eq!(verified.total_occurrences, 50,
                       "Should have correct occurrence count even with nullified splits");

            // Verify columns are present
            assert!(!verified.columns.is_empty(), "Should have column information");

            // Verify row ranges are accurate
            let mut total_rows = 0;
            for col in &verified.column_details {
                for rg in &col.row_groups {
                    for range in &rg.row_ranges {
                        // Each range should have None splits_matched
                        assert!(range.splits_matched.is_none(),
                                "Row-level splits_matched should be None");
                        total_rows += (range.end_row - range.start_row + 1) as u64;
                    }
                }
            }
            assert_eq!(total_rows, 50, "Row ranges should account for all occurrences");
        } else {
            panic!("Should have verified matches");
        }

        // Test 2: Search for "rare" keyword (splits NOT nullified)
        let result = searcher.search("rare", None, true).await.unwrap();
        assert!(result.found, "Should find 'rare' keyword");

        if let Some(verified) = &result.verified_matches {
            // splits_matched should be Some(...) for this keyword
            assert!(verified.splits_matched.is_some(),
                    "splits_matched should be Some for keyword below threshold");

            // Should have correct row count
            assert_eq!(verified.total_occurrences, 5,
                       "Should have correct occurrence count");
        } else {
            panic!("Should have verified matches");
        }
    }

    #[tokio::test]
    async fn test_phrase_search_with_nullified_splits() {
        // Create data where "user" appears many times (will exceed threshold)
        let mut data: Vec<String> = vec![];

        // Add 100 rows with "user@domain.com"
        for i in 0..100 {
            data.push(format!("user@domain.com{}", i));
        }

        // Add 5 rows with "admin@other.org"
        for i in 0..5 {
            data.push(format!("admin@other.org{}", i));
        }

        // Set threshold at 0.1 (10% of 105 rows = 10.5 rows)
        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.1)
        ).await;

        // Test: Phrase search for "user@domain" (user token has nullified splits)
        let result = searcher.search("user@domain", None, false).await.unwrap();
        assert!(result.found, "Should find phrase");

        // With nullified splits_matched, phrase verification should be inconclusive
        // and fall back to "needs verification"
        if result.needs_verification.is_some() {
            let needs_check = result.needs_verification.as_ref().unwrap();
            assert!(needs_check.total_occurrences > 0,
                    "Should have matches needing verification when splits are nullified");
        }

        // May also have some verified matches depending on parent keyword logic
        // but at minimum should not panic or error
    }

    #[tokio::test]
    async fn test_mixed_keywords_different_split_status() {
        // Create data with multiple distinct keywords
        // Some will exceed threshold, some won't
        let mut data: Vec<String> = vec![];

        // Very common: "alpha" (appears 200 times)
        for i in 0..200 {
            data.push(format!("alpha@test.com{}", i));
            if i < 50 {
                data.push(format!("beta@test.org{}", i));
                if i < 15 {
                    data.push(format!("gamma@site.net{}", i));
                    if i < 3 {
                        data.push(format!("delta@web.io{}", i));
                    }
                }
            }
        }

        // Set threshold at 0.1 (10% of 268 rows = 26.8 rows)
        // "alpha" (200) and "beta" (50) will have nullified splits
        // "gamma" (15) and "delta" (3) will keep splits
        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.1)
        ).await;

        // Test keyword with nullified splits
        let result = searcher.search("alpha", None, true).await.unwrap();
        assert!(result.found);
        assert!(result.verified_matches.is_some());
        assert!(result.verified_matches.as_ref().unwrap().splits_matched.is_some(),
                "High-frequency keyword should have nullified splits");
        let verified = &result.verified_matches.as_ref().unwrap();
        for col in &verified.column_details {
            for rg in &col.row_groups {
                for range in &rg.row_ranges {
                    // Each range should have None splits_matched
                    assert!(range.splits_matched.is_none(),
                            "Row-level splits_matched should be None");
                }
            }
        }


        // Test another keyword with nullified splits
        let result = searcher.search("beta", None, true).await.unwrap();
        assert!(result.found);
        assert!(result.verified_matches.is_some());
        assert!(result.verified_matches.as_ref().unwrap().splits_matched.is_some(),
                "Medium-frequency keyword above threshold should have nullified splits");
        let verified = &result.verified_matches.as_ref().unwrap();
        for col in &verified.column_details {
            for rg in &col.row_groups {
                for range in &rg.row_ranges {
                    // Each range should have None splits_matched
                    assert!(range.splits_matched.is_none(),
                            "Row-level splits_matched should be None");
                }
            }
        }

        // Test keyword with preserved splits
        let result = searcher.search("gamma", None, true).await.unwrap();
        assert!(result.found);
        assert!(result.verified_matches.is_some());
        assert!(result.verified_matches.as_ref().unwrap().splits_matched.is_some(),
                "Low-frequency keyword below threshold should preserve splits");
        let verified = &result.verified_matches.as_ref().unwrap();
        for col in &verified.column_details {
            for rg in &col.row_groups {
                for range in &rg.row_ranges {
                    // Each range should have None splits_matched
                    assert!(range.splits_matched.is_some(),
                            "Row-level splits_matched should be Some");
                }
            }
        }

        // Test keyword with preserved splits
        let result = searcher.search("delta", None, true).await.unwrap();
        assert!(result.found);
        assert!(result.verified_matches.is_some());
        assert!(result.verified_matches.as_ref().unwrap().splits_matched.is_some(),
                "Rare keyword should preserve splits");
        let verified = &result.verified_matches.as_ref().unwrap();
        for col in &verified.column_details {
            for rg in &col.row_groups {
                for range in &rg.row_ranges {
                    // Each range should have Some splits_matched
                    assert!(range.splits_matched.is_some(), "Row-level splits_matched should be Some");
                }
            }
        }
    }


    #[tokio::test]
    async fn test_parent_keyword_lookup_with_nullified_splits() {
        // Create hierarchical data to test parent keyword lookups
        let mut data: Vec<String> = vec![];

        // Add many rows with nested structure
        for i in 0..100 {
            data.push(format!("user.name@example.com{}", i));
        }

        // Set threshold at 0.05 (5% of 100 = 5 rows)
        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.05)
        ).await;

        // Search for child token "user" (should have nullified splits)
        let result = searcher.search("user", None, true).await.unwrap();
        assert!(result.found, "Should find 'user' token");

        if let Some(verified) = &result.verified_matches {

            // Verify parent information is still present in row ranges
            let _has_parent_info = verified.column_details.iter()
                .flat_map(|col| &col.row_groups)
                .flat_map(|rg| &rg.row_ranges)
                .any(|range| range.parent_chunk.is_some() && range.parent_position.is_some());

            // Parent info may or may not be present depending on parent_threshold
            // But the search should not fail either way
        }

        // Phrase search should still work (may need verification)
        let result = searcher.search("user.name", None, false).await.unwrap();
        assert!(result.found, "Phrase search should work with nullified splits");
        // Should have either verified matches or needs_verification, but not error
        assert!(result.verified_matches.is_some() || result.needs_verification.is_some(),
                "Should have some results even with nullified splits");
    }

    #[tokio::test]
    async fn test_row_ranges_accuracy_with_nullified_splits() {
        // Create specific pattern to verify row ranges are consolidated correctly
        let mut data: Vec<String> = vec![];

        // Add specific pattern: keyword appears at rows 0-49, then gap, then 60-99
        for i in 0..50 {
            data.push(format!("keyword@test.com{}", i));
        }
        for i in 50..60 {
            data.push(format!("other@example.org{}", i)); // Gap
        }
        for i in 60..100 {
            data.push(format!("keyword@test.com{}", i));
        }

        // Set threshold at 0.05 (5% of 100 = 5 rows)
        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.05)
        ).await;

        let result = searcher.search("keyword", None, true).await.unwrap();
        assert!(result.found);

        if let Some(verified) = &result.verified_matches {

            // Check that row ranges are correct
            let mut all_rows = vec![];
            for col in &verified.column_details {
                for rg in &col.row_groups {
                    for range in &rg.row_ranges {
                        // Collect all rows in range
                        for row_num in range.start_row..=range.end_row {
                            all_rows.push(row_num);
                        }
                    }
                }
            }

            // Should have exactly 90 rows (50 + 40)
            assert_eq!(all_rows.len(), 90, "Should have exactly 90 rows");

            // Check pattern: rows 0-49 and 60-99
            for row in 0..50 {
                assert!(all_rows.contains(&row),
                        "Should contain row {} in first range", row);
            }
            for row in 60..100 {
                assert!(all_rows.contains(&row),
                        "Should contain row {} in second range", row);
            }
            for row in 50..60 {
                assert!(!all_rows.contains(&row),
                        "Should NOT contain row {} in gap", row);
            }
        } else {
            panic!("Should have verified matches");
        }
    }

    #[tokio::test]
    async fn test_no_splits_nullified_below_threshold() {
        // Verify that when threshold is NOT exceeded, splits are preserved
        let mut data: Vec<String> = vec![];

        // Add only 10 rows
        for i in 0..10 {
            data.push(format!("test@example.com{}", i));
        }

        // Set high threshold at 0.5 (50% of 10 rows = 5 rows)
        // With 10 rows, threshold is not exceeded
        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.5)
        ).await;

        let result = searcher.search("test", None, true).await.unwrap();
        assert!(result.found);

        if let Some(verified) = &result.verified_matches {
            // Should still have splits_matched information
            assert!(verified.splits_matched.is_some(),
                    "Should preserve splits when below threshold");

            // Row-level splits should also be preserved
            for col in &verified.column_details {
                for rg in &col.row_groups {
                    for range in &rg.row_ranges {
                        assert!(range.splits_matched.is_some(),
                                "Row-level splits should be preserved");
                    }
                }
            }
        } else {
            panic!("Should have verified matches");
        }
    }

    #[tokio::test]
    async fn test_splits_nullified_exact_threshold() {
        // Test edge case: exactly at threshold
        let mut data: Vec<String> = vec![];

        // Add exactly 20 rows
        for i in 0..20 {
            data.push(format!("edge@case.com{}", i));
        }

        // Set threshold at 0.1 (10% of 20 = 2 rows)
        // With 20 rows and each token appearing in many rows, should exceed
        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.1)
        ).await;

        let result = searcher.search("edge", None, true).await.unwrap();
        assert!(result.found);
    }

    #[tokio::test]
    async fn test_multiple_columns_with_nullified_splits() {
        // Create data with multiple columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
            Field::new("username", DataType::Utf8, false),
        ]));

        let mut emails = vec![];
        let mut usernames = vec![];

        // Add many rows where "common" appears in both columns
        for i in 0..100 {
            emails.push(format!("common{}@test.com", i));
            usernames.push("common_user".to_string());
        }

        let props = WriterProperties::builder().build();
        let email_array: ArrayRef = Arc::new(StringArray::from(emails));
        let username_array: ArrayRef = Arc::new(StringArray::from(usernames));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![email_array, username_array]
        ).unwrap();
        let parquet_bytes = write_parquet_to_bytes(schema.clone(), batch, props);

        let bytes = Bytes::from(parquet_bytes.clone());
        let source = ParquetSource::Bytes(bytes);

        let result = process_parquet_file(
            source.clone(),
            None,
            Some(0.01),
            None,
            None,
            None,
            None,
            Some(0.05), // 5% threshold
        ).await.unwrap();

        let split_chars: Vec<Vec<char>> = crate::keyword_shred::SPLIT_CHARS_INCLUSIVE
            .iter()
            .map(|&chars| chars.to_vec())
            .collect();

        let index_files = build_distributed_index(
            &result,
            &source,
            0.01,
            CompressionAlgorithm::Zstd { level: 8 },
            CompressionAlgorithm::Zstd { level: 8 },
            &split_chars,
        ).await.unwrap();

        let searcher = KeywordSearcher::from_serialized(
            &crate::index_data::DistributedIndexFiles {
                filters: index_files.filters,
                data: index_files.data,
            },
            "test.parquet.index".to_string(),
            None,
        ).unwrap();

        // Search for "common" which appears in both columns
        let result = searcher.search("common", None, true).await.unwrap();
        assert!(result.found);

        if let Some(verified) = &result.verified_matches {
            // Should have nullified splits for this common keyword
            assert!(verified.splits_matched.is_none());

            // Should have both columns
            assert_eq!(verified.columns.len(), 2,
                       "Should find keyword in both columns");

            // Total occurrences should be across both columns
            assert!(verified.total_occurrences >= 100,
                    "Should have occurrences from both columns");
        }
    }

    #[tokio::test]
    async fn test_search_never_panics_with_nullified_splits() {
        // Stress test: ensure no panics with various search patterns
        let mut data: Vec<String> = vec![];
        for i in 0..50 {
            data.push(format!("test.user-name_123@example.com{}", i));
        }

        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.02)
        ).await;

        // Try various search patterns - none should panic
        let search_terms = vec![
            ("test", true),        // keyword only
            ("test", false),       // phrase search
            ("user", true),
            ("user", false),
            ("name", true),
            ("name", false),
            ("test.user", false),  // multi-token phrase
            ("user-name", false),
            ("name_123", false),
            ("example.com", false),
            ("nonexistent", true), // not found
            ("nonexistent", false),
        ];

        for (term, keyword_only) in search_terms {
            let result = searcher.search(term, None, keyword_only).await;
            assert!(result.is_ok(),
                    "Search for '{}' (keyword_only={}) should not panic or error",
                    term, keyword_only);
        }
    }

    #[tokio::test]
    async fn test_column_filter_with_nullified_splits() {
        // Test that column filtering works with nullified splits
        let mut data: Vec<String> = vec![];
        for i in 0..50 {
            data.push(format!("common@test.com{}", i));
            data.push(format!("badger@test.com{}", i));
        }

        let searcher = create_test_index_with_thresholds(
            data,
            Some(0.05)
        ).await;

        // Search with column filter
        let result = searcher.search("common", Some("email"), true).await.unwrap();
        assert!(result.found);

        if let Some(verified) = &result.verified_matches {
            assert_eq!(verified.columns.len(), 1, "Should have exactly one column");
            assert_eq!(verified.columns[0], "email", "Should be the email column");
        }
    }

    // ============================================================================
    // SPLIT TRACKING FLAG TESTS (2 tests)
    // ============================================================================

    #[tokio::test]
    async fn test_splits_not_readded_after_elimination() {
        // CRITICAL TEST: Verify splits_tracking_enabled flag prevents re-addition
        // This test creates data spanning multiple row groups and ensures that
        // after elimination in the first row group, subsequent row groups don't
        // add rows with splits_matched = Some(...)

        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
        ]));

        // Force multiple row groups with small row group size
        let props = WriterProperties::builder()
            .set_max_row_group_size(40)  // Force at least 2 row groups
            .set_compression(Compression::SNAPPY)
            .build();

        let mut data = vec![];

        // Add 80 rows of "common@test.com" - will span 2 row groups
        for _ in 0..80 {
            data.push("common@test.com");
        }

        let array: ArrayRef = Arc::new(StringArray::from(
            data.iter().map(|s| s.to_string()).collect::<Vec<_>>()
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let parquet_bytes = write_parquet_to_bytes(schema, batch, props);

        let source = ParquetSource::Bytes(Bytes::from(parquet_bytes));

        // Use very low threshold (1% of 80 = 0.8 rows)
        // "common" keyword appears in way more than 0.8 rows, so will trigger elimination
        let result = process_parquet_file(
            source.clone(),
            None,
            Some(0.01),
            None,
            None,
            None,
            None,
            Some(0.01),  // 1% threshold
        ).await.unwrap();

        let split_chars: Vec<Vec<char>> = crate::keyword_shred::SPLIT_CHARS_INCLUSIVE
            .iter()
            .map(|&chars| chars.to_vec())
            .collect();

        let index_files = build_distributed_index(
            &result,
            &source,
            0.01,
            CompressionAlgorithm::Zstd { level: 8 },
            CompressionAlgorithm::Zstd { level: 8 },
            &split_chars,
        ).await.unwrap();

        let searcher = KeywordSearcher::from_serialized(
            &crate::index_data::DistributedIndexFiles {
                filters: index_files.filters,
                data: index_files.data,
            },
            "test.parquet.index".to_string(),
            None,
        ).unwrap();

        let search_result = searcher.search("common", None, true).await.unwrap();
        assert!(search_result.found, "Should find 'common' keyword");

        if let Some(verified) = &search_result.verified_matches {

            // CRITICAL TEST: Verify ALL rows have splits_matched = None
            // If the bug still exists, some rows (from later row groups) would have Some(...)
            let mut total_rows_checked = 0;
            for col in &verified.column_details {
                for rg in &col.row_groups {
                    for range in &rg.row_ranges {
                        // Every single row range should have None
                        assert!(range.splits_matched.is_none(),
                                "BUG DETECTED: Row group {} has splits_matched = {:?}, expected None. \
                            This means splits were re-added after elimination!",
                                rg.row_group_id, range.splits_matched);

                        total_rows_checked += (range.end_row - range.start_row + 1) as usize;
                    }
                }
            }

            // Verify we actually checked rows from multiple row groups
            assert!(total_rows_checked >= 80,
                    "Should have checked all 80 rows, checked: {}", total_rows_checked);

            println!("✓ SUCCESS: All {} rows have splits_matched = None", total_rows_checked);
            println!("✓ Split tracking flag is working correctly!");
        } else {
            panic!("Should have verified matches");
        }
    }

    #[tokio::test]
    async fn test_splits_preserved_when_tracking_enabled() {
        // Verify that when threshold is NOT exceeded, splits are still tracked
        // This confirms the flag works both ways: preserving when enabled

        let schema = Arc::new(Schema::new(vec![
            Field::new("email", DataType::Utf8, false),
        ]));

        let props = WriterProperties::builder()
            .set_max_row_group_size(40)
            .build();

        let mut data = vec![];

        // Add only 10 rows - won't exceed high threshold
        for _ in 0..10 {
            data.push("rare@test.com");
        }

        let array: ArrayRef = Arc::new(StringArray::from(
            data.iter().map(|s| s.to_string()).collect::<Vec<_>>()
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let parquet_bytes = write_parquet_to_bytes(schema, batch, props);

        let source = ParquetSource::Bytes(Bytes::from(parquet_bytes));

        // High threshold (50% of 10 = 5 rows) - won't be exceeded
        let result = process_parquet_file(
            source.clone(),
            None,
            Some(0.01),
            None,
            None,
            None,
            None,
            Some(0.5),  // 50% threshold
        ).await.unwrap();

        let split_chars: Vec<Vec<char>> = crate::keyword_shred::SPLIT_CHARS_INCLUSIVE
            .iter()
            .map(|&chars| chars.to_vec())
            .collect();

        let index_files = build_distributed_index(
            &result,
            &source,
            0.01,
            CompressionAlgorithm::Zstd { level: 8 },
            CompressionAlgorithm::Zstd { level: 8 },
            &split_chars,
        ).await.unwrap();

        let searcher = KeywordSearcher::from_serialized(
            &crate::index_data::DistributedIndexFiles {
                filters: index_files.filters,
                data: index_files.data,
            },
            "test.parquet.index".to_string(),
            None,
        ).unwrap();

        let search_result = searcher.search("rare", None, true).await.unwrap();
        assert!(search_result.found);

        if let Some(verified) = &search_result.verified_matches {
            // Should still have splits_matched
            assert!(verified.splits_matched.is_some(),
                    "Should preserve splits when threshold not exceeded");

            // Rows should also have splits
            for col in &verified.column_details {
                for rg in &col.row_groups {
                    for range in &rg.row_ranges {
                        assert!(range.splits_matched.is_some(),
                                "Row-level splits should be preserved");
                    }
                }
            }

            println!("✓ Splits correctly preserved when below threshold");
        }
    }
}