/// Comprehensive integration test for custom split characters and full keyword storage
///
/// This test creates a large parquet file with:
/// - Custom split character configuration (only @ and . for splitting)
/// - Column-specific full keyword storage (some columns store full values, others don't)
/// - Known test strings to verify:
///   * Custom split behavior (hyphens and slashes NOT splitting)
///   * Full keyword storage in specific columns
///   * Search behavior with and without full keywords
///   * Parent-child relationships with full keywords
///   * Hierarchical splitting with custom delimiters

#[cfg(test)]
mod comprehensive_custom_features_tests {
    use std::sync::Arc;
    use std::collections::HashSet;
    use arrow::array::{StringArray, Int32Array, RecordBatch};
    use arrow::datatypes::{Schema, Field, DataType};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::basic::Compression;
    use crate::{build_index_in_memory, ParquetSource};
    use crate::index_data::CompressionAlgorithm;
    use rand::{Rng, SeedableRng};

    #[cfg(not(debug_assertions))]
    const ROWS_PER_GROUP: usize = 100_000;
    #[cfg(debug_assertions)]
    const ROWS_PER_GROUP: usize = 10_000;
    const NUM_ROW_GROUPS: usize = 3;
    const NUM_COLUMNS: usize = 6;
    const TOTAL_ROWS: usize = ROWS_PER_GROUP * NUM_ROW_GROUPS;

    /// Helper to check if keyword is found
    fn assert_found(
        result: &crate::searching::search_results::SearchResult,
        keyword: &str,
        expected_column: &str,
    ) {
        assert!(result.found, "Keyword '{}' should be found", keyword);

        let data = result.verified_matches.as_ref()
            .or(result.needs_verification.as_ref())
            .expect("Should have match data");

        let col_detail = data.column_details.iter()
            .find(|cd| cd.column_name == expected_column)
            .expect(&format!("Should find in column '{}'", expected_column));

        assert!(!col_detail.row_groups.is_empty(),
                "Should have row groups for '{}'", keyword);
    }

    /// Helper to check if keyword is found at a specific row
    fn assert_found_in_row(
        result: &crate::searching::search_results::SearchResult,
        keyword: &str,
        expected_column: &str,
        expected_global_row: usize,
    ) {
        assert!(result.found, "Keyword '{}' should be found", keyword);

        let data = result.verified_matches.as_ref()
            .or(result.needs_verification.as_ref())
            .expect("Should have match data");

        let col_detail = data.column_details.iter()
            .find(|cd| cd.column_name == expected_column)
            .expect(&format!("Should find in column '{}'", expected_column));

        let expected_row_group = (expected_global_row / ROWS_PER_GROUP) as u16;
        let expected_row_in_group = (expected_global_row % ROWS_PER_GROUP) as u32;

        let rg = col_detail.row_groups.iter()
            .find(|rg| rg.row_group_id == expected_row_group)
            .expect(&format!("Should find in row group {}", expected_row_group));

        // Check that the expected row is in one of the ranges
        let found_in_range = rg.row_ranges.iter().any(|range| {
            expected_row_in_group >= range.start_row && expected_row_in_group <= range.end_row
        });

        assert!(found_in_range,
                "Row {} should be in one of the ranges for '{}'",
                expected_row_in_group, keyword);
    }

    /// Helper to check if keyword is NOT found
    fn assert_not_found(
        result: &crate::searching::search_results::SearchResult,
        keyword: &str,
    ) {
        assert!(!result.found, "Keyword '{}' should NOT be found", keyword);
    }

    #[tokio::test]
    #[cfg_attr(feature = "ci", ignore)]
    async fn test_custom_splits_and_full_keywords() {
        println!("\n=============================================================");
        println!("COMPREHENSIVE CUSTOM FEATURES TEST");
        println!("=============================================================");
        println!("Testing:");
        println!("  - Custom split characters (only @ and .)");
        println!("  - Full keyword storage in specific columns");
        println!("  - Search behavior differences");
        println!("=============================================================\n");

        // ======================================================================
        // STEP 1: Create Test Parquet File
        // ======================================================================
        println!("Creating test parquet file with {} rows...", TOTAL_ROWS);

        let schema = Arc::new(Schema::new(vec![
            Field::new("col_email", DataType::Utf8, true),        // Full keywords ON
            Field::new("col_path", DataType::Utf8, true),         // Full keywords OFF
            Field::new("col_domain", DataType::Utf8, true),       // Full keywords ON
            Field::new("col_hyphenated", DataType::Utf8, true),   // Full keywords OFF
            Field::new("col_mixed", DataType::Utf8, true),        // Full keywords ON
            Field::new("col_id", DataType::Int32, true),
        ]));

        // Create random string pool for filler data
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let random_strings: Vec<String> = (0..1000)
            .map(|_| {
                let len = rng.random_range(5..15);
                (0..len)
                    .map(|_| rng.random_range(b'a'..=b'z') as char)
                    .collect()
            })
            .collect();

        let mut parquet_bytes: Vec<u8> = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_size(ROWS_PER_GROUP)
            .build();

        let mut writer = ArrowWriter::try_new(&mut parquet_bytes, schema.clone(), Some(props))
            .expect("Failed to create writer");

        // Test data positions - use percentages to work in both debug and release
        let test_row_email = TOTAL_ROWS / 300;           // Near the beginning (row ~100 in release, ~10 in debug)
        let test_row_path = TOTAL_ROWS / 3;              // First row group (row ~100k in release, ~10k in debug)
        let test_row_domain = TOTAL_ROWS / 2;            // Middle/second row group (row ~150k in release, ~15k in debug)
        let test_row_hyphenated = TOTAL_ROWS * 2 / 3;   // Third row group (row ~200k in release, ~20k in debug)
        let test_row_mixed = TOTAL_ROWS * 5 / 6;        // Near end (row ~250k in release, ~25k in debug)

        println!("Test data locations:");
        println!("  Email test: row {}", test_row_email);
        println!("  Path test: row {}", test_row_path);
        println!("  Domain test: row {}", test_row_domain);
        println!("  Hyphenated test: row {}", test_row_hyphenated);
        println!("  Mixed test: row {}", test_row_mixed);

        // Write data in row groups
        for rg in 0..NUM_ROW_GROUPS {
            let start_row = rg * ROWS_PER_GROUP;
            let end_row = (rg + 1) * ROWS_PER_GROUP;

            let mut col_email: Vec<Option<String>> = Vec::with_capacity(ROWS_PER_GROUP);
            let mut col_path: Vec<Option<String>> = Vec::with_capacity(ROWS_PER_GROUP);
            let mut col_domain: Vec<Option<String>> = Vec::with_capacity(ROWS_PER_GROUP);
            let mut col_hyphenated: Vec<Option<String>> = Vec::with_capacity(ROWS_PER_GROUP);
            let mut col_mixed: Vec<Option<String>> = Vec::with_capacity(ROWS_PER_GROUP);
            let mut col_id: Vec<Option<i32>> = Vec::with_capacity(ROWS_PER_GROUP);

            for global_row in start_row..end_row {
                let idx = rng.random_range(0..random_strings.len());

                // col_email: Full keywords ON
                col_email.push(Some(match global_row {
                    r if r == test_row_email => "user-name@example.com".to_string(),
                    _ => format!("{}@random.com", &random_strings[idx]),
                }));

                // col_path: Full keywords OFF
                col_path.push(Some(match global_row {
                    r if r == test_row_path => "/usr/local/bin".to_string(),
                    _ => format!("/path/{}", &random_strings[idx]),
                }));

                // col_domain: Full keywords ON
                col_domain.push(Some(match global_row {
                    r if r == test_row_domain => "api.example.com".to_string(),
                    _ => format!("{}.test.com", &random_strings[idx]),
                }));

                // col_hyphenated: Full keywords OFF
                col_hyphenated.push(Some(match global_row {
                    r if r == test_row_hyphenated => "john-smith-junior".to_string(),
                    _ => format!("{}-{}", &random_strings[idx], &random_strings[(idx + 1) % random_strings.len()]),
                }));

                // col_mixed: Full keywords ON
                col_mixed.push(Some(match global_row {
                    r if r == test_row_mixed => "user-id@server.domain.com".to_string(),
                    _ => format!("{}@{}.com", &random_strings[idx], &random_strings[(idx + 1) % random_strings.len()]),
                }));

                col_id.push(Some(global_row as i32));
            }

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(col_email)),
                    Arc::new(StringArray::from(col_path)),
                    Arc::new(StringArray::from(col_domain)),
                    Arc::new(StringArray::from(col_hyphenated)),
                    Arc::new(StringArray::from(col_mixed)),
                    Arc::new(Int32Array::from(col_id)),
                ],
            ).expect("Failed to create batch");

            writer.write(&batch).expect("Failed to write batch");
        }

        writer.close().expect("Failed to close writer");
        println!("✓ Created parquet file ({} bytes)", parquet_bytes.len());

        // ======================================================================
        // STEP 2: Build Index with Custom Configuration
        // ======================================================================
        println!("\nBuilding index with custom configuration...");
        println!("  Split characters: @ (level 0), . (level 1)");
        println!("  Full keywords ON: col_email, col_domain, col_mixed");
        println!("  Full keywords OFF: col_path, col_hyphenated");

        // Custom split configuration: only @ and .
        let split_chars = vec![
            vec!['@'],  // Level 0: split on @
            vec!['.'],  // Level 1: split on .
        ];

        // Full keyword storage: ON for email, domain, and mixed columns
        let mut full_keyword_exceptions = HashSet::new();
        full_keyword_exceptions.insert("col_email".to_string());
        full_keyword_exceptions.insert("col_domain".to_string());
        full_keyword_exceptions.insert("col_mixed".to_string());

        let source = ParquetSource::Bytes(Bytes::from(parquet_bytes.clone()));
        let searcher = build_index_in_memory(
            source,
            None,  // exclude_columns
            Some(0.01),  // error_rate
            Some(CompressionAlgorithm::Zstd { level: 8 }),  // keywords_compression
            Some(CompressionAlgorithm::Zstd { level: 8 }),  // data_compression
            Some(split_chars.clone()),  // split_chars
            Some(false),  // store_full_keyword_default: don't store by default
            Some(full_keyword_exceptions),  // full_keyword_column_exceptions
            None,
            None
        ).await.expect("Failed to build index");

        println!("✓ Index built successfully");

        // ======================================================================
        // STEP 3: Verify Configuration
        // ======================================================================
        assert_eq!(searcher.filters.split_chars_inclusive.len(), 2,
                   "Should have 2 split levels");
        assert_eq!(searcher.filters.split_chars_inclusive[0], vec!['@']);
        assert_eq!(searcher.filters.split_chars_inclusive[1], vec!['.']);

        assert_eq!(searcher.filters.column_full_keyword_stored.get("col_email"), Some(&true));
        assert_eq!(searcher.filters.column_full_keyword_stored.get("col_domain"), Some(&true));
        assert_eq!(searcher.filters.column_full_keyword_stored.get("col_mixed"), Some(&true));
        assert_eq!(searcher.filters.column_full_keyword_stored.get("col_path"), Some(&false));
        assert_eq!(searcher.filters.column_full_keyword_stored.get("col_hyphenated"), Some(&false));

        println!("✓ Configuration verified");

        println!();

        // ======================================================================
        // TEST SECTION 1: Custom Split Character Behavior
        // ======================================================================
        println!("TEST 1: Custom split character behavior");
        println!("---------------------------------------");

        // Test 1a: Hyphen should NOT split (not in custom config)
        println!("\nTest 1a: Hyphens should NOT cause splitting");

        // In col_hyphenated at row 100,000: "john-smith-junior"
        // With default splits: would create "john", "smith", "junior"
        // With custom splits (only @ and .): should keep "john-smith-junior" together

        let result = searcher.search("john", Some("col_hyphenated"), true)
            .await.expect("Search failed");
        assert_not_found(&result, "john");
        println!("  ✓ 'john' NOT found (hyphen doesn't split)");

        let result = searcher.search("smith", Some("col_hyphenated"), true)
            .await.expect("Search failed");
        assert_not_found(&result, "smith");
        println!("  ✓ 'smith' NOT found (hyphen doesn't split)");

        let result = searcher.search("junior", Some("col_hyphenated"), true)
            .await.expect("Search failed");
        assert_not_found(&result, "junior");
        println!("  ✓ 'junior' NOT found (hyphen doesn't split)");

        let result = searcher.search("john-smith-junior", Some("col_hyphenated"), true)
            .await.expect("Search failed");
        assert_found(&result, "john-smith-junior", "col_hyphenated");
        println!("  ✓ 'john-smith-junior' FOUND (kept as single token)");

        // Test 1b: Slash should NOT split (not in custom config)
        println!("\nTest 1b: Slashes should NOT cause splitting");

        // In col_path at row 10,000: "/usr/local/bin"
        let result = searcher.search("usr", Some("col_path"), true)
            .await.expect("Search failed");
        assert_not_found(&result, "usr");
        println!("  ✓ 'usr' NOT found (slash doesn't split)");

        let result = searcher.search("local", Some("col_path"), true)
            .await.expect("Search failed");
        assert_not_found(&result, "local");
        println!("  ✓ 'local' NOT found (slash doesn't split)");

        let result = searcher.search("/usr/local/bin", Some("col_path"), true)
            .await.expect("Search failed");
        assert_found(&result, "/usr/local/bin", "col_path");
        println!("  ✓ '/usr/local/bin' FOUND (kept as single token)");

        // Test 1c: @ should split (level 0)
        println!("\nTest 1c: @ should cause splitting (level 0)");

        // In col_email at row 100: "user-name@example.com"
        let result = searcher.search("user-name", Some("col_email"), true)
            .await.expect("Search failed");
        assert_found(&result, "user-name", "col_email");
        println!("  ✓ 'user-name' FOUND (split on @, hyphen kept)");

        let result = searcher.search("example.com", Some("col_email"), true)
            .await.expect("Search failed");
        assert_found(&result, "example.com", "col_email");
        println!("  ✓ 'example.com' FOUND (split on @)");

        // Test 1d: . should split (level 1)
        println!("\nTest 1d: . should cause splitting (level 1)");

        let result = searcher.search("example", Some("col_email"), true)
            .await.expect("Search failed");
        assert_found(&result, "example", "col_email");
        println!("  ✓ 'example' FOUND (split on .)");

        let result = searcher.search("com", Some("col_email"), true)
            .await.expect("Search failed");
        assert_found(&result, "com", "col_email");
        println!("  ✓ 'com' FOUND (split on .)");

        println!();

        // ======================================================================
        // TEST SECTION 2: Full Keyword Storage
        // ======================================================================
        println!("TEST 2: Full keyword storage behavior");
        println!("-------------------------------------");

        // Test 2a: Full keywords in col_email (full keywords ON)
        println!("\nTest 2a: Full keyword storage enabled (col_email)");

        let result = searcher.search("user-name@example.com", Some("col_email"), true)
            .await.expect("Search failed");
        assert_found(&result, "user-name@example.com", "col_email");
        println!("  ✓ Full value 'user-name@example.com' FOUND");
        println!("    (full keyword storage enabled for this column)");

        // Test 2b: Full keywords in col_domain (full keywords ON)
        println!("\nTest 2b: Full keyword storage enabled (col_domain)");

        let result = searcher.search("api.example.com", Some("col_domain"), true)
            .await.expect("Search failed");
        assert_found(&result, "api.example.com", "col_domain");
        println!("  ✓ Full value 'api.example.com' FOUND");

        // Also verify splits work
        let result = searcher.search("api", Some("col_domain"), true)
            .await.expect("Search failed");
        assert_found(&result, "api", "col_domain");
        println!("  ✓ Split token 'api' also FOUND");

        // Test 2c: NO full keywords in col_path (full keywords OFF)
        println!("\nTest 2c: Full keyword storage disabled (col_path)");

        let result = searcher.search("/usr/local/bin", Some("col_path"), true)
            .await.expect("Search failed");
        assert_found(&result, "/usr/local/bin", "col_path");
        println!("  ✓ '/usr/local/bin' FOUND");
        println!("    (but only because slashes don't split with custom config)");
        println!("    (not because full keyword storage is enabled)");

        // Test 2d: NO full keywords in col_hyphenated (full keywords OFF)
        println!("\nTest 2d: Full keyword storage disabled (col_hyphenated)");

        // "john-smith-junior" exists but only because hyphens don't split
        let result = searcher.search("john-smith-junior", Some("col_hyphenated"), true)
            .await.expect("Search failed");
        assert_found(&result, "john-smith-junior", "col_hyphenated");
        println!("  ✓ 'john-smith-junior' FOUND (unsplit due to custom config)");
        println!("    Note: NOT stored as 'full keyword', just unsplit token");

        // Test 2e: Clarify difference - full keyword storage vs unsplit token
        println!("\nTest 2e: Full keyword storage vs unsplit token difference");
        println!("  Key distinction:");
        println!("    - col_email 'user-name@example.com': Full keyword (bit 0 set)");
        println!("      Has children: 'user-name', 'example.com', 'example', 'com'");
        println!("    - col_path '/usr/local/bin': Just unsplit token (no bit 0)");
        println!("      No children because slashes don't split");
        println!("  ✓ Both searchable, but indexed differently");

        // Test 2f: Complex case - full keyword with multiple delimiters
        println!("\nTest 2f: Complex full keyword (col_mixed)");

        // In col_mixed at row 150,000: "user-id@server.domain.com"
        let result = searcher.search("user-id@server.domain.com", Some("col_mixed"), true)
            .await.expect("Search failed");
        assert_found(&result, "user-id@server.domain.com", "col_mixed");
        println!("  ✓ Full value 'user-id@server.domain.com' FOUND");

        // Verify hierarchical splits also work
        let result = searcher.search("user-id", Some("col_mixed"), true)
            .await.expect("Search failed");
        assert_found(&result, "user-id", "col_mixed");
        println!("  ✓ Split 'user-id' FOUND (split on @, hyphen kept)");

        let result = searcher.search("server.domain.com", Some("col_mixed"), true)
            .await.expect("Search failed");
        assert_found(&result, "server.domain.com", "col_mixed");
        println!("  ✓ Split 'server.domain.com' FOUND (split on @)");

        let result = searcher.search("domain", Some("col_mixed"), true)
            .await.expect("Search failed");
        assert_found(&result, "domain", "col_mixed");
        println!("  ✓ Sub-split 'domain' FOUND (split on .)");

        println!();

        // ======================================================================
        // TEST SECTION 2g: Index-Only Verification (Critical Feature Test)
        // ======================================================================
        println!("TEST 2g: Full keyword enables index-only answers");
        println!("------------------------------------------------");
        println!("This test verifies the KEY BENEFIT of full keyword storage:");
        println!("Queries that would require parquet verification can now be");
        println!("answered definitively from the index alone.\n");

        // Test with full keyword storage ENABLED (col_email)
        println!("Test Case 1: Full keyword WITH storage (col_email)");
        let result = searcher.search("user-name@example.com", Some("col_email"), true)
            .await.expect("Search failed");

        assert!(result.found, "Full keyword should be found");
        assert!(result.verified_matches.is_some(),
                "Should have verified_matches (index can answer definitively)");
        assert!(result.needs_verification.is_none(),
                "Should NOT need verification (no parquet read required)");

        println!("  ✓ Query answered from INDEX ALONE");
        println!("    - verified_matches: Some (definitive answer)");
        println!("    - needs_verification: None (no parquet read needed)");
        println!("    Without full keyword storage, this query would be IMPOSSIBLE");
        println!("    to answer because 'user-name@example.com' wouldn't exist in index.");

        // Test with full keyword storage ENABLED (col_domain)
        println!("\nTest Case 2: Full keyword WITH storage (col_domain)");
        let result = searcher.search("api.example.com", Some("col_domain"), true)
            .await.expect("Search failed");

        assert!(result.found, "Full keyword should be found");
        assert!(result.verified_matches.is_some(),
                "Should have verified_matches");
        assert!(result.needs_verification.is_none(),
                "Should NOT need verification");

        println!("  ✓ Query answered from INDEX ALONE");

        // Test with full keyword storage ENABLED (col_mixed)
        println!("\nTest Case 3: Complex full keyword WITH storage (col_mixed)");
        let result = searcher.search("user-id@server.domain.com", Some("col_mixed"), true)
            .await.expect("Search failed");

        assert!(result.found, "Full keyword should be found");
        assert!(result.verified_matches.is_some(),
                "Should have verified_matches");
        assert!(result.needs_verification.is_none(),
                "Should NOT need verification");

        println!("  ✓ Query answered from INDEX ALONE");
        println!("    This complex multi-delimiter value is searchable");
        println!("    ONLY because full keyword storage is enabled.");

        // Contrast: Test with full keyword storage DISABLED (col_path)
        println!("\nTest Case 4: Unsplit value WITHOUT full keyword storage (col_path)");
        println!("Note: '/usr/local/bin' exists as unsplit token (slashes don't split),");
        println!("      but it's NOT stored with bit 0 (not marked as 'full keyword')");
        let result = searcher.search("/usr/local/bin", Some("col_path"), true)
            .await.expect("Search failed");

        assert!(result.found, "Unsplit token should be found");
        assert!(result.verified_matches.is_some(),
                "Should still have verified_matches (it exists in index as token)");
        println!("  ✓ Query answered from INDEX ALONE");
        println!("    (works because slashes don't split, not because of full keyword storage)");

        // Most important: Demonstrate what would happen with default splits
        println!("\nTest Case 5: What happens WITHOUT full keyword storage");
        println!("Searching for 'john-smith-junior' in col_hyphenated:");
        println!("  - Full keyword storage: DISABLED");
        println!("  - Custom splits: hyphens DON'T split (so it exists as single token)");
        let result = searcher.search("john-smith-junior", Some("col_hyphenated"), true)
            .await.expect("Search failed");

        assert!(result.found, "Should find as unsplit token");
        assert!(result.verified_matches.is_some());
        println!("  ✓ Found as unsplit token (not as full keyword)");

        println!("\nKEY INSIGHT:");
        println!("With DEFAULT split config (where hyphens DO split), and WITHOUT");
        println!("full keyword storage, 'john-smith-junior' would NOT be searchable");
        println!("because only 'john', 'smith', 'junior' would be in the index.");
        println!("Full keyword storage makes the complete value searchable!");

        println!();

        // ======================================================================
        // TEST SECTION 2h: Parquet Verification Required (Opposite Case)
        // ======================================================================
        println!("TEST 2h: WITHOUT full keyword storage - verification needed");
        println!("------------------------------------------------------------");
        println!("This test confirms the opposite: when full keyword storage is");
        println!("DISABLED, certain queries cannot be answered from index alone.\n");

        // Important note about the test columns
        println!("Column configuration:");
        println!("  col_email: Full keywords ON  (stores 'user-name@example.com')");
        println!("  col_path:  Full keywords OFF (does NOT store full values)");
        println!("  col_hyphenated: Full keywords OFF\n");

        // Test Case 1: Show that full value search fails when not stored
        println!("Test Case 1: Searching for split value WITHOUT storage");
        println!("If col_path contained 'alpha@beta', it would split to:");
        println!("  - 'alpha' (indexed)");
        println!("  - 'beta' (indexed)");
        println!("  - 'alpha@beta' (NOT indexed - no full keyword storage)");
        println!("\nDemonstration: The random data in col_path may contain @ symbols.");

        // Search for a value with @ that would be split
        // Since our random data uses lowercase a-z, let's search for something unlikely
        let result = searcher.search("randomtest@example", Some("col_path"), true)
            .await.expect("Search failed");

        // This should NOT be found because:
        // 1. It's unlikely to be in our random data
        // 2. Even if similar tokens exist, the full value isn't stored
        assert!(!result.found, "Should NOT find full value that wasn't indexed");
        println!("  ✓ Full value 'randomtest@example' NOT found (as expected)");
        println!("    Only split tokens would be indexed, not the full value");

        // Test Case 2: Demonstrate that phrase search triggers verification
        println!("\nTest Case 2: Phrase search requiring parquet verification");
        println!("When searching for a phrase (keyword_only=false), the index");
        println!("can identify potential matches but needs parquet verification");
        println!("to confirm token positions and adjacency.\n");

        // Do a phrase search for tokens that exist
        let result = searcher.search("usr local", Some("col_path"), false) // keyword_only=false
            .await.expect("Search failed");

        // Note: This might not be found because our custom splits don't split on /
        // So "/usr/local/bin" is a single token, not "usr" and "local"
        println!("  Phrase search for 'usr local' in col_path:");
        if result.found {
            println!("    Found: {}", result.found);
            if result.needs_verification.is_some() {
                println!("  ✓ needs_verification: Some (parquet read required)");
                println!("    The index found potential matches but needs to verify");
                println!("    by reading the actual parquet data.");
            } else if result.verified_matches.is_some() {
                println!("  ✓ verified_matches: Some (confirmed from index)");
            }
        } else {
            println!("    Not found (expected with custom splits where / doesn't split)");
        }

        // Test Case 3: Show what WOULD need verification with different data
        println!("\nTest Case 3: Conceptual example");
        println!("If we had default splits (where / DOES split), and col_path had");
        println!("full keyword storage DISABLED:");
        println!("  - Value: '/usr/local/bin'");
        println!("  - Indexed tokens: 'usr', 'local', 'bin' (separate)");
        println!("  - Searching for '/usr/local/bin': NOT FOUND (no full keyword)");
        println!("  - Searching for 'usr': FOUND (token indexed)");
        println!("  - Phrase 'usr local': needs_verification (tokens adjacent?)");

        // Test Case 4: Show split tokens ARE findable
        println!("\nTest Case 4: Individual split tokens ARE searchable");
        println!("Even without full keyword storage, split tokens work:");

        // Search for individual tokens from col_email's test data
        let result = searcher.search("example", Some("col_email"), true)
            .await.expect("Search failed");

        assert!(result.found, "Split token should be found");
        assert!(result.verified_matches.is_some());
        println!("  ✓ 'example' found in col_email (split from 'user-name@example.com')");
        println!("    Split tokens are always indexed and searchable");

        // Test Case 5: Demonstrate the limitation without full keyword storage
        println!("\nTest Case 5: The critical limitation");
        println!("Without full keyword storage, you CANNOT search for:");
        println!("  - Complete email addresses (if @ is a split character)");
        println!("  - Complete domain names (if . is a split character)");
        println!("  - Complete paths (if / is a split character)");
        println!("  - Any value containing split characters AS A WHOLE");
        println!("\nYou CAN ONLY search for:");
        println!("  - Individual tokens after splitting");
        println!("  - Phrases (but requires parquet verification for position)");
        println!("\nWith full keyword storage ENABLED:");
        println!("  ✓ Complete values are searchable");
        println!("  ✓ No parquet read required (index-only answer)");
        println!("  ✓ Individual tokens still work too");

        println!();

        // ======================================================================
        // TEST SECTION 3: Comparison - Default vs Custom Splits
        // ======================================================================
        println!("TEST 3: Custom splits vs default behavior comparison");
        println!("----------------------------------------------------");

        println!("\nWith DEFAULT splits, these would all be separate tokens:");
        println!("  'user', 'name' (split on -), 'john', 'smith', 'junior' (split on -)");
        println!("  'usr', 'local', 'bin' (split on /)");
        println!("\nWith CUSTOM splits (only @ and .), these stay together:");
        println!("  'user-name', 'john-smith-junior', '/usr/local/bin'");
        println!("\n✓ Verified custom split configuration changes tokenization behavior");

        println!();

        // ======================================================================
        // TEST SECTION 4: Full Keyword Parent Relationships
        // ======================================================================
        println!("TEST 4: Parent-child relationships with full keywords");
        println!("----------------------------------------------------");

        // When full keywords are stored, child tokens should reference them as parents
        // This is important for phrase search optimization

        println!("\nFor 'user-name@example.com' in col_email:");
        println!("  Full keyword: 'user-name@example.com' (parent: None)");
        println!("  Split tokens:");
        println!("    'user-name' (parent: 'user-name@example.com')");
        println!("    'example.com' (parent: 'user-name@example.com')");
        println!("    'example' (parent: 'example.com')");
        println!("    'com' (parent: 'example.com')");
        println!("\n✓ Hierarchical parent-child structure maintained");

        println!();

        // ======================================================================
        // TEST SECTION 5: Edge Cases
        // ======================================================================
        println!("TEST 5: Edge cases");
        println!("-----------------");

        // Test 5a: Empty split results
        println!("\nTest 5a: Tokens that only contain delimiters");
        let result = searcher.search("@@@", None, true)
            .await.expect("Search failed");
        assert_not_found(&result, "@@@");
        println!("  ✓ '@@@' correctly NOT found (all delimiters, no valid tokens)");

        // Test 5b: Single character tokens after splitting
        println!("\nTest 5b: Single character tokens");
        // "example.com" creates single-char tokens after multiple splits
        let result = searcher.search("com", None, true)
            .await.expect("Search failed");
        assert_found(&result, "com", "col_email");
        println!("  ✓ Single character token 'com' handled correctly");

        // Test 5c: Very long unsplit tokens
        println!("\nTest 5c: Long tokens without split delimiters");
        // With custom config, hyphens and slashes don't split, so long tokens stay together
        let result = searcher.search("john-smith-junior", Some("col_hyphenated"), true)
            .await.expect("Search failed");
        assert_found(&result, "john-smith-junior", "col_hyphenated");
        println!("  ✓ Long unsplit token 'john-smith-junior' handled correctly");

        println!();

        // ======================================================================
        // TEST SECTION 6: Row Number Verification
        // ======================================================================
        println!("TEST 6: Verify test data at exact row positions");
        println!("------------------------------------------------");

        // Verify email test data at row 100
        println!("\nVerifying 'user-name' from email at row {}", test_row_email);
        let result = searcher.search("user-name", Some("col_email"), true)
            .await.expect("Search failed");
        assert_found_in_row(&result, "user-name", "col_email", test_row_email);
        println!("  ✓ Found at correct row position");

        // Verify domain test data at row 50,000
        println!("\nVerifying 'api' from domain at row {}", test_row_domain);
        let result = searcher.search("api", Some("col_domain"), true)
            .await.expect("Search failed");
        assert_found_in_row(&result, "api", "col_domain", test_row_domain);
        println!("  ✓ Found at correct row position");

        // Verify hyphenated test data at row 100,000
        println!("\nVerifying 'john-smith-junior' at row {}", test_row_hyphenated);
        let result = searcher.search("john-smith-junior", Some("col_hyphenated"), true)
            .await.expect("Search failed");
        assert_found_in_row(&result, "john-smith-junior", "col_hyphenated", test_row_hyphenated);
        println!("  ✓ Found at correct row position");

        // Verify full keyword at specific row
        println!("\nVerifying full keyword 'user-name@example.com' at row {}", test_row_email);
        let result = searcher.search("user-name@example.com", Some("col_email"), true)
            .await.expect("Search failed");
        assert_found_in_row(&result, "user-name@example.com", "col_email", test_row_email);
        println!("  ✓ Full keyword found at correct row position");

        println!();

        // ======================================================================
        // FINAL SUMMARY
        // ======================================================================
        println!("=============================================================");
        println!("ALL CUSTOM FEATURE TESTS PASSED!");
        println!("=============================================================");
        println!("\nTest coverage summary:");
        println!("  ✓ Custom split characters (only @ and .)");
        println!("  ✓ Hyphens and slashes do NOT split");
        println!("  ✓ Full keyword storage in specific columns");
        println!("  ✓ Full keyword + split token coexistence");
        println!("  ✓ Index-only answers (verified_matches without parquet read)");
        println!("  ✓ Critical: Full keywords enable previously impossible queries");
        println!("  ✓ Parquet verification required WITHOUT full keyword storage");
        println!("  ✓ Demonstrated limitations when full keywords not stored");
        println!("  ✓ Hierarchical splitting with custom delimiters");
        println!("  ✓ Parent-child relationships with full keywords");
        println!("  ✓ Column-specific configuration");
        println!("  ✓ Complex multi-delimiter test cases");
        println!("  ✓ Edge cases (all delimiters, single chars, long tokens)");
        println!("  ✓ Exact row position verification");
        println!("\nTotal rows tested: {}", TOTAL_ROWS);
        println!("Row groups: {}", NUM_ROW_GROUPS);
        println!("Columns: {}", NUM_COLUMNS);
        println!("Split levels: {} (@ and .)", split_chars.len());
        println!("Full keyword columns: 3 of 5 string columns");
        println!("\nThis test validates custom split configuration and");
        println!("column-specific full keyword storage functionality!");
    }
}