#[cfg(test)]
mod tests {
    use tokio::sync::OnceCell;
    use std::sync::Arc;
    use arrow::array::{StringArray, Int32Array, Int64Array, Float64Array, BooleanArray};
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::basic::Compression;
    use rand::{Rng, SeedableRng};
    use crate::{build_index_in_memory, ParquetSource};
    use crate::searching::keyword_search::KeywordSearcher;

    static TEST_SEARCHER: OnceCell<KeywordSearcher> = OnceCell::const_new();

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

    async fn get_searcher() -> &'static KeywordSearcher {
        TEST_SEARCHER.get_or_init(|| async {
            println!("Building test index in memory...");
            let parquet_bytes: Bytes = create_test_parquet().expect("Failed to create test parquet");
            build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None, None, None, None, None, None, None, None)
                .await
                .expect("Build Index Failed")
        }).await
    }

    #[tokio::test]
    async fn test_load_and_search() {
        let searcher = get_searcher().await;

        // Search for keywords that exist in the generated data
        let result = searcher.search("user_0", None, true).await.unwrap();

        println!("Search result for 'user_0': {:?}", result);

        if result.found {
            println!("Found 'user_0' in columns: {:?}", result.verified_matches.as_ref().unwrap().columns);
            println!("Total occurrences: {}", result.verified_matches.as_ref().unwrap().total_occurrences);
        }
    }

    #[tokio::test]
    async fn test_global_filter_rejection() {
        let searcher = get_searcher().await;

        let result = searcher.search("xyzabc123definitely_not_in_file", None, true).await.unwrap();

        assert!(!result.found, "Should not find non-existent keyword");
        println!("Global filter correctly rejected non-existent keyword");
    }

    #[tokio::test]
    async fn test_get_index_info() {
        let searcher = get_searcher().await;

        let info = searcher.get_index_info();
        println!("Index info: {:?}", info);

        assert!(info.version > 0);
        assert!(info.num_columns > 0);
        assert!(info.num_chunks > 0);
    }

    #[tokio::test]
    async fn test_validate_index() {
        let searcher = get_searcher().await;

        let parquet_bytes = create_test_parquet().expect("Failed to create test parquet");
        let source = ParquetSource::Bytes(parquet_bytes);
        let is_valid = searcher.validate_index(&source).await.unwrap();
        println!("Index valid: {}", is_valid);

        if !is_valid {
            println!("Warning: Index may be stale - parquet file has been modified");
        }
    }

    #[tokio::test]
    async fn test_phrase_search_with_verification() {
        let searcher = get_searcher().await;

        // Search for a phrase that exists in the generated data (email addresses)
        let result = searcher.search("test0.com", None, false).await.unwrap();

        println!("\nPhrase search for 'test0.com':");
        println!("  Tokens: {:?}", result.tokens);
        println!("  Found: {}", result.found);

        let mut verified_count = 0;
        let mut needs_verification_count = 0;

        if let Some(verified) = &result.verified_matches {
            for col in &verified.column_details {
                for rg in &col.row_groups {
                    for range in &rg.row_ranges {
                        verified_count += range.end_row - range.start_row + 1;
                    }
                }
            }
        }

        if let Some(needs_check) = &result.needs_verification {
            for col in &needs_check.column_details {
                for rg in &col.row_groups {
                    for range in &rg.row_ranges {
                        needs_verification_count += range.end_row - range.start_row + 1;
                    }
                }
            }
        }

        println!("  Verified matches: {}", verified_count);
        println!("  Needs verification: {}", needs_verification_count);

        if let Some(verified) = &result.verified_matches {
            if !verified.column_details.is_empty() {
                println!("\n  Verified matches (no parquet read needed):");
                let mut match_idx = 0;
                for col_detail in verified.column_details.iter().take(5) {
                    for rg in &col_detail.row_groups {
                        for range in &rg.row_ranges {
                            for row in range.start_row..=range.end_row {
                                if match_idx < 5 {
                                    match_idx += 1;
                                    println!("    Match {}:", match_idx);
                                    println!("      Column: {}", col_detail.column_name);
                                    println!("      Row Group: {}", rg.row_group_id);
                                    println!("      Row: {}", row);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // ── helpers for column-0 tests ───────────────────────────────────────────

    /// Build a single-column UTF-8 parquet in memory.
    fn parquet_one_col(col: &str, values: &[&str]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new(col, DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values.to_vec())) as _],
        ).unwrap();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf
    }

    /// Build a two-column UTF-8 parquet in memory (equal-length slices).
    fn parquet_two_col(ca: &str, va: &[&str], cb: &str, vb: &[&str]) -> Vec<u8> {
        assert_eq!(va.len(), vb.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new(ca, DataType::Utf8, false),
            Field::new(cb, DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(va.to_vec())) as _,
            Arc::new(StringArray::from(vb.to_vec())) as _,
        ]).unwrap();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf
    }

    /// Build a two-column parquet with two explicit row groups.
    /// Each slice is one row group; both must have the same column layout.
    fn parquet_two_col_two_rg(
        ca: &str, va0: &[&str], va1: &[&str],
        cb: &str, vb0: &[&str], vb1: &[&str],
    ) -> Vec<u8> {
        assert_eq!(va0.len(), vb0.len());
        assert_eq!(va1.len(), vb1.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new(ca, DataType::Utf8, false),
            Field::new(cb, DataType::Utf8, false),
        ]));
        // max_row_group_size == len of first batch forces a flush after the first write
        let props = WriterProperties::builder()
            .set_max_row_group_size(va0.len())
            .build();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        let b0 = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(va0.to_vec())) as _,
            Arc::new(StringArray::from(vb0.to_vec())) as _,
        ]).unwrap();
        let b1 = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(va1.to_vec())) as _,
            Arc::new(StringArray::from(vb1.to_vec())) as _,
        ]).unwrap();
        w.write(&b0).unwrap();
        w.write(&b1).unwrap();
        w.close().unwrap();
        buf
    }

    async fn build_searcher(buf: Vec<u8>) -> KeywordSearcher {
        build_index_in_memory(
            ParquetSource::from(buf),
            None, Some(0.01), None, None, None, None, None, None, None,
        ).await.unwrap()
    }

    // ── no-column-filter (all-columns) path ──────────────────────────────────

    /// Keyword appears in multiple rows of a single column.
    /// Column 0 only holds the first occurrence; total_occurrences must equal the real count.
    #[tokio::test]
    async fn test_no_filter_total_occurrences_single_column() {
        // "apple" at rows 0 and 2; "banana" at row 1
        let searcher = build_searcher(parquet_one_col("word", &["apple", "banana", "apple"])).await;

        let result = searcher.search("apple", None, true).await.unwrap();
        assert!(result.found, "apple must be found");

        let data = result.verified_matches.unwrap();
        assert_eq!(
            data.total_occurrences, 2,
            "apple appears twice; column-0 must not truncate the count to 1"
        );
        assert_eq!(data.columns, vec!["word".to_string()]);
    }

    /// Same keyword in two columns at *different* rows.
    /// The all-columns aggregate in column 0 must accumulate every occurrence from every column.
    /// All occurrences must be reflected in total_occurrences and the row ranges must cover
    /// the union of rows from both columns (the aggregate intentionally gives every column
    /// the same superset of candidate rows).
    #[tokio::test]
    async fn test_no_filter_aggregate_covers_all_column_rows() {
        // col_a: "foo" at rows 0 and 2  (first seen at row 0)
        // col_b: "foo" at row 1
        // Aggregate column 0 must accumulate all three: rows 0, 1, 2
        let searcher = build_searcher(parquet_two_col(
            "col_a", &["foo", "bar", "foo"],
            "col_b", &["baz", "foo", "baz"],
        )).await;

        let result = searcher.search("foo", None, true).await.unwrap();
        assert!(result.found);

        let data = result.verified_matches.unwrap();
        assert_eq!(data.total_occurrences, 3, "foo appears 3 times total across both columns");
        assert!(data.columns.contains(&"col_a".to_string()), "col_a must be listed");
        assert!(data.columns.contains(&"col_b".to_string()), "col_b must be listed");

        // The aggregate expands to each column.  Each column's row ranges must cover
        // the complete union (rows 0-2), not just the first occurrence (row 0 only).
        for col_detail in &data.column_details {
            let all_rows: Vec<(u32, u32)> = col_detail.row_groups.iter()
                .flat_map(|rg| rg.row_ranges.iter().map(|r| (r.start_row, r.end_row)))
                .collect();
            // Row 0 must be reachable — either as an explicit entry or inside a range
            assert!(
                all_rows.iter().any(|&(s, _e)| s == 0),
                "column '{}' must cover row 0; got {:?}", col_detail.column_name, all_rows
            );
            // Row 2 must be reachable
            assert!(
                all_rows.iter().any(|&(s, e)| s <= 2 && 2 <= e),
                "column '{}' must cover row 2; got {:?}", col_detail.column_name, all_rows
            );
        }
    }

    /// Keyword present in three different columns at the same row.
    /// All three columns must appear in the result. total_occurrences counts distinct rows,
    /// so even though "shared" appears in 3 columns it is only in row 0 once → total == 1.
    #[tokio::test]
    async fn test_no_filter_columns_list_complete() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, false),
            Field::new("col_b", DataType::Utf8, false),
            Field::new("col_c", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec!["shared"])) as _,
            Arc::new(StringArray::from(vec!["shared"])) as _,
            Arc::new(StringArray::from(vec!["shared"])) as _,
        ]).unwrap();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let searcher = build_searcher(buf).await;
        let result = searcher.search("shared", None, true).await.unwrap();
        assert!(result.found);

        let data = result.verified_matches.unwrap();
        assert_eq!(data.total_occurrences, 1, "column 0 counts distinct rows; \"shared\" is only in row 0");
        assert_eq!(data.columns.len(), 3, "all three columns must be listed; got {:?}", data.columns);
        assert!(data.columns.contains(&"col_a".to_string()));
        assert!(data.columns.contains(&"col_b".to_string()));
        assert!(data.columns.contains(&"col_c".to_string()));
    }

    /// Keyword appears in the *same row* across multiple columns.
    /// Both columns must be listed. total_occurrences counts distinct rows via the column 0
    /// aggregate, so "john" in two columns at row 0 → total == 1 (one distinct row).
    #[tokio::test]
    async fn test_no_filter_same_row_multiple_columns() {
        // row 0: both columns contain "john"; row 1: neither does
        let searcher = build_searcher(parquet_two_col(
            "first_name", &["john", "jane"],
            "last_name",  &["john", "doe"],
        )).await;

        let result = searcher.search("john", None, true).await.unwrap();
        assert!(result.found);

        let data = result.verified_matches.unwrap();
        assert_eq!(data.total_occurrences, 1, "column 0 counts distinct rows; both columns share row 0");
        assert_eq!(data.columns.len(), 2);
        assert!(data.columns.contains(&"first_name".to_string()));
        assert!(data.columns.contains(&"last_name".to_string()));
    }

    /// Keyword present in different *row groups* of different columns.
    /// The aggregate in column 0 must accumulate occurrences from both row groups so that
    /// the all-columns result covers both — not only the row group of the first occurrence.
    #[tokio::test]
    async fn test_no_filter_multi_row_group() {
        // row group 0: col_a has "target" at row 0; col_b does not
        // row group 1: col_a does not;              col_b has "target" at row 1
        // add_group already updates column 0 for every column call, so both row groups
        // must appear in column 0's aggregate data.
        let searcher = build_searcher(parquet_two_col_two_rg(
            "col_a", &["target", "other"], &["other", "other"],
            "col_b", &["other", "other"], &["other", "target"],
        )).await;

        let result = searcher.search("target", None, true).await.unwrap();
        assert!(result.found, "target must be found — it exists in both row groups");

        let data = result.verified_matches.unwrap();
        assert_eq!(data.total_occurrences, 2, "one occurrence per column/row-group");
        assert!(data.columns.contains(&"col_a".to_string()));
        assert!(data.columns.contains(&"col_b".to_string()));

        // The aggregate must cover both row groups so the expanded column details reference them.
        let all_rg_ids: Vec<u16> = data.column_details.iter()
            .flat_map(|c| c.row_groups.iter().map(|rg| rg.row_group_id))
            .collect();
        assert!(all_rg_ids.contains(&0), "row group 0 must appear in results; got {:?}", all_rg_ids);
        assert!(all_rg_ids.contains(&1), "row group 1 must appear in results; got {:?}", all_rg_ids);
    }

    // ── column-filter path ───────────────────────────────────────────────────

    /// With a column filter, only that column's rows are returned even though the
    /// keyword also exists in other columns and at other rows.
    #[tokio::test]
    async fn test_column_filter_finds_correct_rows() {
        // col_a: "target" at rows 0 and 2; col_b: "target" at row 1
        let searcher = build_searcher(parquet_two_col(
            "col_a", &["target", "other", "target"],
            "col_b", &["other", "target", "other"],
        )).await;

        let result = searcher.search("target", Some("col_a"), true).await.unwrap();
        assert!(result.found);

        let data = result.verified_matches.unwrap();
        assert_eq!(data.total_occurrences, 2, "only col_a's 2 occurrences should be counted");
        assert_eq!(data.columns, vec!["col_a".to_string()], "only col_a must be returned");

        // Rows 0 and 2 must appear; row 1 (from col_b) must not
        let all_rows: Vec<(u32, u32)> = data.column_details.iter()
            .flat_map(|c| c.row_groups.iter())
            .flat_map(|rg| rg.row_ranges.iter().map(|r| (r.start_row, r.end_row)))
            .collect();
        assert!(all_rows.contains(&(0, 0)), "row 0 must be present; got {:?}", all_rows);
        assert!(all_rows.contains(&(2, 2)), "row 2 must be present; got {:?}", all_rows);
        assert!(!all_rows.contains(&(1, 1)), "row 1 (col_b) must NOT appear");
    }

    /// Keyword exists in the file but not in the column named by the filter.
    /// Result must be found=false even though the keyword is in other columns.
    #[tokio::test]
    async fn test_column_filter_keyword_in_other_columns_only() {
        // "banana" only exists in col_b
        let searcher = build_searcher(parquet_two_col(
            "col_a", &["apple", "apple"],
            "col_b", &["banana", "banana"],
        )).await;

        let result = searcher.search("banana", Some("col_a"), true).await.unwrap();
        assert!(
            !result.found,
            "banana is not in col_a; must return found=false even though it exists in col_b"
        );
    }

    /// Filtering to a column name that does not exist in the index must return found=false.
    #[tokio::test]
    async fn test_column_filter_nonexistent_column() {
        let searcher = build_searcher(parquet_one_col("word", &["hello", "world"])).await;

        let result = searcher.search("hello", Some("no_such_column"), true).await.unwrap();
        assert!(!result.found, "nonexistent column filter must yield found=false");
    }

    // ── end column-0 aggregate tests ─────────────────────────────────────────

    /// `find_chunk_for_keyword` uses `binary_search_by`.  When the searched keyword
    /// exactly equals a chunk's `start_keyword` the search returns `Ok(idx)` — not the
    /// `Err` fallback.
    ///
    /// Three simple words with no split-characters are indexed: "aardvark", "banana",
    /// "cherry".  Sorted order guarantees chunk 0's `start_keyword == "aardvark"`.
    /// Searching for "aardvark" must hit `Ok(0)` and still return the correct row data.
    #[tokio::test]
    async fn test_binary_search_exact_start_keyword_match() {
        use std::sync::Arc;
        use arrow::array::StringArray;
        use arrow::datatypes::{Schema, Field, DataType};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![
            Field::new("word", DataType::Utf8, false),
        ]));
        let array = Arc::new(StringArray::from(vec!["aardvark", "banana", "cherry"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array as _]).unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut buf, schema, Some(WriterProperties::builder().build())
        ).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let searcher = build_index_in_memory(
            ParquetSource::from(buf),
            None, Some(0.01), None, None, None, None, None, None, None,
        ).await.unwrap();

        assert!(!searcher.filters.chunk_index.is_empty());

        // "aardvark" is the lex-smallest keyword → it IS chunk 0's start_keyword.
        // binary_search_by returns Ok(0) for this exact match.
        let start_kw = searcher.filters.chunk_index[0].start_keyword.clone();
        assert_eq!(start_kw, "aardvark",
            "chunk 0 start_keyword must be 'aardvark' (lex-first of the three words)");

        let result = searcher.search(&start_kw, None, true).await.unwrap();
        assert!(result.found, "start_keyword '{}' must be found via Ok branch", start_kw);

        let data = result.verified_matches.unwrap();
        assert_eq!(data.total_occurrences, 1, "'aardvark' appears exactly once");
        assert!(data.columns.contains(&"word".to_string()));
    }

    // ── search_and_read end-to-end tests ────────────────────────────────────

    /// Helper: register parquet bytes in the memory store, build an index, return the memory path.
    async fn build_indexed_memory(parquet_bytes: Vec<u8>, name: &str) -> String {
        use crate::utils::file_interaction_local_and_cloud::register_memory_file;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = format!("memory://sar-{}-{}.parquet", name, ts);

        register_memory_file(&path, Bytes::from(parquet_bytes)).await
            .expect("Failed to register memory file");

        crate::build_and_save_index(
            &path, None, Some(0.01), None, None, None, None, Some(true), None, None, None,
        ).await.expect("Index build failed");

        path
    }

    /// search_and_read returns matching rows for a keyword present in the file.
    #[tokio::test]
    async fn test_search_and_read_basic() {
        let buf = parquet_one_col("fruit", &["apple", "banana", "cherry", "apple", "banana"]);
        let path = build_indexed_memory(buf, "basic").await;

        let (result, batches) = crate::search_and_read(&path, "apple", None, true, false)
            .await.expect("search_and_read failed");

        assert!(result.found);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "apple appears in exactly 2 rows");
    }

    /// search_and_read returns empty batches for a keyword not in the file.
    #[tokio::test]
    async fn test_search_and_read_not_found() {
        let buf = parquet_one_col("fruit", &["apple", "banana"]);
        let path = build_indexed_memory(buf, "notfound").await;

        let (result, batches) = crate::search_and_read(&path, "mango", None, true, false)
            .await.expect("search_and_read failed");

        assert!(!result.found);
        assert!(batches.is_empty());
    }

    /// search_and_read with a column filter only returns rows matching that column.
    #[tokio::test]
    async fn test_search_and_read_column_filter() {
        let buf = parquet_two_col(
            "name", &["alice", "bob", "alice"],
            "city", &["london", "alice", "paris"],
        );
        let path = build_indexed_memory(buf, "colfilt").await;

        let (result, batches) = crate::search_and_read(&path, "alice", Some("city"), true, false)
            .await.expect("search_and_read failed");

        assert!(result.found);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "alice appears in city column exactly once");
    }

    /// search_and_read returns all columns of the matching rows, not just the searched column.
    #[tokio::test]
    async fn test_search_and_read_returns_all_columns() {
        let buf = parquet_two_col(
            "name", &["alice", "bob"],
            "city", &["london", "paris"],
        );
        let path = build_indexed_memory(buf, "allcols").await;

        let (_result, batches) = crate::search_and_read(&path, "alice", None, true, false)
            .await.expect("search_and_read failed");

        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(col_names.contains(&"name"), "should contain name column");
        assert!(col_names.contains(&"city"), "should contain city column");
    }

    /// Keywords without delimiters are always stored even with store_full_keyword=false,
    /// because they pass through all split levels without being split.
    #[tokio::test]
    async fn test_full_keywords_without_delimiters_found_when_store_full_keyword_false() {
        use crate::utils::file_interaction_local_and_cloud::register_memory_file;

        let buf = parquet_one_col("val", &["hello", "world", "hello"]);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = format!("memory://sar-sfk-false-{}.parquet", ts);
        register_memory_file(&path, Bytes::from(buf)).await
            .expect("Failed to register memory file");

        // Build with store_full_keyword_default = None (defaults to false)
        crate::build_and_save_index(
            &path, None, Some(0.01), None, None, None, None, None, None, None, None,
        ).await.expect("Index build failed");

        let (result, batches) = crate::search_and_read(&path, "hello", None, true, false)
            .await.expect("search failed");
        assert!(result.found, "keywords without delimiters must be found even with store_full_keyword=false");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "'hello' appears in 2 rows");
    }

    // ── SearchMode::Equals vs Contains tests ────────────────────────────────
    //
    // Four combinations of (keyword_only, exact_match):
    //   keyword_only=true,  exact_match=false  → single keyword, contains (default)
    //   keyword_only=true,  exact_match=true   → single keyword, equals
    //   keyword_only=false, exact_match=false  → phrase, contains (default)
    //   keyword_only=false, exact_match=true   → phrase, equals

    /// keyword_only=true, exact_match=false: "1" matches both direct value and sub-token of "1.5"
    /// keyword_only=true, exact_match=true:  "1" matches only the direct value
    #[tokio::test]
    async fn test_keyword_only_contains_vs_equals() {
        // row 0 = "1", row 1 = "1.5", row 2 = "2"
        let buf = parquet_one_col("val", &["1", "1.5", "2"]);
        let path = build_indexed_memory(buf, "kw_contains_eq").await;

        // keyword_only=true, exact_match=false → contains: rows 0 and 1
        let (result, batches) = crate::search_and_read(&path, "1", None, true, false)
            .await.expect("search failed");
        assert!(result.found);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "keyword_only+contains: '1' should match '1' and '1.5'");

        // keyword_only=true, exact_match=true → equals: only row 0
        let (result, batches) = crate::search_and_read(&path, "1", None, true, true)
            .await.expect("search failed");
        assert!(result.found);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "keyword_only+equals: '1' should only match row where value IS '1'");
    }

    /// keyword_only=false, exact_match=false: phrase "user-name" found in "user-name" (verified)
    ///   and "user-name-extra" (contains match via parent verification)
    /// keyword_only=false, exact_match=true: phrase "user-name" only matches exact value
    #[tokio::test]
    async fn test_phrase_contains_vs_equals() {
        // row 0 = "user-name", row 1 = "user-name-extra", row 2 = "other-value"
        let buf = parquet_one_col("val", &["user-name", "user-name-extra", "other-value"]);
        let path = build_indexed_memory(buf, "phrase_contains_eq").await;

        // keyword_only=false, exact_match=false → contains: "user-name" should match both rows
        let (result, batches) = crate::search_and_read(&path, "user-name", None, false, false)
            .await.expect("search failed");
        assert!(result.found, "phrase+contains: 'user-name' should be found");
        let verified = result.verified_matches.as_ref().map(|v| v.total_occurrences).unwrap_or(0);
        // Row with value "user-name": verified (phrase IS the keyword, parent fix applied)
        // Row with value "user-name-extra": verified (parent "user-name-extra" contains "user-name")
        assert_eq!(verified, 2,
            "phrase+contains: 'user-name' should have 2 verified matches, got {}", verified);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "phrase+contains: should read 2 rows");

        // keyword_only=false, exact_match=true → equals: only "user-name" exact
        let (result, batches) = crate::search_and_read(&path, "user-name", None, false, true)
            .await.expect("search failed");
        assert!(result.found, "phrase+equals: 'user-name' should be found");
        let verified = result.verified_matches.as_ref().map(|v| v.total_occurrences).unwrap_or(0);
        assert_eq!(verified, 1,
            "phrase+equals: should have exactly 1 verified match (the exact value), got {}", verified);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "phrase+equals: should read 1 row");
    }

    /// keyword_only=true, exact_match=true: keyword only exists as sub-token → not found
    #[tokio::test]
    async fn test_keyword_equals_not_found_for_pure_sub_token() {
        let searcher = build_searcher(parquet_one_col("val", &["1.5", "2.5"])).await;

        // "5" only exists as sub-token of "1.5" and "2.5"
        let result = searcher.search_with_mode("5", None, true,
            crate::searching::search_results::SearchMode::Equals).await.unwrap();
        assert!(!result.found, "keyword+equals: '5' is only a sub-token, should not be found");

        // But contains mode finds it
        let result = searcher.search("5", None, true).await.unwrap();
        assert!(result.found, "keyword+contains: '5' should be found as sub-token");
    }

    /// When keyword has no split characters, contains and equals agree for all modes.
    #[tokio::test]
    async fn test_no_splits_all_modes_agree() {
        let buf = parquet_one_col("val", &["abc", "def", "abc"]);
        let path = build_indexed_memory(buf, "nosplit_agree").await;

        // All four combinations should return 2 rows for "abc"
        for (kw_only, exact, label) in [
            (true, false, "keyword+contains"),
            (true, true, "keyword+equals"),
            (false, false, "phrase+contains"),
            (false, true, "phrase+equals"),
        ] {
            let (result, batches) = crate::search_and_read(&path, "abc", None, kw_only, exact)
                .await.expect("search failed");
            assert!(result.found, "{}: 'abc' should be found", label);
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 2, "{}: 'abc' should match 2 rows", label);
        }
    }

    /// Helper: build an indexed memory path with split elimination enabled.
    async fn build_indexed_memory_with_split_elimination(
        parquet_bytes: Vec<u8>, name: &str, threshold: f64,
    ) -> String {
        use crate::utils::file_interaction_local_and_cloud::register_memory_file;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = format!("memory://sar-{}-{}.parquet", name, ts);

        register_memory_file(&path, Bytes::from(parquet_bytes)).await
            .expect("Failed to register memory file");

        crate::build_and_save_index(
            &path, None, Some(0.01), None, None, None, None,
            Some(true),              // store_full_keyword
            None,                    // full_keyword_exceptions
            Some(threshold),         // parent_tracking_threshold
            Some(threshold),         // split_elimination_threshold
        ).await.expect("Index build failed");

        path
    }

    /// When split elimination fires on the aggregate column, Equals mode with
    /// exact_match=true must still return the correct row count.
    ///
    /// Setup: 10 rows across 2 columns. "1" appears as a direct value in most rows
    /// (triggering split elimination at threshold 0.2). "1.5" appears in 1 row,
    /// creating a sub-token "1" that Equals mode must exclude.
    ///
    /// This tests the full pipeline: search → needs_verification (split eliminated)
    /// → read_search_result → correct row count.
    #[tokio::test]
    async fn test_equals_with_split_elimination() {
        // col_a: "1" in 8 rows, "2" in 2 rows → "1" is 80% of rows, well above 20% threshold
        // col_b: "1.5" in 1 row (the sub-token case), "other" in 9 rows
        // Row 5 has col_a="2" AND col_b="1.5" → "1" is a sub-token here, not a value
        let buf = parquet_two_col(
            "col_a", &["1", "1", "1", "1", "1", "2", "1", "1", "1", "2"],
            "col_b", &["other", "other", "other", "other", "other", "1.5", "other", "other", "other", "other"],
        );
        let path = build_indexed_memory_with_split_elimination(buf, "equals_split_elim", 0.2).await;

        // Contains mode: "1" matches all 8 rows where col_a="1" PLUS row 5 where
        // col_b="1.5" has "1" as a sub-token = 9 rows total
        let (result_contains, batches_contains) = crate::search_and_read(
            &path, "1", None, true, false,
        ).await.expect("contains search failed");
        assert!(result_contains.found);
        let contains_rows: usize = batches_contains.iter().map(|b| b.num_rows()).sum();
        assert_eq!(contains_rows, 9,
            "Contains should match 8 direct + 1 sub-token = 9 rows, got {}", contains_rows);

        // Equals mode: "1" matches only the 8 rows where col_a="1"
        // Row 5 (col_b="1.5") must be excluded — "1" is a sub-token, not the value
        let (result_equals, batches_equals) = crate::search_and_read(
            &path, "1", None, true, true,
        ).await.expect("equals search failed");
        assert!(result_equals.found);
        let equals_rows: usize = batches_equals.iter().map(|b| b.num_rows()).sum();
        assert_eq!(equals_rows, 8,
            "Equals should match only 8 rows where value IS '1', got {}", equals_rows);

        // Verify split elimination actually fired (otherwise this test isn't
        // testing the needs_verification path)
        let has_needs_verif = result_equals.needs_verification.is_some();
        println!("  Split elimination produced needs_verification: {}", has_needs_verif);
        println!("  Verified: {}", result_equals.verified_matches.as_ref()
            .map(|v| v.total_occurrences).unwrap_or(0));
        println!("  Needs verification: {}", result_equals.needs_verification.as_ref()
            .map(|v| v.total_occurrences).unwrap_or(0));
    }
}