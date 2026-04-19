//! Tests for `combine_and` / `combine_or` when split elimination has forced
//! one or more input `SearchResult`s to carry `needs_verification` rows
//! instead of (or alongside) `verified_matches`.
//!
//! Before the fix in this session, `combine_and` and `combine_or` silently
//! dropped `needs_verification` data — producing fewer rows than the SQL
//! equivalent. These tests exercise the correctness path end-to-end:
//! build an in-memory parquet with known data, force split elimination via
//! `split_elimination_threshold`, run per-column searches in `Equals` mode,
//! combine them, read matching rows, and assert row identity against a
//! manually-computed expected set.
//!
//! All tests use `ParquetSource::Bytes` registered into the in-memory
//! object store — no temp files on disk. Row IDs are returned via the
//! `id` column, which is a monotonically increasing row index that makes
//! it trivial to compare against the expected row set.

#[cfg(test)]
mod combined_verification_tests {
    use std::sync::Arc;
    use std::collections::HashSet;

    use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use crate::build_and_save_index;
    use crate::searching::keyword_search::KeywordSearcher;
    use crate::searching::pruned_reader::PrunedParquetReader;
    use crate::searching::search_results::SearchMode;
    use crate::utils::file_interaction_local_and_cloud::register_memory_file;

    /// Build a parquet with the given column data and register it under a
    /// unique `memory://` path. Returns `(path, row_count)`.
    async fn build_memory_parquet(
        test_name: &str,
        col_a: Vec<String>,
        col_b: Vec<String>,
    ) -> (String, usize) {
        assert_eq!(col_a.len(), col_b.len(), "col_a and col_b must be equal length");
        let n = col_a.len();
        let ids: Vec<i32> = (0..n as i32).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("col_a", DataType::Utf8, false),
            Field::new("col_b", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(col_a)) as ArrayRef,
                Arc::new(StringArray::from(col_b)) as ArrayRef,
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        // Unique per-test path to avoid parallel-test interference in the
        // global MEMORY_STORE.
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = format!("memory://combined-verif-{}-{}.parquet", test_name, ts);
        register_memory_file(&path, Bytes::from(buffer)).await.unwrap();
        (path, n)
    }

    /// Extract the `id` column from returned batches as a sorted set of i32s.
    fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
        let mut out = Vec::new();
        for batch in batches {
            let idx = batch.schema().index_of("id").unwrap();
            let ids = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..ids.len() {
                out.push(ids.value(i));
            }
        }
        out.sort_unstable();
        out
    }

    /// Build an index with an aggressive split-elimination threshold so that
    /// any keyword with > `threshold * n_rows` row occurrences has its split
    /// + parent info cleared.
    async fn build_index(path: &str, split_elim: f64) -> KeywordSearcher {
        build_and_save_index(
            path,
            None,
            Some(0.01),
            None,
            None,
            None,
            None,
            Some(true),
            None,
            Some(split_elim),
            Some(split_elim),
        )
        .await
        .unwrap();
        KeywordSearcher::load(path, None).await.unwrap()
    }

    fn reader_for(searcher: &KeywordSearcher, path: &str) -> PrunedParquetReader {
        PrunedParquetReader::from_path(path).with_metadata_cache(
            searcher.filters.parquet_metadata_offset,
            searcher.filters.parquet_metadata_length,
        )
    }

    // =========================================================================
    // Test 1: combine_or with split-elimination on ONE side
    // =========================================================================
    //
    // col_a has "FREQ_A" at every even row (50 rows) — triggers split-elim at
    //   split_elim_threshold=0.1 (threshold = 10 Row objects, 50 > 10).
    // col_b has "RARE_B" at only 5 specific rows — does NOT trigger split-elim.
    //
    // After the fix: combine_or must include rows matched by col_a via the
    //   needs_verification path. Before the fix those rows were dropped.
    #[tokio::test]
    async fn combine_or_one_term_split_eliminated() {
        let n = 100;
        let mut col_a = Vec::with_capacity(n);
        let mut col_b = Vec::with_capacity(n);
        let rare_b_rows: HashSet<i32> = [7, 19, 42, 71, 88].iter().copied().collect();
        for i in 0..n as i32 {
            // Even rows get "FREQ_A"; odd rows get a per-row unique filler
            // so "FREQ_A" occurrences can't RLE-merge into one Row object.
            if i % 2 == 0 {
                col_a.push("FREQ_A".to_string());
            } else {
                col_a.push(format!("fill_a_{}", i));
            }
            if rare_b_rows.contains(&i) {
                col_b.push("RARE_B".to_string());
            } else {
                col_b.push(format!("fill_b_{}", i));
            }
        }

        let (path, _) = build_memory_parquet("or_one_split", col_a, col_b).await;
        let searcher = build_index(&path, 0.1).await;

        let r_a = searcher
            .search_with_mode("FREQ_A", Some("col_a"), false, SearchMode::Equals)
            .await
            .unwrap();
        let r_b = searcher
            .search_with_mode("RARE_B", Some("col_b"), false, SearchMode::Equals)
            .await
            .unwrap();

        assert!(r_a.found, "FREQ_A must be found");
        assert!(r_b.found, "RARE_B must be found");
        assert!(
            r_a.needs_verification.is_some(),
            "FREQ_A should have triggered split-elim on col_a"
        );
        assert!(
            r_b.verified_matches.is_some() && r_b.needs_verification.is_none(),
            "RARE_B should be fully verified (below split-elim threshold)"
        );

        let combined = KeywordSearcher::combine_or(&[r_a, r_b]).unwrap();
        let batches = reader_for(&searcher, &path)
            .read_combined_rows(&combined, None)
            .await
            .unwrap();
        let got_ids = collect_ids(&batches);

        // Expected: union of {even rows} and {7, 19, 42, 71, 88}.
        let mut expected: HashSet<i32> = (0..n as i32).filter(|i| i % 2 == 0).collect();
        expected.extend(rare_b_rows.iter());
        let mut expected_sorted: Vec<i32> = expected.into_iter().collect();
        expected_sorted.sort_unstable();

        assert_eq!(
            got_ids, expected_sorted,
            "combine_or must include needs_verification rows from r_a AND verified rows from r_b"
        );
    }

    // =========================================================================
    // Test 2: combine_and with split-elimination on BOTH sides
    // =========================================================================
    //
    // Both col_a and col_b have frequent keywords that trigger split-elim.
    // Combined AND must intersect the two verified sets correctly — which
    // requires both pending sets to survive into the reader and both
    // predicates to be evaluated per row.
    #[tokio::test]
    async fn combine_and_both_split_eliminated() {
        let n = 120;
        let mut col_a = Vec::with_capacity(n);
        let mut col_b = Vec::with_capacity(n);

        // col_a: "FREQ_A" on multiples of 3; non-consecutive to avoid RLE.
        // col_b: "FREQ_B" on multiples of 4; non-consecutive to avoid RLE.
        for i in 0..n as i32 {
            if i % 3 == 0 {
                col_a.push("FREQ_A".to_string());
            } else {
                col_a.push(format!("fill_a_{}", i));
            }
            if i % 4 == 0 {
                col_b.push("FREQ_B".to_string());
            } else {
                col_b.push(format!("fill_b_{}", i));
            }
        }

        let (path, _) = build_memory_parquet("and_both_split", col_a, col_b).await;
        let searcher = build_index(&path, 0.1).await;

        let r_a = searcher
            .search_with_mode("FREQ_A", Some("col_a"), false, SearchMode::Equals)
            .await
            .unwrap();
        let r_b = searcher
            .search_with_mode("FREQ_B", Some("col_b"), false, SearchMode::Equals)
            .await
            .unwrap();

        assert!(
            r_a.needs_verification.is_some() && r_b.needs_verification.is_some(),
            "Both terms must have been split-eliminated for this test to be meaningful"
        );

        let combined = KeywordSearcher::combine_and(&[r_a, r_b]).unwrap();
        let batches = reader_for(&searcher, &path)
            .read_combined_rows(&combined, None)
            .await
            .unwrap();
        let got_ids = collect_ids(&batches);

        // Expected: rows that are BOTH multiples of 3 AND multiples of 4 → multiples of 12.
        let expected: Vec<i32> = (0..n as i32).filter(|i| i % 3 == 0 && i % 4 == 0).collect();

        assert_eq!(
            got_ids, expected,
            "combine_and must intersect per-term (verified ∪ pending-that-passes) sets correctly"
        );
    }

    // =========================================================================
    // Test 3: pending rows whose actual column value does NOT match the query
    // must be excluded
    // =========================================================================
    //
    // This is the exact regression the correctness fix addresses. col_a
    // contains three kinds of value:
    //   - "FREQ_A" (true matches for Equals("FREQ_A"))
    //   - "FREQ_A-suffix" (false positives: the index sees "FREQ_A" as a
    //     sub-token from hyphen splitting, but the column value is NOT "FREQ_A")
    //   - filler unique values (not indexed for FREQ_A at all)
    //
    // split_elim forces both true matches and false positives into the same
    // needs_verification bucket. The reader must evaluate the predicate against
    // the actual column value and keep only the true matches.
    #[tokio::test]
    async fn combine_or_excludes_pending_rows_that_fail_predicate() {
        let n = 100;
        let mut col_a = Vec::with_capacity(n);
        let mut col_b = Vec::with_capacity(n);

        // Rows 0, 4, 8, ... (multiples of 4) → true "FREQ_A" match. 25 rows.
        // Rows 1, 5, 9, ... (mod 4 == 1) → "FREQ_A-suffix" false positive. 25 rows.
        // Rest → filler.
        let mut true_match_ids: Vec<i32> = Vec::new();
        for i in 0..n as i32 {
            match i % 4 {
                0 => {
                    col_a.push("FREQ_A".to_string());
                    true_match_ids.push(i);
                }
                1 => col_a.push("FREQ_A-suffix".to_string()),
                _ => col_a.push(format!("fill_a_{}", i)),
            }
            col_b.push(format!("fill_b_{}", i));
        }

        let (path, _) = build_memory_parquet("excludes_false_positives", col_a, col_b).await;
        let searcher = build_index(&path, 0.1).await;

        let r_a = searcher
            .search_with_mode("FREQ_A", Some("col_a"), false, SearchMode::Equals)
            .await
            .unwrap();
        assert!(
            r_a.needs_verification.is_some(),
            "split-elim must have fired for FREQ_A (mix of root + sub-token occurrences \
             produces enough Row objects to exceed the threshold)"
        );

        // combine_or of just this one term still routes through the combined
        // pipeline and must apply verification correctly.
        let combined = KeywordSearcher::combine_or(&[r_a]).unwrap();
        let batches = reader_for(&searcher, &path)
            .read_combined_rows(&combined, None)
            .await
            .unwrap();
        let got_ids = collect_ids(&batches);

        assert_eq!(
            got_ids, true_match_ids,
            "Reader must drop false-positive rows where col_a is \"FREQ_A-suffix\" \
             — they have FREQ_A as a sub-token in the index but don't equal the query"
        );
        assert_eq!(got_ids.len(), 25, "Expected exactly 25 true FREQ_A matches");
    }

    // =========================================================================
    // Test 4: combine_or mixing a verified-only term with a split-eliminated term
    // =========================================================================
    //
    // Covers the hybrid case: one term's rows all go to verified_matches, the
    // other's all go to needs_verification. The reader must stitch both into
    // a single union without dropping either.
    #[tokio::test]
    async fn combine_or_mixed_verified_and_pending() {
        let n = 80;
        let mut col_a = Vec::with_capacity(n);
        let mut col_b = Vec::with_capacity(n);

        // col_a: "FREQ_A" on odd rows only (40 rows, non-consecutive → split-elim).
        // col_b: "RARE_B" on rows {5, 25, 60} only (3 rows, below threshold → verified).
        let rare_b_rows: HashSet<i32> = [5i32, 25, 60].iter().copied().collect();
        for i in 0..n as i32 {
            if i % 2 == 1 {
                col_a.push("FREQ_A".to_string());
            } else {
                col_a.push(format!("fill_a_{}", i));
            }
            if rare_b_rows.contains(&i) {
                col_b.push("RARE_B".to_string());
            } else {
                col_b.push(format!("fill_b_{}", i));
            }
        }

        let (path, _) = build_memory_parquet("mixed_verif_pending", col_a, col_b).await;
        let searcher = build_index(&path, 0.1).await;

        let r_a = searcher
            .search_with_mode("FREQ_A", Some("col_a"), false, SearchMode::Equals)
            .await
            .unwrap();
        let r_b = searcher
            .search_with_mode("RARE_B", Some("col_b"), false, SearchMode::Equals)
            .await
            .unwrap();

        assert!(
            r_a.needs_verification.is_some() && r_a.verified_matches.is_none(),
            "FREQ_A should be fully in needs_verification after split-elim"
        );
        assert!(
            r_b.verified_matches.is_some() && r_b.needs_verification.is_none(),
            "RARE_B should be fully verified"
        );

        let combined = KeywordSearcher::combine_or(&[r_a, r_b]).unwrap();
        let batches = reader_for(&searcher, &path)
            .read_combined_rows(&combined, None)
            .await
            .unwrap();
        let got_ids = collect_ids(&batches);

        let mut expected: HashSet<i32> = (0..n as i32).filter(|i| i % 2 == 1).collect();
        expected.extend(rare_b_rows.iter());
        let mut expected_sorted: Vec<i32> = expected.into_iter().collect();
        expected_sorted.sort_unstable();

        assert_eq!(
            got_ids, expected_sorted,
            "combine_or must union verified rows (from r_b) with pending-that-passes rows (from r_a)"
        );
    }
}
