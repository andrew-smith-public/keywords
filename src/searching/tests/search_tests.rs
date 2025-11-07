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
    use crate::searching::keyword_search::KeywordSearcher;

    static TEST_SEARCHER: OnceCell<KeywordSearcher> = OnceCell::const_new();

    /// Generate a small test parquet file with 500 distinct values, 1000 rows
    fn create_test_parquet() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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

        Ok(buffer)
    }

    async fn get_searcher() -> &'static KeywordSearcher {
        TEST_SEARCHER.get_or_init(|| async {
            println!("Building test index in memory...");
            let parquet_bytes = create_test_parquet().expect("Failed to create test parquet");
            build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None)
                .await
                .expect("Build Index Failed")
        }).await
    }

    #[tokio::test]
    async fn test_load_and_search() {
        let searcher = get_searcher().await;

        // Search for keywords that exist in the generated data
        let result = searcher.search("user_0", None, true).unwrap();

        println!("Search result for 'user_0': {:?}", result);

        if result.found {
            println!("Found 'user_0' in columns: {:?}", result.verified_matches.as_ref().unwrap().columns);
            println!("Total occurrences: {}", result.verified_matches.as_ref().unwrap().total_occurrences);
        }
    }

    #[tokio::test]
    async fn test_global_filter_rejection() {
        let searcher = get_searcher().await;

        let result = searcher.search("xyzabc123definitely_not_in_file", None, true).unwrap();

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
        let result = searcher.search("test0.com", None, false).unwrap();

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
}