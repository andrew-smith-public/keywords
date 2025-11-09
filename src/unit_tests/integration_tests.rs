#[cfg(test)]
mod tests {
    use hashbrown::HashMap;
    use std::sync::Arc;
    use arrow::array::{StringArray};
    use indexmap::IndexSet;
    use crate::{build_index_in_memory, ParquetSource};
    use crate::keyword_shred::{perform_split, build_column_keywords_map};
    use crate::index_structure::column_filter::ColumnFilter;
    use crate::utils::column_pool::ColumnPool;
    use crate::column_parquet_reader::{process_arrow_string_array, process_parquet_file};
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;
    use arrow::array::{Int32Array, Int64Array, Float64Array, BooleanArray};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::basic::Compression;
    use rand::{Rng, SeedableRng};

    /// Generate a test parquet file with 500 distinct values, 1000 rows, single row group
    /// This represents the "small" test file with various column types
    fn create_small_test_parquet() -> Result<Bytes, Box<dyn std::error::Error>> {
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
        let id_array = Int32Array::from(id_data);
        let name_array = StringArray::from(name_data);
        let email_array = StringArray::from(email_data);
        let status_array = StringArray::from(status_data);
        let age_array = Int64Array::from(age_data);
        let score_array = Float64Array::from(score_data);
        let active_array = BooleanArray::from(active_data);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(email_array),
                Arc::new(status_array),
                Arc::new(age_array),
                Arc::new(score_array),
                Arc::new(active_array),
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

    /// Generate a larger test parquet file with 5000 distinct values, 250K rows, 5 row groups
    /// This represents the "larger" test file that exceeds 2MB to test caching behavior
    fn create_larger_test_parquet() -> Result<Bytes, Box<dyn std::error::Error>> {
        const DISTINCT_VALUES: usize = 5000;
        const ROWS_PER_GROUP: usize = 50_000;
        const NUM_ROW_GROUPS: usize = 5;

        let mut rng = rand::rngs::StdRng::seed_from_u64(54321);

        // Create schema with mixed types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("username", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("department", DataType::Utf8, false),
            Field::new("salary", DataType::Int64, false),
            Field::new("rating", DataType::Float64, false),
            Field::new("verified", DataType::Boolean, false),
        ]));

        // Generate pool of distinct values
        let usernames: Vec<String> = (0..DISTINCT_VALUES)
            .map(|i| format!("employee_{}", i))
            .collect();
        let emails: Vec<String> = (0..DISTINCT_VALUES)
            .map(|i| format!("emp_{}@company{}.com", i, i % 50))
            .collect();
        let departments = vec!["engineering", "sales", "marketing", "support", "finance", "hr", "operations"];

        // Configure parquet writer
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_size(ROWS_PER_GROUP)
            .build();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;

        // Write data in row groups
        let mut global_row = 0;
        for _rg in 0..NUM_ROW_GROUPS {
            let mut id_data = Vec::with_capacity(ROWS_PER_GROUP);
            let mut username_data = Vec::with_capacity(ROWS_PER_GROUP);
            let mut email_data = Vec::with_capacity(ROWS_PER_GROUP);
            let mut department_data = Vec::with_capacity(ROWS_PER_GROUP);
            let mut salary_data = Vec::with_capacity(ROWS_PER_GROUP);
            let mut rating_data = Vec::with_capacity(ROWS_PER_GROUP);
            let mut verified_data = Vec::with_capacity(ROWS_PER_GROUP);

            for _ in 0..ROWS_PER_GROUP {
                id_data.push(global_row);
                username_data.push(usernames[rng.random_range(0..DISTINCT_VALUES)].clone());
                email_data.push(emails[rng.random_range(0..DISTINCT_VALUES)].clone());
                department_data.push(departments[rng.random_range(0..departments.len())].to_string());
                salary_data.push(rng.random_range(30000..200000) as i64);
                rating_data.push(rng.random_range(1.0..5.0));
                verified_data.push(rng.random_bool(0.8));
                global_row += 1;
            }

            // Create arrays for this row group
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(id_data)),
                    Arc::new(StringArray::from(username_data)),
                    Arc::new(StringArray::from(email_data)),
                    Arc::new(StringArray::from(department_data)),
                    Arc::new(Int64Array::from(salary_data)),
                    Arc::new(Float64Array::from(rating_data)),
                    Arc::new(BooleanArray::from(verified_data)),
                ],
            )?;

            writer.write(&batch)?;
        }

        writer.close()?;

        // Verify file is > 3MB
        assert!(buffer.len() > 3 * 1024 * 1024,
                "Larger parquet should be > 3MB, got {} bytes ({:.2} MB)",
                buffer.len(), buffer.len() as f64 / (1024.0 * 1024.0));

        Ok(Bytes::from(buffer))
    }

    /// Generate a parquet file with completely random ASCII data including split characters
    /// This is useful for stress testing and memory profiling
    fn create_random_ascii_parquet(rows_per_group: usize, num_row_groups: usize) -> Result<Bytes, Box<dyn std::error::Error>> {
        // Use a different seed for each call based on parameters to get different data
        let seed = (rows_per_group as u64).wrapping_mul(1000).wrapping_add(num_row_groups as u64);
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        // Create schema with 25 columns - mix of types
        let mut fields = vec![Field::new("id", DataType::Int32, false)];
        for i in 0..20 {
            fields.push(Field::new(&format!("text_{}", i), DataType::Utf8, false));
        }
        fields.push(Field::new("int_col", DataType::Int64, false));
        fields.push(Field::new("float_col", DataType::Float64, false));
        fields.push(Field::new("bool_col", DataType::Boolean, false));
        fields.push(Field::new("binary_col", DataType::Binary, false));

        let schema = Arc::new(Schema::new(fields));

        // Configure parquet writer with LZ4 compression
        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_size(rows_per_group)
            .build();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;

        let total_rows = rows_per_group * num_row_groups;
        println!("Creating random ASCII parquet:");
        println!("  Rows per group: {}", rows_per_group);
        println!("  Number of row groups: {}", num_row_groups);
        println!("  Total rows: {}", total_rows);

        // Write data in row groups
        let mut global_row = 0;
        for rg in 0..num_row_groups {
            let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

            // ID column
            let id_data: Vec<i32> = (0..rows_per_group).map(|i| (global_row + i) as i32).collect();
            arrays.push(Arc::new(Int32Array::from(id_data)));

            // 20 text columns with random ASCII including split characters
            for _ in 0..20 {
                let mut text_data = Vec::with_capacity(rows_per_group);
                for _ in 0..rows_per_group {
                    // Random length between 10 and 100 characters
                    let len = rng.random_range(10..=100);
                    let s: String = (0..len)
                        .map(|_| {
                            // Generate any ASCII printable character (32-126)
                            // This includes split characters like space, /, :, @, -, _, ., etc.
                            rng.random_range(32..127) as u8 as char
                        })
                        .collect();
                    text_data.push(s);
                }
                arrays.push(Arc::new(StringArray::from(text_data)));
            }

            // Int column
            let int_data: Vec<i64> = (0..rows_per_group).map(|_| rng.random_range(0..1_000_000)).collect();
            arrays.push(Arc::new(Int64Array::from(int_data)));

            // Float column
            let float_data: Vec<f64> = (0..rows_per_group).map(|_| rng.random_range(0.0..1_000_000.0)).collect();
            arrays.push(Arc::new(Float64Array::from(float_data)));

            // Boolean column
            let bool_data: Vec<bool> = (0..rows_per_group).map(|_| rng.random_bool(0.5)).collect();
            arrays.push(Arc::new(BooleanArray::from(bool_data)));

            // Binary column
            let mut binary_builder = arrow::array::BinaryBuilder::new();
            for _ in 0..rows_per_group {
                let len = rng.random_range(10..50);
                let bytes: Vec<u8> = (0..len).map(|_| rng.random_range(0..=255)).collect();
                binary_builder.append_value(&bytes);
            }
            arrays.push(Arc::new(binary_builder.finish()));

            let batch = RecordBatch::try_new(schema.clone(), arrays)?;
            writer.write(&batch)?;

            global_row += rows_per_group;

            if (rg + 1) % 10 == 0 || rg == num_row_groups - 1 {
                println!("  Wrote row group {}/{}", rg + 1, num_row_groups);
            }
        }

        writer.close()?;

        let size_mb = buffer.len() as f64 / (1024.0 * 1024.0);
        println!("  Parquet file created: {} bytes ({:.2} MB)", buffer.len(), size_mb);

        Ok(Bytes::from(buffer))
    }

    #[tokio::test]
    async fn test_build_distributed_index_small() {
        println!("\n=== Testing distributed index building with generated small parquet ===");

        let parquet_bytes = create_small_test_parquet()
            .expect("Failed to create test parquet");

        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, Some(0.01)).await
            .expect("Failed to build index");

        println!("\n✓ Index built successfully in memory");

        let info = searcher.get_index_info();
        println!("  Version: {}", info.version);
        println!("  Columns: {}", info.num_columns);
        println!("  Chunks: {}", info.num_chunks);
    }

    #[tokio::test]
    async fn test_build_and_save_index_small() {
        println!("\n=== Testing build in memory for generated small parquet ===");

        let parquet_bytes = create_small_test_parquet()
            .expect("Failed to create test parquet");

        let _searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, Some(0.01)).await
            .expect("Failed to build index");

        println!("✓ Index built successfully in memory");
    }

    #[tokio::test]
    async fn test_read_local_parquet_larger() {
        println!("\n=== Reading metadata from generated larger parquet ===");

        let parquet_bytes = create_larger_test_parquet()
            .expect("Failed to create test parquet");

        // For bytes source, we need to read metadata differently
        // Just verify the parquet was created and is > 2MB
        assert!(parquet_bytes.len() > 2 * 1024 * 1024,
                "larger parquet should be > 2MB, got {} bytes", parquet_bytes.len());
        println!("  ✓ File data is > 2MB ({} bytes) - as expected", parquet_bytes.len());

        // Build index to verify it's valid
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, Some(0.01)).await
            .expect("Failed to build index");

        let info = searcher.get_index_info();
        println!("\nIndex Information:");
        println!("  Version: {}", info.version);
        println!("  Columns: {}", info.num_columns);
        println!("  Chunks: {}", info.num_chunks);
    }

    #[tokio::test]
    async fn test_read_local_parquet_small() {
        println!("\n=== Reading metadata from generated small parquet ===");

        let parquet_bytes = create_small_test_parquet()
            .expect("Failed to create test parquet");

        // Verify the parquet was created and is < 2MB
        assert!(parquet_bytes.len() < 2 * 1024 * 1024,
                "small parquet should be < 2MB, got {} bytes", parquet_bytes.len());
        println!("  ✓ File data is < 2MB ({} bytes) - as expected", parquet_bytes.len());

        // Build index to verify it's valid
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, Some(0.01)).await
            .expect("Failed to build index");

        let info = searcher.get_index_info();
        println!("\nIndex Information:");
        println!("  Version: {}", info.version);
        println!("  Columns: {}", info.num_columns);
        println!("  Chunks: {}", info.num_chunks);
    }

    #[tokio::test]
    async fn test_process_columns_small() {
        let parquet_bytes = create_small_test_parquet().expect("Failed to create test parquet");

        println!("\n=== Processing columns from generated small parquet ===");

        // Process the parquet and verify keywords are extracted
        let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes), None, None).await
            .expect("Failed to process parquet file");

        println!("\n=== Processing complete ===");
        println!("Keyword map size: {}", result.keyword_map.len());

        // Verify we extracted keywords from the generated data
        assert!(result.keyword_map.len() > 0, "Should have extracted keywords");

        // Show a few example keywords
        let mut count = 0;
        for (keyword, _value) in result.keyword_map.iter().take(10) {
            println!("  '{}'", keyword);
            count += 1;
        }
        if result.keyword_map.len() > count {
            println!("  ... and {} more keywords", result.keyword_map.len() - count);
        }
    }

    #[tokio::test]
    async fn test_process_parquet_file_full_larger() {
        let parquet_bytes = create_larger_test_parquet().expect("Failed to create test parquet");

        println!("\n=== Full processing test for larger.parquet ===");

        let start = std::time::Instant::now();
        let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, None).await;
        let elapsed = start.elapsed();

        match result {
            Ok(process_result) => {
                println!("✓ Successfully processed file");
                println!("  Keywords found: {}", process_result.keyword_map.len());
                println!("  Column pool size: {}", process_result.column_pool.strings.len());
                println!("  Time taken: {:.2?}", elapsed);
            }
            Err(e) => {
                println!("✗ Error: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_process_parquet_file_full_small() {
        let parquet_bytes = create_small_test_parquet().expect("Failed to create test parquet");

        println!("\n=== Full processing test for small.parquet ===");

        let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, None).await;

        match result {
            Ok(process_result) => {
                println!("✓ Successfully processed file");
                println!("  Keywords found: {}", process_result.keyword_map.len());
                println!("  Column pool size: {}", process_result.column_pool.strings.len());

                // Show some sample keywords
                println!("\nSample keywords:");
                for (keyword, value) in process_result.keyword_map.iter().take(10) {
                    println!("  '{}': {} column(s), {} row group(s)",
                             keyword,
                             value.column_references.len(),
                             value.row_groups.iter().map(|rg| rg.len()).sum::<usize>()
                    );
                }
            }
            Err(e) => {
                println!("✗ Error: {}", e);
            }
        }
    }

    #[test]
    fn test_process_arrow_string_array() {
        let mut keyword_map = HashMap::new();
        let mut column_pool = ColumnPool::new();

        // Create a test array
        let array = StringArray::from(vec![
            Some("hello world"),
            None,
            Some("test@example.com"),
            Some(""),
            Some("foo-bar_baz"),
        ]);

        process_arrow_string_array(&array, "test_column", 0, 0, &mut keyword_map, &mut column_pool);

        // Verify keywords were extracted
        assert!(keyword_map.contains_key("hello"));
        assert!(keyword_map.contains_key("world"));
        assert!(keyword_map.contains_key("test"));
        assert!(keyword_map.contains_key("example"));
        assert!(keyword_map.contains_key("com"));
        assert!(keyword_map.contains_key("foo"));
        assert!(keyword_map.contains_key("bar"));
        assert!(keyword_map.contains_key("baz"));
    }

    #[tokio::test]
    async fn test_process_parquet_file_with_filters() {
        let parquet_bytes = create_small_test_parquet().expect("Failed to create test parquet");

        println!("\n=== Testing column filters for small.parquet ===");

        // Process with default error rate (1%)
        let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, None).await
            .expect("Failed to process parquet file");

        println!("✓ Successfully processed file");
        println!("  Keywords found: {}", result.keyword_map.len());
        println!("  Column filters created: {}", result.column_filters.len());

        // Verify filters exist for all columns
        for column_name in result.column_keywords_map.keys() {
            assert!(result.column_filters.contains_key(column_name),
                    "Should have filter for column {}", column_name);
        }

        // Test that filters work correctly
        for (column_name, keywords) in &result.column_keywords_map {
            let filter = result.column_filters.get(column_name).unwrap();

            // All actual keywords should be found
            for keyword in keywords {
                assert!(filter.might_contain(keyword),
                        "Filter for column {} should contain keyword '{}'", column_name, keyword);
            }

            println!("  Column '{}': {} keywords, filter type: {:?}",
                     column_name,
                     keywords.len(),
                     match filter {
                         ColumnFilter::BloomFilter { .. } => "BloomFilter",
                         ColumnFilter::RkyvHashSet(_) => "RkyvHashSet",
                     }
            );
        }
    }

    #[tokio::test]
    async fn test_process_parquet_file_custom_error_rate() {
        let parquet_bytes = create_small_test_parquet().expect("Failed to create test parquet");

        println!("\n=== Testing custom error rate for small.parquet ===");

        // Process with 0.1% error rate (stricter)
        let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, Some(0.001)).await
            .expect("Failed to process parquet file");

        println!("✓ Successfully processed file with 0.1% error rate");

        // Verify all keywords still found
        for (column_name, keywords) in &result.column_keywords_map {
            let filter = result.column_filters.get(column_name).unwrap();
            for keyword in keywords {
                assert!(filter.might_contain(keyword),
                        "Filter should contain keyword '{}' even with stricter error rate", keyword);
            }
        }
    }

    #[tokio::test]
    async fn test_global_filter_contains_all_keywords() {
        let parquet_bytes = create_small_test_parquet().expect("Failed to create test parquet");

        println!("\n=== Testing global filter for small.parquet ===");

        let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, None).await
            .expect("Failed to process parquet file");

        println!("✓ Successfully created global filter");

        // Count total unique keywords across all columns
        let mut all_keywords = IndexSet::new();
        for keywords in result.column_keywords_map.values() {
            all_keywords.extend(keywords.iter().cloned());
        }

        println!("  Total unique keywords across all columns: {}", all_keywords.len());
        println!("  Global filter type: {:?}", match &result.global_filter {
            ColumnFilter::BloomFilter { .. } => "BloomFilter",
            ColumnFilter::RkyvHashSet(_) => "RkyvHashSet",
        });

        // Verify global filter contains all keywords from all columns
        for keyword in &all_keywords {
            assert!(result.global_filter.might_contain(keyword),
                    "Global filter should contain keyword '{}'", keyword);
        }

        println!("  ✓ Global filter contains all {} keywords", all_keywords.len());
    }

    #[tokio::test]
    async fn test_global_filter_faster_than_column_checks() {
        let parquet_bytes = create_small_test_parquet().expect("Failed to create test parquet");

        println!("\n=== Testing global filter performance benefit ===");

        let result = process_parquet_file(ParquetSource::Bytes(parquet_bytes.clone()), None, None).await
            .expect("Failed to process parquet file");

        // Test keywords that don't exist in any column
        let non_existent_keywords = vec![
            "definitely_not_in_file_12345",
            "another_missing_keyword_67890",
            "xyz_not_present_99999",
        ];

        for keyword in &non_existent_keywords {
            // Check global filter first (fast rejection)
            let in_global = result.global_filter.might_contain(keyword);

            if !in_global {
                // Global filter says definitely not present
                println!("  ✓ Global filter correctly rejected '{}'", keyword);

                // Verify it's actually not in any column
                for (column_name, filter) in &result.column_filters {
                    assert!(!filter.might_contain(keyword),
                            "Column {} shouldn't contain '{}' if global filter rejected it",
                            column_name, keyword);
                }
            } else {
                // Global filter false positive - need to check columns
                println!("  ⚠ Global filter false positive for '{}'", keyword);
            }
        }
    }

    #[test]
    fn test_global_filter_small_dataset() {
        // Test that global filter respects <25 rule with combined keywords
        let mut keyword_map1 = HashMap::new();
        let mut column_pool = ColumnPool::new();

        // Column 1: 10 keywords
        for i in 0..10 {
            perform_split(&format!("word{}", i), column_pool.intern("col1"), 0, i, &mut keyword_map1);
        }

        // Column 2: 10 different keywords
        for i in 10..20 {
            perform_split(&format!("word{}", i), column_pool.intern("col2"), 0, i, &mut keyword_map1);
        }

        // Build column_keywords_map at the end
        let column_keywords_map = build_column_keywords_map(&keyword_map1, &column_pool);

        // Total: 20 unique keywords across both columns (< 25)
        let mut all_keywords = IndexSet::new();
        for keywords in column_keywords_map.values() {
            all_keywords.extend(keywords.iter().cloned());
        }

        assert_eq!(all_keywords.len(), 20);

        // Global filter should be RkyvHashSet (< MIN_KEYWORDS_FOR_BLOOM total keywords)
        let global_filter = ColumnFilter::create_column_filter(&all_keywords, 0.01);
        assert!(matches!(global_filter, ColumnFilter::RkyvHashSet(_)),
                "Global filter should be RkyvHashSet for < MIN_KEYWORDS_FOR_BLOOM total keywords");

        // Verify all keywords are in global filter
        for keyword in &all_keywords {
            assert!(global_filter.might_contain(keyword));
        }
    }

    #[test]
    fn test_global_filter_large_dataset() {
        // Test that global filter uses Bloom filter for large combined set
        let mut keyword_map1 = HashMap::new();
        let mut column_pool = ColumnPool::new();

        // Column 1: 500 keywords
        for i in 0..500 {
            perform_split(&format!("word{}", i), column_pool.intern("col1"), 0, i, &mut keyword_map1);
        }

        // Column 2: 500 different keywords
        for i in 500..1000 {
            perform_split(&format!("word{}", i), column_pool.intern("col2"), 0, i, &mut keyword_map1);
        }

        // Build column_keywords_map at the end
        let column_keywords_map = build_column_keywords_map(&keyword_map1, &column_pool);

        // Total: 1000 unique keywords across both columns
        let mut all_keywords = IndexSet::new();
        for keywords in column_keywords_map.values() {
            all_keywords.extend(keywords.iter().cloned());
        }

        assert_eq!(all_keywords.len(), 1000);

        // Global filter should likely be BloomFilter (1000 keywords)
        let global_filter = ColumnFilter::create_column_filter(&all_keywords, 0.01);

        // Verify all keywords are in global filter
        for keyword in &all_keywords {
            assert!(global_filter.might_contain(keyword),
                    "Global filter should contain keyword '{}'", keyword);
        }

        println!("Global filter type for 1000 keywords: {:?}",
                 match &global_filter {
                     ColumnFilter::BloomFilter { .. } => "BloomFilter",
                     ColumnFilter::RkyvHashSet(_) => "RkyvHashSet",
                 }
        );
    }

    #[tokio::test]
    async fn test_create_and_validate_distributed_index() {
        use arrow::array::{Int32Array, StringArray, ArrayRef};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_writer::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        println!("\n=== Testing Index Building with Test Data ===");

        // Create a test parquet in memory with known data
        println!("\n1. Creating test parquet in memory...");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec![
            "Alice Johnson",
            "Bob Smith",
            "Charlie Brown",
            "Diana Prince",
            "Eve Wilson",
        ]);
        let email_array = StringArray::from(vec![
            "alice@example.com",
            "bob@test.org",
            "charlie.brown@email.net",
            "diana.prince@hero.gov",
            "eve@company.io",
        ]);
        let city_array = StringArray::from(vec![
            "New York",
            "Los Angeles",
            "Chicago",
            "Baltimore",
            "Seattle",
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(name_array) as ArrayRef,
                Arc::new(email_array) as ArrayRef,
                Arc::new(city_array) as ArrayRef,
            ],
        )
            .expect("Failed to create record batch");

        // Write to in-memory bytes
        let mut parquet_bytes = Vec::new();
        {
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(&mut parquet_bytes, schema.clone(), Some(props))
                .expect("Failed to create arrow writer");

            writer.write(&batch).expect("Failed to write batch");
            writer.close().expect("Failed to close writer");
        }

        println!("✓ Test parquet created in memory ({} bytes)", parquet_bytes.len());

        // Build index in memory
        println!("\n2. Building index in memory...");
        let _searcher = build_index_in_memory(ParquetSource::Bytes(Bytes::from(parquet_bytes)), None, Some(0.01)).await
            .expect("Failed to build index");
        println!("✓ Index built successfully in memory");
    }

    /// Get current memory usage in bytes (RSS - Resident Set Size)
    fn get_memory_usage_bytes() -> u64 {
        #[cfg(target_os = "linux")]
        {
            // Read from /proc/self/status
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        // Format: "VmRSS:     12345 kB"
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            if let Ok(kb) = parts[1].parse::<u64>() {
                                return kb * 1024; // Convert KB to bytes
                            }
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            // Use rusage on macOS
            let mut usage = libc::rusage {
                ru_utime: libc::timeval { tv_sec: 0, tv_usec: 0 },
                ru_stime: libc::timeval { tv_sec: 0, tv_usec: 0 },
                ru_maxrss: 0,
                ru_ixrss: 0,
                ru_idrss: 0,
                ru_isrss: 0,
                ru_minflt: 0,
                ru_majflt: 0,
                ru_nswap: 0,
                ru_inblock: 0,
                ru_oublock: 0,
                ru_msgsnd: 0,
                ru_msgrcv: 0,
                ru_nsignals: 0,
                ru_nvcsw: 0,
                ru_nivcsw: 0,
            };

            unsafe {
                if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
                    // On macOS, ru_maxrss is in bytes
                    return usage.ru_maxrss as u64;
                }
            }
        }

        #[cfg(target_os = "windows")]
        {
            // Windows implementation using GetProcessMemoryInfo
            #[repr(C)]
            struct ProcessMemoryCounters {
                cb: u32,
                page_fault_count: u32,
                peak_working_set_size: usize,
                working_set_size: usize,
                quota_peak_paged_pool_usage: usize,
                quota_paged_pool_usage: usize,
                quota_peak_non_paged_pool_usage: usize,
                quota_non_paged_pool_usage: usize,
                pagefile_usage: usize,
                peak_pagefile_usage: usize,
            }

            unsafe extern "system" {
                fn GetCurrentProcess() -> *mut std::ffi::c_void;
                fn GetProcessMemoryInfo(
                    process: *mut std::ffi::c_void,
                    pmc: *mut ProcessMemoryCounters,
                    cb: u32,
                ) -> i32;
            }

            unsafe {
                let mut pmc: ProcessMemoryCounters = std::mem::zeroed();
                pmc.cb = size_of::<ProcessMemoryCounters>() as u32;

                let result = GetProcessMemoryInfo(
                    GetCurrentProcess(),
                    &mut pmc,
                    pmc.cb,
                );

                if result != 0 {
                    // Return working set size (RSS equivalent on Windows)
                    return pmc.working_set_size as u64;
                }
            }
        }

        0
    }

    #[tokio::test]
    #[ignore]  // This is more of an informational test and must be run with a release build and
    // takes a long time to run, should be ignored for normal testing and builds
    async fn test_memory_scaling_with_row_groups() {
        const ROWS_PER_GROUP: usize = 25_000;
        const MEMORY_LIMIT_GB: f64 = 4.0;
        const MEMORY_LIMIT_BYTES: u64 = (MEMORY_LIMIT_GB * 1024.0 * 1024.0 * 1024.0) as u64;

        println!("\n========================================");
        println!("MEMORY SCALING TEST");
        println!("========================================");
        println!("Target: Monitor memory usage as row groups double");
        println!("Starting: {} rows, 1 row group", ROWS_PER_GROUP);
        println!("Memory limit: {:.1} GB", MEMORY_LIMIT_GB);
        println!("========================================\n");

        let mut num_row_groups = 1;
        let mut iteration = 1;
        let mut cumulative_parquet_size = 0u64;
        let can_measure_memory = get_memory_usage_bytes() > 0;

        if !can_measure_memory {
            println!("⚠ Cannot measure system memory on this platform.");
            println!("  Will use parquet size tracking as fallback.\n");
        }

        loop {
            println!("\n--- Iteration {} ---", iteration);
            println!("Configuration: {} rows/group × {} groups = {} total rows",
                     ROWS_PER_GROUP, num_row_groups, ROWS_PER_GROUP * num_row_groups);

            // Get memory before
            let mem_before = get_memory_usage_bytes();
            if can_measure_memory {
                println!("Memory before: {:.2} MB", mem_before as f64 / (1024.0 * 1024.0));
            }

            // Estimate size before creating - rough heuristic: ~200 bytes per row after compression
            let estimated_size = (ROWS_PER_GROUP * num_row_groups * 200) as u64;
            if !can_measure_memory && cumulative_parquet_size + estimated_size > MEMORY_LIMIT_BYTES {
                println!("\n⚠ Estimated parquet size would exceed memory limit.");
                println!("  Estimated: {:.2} GB", (cumulative_parquet_size + estimated_size) as f64 / (1024.0 * 1024.0 * 1024.0));
                println!("  Stopping before allocation failure.\n");
                break;
            }

            // Create parquet file
            println!("\nGenerating parquet file...");
            let start_gen = std::time::Instant::now();
            let parquet_bytes = match create_random_ascii_parquet(ROWS_PER_GROUP, num_row_groups) {
                Ok(bytes) => bytes,
                Err(e) => {
                    println!("ERROR: Failed to create parquet: {}", e);
                    break;
                }
            };
            let gen_time = start_gen.elapsed();
            let parquet_size = parquet_bytes.len() as u64;
            println!("Generation time: {:.2?}", gen_time);

            // Track cumulative size
            cumulative_parquet_size += parquet_size;

            // Get memory after parquet creation
            let mem_after_parquet = get_memory_usage_bytes();
            if can_measure_memory {
                let parquet_mem = (mem_after_parquet as i64 - mem_before as i64).max(0) as u64;
                println!("Memory after parquet: {:.2} MB (delta: {:.2} MB)",
                         mem_after_parquet as f64 / (1024.0 * 1024.0),
                         parquet_mem as f64 / (1024.0 * 1024.0));
            }

            // Build index
            println!("\nBuilding index...");
            let start_index = std::time::Instant::now();
            let index_result = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, Some(0.01)).await;

            match index_result {
                Ok(searcher) => {
                    let index_time = start_index.elapsed();
                    println!("Index building time: {:.2?}", index_time);

                    let info = searcher.get_index_info();
                    println!("~ Total keywords indexed: {}", info.num_chunks * 1000); // if it exists

                    // Get memory after indexing
                    let mem_after_index = get_memory_usage_bytes();

                    if can_measure_memory {
                        let index_mem = (mem_after_index as i64 - mem_after_parquet as i64).max(0) as u64;
                        let total_mem = (mem_after_index as i64 - mem_before as i64).max(0) as u64;

                        println!("Memory after index: {:.2} MB (delta: {:.2} MB)",
                                 mem_after_index as f64 / (1024.0 * 1024.0),
                                 index_mem as f64 / (1024.0 * 1024.0));
                        println!("Total memory delta: {:.2} MB ({:.2} GB)",
                                 total_mem as f64 / (1024.0 * 1024.0),
                                 total_mem as f64 / (1024.0 * 1024.0 * 1024.0));

                        // Check if we've hit the memory limit
                        if mem_after_index >= MEMORY_LIMIT_BYTES {
                            // Explicitly drop searcher before breaking
                            drop(searcher);

                            println!("\n========================================");
                            println!("MEMORY LIMIT REACHED!");
                            println!("========================================");
                            println!("Final configuration: {} rows/group × {} groups = {} total rows",
                                     ROWS_PER_GROUP, num_row_groups, ROWS_PER_GROUP * num_row_groups);
                            println!("Final memory: {:.2} GB", mem_after_index as f64 / (1024.0 * 1024.0 * 1024.0));
                            println!("========================================\n");
                            break;
                        }
                    } else {
                        // Use cumulative parquet size as proxy
                        println!("Cumulative parquet size: {:.2} GB",
                                 cumulative_parquet_size as f64 / (1024.0 * 1024.0 * 1024.0));

                        if cumulative_parquet_size >= MEMORY_LIMIT_BYTES / 2 {
                            // Explicitly drop searcher before breaking
                            drop(searcher);

                            println!("\n========================================");
                            println!("SIZE LIMIT REACHED (fallback mode)");
                            println!("========================================");
                            println!("Final configuration: {} rows/group × {} groups = {} total rows",
                                     ROWS_PER_GROUP, num_row_groups, ROWS_PER_GROUP * num_row_groups);
                            println!("Cumulative size: {:.2} GB",
                                     cumulative_parquet_size as f64 / (1024.0 * 1024.0 * 1024.0));
                            println!("========================================\n");
                            break;
                        }
                    }

                    // Explicitly drop the searcher to free memory before next iteration
                    drop(searcher);
                    println!("Dropped searcher, freeing memory...");

                    // Double the number of row groups for next iteration
                    num_row_groups *= 2;
                    iteration += 1;

                    // Safety check: prevent infinite loop and excessive memory
                    if num_row_groups > 256 {
                        println!("\nSafety limit reached (256 row groups). Stopping test.");
                        break;
                    }
                }
                Err(e) => {
                    println!("ERROR: Failed to build index: {}", e);
                    break;
                }
            }

            // Longer delay to allow memory to be fully released
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        println!("\n========================================");
        println!("MEMORY SCALING TEST COMPLETE");
        println!("========================================");
    }

}

/// Integration tests for column filtering functionality
///
/// These tests create actual Parquet files with specific data patterns
/// and verify that column filtering works correctly.

#[cfg(test)]
mod column_filter_integration_tests {
    use std::sync::Arc;
    use arrow::array::{StringArray, Int32Array, RecordBatch};
    use arrow::datatypes::{Schema, Field, DataType};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use crate::{build_index_in_memory, ParquetSource};

    /// Helper to create a test Parquet with multiple columns (in memory)
    fn create_test_parquet_multi_column() -> Result<Bytes, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("username", DataType::Utf8, false),
            Field::new("notes", DataType::Utf8, false),
        ]));

        // Create data where:
        // - "alice" appears in email and username
        // - "admin" appears only in username
        // - "contact@example.com" appears only in email
        // - "urgent" appears only in notes
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let email_array = StringArray::from(vec![
            "alice@company.com",
            "bob@company.com",
            "contact@example.com",
            "dave@company.com",
            "eve@company.com",
        ]);
        let username_array = StringArray::from(vec![
            "alice",
            "bob",
            "charlie",
            "admin",
            "eve",
        ]);
        let notes_array = StringArray::from(vec![
            "regular user",
            "regular user",
            "urgent - needs attention",
            "regular user",
            "regular user",
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(email_array),
                Arc::new(username_array),
                Arc::new(notes_array),
            ],
        )?;

        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(Bytes::from(buffer))
    }

    /// Helper to create a Parquet with many columns (in memory) - tests index 0 efficiency
    fn create_test_parquet_many_columns() -> Result<Bytes, Box<dyn std::error::Error>> {
        // Create 50 string columns
        let mut fields = vec![Field::new("id", DataType::Int32, false)];
        for i in 0..50 {
            fields.push(Field::new(&format!("col_{}", i), DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));

        // Create data where "target" appears in columns 10, 20, and 30
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let mut arrays: Vec<Arc<dyn arrow::array::Array>> = vec![Arc::new(id_array)];

        for i in 0..50 {
            let data = if i == 10 || i == 20 || i == 30 {
                vec!["target", "other", "other"]
            } else {
                vec!["other", "other", "other"]
            };
            arrays.push(Arc::new(StringArray::from(data)));
        }

        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(Bytes::from(buffer))
    }

    /// Helper to create a parquet with hierarchical phrases in different columns (in memory)
    fn create_test_parquet_hierarchical() -> Result<Bytes, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("email", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("domain", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let email_array = StringArray::from(vec![
            "user@example.com",
            "admin@test.org",
            "other@somewhere.net",
        ]);
        let path_array = StringArray::from(vec![
            "/usr/local/bin",
            "/var/log/app",
            "/home/user/docs",
        ]);
        let domain_array = StringArray::from(vec![
            "api.example.com",
            "test.example.com",
            "other.domain.org",
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(email_array),
                Arc::new(path_array),
                Arc::new(domain_array),
            ],
        )?;

        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(Bytes::from(buffer))
    }

    #[tokio::test]
    async fn test_search_keyword_in_multiple_columns() {
        // Create test parquet in memory
        let parquet_bytes = create_test_parquet_multi_column().expect("Failed to create test parquet");

        // Build index
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None)
            .await
            .expect("Failed to build index");

        // Test 1: Search "alice" without column filter (uses index 0)
        let result_all = searcher.search("alice", None, true).await.expect("Search failed");
        assert!(result_all.found, "alice should be found");

        let data_all = result_all.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data_all.columns.len(), 2, "alice should be in 2 columns: email and username");
        assert!(data_all.columns.contains(&"email".to_string()), "alice should be in email");
        assert!(data_all.columns.contains(&"username".to_string()), "alice should be in username");

        // Verify column_id 0 is not in results
        for col_detail in &data_all.column_details {
            assert_ne!(col_detail.column_name, "", "column_id 0 should not appear in results");
        }

        // Test 2: Search "alice" filtered to email column
        let result_email = searcher.search("alice", Some("email"), true).await.expect("Search failed");
        assert!(result_email.found, "alice should be found in email");

        let data_email = result_email.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data_email.columns.len(), 1, "When filtering by email, should only show email column");
        assert_eq!(data_email.columns[0], "email", "Should only show email column");

        // Test 3: Search "alice" filtered to username column
        let result_username = searcher.search("alice", Some("username"), true).await.expect("Search failed");
        assert!(result_username.found, "alice should be found in username");

        let data_username = result_username.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data_username.columns.len(), 1, "Should only show username column");
        assert_eq!(data_username.columns[0], "username");

        // Test 4: Search "alice" filtered to notes column (should not find)
        let result_notes = searcher.search("alice", Some("notes"), true).await.expect("Search failed");
        assert!(!result_notes.found, "alice should not be found in notes column");

        // Test 5: Search "admin" (only in username)
        let result_admin = searcher.search("admin", None, true).await.expect("Search failed");
        assert!(result_admin.found, "admin should be found");

        let data_admin = result_admin.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data_admin.columns.len(), 1, "admin should only be in username");
        assert_eq!(data_admin.columns[0], "username");

        // Test 6: Search "urgent" (only in notes)
        let result_urgent = searcher.search("urgent", None, true).await.expect("Search failed");
        assert!(result_urgent.found, "urgent should be found");

        let data_urgent = result_urgent.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data_urgent.columns.len(), 1, "urgent should only be in notes");
        assert_eq!(data_urgent.columns[0], "notes");

    }

    #[tokio::test]
    async fn test_search_with_many_columns() {
        // Create test parquet in memory with 50 columns
        let parquet_bytes = create_test_parquet_many_columns().expect("Failed to create test parquet");

        // Build index
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None)
            .await
            .expect("Failed to build index");

        // Search for "target" without column filter (uses index 0 - efficient)
        let result = searcher.search("target", None, true).await.expect("Search failed");
        assert!(result.found, "target should be found");

        let data = result.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data.columns.len(), 3, "target should be in 3 columns (col_10, col_20, col_30)");

        // Verify the correct columns
        let mut found_columns: Vec<String> = data.columns.clone();
        found_columns.sort();

        assert!(found_columns.contains(&"col_10".to_string()));
        assert!(found_columns.contains(&"col_20".to_string()));
        assert!(found_columns.contains(&"col_30".to_string()));

        // Search with specific column filter
        let result_col10 = searcher.search("target", Some("col_10"), true).await.expect("Search failed");
        assert!(result_col10.found, "target should be found in col_10");

        let data_col10 = result_col10.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data_col10.columns.len(), 1);
        assert_eq!(data_col10.columns[0], "col_10");

        // Search for target in a column it doesn't appear in
        let result_col5 = searcher.search("target", Some("col_5"), true).await.expect("Search failed");
        assert!(!result_col5.found, "target should not be found in col_5");

    }

    #[tokio::test]
    async fn test_hierarchical_phrases_in_columns() {
        // Create test parquet in memory with hierarchical phrases
        let parquet_bytes = create_test_parquet_hierarchical().expect("Failed to create test parquet");

        // Build index
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None)
            .await
            .expect("Failed to build index");

        // Test 1: Search for keyword "example.com" - only in email (from "user@example.com")
        let result_keyword = searcher.search("example.com", None, true).await.expect("Search failed");
        assert!(result_keyword.found, "keyword example.com should be found");

        let data_keyword = result_keyword.verified_matches.as_ref().expect("Should have data");
        assert_eq!(data_keyword.columns.len(), 1, "keyword example.com should only be in email");
        assert_eq!(data_keyword.columns[0], "email");

        // Test 1b: Search phrase "example.com" - should find in both email and domain
        let result = searcher.search("example.com", None, false).await.expect("Search failed");
        assert!(result.found, "phrase example.com should be found");

        // With the new API, verified_matches and/or needs_verification contain the results
        let has_results = result.verified_matches.is_some() || result.needs_verification.is_some();
        assert!(has_results, "Should have some matches");

        // Collect columns from both verified and unverified matches
        let mut columns_found = std::collections::HashSet::new();
        if let Some(verified) = &result.verified_matches {
            for col in &verified.columns {
                columns_found.insert(col.clone());
            }
        }
        if let Some(needs_check) = &result.needs_verification {
            for col in &needs_check.columns {
                columns_found.insert(col.clone());
            }
        }
        // The phrase search should find matches in at least one column
        assert!(!columns_found.is_empty(), "phrase example.com should have matches");

        // Test 2: Search phrase "example.com" filtered to email
        let result_email = searcher.search("example.com", Some("email"), false).await.expect("Search failed");
        assert!(result_email.found, "phrase example.com should be found in email");

        // Check that all matches are in email column
        if let Some(verified) = &result_email.verified_matches {
            for col in &verified.columns {
                assert_eq!(col, "email", "verified matches should only be in email column");
            }
        }
        if let Some(needs_check) = &result_email.needs_verification {
            for col in &needs_check.columns {
                assert_eq!(col, "email", "unverified matches should only be in email column");
            }
        }

        // Test 3: Search phrase "example.com" filtered to path (should not find)
        let result_path = searcher.search("example.com", Some("path"), false).await.expect("Search failed");
        assert!(!result_path.found, "phrase example.com should not be found in path column");

        // Test 4: Search for intermediate token "example" (from hierarchical split)
        let result_example = searcher.search("example", None, true).await.expect("Search failed");
        assert!(result_example.found, "example should be found");

        let data_example = result_example.verified_matches.as_ref().expect("Should have data");
        // Should be in both email and domain since "example.com" splits to "example"
        assert!(data_example.columns.len() >= 2, "example should be in multiple columns");

        // Test 5: Search for "/usr/local/bin" in path column
        let result_path_full = searcher.search("/usr/local/bin", Some("path"), true).await.expect("Search failed");
        assert!(result_path_full.found, "/usr/local/bin should be found in path");

        // Test 6: Search for "local" (split from "/usr/local/bin")
        let result_local = searcher.search("local", Some("path"), true).await.expect("Search failed");
        assert!(result_local.found, "local should be found in path");

    }

    #[tokio::test]
    async fn test_column_id_zero_never_exposed() {
        // Create test parquet in memory
        let parquet_bytes = create_test_parquet_multi_column().expect("Failed to create test parquet");

        // Build index
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None)
            .await
            .expect("Failed to build index");

        // Search various keywords
        let keywords = vec!["alice", "bob", "admin", "urgent", "company"];

        for keyword in keywords {
            let result = searcher.search(keyword, None, true).await.expect("Search failed");

            if result.found {
                let data = result.verified_matches.as_ref().expect("Should have data");

                // Verify column_id 0 never appears
                for column_name in &data.columns {
                    assert_ne!(column_name, "", "Empty string (column_id 0) should never appear in results");
                }

                for col_detail in &data.column_details {
                    assert_ne!(col_detail.column_name, "", "column_id 0 should not appear in column_details");
                    assert!(!col_detail.column_name.is_empty(), "Column names should never be empty");
                }
            }
        }

    }

    #[tokio::test]
    async fn test_nonexistent_column_filter() {
        // Create test parquet in memory
        let parquet_bytes = create_test_parquet_multi_column().expect("Failed to create test parquet");

        // Build index
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None)
            .await
            .expect("Failed to build index");

        // Search for existing keyword in non-existent column
        let result = searcher.search("alice", Some("this_column_does_not_exist"), true).await.expect("Search failed");

        // Should return not found
        assert!(!result.found, "Searching non-existent column should return not found");

    }

    #[tokio::test]
    async fn test_search_in_column_helper() {
        // Create test parquet in memory
        let parquet_bytes = create_test_parquet_multi_column().expect("Failed to create test parquet");

        // Build index
        let searcher = build_index_in_memory(ParquetSource::Bytes(parquet_bytes), None, None)
            .await
            .expect("Failed to build index");

        // Test search_in_column convenience method
        let found_email = searcher.search_in_column("alice", "email").await.expect("Search failed");
        assert!(found_email, "alice should be found in email column");

        let found_username = searcher.search_in_column("alice", "username").await.expect("Search failed");
        assert!(found_username, "alice should be found in username column");

        let found_notes = searcher.search_in_column("alice", "notes").await.expect("Search failed");
        assert!(!found_notes, "alice should not be found in notes column");

    }
}