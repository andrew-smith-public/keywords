#[cfg(test)]
mod performance_tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use arrow::array::{ArrayRef, StringArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::basic::{Compression, ZstdLevel, BrotliLevel, GzipLevel};
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use std::fs::File;
    use crate::build_and_save_index;
    use crate::searching::keyword_search::KeywordSearcher;
    use crate::searching::pruned_reader::PrunedParquetReader;
    use crate::utils::file_interaction_local_and_cloud::get_object_store;

    // ============================================================================
    // Configuration Structures
    // ============================================================================

    /// Global test configuration
    struct TestConfig {
        /// Number of times to run each test (for averaging)
        iterations: usize,
    }

    impl Default for TestConfig {
        fn default() -> Self {
            Self {
                iterations: 1,
            }
        }
    }

    /// Configuration for generating a parquet file
    #[derive(Clone)]
    struct ParquetConfig {
        compression: Compression,
        num_row_groups: usize,
        rows_per_group: usize,
        string_pool_size: usize,
        num_columns: usize,
        scenario: DataScenario,
    }

    impl Default for ParquetConfig {
        fn default() -> Self {
            Self {
                compression: Compression::SNAPPY,
                num_row_groups: 5,
                rows_per_group: 100_000,
                string_pool_size: 5000,
                num_columns: 10,
                scenario: DataScenario::Normal,
            }
        }
    }

    impl ParquetConfig {
        fn with_compression(mut self, compression: Compression) -> Self {
            self.compression = compression;
            self
        }

        fn with_num_row_groups(mut self, num_row_groups: usize, rows_per_group: usize) -> Self {
            self.num_row_groups = num_row_groups;
            self.rows_per_group = rows_per_group;
            self
        }

        fn with_string_pool_size(mut self, size: usize) -> Self {
            self.string_pool_size = size;
            self
        }

        fn with_scenario(mut self, scenario: DataScenario) -> Self {
            self.scenario = scenario;
            self
        }
    }

    /// Data generation scenario
    #[derive(Clone, Copy, Debug)]
    enum DataScenario {
        /// Normal case: search values exist together in same rows
        Normal,
        /// False positive: each search value exists, but never in the same row
        FalsePositive,
        /// Bloom miss: search values don't exist at all (tests bloom filter rejection)
        BloomMiss,
    }

    /// Results from a single performance test run
    #[derive(Clone)]
    struct TestResult {
        keyword_time: Duration,
        datafusion_time: Duration,
        naive_time: Duration,
        rows_found: usize,
        index_build_time: Duration,
    }

    impl TestResult {
        fn average(results: &[TestResult]) -> Self {
            let n = results.len() as u32;
            Self {
                keyword_time: Duration::from_secs_f64(
                    results.iter().map(|r| r.keyword_time.as_secs_f64()).sum::<f64>() / n as f64
                ),
                datafusion_time: Duration::from_secs_f64(
                    results.iter().map(|r| r.datafusion_time.as_secs_f64()).sum::<f64>() / n as f64
                ),
                naive_time: Duration::from_secs_f64(
                    results.iter().map(|r| r.naive_time.as_secs_f64()).sum::<f64>() / n as f64
                ),
                rows_found: results[0].rows_found, // Should be same for all
                index_build_time: Duration::from_secs_f64(
                    results.iter().map(|r| r.index_build_time.as_secs_f64()).sum::<f64>() / n as f64
                ),
            }
        }
    }

    /// Row in a comparison table
    struct ComparisonRow {
        dimension: String,
        file_size_mb: f64,
        index_build_time: Duration,
        keyword_time: Duration,
        datafusion_time: Duration,
        naive_time: Duration,
        rows_found: usize,
    }

    // ============================================================================
    // Helper Functions
    // ============================================================================

    /// Generate a random string pool
    fn generate_string_pool(size: usize) -> Vec<String> {
        let mut rng = rand::rng();
        (0..size)
            .map(|_| {
                let len = rng.random_range(10..=20);
                std::iter::repeat_with(|| rng.sample(Alphanumeric) as char)
                    .filter(|c| c.is_alphabetic())
                    .take(len)
                    .collect()
            })
            .collect()
    }

    /// Generate a parquet file with specified configuration
    /// Returns (file_path, search_values)
    fn generate_parquet_file(
        config: ParquetConfig,
    ) -> Result<(String, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
        let mut rng = rand::rng();

        // Generate string pool
        let string_pool = generate_string_pool(config.string_pool_size);

        // Create schema
        let fields: Vec<Field> = (0..config.num_columns)
            .map(|i| Field::new(format!("col_{}", i), DataType::Utf8, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Create temporary file
        let file_path = std::env::temp_dir()
            .join(format!("perf_test_{}.parquet", rand::random::<u32>()))
            .to_string_lossy()
            .to_string();

        let file = File::create(&file_path)?;

        let props = WriterProperties::builder()
            .set_compression(config.compression)
            .set_max_row_group_size(config.rows_per_group)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        let mut search_values = Vec::new();

        match config.scenario {
            DataScenario::Normal => {
                // Generate data where search values appear together
                for rg in 0..config.num_row_groups {
                    let mut columns: Vec<ArrayRef> = Vec::new();

                    for col_idx in 0..config.num_columns {
                        let values: Vec<String> = (0..config.rows_per_group)
                            .map(|_| {
                                let idx = rng.random_range(0..string_pool.len());
                                string_pool[idx].clone()
                            })
                            .collect();

                        // Remember first row values from first 3 columns in first row group
                        if rg == 0 && col_idx < 3 {
                            search_values.push(values[0].clone());
                        }

                        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
                    }

                    let batch = RecordBatch::try_new(schema.clone(), columns)?;
                    writer.write(&batch)?;
                }
            }

            DataScenario::FalsePositive => {
                // Generate data where each search value exists but never together
                // Strategy: Put value[0] in rows 0-999 of col_0
                //          Put value[1] in rows 1000-1999 of col_1
                //          Put value[2] in rows 2000-2999 of col_2

                for rg in 0..config.num_row_groups {
                    let mut columns: Vec<ArrayRef> = Vec::new();

                    for col_idx in 0..config.num_columns {
                        let mut values: Vec<String> = (0..config.rows_per_group)
                            .map(|_| {
                                let idx = rng.random_range(0..string_pool.len());
                                string_pool[idx].clone()
                            })
                            .collect();

                        // Plant search values in non-overlapping rows
                        if rg == 0 {
                            match col_idx {
                                0 => {
                                    // Plant search_value[0] in rows 0-999
                                    let search_val = string_pool[rng.random_range(0..string_pool.len())].clone();
                                    if search_values.is_empty() {
                                        search_values.push(search_val.clone());
                                    }
                                    for row in 0..1000.min(config.rows_per_group) {
                                        values[row] = search_values[0].clone();
                                    }
                                }
                                1 => {
                                    // Plant search_value[1] in rows 1000-1999
                                    let search_val = string_pool[rng.random_range(0..string_pool.len())].clone();
                                    if search_values.len() < 2 {
                                        search_values.push(search_val.clone());
                                    }
                                    for row in 1000..2000.min(config.rows_per_group) {
                                        values[row] = search_values[1].clone();
                                    }
                                }
                                2 => {
                                    // Plant search_value[2] in rows 2000-2999
                                    let search_val = string_pool[rng.random_range(0..string_pool.len())].clone();
                                    if search_values.len() < 3 {
                                        search_values.push(search_val.clone());
                                    }
                                    for row in 2000..3000.min(config.rows_per_group) {
                                        values[row] = search_values[2].clone();
                                    }
                                }
                                _ => {}
                            }
                        }

                        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
                    }

                    let batch = RecordBatch::try_new(schema.clone(), columns)?;
                    writer.write(&batch)?;
                }

                // Ensure we have 3 search values
                while search_values.len() < 3 {
                    search_values.push(string_pool[rng.random_range(0..string_pool.len())].clone());
                }
            }

            DataScenario::BloomMiss => {
                // Generate data normally
                for _rg in 0..config.num_row_groups {
                    let mut columns: Vec<ArrayRef> = Vec::new();

                    for _col_idx in 0..config.num_columns {
                        let values: Vec<String> = (0..config.rows_per_group)
                            .map(|_| {
                                let idx = rng.random_range(0..string_pool.len());
                                string_pool[idx].clone()
                            })
                            .collect();

                        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
                    }

                    let batch = RecordBatch::try_new(schema.clone(), columns)?;
                    writer.write(&batch)?;
                }

                // Generate search values that don't exist in the data
                search_values = (0..3)
                    .map(|_| {
                        let len = rng.random_range(10..=20);
                        let mut val = String::new();
                        val.push_str("NOTFOUND");
                        for _ in 8..len {
                            val.push(rng.sample(Alphanumeric) as char);
                        }
                        val
                    })
                    .collect();
            }
        }

        writer.close()?;

        Ok((file_path, search_values))
    }

    /// Build index and return timing
    async fn build_index_timed(
        file_path: &str,
    ) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        build_and_save_index(file_path, None, Some(0.01), None).await?;
        Ok(start.elapsed())
    }

    /// Run all three performance tests and return results
    async fn run_performance_test(
        file_path: &str,
        search_values: &[String],
    ) -> Result<TestResult, Box<dyn std::error::Error + Send + Sync>> {
        // Build index
        let index_build_time = build_index_timed(file_path).await?;

        // Method 1: Keyword Index
        let keyword_start = Instant::now();

        let searcher = KeywordSearcher::load(file_path, None).await?;
        let result0 = searcher.search(&search_values[0], Some("col_0"), true).await?;
        let result1 = searcher.search(&search_values[1], Some("col_1"), true).await?;
        let result2 = searcher.search(&search_values[2], Some("col_2"), true).await?;

        let combined = KeywordSearcher::combine_and(&[result0, result1, result2]);

        let keyword_rows = if let Some(combined_result) = combined {
            let reader = PrunedParquetReader::from_path(file_path);
            let batches = reader.read_combined_rows(&combined_result, None).await?;
            batches.iter().map(|b| b.num_rows()).sum()
        } else {
            0
        };

        let keyword_time = keyword_start.elapsed();

        // Method 2: DataFusion
        let datafusion_start = Instant::now();

        use datafusion::prelude::*;
        let ctx = SessionContext::new();
        ctx.register_parquet("data", file_path, Default::default()).await?;

        let query = format!(
            "SELECT * FROM data WHERE col_0 = '{}' AND col_1 = '{}' AND col_2 = '{}'",
            search_values[0], search_values[1], search_values[2]
        );

        let df = ctx.sql(&query).await?;
        let df_batches = df.collect().await?;
        let datafusion_rows: usize = df_batches.iter().map(|b| b.num_rows()).sum();

        let datafusion_time = datafusion_start.elapsed();

        // Method 3: Naive
        let naive_start = Instant::now();

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use arrow::compute::filter_record_batch;
        use arrow::array::BooleanArray;

        let (store, path) = get_object_store(file_path).await?;
        let file_bytes = store.get(&path).await?.bytes().await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file_bytes)?;
        let mut reader = builder.build()?;

        let mut matching_batches = Vec::new();

        while let Some(batch) = reader.next() {
            let batch = batch?;

            let schema = batch.schema();
            let col0 = batch.column(schema.index_of("col_0")?)
                .as_any().downcast_ref::<StringArray>().unwrap();
            let col1 = batch.column(schema.index_of("col_1")?)
                .as_any().downcast_ref::<StringArray>().unwrap();
            let col2 = batch.column(schema.index_of("col_2")?)
                .as_any().downcast_ref::<StringArray>().unwrap();

            let mask: Vec<bool> = (0..batch.num_rows())
                .map(|i| {
                    col0.value(i) == search_values[0] &&
                        col1.value(i) == search_values[1] &&
                        col2.value(i) == search_values[2]
                })
                .collect();

            let mask_array = BooleanArray::from(mask);
            let filtered = filter_record_batch(&batch, &mask_array)?;

            if filtered.num_rows() > 0 {
                matching_batches.push(filtered);
            }
        }

        let naive_rows: usize = matching_batches.iter().map(|b| b.num_rows()).sum();
        let naive_time = naive_start.elapsed();

        // Verify all methods agree (for normal scenario)
        assert_eq!(keyword_rows, naive_rows, "Keyword and Naive should match");
        assert_eq!(keyword_rows, datafusion_rows, "Keyword and DataFusion should match");

        Ok(TestResult {
            keyword_time,
            datafusion_time,
            naive_time,
            rows_found: keyword_rows,
            index_build_time,
        })
    }

    /// Get file size in MB
    fn get_file_size_mb(file_path: &str) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let metadata = std::fs::metadata(file_path)?;
        Ok(metadata.len() as f64 / (1024.0 * 1024.0))
    }

    /// Clean up test files
    fn cleanup(file_path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if std::path::Path::new(file_path).exists() {
            std::fs::remove_file(file_path)?;
        }
        let index_dir = format!("{}.index", file_path);
        if std::path::Path::new(&index_dir).exists() {
            std::fs::remove_dir_all(index_dir)?;
        }
        Ok(())
    }

    /// Display comparison table in Markdown format
    fn display_comparison_table(
        results: &[ComparisonRow],
        title: &str,
        dimension_label: &str,
    ) {
        println!("\n## {}\n", title);

        // Table header
        println!("| {} | File Size | Index Build | Keyword Index | DataFusion | Speedup | Naive | Rows |",
                 dimension_label);
        println!("|{}|-----------|-------------|---------------|------------|---------|-------|------|",
                 "-".repeat(dimension_label.len()));

        // Table rows
        for row in results {
            let speedup = row.datafusion_time.as_secs_f64() / row.keyword_time.as_secs_f64();
            println!("| {} | {:.2} MB | {:?} | {:?} | {:?} | {:.2}x | {:?} | {} |",
                     row.dimension,
                     row.file_size_mb,
                     row.index_build_time,
                     row.keyword_time,
                     row.datafusion_time,
                     speedup,
                     row.naive_time,
                     row.rows_found);
        }

        println!();

        // Analysis
        let best_keyword = results.iter().min_by_key(|r| r.keyword_time).unwrap();
        let best_datafusion = results.iter().min_by_key(|r| r.datafusion_time).unwrap();
        let best_naive = results.iter().min_by_key(|r| r.naive_time).unwrap();

        println!("**Analysis:**");
        println!("- Fastest Keyword Index: `{}` ({:?})", best_keyword.dimension, best_keyword.keyword_time);
        println!("- Fastest DataFusion: `{}` ({:?})", best_datafusion.dimension, best_datafusion.datafusion_time);
        println!("- Fastest Naive: `{}` ({:?})", best_naive.dimension, best_naive.naive_time);

        if let Some((smallest, _)) = results.iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.file_size_mb.partial_cmp(&b.file_size_mb).unwrap()) {
            println!("- Smallest file: `{}` ({:.2} MB)", results[smallest].dimension, results[smallest].file_size_mb);
        }

        println!();
    }

    // ============================================================================
    // Test 1: Compression Algorithm Comparison
    // ============================================================================

    #[tokio::test]
    async fn test_compression_comparison() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let test_config = TestConfig::default();

        // Define compression algorithms to test (all valid for Parquet)
        let compressions = vec![
            ("GZIP-9", Compression::GZIP(GzipLevel::try_new(9).unwrap())),
            ("ZSTD-18", Compression::ZSTD(ZstdLevel::try_new(18).unwrap())),
            ("SNAPPY", Compression::SNAPPY),
            ("LZ4", Compression::LZ4),
            ("BROTLI-9", Compression::BROTLI(BrotliLevel::try_new(9).unwrap())),
            ("UNCOMPRESSED", Compression::UNCOMPRESSED),
        ];

        let mut results = Vec::new();

        for (name, compression) in compressions {

            let mut iteration_results = Vec::new();

            for _ in 0..test_config.iterations {
                let config = ParquetConfig::default().with_compression(compression);
                let (file_path, search_values) = generate_parquet_file(config)?;

                let test_result = run_performance_test(&file_path, &search_values).await?;
                let file_size = get_file_size_mb(&file_path)?;

                cleanup(&file_path)?;

                iteration_results.push((test_result, file_size));
            }

            // Average results
            let avg_result = TestResult::average(
                &iteration_results.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>()
            );
            let avg_size = iteration_results.iter().map(|(_, s)| s).sum::<f64>() / iteration_results.len() as f64;

            results.push(ComparisonRow {
                dimension: name.to_string(),
                file_size_mb: avg_size,
                index_build_time: avg_result.index_build_time,
                keyword_time: avg_result.keyword_time,
                datafusion_time: avg_result.datafusion_time,
                naive_time: avg_result.naive_time,
                rows_found: avg_result.rows_found,
            });
        }

        display_comparison_table(
            &results,
            "Compression Algorithm Comparison (500k rows, 5 row groups)",
            "Compression"
        );

        Ok(())
    }

    // ============================================================================
    // Test 2: Row Group Count Comparison
    // ============================================================================

    #[tokio::test]
    async fn test_row_group_comparison() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let test_config = TestConfig::default();

        let row_group_counts = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50];

        let mut results = Vec::new();

        for num_row_groups in row_group_counts {
            let mut iteration_results = Vec::new();

            for _ in 0..test_config.iterations {
                let config = ParquetConfig::default().with_num_row_groups(num_row_groups, 500_000 / num_row_groups);
                let (file_path, search_values) = generate_parquet_file(config)?;

                let test_result = run_performance_test(&file_path, &search_values).await?;
                let file_size = get_file_size_mb(&file_path)?;

                cleanup(&file_path)?;

                iteration_results.push((test_result, file_size));
            }

            let avg_result = TestResult::average(
                &iteration_results.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>()
            );
            let avg_size = iteration_results.iter().map(|(_, s)| s).sum::<f64>() / iteration_results.len() as f64;

            results.push(ComparisonRow {
                dimension: format!("{} RG", num_row_groups),
                file_size_mb: avg_size,
                index_build_time: avg_result.index_build_time,
                keyword_time: avg_result.keyword_time,
                datafusion_time: avg_result.datafusion_time,
                naive_time: avg_result.naive_time,
                rows_found: avg_result.rows_found,
            });
        }

        display_comparison_table(
            &results,
            "Row Group Count Comparison (500k total rows)",
            "Row Groups"
        );

        Ok(())
    }

    // ============================================================================
    // Test 3: String Pool Size (Cardinality) Comparison
    // ============================================================================

    #[tokio::test]
    async fn test_cardinality_comparison() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let test_config = TestConfig::default();

        let pool_sizes = vec![50, 500, 5_000, 50_000, 500_000];

        let mut results = Vec::new();

        for pool_size in pool_sizes {
            let mut iteration_results = Vec::new();

            for _ in 0..test_config.iterations {
                let config = ParquetConfig::default().with_string_pool_size(pool_size);
                let (file_path, search_values) = generate_parquet_file(config)?;

                let test_result = run_performance_test(&file_path, &search_values).await?;
                let file_size = get_file_size_mb(&file_path)?;

                cleanup(&file_path)?;

                iteration_results.push((test_result, file_size));
            }

            let avg_result = TestResult::average(
                &iteration_results.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>()
            );
            let avg_size = iteration_results.iter().map(|(_, s)| s).sum::<f64>() / iteration_results.len() as f64;

            results.push(ComparisonRow {
                dimension: format!("{}", pool_size),
                file_size_mb: avg_size,
                index_build_time: avg_result.index_build_time,
                keyword_time: avg_result.keyword_time,
                datafusion_time: avg_result.datafusion_time,
                naive_time: avg_result.naive_time,
                rows_found: avg_result.rows_found,
            });
        }

        display_comparison_table(
            &results,
            "String Pool Size (Cardinality) Comparison (500k rows)",
            "Pool Size"
        );

        Ok(())
    }

    // ============================================================================
    // Test 4: False Positive Scenario (Keywords Exist But Not Together)
    // ============================================================================

    #[tokio::test]
    async fn test_false_positive_scenario() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let test_config = TestConfig::default();

        let mut iteration_results = Vec::new();

        for _ in 0..test_config.iterations {
            let config = ParquetConfig::default()
                .with_scenario(DataScenario::FalsePositive);
            let (file_path, search_values) = generate_parquet_file(config)?;

            let test_result = run_performance_test(&file_path, &search_values).await?;
            let file_size = get_file_size_mb(&file_path)?;

            cleanup(&file_path)?;

            iteration_results.push((test_result, file_size));
        }

        let avg_result = TestResult::average(
            &iteration_results.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>()
        );

        println!("## False Positive Scenario Results\n");
        println!("**Scenario:** Each keyword exists in the data, but they never appear together in the same row.\n");
        println!("| Method | Time | Rows Found |");
        println!("|--------|------|------------|");
        println!("| Keyword Index | {:?} | {} |", avg_result.keyword_time, avg_result.rows_found);
        println!("| DataFusion | {:?} | {} |", avg_result.datafusion_time, avg_result.rows_found);
        println!("| Naive | {:?} | {} |", avg_result.naive_time, avg_result.rows_found);
        println!();
        println!("**Analysis:**");
        println!("- Keyword Index can quickly prune using combine_and (finds keywords but intersection is empty)");
        println!("- DataFusion and Naive must scan and filter all data");
        println!("- Expected rows found: 0 (keywords exist but never together)");
        println!();

        Ok(())
    }

    // ============================================================================
    // Test 5: Bloom Filter Miss Scenario (Keywords Don't Exist)
    // ============================================================================

    #[tokio::test]
    async fn test_bloom_miss_scenario() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let test_config = TestConfig::default();

        let mut iteration_results = Vec::new();

        for _ in 0..test_config.iterations {
            let config = ParquetConfig::default()
                .with_scenario(DataScenario::BloomMiss);
            let (file_path, search_values) = generate_parquet_file(config)?;

            let test_result = run_performance_test(&file_path, &search_values).await?;
            let file_size = get_file_size_mb(&file_path)?;

            cleanup(&file_path)?;

            iteration_results.push((test_result, file_size));
        }

        let avg_result = TestResult::average(
            &iteration_results.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>()
        );

        println!("## Bloom Filter Miss Scenario Results\n");
        println!("**Scenario:** Keywords don't exist in the data at all.\n");
        println!("| Method | Time | Rows Found |");
        println!("|--------|------|------------|");
        println!("| Keyword Index | {:?} | {} |", avg_result.keyword_time, avg_result.rows_found);
        println!("| DataFusion | {:?} | {} |", avg_result.datafusion_time, avg_result.rows_found);
        println!("| Naive | {:?} | {} |", avg_result.naive_time, avg_result.rows_found);
        println!();
        println!("**Analysis:**");
        println!("- Keyword Index rejects immediately via bloom filter (no file I/O needed)");
        println!("- DataFusion must still scan the file to confirm non-existence");
        println!("- Naive must read and filter all data");
        println!("- Expected rows found: 0 (keywords don't exist)");
        println!("- Speedup demonstrates bloom filter early rejection advantage");
        println!();

        Ok(())
    }
}