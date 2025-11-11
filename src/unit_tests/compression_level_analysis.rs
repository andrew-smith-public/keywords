#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;
    use arrow::array::{ArrayRef, StringArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::basic::Compression;
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use std::fs::File;
    use crate::build_and_save_index;
    use crate::index_data::CompressionAlgorithm;
    use serial_test::serial;

    #[cfg_attr(feature = "perf_detail", tokio::test)]
    #[cfg_attr(feature = "perf_detail", serial)]
    async fn test_compression_level_analysis() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("\n=== Compression Level Analysis ===\n");
        println!("This test measures index build time and data.bin size for each Zstd compression level.");
        println!("Goal: Find optimal balance between compression ratio and build time.\n");

        // Step 1: Generate random string pool (5000 strings, 10-20 chars)
        println!("Generating random string pool...");
        let mut rng = rand::rng();
        let string_pool: Vec<String> = (0..5000)
            .map(|_| {
                let len = rng.random_range(10..=20);
                std::iter::repeat_with(|| rng.sample(Alphanumeric) as char)
                    .filter(|c| c.is_alphabetic())
                    .take(len)
                    .collect()
            })
            .collect();

        // Step 2: Create schema with 10 columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Utf8, false),
            Field::new("col_1", DataType::Utf8, false),
            Field::new("col_2", DataType::Utf8, false),
            Field::new("col_3", DataType::Utf8, false),
            Field::new("col_4", DataType::Utf8, false),
            Field::new("col_5", DataType::Utf8, false),
            Field::new("col_6", DataType::Utf8, false),
            Field::new("col_7", DataType::Utf8, false),
            Field::new("col_8", DataType::Utf8, false),
            Field::new("col_9", DataType::Utf8, false),
        ]));

        // Step 3: Create parquet file
        let file_path = std::env::temp_dir()
            .join("test_compression_analysis.parquet")
            .to_string_lossy()
            .to_string();

        println!("Creating parquet file: {}", file_path);
        let file = File::create(&file_path)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_size(100_000)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // Step 4: Write 5 row groups with 100,000 rows each (500k rows total)
        println!("Writing 5 row groups with 100,000 rows each (500,000 rows total)...");
        for rg in 0..5 {
            let mut columns: Vec<ArrayRef> = Vec::new();

            for _col_idx in 0..10 {
                let values: Vec<String> = (0..100_000)
                    .map(|_| {
                        let idx = rng.random_range(0..string_pool.len());
                        string_pool[idx].clone()
                    })
                    .collect();

                columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
            }

            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            writer.write(&batch)?;

            if (rg + 1) % 5 == 0 {
                println!("  Written {} row groups", rg + 1);
            }
        }

        writer.close()?;

        // Print parquet file size
        let file_metadata = std::fs::metadata(&file_path)?;
        let parquet_size_bytes = file_metadata.len();
        let parquet_size_mb = parquet_size_bytes as f64 / (1024.0 * 1024.0);
        println!("Parquet file created: 500,000 rows total");
        println!("Parquet file size: {} bytes ({:.2} MB)\n", parquet_size_bytes, parquet_size_mb);

        // Step 5: Test each compression level
        println!("Testing compression levels...\n");

        #[derive(Debug)]
        struct CompressionResult {
            level: String,
            build_time_ms: u128,
            data_bin_size: u64,
            filters_size: u64,
            total_index_size: u64,
            compression_ratio: f64,
        }

        let mut results = Vec::new();

        // Test configurations: None, and Zstd levels 1-22
        let mut configs: Vec<(String, CompressionAlgorithm, CompressionAlgorithm)> = Vec::new();

        // Add "None" as baseline
        configs.push((
            "None".to_string(),
            CompressionAlgorithm::None,
            CompressionAlgorithm::None
        ));

        // Add Zstd levels 1-22
        for level in -7..=22 {
            configs.push((
                format!("Zstd:{}", level),
                CompressionAlgorithm::Zstd { level },
                CompressionAlgorithm::Zstd { level }
            ));
        }

        let baseline_size = Arc::new(std::sync::Mutex::new(None));

        for (idx, (level_name, keywords_comp, data_comp)) in configs.iter().enumerate() {
            let prefix = format!("compression_test_{}_", idx);

            print!("Testing {:<10} ... ", level_name);
            std::io::Write::flush(&mut std::io::stdout())?;

            // Build index with this compression level
            let build_start = Instant::now();
            build_and_save_index(
                &file_path,
                None,
                Some(0.01),
                Some(&prefix),
                Some(*keywords_comp),
                Some(*data_comp)
            ).await?;
            let build_time = build_start.elapsed();

            // Measure index file sizes
            use crate::utils::file_interaction_local_and_cloud::get_object_store;

            let data_bin_path = format!("{}.index/{}data.bin", file_path, prefix);
            let filters_path = format!("{}.index/{}filters.rkyv", file_path, prefix);

            let (store, data_path) = get_object_store(&data_bin_path).await?;
            let data_meta = store.head(&data_path).await?;
            let data_bin_size = data_meta.size;

            let (store, filters_obj_path) = get_object_store(&filters_path).await?;
            let filters_meta = store.head(&filters_obj_path).await?;
            let filters_size = filters_meta.size;

            let total_index_size = data_bin_size + filters_size;

            // Store baseline (None) size for comparison
            if level_name == "None" {
                *baseline_size.lock().unwrap() = Some(total_index_size);
            }

            let compression_ratio = if let Some(baseline) = *baseline_size.lock().unwrap() {
                baseline as f64 / total_index_size as f64
            } else {
                1.0
            };

            println!("done in {:>7.2?} | data.bin: {:>7.2} MB | total: {:>7.2} MB | ratio: {:>5.2}x",
                     build_time,
                     data_bin_size as f64 / (1024.0 * 1024.0),
                     total_index_size as f64 / (1024.0 * 1024.0),
                     compression_ratio);

            results.push(CompressionResult {
                level: level_name.clone(),
                build_time_ms: build_time.as_millis(),
                data_bin_size,
                filters_size,
                total_index_size,
                compression_ratio,
            });
        }

        println!("\n=== Compression Analysis Results ===\n");

        // Print detailed table
        println!("┌────────────┬──────────────┬──────────────┬──────────────┬──────────────┬────────────┐");
        println!("│ Level      │ Build Time   │ data.bin     │ filters.rkyv │ Total Index  │ Ratio      │");
        println!("├────────────┼──────────────┼──────────────┼──────────────┼──────────────┼────────────┤");

        for result in &results {
            println!("│ {:<10} │ {:>10} ms │ {:>10.2} MB │ {:>10.2} MB │ {:>10.2} MB │ {:>8.2}x │",
                     result.level,
                     result.build_time_ms,
                     result.data_bin_size as f64 / (1024.0 * 1024.0),
                     result.filters_size as f64 / (1024.0 * 1024.0),
                     result.total_index_size as f64 / (1024.0 * 1024.0),
                     result.compression_ratio);
        }

        println!("└────────────┴──────────────┴──────────────┴──────────────┴──────────────┴────────────┘");

        // Analysis: Find sweet spots
        println!("\n=== Analysis ===\n");

        // Find fastest compression that still gives good ratio (> 2x)
        let good_compression_threshold = 2.0;
        let fast_good_compression = results.iter()
            .filter(|r| r.level != "None" && r.compression_ratio >= good_compression_threshold)
            .min_by_key(|r| r.build_time_ms);

        if let Some(best) = fast_good_compression {
            println!("Fastest level with >{:.1}x compression: {} ({} ms, {:.2}x ratio)",
                     good_compression_threshold,
                     best.level,
                     best.build_time_ms,
                     best.compression_ratio);
        }

        // Find best compression ratio
        let best_compression = results.iter()
            .filter(|r| r.level != "None")
            .max_by(|a, b| a.compression_ratio.partial_cmp(&b.compression_ratio).unwrap());

        if let Some(best) = best_compression {
            println!("Best compression ratio: {} ({:.2}x, {} ms)",
                     best.level,
                     best.compression_ratio,
                     best.build_time_ms);
        }

        // Find best balance (consider both time and size)
        // Score = compression_ratio / (build_time_ms / baseline_time_ms)
        let baseline_time = results[0].build_time_ms as f64;
        let mut scored_results: Vec<_> = results.iter()
            .filter(|r| r.level != "None")
            .map(|r| {
                let time_factor = r.build_time_ms as f64 / baseline_time;
                let score = r.compression_ratio / time_factor;
                (r, score)
            })
            .collect();

        scored_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        if let Some((best, score)) = scored_results.first() {
            println!("Best balance (ratio/time): {} (score: {:.3}, {:.2}x ratio, {} ms)",
                     best.level,
                     score,
                     best.compression_ratio,
                     best.build_time_ms);
        }

        // Time cost analysis
        println!("\nTime Cost Analysis:");
        let none_time = results[0].build_time_ms;
        for result in results.iter().skip(1).take(5) {
            let overhead = ((result.build_time_ms as f64 / none_time as f64) - 1.0) * 100.0;
            println!("  {:<10} +{:>5.1}% build time for {:>5.2}x compression",
                     result.level,
                     overhead,
                     result.compression_ratio);
        }

        // Size savings analysis
        println!("\nSize Savings Analysis:");
        let none_size = results[0].total_index_size;
        for result in results.iter().skip(1).take(5) {
            let savings = none_size - result.total_index_size;
            let savings_pct = (savings as f64 / none_size as f64) * 100.0;
            println!("  {:<10} saves {:>6.2} MB ({:>5.1}% reduction)",
                     result.level,
                     savings as f64 / (1024.0 * 1024.0),
                     savings_pct);
        }

        println!("\n=== Recommendations ===\n");
        println!("Current default: Zstd:15");
        println!();
        println!("Consider these trade-offs:");
        println!("  • Fast indexing, good compression: Zstd:3-6");
        println!("  • Balanced: Zstd:10-15");
        println!("  • Maximum compression: Zstd:20-22 (slow but smallest)");
        println!("  • No compression: Only if build speed is critical");

        // Clean up: delete the test file and all index variants
        std::fs::remove_file(&file_path)?;
        std::fs::remove_dir_all(format!("{}.index", file_path))?;

        Ok(())
    }
}