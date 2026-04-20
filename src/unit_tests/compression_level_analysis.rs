#[cfg(test)]
#[cfg(feature = "compression_comparison")]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use arrow::array::{ArrayRef, StringArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::basic::Compression;
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use std::fs::File;
    #[cfg(feature = "perf_generate_figures")]
    use serial_test::serial;
    use crate::build_and_save_index;
    use crate::index_data::CompressionAlgorithm;
    use crate::searching::keyword_search::KeywordSearcher;
    use crate::searching::search_results::SearchMode;

    // ────────────────────────────────────────────────────────────────────
    // Shared types and helpers
    // ────────────────────────────────────────────────────────────────────

    /// One search the analysis should run against every built index.
    /// `column = None` means a cross-column (aggregate) search.
    struct SearchSpec {
        label: String,
        keyword: String,
        column: Option<String>,
        mode: SearchMode,
    }

    /// Non-compression arguments to `build_and_save_index`. Compression is
    /// filled in per-config by the helper.
    #[derive(Clone, Default)]
    struct BuildParams {
        exclude_columns: Option<HashSet<String>>,
        error_rate: Option<f64>,
        split_chars: Option<Vec<Vec<char>>>,
        store_full_keyword_default: Option<bool>,
        full_keyword_column_exceptions: Option<HashSet<String>>,
        parent_tracking_threshold: Option<f64>,
        split_elimination_threshold: Option<f64>,
    }

    /// Per-level measurement. One row per compression config.
    struct CompressionResult {
        level: String,
        build_time_ms: u128,
        data_bin_size: u64,
        filters_size: u64,
        total_index_size: u64,
        compression_ratio: f64,
        per_search_time_us: Vec<u128>,
        total_search_time_us: u128,
    }

    /// Build the full compression-config list: `None` baseline, `Lz4`, plus
    /// every Zstd level from -7 through 22. Used by the synthetic test where
    /// build time per config is small enough to afford full coverage.
    fn full_config_list() -> Vec<(String, CompressionAlgorithm, CompressionAlgorithm)> {
        let mut configs: Vec<(String, CompressionAlgorithm, CompressionAlgorithm)> = Vec::new();
        configs.push((
            "None".to_string(),
            CompressionAlgorithm::None,
            CompressionAlgorithm::None,
        ));
        configs.push((
            "Lz4".to_string(),
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Lz4,
        ));
        for level in -7..=22 {
            configs.push((
                format!("Zstd:{}", level),
                CompressionAlgorithm::Zstd { level },
                CompressionAlgorithm::Zstd { level },
            ));
        }
        configs
    }

    /// A representative subset suitable for expensive sources (e.g. yellow
    /// taxi) — build + measure across enough configs to see the curve without
    /// burning full wall time across every zstd level. Includes `None`, `Lz4`,
    /// and the fast/default zstd anchors.
    ///
    /// The slowest levels (Zstd:15 and Zstd:22) are gated behind the
    /// `bigzstd` feature because each can take minutes per build on real-
    /// world data without moving the compression ratio meaningfully.
    fn representative_config_list() -> Vec<(String, CompressionAlgorithm, CompressionAlgorithm)> {
        #[allow(unused_mut)]
        let mut configs = vec![
            ("None".to_string(), CompressionAlgorithm::None, CompressionAlgorithm::None),
            ("Lz4".to_string(), CompressionAlgorithm::Lz4, CompressionAlgorithm::Lz4),
            ("Zstd:1".to_string(), CompressionAlgorithm::Zstd { level: 1 }, CompressionAlgorithm::Zstd { level: 1 }),
            ("Zstd:3".to_string(), CompressionAlgorithm::Zstd { level: 3 }, CompressionAlgorithm::Zstd { level: 3 }),
            ("Zstd:8".to_string(), CompressionAlgorithm::Zstd { level: 8 }, CompressionAlgorithm::Zstd { level: 8 }),
        ];
        #[cfg(feature = "bigzstd")]
        {
            configs.push(("Zstd:15".to_string(), CompressionAlgorithm::Zstd { level: 15 }, CompressionAlgorithm::Zstd { level: 15 }));
            configs.push(("Zstd:22".to_string(), CompressionAlgorithm::Zstd { level: 22 }, CompressionAlgorithm::Zstd { level: 22 }));
        }
        configs
    }

    /// For each compression config, build the index with that config, measure
    /// `data.bin` / `filters.rkyv` sizes, then load a searcher and run every
    /// `SearchSpec` measuring wall time per search. Returns one
    /// `CompressionResult` per config.
    ///
    /// The baseline for `compression_ratio` is whichever config is named
    /// exactly `"None"` (defaults to 1.0 if no baseline is present).
    async fn measure_compression_configs(
        parquet_file_path: &str,
        build_params: &BuildParams,
        searches: &[SearchSpec],
        configs: &[(String, CompressionAlgorithm, CompressionAlgorithm)],
    ) -> Result<Vec<CompressionResult>, Box<dyn std::error::Error + Send + Sync>> {
        use crate::utils::file_interaction_local_and_cloud::get_object_store;

        let mut results = Vec::new();
        let mut baseline_size: Option<u64> = None;

        for (idx, (level_name, keywords_comp, data_comp)) in configs.iter().enumerate() {
            let prefix = format!("compression_test_{}_", idx);

            print!("  {:<10} ... ", level_name);
            std::io::Write::flush(&mut std::io::stdout())?;

            // Build index with this compression config
            let build_start = Instant::now();
            build_and_save_index(
                parquet_file_path,
                build_params.exclude_columns.clone(),
                build_params.error_rate,
                Some(&prefix),
                Some(*keywords_comp),
                Some(*data_comp),
                build_params.split_chars.clone(),
                build_params.store_full_keyword_default,
                build_params.full_keyword_column_exceptions.clone(),
                build_params.parent_tracking_threshold,
                build_params.split_elimination_threshold,
            ).await?;
            let build_time = build_start.elapsed();

            // Measure index file sizes
            let data_bin_path = format!("{}.index/{}data.bin", parquet_file_path, prefix);
            let filters_path = format!("{}.index/{}filters.rkyv", parquet_file_path, prefix);

            let (store, data_path) = get_object_store(&data_bin_path).await?;
            let data_bin_size = store.head(&data_path).await?.size;

            let (store, filters_obj_path) = get_object_store(&filters_path).await?;
            let filters_size = store.head(&filters_obj_path).await?.size;

            let total_index_size = data_bin_size + filters_size;

            if level_name == "None" {
                baseline_size = Some(total_index_size);
            }

            let compression_ratio = baseline_size
                .map(|b| b as f64 / total_index_size as f64)
                .unwrap_or(1.0);

            // Run searches against a freshly-loaded searcher so I/O / decompress
            // costs are paid from cold, matching a real one-shot query.
            let searcher = KeywordSearcher::load(parquet_file_path, Some(&prefix)).await?;

            let mut per_search_time_us = Vec::with_capacity(searches.len());
            let mut total_search = Duration::ZERO;
            for spec in searches {
                let s_start = Instant::now();
                let _result = searcher.search_with_mode(
                    &spec.keyword,
                    spec.column.as_deref(),
                    false,
                    spec.mode,
                ).await?;
                let elapsed = s_start.elapsed();
                per_search_time_us.push(elapsed.as_micros());
                total_search += elapsed;
            }

            println!(
                "build {:>7.2?} | data.bin {:>7.2} MB | total {:>7.2} MB | ratio {:>5.2}x | search {:>7.2?}",
                build_time,
                data_bin_size as f64 / (1024.0 * 1024.0),
                total_index_size as f64 / (1024.0 * 1024.0),
                compression_ratio,
                total_search,
            );

            results.push(CompressionResult {
                level: level_name.clone(),
                build_time_ms: build_time.as_millis(),
                data_bin_size,
                filters_size,
                total_index_size,
                compression_ratio,
                per_search_time_us,
                total_search_time_us: total_search.as_micros(),
            });
        }

        Ok(results)
    }

    /// Pretty-print the headline build/size/search table plus per-search
    /// breakdown and the analysis callouts (best ratio, best balance, savings
    /// relative to None baseline).
    fn print_compression_analysis(
        results: &[CompressionResult],
        searches: &[SearchSpec],
        title: &str,
    ) {
        println!("\n=== {} ===\n", title);

        // Main table
        println!("┌────────────┬──────────────┬──────────────┬──────────────┬──────────────┬────────────┬──────────────┐");
        println!("│ Level      │ Build Time   │ data.bin     │ filters.rkyv │ Total Index  │ Ratio      │ Search Total │");
        println!("├────────────┼──────────────┼──────────────┼──────────────┼──────────────┼────────────┼──────────────┤");
        for r in results {
            println!(
                "│ {:<10} │ {:>10} ms │ {:>10.2} MB │ {:>10.2} MB │ {:>10.2} MB │ {:>8.2}x │ {:>10.2} ms │",
                r.level,
                r.build_time_ms,
                r.data_bin_size as f64 / (1024.0 * 1024.0),
                r.filters_size as f64 / (1024.0 * 1024.0),
                r.total_index_size as f64 / (1024.0 * 1024.0),
                r.compression_ratio,
                r.total_search_time_us as f64 / 1000.0,
            );
        }
        println!("└────────────┴──────────────┴──────────────┴──────────────┴──────────────┴────────────┴──────────────┘");

        // Per-search breakdown
        println!("\nPer-search wall time (ms):");
        let label_col_width = searches.iter().map(|s| s.label.len()).max().unwrap_or(0).max(8);
        print!("  {:<width$}", "Level", width = 10);
        for spec in searches {
            print!(" │ {:>width$}", spec.label, width = label_col_width.max(8));
        }
        println!();
        print!("  {:-<width$}", "", width = 10);
        for _ in searches {
            print!("─┼─{:─<width$}", "", width = label_col_width.max(8));
        }
        println!();
        for r in results {
            print!("  {:<10}", r.level);
            for us in &r.per_search_time_us {
                print!(" │ {:>width$.2}", *us as f64 / 1000.0, width = label_col_width.max(8));
            }
            println!();
        }

        // Analysis callouts
        println!("\n=== Analysis ===\n");

        let good_compression_threshold = 2.0;
        if let Some(best) = results.iter()
            .filter(|r| r.level != "None" && r.compression_ratio >= good_compression_threshold)
            .min_by_key(|r| r.build_time_ms)
        {
            println!(
                "Fastest level with >{:.1}x compression: {} ({} ms build, {:.2}x ratio)",
                good_compression_threshold, best.level, best.build_time_ms, best.compression_ratio
            );
        }

        if let Some(best) = results.iter()
            .filter(|r| r.level != "None")
            .max_by(|a, b| a.compression_ratio.partial_cmp(&b.compression_ratio).unwrap())
        {
            println!(
                "Best compression ratio: {} ({:.2}x, {} ms build)",
                best.level, best.compression_ratio, best.build_time_ms
            );
        }

        if let Some(fastest_search) = results.iter()
            .min_by_key(|r| r.total_search_time_us)
        {
            println!(
                "Fastest total search time: {} ({:.2} ms)",
                fastest_search.level, fastest_search.total_search_time_us as f64 / 1000.0
            );
        }

        // Relative cost vs None
        if let Some(none) = results.iter().find(|r| r.level == "None") {
            println!("\nSize + search time vs None (uncompressed) baseline:");
            for r in results.iter().filter(|r| r.level != "None") {
                let size_pct = (1.0 - (r.total_index_size as f64 / none.total_index_size as f64)) * 100.0;
                let search_overhead_pct = ((r.total_search_time_us as f64 / none.total_search_time_us as f64) - 1.0) * 100.0;
                println!(
                    "  {:<10} {:>5.1}% smaller index, {:>+5.1}% search time",
                    r.level, size_pct, search_overhead_pct
                );
            }
        }
    }

    // ────────────────────────────────────────────────────────────────────
    // Test 1: synthetic 500k-row random-string parquet
    // ────────────────────────────────────────────────────────────────────

    #[cfg_attr(feature = "compression_comparison", tokio::test)]
    #[cfg_attr(feature = "perf_generate_figures", serial)]
    async fn test_compression_level_analysis() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("\n=== Compression Level Analysis (synthetic) ===\n");
        println!("Measures build time, index size, and search wall time across");
        println!("Zstd compression levels on 500k-row synthetic parquet.\n");

        // Generate random string pool
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

        // Pick a handful of pool entries up front to use as search targets so
        // they're guaranteed to exist in the data.  Mix of specific-column
        // and aggregate (no column filter) searches.
        let search_keyword_hot = string_pool[0].clone();
        let search_keyword_warm = string_pool[2500].clone();
        let search_keyword_cold = string_pool[4999].clone();

        // Create schema + parquet
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

        let file_path = std::env::temp_dir()
            .join("test_compression_analysis.parquet")
            .to_string_lossy()
            .to_string();

        println!("Writing parquet: {}", file_path);
        let file = File::create(&file_path)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_size(100_000)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        for rg in 0..5 {
            let mut columns: Vec<ArrayRef> = Vec::new();
            for _ in 0..10 {
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
                println!("  wrote {} row groups", rg + 1);
            }
        }
        writer.close()?;

        let parquet_size_bytes = std::fs::metadata(&file_path)?.len();
        println!(
            "Parquet file: {} bytes ({:.2} MB)\n",
            parquet_size_bytes,
            parquet_size_bytes as f64 / (1024.0 * 1024.0)
        );

        let searches = vec![
            SearchSpec {
                label: "hot/col_0".to_string(),
                keyword: search_keyword_hot,
                column: Some("col_0".to_string()),
                mode: SearchMode::Contains,
            },
            SearchSpec {
                label: "warm/agg".to_string(),
                keyword: search_keyword_warm,
                column: None,
                mode: SearchMode::Contains,
            },
            SearchSpec {
                label: "cold/col_9".to_string(),
                keyword: search_keyword_cold,
                column: Some("col_9".to_string()),
                mode: SearchMode::Contains,
            },
        ];

        let build_params = BuildParams {
            error_rate: Some(0.01),
            ..Default::default()
        };

        let configs = full_config_list();
        println!("Testing {} compression configs...\n", configs.len());

        let results = measure_compression_configs(
            &file_path,
            &build_params,
            &searches,
            &configs,
        ).await?;

        print_compression_analysis(
            &results,
            &searches,
            "Compression Analysis Results (synthetic 500k rows × 10 cols)",
        );

        // Cleanup
        std::fs::remove_file(&file_path)?;
        std::fs::remove_dir_all(format!("{}.index", file_path))?;

        Ok(())
    }

    // ────────────────────────────────────────────────────────────────────
    // Test 2: real-world parquet — NYC Yellow Taxi April 2020
    // ────────────────────────────────────────────────────────────────────

    const YELLOW_TAXI_URL: &str =
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-04.parquet";

    #[cfg_attr(feature = "compression_comparison", tokio::test)]
    #[cfg_attr(feature = "perf_generate_figures", serial)]
    async fn test_yellow_taxi_compression_level_analysis() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("\n=== Compression Level Analysis (NYC Yellow Taxi April 2020) ===\n");
        println!("Measures build time, index size, and search wall time across");
        println!("a representative set of Zstd levels on a real-world parquet.\n");

        // Download once
        println!("Downloading {}...", YELLOW_TAXI_URL);
        let response = reqwest::get(YELLOW_TAXI_URL).await?;
        let parquet_bytes = response.bytes().await?;
        println!(
            "  {:.2} MB received",
            parquet_bytes.len() as f64 / (1024.0 * 1024.0)
        );

        let file_path = std::env::temp_dir()
            .join("yellow_taxi_compression_analysis.parquet")
            .to_string_lossy()
            .to_string();
        std::fs::write(&file_path, &parquet_bytes)?;

        // Same three-column query shape as test_yellow_taxi_three_column_and_vs_datafusion,
        // so the search-time numbers here line up with the rest of the yellow_taxi perf
        // tests — any movement across compression levels tells us directly how much of
        // the 3-column AND index-phase cost is decompression-bound.
        let searches = vec![
            SearchSpec {
                label: "passenger=1".to_string(),
                keyword: "1".to_string(),
                column: Some("passenger_count".to_string()),
                mode: SearchMode::Equals,
            },
            SearchSpec {
                label: "vendor=2".to_string(),
                keyword: "2".to_string(),
                column: Some("VendorID".to_string()),
                mode: SearchMode::Equals,
            },
            SearchSpec {
                label: "payment=1".to_string(),
                keyword: "1".to_string(),
                column: Some("payment_type".to_string()),
                mode: SearchMode::Equals,
            },
        ];

        // Match the rest of the yellow_taxi test suite so split-elimination
        // fires on the high-frequency columns.
        let build_params = BuildParams {
            exclude_columns: Some(HashSet::from([
                "tpep_pickup_datetime".to_string(),
                "tpep_dropoff_datetime".to_string(),
            ])),
            error_rate: Some(0.01),
            store_full_keyword_default: Some(true),
            parent_tracking_threshold: Some(0.2),
            split_elimination_threshold: Some(0.2),
            ..Default::default()
        };

        let configs = representative_config_list();
        println!("Testing {} representative compression configs...\n", configs.len());

        let results = measure_compression_configs(
            &file_path,
            &build_params,
            &searches,
            &configs,
        ).await?;

        print_compression_analysis(
            &results,
            &searches,
            "Compression Analysis Results (NYC Yellow Taxi April 2020)",
        );

        // Cleanup
        std::fs::remove_file(&file_path)?;
        std::fs::remove_dir_all(format!("{}.index", file_path))?;

        Ok(())
    }
}
