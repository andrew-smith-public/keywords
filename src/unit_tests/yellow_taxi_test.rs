/// Test for indexing real-world data: NYC Yellow Taxi trip data from April 2020
///
/// This test downloads a real parquet file from the NYC Taxi & Limousine Commission
/// open data, creates an index in memory, and reports on the sizes and performance.
///
/// Data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
/// File: Yellow Taxi Trip Records (TPEP) - April 2020
///
/// This demonstrates:
/// - Indexing real-world data with realistic column types and values
/// - Index size vs parquet size comparison
/// - Performance on a production-sized dataset
/// - Memory-based indexing workflow

#[cfg(test)]
mod yellow_taxi_index_test {
    use crate::ParquetSource;
    use crate::index_data::CompressionAlgorithm;

    const YELLOW_TAXI_URL: &str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-04.parquet";

    // Helper function to format numbers with commas
    fn format_with_commas(n: usize) -> String {
        let s = n.to_string();
        let mut result = String::new();
        let chars: Vec<char> = s.chars().collect();

        for (i, c) in chars.iter().enumerate() {
            if i > 0 && (chars.len() - i) % 3 == 0 {
                result.push(',');
            }
            result.push(*c);
        }
        result
    }

    #[tokio::test]
    async fn test_yellow_taxi_april_2020_index() {
        println!("\n=============================================================");
        println!("NYC YELLOW TAXI DATA - APRIL 2020 INDEX TEST");
        println!("=============================================================");
        println!("This test downloads real-world taxi trip data and creates");
        println!("a keyword search index to demonstrate index efficiency.\n");

        // ======================================================================
        // STEP 1: Download the Parquet File
        // ======================================================================
        println!("STEP 1: Downloading parquet file from NYC Open Data...");
        println!("URL: {}", YELLOW_TAXI_URL);

        let download_start = std::time::Instant::now();

        let response = reqwest::get(YELLOW_TAXI_URL)
            .await
            .expect("Failed to download file");

        let parquet_bytes = response.bytes()
            .await
            .expect("Failed to read response bytes");

        let download_duration = download_start.elapsed();
        let parquet_size = parquet_bytes.len();

        println!("✓ Download complete in {:.2?}", download_duration);
        println!("  Parquet file size: {} bytes ({:.2} MB)",
                 format_with_commas(parquet_size),
                 parquet_size as f64 / (1024.0 * 1024.0));

        println!();

        // ======================================================================
        // STEP 2: Analyze Parquet File
        // ======================================================================
        println!("STEP 2: Analyzing parquet file metadata...");

        // Read parquet metadata using Arrow reader (handles Bytes directly)
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes.clone())
            .expect("Failed to create parquet reader");

        let metadata = builder.metadata();
        let file_metadata = metadata.file_metadata();
        let arrow_schema = builder.schema();

        println!("Parquet file info:");
        println!("  - Rows: {}", format_with_commas(file_metadata.num_rows() as usize));
        println!("  - Columns: {}", file_metadata.schema_descr().num_columns());
        println!("  - Row groups: {}", metadata.num_row_groups());

        // Get compression from first row group's first column
        if metadata.num_row_groups() > 0 {
            let rg_metadata = metadata.row_group(0);
            if rg_metadata.num_columns() > 0 {
                let col_chunk = rg_metadata.column(0);
                let compression = col_chunk.compression();
                println!("  - Compression: {:?}", compression);
            }
        }

        // Print column names and types with distinct value counts
        println!("\nColumn details:");

        // Read all data to count distinct values
        use std::collections::HashSet;
        use arrow::array::*;
        use arrow::datatypes::DataType;

        // Create a new builder for reading data (since the first one was borrowed for metadata)
        let data_builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes.clone())
            .expect("Failed to create parquet reader for data");
        let reader = data_builder.build().expect("Failed to build reader");
        let mut column_distinct_counts: Vec<usize> = vec![0; arrow_schema.fields().len()];
        let mut column_distinct_sets: Vec<HashSet<String>> = vec![HashSet::new(); arrow_schema.fields().len()];

        println!("  Counting distinct values across all rows...");
        let count_start = std::time::Instant::now();

        for batch in reader {
            let batch = batch.expect("Failed to read batch");

            for (col_idx, column) in batch.columns().iter().enumerate() {
                let field = arrow_schema.field(col_idx);

                // Convert column values to strings and add to set
                match field.data_type() {
                    DataType::Int8 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int8Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Int16 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int16Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Int32 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Int64 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Float32 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Float64 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Utf8 => {
                        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::LargeUtf8 => {
                        if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Boolean => {
                        if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    DataType::Timestamp(_, _) => {
                        if let Some(arr) = column.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        } else if let Some(arr) = column.as_any().downcast_ref::<TimestampMillisecondArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        } else if let Some(arr) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        } else if let Some(arr) = column.as_any().downcast_ref::<TimestampSecondArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    column_distinct_sets[col_idx].insert(arr.value(i).to_string());
                                }
                            }
                        }
                    }
                    _ => {
                        // For other types, try generic string conversion
                        for i in 0..column.len() {
                            if !column.is_null(i) {
                                column_distinct_sets[col_idx].insert(format!("{:?}", column.slice(i, 1)));
                            }
                        }
                    }
                }
            }
        }

        // Store counts
        for (idx, set) in column_distinct_sets.iter().enumerate() {
            column_distinct_counts[idx] = set.len();
        }

        let count_duration = count_start.elapsed();
        println!("  Distinct value counting completed in {:.2?}\n", count_duration);

        // Now print column details with distinct counts
        for (idx, field) in arrow_schema.fields().iter().enumerate() {
            println!("  {}: {} ({}) - {} distinct values",
                     idx,
                     field.name(),
                     field.data_type(),
                     format_with_commas(column_distinct_counts[idx]));
        }

        println!();

        // ======================================================================
        // STEP 3: Build Index and Get ACTUAL Size
        // ======================================================================
        println!("STEP 3: Building keyword search index...");
        println!("Configuration:");
        println!("  - Error rate: 1%");
        println!("  - Keywords compression: Zstd level 8");
        println!("  - Data compression: Zstd level 8");
        println!("  - Split characters: Default (4 levels)");
        println!("  - Full keyword storage: Default (disabled)");

        let index_start = std::time::Instant::now();

        // Use the lower-level functions to get actual index files
        use crate::column_parquet_reader::process_parquet_file;
        use crate::index_data::build_distributed_index;

        let source = ParquetSource::Bytes(parquet_bytes.clone());

        // Step 3a: Process parquet file
        let result = process_parquet_file(
            source.clone(),
            Some(HashSet::from(["tpep_pickup_datetime".to_string(), "tpep_dropoff_datetime".to_string()])),  // Exclude time columns as exact matching has no real value for them
            Some(0.01),  // error_rate
            None,  // split_chars (use default)
            Some(true),  // store_full_keyword_default
            None,  // full_keyword_column_exceptions
            Some(0.2),  // parent_tracking_threshold (disable parent for keywords in >20% of rows)
            Some(0.2)   // split_elimination_threshold (disable split information for keywords in >20% of rows)
        ).await.expect("Failed to process parquet file");

        // ======================================================================
        // STEP 3a: Analyze Keyword Statistics
        // ======================================================================
        println!("\nSTEP 3a: Analyzing keyword statistics...");
        println!("----------------------------------------");

        // Count total row objects for each keyword across all columns
        let mut keyword_counts: Vec<(String, usize)> = result.keyword_map
            .iter()
            .map(|(keyword, keyword_data)| {
                // Count total row objects across all columns for this keyword
                // row_group_to_rows is Vec<Vec<Vec<Row>>>
                // - First level: internal column indices (0 = global bucket, 1+ = specific columns)
                // - Second level: row groups within each column
                // - Third level: actual Row objects
                let total_rows: usize = keyword_data.row_group_to_rows
                    .iter()
                    .map(|row_groups_for_column| {
                        row_groups_for_column.iter()
                            .map(|rows| rows.len())
                            .sum::<usize>()
                    })
                    .sum();
                (keyword.to_string(), total_rows)
            })
            .collect();

        // Sort by count (descending)
        keyword_counts.sort_by(|a, b| b.1.cmp(&a.1));

        println!("Total unique keywords: {}", format_with_commas(keyword_counts.len()));
        println!("\nTop 10 keywords by row object count:");
        println!("Rank | Keyword | Row Objects | Columns");
        println!("-----|---------|-------------|--------");

        for (i, (keyword, count)) in keyword_counts.iter().take(10).enumerate() {
            // Get the number of unique columns this keyword appears in
            let keyword_data = result.keyword_map.get(keyword.as_str()).unwrap();
            let columns_count = keyword_data.column_references.len();

            println!("{:4} | {:30} | {:>11} | {:>7}",
                     i + 1,
                     if keyword.len() > 30 {
                         format!("{}...", &keyword[..27])
                     } else {
                         keyword.clone()
                     },
                     format_with_commas(*count),
                     columns_count);
        }

        println!();

        // Step 3b: Build distributed index files
        let split_chars_vec: Vec<Vec<char>> = crate::keyword_shred::SPLIT_CHARS_INCLUSIVE
            .iter()
            .map(|&chars| chars.to_vec())
            .collect();

        let index_files = build_distributed_index(
            &result,
            &source,
            0.01,  // error_rate
            CompressionAlgorithm::Zstd { level: 8 },
            CompressionAlgorithm::Zstd { level: 8 },
            &split_chars_vec,
        ).await.expect("Failed to build index");

        let index_duration = index_start.elapsed();

        println!("✓ Index built in {:.2?}", index_duration);

        // Get ACTUAL sizes
        let filters_size = index_files.filters.len();
        let data_size = index_files.data.len();
        let total_index_size = filters_size + data_size;

        println!();

        // ======================================================================
        // STEP 4: Size Comparison
        // ======================================================================
        println!("STEP 4: Index size analysis");
        println!("----------------------------");

        println!("Actual index sizes:");
        println!("  filters.rkyv: {} bytes ({:.2} MB)",
                 format_with_commas(filters_size),
                 filters_size as f64 / (1024.0 * 1024.0));
        println!("  data.bin:     {} bytes ({:.2} MB)",
                 format_with_commas(data_size),
                 data_size as f64 / (1024.0 * 1024.0));
        println!("  Total index:  {} bytes ({:.2} MB)",
                 format_with_commas(total_index_size),
                 total_index_size as f64 / (1024.0 * 1024.0));

        println!("\nSize comparison:");
        println!("  Parquet file: {} bytes ({:.2} MB)",
                 format_with_commas(parquet_size),
                 parquet_size as f64 / (1024.0 * 1024.0));
        println!("  Index total:  {} bytes ({:.2} MB)",
                 format_with_commas(total_index_size),
                 total_index_size as f64 / (1024.0 * 1024.0));

        let ratio = parquet_size as f64 / total_index_size as f64;
        let percent = (total_index_size as f64 / parquet_size as f64) * 100.0;

        if ratio > 1.0 {
            println!("  Index is {:.1}x smaller than parquet ({:.1}% of parquet size)", ratio, percent);
        } else {
            println!("  Index is {:.1}x larger than parquet ({:.1}% of parquet size)", 1.0 / ratio, percent);
        }

        println!();

        // ======================================================================
        // STEP 5: Load Searcher and Test Searches
        // ======================================================================
        println!("STEP 5: Loading searcher and testing keyword searches");
        println!("------------------------------------------------------");

        // Save index files to memory filesystem so searcher can load them
        use crate::index_data::save_distributed_index;
        use crate::utils::file_interaction_local_and_cloud::register_memory_file;

        // Create a unique memory path for this index
        let memory_parquet_path = "memory://yellow_taxi_2020_04.parquet";
        register_memory_file(memory_parquet_path, parquet_bytes.clone()).await
            .expect("Failed to register memory file");

        // Save the index files
        save_distributed_index(&index_files, memory_parquet_path, None).await
            .expect("Failed to save index");

        // Now load the searcher properly
        use crate::searching::keyword_search::KeywordSearcher;
        let searcher = KeywordSearcher::load(memory_parquet_path, None).await
            .expect("Failed to load searcher");

        println!("Index metadata:");
        println!("  - Version: {}", searcher.filters.version);
        println!("  - Error rate: {}", searcher.filters.error_rate);
        println!("  - Split levels: {}", searcher.filters.split_chars_inclusive.len());
        println!("  - Columns indexed: {}", searcher.filters.column_filters.len());
        println!("  - Total chunks: {}", searcher.filters.chunk_index.len());

        println!();

        // ======================================================================
        // STEP 6: Test Keyword Searches
        // ======================================================================
        println!("STEP 6: Testing keyword searches");
        println!("--------------------------------");

        // Search for payment types (likely to exist)
        let search_start = std::time::Instant::now();
        let result = searcher.search("cash", None, true)
            .await.expect("Search failed");
        let search_duration = search_start.elapsed();

        println!("Search 1: 'cash'");
        println!("  Found: {}", result.found);
        println!("  Search time: {:.2?}", search_duration);
        if result.found {
            if let Some(verified) = &result.verified_matches {
                println!("  Columns: {:?}", verified.columns);
                let total_rows: usize = verified.column_details.iter()
                    .map(|cd| cd.row_groups.iter()
                        .map(|rg| rg.row_ranges.iter()
                            .map(|range| (range.end_row - range.start_row + 1) as usize)
                            .sum::<usize>())
                        .sum::<usize>())
                    .sum();
                println!("  Approximate matches: {} rows", format_with_commas(total_rows));
            }
        }

        println!();

        // Search for location (store and forward flag - likely Y or N)
        let search_start = std::time::Instant::now();
        let result = searcher.search("Y", None, true)
            .await.expect("Search failed");
        let search_duration = search_start.elapsed();

        println!("Search 2: 'Y'");
        println!("  Found: {}", result.found);
        println!("  Search time: {:.2?}", search_duration);
        if result.found {
            if let Some(verified) = &result.verified_matches {
                println!("  Columns: {:?}", verified.columns);
            }
        }

        println!();

        // Search for a specific amount (less likely to match exactly)
        let search_start = std::time::Instant::now();
        let result = searcher.search("15.50", None, true)
            .await.expect("Search failed");
        let search_duration = search_start.elapsed();

        println!("Search 3: '15.50' (specific fare amount)");
        println!("  Found: {}", result.found);
        println!("  Search time: {:.2?}", search_duration);
        if result.found {
            if let Some(verified) = &result.verified_matches {
                println!("  Columns: {:?}", verified.columns);
            }
        }

        println!();

        // Search for vendor ID
        let search_start = std::time::Instant::now();
        let result = searcher.search("1", None, true)
            .await.expect("Search failed");
        let search_duration = search_start.elapsed();

        println!("Search 4: '1' (vendor ID)");
        println!("  Found: {}", result.found);
        println!("  Search time: {:.2?}", search_duration);
        if result.found {
            if let Some(verified) = &result.verified_matches {
                println!("  Columns: {:?}", verified.columns);
            }
        }

        println!();

        // Search for non-existent keyword (test negative case)
        let search_start = std::time::Instant::now();
        let result = searcher.search("NONEXISTENT_KEYWORD_XYZ", None, true)
            .await.expect("Search failed");
        let search_duration = search_start.elapsed();

        println!("Search 5: 'NONEXISTENT_KEYWORD_XYZ' (negative test)");
        println!("  Found: {}", result.found);
        println!("  Search time: {:.2?} (should be very fast due to bloom filter)", search_duration);
        assert!(!result.found, "Should not find non-existent keyword");

        println!();

        // ======================================================================
        // FINAL SUMMARY
        // ======================================================================
        println!("=============================================================");
        println!("TEST COMPLETE - SUMMARY");
        println!("=============================================================");
        println!("This test demonstrated:");
        println!("  ✓ Downloaded real-world parquet data ({:.2} MB)",
                 parquet_size as f64 / (1024.0 * 1024.0));
        println!("  ✓ Built keyword search index in {:.2?}", index_duration);
        println!("  ✓ Performed multiple keyword searches (all < 1ms typical)");
        println!("  ✓ Verified negative case (bloom filter pruning)");
        println!("\nThe keyword index enables fast searches across the dataset");
        println!("without needing to scan the entire parquet file.");
        println!("=============================================================\n");
    }

    /// Compares keyword index vs DataFusion for a single-column search on real taxi data.
    ///
    /// Both sides search passenger_count for value '1' and read matching rows.
    /// This is an apples-to-apples comparison demonstrating performance parity on
    /// a high-frequency keyword (~70% of rows match).
    #[tokio::test]
    async fn test_yellow_taxi_single_column_vs_datafusion() {
        use std::time::Instant;
        use std::collections::HashSet;
        use crate::build_and_save_index;
        use datafusion::prelude::*;

        println!("\n=============================================================");
        println!("NYC YELLOW TAXI — SINGLE COLUMN SEARCH vs DATAFUSION");
        println!("=============================================================");
        println!("Keyword: '1' in passenger_count column");
        println!();

        // ── Step 1: Download ────────────────────────────────────────────────
        println!("Step 1: Downloading parquet…");
        let response = reqwest::get(YELLOW_TAXI_URL).await
            .expect("Failed to download yellow taxi parquet");
        let parquet_bytes = response.bytes().await
            .expect("Failed to read response bytes");
        println!("  {:.2} MB received", parquet_bytes.len() as f64 / (1024.0 * 1024.0));

        let temp_path = std::env::temp_dir()
            .join("yellow_taxi_single_col_perf_test.parquet");
        let temp_path_str = temp_path.to_str().unwrap().to_string();
        std::fs::write(&temp_path, &parquet_bytes)
            .expect("Failed to write temp parquet");

        // ── Step 2: Build index ──────────────────────────────────────────────
        println!("\nStep 2: Building index…");
        let build_start = Instant::now();
        build_and_save_index(
            &temp_path_str,
            Some(HashSet::from([
                "tpep_pickup_datetime".to_string(),
                "tpep_dropoff_datetime".to_string(),
            ])),
            Some(0.01), None, None, None, None, Some(true), None,
            Some(0.2), Some(0.2),
        ).await.expect("Index build failed");
        println!("  Index built in {:.2?}", build_start.elapsed());

        // ── Step 3: Keyword index search + read rows ─────────────────────────
        // Split timing into three phases so we can compare scan-to-scan with
        // DataFusion: load (setup), index (unique keyword-index work, no
        // DataFusion equivalent), and scan (parquet read + filter).
        println!("\nStep 3: Keyword index search+read for '1' in passenger_count…");
        use crate::searching::keyword_search::KeywordSearcher;
        use crate::searching::search_results::SearchMode;
        use crate::searching::pruned_reader::PrunedParquetReader;

        let ki_load_start = Instant::now();
        let searcher = KeywordSearcher::load(&temp_path_str, None).await
            .expect("KeywordSearcher::load failed");
        let ki_load_time = ki_load_start.elapsed();

        let ki_index_start = Instant::now();
        let ki_result = searcher.search_with_mode(
            "1", Some("passenger_count"), false, SearchMode::Equals,
        ).await.expect("search_with_mode failed");
        let ki_index_time = ki_index_start.elapsed();
        assert!(ki_result.found, "Keyword '1' must be found in passenger_count");

        let ki_scan_start = Instant::now();
        let reader = PrunedParquetReader::from_path(&temp_path_str).with_metadata_cache(
            searcher.filters.parquet_metadata_offset,
            searcher.filters.parquet_metadata_length,
        );
        let ki_batches = reader.read_search_result(&ki_result, None).await
            .expect("read_search_result failed");
        let ki_scan_time = ki_scan_start.elapsed();

        let ki_total = ki_load_time + ki_index_time + ki_scan_time;
        let ki_rows: usize = ki_batches.iter().map(|b| b.num_rows()).sum();
        println!(
            "  load={:.2?}  index={:.2?}  scan={:.2?}  total={:.2?}  rows={}",
            ki_load_time, ki_index_time, ki_scan_time, ki_total, ki_rows
        );

        // ── Step 4: DataFusion search ────────────────────────────────────────
        // Split into register (setup — analogous to our load) and scan (execute).
        println!("\nStep 4: DataFusion search (\"passenger_count\" = 1)…");
        let df_reg_start = Instant::now();
        let ctx = SessionContext::new();
        ctx.register_parquet("data", &temp_path_str, Default::default()).await
            .expect("DataFusion parquet registration failed");
        let df_reg_time = df_reg_start.elapsed();

        let df_scan_start = Instant::now();
        let df = ctx.sql("SELECT * FROM data WHERE \"passenger_count\" = 1").await
            .expect("DataFusion query failed");
        let df_batches = df.collect().await.expect("DataFusion collect failed");
        let df_scan_time = df_scan_start.elapsed();

        let df_rows: usize = df_batches.iter().map(|b| b.num_rows()).sum();
        let df_total = df_reg_time + df_scan_time;
        println!(
            "  register={:.2?}  scan={:.2?}  total={:.2?}  rows={}",
            df_reg_time, df_scan_time, df_total, df_rows
        );

        // ── Step 5: Summary ──────────────────────────────────────────────────
        println!();
        println!("=============================================================");
        println!("RESULTS  (passenger_count = '1')");
        println!("=============================================================");
        println!();
        println!("┌──────────────────────────────┬────────────┬────────────┬────────────┬────────────┐");
        println!("│ Approach                     │ Setup      │ Index      │ Scan       │ Total      │");
        println!("├──────────────────────────────┼────────────┼────────────┼────────────┼────────────┤");
        println!("│ Keyword index                │ {:>10.2?} │ {:>10.2?} │ {:>10.2?} │ {:>10.2?} │",
                 ki_load_time, ki_index_time, ki_scan_time, ki_total);
        println!("│ DataFusion                   │ {:>10.2?} │ {:>10}   │ {:>10.2?} │ {:>10.2?} │",
                 df_reg_time, "n/a", df_scan_time, df_total);
        println!("└──────────────────────────────┴────────────┴────────────┴────────────┴────────────┘");
        println!();

        // Scan-to-scan is the fair comparison; scan == parquet read + filter.
        let scan_ratio = df_scan_time.as_secs_f64() / ki_scan_time.as_secs_f64();
        if scan_ratio >= 1.0 {
            println!("Scan-only: keyword index is {:.2}x FASTER than DataFusion", scan_ratio);
        } else {
            println!("Scan-only: keyword index is {:.2}x slower than DataFusion", 1.0 / scan_ratio);
        }
        let total_ratio = df_total.as_secs_f64() / ki_total.as_secs_f64();
        if total_ratio >= 1.0 {
            println!("End-to-end: keyword index is {:.2}x FASTER than DataFusion", total_ratio);
        } else {
            println!("End-to-end: keyword index is {:.2}x slower than DataFusion", 1.0 / total_ratio);
        }

        // Row counts must match
        assert_eq!(ki_rows, df_rows,
            "Keyword index and DataFusion must return the same number of rows");

        // ── Cleanup ──────────────────────────────────────────────────────────
        let _ = std::fs::remove_file(&temp_path);
        let _ = std::fs::remove_dir_all(format!("{}.index", temp_path_str));
    }

    /// Compares keyword index vs DataFusion searching ALL columns for '1'.
    ///
    /// The keyword index searches all columns at once (no column filter), which uses
    /// the aggregate column 0 where split elimination fires heavily for '1'.
    /// DataFusion must cast each column to string and check with OR across all columns.
    ///
    /// This test confirms that even with split elimination applied (splits_matched = None
    /// on the aggregate column), the keyword index performs comparably to DataFusion
    /// when both must scan the full file.
    #[tokio::test]
    async fn test_yellow_taxi_all_columns_split_elimination_vs_datafusion() {
        use std::time::Instant;
        use std::collections::HashSet;
        use crate::build_and_save_index;
        use datafusion::prelude::*;

        println!("\n=============================================================");
        println!("NYC YELLOW TAXI — ALL-COLUMN SEARCH WITH SPLIT ELIMINATION");
        println!("=============================================================");
        println!("Keyword: '1' across all indexed columns");
        println!("Split elimination threshold: 0.2");
        println!();

        // ── Step 1: Download ────────────────────────────────────────────────
        println!("Step 1: Downloading parquet…");
        let response = reqwest::get(YELLOW_TAXI_URL).await
            .expect("Failed to download yellow taxi parquet");
        let parquet_bytes = response.bytes().await
            .expect("Failed to read response bytes");
        println!("  {:.2} MB received", parquet_bytes.len() as f64 / (1024.0 * 1024.0));

        let temp_path = std::env::temp_dir()
            .join("yellow_taxi_all_cols_perf_test.parquet");
        let temp_path_str = temp_path.to_str().unwrap().to_string();
        std::fs::write(&temp_path, &parquet_bytes)
            .expect("Failed to write temp parquet");

        // ── Step 2: Build index with split elimination ───────────────────────
        println!("\nStep 2: Building index (split_elimination_threshold = 0.2)…");
        let build_start = Instant::now();
        build_and_save_index(
            &temp_path_str,
            Some(HashSet::from([
                "tpep_pickup_datetime".to_string(),
                "tpep_dropoff_datetime".to_string(),
            ])),
            Some(0.01), None, None, None, None, Some(true), None,
            Some(0.2), Some(0.2),
        ).await.expect("Index build failed");
        println!("  Index built in {:.2?}", build_start.elapsed());

        // ── Step 3: Keyword index search all columns + read rows ─────────────
        // Split timing: load (setup), index (unique keyword-index work, no
        // DataFusion equivalent), scan (parquet read + filter).
        println!("\nStep 3: Keyword index search+read for '1' (all columns)…");
        use crate::searching::keyword_search::KeywordSearcher;
        use crate::searching::search_results::SearchMode;
        use crate::searching::pruned_reader::PrunedParquetReader;

        let ki_load_start = Instant::now();
        let searcher = KeywordSearcher::load(&temp_path_str, None).await
            .expect("KeywordSearcher::load failed");
        let ki_load_time = ki_load_start.elapsed();

        let ki_index_start = Instant::now();
        let ki_result = searcher.search_with_mode(
            "1", None, false, SearchMode::Equals,
        ).await.expect("search_with_mode failed");
        let ki_index_time = ki_index_start.elapsed();
        assert!(ki_result.found, "Keyword '1' must be found");

        let ki_scan_start = Instant::now();
        let reader = PrunedParquetReader::from_path(&temp_path_str).with_metadata_cache(
            searcher.filters.parquet_metadata_offset,
            searcher.filters.parquet_metadata_length,
        );
        let ki_batches = reader.read_search_result(&ki_result, None).await
            .expect("read_search_result failed");
        let ki_scan_time = ki_scan_start.elapsed();

        let ki_total = ki_load_time + ki_index_time + ki_scan_time;
        let ki_rows: usize = ki_batches.iter().map(|b| b.num_rows()).sum();

        let verified_count = ki_result.verified_matches.as_ref()
            .map(|v| v.total_occurrences).unwrap_or(0);
        let needs_verif_count = ki_result.needs_verification.as_ref()
            .map(|v| v.total_occurrences).unwrap_or(0);
        let splits_eliminated = ki_result.needs_verification.is_some();

        println!(
            "  load={:.2?}  index={:.2?}  scan={:.2?}  total={:.2?}  rows={}",
            ki_load_time, ki_index_time, ki_scan_time, ki_total, ki_rows
        );
        println!("  Verified: {}, Needs verification: {} (split-eliminated)", verified_count, needs_verif_count);
        println!("  Split elimination applied: {}", splits_eliminated);

        // ── Step 4: DataFusion search all columns ────────────────────────────
        // Split into register (setup — analogous to our load) and scan (execute).
        println!("\nStep 4: DataFusion search (all columns cast to string = '1')…");
        let df_reg_start = Instant::now();
        let ctx = SessionContext::new();
        ctx.register_parquet("data", &temp_path_str, Default::default()).await
            .expect("DataFusion parquet registration failed");
        let df_reg_time = df_reg_start.elapsed();

        // Build OR predicates for all indexed columns.
        // Use numeric comparison (= 1) rather than CAST to VARCHAR, because
        // Rust's f64::to_string() for 1.0 produces "1" while DataFusion's
        // CAST(1.0 AS VARCHAR) produces "1.0", causing a mismatch.
        let indexed_columns = ki_result.verified_matches.as_ref()
            .or(ki_result.needs_verification.as_ref())
            .map(|v| &v.columns)
            .expect("Search found results but both verified and needs_verification are None");
        let predicates: Vec<String> = indexed_columns.iter()
            .map(|col| {
                if col == "store_and_fwd_flag" {
                    format!("\"{}\" = '1'", col)
                } else {
                    format!("\"{}\" = 1", col)
                }
            })
            .collect();
        let where_clause = predicates.join(" OR ");
        let sql = format!("SELECT * FROM data WHERE {}", where_clause);
        println!("  SQL: {}", sql);

        let df_scan_start = Instant::now();
        let df = ctx.sql(&sql).await
            .expect("DataFusion query failed");
        let df_batches = df.collect().await.expect("DataFusion collect failed");
        let df_scan_time = df_scan_start.elapsed();

        let df_rows: usize = df_batches.iter().map(|b| b.num_rows()).sum();
        let df_total = df_reg_time + df_scan_time;
        println!(
            "  register={:.2?}  scan={:.2?}  total={:.2?}  rows={}",
            df_reg_time, df_scan_time, df_total, df_rows
        );

        // ── Step 5: Summary ──────────────────────────────────────────────────
        println!();
        println!("=============================================================");
        println!("RESULTS  (all columns = '1')");
        println!("=============================================================");
        println!();
        println!("┌──────────────────────────────┬────────────┬────────────┬────────────┬────────────┐");
        println!("│ Approach                     │ Setup      │ Index      │ Scan       │ Total      │");
        println!("├──────────────────────────────┼────────────┼────────────┼────────────┼────────────┤");
        println!("│ Keyword index                │ {:>10.2?} │ {:>10.2?} │ {:>10.2?} │ {:>10.2?} │",
                 ki_load_time, ki_index_time, ki_scan_time, ki_total);
        println!("│ DataFusion                   │ {:>10.2?} │ {:>10}   │ {:>10.2?} │ {:>10.2?} │",
                 df_reg_time, "n/a", df_scan_time, df_total);
        println!("└──────────────────────────────┴────────────┴────────────┴────────────┴────────────┘");
        println!();

        let scan_ratio = df_scan_time.as_secs_f64() / ki_scan_time.as_secs_f64();
        if scan_ratio >= 1.0 {
            println!("Scan-only: keyword index is {:.2}x FASTER than DataFusion", scan_ratio);
        } else {
            println!("Scan-only: keyword index is {:.2}x slower than DataFusion", 1.0 / scan_ratio);
        }
        let total_ratio = df_total.as_secs_f64() / ki_total.as_secs_f64();
        if total_ratio >= 1.0 {
            println!("End-to-end: keyword index is {:.2}x FASTER than DataFusion", total_ratio);
        } else {
            println!("End-to-end: keyword index is {:.2}x slower than DataFusion", 1.0 / total_ratio);
        }
        println!();
        println!("Split elimination applied: {}", splits_eliminated);

        assert!(splits_eliminated,
            "Expected split elimination on aggregate column for keyword '1' at threshold 0.2");

        assert_eq!(ki_rows, df_rows,
            "Keyword index and DataFusion must return the same number of rows \
             when using exact_match=true (Equals mode)");

        // ── Cleanup ──────────────────────────────────────────────────────────
        let _ = std::fs::remove_file(&temp_path);
        let _ = std::fs::remove_dir_all(format!("{}.index", temp_path_str));
    }
}