#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;
    use arrow::array::{ArrayRef, StringArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::basic::{Compression, GzipLevel};
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use std::fs::File;
    use crate::build_and_save_index;

    #[tokio::test]
    async fn test_performance_comparison() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("\n=== Performance Test: Keyword Index vs Pushdown Predicate ===\n");
        let index_file_prefix = Some("test_performance_comparison_");

        // Step 1: Generate random string pool (5000 strings, 10-20 chars)
        println!("Generating random string pool...");
        let mut rng = rand::rng();
        let string_pool: Vec<String> = (0..5000)
            .map(|_| {
                let len = rng.random_range(10..=20);
                // Generate alphabetic-only string
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
            .join("test_performance.parquet")
            .to_string_lossy()
            .to_string();

        println!("Creating parquet file: {}", file_path);
        let file = File::create(&file_path)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::GZIP(GzipLevel::try_new(9).unwrap()))
            .set_max_row_group_size(100_000)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // Remember values from first row of first row group (columns 0, 1, 2)
        let mut remembered_values = Vec::new();

        // Step 4: Write 5 row groups with 100,000 rows each
        println!("Writing 5 row groups with 100,000 rows each...");
        for rg in 0..5 {
            let mut columns: Vec<ArrayRef> = Vec::new();

            for col_idx in 0..10 {
                let values: Vec<String> = (0..100_000)
                    .map(|_| {
                        let idx = rng.random_range(0..string_pool.len());
                        string_pool[idx].clone()
                    })
                    .collect();

                // Remember first row values from columns 0, 1, 2 in first row group
                if rg == 0 && col_idx < 3 {
                    remembered_values.push(values[0].clone());
                }

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
        let file_size_bytes = file_metadata.len();
        let file_size_mb = file_size_bytes as f64 / (1024.0 * 1024.0);
        println!("Parquet file created: {} rows total", 10 * 10_000);
        println!("Parquet file size: {} bytes ({:.2} MB)\n", file_size_bytes, file_size_mb);

        println!("Remembered values from row group 0, row 0:");
        println!("  col_0: {}", remembered_values[0]);
        println!("  col_1: {}", remembered_values[1]);
        println!("  col_2: {}", remembered_values[2]);
        println!();

        // Step 5: Build keyword index
        println!("Building keyword index...");
        let index_start = Instant::now();

        build_and_save_index(&file_path, None, Some(0.01), index_file_prefix).await?;

        let index_time = index_start.elapsed();
        println!("Index built in: {:?}\n", index_time);

        // Step 5.5: Test raw disk I/O performance for data.bin
        println!("Testing raw disk I/O performance...");
        use crate::utils::file_interaction_local_and_cloud::get_object_store;

        let data_bin_path = format!("{}.index/{}", file_path,
                                    if let Some(prefix) = index_file_prefix {
                                        format!("{}data.bin", prefix)
                                    } else {
                                        "data.bin".to_string()
                                    }
        );

        // Get data.bin file size
        let (store, path) = get_object_store(&data_bin_path).await?;
        let metadata = store.head(&path).await?;
        let data_bin_size = metadata.size;
        let data_bin_size_mb = data_bin_size as f64 / (1024.0 * 1024.0);
        println!("data.bin size: {} bytes ({:.2} MB)", data_bin_size, data_bin_size_mb);

        // Test 1: Read entire data.bin file
        println!("\nTest 1: Reading entire data.bin file...");
        let read_all_start = Instant::now();
        let entire_file = store.get(&path).await?.bytes().await?;
        let read_all_time = read_all_start.elapsed();
        let throughput_mbps = data_bin_size_mb / read_all_time.as_secs_f64();
        println!("  Time to read entire file: {:?}", read_all_time);
        println!("  Throughput: {:.2} MB/s", throughput_mbps);
        drop(entire_file); // Release memory

        // Test 2: Read first chunk (simulating what search does)
        println!("\nTest 2: Reading first chunk (range read)...");
        use crate::searching::keyword_search::KeywordSearcher;
        let temp_searcher = KeywordSearcher::load(&file_path, index_file_prefix).await?;

        if !temp_searcher.filters.chunk_index.is_empty() {
            let first_chunk = &temp_searcher.filters.chunk_index[0];
            let chunk_size = first_chunk.total_length;
            let chunk_size_kb = chunk_size as f64 / 1024.0;
            println!("  First chunk size: {} bytes ({:.2} KB)", chunk_size, chunk_size_kb);

            let chunk_read_start = Instant::now();
            let range = first_chunk.offset..(first_chunk.offset + chunk_size as u64);
            let _chunk_bytes = store.get_range(&path, range).await?;
            let chunk_read_time = chunk_read_start.elapsed();

            println!("  Time to read chunk: {:?}", chunk_read_time);
            println!("  Chunk read throughput: {:.2} MB/s",
                     (chunk_size_kb / 1024.0) / chunk_read_time.as_secs_f64());

            // Test 3: Read same chunk again (to test OS cache)
            println!("\nTest 3: Reading same chunk again (OS cache test)...");
            let cache_read_start = Instant::now();
            let range = first_chunk.offset..(first_chunk.offset + chunk_size as u64);
            let _cached_bytes = store.get_range(&path, range).await?;
            let cache_read_time = cache_read_start.elapsed();
            println!("  Time to read cached chunk: {:?}", cache_read_time);

            let speedup = chunk_read_time.as_secs_f64() / cache_read_time.as_secs_f64();
            println!("  Cache speedup: {:.2}x faster", speedup);
        }

        println!();

        // Step 6: Test keyword search performance
        println!("Testing keyword search...");
        use crate::searching::pruned_reader::PrunedParquetReader;

        let searcher = KeywordSearcher::load(&file_path, index_file_prefix).await?;
        let reader = PrunedParquetReader::from_path(&file_path);

        // Time just the index search (no parquet reading)
        let index_search_start = Instant::now();

        // Search for the three remembered values with detailed timing for first search
        println!("\n--- Detailed timing breakdown for first search ---");

        let keyword = &remembered_values[0];
        let _column_filter = Some("col_0");

        // Phase 1: Bloom filter check
        let bloom_start = Instant::now();
        let _bloom_check = if let Some(filter) = searcher.filters.column_filters.get("col_0") {
            filter.might_contain(keyword)
        } else {
            false
        };
        let bloom_time = bloom_start.elapsed();

        // Phase 2: Find chunk
        let chunk_start = Instant::now();
        let chunk_result = searcher.filters.chunk_index.binary_search_by(|chunk| {
            chunk.start_keyword.as_str().cmp(keyword)
        });
        let chunk_idx = chunk_result.unwrap_or_else(|idx| if idx == 0 { 0 } else { idx - 1 });
        let chunk_time = chunk_start.elapsed();

        // Phase 3: Read chunk from disk (I/O only)
        use crate::index_structure::index_files::{index_filename, IndexFile};

        let chunk_info = &searcher.filters.chunk_index[chunk_idx];
        let data_path = format!("{}/{}",
                                format!("{}.index", file_path),
                                index_filename(IndexFile::Data, index_file_prefix.as_deref()));

        let io_start = Instant::now();
        let (store, obj_path) = get_object_store(&data_path).await?;
        let start = chunk_info.offset;
        let length = chunk_info.total_length as u64;
        let range = start..(start + length);
        let result = store.get_range(&obj_path, range).await?;

        // Copy once into aligned buffer (optimization #1)
        let mut buffer = AlignedVec::<16>::new();
        buffer.extend_from_slice(&result);
        let io_time = io_start.elapsed();

        // Phase 4: Deserialize keywords
        use rkyv::util::AlignedVec;
        use rkyv::Archived;
        use rkyv::rancor::Error as RkyvError;

        let deser_keywords_start = Instant::now();
        let keyword_length = chunk_info.keyword_list_length as usize;

        // Access keywords directly from aligned buffer (no additional copy)
        let archived_keywords: &Archived<Vec<String>> = rkyv::access(&buffer[..keyword_length])
            .map_err(|e: RkyvError| format!("Failed to deserialize: {}", e))?;
        let keywords: Vec<String> = archived_keywords.iter().map(|s| s.to_string()).collect();
        let deser_keywords_time = deser_keywords_start.elapsed();

        // Phase 5: Zero-copy access to data (NO deserialization yet)
        let deser_data_start = Instant::now();

        // Access data directly from aligned buffer (no additional copy)
        use crate::index_data::KeywordDataFlat;
        let archived_data: &Archived<Vec<KeywordDataFlat>> = rkyv::access(&buffer[keyword_length..])
            .map_err(|e: RkyvError| format!("Failed to deserialize: {}", e))?;
        // Zero-copy: just getting pointer, no conversion yet
        let deser_data_time = deser_data_start.elapsed();

        // Phase 6: Binary search
        let binary_search_start = Instant::now();
        let position = keywords.binary_search_by(|k| k.as_str().cmp(keyword)).unwrap();
        let binary_search_time = binary_search_start.elapsed();

        // Phase 7: Build result structures - ONLY convert the one item we need
        let build_start = Instant::now();
        let archived_data_item = &archived_data[position];  // Still zero-copy!

        use crate::searching::search_results::{ColumnLocation, RowGroupLocation, RowRange};

        let mut column_details = Vec::new();
        let mut total_occurrences = 0u64;
        let all_column_ids: Vec<u32> = archived_data_item.columns.iter()
            .map(|col| col.column_id.to_native())  // Convert from archived
            .filter(|&id| id != 0)
            .collect();

        for col in archived_data_item.columns.iter() {
            let column_id: u32 = col.column_id.to_native();  // Convert from archived
            if Some(0) == Some(0) && column_id != 0 {
                continue;
            }
            let column_name = if column_id == 0 {
                "_all_columns_aggregate_".to_string()
            } else {
                searcher.filters.column_pool.get(column_id).unwrap().to_string()
            };
            let mut row_groups = Vec::new();
            for rg in col.row_groups.iter() {
                let row_group_id: u16 = rg.row_group_id.to_native();  // Convert from archived
                let mut row_ranges = Vec::new();
                for flat_row in rg.rows.iter() {
                    row_ranges.push(RowRange {
                        start_row: flat_row.row.to_native(),  // Convert from archived
                        end_row: flat_row.row.to_native() + flat_row.additional_rows.to_native(),
                        splits_matched: flat_row.splits_matched.to_native(),
                        parent_chunk: flat_row.parent_chunk.as_ref().map(|c| c.to_native()),
                        parent_position: flat_row.parent_position.as_ref().map(|p| p.to_native()),
                    });
                    total_occurrences = total_occurrences.saturating_add(flat_row.additional_rows.to_native() as u64 + 1);
                }
                row_groups.push(RowGroupLocation { row_group_id, row_ranges });
            }
            column_details.push(ColumnLocation { column_name, row_groups });
        }

        if !column_details.is_empty() {
            let aggregate_row_groups = column_details[0].row_groups.clone();
            column_details.clear();
            for column_id in all_column_ids {
                if let Some(column_name) = searcher.filters.column_pool.get(column_id) {
                    column_details.push(ColumnLocation {
                        column_name: column_name.to_string(),
                        row_groups: aggregate_row_groups.clone(),
                    });
                }
            }
        }
        let build_time = build_start.elapsed();

        let detailed_total = bloom_time + chunk_time + io_time + deser_keywords_time + deser_data_time + binary_search_time + build_time;

        println!("  Bloom filter check:      {:>8.3?} ({:>5.1}%)",
                 bloom_time,
                 (bloom_time.as_micros() as f64 / detailed_total.as_micros() as f64) * 100.0);
        println!("  Find chunk (binary):     {:>8.3?} ({:>5.1}%)",
                 chunk_time,
                 (chunk_time.as_micros() as f64 / detailed_total.as_micros() as f64) * 100.0);
        println!("  Disk I/O:                {:>8.3?} ({:>5.1}%)",
                 io_time,
                 (io_time.as_micros() as f64 / detailed_total.as_micros() as f64) * 100.0);
        println!("  Deser keywords:          {:>8.3?} ({:>5.1}%)",
                 deser_keywords_time,
                 (deser_keywords_time.as_micros() as f64 / detailed_total.as_micros() as f64) * 100.0);
        println!("  Deser data:              {:>8.3?} ({:>5.1}%)",
                 deser_data_time,
                 (deser_data_time.as_micros() as f64 / detailed_total.as_micros() as f64) * 100.0);
        println!("  Binary search keyword:   {:>8.3?} ({:>5.1}%)",
                 binary_search_time,
                 (binary_search_time.as_micros() as f64 / detailed_total.as_micros() as f64) * 100.0);
        println!("  Build result:            {:>8.3?} ({:>5.1}%)",
                 build_time,
                 (build_time.as_micros() as f64 / detailed_total.as_micros() as f64) * 100.0);
        println!("  Total:                   {:>8.3?}", detailed_total);
        println!("----------------------------------------------\n");

        // Now do the actual search for comparison
        let search_result0 = searcher.search(&remembered_values[0], Some("col_0"), true).await?;
        let index_search_time_1 = index_search_start.elapsed();
        let index_search_start_2 = Instant::now();
        let search_result1 = searcher.search(&remembered_values[1], Some("col_1"), true).await?;
        let index_search_time_2 = index_search_start_2.elapsed();
        let index_search_start_3 = Instant::now();
        let search_result2 = searcher.search(&remembered_values[2], Some("col_2"), true).await?;
        let index_search_time_3 = index_search_start_3.elapsed();

        // Combine with AND logic - no conversion needed!
        let index_search_start_a = Instant::now();
        let combined = KeywordSearcher::combine_and(&[search_result0, search_result1, search_result2])
            .ok_or("No combined results")?;

        let index_search_time_a = index_search_start_a.elapsed();
        let index_search_time = index_search_start.elapsed();

        // Time just the parquet file reading
        let parquet_read_start = Instant::now();

        // Read the matching rows with metadata caching for optimal performance
        let metadata_cache = Some((
            searcher.filters.parquet_metadata_offset,
            searcher.filters.parquet_metadata_length
        ));
        let batches = reader.read_combined_rows_with_metadata(&combined, None, metadata_cache).await?;
        let keyword_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let parquet_read_time = parquet_read_start.elapsed();

        // Total time
        let keyword_total_time = index_search_time + parquet_read_time;

        println!("Keyword index search time:  {:?}", index_search_time);
        println!("Keyword index search time1: {:?}", index_search_time_1);
        println!("Keyword index search time2: {:?}", index_search_time_2);
        println!("Keyword index search time3: {:?}", index_search_time_3);
        println!("Keyword index search timeA: {:?}", index_search_time_a);
        println!("Parquet file read time:     {:?}", parquet_read_time);
        println!("Total time:                 {:?}", keyword_total_time);
        println!("Rows found: {}", keyword_rows);

        // Print detailed match information
        println!("\nMatched rows from keyword index:");
        for rg in &combined.row_groups {
            println!("  Row Group {}:", rg.row_group_id);
            for range in &rg.row_ranges {
                for row in range.start_row..=range.end_row {
                    println!("    Row {}", row);
                }
            }
        }

        // Print actual data from matched rows
        println!("\nMatched row data:");
        for (batch_idx, batch) in batches.iter().enumerate() {
            let schema = batch.schema();
            let col0_idx = schema.index_of("col_0")?;
            let col1_idx = schema.index_of("col_1")?;
            let col2_idx = schema.index_of("col_2")?;

            let col0 = batch.column(col0_idx).as_any().downcast_ref::<StringArray>().unwrap();
            let col1 = batch.column(col1_idx).as_any().downcast_ref::<StringArray>().unwrap();
            let col2 = batch.column(col2_idx).as_any().downcast_ref::<StringArray>().unwrap();

            for row_idx in 0..batch.num_rows() {
                println!("  Batch {}, Row {}: col_0='{}', col_1='{}', col_2='{}'",
                         batch_idx, row_idx, col0.value(row_idx), col1.value(row_idx), col2.value(row_idx));
            }
        }
        println!();

        // Step 7: Test with Apache DataFusion (proper pushdown predicate with row group pruning)
        println!("Testing Apache DataFusion with automatic pushdown predicates...");
        use datafusion::prelude::*;
        use datafusion::execution::context::SessionContext;

        let datafusion_start = Instant::now();

        // Create DataFusion context
        let ctx = SessionContext::new();

        // Register the parquet file as a table
        ctx.register_parquet("test_table", &file_path, ParquetReadOptions::default()).await?;

        // Build SQL query with predicates (DataFusion will automatically do row group pruning)
        let sql = format!(
            "SELECT * FROM test_table WHERE col_0 = '{}' AND col_1 = '{}' AND col_2 = '{}'",
            remembered_values[0], remembered_values[1], remembered_values[2]
        );

        // Execute query - DataFusion handles:
        // 1. Row group statistics evaluation
        // 2. Row group pruning (skipping groups that can't contain the values)
        // 3. Column pruning
        // 4. Predicate pushdown to Parquet reader
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        let datafusion_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let datafusion_time = datafusion_start.elapsed();

        println!("DataFusion completed in: {:?}", datafusion_time);
        println!("Rows found: {}", datafusion_rows);

        // Print actual data from matched rows
        println!("\nMatched row data from DataFusion:");
        for (batch_idx, batch) in batches.iter().enumerate() {
            if batch.num_rows() == 0 {
                continue;
            }

            let schema = batch.schema();
            let col0_idx = schema.index_of("col_0")?;
            let col1_idx = schema.index_of("col_1")?;
            let col2_idx = schema.index_of("col_2")?;

            // Debug: print column types
            let col0_type = batch.column(col0_idx).data_type();
            let col1_type = batch.column(col1_idx).data_type();
            let col2_type = batch.column(col2_idx).data_type();
            println!("  Batch {} column types: col_0={:?}, col_1={:?}, col_2={:?}",
                     batch_idx, col0_type, col1_type, col2_type);

            // Try to downcast - if it fails, skip printing
            let col0 = match batch.column(col0_idx).as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => {
                    println!("  Batch {}: Unable to downcast to StringArray", batch_idx);
                    continue;
                }
            };
            let col1 = match batch.column(col1_idx).as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => continue,
            };
            let col2 = match batch.column(col2_idx).as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => continue,
            };

            for row_idx in 0..batch.num_rows() {
                println!("  Batch {}, Row {}: col_0='{}', col_1='{}', col_2='{}'",
                         batch_idx, row_idx, col0.value(row_idx), col1.value(row_idx), col2.value(row_idx));
            }
        }

        // Get execution metrics to show row group pruning effectiveness
        // Note: DataFusion's metrics API shows how many row groups were actually read
        println!("Note: DataFusion automatically pruned row groups using Parquet statistics");
        println!("      (For random high-cardinality data, minimal pruning is expected)");
        println!();

        // Step 8: Also test naive approach (read all data, filter in memory) for comparison
        println!("Testing naive approach (read all data, filter in memory)...");
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use arrow::compute::filter_record_batch;
        use arrow::array::BooleanArray;

        let naive_start = Instant::now();

        let (store, path) = get_object_store(&file_path).await?;
        let file_bytes = store.get(&path).await?.bytes().await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file_bytes)?;
        let mut reader = builder.build()?;

        let mut matching_batches = Vec::new();

        while let Some(batch) = reader.next() {
            let batch = batch?;

            // Apply filter: col_0 == val0 AND col_1 == val1 AND col_2 == val2
            let schema = batch.schema();
            let col0_idx = schema.index_of("col_0")?;
            let col1_idx = schema.index_of("col_1")?;
            let col2_idx = schema.index_of("col_2")?;

            let col0 = batch.column(col0_idx).as_any().downcast_ref::<StringArray>().unwrap();
            let col1 = batch.column(col1_idx).as_any().downcast_ref::<StringArray>().unwrap();
            let col2 = batch.column(col2_idx).as_any().downcast_ref::<StringArray>().unwrap();

            let mask: Vec<bool> = (0..batch.num_rows())
                .map(|i| {
                    col0.value(i) == remembered_values[0] &&
                        col1.value(i) == remembered_values[1] &&
                        col2.value(i) == remembered_values[2]
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

        println!("Naive approach completed in: {:?}", naive_time);
        println!("Rows found: {}", naive_rows);

        // Print actual data from matched rows
        println!("\nMatched row data from naive approach:");
        for (batch_idx, batch) in matching_batches.iter().enumerate() {
            let schema = batch.schema();
            let col0_idx = schema.index_of("col_0")?;
            let col1_idx = schema.index_of("col_1")?;
            let col2_idx = schema.index_of("col_2")?;

            let col0 = batch.column(col0_idx).as_any().downcast_ref::<StringArray>().unwrap();
            let col1 = batch.column(col1_idx).as_any().downcast_ref::<StringArray>().unwrap();
            let col2 = batch.column(col2_idx).as_any().downcast_ref::<StringArray>().unwrap();

            for row_idx in 0..batch.num_rows() {
                println!("  Batch {}, Row {}: col_0='{}', col_1='{}', col_2='{}'",
                         batch_idx, row_idx, col0.value(row_idx), col1.value(row_idx), col2.value(row_idx));
            }
        }
        println!();

        // Step 9: Compare all three approaches
        println!("=== Performance Comparison ===");
        println!("┌─────────────────────────────────┬──────────────┬──────────┐");
        println!("│ Approach                        │ Time         │ Speedup  │");
        println!("├─────────────────────────────────┼──────────────┼──────────┤");
        println!("│ Keyword Index (this project)    │ {:>11.2?} │ baseline │", keyword_total_time);

        let df_vs_keyword = datafusion_time.as_secs_f64() / keyword_total_time.as_secs_f64();
        let df_speedup_str = if keyword_total_time < datafusion_time {
            format!("{:.2}x faster", df_vs_keyword)
        } else {
            format!("{:.2}x slower", 1.0 / df_vs_keyword)
        };
        println!("│ DataFusion (pushdown + pruning) │ {:>11.2?} │ {:>8} │", datafusion_time, df_speedup_str);

        let naive_vs_keyword = naive_time.as_secs_f64() / keyword_total_time.as_secs_f64();
        let naive_speedup_str = if keyword_total_time < naive_time {
            format!("{:.2}x faster", naive_vs_keyword)
        } else {
            format!("{:.2}x slower", 1.0 / naive_vs_keyword)
        };
        println!("│ Naive (read all, filter)        │ {:>11.2?} │ {:>8} │", naive_time, naive_speedup_str);
        println!("└─────────────────────────────────┴──────────────┴──────────┘");
        println!();

        println!("Keyword Index Timing Breakdown:");
        println!("  Index search:   {:>11.2?} ({:.1}%)",
                 index_search_time,
                 (index_search_time.as_secs_f64() / keyword_total_time.as_secs_f64()) * 100.0);
        println!("  Parquet read:   {:>11.2?} ({:.1}%)",
                 parquet_read_time,
                 (parquet_read_time.as_secs_f64() / keyword_total_time.as_secs_f64()) * 100.0);
        println!("  ────────────────────────────");
        println!("  Total:          {:>11.2?}", keyword_total_time);
        println!();

        println!("Analysis:");
        println!("- Random high-cardinality data limits row group pruning effectiveness");
        println!("- All approaches must evaluate similar numbers of row groups");
        println!("- Keyword index advantage: Pre-computed bloom filters + binary search");
        println!("- DataFusion advantage: Optimized query execution engine");
        println!();

        println!("Rows matched:");
        println!("  Keyword index: {}", keyword_rows);
        println!("  DataFusion:    {}", datafusion_rows);
        println!("  Naive:         {}", naive_rows);

        assert_eq!(keyword_rows, datafusion_rows, "Keyword index and DataFusion row counts should match!");
        assert_eq!(keyword_rows, naive_rows, "Keyword index and naive row counts should match!");

        // Clean up: delete the test file and index
        std::fs::remove_file(&file_path)?;
        std::fs::remove_dir_all(format!("{}.index", file_path))?;

        Ok(())
    }
}