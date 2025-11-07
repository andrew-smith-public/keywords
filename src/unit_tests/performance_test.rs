#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;
    use arrow::array::{ArrayRef, StringArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::basic::Compression;
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use std::fs::File;
    use crate::build_and_save_index;

    #[tokio::test]
    async fn test_performance_comparison() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("\n=== Performance Test: Keyword Index vs Pushdown Predicate ===\n");
        let index_file_prefix = Some("test_performance_comparison");

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
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(25_000)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // Remember values from first row of first row group (columns 0, 1, 2)
        let mut remembered_values = Vec::new();

        // Step 4: Write 10 row groups with 10,000 rows each
        println!("Writing 10 row groups with 10,000 rows each...");
        for rg in 0..10 {
            let mut columns: Vec<ArrayRef> = Vec::new();

            for col_idx in 0..10 {
                let values: Vec<String> = (0..10_000)
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
        println!("Parquet file created: {} rows total\n", 10 * 10_000);

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

        // Step 6: Test keyword search performance
        println!("Testing keyword search...");
        use crate::searching::keyword_search::KeywordSearcher;
        use crate::searching::pruned_reader::PrunedParquetReader;
        use crate::searching::search_results::KeywordSearchResult;

        let searcher = KeywordSearcher::load(&file_path, index_file_prefix).await?;
        let reader = PrunedParquetReader::from_path(&file_path);

        let keyword_start = Instant::now();

        // Search for the three remembered values
        let search_result0 = searcher.search(&remembered_values[0], None, true)?;
        let search_result1 = searcher.search(&remembered_values[1], None, true)?;
        let search_result2 = searcher.search(&remembered_values[2], None, true)?;

        // Convert SearchResult to KeywordSearchResult for combine_and
        let result0 = KeywordSearchResult {
            keyword: search_result0.query.clone(),
            found: search_result0.found,
            data: search_result0.verified_matches.clone(),
        };
        let result1 = KeywordSearchResult {
            keyword: search_result1.query.clone(),
            found: search_result1.found,
            data: search_result1.verified_matches.clone(),
        };
        let result2 = KeywordSearchResult {
            keyword: search_result2.query.clone(),
            found: search_result2.found,
            data: search_result2.verified_matches.clone(),
        };

        // Combine with AND logic
        let combined = KeywordSearcher::combine_and(&[result0, result1, result2])
            .ok_or("No combined results")?;

        // Read the matching rows
        let batches = reader.read_combined_rows(&combined, None).await?;
        let keyword_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let keyword_time = keyword_start.elapsed();

        println!("Keyword search completed in: {:?}", keyword_time);
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
            let col0 = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let col1 = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let col2 = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();

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
        use crate::utils::file_interaction_local_and_cloud::get_object_store;

        let naive_start = Instant::now();

        let (store, path) = get_object_store(&file_path).await?;
        let file_bytes = store.get(&path).await?.bytes().await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file_bytes)?;
        let mut reader = builder.build()?;

        let mut matching_batches = Vec::new();

        while let Some(batch) = reader.next() {
            let batch = batch?;

            // Apply filter: col_0 == val0 AND col_1 == val1 AND col_2 == val2
            let col0 = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let col1 = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let col2 = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();

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
        println!();

        // Step 9: Compare all three approaches
        println!("=== Performance Comparison ===");
        println!("┌─────────────────────────────────┬──────────────┬──────────┐");
        println!("│ Approach                        │ Time         │ Speedup  │");
        println!("├─────────────────────────────────┼──────────────┼──────────┤");
        println!("│ Keyword Index (this project)    │ {:>11.2?} │ baseline │", keyword_time);

        let df_vs_keyword = datafusion_time.as_secs_f64() / keyword_time.as_secs_f64();
        let df_speedup_str = if keyword_time < datafusion_time {
            format!("{:.2}x faster", df_vs_keyword)
        } else {
            format!("{:.2}x slower", 1.0 / df_vs_keyword)
        };
        println!("│ DataFusion (pushdown + pruning) │ {:>11.2?} │ {:>8} │", datafusion_time, df_speedup_str);

        let naive_vs_keyword = naive_time.as_secs_f64() / keyword_time.as_secs_f64();
        let naive_speedup_str = if keyword_time < naive_time {
            format!("{:.2}x faster", naive_vs_keyword)
        } else {
            format!("{:.2}x slower", 1.0 / naive_vs_keyword)
        };
        println!("│ Naive (read all, filter)        │ {:>11.2?} │ {:>8} │", naive_time, naive_speedup_str);
        println!("└─────────────────────────────────┴──────────────┴──────────┘");
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