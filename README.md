# Keywords - High-Performance Keyword Search for Parquet Files

**Fast keyword search for Parquet files achieving 5-170x speedups over Apache DataFusion, with 7.5x improvement for selective queries on moderate-cardinality data.**

## Executive Summary

High-performance Rust library and CLI tool for building keyword indexes on Parquet files and performing fast searches. Enables searching large files without loading data into memory, with performance exceeding Apache DataFusion where traditional row group statistics provide limited pruning.

## Key Features & Technical Achievements

**Core Implementation**
- **Hierarchical keyword extraction** with 4 levels of delimiter splitting (whitespace → paths → dots → word boundaries)
- **Zero-copy deserialization** using rkyv for memory-efficient operations
- **Run-length encoding** for consecutive rows to reduce memory footprint
- **Adaptive storage strategies**: Automatic selection between bloom filters (large sets) and HashSets (small sets)
- **Parent keyword tracking** enables phrase search verification without reading Parquet data
- **Index 0 optimization**: Aggregates keywords across all columns for efficient unfiltered searches on wide tables

**Production-Oriented Patterns**
- **Comprehensive testing**: 13 test modules covering edge cases, integration, and performance scenarios
- **Distributed index structure with dynamic chunk sizing** for efficient partial loading
- **Centralized configuration**: All tunable parameters in dedicated config module

**Tested Capabilities**
- Successfully indexes and searches individual Parquet files
- 7.5x faster than DataFusion for selective queries on moderate-cardinality data
- 81.8x faster when keywords exist separately but not together
- 170x faster when keywords don't exist (bloom filter rejection)
- Indexes approximately 15 million keywords in 4GB RAM (high-cardinality data)

## Project Context

This proof-of-concept was developed over 2-3 weeks to explore the foundational architecture for a fast, low-cost multi-petabyte query system capable of running purely from cloud object storage (e.g., AWS S3) without requiring expensive local NVMe storage.

Built as a first Rust project using hands-on implementation combined with AI-assisted learning for Rust-specific patterns.

### Current Status

**Completed:**
- Index and search individual Parquet files efficiently
- Optimized indexing with tested performance characteristics
- Demonstrated concept feasibility for scaling to larger systems
- Comprehensive configuration management system

**Future Enhancements** (detailed in Roadmap section):
- **Multi-file index consolidation** - Largest performance improvement potential, pruning thousands of files in 1 index lookup
- Limit detail size for a keyword to control index size
- Case-insensitive search options
- Wildcard and regex support
- Additional statistics for analytical queries
- Integration with query engines (Spark, Trino, Presto)

---

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd keywords

# Build the project
cargo build --release
```

### Basic Usage

```bash
# Create an index for a Parquet file
keywords index data.parquet

# Search for a keyword
keywords search data.parquet "user@example.com"

# Show help
keywords --help
```

### Library Usage

```rust
use keywords::{build_and_save_index, search};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Build an index
    build_and_save_index("data.parquet", None, None, None).await?;

    // Search for a keyword
    let result = search("data.parquet", "example", None, true).await?;

    if let Some(data) = result.verified_matches {
        println!("Found in columns: {:?}", data.columns);
        println!("Total occurrences: {}", data.total_occurrences);
    }

    Ok(())
}
```

---

## Architecture

### Index Structure

The index is stored as a `.index` directory adjacent to the Parquet file:

```
data.parquet
data.parquet.index/
├── filters.rkyv          # Bloom filters, metadata, column pool, and chunk index
└── data.bin              # Chunked keyword lists and occurrence data
```

### Core Components

#### 1. **Keyword Extraction** (`keyword_shred.rs`)
- Hierarchical splitting with 4 delimiter levels
- Parent keyword tracking for phrase search
- Run-length encoding for consecutive rows
- Efficient memory usage with `SmallVec` and `Rc<str>`

#### 2. **Index Building** (`index_data.rs`, `column_parquet_reader.rs`)
- Processes all string columns from Parquet files
- Creates bloom filters for fast existence checks
- Builds distributed index structure
- Designed for both local and cloud (S3) storage

#### 3. **Search Engine** (`searching/keyword_search.rs`)
- Global bloom filter for quick rejection
- Binary search for chunk location
- Exact match within chunks
- Parent keyword verification for phrases

#### 4. **Column Filters** (`index_structure/column_filter.rs`)
- Automatic selection between Bloom filter and HashSet
- Configurable false positive rate (default 1%)
- Space-efficient for large keyword sets

### Index 0 Optimization (Global Aggregate)

The index includes a special column_id 0 that aggregates keyword occurrences across **ALL** columns. This enables efficient searching when you don't know which column contains a keyword.

**Benefits:**
- Eliminates need to check bloom filters for every column
- Single lookup determines if keyword exists anywhere in file
- Particularly valuable for files with many columns

### Hierarchical Keyword Splitting

Keywords are split across 4 delimiter levels in sequence:

1. **Level 0**: Whitespace & structural: ` `, `\r`, `\n`, `\t`, `'`, `"`, `<`, `>`, `(`, `)`, `|`, `,`, `!`, `;`, `{`, `}`, `*`
2. **Level 1**: Path/network: `/`, `@`, `=`, `:`, `\`, `?`, `&`
3. **Level 2**: Dot notation: `.`, `$`, `#`, `` ` ``, `~`, `^`, `+`
4. **Level 3**: Word separators: `-`, `_`

**Example: "user@example.com"**
```
Level 0: No split → continue
Level 1: Split on @ → ["user@example.com", "user", "example.com"]
Level 2: "example.com" splits on . → ["example.com", "example", "com"]
Level 3: No further splits
Final keywords: ["user@example.com", "user", "example.com", "example", "com"]
```

This hierarchical approach enables:
- Searching for complete 'words': `"user@example.com"`
- Searching for components: `"example"` or `"com"`
- Efficient phrase matching using parent tracking
- Does not bloat by storing entire sentences, full json objects or full pieces of xml

### Performance Characteristics

**Index Building:**
- Memory: Proportional to unique keyword count (~15 million keywords in 4GB RAM for high-cardinality data)
- Single-threaded but I/O optimized

**Searching:**
- Time complexity: O(1) for bloom filter check + O(log n) for binary search
- Search time independent of Parquet file size
- Performance exceeds Apache DataFusion where row group statistics provide limited pruning
- Verified matches require no Parquet file access (uses parent tracking)

**Memory Usage:**
- Indexing: Proportional to unique keyword count
- Searching: Only index loaded, not Parquet data


### Example Performance Results

Testing on representative hardware with a 500,000-row Parquet file containing 5,000 random values across 10 columns (see [`test_performance_with_debug`](src/unit_tests/test_performance_with_debug.rs) for full test implementation):

```
┌─────────────────────────────────┬──────────────┬──────────────┐
│ Approach                        │ Time         │ Speedup      │
├─────────────────────────────────┼──────────────┼──────────────┤
│ Keyword Index (this project)    │      12.41ms │ baseline     │
│ DataFusion (pushdown + pruning) │      96.01ms │ 7.74x faster │
│ Naive (read all, filter)        │      93.37ms │ 7.52x faster │
└─────────────────────────────────┴──────────────┴──────────────┘
```

**Key Observations:**
- Keyword index: **7.5x faster** than Apache DataFusion for this workload
- DataFusion and naive approaches show similar performance (19ms vs 21ms), demonstrating that row group statistics provide minimal pruning benefit for higher-cardinality random data
- Pre-computed bloom filters eliminate the runtime cost of statistics evaluation
- Absolute times vary based on hardware, file size, and data characteristics
- Entire file pruning at the bloom filter stage or through row combination (filtering values in multiple columns where values exist, but not in the same row) is much faster

**Test Details:**
- Comparison includes Apache DataFusion 45.0 (industry-standard query engine with automatic row group pruning)
- Three-way comparison validates that traditional optimization techniques (row group statistics) are ineffective for this data pattern
- Full test code available in [`src/unit_tests/performance_test.rs`](src/unit_tests/performance_test.rs)

**When These Results Apply:**
- High-cardinality data (many unique values per column)
- Random or unstructured data distributions (logs, JSON fields, user-generated content)
- Selective keyword searches (finding specific values, not aggregations)
- Scenarios where row group statistics provide limited pruning opportunities

**Note:** These are representative results from development testing. The keyword index advantage is most pronounced for selective queries on higher-cardinality, unstructured data where traditional statistics-based pruning is ineffective. Sorted or clustered data may favor traditional query engines.

### Other Performance Results

Full test code available in [performance_comparison_test.rs](src/unit_tests/performance_comparison_test.rs)

**By Parquet Compression Algorithm**

| Compression | File Size | Index Build | Keyword Index | DataFusion | Speedup | Naive | Rows |
|-----------|-----------|-------------|---------------|------------|---------|-------|------|
| GZIP-9 | 10.88 MB | 4.3583499s | 13.6222ms | 156.594ms | 11.50x | 90.9093ms | 1 |
| ZSTD-18 | 10.76 MB | 2.4186176s | 7.0567ms | 85.801ms | 12.16x | 103.8817ms | 1 |
| SNAPPY | 12.04 MB | 2.8104062s | 4.3517ms | 44.3808ms | 10.20x | 72.8606ms | 1 |
| LZ4 | 12.24 MB | 2.8064314s | 5.873ms | 135.093ms | 23.00x | 136.323ms | 1 |
| BROTLI-9 | 10.76 MB | 3.3728874s | 27.9003ms | 464.6475ms | 16.65x | 340.8772ms | 1 |
| UNCOMPRESSED | 12.31 MB | 3.9386614s | 7.2508ms | 36.9216ms | 5.09x | 66.7026ms | 1 |

**By Number of Row Groups**

| Row Groups | File Size | Index Build | Keyword Index | DataFusion | Speedup | Naive | Rows |
|----------|-----------|-------------|---------------|------------|---------|-------|------|
| 1 RG | 8.63 MB | 3.921823s | 44.7161ms | 351.0242ms | 7.85x | 294.6081ms | 1 |
| 2 RG | 9.48 MB | 2.2785905s | 5.574ms | 55.2946ms | 9.92x | 80.5ms | 1 |
| 3 RG | 10.34 MB | 2.2239383s | 4.5927ms | 49.5821ms | 10.80x | 75.0611ms | 1 |
| 4 RG | 11.19 MB | 2.5387176s | 5.355ms | 78.0208ms | 14.57x | 90.2933ms | 1 |
| 5 RG | 12.05 MB | 2.7668245s | 5.1681ms | 76.8404ms | 14.87x | 104.251ms | 1 |
| 6 RG | 12.88 MB | 3.0459368s | 8.6993ms | 234.5235ms | 26.96x | 153.8865ms | 1 |
| 7 RG | 13.72 MB | 3.204549s | 11.4988ms | 510.6157ms | 44.41x | 325.7415ms | 1 |
| 8 RG | 14.61 MB | 3.8799205s | 6.4055ms | 205.0157ms | 32.01x | 76.0354ms | 1 |
| 9 RG | 15.47 MB | 2.1264371s | 3.5884ms | 32.9162ms | 9.17x | 68.347ms | 1 |
| 10 RG | 16.29 MB | 2.1735702s | 3.6252ms | 36.2306ms | 9.99x | 70.1446ms | 1 |
| 20 RG | 24.70 MB | 2.3807085s | 3.336ms | 45.7805ms | 13.72x | 79.8898ms | 1 |
| 30 RG | 32.45 MB | 2.6266485s | 3.0417ms | 51.8349ms | 17.04x | 89.0738ms | 1 |
| 40 RG | 39.11 MB | 2.9888384s | 3.5162ms | 52.9785ms | 15.07x | 95.2891ms | 1 |
| 50 RG | 44.85 MB | 3.1646377s | 3.096ms | 62.1345ms | 20.07x | 103.1466ms | 1 |

**By Cardinality (number of random strings in the pool used to build the parquet file)**

| Pool Size | File Size | Index Build | Keyword Index | DataFusion | Speedup | Naive | Rows |
|---------|-----------|-------------|---------------|------------|---------|-------|------|
| 50 | 3.66 MB | 1.0510112s | 11.4002ms | 88.745ms | 7.78x | 142.8014ms | 3 |
| 500 | 5.83 MB | 2.9693827s | 5.4315ms | 42.7186ms | 7.86x | 65.1274ms | 1 |
| 5000 | 12.06 MB | 2.5237372s | 4.4687ms | 35.3959ms | 7.92x | 68.5236ms | 1 |
| 50000 | 46.08 MB | 5.8127867s | 18.4448ms | 92.0191ms | 4.99x | 152.171ms | 1 |
| 500000 | 87.69 MB | 13.3885345s | 30.5061ms | 174.8759ms | 5.73x | 221.0582ms | 1 |

**Where all keywords exist within columns, but not together in any row**

| Method | Time | Rows Found |
|--------|------|------------|
| Keyword Index | 2.7821ms | 0 |
| DataFusion | 227.592ms | 0 |
| Naive | 340.7123ms | 0 |

An **81.8x** speedup

**Where keywords do not exist in the data at all (bloom filter rejection)**

| Method | Time | Rows Found |
|--------|------|------------|
| Keyword Index | 1.3479ms | 0 |
| DataFusion | 229.1298ms | 0 |
| Naive | 320.7045ms | 0 |

A **170x** speedup

---

## Features

### Fast Indexing
- Processes all string columns from Parquet files
- Optimized I/O with minimal memory allocations
- Smart file reading: complete caching for files <2MB
- Efficient metadata extraction with range requests for large files

### Fast Searches
- Bloom filter-based existence checks (configurable false positive rate)
- Binary search to locate data chunks
- Parent keyword verification without Parquet access
- Configurable column filtering

### Phrase Search
- Multi-token phrase matching using parent relationships
- Can verify some multi-token matches without reading the Parquet file

### Cloud Storage Support
- Architecture designed for cloud storage (S3, Azure, GCP via `object_store` crate)
- Automatic retry logic for transient failures
- Efficient range requests for metadata
- Basic S3 read functions tested; full cloud indexing and searching designed but not extensively tested

### Validation
- Index validation checks file size, ETag, and last modified time
- Prevents stale index usage
- Clear error messages for missing or outdated indexes

---

## Dependencies

Key dependencies used in this project:

- **arrow** (57.0.0): Arrow data format integration
- **bytes** (1.10.1): Efficient byte buffer management
- **dashmap** (6.1.0): Concurrent hash map
- **futures** (0.3.31): Async programming primitives
- **hashbrown** (0.16.0): High-performance hash maps
- **indexmap** (2.12.0): Order-preserving hash maps
- **object_store** (0.12.4): Cloud storage abstraction (S3, Azure, GCP)
- **once_cell** (1.21.3): Single assignment cells for lazy initialization
- **parquet** (57.0.0): Parquet file format support
- **rand** (0.9.2): Random number generation for testing
- **rkyv** (0.8.12): Zero-copy deserialization
- **smallvec** (1.15.1): Stack-allocated vectors for common small cases
- **tokio** (1.48.0): Async runtime

See `Cargo.toml` for complete dependency list with versions.

---

## Project Structure

```
keywords/
├── src/
│   ├── main.rs                    # CLI entry point
│   ├── lib.rs                     # Public library API
│   ├── keyword_shred.rs           # Hierarchical keyword extraction
│   ├── column_parquet_reader.rs   # Parquet file processing
│   ├── index_data.rs              # Index building and serialization
│   ├── index_structure/
│   │   ├── column_filter.rs       # Bloom filter implementation
│   │   ├── index_files.rs         # Index file path management
│   │   └── mod.rs
│   ├── searching/
│   │   ├── keyword_search.rs      # Search implementation
│   │   ├── search_results.rs      # Search result types
│   │   ├── pruned_reader.rs       # Optimized Parquet reading
│   │   └── tests/                 # Search-specific tests
│   ├── utils/
│   │   ├── column_pool.rs         # Column name deduplication
│   │   ├── file_interaction_local_and_cloud.rs  # Storage abstraction
│   │   └── mod.rs
│   ├── unit_tests/                # Integration and performance tests
│   └── keyword_shred/test/        # Hierarchical keyword extraction tests
├── Cargo.toml
├── COPYRIGHT.txt
└── README.md
```

---

## Limitations

Current implementation has the following constraints (appropriate for POC phase):

- **Case-sensitive**: "Email" and "email" are treated as different keywords
- **Exact token match**: No wildcard support; searches match tokens as split during indexing
- **Memory bound during indexing**: Entire index must fit in memory during construction. No disk spooling during index build.
- **Memory bound during searching**: Entire index must fit in memory
- **Single-threaded indexing**: Indexing is not parallelized (but I/O is optimized)
- **No incremental updates**: Index must be rebuilt if Parquet file changes
- **String columns only**: Currently focuses on string data types

---

## Index Size Management

### Challenge

High-cardinality columns with many rows per keyword can create excessively large indexes that provide minimal performance benefit over direct Parquet filtering. When a keyword appears in many rows, storing detailed row-level information for each occurrence may cause the index to grow disproportionately to the benefit provided.

### Current Behavior

The index currently stores complete row-level detail for every keyword occurrence, including:
- Row numbers with run-length encoding
- Row group mappings
- Parent keyword relationships for phrase search
- Split-level metadata

For keywords appearing in a small to moderate number of rows, this provides excellent search performance with minimal overhead. However, for very common keywords appearing across many rows, the index size can grow substantially while providing diminishing returns.

### Planned Solution

A threshold-based approach that gracefully degrades behavior for high-frequency keywords:

**1. Row Information Threshold** (configurable parameter requiring profiling)
- Keywords below threshold: Store full row details (current behavior)
- Keywords above threshold: Switch to summary-only storage

The optimal threshold value depends on the specific dataset characteristics and would need to be determined through profiling and measurement.

**2. Degraded Storage Mode** (for keywords exceeding threshold)
Instead of storing every row occurrence, store only:
- Keyword existence in column (bloom filter - already present)
- Approximate occurrence count
- Row group statistics (which row groups contain the keyword, not individual rows)
- Indication that this keyword is in "high-frequency mode"

**3. Search Behavior Adaptation**
- **Selective keywords** (below threshold): Use indexed row details for instant results
- **Common keywords** (above threshold): Return indication that Parquet-level filtering is needed
- **Clear result indication**: `SearchResult` distinguishes between verified matches (from index) and high-frequency keywords requiring verification

**4. Trade-off Analysis**

This approach provides several benefits:

| Aspect | Benefit |
|--------|---------|
| **Index Size** | Prevents index from growing larger than the Parquet file itself |
| **Memory** | Keeps memory requirements predictable and bounded |
| **Search Speed** | Maintains fast searches for selective queries while being honest about limitations for common terms |
| **Practicality** | Very common keywords typically indicate the term isn't selective enough for index optimization anyway |

The system would provide clear user feedback when searches encounter high-frequency keywords, suggesting either Parquet-level filtering or more specific search terms.

### Implementation Status

**Current State**: All keywords store full row detail regardless of frequency. The system works well for datasets with moderate keyword frequencies.

**Threshold Approach**: Designed but not yet implemented. The architecture already supports different storage strategies (bloom filter vs HashSet based on size), making this enhancement a natural extension.

**When to Consider Implementing**:
- If monitoring shows indexes growing disproportionately large relative to Parquet files
- If common queries frequently return very large result sets
- If memory constraints during indexing become problematic for production workloads

For the current POC phase, the full-detail approach provides valuable insights into real-world cardinality distributions and helps validate whether the threshold-based approach would provide meaningful benefits for production use cases.

**With Additional Development Time**: Profiling on representative production datasets would inform specific threshold values and validate the memory/performance trade-offs. Additionally, benchmark testing across different data distributions (uniform vs. power-law keyword frequencies) would help quantify when the threshold approach provides meaningful benefits versus the current full-detail implementation.

---

## Advanced Features

### Parent Keyword Tracking

Keywords maintain references to their parent keywords in the split hierarchy. This enables efficient phrase search without reading the Parquet file.

### Bloom Filter Configuration

Customize the false positive rate during index creation:

```rust
use keywords::build_and_save_index;
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Build index with 0.1% false positive rate (lower = more memory)
    let error_rate = Some(0.001);
    let exclude_columns = None;

    build_and_save_index("data.parquet", exclude_columns, error_rate, None).await?;

    Ok(())
}
```

### Excluding Columns

Exclude specific columns from indexing to reduce index size and improve performance. Consider excluding:

- **Unique identifiers**: Columns like `id`, `uuid`, `transaction_id` that are never searched by keyword
- **High cardinality, low-value columns**: Data that's unique per row but not useful for search (e.g., raw JSON blobs, internal codes)
- **Time fields**: Timestamps and dates are typically queried by range, not exact match, making keyword indexing inefficient
- **Binary or encoded data**: Base64 strings, hashes, or other encoded values

```rust
use keywords::build_and_save_index;
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut exclude = HashSet::new();
    exclude.insert("id".to_string());               // Unique identifier
    exclude.insert("user_uuid".to_string());        // High cardinality, never searched
    exclude.insert("created_at".to_string());       // Time field (use range queries)
    exclude.insert("updated_at".to_string());       // Time field
    exclude.insert("internal_metadata".to_string()); // Raw JSON, not useful for keyword search

    build_and_save_index("data.parquet", Some(exclude), None, None).await?;

    Ok(())
}
```

**Why exclude these columns?**
- Reduces index size (less memory needed)
- Faster indexing (fewer keywords to process)
- Better search performance (smaller bloom filters)
- Focuses index on columns that benefit from keyword search

---

## Parallelization Strategy

Single-threaded was chosen for the POC phase to reduce complexity while establishing baseline performance and only dealing with single files.

---

## Roadmap / Future Improvements

**Near-term Enhancements:**
- Case-insensitive search option (store keywords normalized)
- Wildcard support (trailing wildcards initially)
- Metadata caching within index (eliminate additional GET requests)
- Block compression in data.bin
- Improved delimiter configuration
- Row information threshold for high-frequency keywords (see Index Size Management section)

**Index Scaling:**
- Multi-file index consolidation at partition level (e.g., daily aggregates)
- Hierarchical index structure with range metadata
- Compaction process for late-arriving data
- Spill-to-disk strategy for large files (temporary partitioned files during indexing)

**Query Capabilities:**
- Leading wildcard support via reverse index
- SQL-like query interface (investigate sqlparser-rs)
- Complex boolean queries (AND, OR, NOT operations)
- Pattern extraction (IP addresses, email domains, etc.)
- Abstract entity searching (hostname/domain matching, CIDR IP matching)

**Performance & Scale:**
- Parallel indexing (multi-threaded column processing)
- Parallel searching
- Additional statistics storage for analytical queries
- Integration with distributed query engines (Spark, Trino, Presto)
- Iceberg table format integration

**Additional Formats:**
- Line-separated JSON
- CSV with schema detection
- Arbitrary text with regex field extraction
- Plain text with line-based processing

**Testing & Validation:**
- Test on large public datasets (e.g., NYC Taxi Trip Data)
- Comprehensive benchmarking suite
- Memory profiling and optimization
- Cost analysis (storage vs compute trade-offs)

---

## FAQ

**Q: How much memory does searching use?**  
A: The entire index is loaded into memory during search. The actual Parquet data is not loaded unless verification is needed.

**Q: Can I update an existing index?**  
A: No. You must rebuild the entire index if the Parquet file changes.

**Q: Is the search thread-safe?**  
A: `KeywordSearcher` is not `Sync` due to internal buffer management. Create one instance per thread or use appropriate synchronization primitives.

**Q: What about transient network failures when accessing cloud storage?**  
A: The `object_store` library includes built-in retry logic with exponential backoff for handling transient failures automatically.

**Q: How do I validate my index is up-to-date?**  
A: Use the `validate_index()` function which checks file size, ETag, and last modified time.

---

Copyright (c) 2025 Andrew Smith. All rights reserved. See [COPYRIGHT.txt](COPYRIGHT.txt) for full terms.