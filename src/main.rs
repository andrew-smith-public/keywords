//! Parquet Keyword Indexer and Searcher - Command Line Interface
//!
//! This binary provides a command-line interface for building keyword indexes from Parquet files
//! and performing fast keyword searches using those indexes. The indexer extracts and indexes all
//! text content from Parquet files, enabling sub-second keyword lookups even in multi-gigabyte files.
//!
//! # Commands
//!
//! - **`index`** - Creates a distributed keyword index for a Parquet file
//! - **`search`** - Searches for a keyword in an indexed Parquet file
//! - **`index_info`** - Displays detailed information about an index
//!
//! # Index Structure
//!
//! The index is stored in a `.index` directory adjacent to the Parquet file:
//! ```text
//! data.parquet
//! data.parquet.index/
//! ├── filters.rkyv       (Bloom filters, metadata, column pool, and chunk index)
//! └── data.bin           (Chunked keyword lists and occurrence data)
//! ```
//!
//! # Usage Examples
//!
//! ```bash
//! # Create an index for a Parquet file
//! keywords index data.parquet
//!
//! # Search for a keyword
//! keywords search data.parquet "email@example.com"
//!
//! # View index information
//! keywords index_info data.parquet
//!
//! # Show help
//! keywords --help
//! ```
//!
//! # Performance
//!
//! - **Searching**: Sub-second lookups even in indexes with millions of keywords
//!
//! # Exit Codes
//!
//! - `0` - Success
//! - `1` - General error (invalid arguments, indexing failure, search failure)
//! - `2` - Index is out of date (search command only)
//! - `3` - Index validation failed (search command only)
//!

// ============================================================================
// Distributed Index Structures
// ============================================================================

use std::env;
use std::process;

/// Entry point for the Parquet keyword indexer and searcher CLI.
///
/// This async function orchestrates command-line argument parsing and delegates to
/// appropriate handler functions based on the command. It supports three main operations:
/// building keyword indexes, searching within them, and viewing index information.
///
/// # Command Structure
///
/// The CLI follows this pattern:
/// ```text
/// keywords <command> <arguments...>
/// ```
///
/// # Supported Commands
///
/// - **`index <file.parquet>`** - Creates a keyword index
/// - **`search <file.parquet> <keyword>`** - Searches for a keyword
/// - **`index_info <file.parquet>`** - Displays index information
/// - **`--help` or `-h`** - Displays help information
///
/// # Error Handling
///
/// - Invalid arguments: Prints error message and help, exits with code 1
/// - Unknown commands: Prints error message and help, exits with code 1
/// - Command-specific errors: Handled by respective handler functions
///
/// # Examples
///
/// Running the binary from the command line:
///
/// ```bash
/// # Index a file
/// $ keywords index data.parquet
/// Indexing file: data.parquet
/// This may take a while for large files...
/// ✓ Indexing completed successfully!
///
/// # Search for a keyword
/// $ keywords search data.parquet "john@example.com"
/// Searching for 'john@example.com' in data.parquet
/// ✓ Keyword found!
///   Columns: ["email", "cc_list"]
///   Total occurrences: 42
///
/// # View index information
/// $ keywords index_info data.parquet
/// Index Information for: data.parquet
/// ...
///
/// # Show help
/// $ keywords --help
/// Parquet Keyword Indexer and Searcher
/// ...
/// ```
#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    // Handle --help flag
    if args.len() == 2 && (args[1] == "--help" || args[1] == "-h") {
        print_help();
        return;
    }

    // Validate minimum arguments
    if args.len() < 3 {
        eprintln!("Error: Not enough arguments\n");
        print_help();
        process::exit(1);
    }

    let command = &args[1];

    match command.as_str() {
        "index" => {
            if args.len() != 3 {
                eprintln!("Error: 'index' command requires exactly one file path\n");
                print_help();
                process::exit(1);
            }
            let file_path = &args[2];
            handle_index(file_path).await;
        }
        "search" => {
            if args.len() != 4 {
                eprintln!("Error: 'search' command requires file path and keyword\n");
                print_help();
                process::exit(1);
            }
            let file_path = &args[2];
            let keyword = &args[3];
            handle_search(file_path, keyword).await;
        }
        "index_info" => {
            if args.len() != 3 {
                eprintln!("Error: 'index_info' command requires exactly one file path\n");
                print_help();
                process::exit(1);
            }
            let file_path = &args[2];
            handle_index_info(file_path).await;
        }
        _ => {
            eprintln!("Error: Unknown command '{}'\n", command);
            print_help();
            process::exit(1);
        }
    }
}

/// Handles the `index` command to create a keyword index for a Parquet file.
///
/// This function builds a complete distributed keyword index from the specified Parquet file.
/// The indexing process:
/// 1. Reads all string columns from the Parquet file
/// 2. Extracts and splits keywords using hierarchical delimiters
/// 3. Builds bloom filters for fast keyword existence checks
/// 4. Creates a distributed index structure for efficient lookups
/// 5. Saves the index to a `.index` directory next to the Parquet file
///
/// # Arguments
///
/// * `file_path` - Path to the Parquet file to index (local or S3 path)
///
/// # Index Output
///
/// Creates `<file_path>.index/` directory containing:
/// - `filters.rkyv` - Bloom filters, metadata, column pool, and chunk index
/// - `data.bin` - Chunked keyword lists and occurrence data
///
/// # Exit Codes
///
/// - `0` - Indexing completed successfully
/// - `1` - Indexing failed (file not found, invalid Parquet, I/O error, etc.)
///
/// # Performance
///
/// - Small files (<100MB): Typically completes in seconds
/// - Medium files (100MB-1GB): Takes 1-5 minutes
///
/// # Examples
///
/// ```bash
/// # Index a local file
/// $ keywords index data.parquet
/// Indexing file: data.parquet
/// This may take a while for large files...
/// ✓ Indexing completed successfully!
///
/// # Index an S3 file
/// $ keywords index s3://my-bucket/data.parquet
/// Indexing file: s3://my-bucket/data.parquet
/// This may take a while for large files...
/// ✓ Indexing completed successfully!
/// ```
///
/// # Notes
///
/// - If an index already exists, it will be overwritten
/// - The process is single-threaded but I/O optimized
/// - Memory usage is proportional to the number of unique keywords
/// - S3 access requires appropriate AWS credentials
async fn handle_index(file_path: &str) {
    println!("Indexing file: {}", file_path);
    println!("This may take a while for large files...\n");

    match keywords::build_and_save_index(file_path, None, None, None).await {
        Ok(()) => {
            println!("\n✓ Indexing completed successfully!");
        }
        Err(e) => {
            eprintln!("\n✗ Error during indexing: {}", e);
            process::exit(1);
        }
    }
}

/// Handles the `search` command to find a keyword in an indexed Parquet file.
///
/// This function performs a fast lookup of a keyword in the pre-built index. The search process:
/// 1. Verifies the index exists
/// 2. Validates the index is up-to-date with the Parquet file
/// 3. Performs a bloom filter check for fast negative results
/// 4. If present, retrieves detailed occurrence information
/// 5. Displays results including columns and occurrence counts
///
/// # Arguments
///
/// * `file_path` - Path to the indexed Parquet file (local or S3)
/// * `keyword` - The keyword to search for (case-sensitive, exact match)
///
/// # Search Behavior
///
/// - **Case sensitive**: "Email" and "email" are different keywords
/// - **Exact match**: Searches for the keyword as-is, no wildcards
/// - **Split-aware**: Matches keywords as they were split during indexing
/// - **Fast negative results**: Uses bloom filters to quickly return "not found"
///
/// # Exit Codes
///
/// - `0` - Search completed successfully (keyword found or not found)
/// - `1` - Index not found or search error
/// - `2` - Index is out of date with the Parquet file
/// - `3` - Index validation failed
///
/// # Output Format
///
/// **When keyword is found:**
/// ```text
/// ✓ Keyword found!
///   Columns: ["email", "description", "notes"]
///   Total occurrences: 127
/// ```
///
/// **When keyword is not found:**
/// ```text
/// ✗ Keyword not found in index
/// ```
///
/// # Examples
///
/// ```bash
/// # Search for an email address
/// $ keywords search data.parquet "user@example.com"
/// Searching for 'user@example.com' in data.parquet
/// ✓ Keyword found!
///   Columns: ["email", "cc"]
///   Total occurrences: 3
///
/// # Search for a word
/// $ keywords search data.parquet "python"
/// Searching for 'python' in data.parquet
/// ✓ Keyword found!
///   Columns: ["description", "tags", "requirements"]
///   Total occurrences: 856
///
/// # Search for non-existent keyword
/// $ keywords search data.parquet "xyz123"
/// Searching for 'xyz123' in data.parquet
/// ✗ Keyword not found in index
/// ```
///
/// # Error Cases
///
/// **Index not found:**
/// ```bash
/// $ keywords search data.parquet "keyword"
/// Error: No index found for 'data.parquet'
/// Please run 'index' command first to create the index.
/// ```
///
/// **Index out of date:**
/// ```bash
/// $ keywords search data.parquet "keyword"
/// Warning: Index is out of date with the parquet file
/// Consider rebuilding the index with 'index' command.
/// ```
///
/// # Performance
///
/// - Search time is independent of Parquet file size
async fn handle_search(file_path: &str, keyword: &str) {
    // First check if index exists
    if !keywords::index_exists(file_path).await {
        eprintln!("Error: No index found for '{}'", file_path);
        eprintln!("Please run 'index' command first to create the index.");
        process::exit(1);
    }

    // Validate index is up-to-date
    match keywords::validate_index(file_path).await {
        Ok(true) => {
            // Index is valid, proceed with search
        }
        Ok(false) => {
            eprintln!("Warning: Index is out of date with the parquet file");
            eprintln!("Consider rebuilding the index with 'index' command.");
            process::exit(2);
        }
        Err(e) => {
            eprintln!("Warning: Could not validate index: {}", e);
            process::exit(3);
        }
    }

    println!("Searching for '{}' in {}", keyword, file_path);

    match keywords::search(file_path, keyword, None, false).await {
        Ok(result) => {
            if result.found {
                println!("\n✓ Keyword found!");
                if let Some(data) = result.verified_matches {
                    println!("  Columns: {:?}", data.columns);
                    println!("  Total occurrences: {:?}", data.total_occurrences);
                }
                if let Some(needs_check) = result.needs_verification {
                    println!("  (Plus {} occurrences needing verification)", needs_check.total_occurrences);
                }
            } else {
                println!("\n✗ Keyword not found in index");
            }
        }
        Err(e) => {
            eprintln!("\n✗ Error during search: {}", e);
            process::exit(1);
        }
    }
}

/// Handles the `index_info` command to display detailed information about an index.
///
/// This function loads the index metadata and presents comprehensive information about
/// the index structure, contents, and validation data. The information includes:
/// - Index version and configuration
/// - Parquet file validation metadata
/// - Column information
/// - Keyword statistics (total and per-column)
/// - Index file sizes
/// - Chunk information
///
/// # Arguments
///
/// * `file_path` - Path to the indexed Parquet file
///
/// # Exit Codes
///
/// - `0` - Information displayed successfully
/// - `1` - Index not found or error reading index
///
/// # Output Format
///
/// ```text
/// ================================================================================
/// Index Information for: data.parquet
/// ================================================================================
///
/// INDEX METADATA
/// --------------
/// Version:              1
/// Error Rate:           0.01 (1.00%)
/// Chunk Size:           1000 keywords
/// Number of Chunks:     15
///
/// PARQUET FILE VALIDATION
/// -----------------------
/// File Size:            52428800 bytes (50.00 MB)
/// Last Modified:        1699564800 (Unix timestamp)
/// ETag:                 "d41d8cd98f00b204e9800998ecf8427e"
///
/// INDEXED COLUMNS
/// ---------------
/// Total Columns:        5
/// Columns:
///   - email
///   - name
///   - description
///   - tags
///   - metadata
///
/// KEYWORD STATISTICS
/// ------------------
/// Total Keywords:       15234
///
/// INDEX FILE SIZES
/// ----------------
/// filters.rkyv:         704512 bytes (688.00 KB)
/// data.bin:             1048576 bytes (1.00 MB)
/// ────────────────────────────────────────
/// Total Index Size:     1753088 bytes (1.67 MB)
///
/// ================================================================================
/// ```
///
/// # Examples
///
/// ```bash
/// # View index information for a local file
/// $ keywords index_info data.parquet
/// ================================================================================
/// Index Information for: data.parquet
/// ...
///
/// # View information for an S3 file
/// $ keywords index_info s3://my-bucket/data.parquet
/// ================================================================================
/// Index Information for: s3://my-bucket/data.parquet
/// ...
/// ```
///
/// # Error Cases
///
/// **Index not found:**
/// ```bash
/// $ keywords index_info data.parquet
/// Error: No index found for 'data.parquet'
/// Please run 'index' command first to create the index.
/// ```
async fn handle_index_info(file_path: &str) {
    // First check if index exists
    if !keywords::index_exists(file_path).await {
        eprintln!("Error: No index found for '{}'", file_path);
        eprintln!("Please run 'index' command first to create the index.");
        process::exit(1);
    }

    // Get index information
    match keywords::get_index_info(file_path, None).await {
        Ok(info) => {
            println!("================================================================================");
            println!("Index Information for: {}", file_path);
            println!("================================================================================");
            println!();

            // Index Metadata
            println!("INDEX METADATA");
            println!("──────────────");
            println!("Version:              {}", info.version);
            println!("Error Rate:           {} ({:.2}%)", info.error_rate, info.error_rate * 100.0);
            println!("Max Chunk Size Bytes: {} keywords", info.max_chunk_size_bytes);
            println!("Number of Chunks:     {}", info.num_chunks);
            println!();

            // Parquet File Validation
            println!("PARQUET FILE VALIDATION");
            println!("───────────────────────");
            println!("File Size:            {} bytes ({:.2} MB)",
                     info.parquet_size,
                     info.parquet_size as f64 / (1024.0 * 1024.0));
            println!("Last Modified:        {} (Unix timestamp)", info.parquet_last_modified);
            if !info.parquet_etag.is_empty() {
                println!("ETag:                 \"{}\"", info.parquet_etag);
            }
            println!();

            // Indexed Columns
            println!("INDEXED COLUMNS");
            println!("───────────────");
            println!("Total Columns:        {}", info.num_columns);
            println!("Columns:");
            for column in &info.indexed_columns {
                println!("  - {}", column);
            }
            println!();

            // Keyword Statistics
            println!("KEYWORD STATISTICS");
            println!("──────────────────");
            println!("Total Keywords:       {}", info.total_keywords);
            println!();

            // Index File Sizes
            println!("INDEX FILE SIZES");
            println!("────────────────");
            println!("filters.rkyv:         {} bytes ({:.2} KB)",
                     info.filters_size,
                     info.filters_size as f64 / 1024.0);
            println!("data.bin:             {} bytes ({:.2} KB)",
                     info.data_size,
                     info.data_size as f64 / 1024.0);
            println!("────────────────────────────────────────");
            println!("Total Index Size:     {} bytes ({:.2} MB)",
                     info.total_size,
                     info.total_size as f64 / (1024.0 * 1024.0));
            println!();
            println!("================================================================================");
        }
        Err(e) => {
            eprintln!("\n✗ Error reading index information: {}", e);
            process::exit(1);
        }
    }
}

/// Prints comprehensive help information for the CLI.
///
/// This function displays usage information, command descriptions, examples, and notes
/// about how to use the Parquet keyword indexer and searcher. Called automatically when:
/// - User passes `--help` or `-h` flag
/// - User provides invalid arguments
/// - User provides an unknown command
///
/// # Output Format
///
/// The help output includes:
/// - Program title
/// - Usage syntax for all commands
/// - Detailed command descriptions
/// - Available options
/// - Practical examples
/// - Important notes about index structure and behavior
///
/// # Examples
///
/// ```bash
/// $ keywords --help
/// Parquet Keyword Indexer and Searcher
///
/// USAGE:
///   keywords index <file.parquet>
///   keywords search <file.parquet> <keyword>
///   keywords index_info <file.parquet>
///   keywords --help
///
/// COMMANDS:
///   index              Create an index for a parquet file
///   search             Search for a keyword in an indexed parquet file
///   index_info         Display detailed information about an index
///
/// OPTIONS:
///   --help, -h         Show this help message
///
/// EXAMPLES:
///   # Create an index
///   keywords index data.parquet
///
///   # Search for a keyword
///   keywords search data.parquet "hello"
///
///   # View index information
///   keywords index_info data.parquet
///
/// NOTE:
///   - The index command creates a .index directory next to the parquet file
///   - You must create an index before searching
///   - Searches are very fast once the index is built
/// ```
fn print_help() {
    println!("Parquet Keyword Indexer and Searcher");
    println!();
    println!("USAGE:");
    println!("  {} index <file.parquet>", env::args().nth(0).unwrap_or_else(|| "program".to_string()));
    println!("  {} search <file.parquet> <keyword>", env::args().nth(0).unwrap_or_else(|| "program".to_string()));
    println!("  {} index_info <file.parquet>", env::args().nth(0).unwrap_or_else(|| "program".to_string()));
    println!("  {} --help", env::args().nth(0).unwrap_or_else(|| "program".to_string()));
    println!();
    println!("COMMANDS:");
    println!("  index              Create an index for a parquet file");
    println!("  search             Search for a keyword in an indexed parquet file");
    println!("  index_info         Display detailed information about an index");
    println!();
    println!("OPTIONS:");
    println!("  --help, -h         Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("  # Create an index");
    println!("  {} index data.parquet", env::args().nth(0).unwrap_or_else(|| "program".to_string()));
    println!();
    println!("  # Search for a keyword");
    println!("  {} search data.parquet \"hello\"", env::args().nth(0).unwrap_or_else(|| "program".to_string()));
    println!();
    println!("  # View index information");
    println!("  {} index_info data.parquet", env::args().nth(0).unwrap_or_else(|| "program".to_string()));
    println!();
    println!("NOTE:");
    println!("  - The index command creates a .index directory next to the parquet file");
    println!("  - You must create an index before searching");
    println!("  - Searches are very fast once the index is built");
    println!("  - Use index_info to view comprehensive details about an existing index");
}