pub mod keyword_search;
pub mod pruned_reader;
pub mod search_results;

// Link to test module (only compiled during tests)
#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;