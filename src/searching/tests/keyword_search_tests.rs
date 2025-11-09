#[cfg(test)]
mod tests {
    use crate::searching::keyword_search::KeywordSearcher;
    use crate::index_structure::column_filter::ColumnFilter;
    use crate::index_data::IndexFilters;
    use crate::utils::column_pool::ColumnPool;

    /// Creates a test searcher with standard split character configuration
    fn create_test_searcher() -> KeywordSearcher {
        let split_chars_inclusive: Vec<Vec<char>> = vec![
            vec!['\r', '\n', '\t', '\'', '"', '<', '>', '(', ')', '|', ',', '!', ';', '{', '}', '*', ' '],
            vec!['/', '@', '=', ':', '\\', '?', '&'],
            vec!['.', '$', '#', '`', '~', '^', '+'],
            vec!['-', '_'],
        ];

        let filters = IndexFilters {
            version: 1,
            parquet_etag: "test".to_string(),
            parquet_size: 0,
            parquet_last_modified: 0,
            error_rate: 0.01,
            split_chars_inclusive,
            column_pool: ColumnPool::new(),
            column_filters: std::collections::HashMap::new(),
            global_filter: ColumnFilter::RkyvHashSet(vec![]),
            chunk_index: vec![],
            parquet_metadata_offset: 5,
            parquet_metadata_length: 5
        };

        KeywordSearcher {
            filters,
            index_dir: String::new(),
            index_file_prefix: None,
        }
    }

    #[test]
    fn test_hierarchical_split_email() {
        let searcher = create_test_searcher();

        // Test what split_phrase produces for "user@example.com"
        let tokens = searcher.split_phrase("user@example.com");

        println!("Tokens from split_phrase('user@example.com'): {:?}", tokens);

        // EXPECTED (from hierarchical indexing):
        // ["user@example.com", "user", "example.com", "example", "com"]
        //
        // With OLD buggy code: ["user", "example", "com"] - length 3 ❌
        // With NEW fixed code: ["com", "example", "example.com", "user", "user@example.com"] - length 5 ✅

        // These assertions PASS with fixed code, FAIL with buggy code
        assert!(tokens.contains(&"user@example.com".to_string()),
                "Missing root parent token 'user@example.com'");
        assert!(tokens.contains(&"example.com".to_string()),
                "Missing intermediate parent token 'example.com'");
        assert!(tokens.contains(&"user".to_string()),
                "Missing leaf token 'user'");
        assert!(tokens.contains(&"example".to_string()),
                "Missing leaf token 'example'");
        assert!(tokens.contains(&"com".to_string()),
                "Missing leaf token 'com'");
        assert_eq!(tokens.len(), 5,
                   "Should have all 5 hierarchical tokens, got {} tokens: {:?}", tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_hyphenated_name() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("john-smith-jr");

        println!("Tokens from split_phrase('john-smith-jr'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // ["john-smith-jr", "john", "smith", "jr"]
        //
        // With OLD buggy code: ["john", "smith", "jr"] - length 3 ❌
        // With NEW fixed code: All 4 tokens present ✅

        assert!(tokens.contains(&"john-smith-jr".to_string()),
                "Missing parent token 'john-smith-jr'");
        assert!(tokens.contains(&"john".to_string()));
        assert!(tokens.contains(&"smith".to_string()));
        assert!(tokens.contains(&"jr".to_string()));
        assert_eq!(tokens.len(), 4,
                   "Should have 4 tokens, got {}: {:?}", tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_path() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("/usr/local/bin");

        println!("Tokens from split_phrase('/usr/local/bin'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // ["/usr/local/bin", "usr", "local", "bin"]
        //
        // With OLD buggy code: ["usr", "local", "bin"] - length 3 ❌
        // With NEW fixed code: All 4 tokens present ✅

        assert!(tokens.contains(&"/usr/local/bin".to_string()),
                "Missing parent token '/usr/local/bin'");
        assert!(tokens.contains(&"usr".to_string()));
        assert!(tokens.contains(&"local".to_string()));
        assert!(tokens.contains(&"bin".to_string()));
        assert_eq!(tokens.len(), 4,
                   "Should have 4 tokens, got {}: {:?}", tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_domain() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("api.example.com");

        println!("Tokens from split_phrase('api.example.com'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // Split at level 2 (dot notation):
        // ["api.example.com", "api", "example.com", "example", "com"]
        //
        // But wait - "example.com" should also be processed:
        // So we get: ["api.example.com", "api", "example.com", "example", "com"]
        //
        // With OLD buggy code: ["api", "example", "com"] - length 3 ❌
        // With NEW fixed code: At least ["api.example.com", "example.com"] parents present ✅

        assert!(tokens.contains(&"api.example.com".to_string()),
                "Missing root parent 'api.example.com'");
        assert!(tokens.contains(&"api".to_string()));
        assert!(tokens.contains(&"example".to_string()));
        assert!(tokens.contains(&"com".to_string()));

        // The exact hierarchical structure should create more than 3 tokens
        assert!(tokens.len() > 3,
                "Should have more than 3 tokens for hierarchical split, got {}: {:?}",
                tokens.len(), tokens);
    }

    #[test]
    fn test_hierarchical_split_complex() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("user-name@test.example.com");

        println!("Tokens from split_phrase('user-name@test.example.com'): {:?}", tokens);

        // EXPECTED (hierarchical):
        // Level 0: No split
        // Level 1: Split on @ → ["user-name@test.example.com", "user-name", "test.example.com"]
        // Level 2: "test.example.com" splits on . → ["test.example.com", "test", "example.com", "example", "com"]
        // Level 3: "user-name" splits on - → ["user-name", "user", "name"]
        //
        // Full set: ["user-name@test.example.com", "user-name", "test.example.com",
        //            "example.com", "user", "name", "test", "example", "com"]
        //
        // With OLD buggy code: Just ["user", "name", "test", "example", "com"] - length 5 ❌
        // With NEW fixed code: Many more tokens including all parents ✅

        assert!(tokens.contains(&"user-name@test.example.com".to_string()),
                "Missing root parent");
        assert!(tokens.contains(&"user-name".to_string()),
                "Missing 'user-name' parent");
        assert!(tokens.contains(&"test.example.com".to_string()),
                "Missing 'test.example.com' parent");

        // Should have significantly more than just the leaf tokens
        assert!(tokens.len() > 5,
                "Should have more than 5 tokens for complex hierarchical split, got {}: {:?}",
                tokens.len(), tokens);
    }

    #[test]
    fn test_single_token_no_split() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("simple");

        println!("Tokens from split_phrase('simple'): {:?}", tokens);

        // No delimiters - should just return the single token
        assert_eq!(tokens.len(), 1);
        assert!(tokens.contains(&"simple".to_string()));
    }

    #[test]
    fn test_empty_string() {
        let searcher = create_test_searcher();

        let tokens = searcher.split_phrase("");

        println!("Tokens from split_phrase(''): {:?}", tokens);

        // Empty string might return empty vec or vec with empty string
        // Either is acceptable
        assert!(tokens.is_empty() || (tokens.len() == 1 && tokens[0].is_empty()),
                "Empty string should return empty vec or vec with one empty string");
    }

    // ========== Parent Verification Tests ==========

    #[test]
    fn test_get_parent_split_level_root() {
        let searcher = create_test_searcher();

        // Bit 0 only = root token
        let splits_matched = 1;
        assert_eq!(searcher.get_parent_split_level(splits_matched), None,
                   "Root token (bit 0 only) should return None");
    }

    #[test]
    fn test_get_parent_split_level_level0() {
        let searcher = create_test_searcher();

        // Bit 1 set (value 2) = started from level 0 split
        let splits_matched = 2;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(0),
                   "Token with only bit 1 set was split at level 0");

        // Bits 1,2,3,4 all set = started from level 0, survived all others
        let splits_matched = 2 | 4 | 8 | 16; // 30
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(0),
                   "Token starting with bit 1 was split at level 0");
    }

    #[test]
    fn test_get_parent_split_level_level1() {
        let searcher = create_test_searcher();

        // Bit 2 set (value 4) = started from level 1 split
        let splits_matched = 4;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(1),
                   "Token with only bit 2 set was split at level 1");

        // Bits 2,3,4 set = started from level 1
        let splits_matched = 4 | 8 | 16; // 28
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(1),
                   "Token with bits 2,3,4 set was split at level 1");
    }

    #[test]
    fn test_get_parent_split_level_level2() {
        let searcher = create_test_searcher();

        // Bit 3 set (value 8) = started from level 2 split
        let splits_matched = 8;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(2),
                   "Token with only bit 3 set was split at level 2");

        // Bits 3,4 set = started from level 2
        let splits_matched = 8 | 16; // 24
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(2),
                   "Token with bits 3,4 set (value 24) was split at level 2");
    }

    #[test]
    fn test_get_parent_split_level_level3() {
        let searcher = create_test_searcher();

        // Bit 4 set (value 16) = started from level 3 split
        let splits_matched = 16;
        assert_eq!(searcher.get_parent_split_level(splits_matched), Some(3),
                   "Token with only bit 4 set was split at level 3");
    }

    #[test]
    fn test_get_min_phrase_split_level_whitespace() {
        let searcher = create_test_searcher();

        // Level 0: whitespace
        assert_eq!(searcher.get_min_phrase_split_level("hello world"), Some(0),
                   "Space is level 0 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("hello\nworld"), Some(0),
                   "Newline is level 0 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_level1() {
        let searcher = create_test_searcher();

        // Level 1: @ / : = \ ? &
        assert_eq!(searcher.get_min_phrase_split_level("user@example.com"), Some(1),
                   "@ is level 1 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("path/to/file"), Some(1),
                   "/ is level 1 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("key:value"), Some(1),
                   "Colon is level 1 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_level2() {
        let searcher = create_test_searcher();

        // Level 2: . $ # ` ~ ^ +
        assert_eq!(searcher.get_min_phrase_split_level("example.com"), Some(2),
                   "Dot is level 2 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("$variable"), Some(2),
                   "Dollar is level 2 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_level3() {
        let searcher = create_test_searcher();

        // Level 3: - _
        assert_eq!(searcher.get_min_phrase_split_level("user-name"), Some(3),
                   "Hyphen is level 3 delimiter");
        assert_eq!(searcher.get_min_phrase_split_level("file_name"), Some(3),
                   "Underscore is level 3 delimiter");
    }

    #[test]
    fn test_get_min_phrase_split_level_mixed() {
        let searcher = create_test_searcher();

        // Should return the MINIMUM (highest priority) level
        assert_eq!(searcher.get_min_phrase_split_level("hello world-name"), Some(0),
                   "Space (level 0) has priority over hyphen (level 3)");
        assert_eq!(searcher.get_min_phrase_split_level("user@example.com"), Some(1),
                   "@ (level 1) has priority over . (level 2)");
        assert_eq!(searcher.get_min_phrase_split_level("file.name-version"), Some(2),
                   ". (level 2) has priority over - (level 3)");
    }

    #[test]
    fn test_get_min_phrase_split_level_no_delimiters() {
        let searcher = create_test_searcher();

        assert_eq!(searcher.get_min_phrase_split_level("simple"), None,
                   "No delimiters should return None");
        assert_eq!(searcher.get_min_phrase_split_level("12345"), None,
                   "Numbers with no delimiters should return None");
    }

}