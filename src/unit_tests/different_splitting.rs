#[cfg(test)]
mod tests {
    use crate::keyword_shred::{perform_split, SplitLookup};
    use hashbrown::HashMap;

    // ========== Custom Split Characters Tests ==========

    #[test]
    fn test_custom_split_chars_only_at_and_dot() {
        let mut keyword_map = HashMap::new();

        // Custom: only split on @ (level 0) and . (level 1)
        let split_chars = vec![vec!['@'], vec!['.']];
        let lookup = SplitLookup::new(&split_chars);

        // Test 1: Email should split on @ and .
        perform_split("user@example.com", 1, 0, 0, &mut keyword_map, &lookup, false);

        assert!(keyword_map.contains_key("user"), "Should split on @");
        assert!(keyword_map.contains_key("example"), "Should split on .");
        assert!(keyword_map.contains_key("com"), "Should split on .");

        // Test 2: Hyphen should NOT split (not in custom config)
        keyword_map.clear();
        perform_split("john-smith", 1, 0, 0, &mut keyword_map, &lookup, false);

        assert_eq!(keyword_map.len(), 1, "Hyphen should not split");
        assert!(keyword_map.contains_key("john-smith"), "Should keep john-smith together");
        assert!(!keyword_map.contains_key("john"), "Should not split on hyphen");
        assert!(!keyword_map.contains_key("smith"), "Should not split on hyphen");

        // Test 3: Slash should NOT split (not in custom config)
        keyword_map.clear();
        perform_split("/usr/local/bin", 1, 0, 0, &mut keyword_map, &lookup, false);

        assert_eq!(keyword_map.len(), 1, "Slash should not split");
        assert!(keyword_map.contains_key("/usr/local/bin"), "Should keep path together");
    }

    #[test]
    fn test_custom_split_chars_only_hyphen() {
        let mut keyword_map = HashMap::new();

        // Only split on hyphen
        let split_chars = vec![vec!['-']];
        let lookup = SplitLookup::new(&split_chars);

        perform_split("john-smith-jr", 1, 0, 0, &mut keyword_map, &lookup, false);

        assert!(keyword_map.contains_key("john"), "Should split on hyphen");
        assert!(keyword_map.contains_key("smith"), "Should split on hyphen");
        assert!(keyword_map.contains_key("jr"), "Should split on hyphen");

        // @ and . should NOT split
        keyword_map.clear();
        perform_split("user@example.com", 1, 0, 0, &mut keyword_map, &lookup, false);

        assert_eq!(keyword_map.len(), 1, "Should not split on @ or .");
        assert!(keyword_map.contains_key("user@example.com"));
    }

    #[test]
    fn test_custom_split_chars_three_levels() {
        let mut keyword_map = HashMap::new();

        // Three levels: space, @, dot
        let split_chars = vec![vec![' '], vec!['@'], vec!['.']];
        let lookup = SplitLookup::new(&split_chars);

        perform_split("user name@example.com", 1, 0, 0, &mut keyword_map, &lookup, false);

        // Should split on space first (level 0)
        assert!(keyword_map.contains_key("user"), "Split on space");
        assert!(keyword_map.contains_key("name@example.com"), "Parent after space split");

        // Then @ (level 1)
        assert!(keyword_map.contains_key("name"), "Split on @");
        assert!(keyword_map.contains_key("example.com"), "Parent after @ split");

        // Then . (level 2)
        assert!(keyword_map.contains_key("example"), "Split on .");
        assert!(keyword_map.contains_key("com"), "Split on .");
    }

    // ========== Full Keyword Storage Tests ==========

    #[test]
    fn test_store_full_keyword_true() {
        let mut keyword_map = HashMap::new();

        let split_chars = vec![vec!['@'], vec!['.'], vec!['-']];
        let lookup = SplitLookup::new(&split_chars);

        perform_split("user@example.com", 1, 0, 0, &mut keyword_map, &lookup, true);

        // Should have the full keyword
        assert!(keyword_map.contains_key("user@example.com"),
                "Full keyword should be stored when store_full_keyword=true");

        // Should also have the splits
        assert!(keyword_map.contains_key("user"), "Should still split");
        assert!(keyword_map.contains_key("example"), "Should still split");
        assert!(keyword_map.contains_key("com"), "Should still split");

        // Verify full keyword has bit 0 set (splits_matched = 1)
        let full_entry = keyword_map.get("user@example.com").unwrap();
        assert_eq!(full_entry.splits_matched & 1, 1,
                   "Full keyword should have bit 0 set");
    }

    #[test]
    fn test_store_full_keyword_false() {
        let mut keyword_map = HashMap::new();

        let split_chars = vec![vec!['@'], vec!['.']];
        let lookup = SplitLookup::new(&split_chars);

        perform_split("user@example.com", 1, 0, 0, &mut keyword_map, &lookup, false);

        // Should NOT have the full keyword
        assert!(!keyword_map.contains_key("user@example.com"),
                "Full keyword should not be stored when store_full_keyword=false");

        // Should only have the splits
        assert!(keyword_map.contains_key("user"));
        assert!(keyword_map.contains_key("example"));
        assert!(keyword_map.contains_key("com"));
    }

    #[test]
    fn test_full_keyword_parent_relationships() {
        let mut keyword_map = HashMap::new();

        let split_chars = vec![vec!['-']];
        let lookup = SplitLookup::new(&split_chars);

        perform_split("john-smith", 1, 0, 42, &mut keyword_map, &lookup, true);

        // Get the entries
        let full_entry = keyword_map.get("john-smith").unwrap();
        let john_entry = keyword_map.get("john").unwrap();
        let smith_entry = keyword_map.get("smith").unwrap();

        // Navigate to rows and check parent references
        for column_rows in &john_entry.row_group_to_rows {
            for rows in column_rows {
                for row in rows {
                    if row.row == 42 {
                        assert!(row.parent_keyword.is_some(),
                                "Child split should have parent");
                        assert_eq!(row.parent_keyword.as_ref().unwrap().as_ref(),
                                   "john-smith",
                                   "Child should reference full keyword as parent");
                    }
                }
            }
        }

        for column_rows in &smith_entry.row_group_to_rows {
            for rows in column_rows {
                for row in rows {
                    if row.row == 42 {
                        assert!(row.parent_keyword.is_some(),
                                "Child split should have parent");
                        assert_eq!(row.parent_keyword.as_ref().unwrap().as_ref(),
                                   "john-smith",
                                   "Child should reference full keyword as parent");
                    }
                }
            }
        }

        // Full keyword should have no parent
        for column_rows in &full_entry.row_group_to_rows {
            for rows in column_rows {
                for row in rows {
                    if row.row == 42 {
                        assert!(row.parent_keyword.is_none(),
                                "Full keyword should have no parent");
                    }
                }
            }
        }
    }

    #[test]
    fn test_full_keyword_empty_string() {
        let mut keyword_map = HashMap::new();

        let split_chars = vec![vec!['-']];
        let lookup = SplitLookup::new(&split_chars);

        perform_split("", 1, 0, 0, &mut keyword_map, &lookup, true);

        assert!(keyword_map.contains_key(""),
                "Empty string should be stored when store_full_keyword=true");
        assert_eq!(keyword_map.len(), 1);
    }

    #[test]
    fn test_full_keyword_only_delimiters() {
        let mut keyword_map = HashMap::new();

        let split_chars = vec![vec!['@']];
        let lookup = SplitLookup::new(&split_chars);

        perform_split("@@@", 1, 0, 0, &mut keyword_map, &lookup, true);

        // Should store full keyword
        assert!(keyword_map.contains_key("@@@"),
                "Should store full keyword even if all delimiters");

        // Should not have any child splits (nothing left after splitting)
        assert_eq!(keyword_map.len(), 1,
                   "Should only have full keyword, no valid splits");
    }

    // ========== Combined Feature Test ==========

    #[test]
    fn test_custom_splits_with_full_keyword() {
        let mut keyword_map = HashMap::new();

        // Custom: only split on @
        let split_chars = vec![vec!['@']];
        let lookup = SplitLookup::new(&split_chars);

        // Store full keyword, but only split on @
        perform_split("user-name@example.com", 1, 0, 0, &mut keyword_map, &lookup, true);

        // Should have full keyword
        assert!(keyword_map.contains_key("user-name@example.com"));

        // Should split on @
        assert!(keyword_map.contains_key("user-name"),
                "Should split on @ but keep hyphen together");
        assert!(keyword_map.contains_key("example.com"),
                "Should split on @ but keep dot together");

        // Should NOT split on . or -
        assert!(!keyword_map.contains_key("user"));
        assert!(!keyword_map.contains_key("name"));
        assert!(!keyword_map.contains_key("example"));
        assert!(!keyword_map.contains_key("com"));

        // Verify parent relationships
        let user_name_entry = keyword_map.get("user-name").unwrap();
        for column_rows in &user_name_entry.row_group_to_rows {
            for rows in column_rows {
                for row in rows {
                    assert!(row.parent_keyword.is_some());
                    assert_eq!(row.parent_keyword.as_ref().unwrap().as_ref(),
                               "user-name@example.com",
                               "Split should reference full keyword as parent");
                }
            }
        }
    }
}