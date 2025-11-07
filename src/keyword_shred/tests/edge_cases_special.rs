use super::*;

#[test]
fn test_mixed_whitespace_types() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Mix of space, tab, newline, carriage return
    perform_split("word1 word2\tword3\nword4\rword5", col_ref, 0, 0, &mut keyword_map);

    // All whitespace types are level 0 delimiters
    assert_eq!(keyword_map.len(), 5, "Should split on all whitespace types");
    assert!(keyword_map.contains_key("word1"));
    assert!(keyword_map.contains_key("word2"));
    assert!(keyword_map.contains_key("word3"));
    assert!(keyword_map.contains_key("word4"));
    assert!(keyword_map.contains_key("word5"));
}

#[test]
fn test_single_character_keyword() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("a", col_ref, 0, 0, &mut keyword_map);

    assert_eq!(keyword_map.len(), 1);
    assert!(keyword_map.contains_key("a"));
}

#[test]
fn test_numeric_keywords() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("123 456.789 0", col_ref, 0, 0, &mut keyword_map);

    assert!(keyword_map.contains_key("123"));
    assert!(keyword_map.contains_key("456"));
    assert!(keyword_map.contains_key("789"));
    assert!(keyword_map.contains_key("0"));
}

#[test]
fn test_special_characters_preserved_in_keywords() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Characters that are NOT delimiters should be preserved
    perform_split("hello%world", col_ref, 0, 0, &mut keyword_map);

    // % is not a delimiter, so this should be one keyword
    assert!(keyword_map.contains_key("hello%world"));
}

#[test]
fn test_alternating_keywords_and_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("a b c d e f", col_ref, 0, 0, &mut keyword_map);

    assert_eq!(keyword_map.len(), 6);
    for c in ['a', 'b', 'c', 'd', 'e', 'f'] {
        let key = c.to_string();
        assert!(keyword_map.contains_key(key.as_str()));
    }
}

#[test]
fn test_unicode_emoji_in_keywords() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("hello😀world🎉test", col_ref, 0, 0, &mut keyword_map);

    // Emojis are not delimiters, should be part of the keyword
    assert!(keyword_map.contains_key("hello😀world🎉test"));
}

#[test]
fn test_mixed_case_sensitivity() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("Hello WORLD TeSt", col_ref, 0, 0, &mut keyword_map);

    // Case should be preserved
    assert!(keyword_map.contains_key("Hello"));
    assert!(keyword_map.contains_key("WORLD"));
    assert!(keyword_map.contains_key("TeSt"));
    assert!(!keyword_map.contains_key("hello"));
}

#[test]
fn test_null_byte_handling() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // String with null byte (if your system supports it)
    let input = "before\0after";
    perform_split(input, col_ref, 0, 0, &mut keyword_map);

    // The null byte is not a delimiter, so should be preserved
    // (though in practice this might be unusual)
    assert!(keyword_map.len() >= 1);
}

#[test]
fn test_zero_values() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // All zero values
    perform_split("keyword", col_ref, 0, 0, &mut keyword_map);

    let kw = keyword_map.get("keyword").unwrap();
    assert_eq!(kw.row_group_to_rows[0][0][0].row, 0);
    assert!(kw.row_groups[0].contains(&0));
}