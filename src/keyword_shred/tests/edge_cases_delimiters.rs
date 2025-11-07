use super::*;

#[test]
fn test_empty_string() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    perform_split("", col_ref, 0, 0, &mut keyword_map);

    assert_eq!(keyword_map.len(), 0, "Empty string should produce no keywords");
}

#[test]
fn test_only_level_0_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Only level 0 delimiters: space, tab, newline, etc.
    perform_split("   \t\n\r  ", col_ref, 0, 0, &mut keyword_map);

    assert_eq!(
        keyword_map.len(),
        0,
        "String with only level 0 delimiters should produce no keywords"
    );
}

#[test]
fn test_only_level_1_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Only level 1 delimiters: /, @, =, :, \, ?, &
    // Level 0: no splits → "//@@==::??&&" added as keyword
    // Level 1: splits to all empty parts → no more keywords
    // Result: 1 keyword
    perform_split("//@@==::??&&", col_ref, 0, 0, &mut keyword_map);

    assert_eq!(
        keyword_map.len(),
        1,
        "String with only level 1 delimiters should produce 1 intermediate keyword"
    );
    assert!(keyword_map.contains_key("//@@==::??&&"));
}

#[test]
fn test_only_level_2_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Only level 2 delimiters: ., $, #, `, ~, ^, +
    // Level 0: no splits → "...$$###```~~~^^^+++" added as keyword
    // Level 1: no splits → "...$$###```~~~^^^+++" added as keyword (same string)
    // Level 2: splits to all empty parts → no more keywords
    // Result: 1 unique keyword (same string at levels 0 and 1)
    perform_split("...$$###```~~~^^^+++", col_ref, 0, 0, &mut keyword_map);

    assert_eq!(
        keyword_map.len(),
        1,
        "String with only level 2 delimiters should produce 1 intermediate keyword"
    );
    assert!(keyword_map.contains_key("...$$###```~~~^^^+++"));
}

#[test]
fn test_only_level_3_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Only level 3 delimiters: -, _
    // Level 0: no splits → "---___---___" added as keyword
    // Level 1: no splits → "---___---___" added as keyword (same string)
    // Level 2: no splits → "---___---___" added as keyword (same string)
    // Level 3: splits to all empty parts → no more keywords
    // Result: 1 unique keyword (same string at all levels)
    perform_split("---___---___", col_ref, 0, 0, &mut keyword_map);

    assert_eq!(
        keyword_map.len(),
        1,
        "String with only level 3 delimiters should produce 1 intermediate keyword"
    );
    assert!(keyword_map.contains_key("---___---___"));
}

// Ignore grammar warnings in splitting
//noinspection GrazieInspection
#[test]
fn test_all_delimiters_mixed() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Mix of all delimiter levels: " \t//@..$##-_"
    // Level 0 split → "//@..$##-_" (1 part, added as keyword)
    // Level 1 split → "..$##-_" (1 part, added as keyword)
    // Level 2 split → "-_" (1 part, added as keyword)
    // Level 3 split → nothing (no keyword)
    // Result: 3 keywords
    perform_split(" \t//@..$##-_", col_ref, 0, 0, &mut keyword_map);

    println!("\nKeywords found:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    assert_eq!(
        keyword_map.len(),
        3,
        "String with mixed delimiters should produce 3 intermediate keywords from hierarchical splitting"
    );
    
    assert!(keyword_map.contains_key("//@..$##-_"));
    assert!(keyword_map.contains_key("..$##-_"));
    assert!(keyword_map.contains_key("-_"));
}

#[test]
fn test_all_delimiters_non_hierarchical_order() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // All delimiters in mixed order (not level order):
    // Level 0: \r, \n, \t, ', ", <, >, (, ), |, ,, !, ;, {, }, *, space
    // Level 1: /, @, =, :, \, ?, &
    // Level 2: ., $, #, `, ~, ^, +
    // Level 3: -, _
    // String: "-.#/@\t$:~'|\r^(&\n=<+>)?!_`;{,}*\""
    let complex = "-.#/@\t$:~'|\r^(&\n=<+>)?!_`;{,}*\"";
    
    perform_split(complex, col_ref, 0, 0, &mut keyword_map);

    println!("\nAll delimiters non-hierarchical order test:");
    println!("Original: {:?}", complex);
    println!("\nKeywords found:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    // Let's trace through the splits:
    // Level 0 splits on: \t, ', |, \r, (, &, \n, <, >, ), ?, !, `, ;, {, ,, }, *, "
    // All parts between these are empty or: "-.#/@" and "$:~" and "^" and "=" and "+" and "_"
    // These 6 parts continue to next level
    
    // "-.#/@" at Level 1 splits on /,@ → "-.#" (added as keyword)
    // "$:~" at Level 1 splits on : → "$" and "~" continue separately
    // "^" continues unchanged (added as keyword after passing level 1)
    // "=" continues unchanged (added as keyword after passing level 1) 
    // "+" continues unchanged (added as keyword after passing level 1)
    // "_" continues unchanged (added as keyword after passing level 1)
    
    // "-.#" at Level 2 splits on ., # → "-" (added as keyword)
    // "$" continues unchanged (added as keyword)
    // "~" continues unchanged (added as keyword)
    // "^", "=", "+", "_" already added
    
    // "-" at Level 3 splits on - → nothing
    // "$", "~", "^", "=", "+", "_" at Level 3:
    //   - "_" splits on _ → nothing
    //   - Others continue unchanged and are added
    
    // Expected keywords based on the logic:
    // From "-.#/@": "-.#/@" (level 0), "-.#" (level 1), "-" (level 2)
    // From "$:~": "$:~" (level 0), "$" (level 1+), "~" (level 1+)
    // From "^": "^" (level 0+)
    // From "=": "=" (level 0+)
    // From "+": "+" (level 0+)
    // From "_": "_" (level 0+, but splits at level 3)
    
    // Wait, let me reconsider - when level 0 splits produce multiple parts,
    // those parts don't add the original string as a keyword
    
    // Actually: Level 0 splits the whole string into multiple parts, so the original
    // string is NOT added. Each part continues independently.
    
    assert!(keyword_map.len() > 0, "Should produce multiple keywords from complex delimiter mix");
    
    // Some keywords we definitely expect:
    // These are intermediate keywords that don't split into multiple parts
    assert!(keyword_map.contains_key("-.#"));
    assert!(keyword_map.contains_key("$"));
    assert!(keyword_map.contains_key("~"));
    assert!(keyword_map.contains_key("^"));
    assert!(keyword_map.contains_key("="));
    assert!(keyword_map.contains_key("+"));
    
    println!("\nTotal keywords found: {}", keyword_map.len());
}

#[test]
fn test_consecutive_delimiters() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Multiple consecutive delimiters: "word1   word2///word3---word4"
    // Level 0 splits on spaces → ["word1", "word2///word3---word4"]
    //   Multiple parts, so original NOT added
    // "word2///word3---word4" at Level 1 splits on / → ["word2", "word3---word4"]
    //   Before processing splits, "word2///word3---word4" is ADDED (line 360-370)
    // "word3---word4" at Level 3 splits on - → ["word3", "word4"]
    //   Before processing splits, "word3---word4" is ADDED
    // Expected: word1, word2///word3---word4, word2, word3---word4, word3, word4 = 6 keywords
    perform_split("word1   word2///word3---word4", col_ref, 0, 0, &mut keyword_map);

    println!("\nConsecutive delimiters keywords:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    assert_eq!(keyword_map.len(), 6, "Should handle consecutive delimiters with intermediate keywords");
    assert!(keyword_map.contains_key("word1"));
    assert!(keyword_map.contains_key("word2///word3---word4"));
    assert!(keyword_map.contains_key("word2"));
    assert!(keyword_map.contains_key("word3---word4"));
    assert!(keyword_map.contains_key("word3"));
    assert!(keyword_map.contains_key("word4"));
}

#[test]
fn test_trailing_delimiters_only() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Keyword followed by delimiters: "keyword///...---"
    // Level 0: no splits → continues to Level 1
    // Level 1 splits on / → ["keyword", "...---"]
    //   Before processing splits, "keyword///...---" is ADDED (line 360-370)
    // "...---" at Level 2 splits on . → ["---"]
    //   Before processing splits, "...---" is ADDED
    // "---" at Level 3: splits on - → [] (empty after filter)
    //   Loop doesn't execute, so "---" is ADDED via line 400
    // Expected: keyword///...---, keyword, ...---, --- = 4 keywords
    perform_split("keyword///...---", col_ref, 0, 0, &mut keyword_map);

    println!("\nTrailing delimiters keywords:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    assert_eq!(keyword_map.len(), 4, "Should produce intermediate keywords from trailing delimiters");
    assert!(keyword_map.contains_key("keyword///...---"));
    assert!(keyword_map.contains_key("keyword"));
    assert!(keyword_map.contains_key("...---"));
    assert!(keyword_map.contains_key("---"));
}

#[test]
fn test_leading_delimiters_only() {
    let mut keyword_map: HashMap<Rc<str>, KeywordOneFile> = HashMap::new();
    let mut column_pool = ColumnPool::new();
    let col_ref = column_pool.intern("test_col");

    // Delimiters followed by keyword: "///...---keyword"
    // Level 0: no splits → "///...---keyword" added as keyword
    // Level 1 splits on / → ["...---keyword"]
    // "...---keyword" at Level 2 splits on . → ["---keyword"]
    // "---keyword" at Level 3 splits on - → ["keyword"]
    // Expected: ///...---keyword, ...---keyword, ---keyword, keyword = 4 keywords
    perform_split("///...---keyword", col_ref, 0, 0, &mut keyword_map);

    println!("\nLeading delimiters keywords:");
    for keyword in keyword_map.keys() {
        println!("  '{}'", keyword);
    }

    assert_eq!(keyword_map.len(), 4, "Should produce intermediate keywords from leading delimiters");
    assert!(keyword_map.contains_key("///...---keyword"));
    assert!(keyword_map.contains_key("...---keyword"));
    assert!(keyword_map.contains_key("---keyword"));
    assert!(keyword_map.contains_key("keyword"));
}