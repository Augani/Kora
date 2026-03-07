//! Glob pattern matching for Pub/Sub channel patterns.
//!
//! Supports `*` (any sequence of characters) and `?` (any single character).

/// Match a glob pattern against a channel name.
///
/// Supports `*` (match any sequence) and `?` (match any single byte).
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
    }

    #[test]
    fn test_star_wildcard() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"chat.*", b"chat.general"));
        assert!(glob_match(b"chat.*", b"chat."));
        assert!(!glob_match(b"chat.*", b"news.general"));
    }

    #[test]
    fn test_question_wildcard() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
        assert!(!glob_match(b"h?llo", b"heello"));
    }

    #[test]
    fn test_combined_wildcards() {
        assert!(glob_match(b"h*o", b"hello"));
        assert!(glob_match(b"h*o", b"ho"));
        assert!(glob_match(b"*?*", b"x"));
        assert!(!glob_match(b"*?*", b""));
    }

    #[test]
    fn test_empty_pattern() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"x"));
    }

    #[test]
    fn test_multiple_stars() {
        assert!(glob_match(b"*.*.*", b"a.b.c"));
        assert!(glob_match(b"*.*.*", b"..."));
        assert!(!glob_match(b"*.*.*", b"a.b"));
    }

    #[test]
    fn test_redis_pubsub_patterns() {
        assert!(glob_match(b"news.*", b"news.art"));
        assert!(glob_match(b"news.*", b"news.sports"));
        assert!(!glob_match(b"news.*", b"chat.general"));
        assert!(glob_match(b"__keyevent@*__:*", b"__keyevent@0__:set"));
    }
}
