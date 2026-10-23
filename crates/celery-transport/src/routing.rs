//! Routing virtuale direct/topic/fanout, come
//! `kombu/transport/virtual/exchange.py`.
//!
//! Le stringhe di pattern prodotte da [`key_to_pattern`] sono **byte-identiche**
//! a quelle di kombu: finiscono nel set `_kombu.binding.<exchange>` di Redis e
//! devono restare leggibili da kombu. Il matching usa la regex crate dopo aver
//! tradotto gli escape in sintassi Rust.

use compact_str::CompactString;
use regex::Regex;

use crate::transport::{Error, ExchangeType, Result};

/// Caratteri che kombu lascia non escapati in `escape_regex`.
const SAFE: &str = ".#*";

/// Replica `kombu.utils.text.escape_regex`.
pub fn escape_regex(input: &str, white: &str) -> String {
    let mut out = String::with_capacity(input.len() + 8);
    for ch in input.chars() {
        if ch.is_alphanumeric() || white.contains(ch) {
            out.push(ch);
        } else if ch == '\0' {
            out.push_str("\\000");
        } else {
            out.push('\\');
            out.push(ch);
        }
    }
    out
}

/// Replica `TopicExchange.key_to_pattern`: `<routing_key>` -> regex Python.
pub fn key_to_pattern(routing_key: &str) -> String {
    let escaped = escape_regex(routing_key, SAFE);
    let mut pattern = String::with_capacity(escaped.len() + 8);
    pattern.push('^');
    for (index, word) in escaped.split('.').enumerate() {
        if index > 0 {
            pattern.push_str("\\.");
        }
        match word {
            "*" => pattern.push_str("[^\\.]+"),
            "#" => pattern.push_str(".*?"),
            literal => pattern.push_str(literal),
        }
    }
    pattern.push('$');
    pattern
}

/// Converte un pattern in sintassi Python (`re`) in sintassi `regex` di Rust.
///
/// `escape_regex` produce `\X` anche per caratteri che in Rust non sono
/// metacaratteri (`\/`, `\_`, `\€`): lì il backslash va tolto.
fn translate_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if ch != '\\' {
            out.push(ch);
            index += 1;
            continue;
        }
        let Some(&next) = chars.get(index + 1) else {
            out.push('\\');
            break;
        };
        // escape_regex usa \000 per il NUL.
        if next == '0' && chars.get(index + 2) == Some(&'0') && chars.get(index + 3) == Some(&'0') {
            out.push_str("\\x00");
            index += 4;
            continue;
        }
        if is_regex_meta(next) {
            out.push('\\');
        }
        out.push(next);
        index += 2;
    }
    out
}

fn is_regex_meta(ch: char) -> bool {
    matches!(
        ch,
        '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
    )
}

/// Matcher topic: regex compilata una volta per binding.
#[derive(Debug, Clone)]
pub struct TopicMatcher {
    pattern: CompactString,
    regex: Regex,
}

impl TopicMatcher {
    /// Compila un pattern nel formato prodotto da [`key_to_pattern`].
    pub fn new(pattern: &str) -> Result<Self> {
        let translated = translate_pattern(pattern);
        let regex = Regex::new(&translated)
            .map_err(|err| Error::InvalidPattern(format!("{pattern}: {err}")))?;
        Ok(Self {
            pattern: CompactString::from(pattern),
            regex,
        })
    }

    /// Pattern originale (quello di kombu, per il broker).
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// `re.match` di Python: match ancorato all'inizio (il pattern è `^...$`).
    pub fn is_match(&self, routing_key: &str) -> bool {
        if self.regex.is_match(routing_key) {
            return true;
        }
        // In Python `$` combacia anche prima di un singolo newline finale.
        match routing_key.strip_suffix('\n') {
            Some(stripped) => self.regex.is_match(stripped),
            None => false,
        }
    }
}

/// Binding preparato: routing key, pattern (solo topic) e matcher compilato.
#[derive(Debug, Clone)]
pub struct PreparedBinding {
    pub queue: CompactString,
    pub routing_key: CompactString,
    pub pattern: Option<CompactString>,
    matcher: Option<TopicMatcher>,
}

impl PreparedBinding {
    /// Prepara un binding per `exchange_type` (come `ExchangeType.prepare_bind`).
    pub fn new(exchange_type: ExchangeType, queue: &str, routing_key: &str) -> Result<Self> {
        let (pattern, matcher) = match exchange_type {
            ExchangeType::Topic => {
                let pattern = key_to_pattern(routing_key);
                let matcher = TopicMatcher::new(&pattern)?;
                (Some(CompactString::from(pattern)), Some(matcher))
            }
            _ => (None, None),
        };
        Ok(Self {
            queue: CompactString::from(queue),
            routing_key: CompactString::from(routing_key),
            pattern,
            matcher,
        })
    }

    /// Vera se il binding intercetta `routing_key` per questo tipo di exchange.
    pub fn matches(&self, exchange_type: ExchangeType, routing_key: &str) -> bool {
        match exchange_type {
            ExchangeType::Direct => self.routing_key == routing_key,
            ExchangeType::Topic => self
                .matcher
                .as_ref()
                .is_some_and(|matcher| matcher.is_match(routing_key)),
            ExchangeType::Fanout => true,
            // I trasporti virtuali non hanno exchange headers: nessun match.
            ExchangeType::Headers => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tabella generata con `kombu.transport.virtual.exchange.TopicExchange.lookup`
    /// (vedi `tests/fixtures/topic_routing.tsv`).
    const TOPIC_CASES: &str = include_str!("../tests/fixtures/topic_routing.tsv");

    #[test]
    fn key_to_pattern_matches_kombu() {
        let mut checked = 0;
        for line in TOPIC_CASES.lines() {
            let mut fields = line.split('\t');
            let binding = fields.next().expect("binding key");
            let published = fields.next().expect("published key");
            let expected_pattern = fields.next().expect("pattern");
            let expected_hit = fields.next().expect("esito") == "1";
            let pattern = key_to_pattern(binding);
            assert_eq!(pattern, expected_pattern, "pattern per {binding:?}");
            let matcher = TopicMatcher::new(&pattern).expect("pattern compilabile");
            assert_eq!(
                matcher.is_match(published),
                expected_hit,
                "match {binding:?} vs {published:?}"
            );
            checked += 1;
        }
        assert!(checked >= 900, "casi verificati: {checked}");
    }

    #[test]
    fn key_to_pattern_examples() {
        assert_eq!(key_to_pattern("#"), "^.*?$");
        assert_eq!(key_to_pattern("*"), "^[^\\.]+$");
        assert_eq!(key_to_pattern("foo.#"), "^foo\\..*?$");
        assert_eq!(key_to_pattern("foo.*"), "^foo\\.[^\\.]+$");
        assert_eq!(key_to_pattern("foo.bar"), "^foo\\.bar$");
        assert_eq!(key_to_pattern("foo*"), "^foo*$");
        assert_eq!(key_to_pattern("città/€"), "^città\\/\\€$");
        assert_eq!(key_to_pattern(""), "^$");
        assert_eq!(key_to_pattern("."), r"^\.$");
    }

    #[test]
    fn topic_wildcards() {
        let hash = TopicMatcher::new(&key_to_pattern("#")).expect("pattern");
        assert!(hash.is_match(""));
        assert!(hash.is_match("a.b.c"));

        let star = TopicMatcher::new(&key_to_pattern("*")).expect("pattern");
        assert!(star.is_match("foo"));
        assert!(!star.is_match("foo.bar"));
        assert!(!star.is_match(""));

        // `#` non copre lo zero-words in kombu: "foo.#" non matcha "foo".
        let suffix = TopicMatcher::new(&key_to_pattern("foo.#")).expect("pattern");
        assert!(suffix.is_match("foo."));
        assert!(suffix.is_match("foo.bar.baz"));
        assert!(!suffix.is_match("foo"));
    }

    #[test]
    fn python_dollar_matches_before_trailing_newline() {
        let matcher = TopicMatcher::new(&key_to_pattern("foo")).expect("pattern");
        assert!(matcher.is_match("foo"));
        assert!(matcher.is_match("foo\n"));
        assert!(!matcher.is_match("foo\n\n"));
    }

    #[test]
    fn direct_and_fanout_bindings() {
        let direct = PreparedBinding::new(ExchangeType::Direct, "q", "rk").expect("binding");
        assert!(direct.matches(ExchangeType::Direct, "rk"));
        assert!(!direct.matches(ExchangeType::Direct, "rk.other"));
        assert!(direct.pattern.is_none());

        let fanout = PreparedBinding::new(ExchangeType::Fanout, "q", "ignored").expect("binding");
        assert!(fanout.matches(ExchangeType::Fanout, "anything"));

        let headers = PreparedBinding::new(ExchangeType::Headers, "q", "rk").expect("binding");
        assert!(!headers.matches(ExchangeType::Headers, "rk"));
    }

    #[test]
    fn invalid_pattern_is_reported() {
        let err = TopicMatcher::new("[").expect_err("pattern non valido");
        assert!(matches!(err, Error::InvalidPattern(_)));
    }
}
