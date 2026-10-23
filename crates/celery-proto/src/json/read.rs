//! Parser JSON pull, senza albero intermedio, con la semantica di `json.loads`.
//!
//! L'input è una sequenza di byte UTF-8/WTF-8 (le `str` Python possono contenere
//! surrogati isolati). Gli offset negli errori sono in code point, come CPython.

use std::borrow::Cow;
use std::fmt;

/// Stringa JSON decodificata.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonStr<'a> {
    /// Nessun escape e UTF-8 valido: prestito dell'input.
    Borrowed(&'a str),
    /// Contiene escape ma nessun surrogato isolato.
    Owned(String),
    /// Contiene surrogati isolati.
    Code(Vec<u32>),
}

impl JsonStr<'_> {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Borrowed(s) => Some(s),
            Self::Owned(s) => Some(s),
            Self::Code(_) => None,
        }
    }

    pub fn code_points(&self) -> Cow<'_, [u32]> {
        match self {
            Self::Borrowed(s) => Cow::Owned(s.chars().map(|c| c as u32).collect()),
            Self::Owned(s) => Cow::Owned(s.chars().map(|c| c as u32).collect()),
            Self::Code(v) => Cow::Borrowed(v),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event<'a> {
    Null,
    Bool(bool),
    /// Intero come testo decimale (arbitrario, il chiamante decide il tipo).
    Int(&'a str),
    Float(f64),
    Str(JsonStr<'a>),
    Key(JsonStr<'a>),
    StartArray,
    EndArray,
    StartObject,
    EndObject,
}

/// Errore con posizione in code point, come `JSONDecodeError.pos`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: &'static str,
    pub pos: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at char {}", self.message, self.pos)
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone, Copy)]
enum ArrState {
    /// Subito dopo `[`: ammette `]` (array vuoto).
    First,
    /// Dopo una virgola: `]` è un errore di trailing comma.
    NeedValue { comma: usize },
    /// Dopo un valore: `,` oppure `]`.
    Delim,
}

#[derive(Debug, Clone, Copy)]
enum ObjState {
    /// Subito dopo `{`: ammette `}` (oggetto vuoto).
    KeyFirst,
    KeyAfterComma {
        comma: usize,
    },
    Colon,
    Value,
    Delim,
}

#[derive(Debug, Clone, Copy)]
enum Ctx {
    Arr(ArrState),
    Obj(ObjState),
}

/// Parser a eventi. `next()` restituisce `None` a fine documento.
pub struct Reader<'a> {
    src: &'a [u8],
    i: usize,
    stack: Vec<Ctx>,
    root_done: bool,
}

impl<'a> Reader<'a> {
    pub fn new(src: &'a [u8]) -> Self {
        Self {
            src,
            i: 0,
            stack: Vec::with_capacity(8),
            root_done: false,
        }
    }

    pub fn next_event(&mut self) -> Result<Option<Event<'a>>, ParseError> {
        loop {
            self.skip_ws();
            match self.stack.last().copied() {
                None => {
                    if self.root_done {
                        if self.i < self.src.len() {
                            return Err(self.error("Extra data", self.i));
                        }
                        return Ok(None);
                    }
                    let ev = self.parse_value()?;
                    if self.stack.is_empty() {
                        self.root_done = true;
                    }
                    return Ok(Some(ev));
                }
                Some(Ctx::Arr(state)) => match state {
                    ArrState::First => {
                        if self.peek() == Some(b']') {
                            self.i += 1;
                            self.stack.pop();
                            self.maybe_root_done();
                            return Ok(Some(Event::EndArray));
                        }
                        self.set_top(Ctx::Arr(ArrState::Delim));
                        return self.parse_value().map(Some);
                    }
                    ArrState::NeedValue { comma } => {
                        if self.peek() == Some(b']') {
                            return Err(
                                self.error("Illegal trailing comma before end of array", comma)
                            );
                        }
                        self.set_top(Ctx::Arr(ArrState::Delim));
                        return self.parse_value().map(Some);
                    }
                    ArrState::Delim => match self.peek() {
                        Some(b']') => {
                            self.i += 1;
                            self.stack.pop();
                            self.maybe_root_done();
                            return Ok(Some(Event::EndArray));
                        }
                        Some(b',') => {
                            let comma = self.i;
                            self.i += 1;
                            self.set_top(Ctx::Arr(ArrState::NeedValue { comma }));
                        }
                        _ => return Err(self.error("Expecting ',' delimiter", self.i)),
                    },
                },
                Some(Ctx::Obj(state)) => match state {
                    ObjState::KeyFirst => {
                        if self.peek() == Some(b'}') {
                            self.i += 1;
                            self.stack.pop();
                            self.maybe_root_done();
                            return Ok(Some(Event::EndObject));
                        }
                        if self.peek() != Some(b'"') {
                            return Err(self.error(
                                "Expecting property name enclosed in double quotes",
                                self.i,
                            ));
                        }
                        let key = self.parse_string()?;
                        self.set_top(Ctx::Obj(ObjState::Colon));
                        return Ok(Some(Event::Key(key)));
                    }
                    ObjState::KeyAfterComma { comma } => {
                        if self.peek() == Some(b'}') {
                            return Err(
                                self.error("Illegal trailing comma before end of object", comma)
                            );
                        }
                        if self.peek() != Some(b'"') {
                            return Err(self.error(
                                "Expecting property name enclosed in double quotes",
                                self.i,
                            ));
                        }
                        let key = self.parse_string()?;
                        self.set_top(Ctx::Obj(ObjState::Colon));
                        return Ok(Some(Event::Key(key)));
                    }
                    ObjState::Colon => {
                        if self.peek() == Some(b':') {
                            self.i += 1;
                            self.set_top(Ctx::Obj(ObjState::Value));
                        } else {
                            return Err(self.error("Expecting ':' delimiter", self.i));
                        }
                    }
                    ObjState::Value => {
                        self.set_top(Ctx::Obj(ObjState::Delim));
                        return self.parse_value().map(Some);
                    }
                    ObjState::Delim => match self.peek() {
                        Some(b'}') => {
                            self.i += 1;
                            self.stack.pop();
                            self.maybe_root_done();
                            return Ok(Some(Event::EndObject));
                        }
                        Some(b',') => {
                            let comma = self.i;
                            self.i += 1;
                            self.set_top(Ctx::Obj(ObjState::KeyAfterComma { comma }));
                        }
                        _ => return Err(self.error("Expecting ',' delimiter", self.i)),
                    },
                },
            }
        }
    }

    fn maybe_root_done(&mut self) {
        if self.stack.is_empty() {
            self.root_done = true;
        }
    }

    fn set_top(&mut self, ctx: Ctx) {
        if let Some(top) = self.stack.last_mut() {
            *top = ctx;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.i).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.peek() {
            match b {
                b' ' | b'\t' | b'\n' | b'\r' => self.i += 1,
                _ => break,
            }
        }
    }

    fn error(&self, message: &'static str, byte_pos: usize) -> ParseError {
        ParseError {
            message,
            pos: count_code_points(self.src, byte_pos),
        }
    }

    fn parse_value(&mut self) -> Result<Event<'a>, ParseError> {
        self.skip_ws();
        let start = self.i;
        let Some(b) = self.peek() else {
            return Err(self.error("Expecting value", start));
        };
        match b {
            b'[' => {
                self.i += 1;
                self.stack.push(Ctx::Arr(ArrState::First));
                Ok(Event::StartArray)
            }
            b'{' => {
                self.i += 1;
                self.stack.push(Ctx::Obj(ObjState::KeyFirst));
                Ok(Event::StartObject)
            }
            b'"' => self.parse_string().map(Event::Str),
            b'n' => {
                self.expect_literal(b"null", start)?;
                Ok(Event::Null)
            }
            b't' => {
                self.expect_literal(b"true", start)?;
                Ok(Event::Bool(true))
            }
            b'f' => {
                self.expect_literal(b"false", start)?;
                Ok(Event::Bool(false))
            }
            b'N' => {
                self.expect_literal(b"NaN", start)?;
                Ok(Event::Float(f64::NAN))
            }
            b'I' => {
                self.expect_literal(b"Infinity", start)?;
                Ok(Event::Float(f64::INFINITY))
            }
            b'-' => {
                if self.src[start..].starts_with(b"-Infinity") {
                    self.i += b"-Infinity".len();
                    Ok(Event::Float(f64::NEG_INFINITY))
                } else {
                    self.parse_number(start)
                }
            }
            b'0'..=b'9' => self.parse_number(start),
            _ => Err(self.error("Expecting value", start)),
        }
    }

    fn expect_literal(&mut self, lit: &[u8], start: usize) -> Result<(), ParseError> {
        if self.src[start..].starts_with(lit) {
            self.i = start + lit.len();
            Ok(())
        } else {
            Err(self.error("Expecting value", start))
        }
    }

    fn parse_number(&mut self, start: usize) -> Result<Event<'a>, ParseError> {
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        match self.peek() {
            Some(b'0') => self.i += 1,
            Some(b'1'..=b'9') => {
                self.i += 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.i += 1;
                }
            }
            _ => return Err(self.error("Expecting value", start)),
        }

        let mut is_float = false;
        if self.peek() == Some(b'.') && matches!(self.src.get(self.i + 1), Some(b'0'..=b'9')) {
            is_float = true;
            self.i += 1;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.i += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            let mut j = self.i + 1;
            if matches!(self.src.get(j), Some(b'+' | b'-')) {
                j += 1;
            }
            if matches!(self.src.get(j), Some(b'0'..=b'9')) {
                is_float = true;
                self.i = j + 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.i += 1;
                }
            }
        }

        let token = &self.src[start..self.i];
        // Il token è sempre ASCII per costruzione.
        let text = std::str::from_utf8(token).map_err(|_| self.error("Expecting value", start))?;
        if is_float {
            let value = text
                .parse::<f64>()
                .map_err(|_| self.error("Expecting value", start))?;
            Ok(Event::Float(value))
        } else {
            Ok(Event::Int(text))
        }
    }

    fn parse_string(&mut self) -> Result<JsonStr<'a>, ParseError> {
        let quote = self.i;
        self.i += 1;
        let content_start = self.i;
        // `None` = nessun escape visto finora (prestito possibile).
        let mut buf: Option<Vec<u32>> = None;

        loop {
            let Some(b) = self.peek() else {
                return Err(self.error("Unterminated string starting at", quote));
            };
            match b {
                b'"' => {
                    self.i += 1;
                    if let Some(cps) = buf {
                        return Ok(code_points_to_json_str(cps));
                    }
                    let raw = &self.src[content_start..self.i - 1];
                    return match std::str::from_utf8(raw) {
                        Ok(s) => Ok(JsonStr::Borrowed(s)),
                        Err(_) => Ok(JsonStr::Code(decode_wtf8(raw))),
                    };
                }
                b'\\' => {
                    if buf.is_none() {
                        // Copia il prefisso già letto e prosegui in modalità code point.
                        buf = Some(decode_wtf8(&self.src[content_start..self.i]));
                    }
                    self.i += 1;
                    let Escape { cp, next } = self.parse_escape(quote)?;
                    self.i = next;
                    if let Some(cps) = buf.as_mut() {
                        cps.push(cp);
                    }
                }
                b if b < 0x20 => {
                    return Err(self.error("Invalid control character at", self.i));
                }
                _ => {
                    if let Some(cps) = buf.as_mut() {
                        let (cp, next) = decode_one(self.src, self.i)
                            .map_err(|_| self.error("Unterminated string starting at", quote))?;
                        cps.push(cp);
                        self.i = next;
                    } else {
                        // Il byte non è un carattere speciale ASCII: si può saltare.
                        self.i += 1;
                    }
                }
            }
        }
    }

    /// Ritorna code point decodificato e nuova posizione (dopo l'escape).
    fn parse_escape(&self, quote: usize) -> Result<Escape, ParseError> {
        let backslash = self.i - 1;
        let Some(b) = self.peek() else {
            return Err(self.error("Unterminated string starting at", quote));
        };
        let simple = match b {
            b'"' => Some(0x22),
            b'\\' => Some(0x5c),
            b'/' => Some(0x2f),
            b'b' => Some(0x08),
            b'f' => Some(0x0c),
            b'n' => Some(0x0a),
            b'r' => Some(0x0d),
            b't' => Some(0x09),
            _ => None,
        };
        if let Some(cp) = simple {
            return Ok(Escape {
                cp,
                next: self.i + 1,
            });
        }
        if b != b'u' {
            return Err(self.error("Invalid \\escape", backslash));
        }
        // Posizione di CPython: backslash + 1 (la 'u').
        let bad = self.error("Invalid \\uXXXX escape", backslash + 1);
        let hex = self
            .src
            .get(self.i + 1..self.i + 5)
            .ok_or_else(|| bad.clone())?;
        let cp = parse_hex4(hex).ok_or(bad)?;
        let next = self.i + 5;

        // Coppia surrogata solo se il basso segue immediatamente come \uXXXX.
        if (0xd800..=0xdbff).contains(&cp)
            && self.src.get(next) == Some(&b'\\')
            && self.src.get(next + 1) == Some(&b'u')
            && let Some(hex2) = self.src.get(next + 2..next + 6)
            && let Some(low) = parse_hex4(hex2)
            && (0xdc00..=0xdfff).contains(&low)
        {
            let combined = 0x1_0000 + ((cp - 0xd800) << 10) + (low - 0xdc00);
            return Ok(Escape {
                cp: combined,
                next: next + 6,
            });
        }
        Ok(Escape { cp, next })
    }
}

struct Escape {
    cp: u32,
    next: usize,
}

fn parse_hex4(b: &[u8]) -> Option<u32> {
    if b.len() != 4 {
        return None;
    }
    let mut v = 0u32;
    for &c in b {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => return None,
        };
        v = (v << 4) | d as u32;
    }
    Some(v)
}

fn code_points_to_json_str(cps: Vec<u32>) -> JsonStr<'static> {
    if cps.iter().all(|&c| char::from_u32(c).is_some()) {
        let s: String = cps.iter().filter_map(|&c| char::from_u32(c)).collect();
        JsonStr::Owned(s)
    } else {
        JsonStr::Code(cps)
    }
}

fn count_code_points(src: &[u8], byte_pos: usize) -> usize {
    let end = byte_pos.min(src.len());
    let mut i = 0;
    let mut n = 0;
    while i < end {
        match decode_one(src, i) {
            Ok((_, next)) => {
                i = next;
                n += 1;
            }
            Err(_) => {
                i += 1;
                n += 1;
            }
        }
    }
    n
}

/// Decodifica un code point UTF-8 o WTF-8 (surrogati isolati inclusi).
fn decode_one(src: &[u8], i: usize) -> Result<(u32, usize), ()> {
    let b0 = *src.get(i).ok_or(())?;
    if b0 < 0x80 {
        return Ok((b0 as u32, i + 1));
    }
    let (len, init) = match b0 {
        0xc2..=0xdf => (2, (b0 & 0x1f) as u32),
        0xe0..=0xef => (3, (b0 & 0x0f) as u32),
        0xf0..=0xf4 => (4, (b0 & 0x07) as u32),
        _ => return Err(()),
    };
    let mut cp = init;
    for k in 1..len {
        let b = *src.get(i + k).ok_or(())?;
        if b & 0xc0 != 0x80 {
            return Err(());
        }
        cp = (cp << 6) | (b & 0x3f) as u32;
    }
    Ok((cp, i + len))
}

fn decode_wtf8(src: &[u8]) -> Vec<u32> {
    let mut out = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < src.len() {
        match decode_one(src, i) {
            Ok((cp, next)) => {
                out.push(cp);
                i = next;
            }
            Err(()) => {
                out.push(0xfffd);
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Vec<Event<'_>>, ParseError> {
        let mut r = Reader::new(s.as_bytes());
        let mut out = Vec::new();
        while let Some(ev) = r.next_event()? {
            out.push(ev);
        }
        Ok(out)
    }

    #[test]
    fn eventi_base() {
        assert_eq!(
            parse("[1, \"a\", true, null]").unwrap(),
            vec![
                Event::StartArray,
                Event::Int("1"),
                Event::Str(JsonStr::Borrowed("a")),
                Event::Bool(true),
                Event::Null,
                Event::EndArray,
            ]
        );
    }

    #[test]
    fn oggetto_e_chiavi() {
        assert_eq!(
            parse("{\"a\": 1, \"a\": 2}").unwrap(),
            vec![
                Event::StartObject,
                Event::Key(JsonStr::Borrowed("a")),
                Event::Int("1"),
                Event::Key(JsonStr::Borrowed("a")),
                Event::Int("2"),
                Event::EndObject,
            ]
        );
    }

    #[test]
    fn numeri_grandi_come_testo() {
        let ev = parse("123456789012345678901234567890").unwrap();
        assert_eq!(ev, vec![Event::Int("123456789012345678901234567890")]);
    }

    #[test]
    fn escape_e_surrogati() {
        let ev = parse(r#""\u00e9\ud83d\ude00\ud800""#).unwrap();
        match &ev[0] {
            Event::Str(JsonStr::Code(cps)) => {
                assert_eq!(cps, &[0xe9, 0x1f600, 0xd800]);
            }
            other => panic!("evento inatteso: {other:?}"),
        }
    }

    #[test]
    fn errori_con_posizione() {
        let casi = [
            ("", "Expecting value", 0),
            (" ", "Expecting value", 1),
            ("[1 2]", "Expecting ',' delimiter", 3),
            ("[1,]", "Illegal trailing comma before end of array", 2),
            (
                "{\"a\":1,}",
                "Illegal trailing comma before end of object",
                6,
            ),
            ("\"abc", "Unterminated string starting at", 0),
            ("1.", "Extra data", 1),
            ("\"\\u12\"", "Invalid \\uXXXX escape", 2),
            ("\"\\x\"", "Invalid \\escape", 1),
            ("\"a\u{1}b\"", "Invalid control character at", 2),
        ];
        for (s, msg, pos) in casi {
            let err = parse(s).unwrap_err();
            assert_eq!((err.message, err.pos), (msg, pos), "input {s:?}");
        }
    }

    #[test]
    fn costanti_non_standard() {
        let ev = parse("[NaN, Infinity, -Infinity]").unwrap();
        assert!(matches!(ev[1], Event::Float(v) if v.is_nan()));
        assert!(matches!(ev[2], Event::Float(v) if v == f64::INFINITY));
        assert!(matches!(ev[3], Event::Float(v) if v == f64::NEG_INFINITY));
    }
}
