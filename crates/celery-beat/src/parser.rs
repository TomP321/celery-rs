//! Parser dei campi crontab: stessa semantica di `crontab_parser` del riferimento.
//!
//! Il riferimento usa `re.match` (ancorato solo all'inizio) e `int()` di Python:
//! qui i pattern, i messaggi e l'ordine dei controlli sono gli stessi.

use crate::error::Error;

/// Parser con `max_`/`min_` come il riferimento (interi Python, anche negativi).
#[derive(Debug, Clone, Copy)]
pub struct CrontabParser {
    pub max_: i64,
    pub min_: i64,
}

/// Espande un campo completo (`'*'`, `'1-5/2'`, `'1,3,5'`, nomi, ...).
pub fn parse(spec: &str, max_: i64, min_: i64) -> Result<Vec<i64>, Error> {
    CrontabParser { max_, min_ }.parse(spec)
}

impl CrontabParser {
    pub fn new(max_: i64, min_: i64) -> Self {
        Self { max_, min_ }
    }

    pub fn parse(&self, spec: &str) -> Result<Vec<i64>, Error> {
        let mut acc = Vec::new();
        for part in spec.split(',') {
            if part.is_empty() {
                return Err(Error::EmptyPart);
            }
            acc.extend(self.parse_part(part)?);
        }
        acc.sort_unstable();
        acc.dedup();
        Ok(acc)
    }

    fn parse_part(&self, part: &str) -> Result<Vec<i64>, Error> {
        // Stessi quattro pattern di `crontab_parser.pats`, in ordine, con la
        // semantica di `re.match` (ancorato all'inizio, gruppi greedy).
        if let Some((from, to, step)) = range_steps_captures(part) {
            return self.range_steps(from, to, step);
        }
        if let Some((from, to)) = range_captures(part) {
            return self.expand_range(Some(from), Some(to));
        }
        if let Some(step) = star_steps_captures(part) {
            return self.star_steps(step);
        }
        if part == "*" {
            return Ok(self.expand_star());
        }
        self.expand_range(Some(part), None)
    }

    fn expand_range(&self, from: Option<&str>, to: Option<&str>) -> Result<Vec<i64>, Error> {
        let fr = self.expand_number(from.unwrap_or_default())?;
        match to {
            Some(to) => {
                let to = self.expand_number(to)?;
                if to < fr {
                    // Wrap su `max_`, come il riferimento.
                    let mut values: Vec<i64> = (fr..self.min_ + self.max_).collect();
                    values.extend(self.min_..=to);
                    Ok(values)
                } else {
                    Ok((fr..=to).collect())
                }
            }
            None => Ok(vec![fr]),
        }
    }

    fn range_steps(
        &self,
        from: &str,
        to: &str,
        step: Option<&str>,
    ) -> Result<Vec<i64>, Error> {
        let Some(step) = step.filter(|s| !s.is_empty()) else {
            return Err(Error::EmptyFilter);
        };
        // Il riferimento espande il range prima di convertire lo step.
        let values = self.expand_range(Some(from), Some(to))?;
        let step = self.expand_step(step)?;
        Ok(slice_step(values, step))
    }

    fn star_steps(&self, step: Option<&str>) -> Result<Vec<i64>, Error> {
        let Some(step) = step.filter(|s| !s.is_empty()) else {
            return Err(Error::EmptyFilter);
        };
        let step = self.expand_step(step)?;
        Ok(slice_step(self.expand_star(), step))
    }

    fn expand_star(&self) -> Vec<i64> {
        (self.min_..self.max_ + self.min_).collect()
    }

    /// `int(toks[...])` del riferimento: solo ValueError con il suo testo.
    fn expand_step(&self, s: &str) -> Result<usize, Error> {
        match parse_python_int(s) {
            Some(Number::Value(value)) => {
                if value == 0 {
                    return Err(Error::ZeroStep);
                }
                if value < 0 {
                    // Irraggiungibile: i pattern non accettano il segno.
                    return Err(Error::InvalidInt {
                        literal: s.to_string(),
                    });
                }
                Ok(usize::try_from(value).unwrap_or(usize::MAX))
            }
            Some(Number::Huge { .. }) => Ok(usize::MAX),
            None => Err(Error::InvalidInt {
                literal: s.to_string(),
            }),
        }
    }

    fn expand_number(&self, s: &str) -> Result<i64, Error> {
        if s.starts_with('-') {
            return Err(Error::NegativeNumber);
        }
        let value = match parse_python_int(s) {
            Some(Number::Value(value)) => value,
            Some(Number::Huge { negative, text }) => {
                return Err(if negative {
                    Error::BeginRange {
                        value: format!("-{text}"),
                        min: self.min_,
                    }
                } else {
                    Error::EndRange {
                        value: text,
                        max: self.max_value(),
                    }
                });
            }
            None => match lookup_name(s) {
                Some(value) => value,
                None => {
                    return Err(Error::InvalidWeekday {
                        literal: s.to_string(),
                    });
                }
            },
        };
        let max_value = self.max_value();
        if value > max_value {
            return Err(Error::EndRange {
                value: value.to_string(),
                max: max_value,
            });
        }
        if value < self.min_ {
            return Err(Error::BeginRange {
                value: value.to_string(),
                min: self.min_,
            });
        }
        Ok(value)
    }

    fn max_value(&self) -> i64 {
        self.min_ + self.max_ - 1
    }
}

/// `\w` dei pattern (le cifre/caratteri Unicode alfanumerici più `_`).
fn is_word(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

/// Primo `-` con prefisso non vuoto di soli `\w`: gruppo 1 non-greedy.
fn first_range(part: &str) -> Option<(&str, &str)> {
    let dash = part.find('-')?;
    if dash == 0 || !part[..dash].chars().all(is_word) {
        return None;
    }
    Some((&part[..dash], &part[dash + 1..]))
}

/// Corsa massima di `\w` (gruppo greedy); `None` se vuota.
fn word_run(text: &str) -> Option<(&str, &str)> {
    let end = text
        .char_indices()
        .find(|(_, ch)| !is_word(*ch))
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    if end == 0 {
        None
    } else {
        Some((&text[..end], &text[end..]))
    }
}

/// `^(\w+?)-(\w+)/(\w+)?`
fn range_steps_captures(part: &str) -> Option<(&str, &str, Option<&str>)> {
    let (from, rest) = first_range(part)?;
    let (to, rest) = word_run(rest)?;
    let rest = rest.strip_prefix('/')?;
    let step = word_run(rest).map(|(run, _)| run);
    Some((from, to, step))
}

/// `^(\w+?)-(\w+)`
fn range_captures(part: &str) -> Option<(&str, &str)> {
    let (from, rest) = first_range(part)?;
    let (to, _) = word_run(rest)?;
    Some((from, to))
}

/// `^\*/(\w+)?`
fn star_steps_captures(part: &str) -> Option<Option<&str>> {
    let rest = part.strip_prefix("*/")?;
    Some(word_run(rest).map(|(run, _)| run))
}

fn slice_step(values: Vec<i64>, step: usize) -> Vec<i64> {
    if step <= 1 {
        return values;
    }
    values.into_iter().step_by(step).collect()
}

#[derive(Debug, PartialEq, Eq)]
enum Number {
    Value(i64),
    /// Valore oltre `i64`: serve solo per il messaggio d'errore.
    Huge { negative: bool, text: String },
}

/// `int(s)` di Python per base 10.
///
/// Accetta spazi Unicode ai bordi, un segno e underscore tra le cifre, come
/// `int()`; le cifre Unicode non ASCII (es. `int('٣')`) non sono coperte.
fn parse_python_int(s: &str) -> Option<Number> {
    let s = s.trim();
    let (negative, body) = match s.as_bytes().first() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    if body.is_empty() {
        return None;
    }
    let mut digits = String::with_capacity(body.len());
    let mut prev_underscore = true;
    for ch in body.chars() {
        if ch == '_' {
            if prev_underscore {
                return None;
            }
            prev_underscore = true;
        } else if ch.is_ascii_digit() {
            digits.push(ch);
            prev_underscore = false;
        } else {
            return None;
        }
    }
    if prev_underscore {
        return None;
    }
    let normalized = digits.trim_start_matches('0');
    let normalized = if normalized.is_empty() { "0" } else { normalized };
    match normalizes_to_i64(normalized, negative) {
        Some(value) => Some(Number::Value(value)),
        None => Some(Number::Huge {
            negative,
            text: normalized.to_string(),
        }),
    }
}

fn normalizes_to_i64(digits: &str, negative: bool) -> Option<i64> {
    let magnitude: i64 = digits.parse().ok()?;
    Some(if negative { -magnitude } else { magnitude })
}

fn lookup_name(name: &str) -> Option<i64> {
    let key: String = name.chars().take(3).collect::<String>().to_lowercase();
    Some(match key.as_str() {
        "sun" => 0,
        "mon" => 1,
        "tue" => 2,
        "wed" => 3,
        "thu" => 4,
        "fri" => 5,
        "sat" => 6,
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(max_: i64, min_: i64, spec: &str) -> Result<Vec<i64>, Error> {
        parse(spec, max_, min_)
    }

    #[test]
    fn star_and_ranges() {
        assert_eq!(p(60, 0, "*").unwrap(), (0..60).collect::<Vec<_>>());
        assert_eq!(p(7, 0, "*").unwrap(), (0..7).collect::<Vec<_>>());
        assert_eq!(p(31, 1, "*").unwrap(), (1..=31).collect::<Vec<_>>());
        assert_eq!(p(60, 0, "1-10").unwrap(), (1..=10).collect::<Vec<_>>());
        assert_eq!(p(12, 0, "11-1").unwrap(), vec![0, 1, 11]);
        assert_eq!(p(60, 1, "2-1").unwrap(), (1..=60).collect::<Vec<_>>());
        assert_eq!(p(60, 0, "1,2,3,4").unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn steps() {
        assert_eq!(p(8, 0, "*/2").unwrap(), vec![0, 2, 4, 6]);
        assert_eq!(p(60, 0, "2-9/5").unwrap(), vec![2, 7]);
        assert_eq!(p(60, 1, "55-5/3").unwrap(), vec![1, 4, 55, 58]);
        assert_eq!(p(60, 0, "2-11/5,3").unwrap(), vec![2, 3, 7]);
        assert_eq!(p(60, 0, "1-9/2").unwrap(), vec![1, 3, 5, 7, 9]);
        assert_eq!(p(8, 1, "*/2").unwrap(), vec![1, 3, 5, 7]);
        assert_eq!(p(12, 1, "2-12/2").unwrap(), vec![2, 4, 6, 8, 10, 12]);
    }

    #[test]
    fn names_and_negative() {
        assert_eq!(p(7, 0, "mon-fri").unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(p(12, 1, "feb").unwrap(), vec![2]);
        assert_eq!(p(7, 0, "MON,SUN").unwrap(), vec![0, 1]);
        assert_eq!(p(12, 1, "december").unwrap(), vec![12]);
    }

    #[test]
    fn invalid_inputs() {
        assert_eq!(p(60, 0, ""), Err(Error::EmptyPart));
        assert_eq!(p(60, 0, "1,,2"), Err(Error::EmptyPart));
        assert_eq!(p(60, 0, "*/"), Err(Error::EmptyFilter));
        assert_eq!(p(60, 0, "1-2/"), Err(Error::EmptyFilter));
        assert_eq!(p(60, 0, "-20"), Err(Error::NegativeNumber));
        assert_eq!(
            p(60, 0, "foo"),
            Err(Error::InvalidWeekday {
                literal: "foo".into()
            })
        );
        assert_eq!(
            p(60, 0, "*/x"),
            Err(Error::InvalidInt {
                literal: "x".into()
            })
        );
        assert_eq!(p(60, 0, "*/0"), Err(Error::ZeroStep));
        assert_eq!(
            p(60, 0, "61"),
            Err(Error::EndRange {
                value: "61".into(),
                max: 59
            })
        );
        assert_eq!(
            p(12, 1, "0"),
            Err(Error::BeginRange {
                value: "0".into(),
                min: 1
            })
        );
    }

    #[test]
    fn python_int_compat() {
        // int('1_0') == 10, ma int('1__0')/int('_1') sono ValueError.
        assert_eq!(p(60, 0, "1_0").unwrap(), vec![10]);
        assert_eq!(
            p(60, 0, "1__0"),
            Err(Error::InvalidWeekday {
                literal: "1__0".into()
            })
        );
        assert_eq!(
            p(60, 0, "*/1_0").unwrap(),
            vec![0, 10, 20, 30, 40, 50]
        );
    }

    #[test]
    fn regex_parity_edge_cases() {
        // `re.match` non è ancorato in fondo: il gruppo greedy si prende il resto.
        assert_eq!(
            p(60, 0, "1-3x"),
            Err(Error::InvalidWeekday {
                literal: "3x".into()
            })
        );
        assert_eq!(
            p(60, 0, "1-2/3x"),
            Err(Error::InvalidInt {
                literal: "3x".into()
            })
        );
        assert_eq!(
            p(60, 0, "5-"),
            Err(Error::InvalidWeekday {
                literal: "5-".into()
            })
        );
    }
}
