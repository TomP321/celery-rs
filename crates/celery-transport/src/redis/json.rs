//! Kombu Redis JSON envelopes with Python dictionary ordering, default separators
//! and ASCII escaping; virtual properties follow kombu's augmentation order.

use bytes::Bytes;
use celery_proto::json::write::{write_float, write_int, write_str};
use celery_proto::json::{Event, JsonStr, Reader};
use compact_str::CompactString;

use crate::message::{Decimal, HeaderValue, Headers, Message, Properties};
use crate::transport::{Error, Result};

/// Decoded envelope.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// Decoded body bytes.
    pub body: Bytes,
    pub content_type: Option<CompactString>,
    pub content_encoding: Option<CompactString>,
    pub headers: Headers,
    /// Message properties including delivery information and tag.
    pub properties: Properties,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub delivery_tag: CompactString,
    pub redelivered: bool,
}

/// Encodes a kombu envelope; all destination queues share the caller's delivery tag.
pub fn encode(
    message: &Message,
    exchange: &str,
    routing_key: &str,
    body_encoding: &str,
    delivery_tag: &str,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.body.len() + 256);
    out.push(b'{');
    out.extend_from_slice(b"\"body\": ");
    if body_encoding.eq_ignore_ascii_case("base64") {
        let encoded = base64_encode(&message.body);
        write_str(&mut out, &encoded);
    } else {
        // Without base64 encoding, kombu JSON writes bytes with a type marker.
        write_value(&mut out, &HeaderValue::Bytes(message.body.clone()));
    }

    out.extend_from_slice(b", \"content-encoding\": ");
    write_optional_str(&mut out, message.content_encoding.as_deref());
    out.extend_from_slice(b", \"content-type\": ");
    write_optional_str(&mut out, message.content_type.as_deref());
    out.extend_from_slice(b", \"headers\": ");
    write_map(&mut out, &message.headers);
    out.extend_from_slice(b", \"properties\": {");

    // Preserve kombu's order: producer properties followed by virtual transport fields.
    let mut first = true;
    for (key, value) in message.properties.as_headers().iter() {
        if matches!(
            key.as_str(),
            "priority" | "delivery_info" | "body_encoding" | "delivery_tag"
        ) {
            continue;
        }
        write_separator(&mut out, &mut first);
        write_str(&mut out, key);
        out.extend_from_slice(b": ");
        write_value(&mut out, value);
    }

    write_separator(&mut out, &mut first);
    out.extend_from_slice(b"\"delivery_info\": {\"exchange\": ");
    write_str(&mut out, exchange);
    out.extend_from_slice(b", \"routing_key\": ");
    write_str(&mut out, routing_key);
    out.push(b'}');

    write_separator(&mut out, &mut first);
    out.extend_from_slice(b"\"priority\": ");
    let priority = message.properties.priority().unwrap_or(0);
    write_int(&mut out, i64::from(priority));

    write_separator(&mut out, &mut first);
    out.extend_from_slice(b"\"body_encoding\": ");
    write_str(&mut out, body_encoding);
    write_separator(&mut out, &mut first);
    out.extend_from_slice(b"\"delivery_tag\": ");
    write_str(&mut out, delivery_tag);

    out.extend_from_slice(b"}}");
    out
}

/// Marks headers and delivery information as redelivered, preserving order and formatting.
pub fn mark_redelivered(raw: &[u8]) -> Result<Vec<u8>> {
    if raw.len() > 64 * 1024 * 1024 {
        return Err(invalid("envelope exceeds 64 MiB"));
    }
    let mut reader = Reader::with_max_depth(raw, 64);
    let Some(event) = next(&mut reader)? else {
        return Err(invalid("empty envelope"));
    };
    let mut envelope = match parse_value(&mut reader, event)? {
        HeaderValue::Map(map) => map,
        _ => return Err(invalid("envelope is not an object")),
    };

    next(&mut reader)?;
    if let Some(HeaderValue::Map(headers)) = envelope.get_mut("headers") {
        headers.insert("redelivered", HeaderValue::Bool(true));
    }
    if let Some(HeaderValue::Map(properties)) = envelope.get_mut("properties")
        && let Some(HeaderValue::Map(delivery_info)) = properties.get_mut("delivery_info")
    {
        delivery_info.insert("redelivered", HeaderValue::Bool(true));
    }

    let mut out = Vec::with_capacity(raw.len() + 48);
    write_map(&mut out, &envelope);
    Ok(out)
}

/// Decodes a kombu envelope.
pub fn decode(raw: &[u8]) -> Result<Envelope> {
    if raw.len() > 64 * 1024 * 1024 {
        return Err(invalid("envelope exceeds 64 MiB"));
    }
    let mut reader = Reader::with_max_depth(raw, 64);
    let Some(event) = next(&mut reader)? else {
        return Err(invalid("empty envelope"));
    };
    let envelope = match parse_value(&mut reader, event)? {
        HeaderValue::Map(map) => map,
        _ => return Err(invalid("envelope is not an object")),
    };

    next(&mut reader)?;
    let content_type = envelope.get_str("content-type").map(CompactString::from);
    let content_encoding = envelope
        .get_str("content-encoding")
        .map(CompactString::from);
    let headers = match envelope.get("headers") {
        Some(HeaderValue::Map(map)) => map.clone(),
        Some(HeaderValue::Null) | None => Headers::new(),
        Some(_) => return Err(invalid("headers is not an object")),
    };
    let properties = match envelope.get("properties") {
        Some(HeaderValue::Map(map)) => Properties::from(map.clone()),
        _ => return Err(invalid("properties is not an object")),
    };

    let body_encoding = properties
        .get_str("body_encoding")
        .unwrap_or("base64")
        .to_string();
    let body = match envelope.get("body") {
        Some(HeaderValue::Str(text)) => {
            if body_encoding.eq_ignore_ascii_case("base64") {
                Bytes::from(
                    base64_decode(text.as_bytes()).ok_or_else(|| invalid("invalid base64 body"))?,
                )
            } else {
                Bytes::copy_from_slice(text.as_bytes())
            }
        }
        Some(HeaderValue::Bytes(bytes)) => bytes.clone(),
        Some(HeaderValue::Null) | None => Bytes::new(),
        Some(_) => return Err(invalid("body is not a string")),
    };

    let delivery_tag = properties
        .get_str("delivery_tag")
        .ok_or_else(|| invalid("missing delivery_tag"))?
        .to_string();
    let delivery_info = properties
        .get("delivery_info")
        .and_then(HeaderValue::as_map);
    let exchange = delivery_info
        .and_then(|info| info.get_str("exchange"))
        .unwrap_or("")
        .to_string();
    let routing_key = delivery_info
        .and_then(|info| info.get_str("routing_key"))
        .unwrap_or("")
        .to_string();
    let redelivered = delivery_info
        .and_then(|info| info.get_bool("redelivered"))
        .or_else(|| headers.get_bool("redelivered"))
        .unwrap_or(false);

    Ok(Envelope {
        body,
        content_type,
        content_encoding,
        headers,
        properties,
        exchange: CompactString::from(exchange),
        routing_key: CompactString::from(routing_key),
        delivery_tag: CompactString::from(delivery_tag),
        redelivered,
    })
}

/// Extracts payload, exchange and routing key from an unacked hash entry.
pub fn split_unacked(raw: &[u8]) -> Result<(Vec<u8>, CompactString, CompactString)> {
    if raw.len() > 64 * 1024 * 1024 {
        return Err(invalid("envelope exceeds 64 MiB"));
    }
    let mut reader = Reader::with_max_depth(raw, 64);
    let Some(event) = next(&mut reader)? else {
        return Err(invalid("empty unacked entry"));
    };
    let value = parse_value(&mut reader, event)?;
    next(&mut reader)?;
    let HeaderValue::List(items) = value else {
        return Err(invalid("unacked entry is not a list"));
    };
    if items.len() != 3 {
        return Err(invalid("unacked entry does not have three elements"));
    }
    let mut payload = Vec::new();
    write_value(&mut payload, &items[0]);
    let exchange = match &items[1] {
        HeaderValue::Str(text) => text.clone(),
        _ => return Err(invalid("invalid exchange in unacked entry")),
    };
    let routing_key = match &items[2] {
        HeaderValue::Str(text) => text.clone(),
        _ => return Err(invalid("invalid routing_key in unacked entry")),
    };
    Ok((payload, exchange, routing_key))
}

/// Encodes payload, exchange and routing key for the unacked hash.
pub fn join_unacked(payload: &[u8], exchange: &str, routing_key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + exchange.len() + routing_key.len() + 8);
    out.push(b'[');
    out.extend_from_slice(payload);
    out.extend_from_slice(b", ");
    write_str(&mut out, exchange);
    out.extend_from_slice(b", ");
    write_str(&mut out, routing_key);
    out.push(b']');
    out
}

fn invalid(message: &str) -> Error {
    Error::Channel(format!("invalid Redis envelope: {message}"))
}

fn write_separator(out: &mut Vec<u8>, first: &mut bool) {
    if *first {
        *first = false;
    } else {
        out.extend_from_slice(b", ");
    }
}

fn write_optional_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(text) => write_str(out, text),
        None => out.extend_from_slice(b"null"),
    }
}

fn write_map(out: &mut Vec<u8>, map: &Headers) {
    out.push(b'{');
    let mut first = true;
    for (key, value) in map.iter() {
        write_separator(out, &mut first);
        write_str(out, key);
        out.extend_from_slice(b": ");
        write_value(out, value);
    }
    out.push(b'}');
}

fn write_value(out: &mut Vec<u8>, value: &HeaderValue) {
    match value {
        HeaderValue::Null => out.extend_from_slice(b"null"),
        HeaderValue::Bool(true) => out.extend_from_slice(b"true"),
        HeaderValue::Bool(false) => out.extend_from_slice(b"false"),
        HeaderValue::Int(number) => write_int(out, *number),
        HeaderValue::BigInt(text) => out.extend_from_slice(text.as_bytes()),
        HeaderValue::Float(number) => write_float(out, *number),
        HeaderValue::Str(text) => write_str(out, text),
        HeaderValue::Bytes(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => write_tagged(out, "bytes", text),
            Err(_) => write_tagged(out, "base64", &base64_encode(bytes)),
        },
        HeaderValue::List(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.extend_from_slice(b", ");
                }
                write_value(out, item);
            }
            out.push(b']');
        }
        HeaderValue::Map(map) => write_map(out, map),
        HeaderValue::Timestamp(seconds) => write_tagged(out, "datetime", &iso8601(*seconds)),
        HeaderValue::Decimal(decimal) => write_tagged(out, "decimal", &decimal_string(*decimal)),
    }
}

fn write_tagged(out: &mut Vec<u8>, kind: &str, value: &str) {
    out.extend_from_slice(b"{\"__type__\": ");
    write_str(out, kind);
    out.extend_from_slice(b", \"__value__\": ");
    write_str(out, value);
    out.push(b'}');
}

fn next<'a>(reader: &mut Reader<'a>) -> Result<Option<Event<'a>>> {
    reader
        .next_event()
        .map_err(|err| invalid(&format!("{err}")))
}

fn key_text(key: JsonStr<'_>) -> CompactString {
    match key {
        JsonStr::Borrowed(text) => CompactString::from(text),
        JsonStr::Owned(text) => CompactString::from(text),
        JsonStr::Code(points) => CompactString::from(String::from_utf8_lossy(
            &points.iter().map(|cp| *cp as u8).collect::<Vec<_>>(),
        )),
    }
}

fn string_text(text: JsonStr<'_>) -> CompactString {
    match text {
        JsonStr::Borrowed(text) => CompactString::from(text),
        JsonStr::Owned(text) => CompactString::from(text),
        JsonStr::Code(points) => {
            let mut out = String::new();
            for cp in points {
                out.push(char::from_u32(cp).unwrap_or('\u{fffd}'));
            }
            CompactString::from(out)
        }
    }
}

fn parse_value<'a>(reader: &mut Reader<'a>, event: Event<'a>) -> Result<HeaderValue> {
    Ok(match event {
        Event::Null => HeaderValue::Null,
        Event::Bool(value) => HeaderValue::Bool(value),
        Event::Int(text) => match text.parse::<i64>() {
            Ok(number) => HeaderValue::Int(number),
            Err(_) => HeaderValue::BigInt(CompactString::from(text)),
        },
        Event::Float(number) => HeaderValue::Float(number),
        Event::Str(text) => HeaderValue::Str(string_text(text)),
        Event::StartArray => {
            let mut items = Vec::new();
            loop {
                let Some(event) = next(reader)? else {
                    return Err(invalid("unterminated array"));
                };
                if matches!(event, Event::EndArray) {
                    break;
                }
                items.push(parse_value(reader, event)?);
            }
            HeaderValue::List(items)
        }
        Event::StartObject => {
            let mut map = Headers::new();
            loop {
                let Some(event) = next(reader)? else {
                    return Err(invalid("unterminated object"));
                };
                match event {
                    Event::EndObject => break,
                    Event::Key(key) => {
                        let Some(value) = next(reader)? else {
                            return Err(invalid("missing value"));
                        };
                        let value = parse_value(reader, value)?;
                        map.insert(key_text(key), value);
                    }
                    _ => return Err(invalid("expected key")),
                }
            }
            decode_tagged(map)
        }
        Event::Key(_) | Event::EndArray | Event::EndObject => {
            return Err(invalid("unexpected event"));
        }
    })
}

// Decodes kombu's typed JSON markers.
fn decode_tagged(map: Headers) -> HeaderValue {
    if map.len() != 2 || !map.contains_key("__type__") || !map.contains_key("__value__") {
        return HeaderValue::Map(map);
    }
    let Some(kind) = map.get_str("__type__") else {
        return HeaderValue::Map(map);
    };
    let value = map.get("__value__").cloned().unwrap_or(HeaderValue::Null);
    match kind {
        "bytes" => match value {
            HeaderValue::Str(text) => HeaderValue::Bytes(Bytes::copy_from_slice(text.as_bytes())),
            other => other,
        },
        "base64" => match value {
            HeaderValue::Str(text) => match base64_decode(text.as_bytes()) {
                Some(bytes) => HeaderValue::Bytes(Bytes::from(bytes)),
                None => HeaderValue::Map(map),
            },
            other => other,
        },
        "datetime" => match value {
            HeaderValue::Str(text) => match parse_iso8601(&text) {
                Some(seconds) => HeaderValue::Timestamp(seconds),
                None => HeaderValue::Str(text),
            },
            other => other,
        },
        "decimal" => match value {
            HeaderValue::Str(text) => match parse_decimal(&text) {
                Some(decimal) => HeaderValue::Decimal(decimal),
                None => HeaderValue::Str(text),
            },
            other => other,
        },
        "uuid" => match value {
            HeaderValue::Map(fields) => match fields.get_str("hex") {
                Some(hex) => HeaderValue::Str(CompactString::from(hex)),
                None => HeaderValue::Map(fields),
            },
            other => other,
        },
        _ => HeaderValue::Map(map),
    }
}

fn decimal_string(decimal: Decimal) -> String {
    let negative = decimal.value < 0;
    let digits = decimal.value.unsigned_abs().to_string();
    let scale = usize::from(decimal.scale);
    let mut out = String::with_capacity(digits.len() + scale + 2);
    if negative {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(&digits);
        return out;
    }
    if digits.len() <= scale {
        out.push_str("0.");
        for _ in 0..scale - digits.len() {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        let split = digits.len() - scale;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

// Interprets a decimal string as sign, digits and scale.
fn parse_decimal(text: &str) -> Option<Decimal> {
    let text = text.trim();
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(index) => (&rest[..index], rest[index + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let mut value: i128 = 0;
    let mut scale: i64 = frac_part.len() as i64 - i64::from(exponent);
    for ch in int_part.chars().chain(frac_part.chars()) {
        let digit = ch.to_digit(10)?;
        value = value.checked_mul(10)?.checked_add(i128::from(digit))?;
    }
    while scale < 0 {
        value = value.checked_mul(10)?;
        scale += 1;
    }
    if negative {
        value = -value;
    }
    let value = i32::try_from(value).ok()?;
    Some(Decimal {
        scale: u8::try_from(scale).ok()?,
        value,
    })
}

// Returns days since the epoch in the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = month_index + if month_index < 10 { 3 } else { -9 };
    (year + i64::from(month <= 2), month as u32, day as u32)
}

// Formats a naive UTC datetime as Python's `datetime.isoformat()` does.
fn iso8601(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let rest = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        rest / 3600,
        (rest % 3600) / 60,
        rest % 60
    )
}

// Parses a naive ISO 8601 datetime as UTC epoch seconds.
fn parse_iso8601(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() < 19 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year: i64 = text.get(0..4)?.parse().ok()?;
    let month: u32 = text.get(5..7)?.parse().ok()?;
    let day: u32 = text.get(8..10)?.parse().ok()?;
    let hour: i64 = text.get(11..13)?.parse().ok()?;
    let minute: i64 = text.get(14..16)?.parse().ok()?;
    let second: i64 = text.get(17..19)?.parse().ok()?;
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encodes standard base64 with padding.
pub fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = chunk.get(1).copied().map(u32::from).unwrap_or(0);
        let b2 = chunk.get(2).copied().map(u32::from).unwrap_or(0);
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(BASE64_ALPHABET[(triple >> 18) as usize & 0x3f] as char);
        out.push(BASE64_ALPHABET[(triple >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            BASE64_ALPHABET[(triple >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64_ALPHABET[triple as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Decodes base64, ignoring characters outside its alphabet as Python does.
pub fn base64_decode(input: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    let mut buffer: u32 = 0;
    let mut bits = 0u32;
    let mut padding = false;
    for &byte in input {
        if byte == b'=' {
            padding = true;
            continue;
        }
        if padding {
            continue;
        }
        let Some(value) = base64_value(byte) else {
            continue;
        };
        buffer = (buffer << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        let mut headers = Headers::new();
        headers.insert("task", HeaderValue::from("add"));
        headers.insert("retries", HeaderValue::Int(2));
        let mut properties = Properties::new();
        properties.set_correlation_id("abc");
        properties.set_reply_to("reply");
        properties.set_delivery_mode(2);
        properties.set_priority(4);
        Message::new(Bytes::from_static(b"{\"a\": 1}"))
            .with_content_type("application/json")
            .with_content_encoding("utf-8")
            .with_headers(headers)
            .with_properties(properties)
    }

    #[test]
    fn envelope_matches_kombu_layout() {
        let encoded = encode(
            &message(),
            "ex1",
            "rk",
            "base64",
            "9526f225-855a-4ba1-85f2-6848c3a04aef",
        );
        // Matches kombu 5.7 byte for byte; see test_publish_matches_kombu.
        assert_eq!(
            String::from_utf8(encoded).expect("utf8"),
            concat!(
                r#"{"body": "eyJhIjogMX0=", "content-encoding": "utf-8", "#,
                r#""content-type": "application/json", "headers": {"task": "add", "retries": 2}, "#,
                r#""properties": {"correlation_id": "abc", "reply_to": "reply", "#,
                r#""delivery_mode": 2, "delivery_info": {"exchange": "ex1", "routing_key": "rk"}, "#,
                r#""priority": 4, "body_encoding": "base64", "#,
                r#""delivery_tag": "9526f225-855a-4ba1-85f2-6848c3a04aef"}}"#
            )
        );
    }

    #[test]
    fn envelope_roundtrip() {
        let encoded = encode(
            &message(),
            "ex1",
            "rk",
            "base64",
            "9526f225-855a-4ba1-85f2-6848c3a04aef",
        );
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(decoded.body, Bytes::from_static(b"{\"a\": 1}"));
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.content_encoding.as_deref(), Some("utf-8"));
        assert_eq!(decoded.exchange, "ex1");
        assert_eq!(decoded.routing_key, "rk");
        assert_eq!(decoded.delivery_tag, "9526f225-855a-4ba1-85f2-6848c3a04aef");
        assert_eq!(decoded.headers.get_i64("retries"), Some(2));
        assert_eq!(decoded.properties.priority(), Some(4));
        assert!(!decoded.redelivered);
    }

    #[test]
    fn non_base64_body_stays_bytes() {
        // Non-base64 bodies retain kombu's bytes marker.
        let encoded = encode(&message(), "", "q", "utf-8", "tag");
        let text = String::from_utf8(encoded.clone()).expect("utf8");
        assert!(text.contains(r#""body": {"__type__": "bytes", "__value__": "{\"a\": 1}"}"#));
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(decoded.body, Bytes::from_static(b"{\"a\": 1}"));
    }

    #[test]
    fn redelivered_is_added_to_headers_and_delivery_info() {
        let encoded = encode(
            &message(),
            "ex1",
            "rk",
            "base64",
            "9526f225-855a-4ba1-85f2-6848c3a04aef",
        );
        let restored = mark_redelivered(&encoded).expect("restore");
        let text = String::from_utf8(restored.clone()).expect("utf8");
        assert!(text.contains(r#""headers": {"task": "add", "retries": 2, "redelivered": true}"#));
        assert!(text.contains(r#""routing_key": "rk", "redelivered": true"#));
        let decoded = decode(&restored).expect("decode");
        assert!(decoded.redelivered);
    }

    #[test]
    fn unacked_payload_layout() {
        let encoded = encode(
            &message(),
            "",
            "q",
            "base64",
            "9526f225-855a-4ba1-85f2-6848c3a04aef",
        );
        let joined = join_unacked(&encoded, "", "q");
        let text = String::from_utf8(joined.clone()).expect("utf8");
        assert!(text.starts_with("[{\"body\": \"eyJhIjogMX0=\""));
        assert!(text.ends_with(r#"}, "", "q"]"#));
        let (payload, exchange, routing_key) = split_unacked(&joined).expect("split");
        assert_eq!(payload, encoded);
        assert_eq!(exchange, "");
        assert_eq!(routing_key, "q");
    }

    #[test]
    fn kombu_typed_values_roundtrip() {
        let mut headers = Headers::new();
        headers.insert("when", HeaderValue::Timestamp(1_577_934_245));
        headers.insert(
            "price",
            HeaderValue::Decimal(Decimal {
                scale: 2,
                value: 125,
            }),
        );
        headers.insert("raw", HeaderValue::Bytes(Bytes::from_static(b"\x00\xff")));
        headers.insert("text", HeaderValue::Bytes(Bytes::from_static(b"abc")));
        headers.insert(
            "big",
            HeaderValue::BigInt(CompactString::from("1180591620717411303424")),
        );
        let message = Message::new(Bytes::from_static(b"x")).with_headers(headers);
        let encoded = encode(&message, "", "q", "base64", "tag");
        let text = String::from_utf8(encoded.clone()).expect("utf8");
        assert!(
            text.contains(
                r#""when": {"__type__": "datetime", "__value__": "2020-01-02T03:04:05"}"#
            )
        );
        assert!(text.contains(r#""price": {"__type__": "decimal", "__value__": "1.25"}"#));
        assert!(text.contains(r#""raw": {"__type__": "base64", "__value__": "AP8="}"#));
        assert!(text.contains(r#""text": {"__type__": "bytes", "__value__": "abc"}"#));
        assert!(text.contains(r#""big": 1180591620717411303424"#));

        let decoded = decode(&encoded).expect("decode");
        assert_eq!(
            decoded.headers.get("when"),
            Some(&HeaderValue::Timestamp(1_577_934_245))
        );
        assert_eq!(
            decoded.headers.get("price"),
            Some(&HeaderValue::Decimal(Decimal {
                scale: 2,
                value: 125
            }))
        );
        assert_eq!(
            decoded.headers.get("raw"),
            Some(&HeaderValue::Bytes(Bytes::from_static(b"\x00\xff")))
        );
        assert_eq!(
            decoded.headers.get("big"),
            Some(&HeaderValue::BigInt(CompactString::from(
                "1180591620717411303424"
            )))
        );
    }

    #[test]
    fn base64_roundtrip() {
        for input in [b"".as_slice(), b"a", b"ab", b"abc", b"\x00\xff\x80\x01"] {
            let encoded = base64_encode(input);
            let decoded = base64_decode(encoded.as_bytes()).expect("decode");
            assert_eq!(decoded, input, "input {input:?}");
        }
        assert_eq!(base64_encode(b"{\"a\": 1}"), "eyJhIjogMX0=");
    }

    #[test]
    fn decimal_strings_match_python() {
        for (decimal, expected) in [
            (
                Decimal {
                    scale: 2,
                    value: 125,
                },
                "1.25",
            ),
            (
                Decimal {
                    scale: 2,
                    value: -5,
                },
                "-0.05",
            ),
            (Decimal { scale: 2, value: 0 }, "0.00"),
            (
                Decimal {
                    scale: 0,
                    value: 42,
                },
                "42",
            ),
        ] {
            assert_eq!(decimal_string(decimal), expected);
        }
        assert_eq!(
            parse_decimal("1.25"),
            Some(Decimal {
                scale: 2,
                value: 125
            })
        );
        assert_eq!(
            parse_decimal("-0.05"),
            Some(Decimal {
                scale: 2,
                value: -5
            })
        );
    }

    #[test]
    fn iso8601_matches_python() {
        assert_eq!(iso8601(1_577_934_245), "2020-01-02T03:04:05");
        assert_eq!(parse_iso8601("2020-01-02T03:04:05"), Some(1_577_934_245));
        assert_eq!(parse_iso8601("1970-01-01T00:00:00"), Some(0));
        assert_eq!(
            parse_iso8601("2020-01-02T03:04:05.123456"),
            Some(1_577_934_245)
        );
    }

    #[test]
    fn invalid_envelopes_are_reported() {
        assert!(decode(b"[]").is_err());
        assert!(decode(b"{\"body\": \"x\"}").is_err());
        assert!(decode(b"not json").is_err());
    }
}
