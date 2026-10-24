//! Conversione fra i tipi condivisi ([`Message`], [`HeaderValue`]) e quelli AMQP
//! di lapin/amq-protocol.
//!
//! I tipi di header prodotti sono quelli di py-amqp (`serialization._write_item`):
//! stringhe e bytes diventano `longstr` (al ricevimento py-amqp li decodifica in
//! `str` se UTF-8 validi, altrimenti `bytes`), gli interi `int32` se ci stanno e
//! `int64` altrimenti, i float `double`, i decimali `D`, i timestamp `T`.

use bytes::Bytes;
use compact_str::CompactString;
use lapin::protocol::BasicProperties;
use lapin::types::{AMQPValue, DecimalValue, FieldArray, FieldTable, LongString, ShortString};

use crate::message::{Decimal, HeaderValue, Headers, Message, Properties};
use crate::transport::{Error, Result};

/// `ShortString` AMQP (max 255 byte) senza panico.
pub(crate) fn short_string(value: &str) -> Result<ShortString> {
    ShortString::try_new(value.to_string())
        .map_err(|_| Error::InvalidArgument(format!("stringa troppo lunga per AMQP: {value:?}")))
}

/// Tabella AMQP da una mappa ordinata di header.
pub(crate) fn to_field_table(headers: &Headers) -> Result<FieldTable> {
    let mut table = FieldTable::default();
    for (key, value) in headers.iter() {
        table.insert(short_string(key)?, value_to_amqp(value)?);
    }
    Ok(table)
}

/// Valore AMQP da un [`HeaderValue`].
pub(crate) fn value_to_amqp(value: &HeaderValue) -> Result<AMQPValue> {
    Ok(match value {
        HeaderValue::Null => AMQPValue::Void,
        HeaderValue::Bool(boolean) => AMQPValue::Boolean(*boolean),
        HeaderValue::Int(number) => match i32::try_from(*number) {
            Ok(small) => AMQPValue::LongInt(small),
            Err(_) => AMQPValue::LongLongInt(*number),
        },
        HeaderValue::BigInt(text) => {
            // py-amqp codifica gli interi con `>q`: oltre i 64 bit non c'è tipo.
            let number = text.parse::<i64>().map_err(|_| {
                Error::InvalidArgument(format!("intero troppo grande per AMQP: {text}"))
            })?;
            AMQPValue::LongLongInt(number)
        }
        HeaderValue::Float(number) => AMQPValue::Double(*number),
        // Come py-amqp: str e bytes sono entrambi `longstr`.
        HeaderValue::Str(text) => AMQPValue::LongString(LongString::from(text.as_bytes())),
        HeaderValue::Bytes(bytes) => AMQPValue::LongString(LongString::from(bytes.as_ref())),
        HeaderValue::List(items) => AMQPValue::FieldArray(FieldArray::from(
            items
                .iter()
                .map(value_to_amqp)
                .collect::<Result<Vec<_>>>()?,
        )),
        HeaderValue::Map(headers) => AMQPValue::FieldTable(to_field_table(headers)?),
        HeaderValue::Timestamp(seconds) => {
            let seconds = u64::try_from(*seconds).map_err(|_| {
                Error::InvalidArgument(format!("timestamp negativo per AMQP: {seconds}"))
            })?;
            AMQPValue::Timestamp(seconds)
        }
        HeaderValue::Decimal(decimal) => AMQPValue::DecimalValue(DecimalValue {
            scale: decimal.scale,
            // Il valore è a 32 bit con segno sui fili: si conservano i bit.
            value: decimal.value as u32,
        }),
    })
}

/// [`HeaderValue`] da un valore AMQP, con i tipi che vede py-amqp.
pub(crate) fn value_from_amqp(value: &AMQPValue) -> HeaderValue {
    match value {
        AMQPValue::Boolean(boolean) => HeaderValue::Bool(*boolean),
        AMQPValue::ShortShortInt(number) => HeaderValue::Int(i64::from(*number)),
        AMQPValue::ShortShortUInt(number) => HeaderValue::Int(i64::from(*number)),
        AMQPValue::ShortInt(number) => HeaderValue::Int(i64::from(*number)),
        AMQPValue::ShortUInt(number) => HeaderValue::Int(i64::from(*number)),
        AMQPValue::LongInt(number) => HeaderValue::Int(i64::from(*number)),
        AMQPValue::LongUInt(number) => HeaderValue::Int(i64::from(*number)),
        AMQPValue::LongLongInt(number) => HeaderValue::Int(*number),
        AMQPValue::Float(number) => HeaderValue::Float(f64::from(*number)),
        AMQPValue::Double(number) => HeaderValue::Float(*number),
        AMQPValue::DecimalValue(decimal) => HeaderValue::Decimal(Decimal {
            scale: decimal.scale,
            value: decimal.value as i32,
        }),
        AMQPValue::LongString(text) => match std::str::from_utf8(text.as_bytes()) {
            // py-amqp prova prima a decodificare in UTF-8 e ripiega sui bytes.
            Ok(text) => HeaderValue::Str(CompactString::from(text)),
            Err(_) => HeaderValue::Bytes(Bytes::copy_from_slice(text.as_bytes())),
        },
        AMQPValue::ShortString(text) => HeaderValue::Str(CompactString::from(text.as_str())),
        AMQPValue::ByteArray(bytes) => HeaderValue::Bytes(Bytes::copy_from_slice(bytes.as_slice())),
        AMQPValue::FieldArray(items) => {
            HeaderValue::List(items.as_slice().iter().map(value_from_amqp).collect())
        }
        AMQPValue::Timestamp(seconds) => {
            HeaderValue::Timestamp(i64::try_from(*seconds).unwrap_or(i64::MAX))
        }
        AMQPValue::FieldTable(table) => HeaderValue::Map(field_table_to_headers(table)),
        AMQPValue::Void => HeaderValue::Null,
    }
}

pub(crate) fn field_table_to_headers(table: &FieldTable) -> Headers {
    let mut headers = Headers::with_capacity(table.inner().len());
    for (key, value) in table {
        headers.insert(key.as_str(), value_from_amqp(value));
    }
    headers
}

fn property_string(key: &str, value: &HeaderValue) -> Result<ShortString> {
    match value {
        HeaderValue::Str(text) => short_string(text),
        HeaderValue::Bytes(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => short_string(text),
            Err(_) => Err(Error::InvalidArgument(format!(
                "proprietà {key} non è UTF-8"
            ))),
        },
        other => Err(Error::InvalidArgument(format!(
            "proprietà {key}: attesa stringa, trovato {other:?}"
        ))),
    }
}

fn property_u8(key: &str, value: &HeaderValue) -> Result<u8> {
    let number = value
        .as_i64()
        .ok_or_else(|| Error::InvalidArgument(format!("proprietà {key}: atteso intero")))?;
    u8::try_from(number)
        .map_err(|_| Error::InvalidArgument(format!("proprietà {key}: fuori da 0..=255")))
}

/// Proprietà AMQP native del messaggio (content-type, headers e `properties`
/// di kombu). Le chiavi sconosciute sono ignorate, come fa py-amqp.
pub(crate) fn message_to_properties(message: &Message) -> Result<BasicProperties> {
    let mut properties = BasicProperties::default();
    if let Some(content_type) = message.content_type.as_deref() {
        properties = properties.with_content_type(short_string(content_type)?);
    }
    if let Some(content_encoding) = message.content_encoding.as_deref() {
        properties = properties.with_content_encoding(short_string(content_encoding)?);
    }
    if !message.headers.is_empty() {
        properties = properties.with_headers(to_field_table(&message.headers)?);
    }
    for (key, value) in message.properties.as_headers().iter() {
        match key.as_str() {
            "delivery_mode" => {
                properties = properties.with_delivery_mode(property_u8(key, value)?);
            }
            "priority" => {
                properties = properties.with_priority(property_u8(key, value)?);
            }
            "correlation_id" => {
                properties = properties.with_correlation_id(property_string(key, value)?);
            }
            "reply_to" => {
                properties = properties.with_reply_to(property_string(key, value)?);
            }
            "expiration" => {
                properties = properties.with_expiration(property_string(key, value)?);
            }
            "message_id" => {
                properties = properties.with_message_id(property_string(key, value)?);
            }
            "type" => {
                properties = properties.with_type(property_string(key, value)?);
            }
            "user_id" => {
                properties = properties.with_user_id(property_string(key, value)?);
            }
            "app_id" => {
                properties = properties.with_app_id(property_string(key, value)?);
            }
            "cluster_id" => {
                properties = properties.with_cluster_id(property_string(key, value)?);
            }
            "timestamp" => {
                let seconds = match value {
                    HeaderValue::Timestamp(seconds) | HeaderValue::Int(seconds) => *seconds,
                    other => {
                        return Err(Error::InvalidArgument(format!(
                            "proprietà timestamp: atteso intero, trovato {other:?}"
                        )));
                    }
                };
                let seconds = u64::try_from(seconds).map_err(|_| {
                    Error::InvalidArgument("proprietà timestamp negativa".to_string())
                })?;
                properties = properties.with_timestamp(seconds);
            }
            // Campi del trasporto virtuale o derivati dagli header: py-amqp li
            // ignora, non sono proprietà AMQP native.
            "content_type"
            | "content_encoding"
            | "application_headers"
            | "delivery_info"
            | "delivery_tag"
            | "body_encoding" => {}
            _ => {}
        }
    }
    Ok(properties)
}

/// Messaggio condiviso da una proprietà AMQP consegnata dal broker.
///
/// `properties` contiene le proprietà native; `content_type`,
/// `content_encoding` e `application_headers` restano nei campi dedicati
/// (`Message::content_type`, `Message::headers`) per non duplicare gli header
/// nel percorso caldo. Il binding Python li ricompone come fa kombu.
pub(crate) fn message_from_amqp(body: Vec<u8>, properties: &BasicProperties) -> Message {
    let headers = headers_from_properties(properties);
    let mut map = Properties::new();
    if let Some(value) = properties.delivery_mode() {
        map.insert("delivery_mode", HeaderValue::Int(i64::from(*value)));
    }
    if let Some(value) = properties.priority() {
        map.insert("priority", HeaderValue::Int(i64::from(*value)));
    }
    if let Some(value) = properties.correlation_id() {
        map.insert("correlation_id", HeaderValue::from(value.as_str()));
    }
    if let Some(value) = properties.reply_to() {
        map.insert("reply_to", HeaderValue::from(value.as_str()));
    }
    if let Some(value) = properties.expiration() {
        map.insert("expiration", HeaderValue::from(value.as_str()));
    }
    if let Some(value) = properties.message_id() {
        map.insert("message_id", HeaderValue::from(value.as_str()));
    }
    if let Some(value) = properties.timestamp() {
        // py-amqp espone il timestamp come int nella mappa delle proprietà.
        map.insert(
            "timestamp",
            HeaderValue::Int(i64::try_from(*value).unwrap_or(i64::MAX)),
        );
    }
    if let Some(value) = properties.kind() {
        map.insert("type", HeaderValue::from(value.as_str()));
    }
    if let Some(value) = properties.user_id() {
        map.insert("user_id", HeaderValue::from(value.as_str()));
    }
    if let Some(value) = properties.app_id() {
        map.insert("app_id", HeaderValue::from(value.as_str()));
    }
    if let Some(value) = properties.cluster_id() {
        map.insert("cluster_id", HeaderValue::from(value.as_str()));
    }
    Message {
        body: Bytes::from(body),
        content_type: properties
            .content_type()
            .as_ref()
            .map(|value| CompactString::from(value.as_str())),
        content_encoding: properties
            .content_encoding()
            .as_ref()
            .map(|value| CompactString::from(value.as_str())),
        headers,
        properties: map,
    }
}

fn headers_from_properties(properties: &BasicProperties) -> Headers {
    properties
        .headers()
        .as_ref()
        .map(field_table_to_headers)
        .unwrap_or_default()
}

/// Verifica che un valore AMQP sia rappresentabile come header condiviso.
#[cfg(test)]
pub(crate) fn roundtrip_value(value: &HeaderValue) -> Result<HeaderValue> {
    Ok(value_from_amqp(&value_to_amqp(value)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_types_roundtrip_through_amqp() {
        let cases = [
            HeaderValue::Null,
            HeaderValue::Bool(true),
            HeaderValue::Int(-42),
            HeaderValue::Int(2_i64.pow(40)),
            HeaderValue::Float(1.5),
            HeaderValue::Str(CompactString::from("città €")),
            HeaderValue::Bytes(Bytes::from_static(&[0, 0xff, 0x80])),
            HeaderValue::List(vec![HeaderValue::Int(1), HeaderValue::Null]),
            HeaderValue::Timestamp(1_700_000_000),
            HeaderValue::Decimal(Decimal {
                scale: 2,
                value: -5,
            }),
        ];
        for value in cases {
            assert_eq!(roundtrip_value(&value).expect("roundtrip"), value);
        }
    }

    #[test]
    fn utf8_long_string_becomes_str_like_py_amqp() {
        let value = HeaderValue::Bytes(Bytes::from_static("text".as_bytes()));
        assert_eq!(
            roundtrip_value(&value).expect("roundtrip"),
            HeaderValue::Str(CompactString::from("text"))
        );
    }

    #[test]
    fn table_preserves_nested_types() {
        let mut inner = Headers::new();
        inner.insert("a", HeaderValue::Int(1));
        let mut headers = Headers::new();
        headers.insert("map", HeaderValue::Map(inner));
        headers.insert("list", HeaderValue::List(vec![HeaderValue::Bool(false)]));
        let table = to_field_table(&headers).expect("tabella");
        assert_eq!(field_table_to_headers(&table), headers);
    }

    #[test]
    fn big_int_out_of_range_is_rejected() {
        let value = HeaderValue::BigInt(CompactString::from("999999999999999999999999"));
        assert!(matches!(
            value_to_amqp(&value),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn properties_roundtrip() {
        let mut properties = Properties::new();
        properties.set_delivery_mode(2);
        properties.set_priority(4);
        properties.set_correlation_id("corr");
        properties.set_reply_to("reply");
        properties.insert("expiration", HeaderValue::from("1000"));
        properties.insert("message_id", HeaderValue::from("m1"));
        properties.insert("timestamp", HeaderValue::Int(1_700_000_000));
        properties.insert("type", HeaderValue::from("task"));
        properties.insert("app_id", HeaderValue::from("app"));
        let message = Message::new(Bytes::from_static(b"{}"))
            .with_content_type("application/json")
            .with_content_encoding("utf-8")
            .with_headers(Headers::new())
            .with_properties(properties);
        let amqp = message_to_properties(&message).expect("proprietà");
        let decoded = message_from_amqp(b"{}".to_vec(), &amqp);
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.content_encoding.as_deref(), Some("utf-8"));
        assert_eq!(decoded.properties.get_i64("delivery_mode"), Some(2));
        assert_eq!(decoded.properties.get_i64("priority"), Some(4));
        assert_eq!(decoded.properties.correlation_id(), Some("corr"));
        assert_eq!(decoded.properties.reply_to(), Some("reply"));
        assert_eq!(decoded.properties.expiration(), Some("1000"));
        assert_eq!(decoded.properties.message_id(), Some("m1"));
        assert_eq!(decoded.properties.timestamp(), Some(1_700_000_000));
        assert_eq!(decoded.properties.message_type(), Some("task"));
        assert_eq!(decoded.properties.app_id(), Some("app"));
    }

    #[test]
    fn long_property_is_rejected_without_panicking() {
        let long = "x".repeat(300);
        let mut properties = Properties::new();
        properties.set_correlation_id(long);
        let message = Message::new(Bytes::new()).with_properties(properties);
        assert!(matches!(
            message_to_properties(&message),
            Err(Error::InvalidArgument(_))
        ));
    }
}
