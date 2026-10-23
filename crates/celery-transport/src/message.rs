//! Messaggi, header e proprietà condivisi da tutti i trasporti.
//!
//! Niente `serde_json::Value`: header e proprietà sono mappe ordinate (l'ordine di
//! inserimento è quello del dizionario Python) di [`HeaderValue`] tipizzati.

use std::fmt;
use std::ops::{Deref, DerefMut};

use bytes::Bytes;
use compact_str::CompactString;

/// Valore di un header (o di una proprietà) di messaggio.
///
/// I tipi corrispondono a quelli che py-amqp/kombu sanno rappresentare in una
/// tabella AMQP e a quelli che il JSON di kombu può veicolare.
#[derive(Debug, Clone, PartialEq)]
pub enum HeaderValue {
    Null,
    Bool(bool),
    Int(i64),
    /// Intero che non entra in `i64`, in forma decimale.
    BigInt(CompactString),
    Float(f64),
    Str(CompactString),
    Bytes(Bytes),
    List(Vec<HeaderValue>),
    Map(Headers),
    /// Secondi dall'epoch Unix (tipo `T` di AMQP).
    Timestamp(i64),
    /// Decimale AMQP (`D`): valore intero e numero di cifre decimali.
    Decimal(Decimal),
}

/// Decimale con segno come lo rappresenta AMQP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal {
    pub scale: u8,
    pub value: i32,
}

impl HeaderValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Int(n) => Some(*n as f64),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Self::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[HeaderValue]> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&Headers> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }
}

impl From<&str> for HeaderValue {
    fn from(value: &str) -> Self {
        Self::Str(CompactString::from(value))
    }
}

impl From<String> for HeaderValue {
    fn from(value: String) -> Self {
        Self::Str(CompactString::from(value))
    }
}

impl From<CompactString> for HeaderValue {
    fn from(value: CompactString) -> Self {
        Self::Str(value)
    }
}

impl From<bool> for HeaderValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for HeaderValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for HeaderValue {
    fn from(value: i32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<u32> for HeaderValue {
    fn from(value: u32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<f64> for HeaderValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<Bytes> for HeaderValue {
    fn from(value: Bytes) -> Self {
        Self::Bytes(value)
    }
}

impl From<Vec<HeaderValue>> for HeaderValue {
    fn from(value: Vec<HeaderValue>) -> Self {
        Self::List(value)
    }
}

impl From<Headers> for HeaderValue {
    fn from(value: Headers) -> Self {
        Self::Map(value)
    }
}

/// Mappa ordinata `str -> HeaderValue` (ordine di inserimento, come `dict`).
///
/// Le mappe di header sono piccole (una decina di voci): una `Vec` ordina,
/// resta compatta in cache e costa un solo `malloc`; la ricerca lineare è più
/// veloce dell'hashing a queste dimensioni.
#[derive(Debug, Clone, Default)]
pub struct Headers {
    entries: Vec<(CompactString, HeaderValue)>,
}

impl PartialEq for Headers {
    /// Uguaglianza da mappa (come `dict`): l'ordine di inserimento non conta.
    fn eq(&self, other: &Self) -> bool {
        self.entries.len() == other.entries.len()
            && self
                .entries
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl Headers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Inserisce o sostituisce un valore. Se la chiave esiste, la posizione
    /// resta quella originale (come `dict[key] = value` in Python).
    pub fn insert(
        &mut self,
        key: impl Into<CompactString>,
        value: impl Into<HeaderValue>,
    ) -> Option<HeaderValue> {
        let key = key.into();
        let value = value.into();
        for (k, v) in &mut self.entries {
            if k == &key {
                return Some(std::mem::replace(v, value));
            }
        }
        self.entries.push((key, value));
        None
    }

    pub fn get(&self, key: &str) -> Option<&HeaderValue> {
        self.entries
            .iter()
            .find_map(|(k, v)| (k == key).then_some(v))
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut HeaderValue> {
        self.entries
            .iter_mut()
            .find_map(|(k, v)| (k == key).then_some(v))
    }

    pub fn remove(&mut self, key: &str) -> Option<HeaderValue> {
        let index = self.entries.iter().position(|(k, _)| k == key)?;
        Some(self.entries.remove(index).1)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&CompactString, &HeaderValue)> {
        self.entries.iter().map(|(k, v)| (k, v))
    }

    pub fn keys(&self) -> impl Iterator<Item = &CompactString> {
        self.entries.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &HeaderValue> {
        self.entries.iter().map(|(_, v)| v)
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(HeaderValue::as_str)
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(HeaderValue::as_i64)
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.get(key).and_then(HeaderValue::as_f64)
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(HeaderValue::as_bool)
    }
}

impl FromIterator<(CompactString, HeaderValue)> for Headers {
    fn from_iter<T: IntoIterator<Item = (CompactString, HeaderValue)>>(iter: T) -> Self {
        Self {
            entries: iter.into_iter().collect(),
        }
    }
}

impl IntoIterator for Headers {
    type Item = (CompactString, HeaderValue);
    type IntoIter = std::vec::IntoIter<(CompactString, HeaderValue)>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

/// Proprietà del messaggio (`properties` di kombu).
///
/// È la stessa mappa ordinata degli header; i metodi tipizzati coprono le
/// proprietà AMQP che Celery usa davvero.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Properties(Headers);

impl Properties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn as_headers(&self) -> &Headers {
        &self.0
    }

    pub fn into_headers(self) -> Headers {
        self.0
    }

    pub fn delivery_mode(&self) -> Option<u8> {
        self.0
            .get_i64("delivery_mode")
            .and_then(|v| u8::try_from(v).ok())
    }

    pub fn priority(&self) -> Option<u8> {
        self.0
            .get_i64("priority")
            .and_then(|v| u8::try_from(v).ok())
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.0.get_str("correlation_id")
    }

    pub fn reply_to(&self) -> Option<&str> {
        self.0.get_str("reply_to")
    }

    pub fn message_id(&self) -> Option<&str> {
        self.0.get_str("message_id")
    }

    pub fn expiration(&self) -> Option<&str> {
        self.0.get_str("expiration")
    }

    pub fn message_type(&self) -> Option<&str> {
        self.0.get_str("type")
    }

    pub fn app_id(&self) -> Option<&str> {
        self.0.get_str("app_id")
    }

    pub fn user_id(&self) -> Option<&str> {
        self.0.get_str("user_id")
    }

    pub fn timestamp(&self) -> Option<i64> {
        match self.0.get("timestamp") {
            Some(HeaderValue::Timestamp(t)) => Some(*t),
            Some(HeaderValue::Int(t)) => Some(*t),
            _ => None,
        }
    }

    pub fn set_delivery_mode(&mut self, mode: u8) {
        self.0
            .insert("delivery_mode", HeaderValue::Int(i64::from(mode)));
    }

    pub fn set_priority(&mut self, priority: u8) {
        self.0
            .insert("priority", HeaderValue::Int(i64::from(priority)));
    }

    pub fn set_correlation_id(&mut self, value: impl Into<CompactString>) {
        self.0
            .insert("correlation_id", HeaderValue::Str(value.into()));
    }

    pub fn set_reply_to(&mut self, value: impl Into<CompactString>) {
        self.0.insert("reply_to", HeaderValue::Str(value.into()));
    }
}

impl Deref for Properties {
    type Target = Headers;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Properties {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Headers> for Properties {
    fn from(value: Headers) -> Self {
        Self(value)
    }
}

impl From<Properties> for Headers {
    fn from(value: Properties) -> Self {
        value.0
    }
}

/// Tag di consegna: contatore per canale in AMQP, uuid4 nei trasporti virtuali.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DeliveryTag {
    Number(u64),
    Uuid(CompactString),
}

impl DeliveryTag {
    /// Nuovo tag per un trasporto virtuale (uuid4, come `kombu.utils.uuid`).
    pub fn uuid() -> Self {
        use std::fmt::Write as _;
        let mut buf = CompactString::new("");
        // La scrittura su una stringa non può fallire.
        let _ = write!(buf, "{}", uuid::Uuid::new_v4());
        Self::Uuid(buf)
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number(n) => Some(*n),
            Self::Uuid(_) => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Uuid(s) => Some(s),
            Self::Number(_) => None,
        }
    }
}

impl fmt::Display for DeliveryTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(n) => write!(f, "{n}"),
            Self::Uuid(s) => f.write_str(s),
        }
    }
}

impl From<u64> for DeliveryTag {
    fn from(value: u64) -> Self {
        Self::Number(value)
    }
}

impl From<&str> for DeliveryTag {
    fn from(value: &str) -> Self {
        Self::Uuid(CompactString::from(value))
    }
}

impl From<CompactString> for DeliveryTag {
    fn from(value: CompactString) -> Self {
        Self::Uuid(value)
    }
}

/// Messaggio pubblicato o consegnato.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub body: Bytes,
    pub content_type: Option<CompactString>,
    pub content_encoding: Option<CompactString>,
    pub headers: Headers,
    pub properties: Properties,
}

impl Message {
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self {
            body: body.into(),
            ..Self::default()
        }
    }

    pub fn with_content_type(mut self, content_type: impl Into<CompactString>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn with_content_encoding(mut self, content_encoding: impl Into<CompactString>) -> Self {
        self.content_encoding = Some(content_encoding.into());
        self
    }

    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_properties(mut self, properties: Properties) -> Self {
        self.properties = properties;
        self
    }
}

/// Messaggio consegnato da un consumatore.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub message: Message,
    pub delivery_tag: DeliveryTag,
    /// Exchange di pubblicazione (`""` per l'exchange anonimo).
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub redelivered: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_preserve_insertion_order() {
        let mut headers = Headers::new();
        headers.insert("z", HeaderValue::Int(1));
        headers.insert("a", HeaderValue::Bool(true));
        headers.insert("m", HeaderValue::Null);
        assert_eq!(
            headers
                .keys()
                .map(CompactString::as_str)
                .collect::<Vec<_>>(),
            vec!["z", "a", "m"]
        );
        assert_eq!(headers.get_i64("z"), Some(1));
        assert_eq!(headers.get_bool("a"), Some(true));
        assert!(headers.contains_key("m"));
        assert_eq!(headers.get_str("missing"), None);
    }

    #[test]
    fn insert_existing_key_keeps_position() {
        let mut headers = Headers::new();
        headers.insert("first", HeaderValue::Int(1));
        headers.insert("second", HeaderValue::Int(2));
        let previous = headers.insert("first", HeaderValue::Int(3));
        assert_eq!(previous, Some(HeaderValue::Int(1)));
        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers
                .keys()
                .map(CompactString::as_str)
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        assert_eq!(headers.get_i64("first"), Some(3));
    }

    #[test]
    fn remove_and_clear() {
        let mut headers = Headers::new();
        headers.insert("a", HeaderValue::Int(1));
        headers.insert("b", HeaderValue::Int(2));
        assert_eq!(headers.remove("a"), Some(HeaderValue::Int(1)));
        assert_eq!(headers.remove("a"), None);
        assert_eq!(headers.len(), 1);
        headers.clear();
        assert!(headers.is_empty());
    }

    #[test]
    fn headers_equality_ignores_insertion_order() {
        let mut first = Headers::new();
        first.insert("x", HeaderValue::Int(1));
        first.insert("y", HeaderValue::Int(2));
        let mut second = Headers::new();
        second.insert("y", HeaderValue::Int(2));
        second.insert("x", HeaderValue::Int(1));
        assert_eq!(first, second);
        second.insert("x", HeaderValue::Int(3));
        assert_ne!(first, second);
        second.insert("z", HeaderValue::Int(4));
        assert_ne!(first, second);
    }

    #[test]
    fn nested_values_roundtrip() {
        let mut inner = Headers::new();
        inner.insert("n", HeaderValue::Int(7));
        let value = HeaderValue::List(vec![
            HeaderValue::Map(inner.clone()),
            HeaderValue::Bytes(Bytes::from_static(b"\x00\xff")),
            HeaderValue::Float(0.5),
        ]);
        let mut headers = Headers::new();
        headers.insert("payload", HeaderValue::Map(inner));
        headers.insert("list", value.clone());
        assert_eq!(headers.get("list"), Some(&value));
        assert_eq!(value.as_list().map(<[HeaderValue]>::len), Some(3));
    }

    #[test]
    fn properties_typed_accessors() {
        let mut properties = Properties::new();
        properties.set_delivery_mode(2);
        properties.set_priority(5);
        properties.set_correlation_id("abc");
        properties.set_reply_to("reply");
        properties.insert("timestamp", HeaderValue::Timestamp(1_700_000_000));
        assert_eq!(properties.delivery_mode(), Some(2));
        assert_eq!(properties.priority(), Some(5));
        assert_eq!(properties.correlation_id(), Some("abc"));
        assert_eq!(properties.reply_to(), Some("reply"));
        assert_eq!(properties.timestamp(), Some(1_700_000_000));
        assert_eq!(properties.get_str("missing"), None);
    }

    #[test]
    fn delivery_tag_display() {
        assert_eq!(DeliveryTag::Number(12).to_string(), "12");
        assert_eq!(DeliveryTag::Number(12).as_u64(), Some(12));
        assert_eq!(DeliveryTag::from("tag").as_str(), Some("tag"));
        let generated = DeliveryTag::uuid();
        let text = generated.as_str().unwrap();
        assert_eq!(text.len(), 36);
        assert_eq!(text.matches('-').count(), 4);
    }

    #[test]
    fn message_builder() {
        let message = Message::new(Bytes::from_static(b"{}"))
            .with_content_type("application/json")
            .with_content_encoding("utf-8");
        assert_eq!(message.body, Bytes::from_static(b"{}"));
        assert_eq!(message.content_type.as_deref(), Some("application/json"));
        assert_eq!(message.content_encoding.as_deref(), Some("utf-8"));
        assert!(message.headers.is_empty());
        assert!(message.properties.is_empty());
    }
}
