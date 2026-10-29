//! Broker URL parsing and transport options using kombu syntax.

use compact_str::CompactString;

use crate::message::{HeaderValue, Headers};
use crate::transport::{Error, Result};

/// Recognized transport scheme as exposed by kombu connection information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    Memory,
    Redis,
    Rediss,
    Amqp,
    Amqps,
    /// Historical AMQP alias resolved to amqp by kombu.
    PyAmqp,
}

impl TransportKind {
    pub fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "memory" => Some(Self::Memory),
            "redis" => Some(Self::Redis),
            "rediss" => Some(Self::Rediss),
            "amqp" => Some(Self::Amqp),
            "amqps" => Some(Self::Amqps),
            "pyamqp" | "librabbitmq" => Some(Self::PyAmqp),
            _ => None,
        }
    }

    /// Returns the original scheme name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Redis => "redis",
            Self::Rediss => "rediss",
            Self::Amqp => "amqp",
            Self::Amqps => "amqps",
            Self::PyAmqp => "pyamqp",
        }
    }

    /// Returns the transport name resolved as kombu does.
    pub fn resolved(self) -> &'static str {
        match self {
            Self::PyAmqp => "amqp",
            other => other.as_str(),
        }
    }

    /// Returns whether the transport uses virtual message envelopes.
    pub fn is_virtual(self) -> bool {
        matches!(self, Self::Memory | Self::Redis | Self::Rediss)
    }

    pub fn is_secure(self) -> bool {
        matches!(self, Self::Rediss | Self::Amqps)
    }

    fn default_port(self) -> Option<u16> {
        match self {
            Self::Memory => None,
            Self::Redis | Self::Rediss => Some(6379),
            Self::Amqp | Self::PyAmqp => Some(5672),
            Self::Amqps => Some(5671),
        }
    }

    fn default_user(self) -> Option<&'static str> {
        match self {
            Self::Amqp | Self::Amqps | Self::PyAmqp => Some("guest"),
            _ => None,
        }
    }

    fn default_password(self) -> Option<&'static str> {
        self.default_user()
    }
}

/// Parsed broker connection URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Url {
    /// Original lowercase scheme name.
    pub scheme: CompactString,
    pub kind: TransportKind,
    pub hostname: Option<CompactString>,
    pub port: Option<u16>,
    pub userid: Option<CompactString>,
    pub password: Option<CompactString>,
    /// Decoded path, or None when absent.
    pub virtual_host: Option<CompactString>,
    /// Decoded query parameters in URL order.
    pub query: Vec<(CompactString, CompactString)>,
}

/// Effective connection parameters including transport defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlInfo {
    pub hostname: CompactString,
    pub port: Option<u16>,
    pub userid: Option<CompactString>,
    pub password: Option<CompactString>,
    pub virtual_host: CompactString,
    pub transport: CompactString,
}

impl Url {
    pub fn parse(input: &str) -> Result<Self> {
        let (scheme, rest) = input
            .split_once("://")
            .ok_or_else(|| Error::InvalidUrl(input.to_string()))?;
        let scheme = scheme.to_lowercase();
        let kind = TransportKind::from_scheme(&scheme)
            .ok_or_else(|| Error::UnsupportedTransport(scheme.clone()))?;
        let scheme = CompactString::from(scheme);

        let rest = match rest.split_once('#') {
            Some((head, _)) => head,
            None => rest,
        };
        let (rest, query) = match rest.split_once('?') {
            Some((head, query)) => (head, parse_query(query)),
            None => (rest, Vec::new()),
        };
        let (netloc, path) = match rest.find('/') {
            Some(index) => (&rest[..index], Some(&rest[index..])),
            None => (rest, None),
        };

        let (userinfo, hostport) = match netloc.rsplit_once('@') {
            Some((userinfo, hostport)) => (Some(userinfo), hostport),
            None => (None, netloc),
        };
        let (userid, password) = match userinfo {
            Some(userinfo) => match userinfo.split_once(':') {
                Some((user, password)) => (decode(user), decode(password)),
                None => (decode(userinfo), None),
            },
            None => (None, None),
        };

        let (host_raw, port_raw) = split_host_port(hostport)?;
        let hostname = decode_without_plus(host_raw);
        let hostname = hostname.map(|host| CompactString::from(host.to_lowercase()));
        let port = match port_raw {
            Some(text) if !text.is_empty() => Some(
                text.parse::<u16>()
                    .map_err(|_| Error::InvalidUrl(format!("invalid port: {text}")))?,
            ),
            _ => None,
        };

        let virtual_host = path.map(|path| path.strip_prefix('/').unwrap_or(path));
        let virtual_host = virtual_host.and_then(decode_without_plus);
        let virtual_host = virtual_host.map(CompactString::from);

        Ok(Self {
            scheme,
            kind,
            hostname,
            port,
            userid,
            password,
            virtual_host,
            query,
        })
    }

    /// Returns the hostname, defaulting to localhost.
    pub fn hostname(&self) -> &str {
        self.hostname.as_deref().unwrap_or("localhost")
    }

    /// Returns the virtual host, defaulting to `/`.
    pub fn virtual_host(&self) -> &str {
        self.virtual_host.as_deref().unwrap_or("/")
    }

    /// Returns effective parameters including transport defaults.
    pub fn info(&self) -> UrlInfo {
        let port = self
            .port
            .filter(|port| *port != 0)
            .or_else(|| self.kind.default_port());
        UrlInfo {
            hostname: CompactString::from(self.hostname()),
            port,
            userid: self
                .userid
                .clone()
                .or_else(|| self.kind.default_user().map(CompactString::from)),
            password: self
                .password
                .clone()
                .or_else(|| self.kind.default_password().map(CompactString::from)),
            virtual_host: CompactString::from(self.virtual_host()),
            transport: CompactString::from(self.kind.resolved()),
        }
    }

    /// Returns a query parameter value.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
    }
}

// Splits host and port, accepting bracketed IPv6 addresses.
fn split_host_port(hostport: &str) -> Result<(&str, Option<&str>)> {
    if let Some(rest) = hostport.strip_prefix('[') {
        let Some((host, after)) = rest.split_once(']') else {
            return Err(Error::InvalidUrl(format!("invalid IPv6: {hostport}")));
        };
        return match after {
            "" => Ok((host, None)),
            _ => match after.strip_prefix(':') {
                Some(port) => Ok((host, Some(port))),
                None => Err(Error::InvalidUrl(format!("invalid IPv6: {hostport}"))),
            },
        };
    }
    match hostport.split_once(':') {
        Some((host, port)) => Ok((host, Some(port))),
        None => Ok((hostport, None)),
    }
}

// Percent-decodes without converting plus signs to spaces, matching urllib unquote.
fn decode_without_plus(input: &str) -> Option<String> {
    if input.is_empty() {
        return None;
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            out.push((high << 4) | low);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

// Decodes a nonempty value, matching kombu's `or None` fallback.
fn decode(input: &str) -> Option<CompactString> {
    decode_without_plus(input).map(CompactString::from)
}

// Percent-decodes with plus signs converted to spaces, matching parse_qsl.
fn decode_plus(input: &str) -> String {
    let replaced = input.replace('+', " ");
    decode_without_plus(&replaced).unwrap_or_default()
}

// Parses query pairs, discarding blank values as urllib parse_qsl does by default.
fn parse_query(query: &str) -> Vec<(CompactString, CompactString)> {
    let mut out = Vec::new();
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        out.push((
            CompactString::from(decode_plus(key)),
            CompactString::from(decode_plus(value)),
        ));
    }
    out
}

/// Redis queue ordering strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueueOrderStrategy {
    #[default]
    RoundRobin,
    Sorted,
    Priority,
}

impl QueueOrderStrategy {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "round_robin" => Ok(Self::RoundRobin),
            "sorted" => Ok(Self::Sorted),
            "priority" => Ok(Self::Priority),
            other => Err(Error::InvalidTransportOption(format!(
                "queue_order_strategy: {other:?}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::Sorted => "sorted",
            Self::Priority => "priority",
        }
    }
}

/// Kombu fanout prefix: a boolean or an explicit string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutPrefix {
    Enabled(bool),
    Custom(CompactString),
}

impl Default for FanoutPrefix {
    fn default() -> Self {
        Self::Enabled(true)
    }
}

impl FanoutPrefix {
    /// Explicit prefix configured as a string.
    pub fn custom_prefix(&self) -> Option<&str> {
        match self {
            Self::Enabled(_) => None,
            Self::Custom(prefix) => Some(prefix),
        }
    }

    pub fn is_enabled(&self) -> bool {
        match self {
            Self::Enabled(enabled) => *enabled,
            Self::Custom(_) => true,
        }
    }
}

/// Transport options with kombu 5.7 Redis defaults. Memory ignores all except
/// deadletter_queue; unrecognized keys are preserved in [`TransportOptions::extra`].
#[derive(Debug, Clone, PartialEq)]
pub struct TransportOptions {
    pub visibility_timeout: f64,
    pub sep: CompactString,
    pub priority_steps: Vec<u8>,
    pub queue_order_strategy: QueueOrderStrategy,
    pub fanout_prefix: FanoutPrefix,
    pub fanout_patterns: bool,
    pub global_keyprefix: CompactString,
    pub unacked_key: CompactString,
    pub unacked_index_key: CompactString,
    pub unacked_mutex_key: CompactString,
    pub unacked_mutex_expire: u64,
    pub health_check_interval: u64,
    pub socket_timeout: Option<f64>,
    pub polling_interval: Option<f64>,
    /// Virtual channel body encoding, defaulting to base64.
    pub body_encoding: CompactString,
    pub deadletter_queue: Option<CompactString>,
    pub extra: Headers,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            visibility_timeout: 3600.0,
            sep: CompactString::from("\u{6}\u{16}"),
            priority_steps: vec![0, 3, 6, 9],
            queue_order_strategy: QueueOrderStrategy::RoundRobin,
            fanout_prefix: FanoutPrefix::Enabled(true),
            fanout_patterns: true,
            global_keyprefix: CompactString::new(""),
            unacked_key: CompactString::from("unacked"),
            unacked_index_key: CompactString::from("unacked_index"),
            unacked_mutex_key: CompactString::from("unacked_mutex"),
            unacked_mutex_expire: 300,
            health_check_interval: 25,
            socket_timeout: None,
            polling_interval: None,
            body_encoding: CompactString::from("base64"),
            deadletter_queue: None,
            extra: Headers::new(),
        }
    }
}

impl TransportOptions {
    /// Builds options from an ordered map.
    pub fn from_headers(headers: &Headers) -> Result<Self> {
        let mut options = Self::default();
        options.apply(headers)?;
        Ok(options)
    }

    /// Applies options over the current values.
    pub fn apply(&mut self, headers: &Headers) -> Result<()> {
        for (key, value) in headers.iter() {
            self.apply_one(key, value)?;
        }
        Ok(())
    }

    fn apply_one(&mut self, key: &str, value: &HeaderValue) -> Result<()> {
        match key {
            "visibility_timeout" => self.visibility_timeout = number(key, value)?,
            "sep" => self.sep = string(key, value)?,
            "priority_steps" => {
                let list = value
                    .as_list()
                    .ok_or_else(|| invalid(key, "list of integers"))?;
                let mut steps = Vec::with_capacity(list.len());
                for item in list {
                    let number = item
                        .as_i64()
                        .ok_or_else(|| invalid(key, "list of integers"))?;
                    steps.push(
                        u8::try_from(number)
                            .map_err(|_| invalid(key, "priority outside 0..=255"))?,
                    );
                }
                self.priority_steps = steps;
            }
            "queue_order_strategy" => {
                self.queue_order_strategy = QueueOrderStrategy::parse(
                    value.as_str().ok_or_else(|| invalid(key, "string"))?,
                )?;
            }
            "fanout_prefix" => {
                self.fanout_prefix = match value {
                    HeaderValue::Bool(enabled) => FanoutPrefix::Enabled(*enabled),
                    HeaderValue::Str(prefix) => FanoutPrefix::Custom(prefix.clone()),
                    _ => return Err(invalid(key, "boolean or string")),
                };
            }
            "fanout_patterns" => {
                self.fanout_patterns = value.as_bool().ok_or_else(|| invalid(key, "boolean"))?
            }
            "global_keyprefix" => self.global_keyprefix = string(key, value)?,
            "unacked_key" => self.unacked_key = string(key, value)?,
            "unacked_index_key" => self.unacked_index_key = string(key, value)?,
            "unacked_mutex_key" => self.unacked_mutex_key = string(key, value)?,
            "unacked_mutex_expire" => {
                self.unacked_mutex_expire = integer(key, value)?;
            }
            "health_check_interval" => {
                self.health_check_interval = integer(key, value)?;
            }
            "socket_timeout" => self.socket_timeout = optional_number(key, value)?,
            "polling_interval" => self.polling_interval = optional_number(key, value)?,
            "body_encoding" => self.body_encoding = string(key, value)?,
            "deadletter_queue" => {
                self.deadletter_queue = match value {
                    HeaderValue::Null => None,
                    HeaderValue::Str(queue) => Some(queue.clone()),
                    _ => return Err(invalid(key, "string or None")),
                };
            }
            _ => {
                self.extra.insert(key, value.clone());
            }
        }
        Ok(())
    }
}

fn invalid(key: &str, expected: &str) -> Error {
    Error::InvalidTransportOption(format!("{key}: expected {expected}"))
}

fn string(key: &str, value: &HeaderValue) -> Result<CompactString> {
    value
        .as_str()
        .map(CompactString::from)
        .ok_or_else(|| invalid(key, "string"))
}

fn number(key: &str, value: &HeaderValue) -> Result<f64> {
    value.as_f64().ok_or_else(|| invalid(key, "number"))
}

fn optional_number(key: &str, value: &HeaderValue) -> Result<Option<f64>> {
    match value {
        HeaderValue::Null => Ok(None),
        other => number(key, other).map(Some),
    }
}

fn integer(key: &str, value: &HeaderValue) -> Result<u64> {
    let number = value.as_i64().ok_or_else(|| invalid(key, "integer"))?;
    u64::try_from(number).map_err(|_| invalid(key, "nonnegative integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Generated with kombu 5.7.0rc1 Connection.info().
    type UrlCase = (
        &'static str,
        &'static str,
        Option<u16>,
        Option<&'static str>,
        Option<&'static str>,
        &'static str,
        &'static str,
    );

    const URL_CASES: &[UrlCase] = &[
        ("memory://", "localhost", None, None, None, "/", "memory"),
        ("memory:///", "localhost", None, None, None, "/", "memory"),
        ("memory://myhost", "myhost", None, None, None, "/", "memory"),
        (
            "memory:///vhost",
            "localhost",
            None,
            None,
            None,
            "vhost",
            "memory",
        ),
        ("memory://HOST", "host", None, None, None, "/", "memory"),
        (
            "redis://",
            "localhost",
            Some(6379),
            None,
            None,
            "/",
            "redis",
        ),
        (
            "redis://localhost",
            "localhost",
            Some(6379),
            None,
            None,
            "/",
            "redis",
        ),
        (
            "redis://localhost:6379",
            "localhost",
            Some(6379),
            None,
            None,
            "/",
            "redis",
        ),
        (
            "redis://localhost/0",
            "localhost",
            Some(6379),
            None,
            None,
            "0",
            "redis",
        ),
        (
            "redis://:pw@localhost:6380/3",
            "localhost",
            Some(6380),
            None,
            Some("pw"),
            "3",
            "redis",
        ),
        (
            "redis://user:pw@localhost:6380/3",
            "localhost",
            Some(6380),
            Some("user"),
            Some("pw"),
            "3",
            "redis",
        ),
        (
            "redis://:pw@localhost:6380",
            "localhost",
            Some(6380),
            None,
            Some("pw"),
            "/",
            "redis",
        ),
        (
            "redis://user@localhost:6380/4",
            "localhost",
            Some(6380),
            Some("user"),
            None,
            "4",
            "redis",
        ),
        (
            "rediss://",
            "localhost",
            Some(6379),
            None,
            None,
            "/",
            "rediss",
        ),
        (
            "rediss://localhost:6387/0",
            "localhost",
            Some(6387),
            None,
            None,
            "0",
            "rediss",
        ),
        (
            "redis://127.0.0.1:6387/0",
            "127.0.0.1",
            Some(6387),
            None,
            None,
            "0",
            "redis",
        ),
        (
            "redis://localhost///",
            "localhost",
            Some(6379),
            None,
            None,
            "//",
            "redis",
        ),
        (
            "redis://localhost//",
            "localhost",
            Some(6379),
            None,
            None,
            "/",
            "redis",
        ),
        (
            "redis://h/foo/bar",
            "h",
            Some(6379),
            None,
            None,
            "foo/bar",
            "redis",
        ),
        ("redis://h/0/", "h", Some(6379), None, None, "0/", "redis"),
        ("redis://h//0", "h", Some(6379), None, None, "/0", "redis"),
        (
            "redis://h/%2Froot",
            "h",
            Some(6379),
            None,
            None,
            "/root",
            "redis",
        ),
        (
            "redis://h/%E2%82%AC",
            "h",
            Some(6379),
            None,
            None,
            "€",
            "redis",
        ),
        (
            "redis://u@v@h/vh",
            "h",
            Some(6379),
            Some("u@v"),
            None,
            "vh",
            "redis",
        ),
        (
            "redis://[::1]:6387/0",
            "::1",
            Some(6387),
            None,
            None,
            "0",
            "redis",
        ),
        (
            "redis://[fe80::1%25eth0]:6387/v",
            "fe80::1%eth0",
            Some(6387),
            None,
            None,
            "v",
            "redis",
        ),
        (
            "REDIS://LocalHost:6387/DB",
            "localhost",
            Some(6387),
            None,
            None,
            "DB",
            "redis",
        ),
        ("redis://h:0/1", "h", Some(6379), None, None, "1", "redis"),
        ("redis://h:/vh", "h", Some(6379), None, None, "vh", "redis"),
        (
            "amqp://",
            "localhost",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "amqps://",
            "localhost",
            Some(5671),
            Some("guest"),
            Some("guest"),
            "/",
            "amqps",
        ),
        (
            "amqp://guest:guest@127.0.0.1:5687//",
            "127.0.0.1",
            Some(5687),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "amqp://user:pass@host:1234/vhost",
            "host",
            Some(1234),
            Some("user"),
            Some("pass"),
            "vhost",
            "amqp",
        ),
        (
            "amqp://host/",
            "host",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "amqp://host//",
            "host",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "amqp://host/%2F",
            "host",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "amqp://host/foo%2Fbar",
            "host",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "foo/bar",
            "amqp",
        ),
        (
            "pyamqp://h//",
            "h",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "pyamqp://h:1234/v",
            "h",
            Some(1234),
            Some("guest"),
            Some("guest"),
            "v",
            "amqp",
        ),
        (
            "amqp://u:p@h",
            "h",
            Some(5672),
            Some("u"),
            Some("p"),
            "/",
            "amqp",
        ),
        (
            "amqp://h:5671/v?ssl_cert_reqs=required",
            "h",
            Some(5671),
            Some("guest"),
            Some("guest"),
            "v",
            "amqp",
        ),
        (
            "amqp://user%40dom:p%3Ass@h/vh",
            "h",
            Some(5672),
            Some("user@dom"),
            Some("p:ss"),
            "vh",
            "amqp",
        ),
        (
            "amqp://[::1]//",
            "::1",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "amqps://u:p@[::1]:5671//x",
            "::1",
            Some(5671),
            Some("u"),
            Some("p"),
            "/x",
            "amqps",
        ),
        (
            "amqp://@h//",
            "h",
            Some(5672),
            Some("guest"),
            Some("guest"),
            "/",
            "amqp",
        ),
        (
            "amqp://u:@h//",
            "h",
            Some(5672),
            Some("u"),
            Some("guest"),
            "/",
            "amqp",
        ),
    ];

    #[test]
    fn url_table_matches_kombu_info() {
        assert!(URL_CASES.len() >= 30);
        for (url, hostname, port, userid, password, vhost, transport) in URL_CASES {
            let parsed = Url::parse(url).unwrap_or_else(|err| panic!("{url}: {err}"));
            let info = parsed.info();
            assert_eq!(info.hostname, *hostname, "hostname for {url}");
            assert_eq!(info.port, *port, "port for {url}");
            assert_eq!(info.userid.as_deref(), *userid, "userid for {url}");
            assert_eq!(info.password.as_deref(), *password, "password for {url}");
            assert_eq!(info.virtual_host, *vhost, "virtual_host for {url}");
            assert_eq!(info.transport, *transport, "transport for {url}");
        }
    }

    #[test]
    fn invalid_urls_are_rejected() {
        assert!(matches!(Url::parse("localhost"), Err(Error::InvalidUrl(_))));
        assert!(matches!(
            Url::parse("sqs://host/"),
            Err(Error::UnsupportedTransport(_))
        ));
        assert!(matches!(
            Url::parse("redis://h:99999/0"),
            Err(Error::InvalidUrl(_))
        ));
        assert!(matches!(
            Url::parse("redis://[::1]x/0"),
            Err(Error::InvalidUrl(_))
        ));
    }

    #[test]
    fn query_string_is_parsed_like_python() {
        let url =
            Url::parse("amqp://h:5671/v?ssl_cert_reqs=required&empty=&flag&sp=a+b").expect("url");
        assert_eq!(url.query_param("ssl_cert_reqs"), Some("required"));
        assert_eq!(url.query_param("sp"), Some("a b"));
        assert_eq!(url.query_param("empty"), None);
        assert_eq!(url.query_param("flag"), None);
    }

    #[test]
    fn percent_decoding_never_panics_on_invalid_sequences() {
        // Invalid escapes and non-UTF-8 bytes are replaced as urllib unquote does.
        let url = Url::parse("redis://h/%aé").expect("url");
        assert_eq!(url.virtual_host(), "%aé");
        let url = Url::parse("redis://h/%ZZ").expect("url");
        assert_eq!(url.virtual_host(), "%ZZ");
        let url = Url::parse("redis://h/%").expect("url");
        assert_eq!(url.virtual_host(), "%");
    }

    #[test]
    fn transport_kind_properties() {
        assert!(TransportKind::Memory.is_virtual());
        assert!(TransportKind::Redis.is_virtual());
        assert!(!TransportKind::Amqp.is_virtual());
        assert!(TransportKind::Rediss.is_secure());
        assert_eq!(TransportKind::PyAmqp.resolved(), "amqp");
    }

    #[test]
    fn transport_options_defaults() {
        let options = TransportOptions::default();
        assert_eq!(options.visibility_timeout, 3600.0);
        assert_eq!(options.sep.as_str(), "\u{6}\u{16}");
        assert_eq!(options.priority_steps, vec![0, 3, 6, 9]);
        assert_eq!(options.queue_order_strategy, QueueOrderStrategy::RoundRobin);
        assert_eq!(options.fanout_prefix, FanoutPrefix::Enabled(true));
        assert!(options.fanout_patterns);
        assert_eq!(options.health_check_interval, 25);
        assert_eq!(options.unacked_mutex_expire, 300);
        assert!(options.socket_timeout.is_none());
        assert_eq!(options.body_encoding, "base64");
    }

    #[test]
    fn transport_options_from_headers() {
        let mut headers = Headers::new();
        headers.insert("visibility_timeout", HeaderValue::Int(30));
        headers.insert(
            "priority_steps",
            HeaderValue::List(vec![0.into(), 5.into()]),
        );
        headers.insert("fanout_prefix", HeaderValue::Bool(false));
        headers.insert("fanout_patterns", HeaderValue::Bool(false));
        headers.insert("queue_order_strategy", HeaderValue::from("sorted"));
        headers.insert("socket_timeout", HeaderValue::Float(1.5));
        headers.insert("deadletter_queue", HeaderValue::from("dead"));
        headers.insert("custom_option", HeaderValue::Int(7));

        let options = TransportOptions::from_headers(&headers).expect("opzioni");
        assert_eq!(options.visibility_timeout, 30.0);
        assert_eq!(options.priority_steps, vec![0, 5]);
        assert_eq!(options.fanout_prefix, FanoutPrefix::Enabled(false));
        assert!(!options.fanout_patterns);
        assert_eq!(options.queue_order_strategy, QueueOrderStrategy::Sorted);
        assert_eq!(options.socket_timeout, Some(1.5));
        assert_eq!(options.deadletter_queue.as_deref(), Some("dead"));
        assert_eq!(options.extra.get_i64("custom_option"), Some(7));
    }

    #[test]
    fn transport_options_reject_wrong_types() {
        let mut headers = Headers::new();
        headers.insert("priority_steps", HeaderValue::from("0,3,6,9"));
        assert!(matches!(
            TransportOptions::from_headers(&headers),
            Err(Error::InvalidTransportOption(_))
        ));

        let mut headers = Headers::new();
        headers.insert("queue_order_strategy", HeaderValue::from("shuffle"));
        assert!(matches!(
            TransportOptions::from_headers(&headers),
            Err(Error::InvalidTransportOption(_))
        ));
    }
}
