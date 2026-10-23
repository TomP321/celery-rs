//! Trait comune dei trasporti, errori e politica di riconnessione.
//!
//! # Contratto per le implementazioni (02b Redis, 02c AMQP)
//!
//! - **ack/reject**: ogni [`Delivery`] va confermata esattamente una volta.
//!   `ack` e `reject(requeue=false)` rimuovono il tag dall'insieme in volo e
//!   sono no-op se il tag è sconosciuto (come `QoS` di kombu); il messaggio
//!   scartato non torna in coda. `reject(requeue=true)` rimette il messaggio in
//!   coda secondo la semantica del broker (memory: in fondo; Redis: `lpush`,
//!   quindi in testa; AMQP: il broker decide) e restituisce
//!   [`Error::UnknownDeliveryTag`] se il tag non esiste (kombu solleva
//!   `KeyError`). `redelivered` deve essere `true` alla riconsegna.
//! - **prefetch**: lo stream non restituisce più di `prefetch` consegne non
//!   ancora confermate; `0` significa illimitato (come `QoS.can_consume` di
//!   kombu virtual). [`ConsumerControl::set_prefetch`] si applica subito:
//!   aumentarlo risveglia lo stream, diminuirlo blocca le consegne finché gli
//!   ack non riportano le consegne in volo sotto la soglia. `add_queue` e
//!   `cancel_queue` hanno effetto immediato senza perdere i messaggi in volo.
//!   `cancel` ferma le consegne e lo stream termina con `None`; come
//!   `basic_cancel` di kombu i messaggi già consegnati restano confermabili
//!   (`ack`/`reject`) finché la connessione resta aperta. Il controllo è
//!   `Send + Sync`: può essere usato da un thread diverso da quello che polla
//!   lo stream (il worker lo fa per `basic_qos`/`add_consumer`).
//! - **ConnectionLost**: se la connessione cade in modo non recuperabile lo
//!   stream emette **esattamente una** `Err(Error::ConnectionLost(..))` e poi
//!   termina con `None`. I messaggi in volo sono persi per questo consumatore:
//!   il worker riconnette e crea un nuovo [`Consumer`]; il broker ripristina i
//!   non confermati secondo le proprie regole (Redis: visibility timeout;
//!   AMQP: requeue alla chiusura del canale). Dopo ConnectionLost le chiamate
//!   `ack`/`reject` possono fallire: il worker non deve assumere che il tag sia
//!   ancora valido sulla nuova connessione.
//! - **get** (`basic_get`): non passa dal consumatore e ignora il prefetch.
//!   Con `no_ack=false` entra nell'insieme in volo del trasporto e va
//!   confermato con `ack`/`reject`; con `no_ack=true` non va confermato.
//! - **dichiarazioni passive**: `passive=true` non crea nulla; se l'entità non
//!   esiste restituisce [`Error::NotFound`], altrimenti riporta lo stato
//!   corrente (per le code il numero di messaggi pronti). Un exchange passivo
//!   non verifica l'equivalenza dei parametri (come AMQP/kombu).
//! - **delete_exchange**: elimina l'exchange e i binding; come in
//!   `virtual.Channel.exchange_delete` elimina anche le code vuote che vi sono
//!   legate. `unbind` su un binding inesistente è un no-op.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use futures_core::Stream;

use crate::message::{Delivery, DeliveryTag, Headers, Message};
use crate::url::TransportOptions;

/// Risultato delle operazioni di trasporto.
pub type Result<T> = std::result::Result<T, Error>;

/// Future boxed: mantiene il trait object-safe (`Box<dyn Transport>`).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Stream di consegne restituito da [`Transport::consume`].
pub type DeliveryStream = Pin<Box<dyn Stream<Item = Result<Delivery>> + Send>>;

/// Errori comuni a tutti i trasporti.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("URL del broker non valido: {0}")]
    InvalidUrl(String),
    #[error("trasporto non supportato: {0}")]
    UnsupportedTransport(String),
    #[error("non implementato: {0}")]
    NotImplemented(String),
    #[error("opzione di trasporto non valida: {0}")]
    InvalidTransportOption(String),
    #[error("argomento non valido: {0}")]
    InvalidArgument(String),
    #[error("exchange non equivalente a quello già dichiarato: {0}")]
    ExchangeNotEquivalent(String),
    #[error("tipo di exchange non supportato: {0}")]
    UnsupportedExchangeType(String),
    #[error("exchange non dichiarato: {0}")]
    UnknownExchange(String),
    #[error("pattern di routing non valido: {0}")]
    InvalidPattern(String),
    #[error("errore di connessione: {0}")]
    Connection(String),
    #[error("errore del canale: {0}")]
    Channel(String),
    #[error("tag di consegna sconosciuto: {0}")]
    UnknownDeliveryTag(String),
    /// Entità dichiarata in modo passivo ma assente sul broker.
    #[error("NOT_FOUND - entità non trovata: {0}")]
    NotFound(String),
    /// Connessione persa: lo stream la emette una volta e poi termina.
    #[error("connessione al broker persa: {0}")]
    ConnectionLost(String),
    #[error("trasporto chiuso")]
    Closed,
}

/// Tipo di exchange (i quattro tipi standard AMQP 0.9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExchangeType {
    #[default]
    Direct,
    Topic,
    Fanout,
    Headers,
}

impl ExchangeType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Topic => "topic",
            Self::Fanout => "fanout",
            Self::Headers => "headers",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "direct" => Ok(Self::Direct),
            "topic" => Ok(Self::Topic),
            "fanout" => Ok(Self::Fanout),
            "headers" => Ok(Self::Headers),
            other => Err(Error::UnsupportedExchangeType(other.to_string())),
        }
    }

    /// True se il routing è risolto dal client (trasporti virtuali).
    pub fn is_virtual(self) -> bool {
        matches!(self, Self::Direct | Self::Topic | Self::Fanout)
    }
}

/// Dichiarazione di un exchange.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExchangeDecl {
    pub name: CompactString,
    pub kind: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    pub arguments: Headers,
    /// Dichiarazione passiva: verifica solo che l'exchange esista.
    pub passive: bool,
}

impl ExchangeDecl {
    pub fn new(name: impl Into<CompactString>, kind: ExchangeType) -> Self {
        Self {
            name: name.into(),
            kind,
            ..Self::default()
        }
    }
}

/// Dichiarazione di una coda. `name` assente: nome generato dal broker.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueDecl {
    pub name: Option<CompactString>,
    pub durable: bool,
    pub auto_delete: bool,
    pub exclusive: bool,
    pub arguments: Headers,
    /// Dichiarazione passiva: verifica solo che la coda esista.
    pub passive: bool,
}

impl QueueDecl {
    pub fn new(name: impl Into<CompactString>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }
}

/// Esito della dichiarazione di una coda.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueInfo {
    /// Nome effettivo (generato se non fornito).
    pub name: CompactString,
    /// Messaggi pronti (non in volo) al momento della dichiarazione.
    pub message_count: u64,
}

/// Binding coda/exchange.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueBinding {
    pub queue: CompactString,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub arguments: Headers,
}

impl QueueBinding {
    pub fn new(
        queue: impl Into<CompactString>,
        exchange: impl Into<CompactString>,
        routing_key: impl Into<CompactString>,
    ) -> Self {
        Self {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            arguments: Headers::new(),
        }
    }
}

/// Richiesta di pubblicazione.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishRequest {
    /// `None` o stringa vuota: exchange anonimo (`routing_key` è la coda).
    pub exchange: Option<CompactString>,
    pub routing_key: CompactString,
    pub message: Message,
}

impl PublishRequest {
    pub fn new(
        exchange: Option<impl Into<CompactString>>,
        routing_key: impl Into<CompactString>,
        message: Message,
    ) -> Self {
        Self {
            exchange: exchange.map(Into::into),
            routing_key: routing_key.into(),
            message,
        }
    }
}

/// Politica di riconnessione con backoff lineare limitato.
///
/// I default sono quelli di Celery per la connessione al broker:
/// `broker_connection_retry=True`, `broker_connection_max_retries=100` e gli
/// intervalli di `kombu.Connection._ensure_connection` (2s, +2s, max 30s).
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    pub enabled: bool,
    /// `None`: nessun limite (kombu con `max_retries=None`).
    pub max_retries: Option<u32>,
    pub interval_start: Duration,
    pub interval_step: Duration,
    pub interval_max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: Some(100),
            interval_start: Duration::from_secs(2),
            interval_step: Duration::from_secs(2),
            interval_max: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Vera se, dopo `retries` tentativi falliti, se ne può fare un altro.
    pub fn should_retry(&self, retries: u32) -> bool {
        if !self.enabled {
            return false;
        }
        match self.max_retries {
            Some(max) => retries < max,
            None => true,
        }
    }

    /// Attesa prima del tentativo successivo (`retries` fallimenti già avvenuti).
    pub fn delay(&self, retries: u32) -> Duration {
        let extra = self.interval_step.saturating_mul(retries);
        self.interval_start
            .saturating_add(extra)
            .min(self.interval_max)
    }
}

/// Configurazione di una connessione: opzioni del trasporto e politica di
/// riconnessione (usata dalle implementazioni che hanno un socket).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConnectConfig {
    pub transport_options: TransportOptions,
    pub retry_policy: RetryPolicy,
}

/// Controllo di un consumatore attivo.
///
/// È un trait object (`Arc<dyn ConsumerControl>`) perché il worker lo usa da
/// thread diversi dal loop che polla lo stream. Tutti i metodi hanno effetto
/// immediato sul consumatore e possono essere chiamati mentre lo stream è in
/// attesa (i waker vengono risvegliati).
pub trait ConsumerControl: Send + Sync {
    /// Cambia il prefetch a runtime (kombu `basic_qos`/`QoS.set`).
    /// `0` = illimitato. Restituisce [`Error::Closed`] se il consumatore è già
    /// stato cancellato. `increment_eventually`/`decrement_eventually` di Celery
    /// restano nel wrapper QoS del worker, che qui imposta il valore assoluto.
    fn set_prefetch<'a>(&'a self, prefetch: u16) -> BoxFuture<'a, Result<()>>;

    /// Aggiunge una coda al consumatore (Celery `add_task_queue`).
    fn add_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Smetti di consumare da una coda (Celery `cancel_task_queue`); i
    /// messaggi già consegnati restano in volo e vanno confermati.
    fn cancel_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Cancella il consumatore: lo stream termina con `None`. Come
    /// `basic_cancel` di kombu, i messaggi già consegnati restano ackabili;
    /// vengono persi alla chiusura della connessione (memory non li ripristina).
    fn cancel<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
}

/// Consumatore attivo: stream di consegne + handle di controllo.
pub struct Consumer {
    /// Consegne: fine (`None`) dopo `cancel` o dopo un `ConnectionLost`.
    pub deliveries: DeliveryStream,
    /// Handle per modificare prefetch/code o cancellare il consumatore.
    pub control: Arc<dyn ConsumerControl>,
}

impl Consumer {
    /// Consuma solo lo stream, scartando l'handle di controllo.
    pub fn into_stream(self) -> DeliveryStream {
        self.deliveries
    }
}

/// Trasporto asincrono verso un broker.
///
/// Un'istanza rappresenta una connessione logica; `consume` crea un
/// [`Consumer`] indipendente con la propria finestra di prefetch. I metodi sono
/// `BoxFuture` per permettere `Box<dyn Transport>` (usato da `connect` e dal
/// binding Python). Il contratto dettagliato è nel commento di modulo.
pub trait Transport: Send + Sync {
    /// Dichiara o verifica un exchange. Con `passive` non crea nulla e
    /// restituisce [`Error::NotFound`] se manca.
    fn declare_exchange<'a>(&'a self, declaration: &'a ExchangeDecl) -> BoxFuture<'a, Result<()>>;

    /// Dichiara o verifica una coda; `passive` non crea nulla.
    fn declare_queue<'a>(&'a self, declaration: &'a QueueDecl) -> BoxFuture<'a, Result<QueueInfo>>;

    fn bind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>>;

    /// Rimuove un binding; binding inesistente o exchange assente sono no-op.
    fn unbind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>>;

    /// Elimina l'exchange (e le code vuote che vi sono legate, come
    /// `virtual.Channel.exchange_delete`).
    fn delete_exchange<'a>(&'a self, exchange: &'a str) -> BoxFuture<'a, Result<()>>;

    fn publish<'a>(&'a self, request: PublishRequest) -> BoxFuture<'a, Result<()>>;

    fn publish_batch<'a>(&'a self, requests: Vec<PublishRequest>) -> BoxFuture<'a, Result<()>>;

    /// Crea un consumatore con prefetch iniziale (`0` = illimitato).
    ///
    /// Stream e controllo condividono lo stato; vedi il contratto di modulo
    /// per prefetch, ack/reject e ConnectionLost.
    fn consume<'a>(
        &'a self,
        queues: &'a [CompactString],
        prefetch: u16,
    ) -> BoxFuture<'a, Result<Consumer>>;

    /// `basic_get`: un messaggio dalla coda, senza passare dal consumatore.
    /// Con `no_ack=false` entra nell'insieme in volo del trasporto e va
    /// confermato con [`Transport::ack`]/[`Transport::reject`].
    fn get<'a>(&'a self, queue: &'a str, no_ack: bool) -> BoxFuture<'a, Result<Option<Delivery>>>;

    /// Conferma una consegna. No-op se il tag è sconosciuto (come il QoS di
    /// kombu), così un ack tardivo dopo un reconnect non fa cadere il worker.
    fn ack<'a>(&'a self, delivery_tag: &'a DeliveryTag) -> BoxFuture<'a, Result<()>>;

    /// Scarta (`requeue=false`, no-op se sconosciuto) o rimette in coda
    /// (`requeue=true`, [`Error::UnknownDeliveryTag`] se sconosciuto).
    fn reject<'a>(
        &'a self,
        delivery_tag: &'a DeliveryTag,
        requeue: bool,
    ) -> BoxFuture<'a, Result<()>>;

    fn purge<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>>;

    fn queue_size<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>>;

    fn delete_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>>;

    /// Chiude la connessione. Gli stream creati da questa istanza emettono una
    /// [`Error::ConnectionLost`] e poi terminano; i non confermati sono persi
    /// secondo la semantica del trasporto.
    fn close<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_defaults_match_celery() {
        let policy = RetryPolicy::default();
        assert!(policy.enabled);
        assert_eq!(policy.max_retries, Some(100));
        assert_eq!(policy.interval_start, Duration::from_secs(2));
        assert_eq!(policy.interval_step, Duration::from_secs(2));
        assert_eq!(policy.interval_max, Duration::from_secs(30));
    }

    #[test]
    fn retry_backoff_grows_and_is_clamped() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.delay(0), Duration::from_secs(2));
        assert_eq!(policy.delay(1), Duration::from_secs(4));
        assert_eq!(policy.delay(14), Duration::from_secs(30));
        assert_eq!(policy.delay(1_000), Duration::from_secs(30));
    }

    #[test]
    fn retry_policy_exhaustion() {
        let policy = RetryPolicy::default();
        assert!(policy.should_retry(0));
        assert!(policy.should_retry(99));
        assert!(!policy.should_retry(100));

        let unlimited = RetryPolicy {
            max_retries: None,
            ..RetryPolicy::default()
        };
        assert!(unlimited.should_retry(u32::MAX));

        let disabled = RetryPolicy {
            enabled: false,
            ..RetryPolicy::default()
        };
        assert!(!disabled.should_retry(0));
    }

    #[test]
    fn exchange_type_parse() {
        for (text, kind) in [
            ("direct", ExchangeType::Direct),
            ("topic", ExchangeType::Topic),
            ("fanout", ExchangeType::Fanout),
            ("headers", ExchangeType::Headers),
        ] {
            match ExchangeType::parse(text) {
                Ok(parsed) => assert_eq!(parsed, kind),
                Err(err) => panic!("{text}: {err}"),
            }
            assert_eq!(kind.as_str(), text);
        }
        assert!(matches!(
            ExchangeType::parse("x-delayed-message"),
            Err(Error::UnsupportedExchangeType(_))
        ));
    }

    #[test]
    fn declarations_builders() {
        let exchange = ExchangeDecl::new("ex", ExchangeType::Topic);
        assert_eq!(exchange.name, "ex");
        assert_eq!(exchange.kind, ExchangeType::Topic);
        assert!(!exchange.durable);
        let queue = QueueDecl::new("q");
        assert_eq!(queue.name.as_deref(), Some("q"));
        let binding = QueueBinding::new("q", "ex", "rk");
        assert_eq!(binding.routing_key, "rk");
    }
}
