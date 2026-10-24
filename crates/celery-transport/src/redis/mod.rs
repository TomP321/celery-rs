//! Trasporto Redis: replica il modello dati di `kombu/transport/redis.py`.
//!
//! # Modello dati
//!
//! - coda = lista Redis; il nome dipende dalla priorità: `coda` per la
//!   priorità 0, `coda<sep><step>` per le altre (`sep` = `\x06\x16`,
//!   `priority_steps` = `[0, 3, 6, 9]`). Si pubblica con `LPUSH` e si consuma
//!   con `RPOP`/`BRPOP`, quindi il messaggio più vecchio è il primo.
//! - exchange = insieme `_kombu.binding.<exchange>` con membri
//!   `routing_key<sep>pattern<sep>queue` (più `_kombu.binding_permanent.*`);
//!   il routing direct/topic è risolto dal client leggendo quell'insieme.
//! - fanout = `PUBLISH` sui topic `/{db}.<exchange>[/<routing_key>]`, consumo
//!   con `PSUBSCRIBE`.
//! - messaggi non confermati = hash `unacked` (valore `[payload, exchange,
//!   routing_key]`) + zset `unacked_index` (score = epoch) + mutex
//!   `unacked_mutex`; ripristino dopo `visibility_timeout` e alla chiusura.
//! - envelope JSON con le stesse chiavi/ordine di kombu (vedi [`json`]).
//!
//! # Differenze volute
//!
//! - `unbind` aggiorna solo lo stato locale: come in kombu 5.7
//!   `virtual.Channel.queue_unbind` non tocca l'insieme dei binding su Redis.
//! - Il prefetch è applicato dal consumatore (kombu: `QoS.can_consume`), non
//!   dal broker.
//! - Il timeout di `BRPOP` è più corto di quello di kombu (250 ms): il cambio
//!   di code/prefetch si applica al ciclo successivo e non interrompe un
//!   `BRPOP` in volo, che perderebbe il messaggio già estratto.
//! - Le opzioni `health_check_interval`, `socket_connect_timeout` e
//!   `polling_interval` sono accettate per compatibilità con la configurazione
//!   di Celery ma non usate: Redis-rs gestisce da sé socket e riconnessione
//!   delle connessioni multiplexate.
//! - Sentinel/Cluster e credenziali dinamiche non sono supportati: il punto di
//!   estensione è [`RedisTransport::build_client`] (si può sostituire il client
//!   con uno gestito da Sentinel senza toccare il resto).

pub mod json;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ::redis::aio::{MultiplexedConnection, PubSub};
use ::redis::{AsyncConnectionConfig, IntoConnectionInfo, Value};
use compact_str::CompactString;
use futures_core::Stream;
use tokio::sync::Notify;

use crate::message::{Delivery, DeliveryTag, HeaderValue, Headers, Message};
use crate::routing::{PreparedBinding, TopicMatcher, key_to_pattern};
use crate::transport::{
    BoxFuture, ClientKind, ConnectConfig, ConnectFailure, Consumer, ConsumerControl,
    DeliveryStream, Error, ExchangeDecl, ExchangeType, PublishRequest, QueueBinding, QueueDecl,
    QueueInfo, Result, Transport,
};
use crate::url::{FanoutPrefix, QueueOrderStrategy, TransportOptions, Url};

/// Timeout di `BRPOP` (secondi). Kombu usa 1s; qui è più corto perché il
/// cambio di code/prefetch viene applicato al ciclo successivo senza
/// interrompere un BRPOP in volo (interromperlo perderebbe il messaggio).
const BRPOP_TIMEOUT: f64 = 0.25;

/// Intervallo del tick usato in attesa di messaggi non prioritari.
const TICK: Duration = Duration::from_millis(500);

/// Intervallo minimo tra due `restore_visible` per consumatore.
const RESTORE_INTERVAL: Duration = Duration::from_secs(1);

/// Chiavi di binding, come `Channel.keyprefix_*`.
const KEY_BINDING: &str = "_kombu.binding.";
const KEY_BINDING_PERMANENT: &str = "_kombu.binding_permanent.";
const KEY_BINDING_DEADLINES: &str = "_kombu.binding_deadlines";

/// TTL minimo di una tabella di binding con `x-expires` (ms), come kombu.
const MIN_BINDING_TTL: u64 = 60 * 60 * 1000;
const BINDING_EXPIRES_FACTOR: u64 = 2;
const BINDING_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Script di `Channel.REFRESH_BINDING`.
const REFRESH_BINDING: &str = r"
redis.call('SADD', KEYS[1], ARGV[1])
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call('ZADD', KEYS[2], 'GT', ms + tonumber(ARGV[3]), ARGV[2])
";

/// Script di `Channel.SWEEP_BINDINGS`.
const SWEEP_BINDINGS: &str = r"
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local overdue = redis.call(
    'ZRANGEBYSCORE', KEYS[1], '-inf', ms, 'LIMIT', 0, tonumber(ARGV[3]))
for _, exchange in ipairs(overdue) do
    if redis.call('EXISTS', ARGV[2] .. exchange) == 0 then
        redis.call('DEL', ARGV[1] .. exchange)
    end
    redis.call('ZREM', KEYS[1], exchange)
end
return #overdue
";

/// Rilascio del mutex: `DEL` solo se il token è ancora il nostro.
const RELEASE_LOCK: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
";

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn transport_error(error: ::redis::RedisError) -> Error {
    if error.is_connection_dropped() || error.is_unrecoverable_error() || error.is_timeout() {
        Error::ConnectionLost(error.to_string())
    } else {
        Error::Channel(error.to_string())
    }
}

fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64())
}

/// Formattazione di un float come `repr()` Python (usata per gli score).
fn float_string(value: f64) -> String {
    let mut out = Vec::with_capacity(24);
    celery_proto::json::write_float(&mut out, value);
    String::from_utf8(out).unwrap_or_default()
}

/// Configurazione immutabile del trasporto.
struct Config {
    client: ::redis::Client,
    options: TransportOptions,
    conn_config: AsyncConnectionConfig,
    sep: CompactString,
    priority_steps: Vec<u8>,
    fanout_prefix: CompactString,
    fanout_patterns: bool,
    keyprefix: CompactString,
    unacked_key: CompactString,
    unacked_index_key: CompactString,
    unacked_mutex_key: CompactString,
    unacked_mutex_expire: u64,
    visibility_timeout: f64,
    body_encoding: CompactString,
    queue_order_strategy: QueueOrderStrategy,
}

impl Config {
    fn key(&self, name: &str) -> String {
        let mut out = String::with_capacity(self.keyprefix.len() + name.len());
        out.push_str(&self.keyprefix);
        out.push_str(name);
        out
    }

    fn binding_key(&self, exchange: &str) -> String {
        self.key(&format!("{KEY_BINDING}{exchange}"))
    }

    fn permanent_binding_key(&self, exchange: &str) -> String {
        self.key(&format!("{KEY_BINDING_PERMANENT}{exchange}"))
    }

    fn deadlines_key(&self) -> String {
        self.key(KEY_BINDING_DEADLINES)
    }

    fn unacked_key(&self) -> String {
        self.key(&self.unacked_key)
    }

    fn unacked_index_key(&self) -> String {
        self.key(&self.unacked_index_key)
    }

    fn unacked_mutex_key(&self) -> String {
        self.key(&self.unacked_mutex_key)
    }

    /// Nome della lista per `queue` alla priorità `pri`, come `_q_for_pri`.
    fn queue_key(&self, queue: &str, pri: u8) -> String {
        let step = self.priority_step(pri);
        let mut out = String::with_capacity(self.keyprefix.len() + queue.len() + 4);
        out.push_str(&self.keyprefix);
        out.push_str(queue);
        if step != 0 {
            out.push_str(&self.sep);
            let _ = std::fmt::Write::write_fmt(&mut out, format_args!("{step}"));
        }
        out
    }

    /// `Channel.priority`: il più grande step <= `pri`.
    fn priority_step(&self, pri: u8) -> u8 {
        let index = self.priority_steps.partition_point(|step| *step <= pri);
        if index == 0 {
            self.priority_steps.last().copied().unwrap_or(0)
        } else {
            self.priority_steps[index - 1]
        }
    }

    /// Topic di `PUBLISH` per un exchange fanout.
    fn publish_topic(&self, exchange: &str, routing_key: &str) -> String {
        let mut out = String::with_capacity(
            self.keyprefix.len()
                + self.fanout_prefix.len()
                + exchange.len()
                + routing_key.len()
                + 1,
        );
        out.push_str(&self.keyprefix);
        out.push_str(&self.fanout_prefix);
        out.push_str(exchange);
        if !routing_key.is_empty() && self.fanout_patterns {
            out.push('/');
            out.push_str(routing_key);
        }
        out
    }
}

/// Exchange dichiarato su questa connessione (stato locale, come kombu).
#[derive(Debug, Clone)]
struct ExchangeInfo {
    kind: ExchangeType,
    durable: bool,
    auto_delete: bool,
    arguments: Headers,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BindingKey {
    queue: CompactString,
    exchange: CompactString,
    routing_key: CompactString,
}

#[derive(Debug, Clone)]
struct FanoutBinding {
    exchange: CompactString,
    pattern: CompactString,
}

/// Un messaggio consegnato e non ancora confermato da questo trasporto.
#[derive(Debug, Clone)]
struct InFlight {
    consumer: Option<u64>,
}

#[derive(Debug)]
struct ConsumerState {
    /// Tutte le code attive (regolari e fanout), nell'ordine di sottoscrizione.
    queues: Vec<CompactString>,
    /// Ordine di consumo delle code regolari (`queue_order_strategy`).
    order: Vec<CompactString>,
    prefetch: usize,
    in_flight: usize,
    cancelled: bool,
    notify: Arc<Notify>,
}

#[derive(Debug, Default)]
struct State {
    exchanges: HashMap<CompactString, ExchangeInfo>,
    bindings: HashSet<BindingKey>,
    fanout_queues: HashMap<CompactString, FanoutBinding>,
    queue_expires: HashMap<CompactString, u64>,
    consumers: HashMap<u64, ConsumerState>,
    in_flight: HashMap<CompactString, Vec<InFlight>>,
    next_consumer: u64,
}

/// Stato condiviso tra il trasporto, i consumatori e i controlli.
struct Shared {
    config: Config,
    /// Connessione multiplexata per comandi e publish.
    command: MultiplexedConnection,
    /// Connessione dedicata alle transazioni `WATCH`/`MULTI`/`EXEC`.
    transactions: tokio::sync::Mutex<MultiplexedConnection>,
    state: Mutex<State>,
    closed: AtomicBool,
}

impl Shared {
    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn command_conn(&self) -> MultiplexedConnection {
        self.command.clone()
    }

    fn exchange_kind(&self, exchange: &str) -> ExchangeType {
        lock(&self.state)
            .exchanges
            .get(exchange)
            .map_or(ExchangeType::Direct, |info| info.kind)
    }

    fn not_found(&self, kind: &str, name: &str) -> Error {
        Error::NotFound(format!("no {kind} '{name}'"))
    }

    /// Righe della tabella di binding, come `Channel.get_table`.
    async fn get_table(
        &self,
        exchange: &str,
    ) -> Result<Vec<(CompactString, Option<CompactString>, CompactString)>> {
        let mut conn = self.command_conn();
        let values: Vec<Vec<u8>> = ::redis::cmd("SMEMBERS")
            .arg(self.config.binding_key(exchange))
            .query_async(&mut conn)
            .await
            .map_err(transport_error)?;
        let sep = self.config.sep.as_str();
        let mut rows = Vec::with_capacity(values.len());
        for value in values {
            let text = String::from_utf8_lossy(&value);
            let mut fields = text.split(sep);
            let (Some(routing_key), Some(pattern), Some(queue), None) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            let pattern = (!pattern.is_empty()).then(|| CompactString::from(pattern));
            rows.push((
                CompactString::from(routing_key),
                pattern,
                CompactString::from(queue),
            ));
        }
        Ok(rows)
    }

    /// `Channel._lookup`: code a cui consegnare `routing_key` sull'exchange.
    async fn lookup(&self, exchange: &str, routing_key: &str) -> Result<Vec<CompactString>> {
        if exchange.is_empty() {
            let queue = if routing_key.is_empty() {
                self.config
                    .options
                    .deadletter_queue
                    .clone()
                    .unwrap_or_default()
            } else {
                CompactString::from(routing_key)
            };
            return Ok(vec![queue]);
        }
        let kind = self.exchange_kind(exchange);
        let rows = self.get_table(exchange).await?;
        let mut queues: Vec<CompactString> = Vec::with_capacity(rows.len());
        for (row_key, pattern, queue) in rows {
            let hit = match kind {
                ExchangeType::Direct => row_key == routing_key,
                ExchangeType::Topic => {
                    let matcher = match pattern {
                        Some(pattern) => TopicMatcher::new(&pattern),
                        None => TopicMatcher::new(&key_to_pattern(&row_key)),
                    };
                    matcher.is_ok_and(|matcher| matcher.is_match(routing_key))
                }
                ExchangeType::Fanout => true,
                ExchangeType::Headers => false,
            };
            if hit && !queues.contains(&queue) {
                queues.push(queue);
            }
        }
        // Come `TopicExchange.deliver`: la deadletter non riceve i messaggi
        // instradati (solo quelli senza route).
        if kind == ExchangeType::Topic
            && let Some(deadletter) = &self.config.options.deadletter_queue
        {
            queues.retain(|queue| queue != deadletter);
        }
        if queues.is_empty()
            && let Some(deadletter) = self.config.options.deadletter_queue.clone()
        {
            self.new_queue(&deadletter, &Headers::new());
            queues.push(deadletter);
        }
        Ok(queues)
    }

    /// `Channel._new_queue`: solo bookkeeping della scadenza.
    fn new_queue(&self, queue: &str, arguments: &Headers) {
        let mut state = lock(&self.state);
        match queue_expire(arguments) {
            Some(expire) => {
                state
                    .queue_expires
                    .insert(CompactString::from(queue), expire);
            }
            None => {
                state.queue_expires.remove(queue);
            }
        }
    }

    fn queue_expire(&self, queue: &str) -> Option<u64> {
        lock(&self.state).queue_expires.get(queue).copied()
    }

    /// `_size`: somma delle `LLEN` di tutte le liste di priorità.
    async fn size(&self, queue: &str) -> Result<u64> {
        let mut conn = self.command_conn();
        let mut pipe = ::redis::pipe();
        pipe.atomic();
        for step in &self.config.priority_steps {
            pipe.cmd("LLEN").arg(self.config.queue_key(queue, *step));
        }
        let sizes: Vec<i64> = pipe.query_async(&mut conn).await.map_err(transport_error)?;
        Ok(sizes
            .into_iter()
            .filter(|size| *size > 0)
            .map(|size| size as u64)
            .sum())
    }

    /// `_has_queue`: esiste almeno una lista di priorità.
    async fn has_queue(&self, queue: &str) -> Result<bool> {
        let mut conn = self.command_conn();
        let mut pipe = ::redis::pipe();
        pipe.atomic();
        for step in &self.config.priority_steps {
            pipe.cmd("EXISTS").arg(self.config.queue_key(queue, *step));
        }
        let exists: Vec<i64> = pipe.query_async(&mut conn).await.map_err(transport_error)?;
        Ok(exists.into_iter().any(|value| value > 0))
    }

    /// `_purge`: `LLEN`+`DEL` per ogni priorità, somma delle dimensioni.
    async fn purge(&self, queue: &str) -> Result<u64> {
        let mut conn = self.command_conn();
        let mut pipe = ::redis::pipe();
        pipe.atomic();
        for step in &self.config.priority_steps {
            let key = self.config.queue_key(queue, *step);
            pipe.cmd("LLEN").arg(&key);
            pipe.cmd("DEL").arg(&key);
        }
        let values: Vec<i64> = pipe.query_async(&mut conn).await.map_err(transport_error)?;
        Ok(values
            .chunks(2)
            .filter(|pair| pair.len() == 2 && pair[0] > 0)
            .map(|pair| pair[0] as u64)
            .sum())
    }

    /// `_delete`: rimuove binding e liste di una coda.
    async fn delete_queue_keys(
        &self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        pattern: Option<&str>,
    ) -> Result<()> {
        let member = binding_member(&self.config.sep, routing_key, pattern, queue);
        let mut conn = self.command_conn();
        let _: i64 = ::redis::cmd("SREM")
            .arg(self.config.binding_key(exchange))
            .arg(&member)
            .query_async(&mut conn)
            .await
            .map_err(transport_error)?;
        let _: i64 = ::redis::cmd("SREM")
            .arg(self.config.permanent_binding_key(exchange))
            .arg(&member)
            .query_async(&mut conn)
            .await
            .map_err(transport_error)?;
        let mut pipe = ::redis::pipe();
        pipe.atomic();
        for step in &self.config.priority_steps {
            pipe.cmd("DEL")
                .arg(self.config.queue_key(queue, *step))
                .ignore();
        }
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(transport_error)?;
        Ok(())
    }

    /// `_maybe_update_queues_expire`: rinnova il TTL delle liste della coda.
    async fn maybe_update_queue_expire(&self, queue: &str) {
        let Some(expire) = self.queue_expire(queue) else {
            return;
        };
        let mut conn = self.command_conn();
        let mut pipe = ::redis::pipe();
        pipe.atomic();
        for step in &self.config.priority_steps {
            pipe.cmd("PEXPIRE")
                .arg(self.config.queue_key(queue, *step))
                .arg(expire)
                .ignore();
        }
        let _ = pipe.query_async::<()>(&mut conn).await;
    }

    /// Aggiunge il messaggio all'hash `unacked` e allo zset `unacked_index`.
    async fn append_unacked(&self, env: &json::Envelope, raw: &[u8]) -> Result<()> {
        let payload = json::join_unacked(raw, &env.exchange, &env.routing_key);
        let score = float_string(now_seconds());
        let mut conn = self.command_conn();
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("ZADD")
            .arg(self.config.unacked_index_key())
            .arg(score)
            .arg(env.delivery_tag.as_str())
            .ignore()
            .cmd("HSET")
            .arg(self.config.unacked_key())
            .arg(env.delivery_tag.as_str())
            .arg(payload)
            .ignore();
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(transport_error)
    }

    /// `QoS.ack`/`reject(requeue=False)`: rimuove il tag dagli indici.
    async fn remove_unacked(&self, tag: &str) -> Result<()> {
        let mut conn = self.command_conn();
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .cmd("ZREM")
            .arg(self.config.unacked_index_key())
            .arg(tag)
            .ignore()
            .cmd("HDEL")
            .arg(self.config.unacked_key())
            .arg(tag)
            .ignore();
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(transport_error)
    }

    fn remove_local(&self, tag: &str) -> bool {
        let mut state = lock(&self.state);
        let Some(entries) = state.in_flight.get_mut(tag) else {
            return false;
        };
        let entry = entries.remove(0);
        if entries.is_empty() {
            state.in_flight.remove(tag);
        }
        if let Some(consumer_id) = entry.consumer
            && let Some(consumer) = state.consumers.get_mut(&consumer_id)
        {
            consumer.in_flight = consumer.in_flight.saturating_sub(1);
            consumer.notify.notify_one();
        }
        true
    }

    fn register_local(&self, tag: &CompactString, consumer: Option<u64>) {
        let mut state = lock(&self.state);
        state
            .in_flight
            .entry(tag.clone())
            .or_default()
            .push(InFlight { consumer });
        if let Some(consumer_id) = consumer
            && let Some(consumer_state) = state.consumers.get_mut(&consumer_id)
        {
            consumer_state.in_flight += 1;
        }
    }

    /// `_restore`/`restore_by_tag`: rimette in coda il messaggio del tag.
    async fn restore_by_tag(&self, tag: &str, leftmost: bool) -> Result<()> {
        let mut conn = self.transactions.lock().await;
        let unacked = self.config.unacked_key();
        let index = self.config.unacked_index_key();
        for _ in 0..8 {
            let mut pipe = ::redis::pipe();
            pipe.cmd("WATCH").arg(&unacked).ignore();
            pipe.query_async::<()>(&mut *conn)
                .await
                .map_err(transport_error)?;
            let payload: Option<Vec<u8>> = ::redis::cmd("HGET")
                .arg(&unacked)
                .arg(tag)
                .query_async(&mut *conn)
                .await
                .map_err(transport_error)?;

            let mut pipe = ::redis::pipe();
            pipe.atomic()
                .cmd("ZREM")
                .arg(&index)
                .arg(tag)
                .ignore()
                .cmd("HDEL")
                .arg(&unacked)
                .arg(tag)
                .ignore();
            if let Some(payload) = payload {
                let (raw, exchange, routing_key) = json::split_unacked(&payload)?;
                let restored = json::mark_redelivered(&raw)?;
                let envelope = json::decode(&restored)?;
                let priority = message_priority(&envelope.properties);
                for queue in self.lookup(&exchange, &routing_key).await? {
                    let key = self.config.queue_key(&queue, priority);
                    if leftmost {
                        pipe.cmd("LPUSH").arg(key).arg(&restored).ignore();
                    } else {
                        pipe.cmd("RPUSH").arg(key).arg(&restored).ignore();
                    }
                }
            }
            let result: Value = pipe
                .query_async(&mut *conn)
                .await
                .map_err(transport_error)?;
            match result {
                Value::Nil => continue, // EXEC annullato: riprova
                _ => return Ok(()),
            }
        }
        Err(Error::Channel(format!(
            "restore del tag {tag} annullato troppe volte"
        )))
    }

    /// `QoS.restore_visible`: ripristina i messaggi scaduti.
    async fn restore_visible(&self) -> Result<()> {
        let ceil = now_seconds() - self.config.visibility_timeout;
        let mut conn = self.command_conn();
        let tags: Vec<String> = ::redis::cmd("ZRANGEBYSCORE")
            .arg(self.config.unacked_index_key())
            .arg("-inf")
            .arg(format!("({}", float_string(ceil)))
            .query_async(&mut conn)
            .await
            .map_err(transport_error)?;
        if tags.is_empty() {
            return Ok(());
        }

        let mut token_buf = compact_str::CompactString::new("");
        use std::fmt::Write as _;
        let _ = write!(token_buf, "{}", uuid::Uuid::new_v4());
        let mut lock_conn = self.command_conn();
        let acquired: Option<String> = ::redis::cmd("SET")
            .arg(self.config.unacked_mutex_key())
            .arg(token_buf.as_str())
            .arg("NX")
            .arg("PX")
            .arg(self.config.unacked_mutex_expire.saturating_mul(1000))
            .query_async(&mut lock_conn)
            .await
            .map_err(transport_error)?;
        if acquired.is_none() {
            // Un altro processo sta ripristinando i messaggi.
            return Ok(());
        }

        let mut result = Ok(());
        for tag in &tags {
            if let Err(error) = self.restore_by_tag(tag, false).await {
                result = Err(error);
                break;
            }
        }
        let _ = ::redis::Script::new(RELEASE_LOCK)
            .key(self.config.unacked_mutex_key())
            .arg(token_buf.as_str())
            .invoke_async::<i64>(&mut lock_conn)
            .await;
        result
    }

    /// `queue_bind` (solo la parte Redis, già dentro il lock dello stato).
    async fn queue_bind(
        &self,
        exchange: &str,
        routing_key: &str,
        pattern: Option<&str>,
        queue: &str,
    ) -> Result<()> {
        let member = binding_member(&self.config.sep, routing_key, pattern, queue);
        let mut conn = self.command_conn();
        if let Some(expire) = self.queue_expire(queue) {
            let ttl = expire
                .saturating_mul(BINDING_EXPIRES_FACTOR)
                .max(MIN_BINDING_TTL);
            ::redis::Script::new(REFRESH_BINDING)
                .key(self.config.binding_key(exchange))
                .key(self.config.deadlines_key())
                .arg(&member)
                .arg(exchange)
                .arg(ttl)
                .invoke_async::<Value>(&mut conn)
                .await
                .map_err(transport_error)?;
        } else {
            let mut pipe = ::redis::pipe();
            pipe.atomic()
                .cmd("SADD")
                .arg(self.config.binding_key(exchange))
                .arg(&member)
                .ignore()
                .cmd("SADD")
                .arg(self.config.permanent_binding_key(exchange))
                .arg(&member)
                .ignore();
            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(transport_error)?;
        }
        Ok(())
    }

    /// `sweep_bindings`: elimina le tabelle di binding scadute.
    async fn sweep_bindings(&self, limit: u64) -> Result<u64> {
        let mut conn = self.command_conn();
        let prefix_binding = self.config.key(KEY_BINDING);
        let prefix_permanent = self.config.key(KEY_BINDING_PERMANENT);
        let removed: i64 = ::redis::Script::new(SWEEP_BINDINGS)
            .key(self.config.deadlines_key())
            .arg(prefix_binding)
            .arg(prefix_permanent)
            .arg(limit)
            .invoke_async(&mut conn)
            .await
            .map_err(transport_error)?;
        Ok(removed as u64)
    }

    /// `maybe_refresh_binding_expiry`: rinnova le tabelle di binding delle
    /// code con `x-expires` e fa pulizia di quelle scadute.
    async fn maintain_bindings(&self, queues: &[CompactString]) -> Result<()> {
        struct Row {
            exchange: String,
            member: String,
            ttl: u64,
        }
        let rows: Vec<Row> = {
            let state = lock(&self.state);
            let mut rows = Vec::new();
            for binding in &state.bindings {
                if !queues.contains(&binding.queue) {
                    continue;
                }
                let Some(expire) = state.queue_expires.get(&binding.queue).copied() else {
                    continue;
                };
                let kind = state
                    .exchanges
                    .get(&binding.exchange)
                    .map_or(ExchangeType::Direct, |info| info.kind);
                let pattern = PreparedBinding::new(kind, &binding.queue, &binding.routing_key)
                    .ok()
                    .and_then(|prepared| prepared.pattern);
                rows.push(Row {
                    exchange: binding.exchange.to_string(),
                    member: binding_member(
                        &self.config.sep,
                        &binding.routing_key,
                        pattern.as_deref(),
                        &binding.queue,
                    ),
                    ttl: expire
                        .saturating_mul(BINDING_EXPIRES_FACTOR)
                        .max(MIN_BINDING_TTL),
                });
            }
            rows
        };
        if rows.is_empty() {
            return Ok(());
        }
        let mut conn = self.command_conn();
        for row in rows {
            let _ = ::redis::Script::new(REFRESH_BINDING)
                .key(self.config.binding_key(&row.exchange))
                .key(self.config.deadlines_key())
                .arg(&row.member)
                .arg(&row.exchange)
                .arg(row.ttl)
                .invoke_async::<Value>(&mut conn)
                .await;
        }
        let _ = self.sweep_bindings(100).await;
        Ok(())
    }
}

fn binding_member(sep: &str, routing_key: &str, pattern: Option<&str>, queue: &str) -> String {
    let mut member =
        String::with_capacity(routing_key.len() + queue.len() + pattern.map_or(0, str::len) + 2);
    member.push_str(routing_key);
    member.push_str(sep);
    member.push_str(pattern.unwrap_or(""));
    member.push_str(sep);
    member.push_str(queue);
    member
}

fn queue_expire(arguments: &Headers) -> Option<u64> {
    match arguments.get("x-expires") {
        Some(HeaderValue::Int(value)) => u64::try_from(*value).ok(),
        Some(HeaderValue::Str(text)) => text.parse().ok(),
        Some(HeaderValue::Float(value)) => Some(*value as u64),
        _ => None,
    }
}

/// `_get_message_priority(message, reverse=False)`: priorità limitata a 0..=9.
fn message_priority(properties: &crate::message::Properties) -> u8 {
    properties.priority().unwrap_or(0).min(9)
}

/// Trasporto Redis.
pub struct RedisTransport {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for RedisTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTransport")
            .field("closed", &self.shared.closed.load(Ordering::Relaxed))
            .finish()
    }
}

impl RedisTransport {
    /// Opzioni di trasporto effettive.
    pub fn options(&self) -> &TransportOptions {
        &self.shared.config.options
    }

    /// Crea il client: punto di estensione per Sentinel/Cluster.
    fn build_client(url: &Url) -> Result<(::redis::Client, i64, AsyncConnectionConfig)> {
        let info = url.info();
        let db = parse_db(url.virtual_host())?;
        let addr = if url.kind.is_secure() {
            install_crypto_provider();
            ::redis::ConnectionAddr::TcpTls {
                host: info.hostname.to_string(),
                port: info.port.unwrap_or(6379),
                insecure: false,
                tls_params: None,
            }
        } else {
            ::redis::ConnectionAddr::Tcp(info.hostname.to_string(), info.port.unwrap_or(6379))
        };
        let mut redis_info = ::redis::RedisConnectionInfo::default().set_db(db);
        if let Some(username) = info.userid.as_deref() {
            redis_info = redis_info.set_username(username);
        }
        if let Some(password) = info.password.as_deref() {
            redis_info = redis_info.set_password(password);
        }
        let connection_info = addr
            .into_connection_info()
            .map_err(|error| Error::Connection(format!("indirizzo Redis non valido: {error}")))?
            .set_redis_settings(redis_info);
        let client = ::redis::Client::open(connection_info)
            .map_err(|error| Error::InvalidUrl(error.to_string()))?;
        Ok((client, db, AsyncConnectionConfig::new()))
    }
}

/// Installa il provider rustls per `rediss://` (una sola volta per processo).
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// `Channel._prepare_virtual_host`: il path dell'URL è il numero di database.
///
/// Come kombu si toglie **un solo** slash iniziale: `//` non è un database
/// valido (`int('/')` fallisce).
pub(crate) fn parse_db(virtual_host: &str) -> Result<i64> {
    if virtual_host.is_empty() || virtual_host == "/" {
        return Ok(0);
    }
    let text = virtual_host.strip_prefix('/').unwrap_or(virtual_host);
    text.parse::<i64>().map_err(|_| {
        Error::InvalidUrl(format!(
            "database Redis non valido: {virtual_host:?} (atteso un intero)"
        ))
    })
}

/// Crea un trasporto Redis da un URL già scomposto.
pub(crate) async fn connect(url: &Url, config: &ConnectConfig) -> Result<RedisTransport> {
    let (client, db, mut conn_config) = RedisTransport::build_client(url)?;
    // Il default di redis-rs (500 ms) è più basso del timeout di BRPOP: senza
    // `socket_timeout` si attende quanto serve, come redis-py.
    conn_config = conn_config.set_response_timeout(
        config
            .transport_options
            .socket_timeout
            .map(|timeout| Duration::from_secs_f64(timeout.max(0.001))),
    );

    let command = client
        .get_multiplexed_async_connection_with_config(&conn_config)
        .await
        .map_err(|error| {
            Error::connect(
                url,
                ClientKind::Redis,
                ConnectFailure::from_message(&error.to_string()),
            )
        })?;
    let transactions = client
        .get_multiplexed_async_connection_with_config(&conn_config)
        .await
        .map_err(|error| {
            Error::connect(
                url,
                ClientKind::Redis,
                ConnectFailure::from_message(&error.to_string()),
            )
        })?;

    let options = config.transport_options.clone();
    let body_encoding = options.body_encoding.clone();
    let visibility_timeout = options.visibility_timeout;
    let sep = options.sep.clone();
    let mut priority_steps = options.priority_steps.clone();
    if priority_steps.is_empty() {
        priority_steps = vec![0, 3, 6, 9];
    }
    priority_steps.sort_unstable();
    priority_steps.dedup();

    let fanout_prefix = match &options.fanout_prefix {
        FanoutPrefix::Enabled(false) => CompactString::new(""),
        FanoutPrefix::Enabled(true) => CompactString::from(format!("/{db}.")),
        FanoutPrefix::Custom(prefix) => {
            CompactString::from(prefix.replace("{db}", &db.to_string()))
        }
    };

    let config = Config {
        client,
        conn_config,
        queue_order_strategy: options.queue_order_strategy,
        fanout_patterns: options.fanout_patterns,
        keyprefix: options.global_keyprefix.clone(),
        unacked_key: options.unacked_key.clone(),
        unacked_index_key: options.unacked_index_key.clone(),
        unacked_mutex_key: options.unacked_mutex_key.clone(),
        unacked_mutex_expire: options.unacked_mutex_expire,
        visibility_timeout,
        body_encoding,
        options,
        sep,
        priority_steps,
        fanout_prefix,
    };
    let shared = Arc::new(Shared {
        config,
        command,
        transactions: tokio::sync::Mutex::new(transactions),
        state: Mutex::new(State::default()),
        closed: AtomicBool::new(false),
    });

    Ok(RedisTransport { shared })
}

/// Messaggio pronto per la pubblicazione.
struct Prepared {
    envelope: Vec<u8>,
    priority: u8,
}

fn prepare_message(
    config: &Config,
    message: &Message,
    exchange: &str,
    routing_key: &str,
) -> Prepared {
    let tag = DeliveryTag::uuid();
    let tag_text = tag.to_string();
    let envelope = json::encode(
        message,
        exchange,
        routing_key,
        &config.body_encoding,
        &tag_text,
    );
    Prepared {
        envelope,
        priority: message_priority(&message.properties),
    }
}

/// Effetto di una pubblicazione su Redis.
enum PublishAction {
    /// `LPUSH` sulla lista della coda (con `PEXPIRE` se ha `x-expires`).
    Lpush {
        key: String,
        payload: Vec<u8>,
        queue: CompactString,
        expire: Option<u64>,
    },
    /// `PUBLISH` sul topic di un exchange fanout.
    Publish { topic: String, payload: Vec<u8> },
}

impl RedisTransport {
    async fn enqueue(&self, requests: Vec<PublishRequest>, pipeline: bool) -> Result<()> {
        self.shared.ensure_open()?;
        let config = &self.shared.config;
        let mut actions: Vec<PublishAction> = Vec::new();
        for request in requests {
            let PublishRequest {
                exchange,
                routing_key,
                message,
            } = request;
            let exchange_name = exchange.as_deref().unwrap_or("");
            let prepared = prepare_message(config, &message, exchange_name, &routing_key);
            let kind = self.shared.exchange_kind(exchange_name);
            if !exchange_name.is_empty() && kind == ExchangeType::Fanout {
                actions.push(PublishAction::Publish {
                    topic: config.publish_topic(exchange_name, &routing_key),
                    payload: prepared.envelope,
                });
                continue;
            }
            let queues = self.shared.lookup(exchange_name, &routing_key).await?;
            for queue in queues {
                if queue.is_empty() {
                    continue;
                }
                let expire = self.shared.queue_expire(&queue);
                actions.push(PublishAction::Lpush {
                    key: config.queue_key(&queue, prepared.priority),
                    payload: prepared.envelope.clone(),
                    queue,
                    expire,
                });
            }
        }
        if actions.is_empty() {
            return Ok(());
        }
        let mut conn = self.shared.command_conn();
        if pipeline {
            let mut pipe = ::redis::pipe();
            for action in &actions {
                match action {
                    PublishAction::Publish { topic, payload } => {
                        pipe.cmd("PUBLISH").arg(topic).arg(payload).ignore();
                    }
                    PublishAction::Lpush {
                        key,
                        payload,
                        queue,
                        expire,
                    } => {
                        pipe.cmd("LPUSH").arg(key).arg(payload).ignore();
                        if let Some(expire) = expire {
                            for step in &config.priority_steps {
                                pipe.cmd("PEXPIRE")
                                    .arg(config.queue_key(queue, *step))
                                    .arg(*expire)
                                    .ignore();
                            }
                        }
                    }
                }
            }
            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(transport_error)?;
            return Ok(());
        }
        for action in &actions {
            match action {
                PublishAction::Publish { topic, payload } => {
                    ::redis::cmd("PUBLISH")
                        .arg(topic)
                        .arg(payload)
                        .query_async::<()>(&mut conn)
                        .await
                        .map_err(transport_error)?;
                }
                PublishAction::Lpush {
                    key,
                    payload,
                    queue,
                    expire,
                } => match expire {
                    // Come `_put` con `x-expires`: pipeline con `PEXPIRE`.
                    Some(expire) => {
                        let mut pipe = ::redis::pipe();
                        pipe.atomic().cmd("LPUSH").arg(key).arg(payload).ignore();
                        for step in &config.priority_steps {
                            pipe.cmd("PEXPIRE")
                                .arg(config.queue_key(queue, *step))
                                .arg(*expire)
                                .ignore();
                        }
                        pipe.query_async::<()>(&mut conn)
                            .await
                            .map_err(transport_error)?;
                    }
                    None => {
                        ::redis::cmd("LPUSH")
                            .arg(key)
                            .arg(payload)
                            .query_async::<()>(&mut conn)
                            .await
                            .map_err(transport_error)?;
                    }
                },
            }
        }
        Ok(())
    }

    async fn get_impl(&self, queue: &str, no_ack: bool) -> Result<Option<Delivery>> {
        self.shared.ensure_open()?;
        let mut conn = self.shared.command_conn();
        for step in &self.shared.config.priority_steps {
            let key = self.shared.config.queue_key(queue, *step);
            let item: Option<Vec<u8>> = ::redis::cmd("RPOP")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .map_err(transport_error)?;
            let Some(raw) = item else { continue };
            self.shared.maybe_update_queue_expire(queue).await;
            let envelope = json::decode(&raw)?;
            if !no_ack {
                self.shared.append_unacked(&envelope, &raw).await?;
                self.shared.register_local(&envelope.delivery_tag, None);
            }
            return Ok(Some(delivery_from(envelope)));
        }
        self.shared.maybe_update_queue_expire(queue).await;
        Ok(None)
    }
}

fn delivery_from(envelope: json::Envelope) -> Delivery {
    Delivery {
        message: Message {
            body: envelope.body,
            content_type: envelope.content_type,
            content_encoding: envelope.content_encoding,
            headers: envelope.headers,
            properties: envelope.properties,
        },
        delivery_tag: DeliveryTag::Uuid(envelope.delivery_tag),
        exchange: envelope.exchange,
        routing_key: envelope.routing_key,
        redelivered: envelope.redelivered,
    }
}

impl Transport for RedisTransport {
    fn declare_exchange<'a>(&'a self, declaration: &'a ExchangeDecl) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let mut state = lock(&self.shared.state);
            if declaration.passive {
                return match state.exchanges.contains_key(&declaration.name) {
                    true => Ok(()),
                    false => Err(self.shared.not_found("exchange", &declaration.name)),
                };
            }
            if let Some(previous) = state.exchanges.get(&declaration.name) {
                let equivalent = previous.kind == declaration.kind
                    && previous.durable == declaration.durable
                    && previous.auto_delete == declaration.auto_delete
                    && previous.arguments == declaration.arguments;
                if !equivalent {
                    return Err(Error::ExchangeNotEquivalent(declaration.name.to_string()));
                }
            } else {
                state.exchanges.insert(
                    declaration.name.clone(),
                    ExchangeInfo {
                        kind: declaration.kind,
                        durable: declaration.durable,
                        auto_delete: declaration.auto_delete,
                        arguments: declaration.arguments.clone(),
                    },
                );
            }
            Ok(())
        })
    }

    fn declare_queue<'a>(&'a self, declaration: &'a QueueDecl) -> BoxFuture<'a, Result<QueueInfo>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let name = declaration.name.clone().unwrap_or_else(|| {
                CompactString::from(format!("amq.gen-{}", uuid::Uuid::new_v4()))
            });
            if declaration.passive {
                if !self.shared.has_queue(&name).await? {
                    return Err(self.shared.not_found("queue", &name));
                }
            } else {
                self.shared.new_queue(&name, &declaration.arguments);
            }
            let count = self.shared.size(&name).await?;
            Ok(QueueInfo {
                name,
                message_count: count,
            })
        })
    }

    fn bind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let key = BindingKey {
                queue: binding.queue.clone(),
                exchange: binding.exchange.clone(),
                routing_key: binding.routing_key.clone(),
            };
            let (kind, prepared) = {
                let state = lock(&self.shared.state);
                let Some(info) = state.exchanges.get(&binding.exchange) else {
                    return Err(Error::UnknownExchange(binding.exchange.to_string()));
                };
                if state.bindings.contains(&key) {
                    return Ok(());
                }
                let kind = info.kind;
                let prepared = PreparedBinding::new(kind, &binding.queue, &binding.routing_key)?;
                (kind, prepared)
            };
            {
                let mut state = lock(&self.shared.state);
                state.bindings.insert(key);
                if kind == ExchangeType::Fanout {
                    state.fanout_queues.insert(
                        binding.queue.clone(),
                        FanoutBinding {
                            exchange: binding.exchange.clone(),
                            pattern: CompactString::from(binding.routing_key.replace('#', "*")),
                        },
                    );
                }
            }
            self.shared
                .queue_bind(
                    &binding.exchange,
                    &binding.routing_key,
                    prepared.pattern.as_deref(),
                    &binding.queue,
                )
                .await
        })
    }

    fn unbind<'a>(&'a self, binding: &'a QueueBinding) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            // Come `virtual.Channel.queue_unbind` sul trasporto Redis: solo
            // stato locale, l'insieme dei binding sul broker resta invariato.
            let mut state = lock(&self.shared.state);
            state.bindings.remove(&BindingKey {
                queue: binding.queue.clone(),
                exchange: binding.exchange.clone(),
                routing_key: binding.routing_key.clone(),
            });
            Ok(())
        })
    }

    fn delete_exchange<'a>(&'a self, exchange: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            {
                let state = lock(&self.shared.state);
                if !state.exchanges.contains_key(exchange) {
                    return Err(self.shared.not_found("exchange", exchange));
                }
            }
            // `virtual.Channel.exchange_delete`: elimina le code vuote legate.
            for (routing_key, pattern, queue) in self.shared.get_table(exchange).await? {
                if self.shared.size(&queue).await? != 0 {
                    continue;
                }
                self.shared
                    .delete_queue_keys(&queue, exchange, &routing_key, pattern.as_deref())
                    .await?;
            }
            let mut state = lock(&self.shared.state);
            state.exchanges.remove(exchange);
            state
                .bindings
                .retain(|binding| binding.exchange != exchange);
            Ok(())
        })
    }

    fn publish<'a>(&'a self, request: PublishRequest) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.enqueue(vec![request], false).await })
    }

    fn publish_batch<'a>(&'a self, requests: Vec<PublishRequest>) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.enqueue(requests, true).await })
    }

    fn consume<'a>(
        &'a self,
        queues: &'a [CompactString],
        prefetch: u16,
    ) -> BoxFuture<'a, Result<Consumer>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            if queues.is_empty() {
                return Err(Error::InvalidArgument(
                    "consume richiede almeno una coda".to_string(),
                ));
            }
            let mut unique: Vec<CompactString> = Vec::with_capacity(queues.len());
            for queue in queues {
                if !unique.contains(queue) {
                    unique.push(queue.clone());
                }
            }
            let id = {
                let mut state = lock(&self.shared.state);
                let id = state.next_consumer;
                state.next_consumer = id.wrapping_add(1);
                state.consumers.insert(
                    id,
                    ConsumerState {
                        order: unique.clone(),
                        queues: unique,
                        prefetch: usize::from(prefetch),
                        in_flight: 0,
                        cancelled: false,
                        notify: Arc::new(Notify::new()),
                    },
                );
                id
            };
            let conn = match self.connect_consumer().await {
                Ok(conn) => conn,
                Err(error) => {
                    lock(&self.shared.state).consumers.remove(&id);
                    return Err(error);
                }
            };
            let stream = RedisStream {
                shared: Arc::clone(&self.shared),
                id,
                conn: Some(conn),
                future: None,
                finished: false,
            };
            let control: Arc<dyn ConsumerControl> = Arc::new(RedisConsumerControl {
                shared: Arc::clone(&self.shared),
                id,
            });
            Ok(Consumer {
                deliveries: Box::pin(stream) as DeliveryStream,
                control,
            })
        })
    }

    fn get<'a>(&'a self, queue: &'a str, no_ack: bool) -> BoxFuture<'a, Result<Option<Delivery>>> {
        Box::pin(async move { self.get_impl(queue, no_ack).await })
    }

    fn ack<'a>(&'a self, delivery_tag: &'a DeliveryTag) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let tag = delivery_tag.to_string();
            self.shared.remove_local(&tag);
            self.shared.remove_unacked(&tag).await
        })
    }

    fn reject<'a>(
        &'a self,
        delivery_tag: &'a DeliveryTag,
        requeue: bool,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let tag = delivery_tag.to_string();
            let known = {
                let state = lock(&self.shared.state);
                state.in_flight.contains_key(tag.as_str())
            };
            if requeue && !known && !self.tag_in_unacked(&tag).await? {
                return Err(Error::UnknownDeliveryTag(tag));
            }
            self.shared.remove_local(&tag);
            if requeue {
                self.shared.restore_by_tag(&tag, true).await
            } else {
                self.shared.remove_unacked(&tag).await
            }
        })
    }

    fn purge<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            self.shared.purge(queue).await
        })
    }

    fn queue_size<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            self.shared.size(queue).await
        })
    }

    fn delete_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.shared.ensure_open()?;
            let rows: Vec<(CompactString, CompactString, Option<CompactString>)> = {
                let mut state = lock(&self.shared.state);
                let rows = state
                    .bindings
                    .iter()
                    .filter(|binding| binding.queue == queue)
                    .map(|binding| {
                        let kind = state
                            .exchanges
                            .get(&binding.exchange)
                            .map_or(ExchangeType::Direct, |info| info.kind);
                        let pattern =
                            PreparedBinding::new(kind, &binding.queue, &binding.routing_key)
                                .ok()
                                .and_then(|prepared| prepared.pattern);
                        (
                            binding.exchange.clone(),
                            binding.routing_key.clone(),
                            pattern,
                        )
                    })
                    .collect();
                state.bindings.retain(|binding| binding.queue != queue);
                state.fanout_queues.remove(queue);
                rows
            };
            for (exchange, routing_key, pattern) in rows {
                self.shared
                    .delete_queue_keys(queue, &exchange, &routing_key, pattern.as_deref())
                    .await?;
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if self.shared.closed.swap(true, Ordering::Relaxed) {
                return Ok(());
            }
            let tags: Vec<CompactString> = {
                let mut state = lock(&self.shared.state);
                for consumer in state.consumers.values_mut() {
                    consumer.cancelled = true;
                    consumer.notify.notify_one();
                }
                state.consumers.clear();
                let tags = state.in_flight.keys().cloned().collect();
                state.in_flight.clear();
                tags
            };
            // `QoS.restore_unacked_once`: i non confermati tornano in coda.
            for tag in tags {
                // Best-effort come `QoS.restore_unacked_once`: un errore non
                // deve impedire la chiusura.
                let _ = self.shared.restore_by_tag(&tag, false).await;
            }
            Ok(())
        })
    }
}

impl RedisTransport {
    async fn tag_in_unacked(&self, tag: &str) -> Result<bool> {
        let mut conn = self.shared.command_conn();
        let exists: bool = ::redis::cmd("HEXISTS")
            .arg(self.shared.config.unacked_key())
            .arg(tag)
            .query_async(&mut conn)
            .await
            .map_err(transport_error)?;
        Ok(exists)
    }

    async fn connect_consumer(&self) -> Result<Conn> {
        let brpop = self
            .shared
            .config
            .client
            .get_multiplexed_async_connection_with_config(&self.shared.config.conn_config)
            .await
            .map_err(|error| Error::Connection(format!("connessione Redis: {error}")))?;
        Ok(Conn {
            brpop,
            pubsub: None,
            topics: Vec::new(),
            last_restore: Instant::now(),
            last_sweep: Instant::now(),
        })
    }
}

/// Connessioni possedute da un consumatore.
struct Conn {
    brpop: MultiplexedConnection,
    pubsub: Option<PubSub>,
    topics: Vec<String>,
    last_restore: Instant,
    last_sweep: Instant,
}

impl Conn {
    /// Allinea le sottoscrizioni pub/sub ai topic richiesti.
    async fn sync_topics(&mut self, config: &Config, topics: &[String]) -> Result<()> {
        if topics == self.topics.as_slice() {
            return Ok(());
        }
        if topics.is_empty() {
            self.pubsub = None;
            self.topics.clear();
            return Ok(());
        }
        if self.pubsub.is_none() {
            self.pubsub = Some(
                config
                    .client
                    .get_async_pubsub()
                    .await
                    .map_err(|error| Error::Connection(format!("pub/sub Redis: {error}")))?,
            );
        }
        let pubsub = self
            .pubsub
            .as_mut()
            .ok_or_else(|| Error::Connection("pub/sub Redis non disponibile".to_string()))?;
        for topic in &self.topics {
            if !topics.contains(topic) {
                pubsub
                    .punsubscribe(topic.as_str())
                    .await
                    .map_err(transport_error)?;
            }
        }
        for topic in topics {
            if !self.topics.contains(topic) {
                pubsub
                    .psubscribe(topic.as_str())
                    .await
                    .map_err(transport_error)?;
            }
        }
        self.topics = topics.to_vec();
        Ok(())
    }
}

/// Snapshot dello stato di un consumatore per il prossimo poll.
struct Snapshot {
    queues: Vec<CompactString>,
    topics: Vec<String>,
    can_consume: bool,
}

enum Outcome {
    Delivery(Delivery),
    Retry,
    Cancelled,
    Lost(String),
}

/// Motivo per cui non c'è una consegna da tentare.
enum SnapshotError {
    Cancelled,
    Lost(String),
}

impl Snapshot {
    fn take(shared: &Shared, id: u64) -> std::result::Result<Self, SnapshotError> {
        if shared.closed.load(Ordering::Relaxed) {
            return Err(SnapshotError::Lost("trasporto chiuso".to_string()));
        }
        let state = lock(&shared.state);
        let Some(consumer) = state.consumers.get(&id) else {
            return Err(SnapshotError::Cancelled);
        };
        if consumer.cancelled {
            return Err(SnapshotError::Cancelled);
        }
        let can_consume = consumer.prefetch == 0 || consumer.in_flight < consumer.prefetch;
        let mut queues: Vec<CompactString> = consumer
            .order
            .iter()
            .filter(|queue| !state.fanout_queues.contains_key(*queue))
            .cloned()
            .collect();
        match shared.config.queue_order_strategy {
            QueueOrderStrategy::Sorted => queues.sort(),
            QueueOrderStrategy::RoundRobin | QueueOrderStrategy::Priority => {}
        }
        let mut topics: Vec<String> = Vec::new();
        for queue in &consumer.queues {
            if let Some(binding) = state.fanout_queues.get(queue) {
                let topic = shared
                    .config
                    .publish_topic(&binding.exchange, &binding.pattern);
                if !topics.contains(&topic) {
                    topics.push(topic);
                }
            }
        }
        topics.sort();
        Ok(Self {
            queues,
            topics,
            can_consume,
        })
    }
}

/// Sottoscrizioni fanout del consumatore, per associare un topic a una coda.
fn fanout_queue_for(shared: &Shared, id: u64, channel: &str) -> Option<CompactString> {
    let state = lock(&shared.state);
    let consumer = state.consumers.get(&id)?;
    // kombu mappa exchange -> ultima coda registrata (`_fanout_to_queue`).
    let mut found = None;
    for queue in consumer.queues.iter().rev() {
        let Some(binding) = state.fanout_queues.get(queue) else {
            continue;
        };
        let topic = shared
            .config
            .publish_topic(&binding.exchange, &binding.pattern);
        let base = shared.config.publish_topic(&binding.exchange, "");
        if topic == channel || topic_exchange(shared, channel) == topic_exchange(shared, &base) {
            found = Some(queue.clone());
            break;
        }
    }
    found
}

/// Nome dell'exchange contenuto in un topic fanout (`/{db}.<exchange>[/<rk>]`).
fn topic_exchange<'a>(shared: &Shared, topic: &'a str) -> &'a str {
    let rest = topic
        .strip_prefix(shared.config.keyprefix.as_str())
        .and_then(|rest| rest.strip_prefix(shared.config.fanout_prefix.as_str()))
        .unwrap_or(topic);
    rest.split('/').next().unwrap_or(rest)
}

/// Future che attende una consegna (o un evento) senza cancellare BRPOP.
type WaitFuture = Pin<Box<dyn Future<Output = (Conn, Outcome)> + Send>>;

/// Stream di consegne di un consumatore Redis.
struct RedisStream {
    shared: Arc<Shared>,
    id: u64,
    conn: Option<Conn>,
    future: Option<WaitFuture>,
    finished: bool,
}

impl Stream for RedisStream {
    type Item = Result<Delivery>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.finished {
                return Poll::Ready(None);
            }
            if let Some(future) = this.future.as_mut() {
                match future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((conn, outcome)) => {
                        this.conn = Some(conn);
                        this.future = None;
                        match outcome {
                            Outcome::Delivery(delivery) => {
                                return Poll::Ready(Some(Ok(delivery)));
                            }
                            Outcome::Retry => continue,
                            Outcome::Cancelled => {
                                this.finished = true;
                                return Poll::Ready(None);
                            }
                            Outcome::Lost(message) => {
                                this.finished = true;
                                return Poll::Ready(Some(Err(Error::ConnectionLost(message))));
                            }
                        }
                    }
                }
            }
            let Some(conn) = this.conn.take() else {
                this.finished = true;
                return Poll::Ready(None);
            };
            let shared = Arc::clone(&this.shared);
            let id = this.id;
            this.future = Some(Box::pin(wait_once(conn, shared, id)));
        }
    }
}

impl Drop for RedisStream {
    fn drop(&mut self) {
        let mut state = lock(&self.shared.state);
        // I messaggi in volo restano tracciati: sono ripristinati alla
        // chiusura o dopo `visibility_timeout` (kombu `do_restore = True`).
        state.consumers.remove(&self.id);
    }
}

async fn wait_once(mut conn: Conn, shared: Arc<Shared>, id: u64) -> (Conn, Outcome) {
    let snapshot = match Snapshot::take(&shared, id) {
        Ok(snapshot) => snapshot,
        Err(SnapshotError::Cancelled) => return (conn, Outcome::Cancelled),
        Err(SnapshotError::Lost(message)) => return (conn, Outcome::Lost(message)),
    };
    if let Err(error) = conn.sync_topics(&shared.config, &snapshot.topics).await {
        return (conn, Outcome::Lost(error.to_string()));
    }

    if snapshot.can_consume && !snapshot.queues.is_empty() {
        let config = &shared.config;
        let mut keys: Vec<String> =
            Vec::with_capacity(snapshot.queues.len() * config.priority_steps.len());
        for step in &config.priority_steps {
            for queue in &snapshot.queues {
                keys.push(config.queue_key(queue, *step));
            }
        }
        let mut brpop = conn.brpop.clone();
        let result: std::result::Result<Option<(String, Vec<u8>)>, ::redis::RedisError> =
            ::redis::cmd("BRPOP")
                .arg(&keys)
                .arg(BRPOP_TIMEOUT)
                .query_async(&mut brpop)
                .await;
        match result {
            Ok(Some((key, raw))) => {
                let queue = match key.rsplit_once(config.sep.as_str()) {
                    Some((queue, suffix))
                        if !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()) =>
                    {
                        CompactString::from(queue)
                    }
                    _ => CompactString::from(key.as_str()),
                };
                // Round robin: la coda appena usata va in fondo (`rotate`).
                if config.queue_order_strategy == QueueOrderStrategy::RoundRobin {
                    let mut state = lock(&shared.state);
                    if let Some(consumer) = state.consumers.get_mut(&id)
                        && let Some(index) = consumer.order.iter().position(|item| *item == queue)
                    {
                        let item = consumer.order.remove(index);
                        consumer.order.push(item);
                    }
                }
                let envelope = match json::decode(&raw) {
                    Ok(envelope) => envelope,
                    Err(error) => return (conn, Outcome::Lost(error.to_string())),
                };
                if let Err(error) = shared.append_unacked(&envelope, &raw).await {
                    return (conn, Outcome::Lost(error.to_string()));
                }
                shared.register_local(&envelope.delivery_tag, Some(id));
                shared.maybe_update_queue_expire(&queue).await;
                (conn, Outcome::Delivery(delivery_from(envelope)))
            }
            Ok(None) => {
                maybe_restore(&shared, &mut conn).await;
                (conn, Outcome::Retry)
            }
            Err(error) => (conn, Outcome::Lost(error.to_string())),
        }
    } else if snapshot.can_consume && conn.pubsub.is_some() {
        let notify = {
            let state = lock(&shared.state);
            match state.consumers.get(&id) {
                Some(consumer) => Arc::clone(&consumer.notify),
                None => return (conn, Outcome::Cancelled),
            }
        };
        let outcome = {
            let Some(pubsub) = conn.pubsub.as_mut() else {
                return (
                    conn,
                    Outcome::Lost("pub/sub Redis non disponibile".to_string()),
                );
            };
            let mut messages = pubsub.on_message();
            let mut pending = None;
            let outcome = tokio::select! {
                biased;
                _ = notify.notified() => Outcome::Retry,
                _ = tokio::time::sleep(TICK) => Outcome::Retry,
                message = std::future::poll_fn(|cx| Pin::new(&mut messages).poll_next(cx)) => {
                    match message {
                        // Il messaggio va registrato fuori dal select: qui
                        // teniamo solo il payload.
                        Some(message) => {
                            pending = Some(message);
                            Outcome::Retry
                        }
                        None => Outcome::Lost("connessione pub/sub chiusa".to_string()),
                    }
                }
            };
            match pending {
                Some(message) => match handle_pubsub(&shared, id, &message).await {
                    Ok(Some(delivery)) => Outcome::Delivery(delivery),
                    Ok(None) => outcome,
                    Err(error) => Outcome::Lost(error.to_string()),
                },
                None => outcome,
            }
        };
        if matches!(outcome, Outcome::Retry) {
            maybe_restore(&shared, &mut conn).await;
            maybe_maintain_bindings(&shared, &mut conn, id).await;
        }
        (conn, outcome)
    } else {
        let notify = {
            let state = lock(&shared.state);
            match state.consumers.get(&id) {
                Some(consumer) => Arc::clone(&consumer.notify),
                None => return (conn, Outcome::Cancelled),
            }
        };
        tokio::select! {
            biased;
            _ = notify.notified() => (conn, Outcome::Retry),
            _ = tokio::time::sleep(TICK) => {
                maybe_restore(&shared, &mut conn).await;
                maybe_maintain_bindings(&shared, &mut conn, id).await;
                (conn, Outcome::Retry)
            }
        }
    }
}

/// Refresh/sweep periodico delle tabelle di binding con `x-expires`.
async fn maybe_maintain_bindings(shared: &Shared, conn: &mut Conn, id: u64) {
    if conn.last_sweep.elapsed() < BINDING_SWEEP_INTERVAL {
        return;
    }
    conn.last_sweep = Instant::now();
    let queues: Vec<CompactString> = {
        let state = lock(&shared.state);
        state
            .consumers
            .get(&id)
            .map(|consumer| consumer.queues.clone())
            .unwrap_or_default()
    };
    if !queues.is_empty() {
        let _ = shared.maintain_bindings(&queues).await;
    }
}

async fn maybe_restore(shared: &Shared, conn: &mut Conn) {
    if conn.last_restore.elapsed() < RESTORE_INTERVAL {
        return;
    }
    conn.last_restore = Instant::now();
    // Best-effort come `MultiChannelPoller.maybe_restore_messages`.
    let _ = shared.restore_visible().await;
}

async fn handle_pubsub(
    shared: &Shared,
    id: u64,
    message: &::redis::Msg,
) -> Result<Option<Delivery>> {
    let Some(queue) = fanout_queue_for(shared, id, message.get_channel_name()) else {
        return Ok(None);
    };
    let raw = message.get_payload_bytes();
    let envelope = json::decode(raw)?;
    shared.append_unacked(&envelope, raw).await?;
    shared.register_local(&envelope.delivery_tag, Some(id));
    if shared.queue_expire(&queue).is_some() {
        shared.maybe_update_queue_expire(&queue).await;
    }
    Ok(Some(delivery_from(envelope)))
}

/// Controllo di un consumatore Redis.
struct RedisConsumerControl {
    shared: Arc<Shared>,
    id: u64,
}

impl RedisConsumerControl {
    fn with_consumer(&self, action: impl FnOnce(&mut ConsumerState)) -> Result<()> {
        let mut state = lock(&self.shared.state);
        let Some(consumer) = state.consumers.get_mut(&self.id) else {
            return Err(Error::Closed);
        };
        if consumer.cancelled {
            return Err(Error::Closed);
        }
        action(consumer);
        consumer.notify.notify_one();
        Ok(())
    }
}

impl ConsumerControl for RedisConsumerControl {
    fn set_prefetch<'a>(&'a self, prefetch: u16) -> BoxFuture<'a, Result<()>> {
        Box::pin(
            async move { self.with_consumer(|consumer| consumer.prefetch = usize::from(prefetch)) },
        )
    }

    fn add_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.with_consumer(|consumer| {
                let queue = CompactString::from(queue);
                if !consumer.queues.contains(&queue) {
                    consumer.queues.push(queue.clone());
                    consumer.order.push(queue);
                }
            })
        })
    }

    fn cancel_queue<'a>(&'a self, queue: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.with_consumer(|consumer| {
                consumer.queues.retain(|known| known != queue);
                consumer.order.retain(|known| known != queue);
            })
        })
    }

    fn cancel<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut state = lock(&self.shared.state);
            let Some(consumer) = state.consumers.get_mut(&self.id) else {
                return Ok(());
            };
            consumer.cancelled = true;
            consumer.notify.notify_one();
            Ok(())
        })
    }
}

/// URL del broker usato dai test (nessun mock).
#[cfg(test)]
fn test_url() -> String {
    std::env::var("CELERY_RS_TEST_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:6390".to_string())
}

#[cfg(test)]
mod tests;
