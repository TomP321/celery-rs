# 04a — Result backend Redis con client Rust

Fase 4 di `PLAN.md`. `python/celery/backends/redis.py` resta il codice del riferimento (logica di
chord, retry, `ResultConsumer`, Sentinel): **non si riscrive la logica del backend**. Si sostituisce
il *client* redis-py con un client Rust che implementa esattamente il sottoinsieme di API redis-py
usato dal backend, così `store_result`, `get_task_meta`, `AsyncResult.get()` (pubsub), chord
counter/join passano dal core Rust senza cambiare comportamento né traffico sul server.
In parallelo un altro agente lavora su `rpc://` (spec 04b): non toccare `backends/rpc.py`.

## Riferimenti
- `python/celery/backends/redis.py`: `ResultConsumer` (pubsub, `_reconnect_pubsub`, `drain_events`,
  `_socket_readable`, `on_after_fork`), `RedisBackend` (`get/mget/set/_set/delete/incr/expire`,
  `on_chord_part_return`, `_new_chord_return`, `_restore_deps`, `set_chord_size`, `ensure`,
  `exception_safe_to_retry`, `connparams`, `_params_from_url`, `_create_client`, `client`,
  `ConnectionPool`), `SentinelBackend`.
- `python/celery/backends/base.py`: `encode`/`decode`/`decode_result`/`_get_result_meta`,
  `BaseKeyValueStoreBackend.get_many`/`_mget_to_results`.
- redis-py (in `.venv-ref`): `Redis`, `Pipeline` (transaction=True → MULTI/EXEC; comandi
  concatenabili, `execute()` → lista risultati, `__enter__/__exit__` → `reset`), `PubSub`
  (`subscribe/unsubscribe/get_message(timeout, ignore_subscribe_messages)/close/connection/
  connection_pool/on_connect/channels`), `ConnectionPool.reset`, `redis.exceptions`.
- `crates/celery-transport/src/redis/` (connessione redis crate già in uso), `crates/celery-py/src/transport.rs`
  (runtime tokio condiviso, fork safety: `runtime()`/`after_fork`), `python/celery/app/_rs_producer.py`
  (schema di fast path, contatori `stats()`, `register_at_fork`).

## Progetto
1. **Crate `celery-backend`** (`crates/celery-backend/src/redis.rs`): client Redis async
   (redis crate, multiplexed) con: GET/MGET/SET (EX)/SETEX/DEL/INCR/EXPIRE/PUBLISH/ZADD/ZCOUNT/
   ZRANGE/RPUSH/LLEN/LRANGE/… quanto usa `redis.py`; pipeline con buffer di comandi eseguito in un
   solo round trip (MULTI/EXEC quando `transaction=True`, come redis-py); pubsub su connessione
   dedicata con coda messaggi limitata e `get_message(timeout)`. Valori come `bytes` (zero copie
   inutili: `Bytes`), mai `serde_json::Value`. Test unitari + test contro Redis reale.
2. **Binding PyO3** (`crates/celery-py/src/backend_redis.rs`): `_celery_rs.RedisClient`,
   `RedisPipeline`, `RedisPubSub`; GIL rilasciato durante l'I/O; runtime condiviso di
   `transport.rs` (stesso meccanismo di fork).
3. **Shim Python** `python/celery/backends/_rs_redis.py`: `RsRedis`, `RsPipeline`, `RsPubSub`,
   `RsConnectionPool` con le stesse firme/ritorni di redis-py per i metodi usati (per esempio
   `get` → `bytes|None`, `incr` → `int`, `pipeline().execute()` → lista con i tipi di redis-py,
   `zadd(name, mapping)`, `set(name, value, ex=None)`, `pubsub(ignore_subscribe_messages=True)`,
   `get_message` → dict `{'type','pattern','channel','data'}` come redis-py). Le eccezioni sono le
   classi di **redis-py** (`redis.exceptions.ConnectionError`, `TimeoutError`, `ResponseError`, …),
   così `connection_errors`, `ensure`, `exception_safe_to_retry` e `_reconnect_pubsub` restano
   invariati. Se il backend accede a internals di redis-py non coperti (per esempio
   `_pubsub.connection.register_connect_callback`, `connection._sock` per `_socket_readable`),
   fornisci un equivalente minimo sullo shim o rendi `ResultConsumer` capace di usare lo shim con
   un ramo dedicato **solo se** è l'istanza Rust; il ramo redis-py resta identico.
4. **Attivazione**: in `RedisBackend._create_client` usa `RsRedis` quando
   `self._get_client() is redis.StrictRedis` del vero modulo (i test upstream sostituiscono
   `Backend.redis` con un finto modulo: lì si resta su quello), il backend è `RedisBackend` (non
   `SentinelBackend`), schema `redis`/`rediss`/`socket` supportati, nessun parametro di
   connessione non mappato (`connparams` sconosciuti, `credential_provider`, ssl con opzioni non
   supportate, `retry_on_timeout`… → redis-py). Disattivazione: env `CELERY_RS_BACKEND=0`
   (nessuna nuova impostazione in `app.conf`). Mappa `socket_timeout`, `socket_connect_timeout`,
   `socket_keepalive`, `health_check_interval`, `client_name`, `max_connections`, `db`,
   `password`/`username`.
5. **JSON dei risultati**: in `base.py`, `encode`/`decode` usano `_celery_rs.json_dumps`/
   `json_loads` quando il serializer è `json` e il content type è quello di kombu (stesso output,
   stesse eccezioni; `accept` rispettato). Gli altri serializer restano a kombu. Vale per tutti i
   backend (anche `cache+memory://`).
6. **Traffico identico**: la sequenza di comandi inviata a Redis deve coincidere con redis-py
   (stesso ordine, MULTI/EXEC dove redis-py li usa). Handshake di connessione: redis-py invia
   `CLIENT SETINFO`/`HELLO` ecc.; se `./compat wire` segnala differenze **solo** nell'handshake,
   fermati e riportale nel messaggio finale invece di imitare il nome della libreria.
7. Fork safety: connessioni scartate nel figlio (`register_at_fork` + `on_after_fork` già nel
   backend), runtime ricreato lazy. Niente modifiche a firme, nomi o default pubblici.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings && cargo test
tools/dev/build-package
tools/dev/build-native
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
.venv-rs/bin/python -m pytest python/tests_rs -q
./compat api                      # 0 differenze
./compat wire                     # 0 scenari diversi
./compat upstream --venv .venv-rs # 0 regressioni
```
Inoltre `python/tests_rs/test_backend_redis.py` (`.venv-rs`, Redis reale su 6390):
- il backend usa davvero `RsRedis` (contatore/flag) negli scenari reali, e redis-py quando
  `CELERY_RS_BACKEND=0` o con Sentinel;
- differenziale ref vs rs: stessi valori in Redis (`celery-task-meta-*`, chord `.j`/`.t`/`.s`) e
  stessa sequenza di comandi (MONITOR) per `store_result`, `get`, `forget`, group/chord con worker
  reale del riferimento (`.venv-ref` `celery worker -P solo`) e client rs, e viceversa;
- `AsyncResult.get(timeout)` via pubsub, `propagate`, `disable_sync_subtasks`, `ResultSet.join`
  e `join_native`, `iter_native`, timeout → stessa `TimeoutError`;
- connessione killata (`CLIENT KILL`) durante `get()` → riconnessione come redis-py;
- fork (os.fork e billiard) e 16 thread concorrenti su `store_result`/`get`.
- Benchmark nuovo `bench/bench_result.py`: latenza `apply_async().get()` p50/p99, throughput
  `get_many`/`join` su 1000 risultati, RSS client; `.venv-ref` vs `.venv-rs` con worker del
  riferimento. Riporta i numeri.

## Fuori scope
`rpc://` (spec 04b), Sentinel, cluster, altri backend (database, cache memcached, …), il worker.
