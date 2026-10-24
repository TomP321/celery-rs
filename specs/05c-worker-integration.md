# 05c — Worker: integrazione `-P prefork` + parità del traffico sul broker

Fase 5 di `PLAN.md`. Sono già in `main`: consumer e loop Rust per `-P solo` (05a,
`python/celery/worker/_rs_consumer.py`, `crates/celery-worker/src/consumer.rs`) e pool Rust con
zygote (05b, `python/celery/concurrency/rs_prefork.py`, `_rs_zygote.py`, `_rs_child.py`,
`crates/celery-worker/src/pool/`). Questa spec li unisce: `celery worker` con il pool di default
(`prefork`) usa consumer, loop **e** pool Rust, con comportamento e traffico sul broker identici al
riferimento. È l'ultima parte della fase 5 prima dell'ottimizzazione.

## Nuovo strumento di verifica (già in `main`)
`./compat wire-worker [--pool solo|prefork] [--broker redis amqp] [-s scenario]`: avvia lo stesso
scenario con il worker del riferimento e con quello di celery-rs (client sempre del riferimento),
cattura il traffico sul broker e confronta risultati, eventi, ack/riconsegne, dichiarazioni e
broadcast (esclusi polling e heartbeat). È stabile ref-vs-ref. Report in
`tools/compat/out/wire-worker-report.md`. Oggi (main):
- `--pool solo`, AMQP: 0 differenze su 10 scenari;
- `--pool solo`, Redis: 10 scenari diversi, **tutti per la manutenzione periodica di kombu sulla
  connessione kombu usata da pidbox/gossip** (rinnovo delle binding con TTL e pulizia:
  `EVALSHA ef7a…`/`ZADD _kombu.binding_deadlines GT`/`SADD _kombu.binding.*`, `EVALSHA 9a2b…`,
  `TIME`): nel worker rs girano meno volte. Vedi `kombu/transport/redis.py`:
  `MultiChannelPoller.maybe_refresh_binding_expiry`, `Transport.register_with_event_loop`
  (timer `call_repeatedly(1, ...)`), `BINDING_SWEEP_INTERVAL`, `MIN_BINDING_TTL`. Senza rinnovo,
  dopo `MIN_BINDING_TTL` (1 h) un altro worker può spazzare via la binding del pidbox e il
  remote control smette di funzionare: è un bug da correggere, non da normalizzare.

## Progetto
1. **Manutenzione kombu**: trova perché i timer periodici registrati dal trasporto kombu
   sull'hub non girano come in `asynloop` (frequenza e numero di esecuzioni) e correggi
   `rsloop`. Gate: `./compat wire-worker --pool solo` → 0 differenze su Redis e AMQP. Aggiungi un
   test che accelera i tempi (per esempio monkeypatch di `BINDING_SWEEP_INTERVAL` e
   `MIN_BINDING_TTL` nel worker di test) e verifica che il rinnovo avvenga ripetutamente.
2. **Risveglio senza polling**: oggi `rsloop` drena l'hub ogni 0,25 s quando non ha timer, quindi i
   comandi di controllo hanno fino a 250 ms di ritardo e il worker idle si sveglia 4 volte al
   secondo. Aggiungi al `Waker` un fd (eventfd su Linux, pipe altrove) leggibile quando notificato
   (`Waker.fileno()`, senza cambiare `notify`/`wait`) e fai dormire `rsloop` nel poller dell'hub
   (epoll) con il fd del Waker registrato e timeout = prossimo timer: zero ritardo per
   pidbox/gossip, nessun risveglio inutile. Misura i risvegli/s del worker idle (prima/dopo).
3. **`-P prefork` → pool Rust**: quando il consumer Rust è attivo (stesse condizioni di 05a, ora
   anche con pool `prefork`), `celery worker` usa `RsTaskPool` al posto di
   `concurrency.prefork.TaskPool`; gli eventi del pool arrivano nel loop dal `Waker`. Il pool Rust
   si usa **solo** insieme al consumer Rust (mai con `asynloop` di kombu); `CELERY_RS_WORKER=0`
   torna a tutto il riferimento. Restano identici: `--concurrency`, `--autoscale` (Autoscaler con
   `grow/shrink`), `--max-tasks-per-child`, `--max-memory-per-child`, `--time-limit`/
   `--soft-time-limit`, `revoke(terminate=True, signal=...)`, `inspect stats` (`pool` con le
   stesse chiavi), `worker_process_init`/`worker_process_shutdown`, `worker_ready`,
   `worker_shutting_down`, shutdown warm/cold/soft con task in corso (stessi esiti di ack/requeue,
   `worker_cancel_long_running_tasks_on_connection_loss`), riconnessione al broker con task in
   esecuzione nei figli. I test upstream `t/unit/concurrency` e `t/unit/worker` devono continuare a
   esercitare il codice del riferimento (billiard), come oggi.
4. **Zygote perso**: oggi il pool resta senza figli. Comportamento richiesto: log `critical`
   chiaro e shutdown del worker con exit code ≠ 0 (così systemd/supervisor lo riavviano), job in
   volo con `WorkerLostError` e messaggi riconsegnati secondo `acks_late`/`reject_on_worker_lost`
   come nel riferimento. Test con `os.kill(zygote_pid, SIGKILL)`.
5. Niente modifiche a firme, nomi o default pubblici; nessuna nuova impostazione in `app.conf`.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressioni
./compat wire-worker --pool solo                                       # 0 differenze
./compat wire-worker --pool prefork                                    # 0 differenze
```
Inoltre `python/tests_rs/test_worker_prefork.py` (`.venv-rs`), worker rs `celery worker` (pool di
default, `-c 4`) come sottoprocesso con client del riferimento: gli stessi scenari di
`test_worker_consumer.py` ripetuti con prefork, più time limit, `--max-tasks-per-child`,
`--max-memory-per-child`, `--autoscale=4,1`, revoke con terminate, figlio ucciso durante un task,
zygote ucciso, shutdown warm/cold con task lunghi nei figli, connessione al broker persa con task
in corso. Ogni scenario confrontato con il worker del riferimento (stessi esiti).
- Benchmark `bench/bench.py --pool prefork --concurrency 4` per `.venv-ref` e `.venv-rs`, Redis e
  AMQP: throughput end-to-end, latenza p50/p99, **PSS dell'albero del worker idle e di picco**
  (baseline in `PLAN.md`: Redis 146/151 MB, AMQP 145/149 MB), risvegli/s da idle. Riporta i numeri.

## Fuori scope
Pidbox/mingle/gossip in Rust, ottimizzazione della memoria (fase 8), beat, `-P threads`,
eventlet/gevent.
