# 07 — CLI: output identico, remote control client nel core Rust, avvio leggero

Fase 7 di `PLAN.md`. `python/celery/bin/` è la copia del riferimento (click) e `./compat cli`
(già in `main`) dà oggi **0 differenze su 190 comandi**: `--help` di tutti i comandi, errori d'uso
ed exit code, `call`/`result`/`purge`, `status`/`inspect`/`control` contro un worker del
riferimento, `multi show/names/get/expand`, `amqp`, `list bindings`, `graph`, `report`, `shell`.
Il comportamento quindi c'è già. Questa spec tiene quel gate a 0 e lavora sui costi, che oggi sono
**peggiori** del riferimento:

| comando (max RSS, `/usr/bin/time`) | ref | rs |
|---|---|---|
| `celery --help` | 46,0 MB | 48,5 MB |
| `celery -A app status` (nessun worker) | 55,5 MB | 59,2 MB |

## Riferimenti
- `celery/celery/bin/*.py` (`base.py`: `CeleryCommand`, `CLIContext`, `CeleryOption`, gestione
  errori ed exit code `EX_*`; `control.py`, `call.py`, `result.py`, `purge.py`, `multi.py`).
- `celery/celery/app/control.py` (`Control.broadcast`, `Inspect._request`, `ping`, `flatten_reply`,
  `Control.mailbox` con `reply_queue_ttl`/`reply_queue_expires`/`producer_pool`).
- kombu `pidbox.py` (`Mailbox._broadcast`, `_publish`, `_collect`, `get_reply_queue`,
  `reply_exchange`, `oid`, `serializer`, `accept`, `limit`, `callback`, `timeout`).
- `python/celery/app/_rs_producer.py` (3b), `python/celery/_rs_message.py`,
  `python/celery/backends/_rs_rpc.py` (4b: consumo da coda con il trasporto Rust),
  `crates/celery-transport` (Redis: pub/sub fanout `/0.celery.pidbox` e code; AMQP: fanout/direct).

## Progetto
1. **Remote control client nel core Rust.** `Control.broadcast(..., reply=True/False)` (quindi
   `inspect`, `control`, `status`, `app.control.ping()`, `revoke`, `rate_limit`, ...) pubblica sul
   mailbox e raccoglie le risposte con il trasporto Rust quando il trasporto è idoneo (stesse
   condizioni e variabile di disattivazione del producer: `CELERY_RS_PRODUCER=0` → kombu), senza
   aprire connessioni kombu né importare redis-py/py-amqp. Identici al riferimento: exchange e coda
   di risposta (nome `oid`, `reply_queue_ttl`/`expires`, auto-delete), serializer/accept, ordine e
   forma delle risposte, `limit`, `callback` chiamato a ogni risposta, `timeout` (si attende
   esattamente come kombu: fino al timeout se `limit` non è raggiunto), `destination`,
   `pattern`/`matcher`, `connection=`/`channel=` passati dall'utente (in quel caso si resta su
   kombu). Gate: `./compat wire` (scenari `control_*` del client) e `./compat cli` a 0.
   `Control.Mailbox`/`self.mailbox` restano disponibili con la stessa interfaccia (un utente può
   ancora usarli direttamente: lì si resta su kombu).
2. **Avvio leggero.** `celery --help`, `celery <cmd> --help`, errori d'uso e `celery multi`
   non devono caricare `_celery_rs` né kombu transport/redis-py/py-amqp se il riferimento non li
   carica; i comandi che parlano col broker caricano solo il trasporto Rust. Usa import pigri senza
   cambiare nomi pubblici (gli attributi dei moduli restano accessibili: `celery.bin.base`,
   `celery.app.control.Mailbox`, ecc.). Verifica con test che leggono `sys.modules` dopo ogni
   comando (lancia la CLI con `-X importtime` o un `sitecustomize` di test, non modificare
   il codice per i test).
3. **`celery call`/`purge`/`result`** già usano producer/backend Rust: verifica che `purge` usi il
   trasporto Rust (stesso conteggio e messaggio `Purged N messages from M known task queues.`) e
   che `result` non apra connessioni kombu inutili.
4. **`celery multi`** reale: `start`/`stop`/`stopwait`/`restart`/`kill` di 2 nodi con worker rs
   (`--detach`, pidfile e logfile in una dir temporanea), stessi output ed exit code del
   riferimento (stessi comandi con worker ref); `celery worker --detach` e `celery beat --detach`.
5. Niente modifiche a firme, nomi, default o testi; nessuna nuova impostazione.

## Test richiesti
- `python/tests_rs/test_cli.py` (`.venv-rs`, ogni caso confrontato con `.venv-ref`): punto 4,
  `celery events --dump` per 2 s con un worker che esegue un task (stessi tipi di evento),
  `purge` senza `-f` con risposta `no`/`yes` su stdin, `control shutdown` (ultimo caso),
  `inspect` con 2 worker (ordine e `limit`), `control` con `-d` su più nodi, broker irraggiungibile
  (stesso errore ed exit code), import presenti in `sys.modules` per `--help`, `status`, `call`.
- Test unitari Rust per la raccolta delle risposte (timeout, limit, risposte di altri `ticket`
  scartate come in kombu).

## Benchmark
Nuovo `bench/bench_cli.py` (ref vs rs, Redis e AMQP, mediana di 10 esecuzioni): tempo e max RSS
di `--help`, `call`, `result`, `purge -f`, `status` (nessun worker), `inspect ping` con 1 worker e
con `--timeout 0.2`, e di `app.control.inspect().ping()` 1000 volte in un processo (latenza p50 per
chiamata con `limit=1`, RSS finale). Obiettivi: RSS rs ≤ ref per `--help`, almeno −10% per
`status`/`inspect ping`/`call`; tempo di avvio rs ≤ ref.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressioni
./compat wire-worker --pool prefork && ./compat wire-beat              # 0 / 0
./compat cli                                                           # 0 differenze
```
Inoltre i numeri di `bench/bench_cli.py` (ref vs rs) nel messaggio finale.

## Fuori scope
Pidbox lato worker (fase 8), `celery events` con curses, `celery shell` con IPython/bpython,
`celery migrate`, `celery upgrade`, `celery logtool`.
