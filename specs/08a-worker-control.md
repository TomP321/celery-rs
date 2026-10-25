# 08a — Worker: pidbox, mingle e gossip sul trasporto Rust (niente kombu nel master)

Fase 8 di `PLAN.md` (ottimizzazione), prima parte. Dalla 05a il master del worker con consumer
Rust (`python/celery/worker/_rs_consumer.py`) apre ancora una connessione **kombu** pigra solo per
remote control (pidbox), mingle, gossip e la lettura degli eventi: per questo carica
`kombu.pidbox`, `kombu.connection`, il trasporto kombu e redis-py/py-amqp, e tiene vivi i loro
fd e timer sull'hub (vedi la manutenzione delle binding Redis corretta in 05c). Obiettivo: il master
non usa più kombu per parlare col broker; tutto passa dal trasporto Rust, con traffico identico.

Misura di partenza (`-P solo`, backend Redis, PSS del master idle; Celery ref vs celery-rs):

| broker | ref | rs oggi | moduli caricati dal master rs oggi |
|---|---|---|---|
| Redis | 54 MB | 50 MB | `redis`, `kombu.transport.redis`, `kombu.pidbox`, `kombu.connection` |
| AMQP | 54 MB | 41 MB | `amqp`, `kombu.transport.pyamqp`, `kombu.pidbox`, `kombu.connection` |

## Riferimenti
- `celery/celery/worker/pidbox.py` (`Pidbox`, `gPidbox`: `Node`, `listen`, `on_message`,
  `reset`, `shutdown`, `_close_channel`), `worker/consumer/control.py`, `worker/control.py`
  (`Panel`, comandi e risposte), `worker/consumer/mingle.py` (`hello`, `sync_with_node`,
  `on_clock_event`, `on_revoked_received`), `worker/consumer/gossip.py` (`Receiver`,
  `get_consumers`, `on_message`, `election`, `on_elect`, `on_node_join/leave/lost`, `clock`),
  `worker/consumer/events.py`, `worker/consumer/heart.py`, `worker/consumer/consumer.py`
  (`connection_for_read/write`, `on_close`, riconnessione).
- kombu: `pidbox.py` (`Node.listen`, `handle_message`, `dispatch`, `reply`, `_publish_reply`,
  `reply_exchange`, nomi delle code `{hostname}.{mailbox}.pidbox`, `exclusive`, `auto_delete`,
  `expires`/`message_ttl`, `oid`), `transport/redis.py` (fanout via PUBLISH/PSUBSCRIBE con
  `fanout_prefix`/`fanout_patterns`, binding con TTL e manutenzione periodica), `messaging.py`.
- Già in `main`: `crates/celery-transport/src/control.rs` e `python/celery/app/_rs_control.py`
  (07: lato client del mailbox, stesso formato dei messaggi e delle code di risposta),
  `crates/celery-worker/src/consumer.rs` (05a), `_rs_message.py`, `Waker` con fd (05c).

## Progetto
1. **Pidbox nel core Rust**: consumer della coda del nodo (`{hostname}.celery.pidbox` e binding
   fanout come kombu, su Redis e AMQP) i cui messaggi arrivano al loop dal `Waker`; il codice del
   riferimento (`Pidbox.on_message` → `Node.handle_message` → `Panel`) resta quello che decide e
   risponde; la risposta parte dal producer Rust verso `reply.celery.pidbox` con `routing_key`/
   `correlation_id`/`ticket` identici. `reset()`/`shutdown()`/riconnessione come nel riferimento.
2. **Gossip** sul trasporto Rust: coda eventi `worker.#` (nomi, `auto_delete`, `expires`,
   `message_ttl` come `Receiver`), stessi handler Python (`on_message`, election, clock,
   join/leave/lost). Gli eventi in uscita già passano dal producer Rust.
3. **Mingle** senza connessione kombu: `hello` broadcast e raccolta risposte via il client di
   controllo Rust della 07 (oggi `mingle` passa `connection=` e quindi ricade su kombu), con le
   stesse risposte e lo stesso effetto (revoche e clock sincronizzati).
4. **Manutenzione Redis**: se le binding con TTL di pidbox/gossip passano al trasporto Rust, anche
   il rinnovo periodico (`REFRESH_BINDING`/sweep, `_kombu.binding_deadlines`) va fatto da Rust con
   lo stesso traffico (gate `wire-worker` su Redis).
5. **Risultato**: con il consumer Rust attivo il master non importa `kombu.connection`,
   `kombu.pidbox`, `kombu.transport.*`, `redis`, `amqp` (salvo che li carichi il backend dei
   risultati o l'utente) e non registra fd kombu sull'hub. `CELERY_RS_WORKER=0` torna a tutto il
   riferimento. `-P solo` e `-P prefork` (pool Rust) entrambi.
6. Niente modifiche a nomi, firme, default o testi di log; nessuna nuova impostazione.

## Test richiesti
- `python/tests_rs/test_worker_control.py` (`.venv-rs`, worker rs come sottoprocesso, client e
  secondo worker del riferimento dove serve, ogni esito confrontato col worker del riferimento):
  tutti i comandi `inspect`/`control` (inclusi `revoke` con terminate, `rate_limit`,
  `time_limit`, `add_consumer`/`cancel_consumer`, `pool_grow/shrink`, `autoscale`, `shutdown`,
  `election`, `heartbeat`, `query_task`), destination/pattern, mingle con worker ref↔rs in
  entrambe le direzioni (revoche e clock sincronizzati), gossip (`on_node_join/leave/lost` con un
  worker ref che entra ed esce), riconnessione al broker (pidbox torna a rispondere), Redis e
  AMQP, `-P solo` e `-P prefork`.
- Moduli del master: un task `-P solo` (e un probe nel master per prefork, per esempio via
  `CELERY_RS_WORKER_STATE`) verifica l'assenza dei moduli elencati al punto 5.
- Test unitari Rust per il consumer pidbox/gossip (binding fanout, nomi code, TTL).

## Benchmark
`bench/bench.py --pool solo` e `--pool prefork --concurrency 4`, Redis e AMQP: PSS idle e di
picco del master/albero, throughput e latenza (non devono peggiorare), risvegli/s da idle; e
latenza di `inspect ping` verso il worker (p50 di 200 chiamate con `limit=1`). Riporta ref vs rs
prima/dopo. Obiettivo: PSS del master `-P solo` Redis ≤ 41 MB, nessun peggioramento altrove.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressioni
./compat wire-worker --pool solo && ./compat wire-worker --pool prefork   # 0 / 0 (incluso two_workers)
./compat wire-beat && ./compat cli                                      # 0 / 0
```

## Fuori scope
mimalloc e profiling generale della memoria (08b), latenze rpc/beat (08c), beat embedded.
