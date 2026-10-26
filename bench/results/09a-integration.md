# 09a — integrazione e memoria

Correzioni al core e al guscio, senza modifiche a `celery/`, ai test originali o alle dipendenze:

- Le revoche senza risposta accettano `timeout=None`: il timeout di raccolta non viene usato senza ticket.
- `RedisClient.ttl` e lo shim restituiscono i TTL Redis, inclusi -1/-2; verificati result_failure_expires e result_chord_expires già implementati.
- UNSUBSCRIBE attende la conferma nel runtime prima di close, evitando sottoscrizioni residue durante la cancellazione rientrante. Nessun comando aggiuntivo.
- Il binding AMQP ignora proprietà sconosciute e valori None prima della conversione, come py-amqp. La ripubblicazione da before_task_publish accetta il parametro Signal.

Test aggiunti: `python/tests_rs/test_integration_fixes.py` (5 casi differenziali ref/rs); test Rust per TTL e unsubscribe confermato.

## Integrazione

| Broker | Ref P/F/S/E | rs P/F/S/E dopo ripetizioni | Regressioni iniziali/finali |
|---|---|---|---|
| Redis | 356/8/13/1 | 356/8/13/1 | 7/0 |
| AMQP | 355/8/14/1 | 355/8/14/1 | 8/0 |

Le 15 regressioni iniziali passano nella verifica mirata. Il giro completo ha segnalato due ulteriori errback AMQP instabili (`test_mutable_errback_called_by_chord_from_chain[errback_old_style]` e variante `_tail`), che passano isolati senza modifiche ulteriori: 4 passed ref e rs per broker. Il report finale include questa ripetizione con provenienza esplicita nei campi `isolated_recheck` e `rs.flaky`; il risultato originale completo rimane conservato.

Errore comune di raccolta: SQLAlchemy assente. Altri servizi assenti e fallimenti comuni sono elencati nel report di integrazione, compresi Django e Azure. Nessuna esclusione aggiunta.

## Comandi ed esiti

- `tools/dev/build-package`: OK prima delle verifiche mirate.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_integration_fixes.py -q`: 5 passed. Primo tentativo con setup AMQP del nuovo test errato, corretto prima del risultato verde.
- `./compat integration -k 'test_revoked or test_chord_expires or test_failure_expires or test_pubsub_reentrant_cancel_from_finalizer or test_retry_with_unpickleable_exception or test_chord_header_id_duplicated_on_rabbitmq_msg_duplication'`: 0 regressioni / 0 errori di esecuzione su entrambi i broker.
- `tools/dev/gates`: eseguito una sola volta, 3683,1 s, exit 1. Build release, clippy, 235 test Rust, 81 test nativi, 229 tests_rs, API, wire, wire-worker solo/prefork, wire-beat e CLI verdi. Upstream: solo il fallimento noto `test_pytest_celery_marker_registration`. Integration: Redis 0 regressioni; AMQP i 2 errback instabili descritti sopra.
- `./compat integration -k test_mutable_errback_called_by_chord_from_chain`: 4 passed per venv/broker, 0 regressioni / 0 errori di esecuzione. Nessuna modifica al codice dopo il gate.
- `git diff --check`: OK.

Log: `target/09a-final-gates.log`, `target/gates/20261026-212135-4026502/`, `target/09a-targeted-integration.log`, `target/09a-errback-targeted.log`. Report completo originale: `target/09a-full-gate-integration/`; ripetizione isolata: `target/09a-isolated-recheck/`; report finale: `tools/compat/out/integration-report.md` e JSON.

## PSS prima/dopo

Confronto sequenziale sotto un solo lock di corsia, a suite terminate. Baseline: sorgenti del commit `2875522` e modulo nativo precedente, conservati in `target/09a-memory-baseline/`, con le stesse dipendenze di `.venv-rs` e bytecode precompilato. Le precedenti misure concorrenti sono archiviate in `target/09a-concurrent-samples/`.

Driver `.venv-rs/bin/python`; worker `bench/bench.py --pool prefork --concurrency 10 --task pure` con 20.000 task e 500 misure di latenza; beat `bench/bench_beat.py`, entrambi i broker. `--venv target/09a-memory-baseline --label 09a-control-before` e poi `--venv .venv-rs --label 09a-control-after`. Tutti i comandi sotto `tools/dev/with-brokers`, exit 0.

| PSS (MiB) | Prima | Dopo | Delta |
|---|---:|---:|---:|
| Worker idle c10 | 84.142 | 83.014 | -1.128 |
| Worker picco c10 | 102.035 | 101.480 | -0.555 |
| Beat redis, 3 voci | 28.266 | 28.459 | +0.193 |
| Beat redis, 1000 voci | 32.920 | 33.256 | +0.336 |
| Beat amqp, 3 voci | 28.236 | 28.334 | +0.098 |
| Beat amqp, 1000 voci | 33.264 | 33.168 | -0.096 |

Nessun incremento sopra 0,5 MB. Latenza seriale p50: 1,915 → 2,083 ms (+0,169 ms), compatibile con la nuova attesa della conferma UNSUBSCRIBE; i JSON conservano anche throughput, p99 e memoria per processo.

Aperto: instabilità dei due errback AMQP nel contesto della suite completa. La ripetizione isolata è verde; il risultato grezzo del gate resta FAIL e non è stato rilanciato. Fallimento pytest noto non indagato.
