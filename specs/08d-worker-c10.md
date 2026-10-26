# 08d — Worker prefork con concurrency 10: minima RAM, massime prestazioni

Obiettivo del proprietario del progetto: **un worker `-P prefork -c 10` che usi meno RAM possibile
rispetto al Celery originale e performi il meglio possibile**. È la metrica principale del
progetto: ogni scelta di questa spec si giudica sul risparmio di RAM (MB e %) e sul throughput
rispetto al riferimento con 10 processi.

Parti dal branch `agent/08b-memory` (lavoro 08b non ancora unito in `main`). Leggi `AGENTS.md`,
`PLAN.md`, `specs/08b-memory.md`, `specs/08b-review.md`, `specs/08b-review2.md`.
Vincoli invariati: API e comportamento identici al riferimento; **oggetti kombu/billiard veri,
nessun `__module__`/`__qualname__`/`__name__` riscritto, nessuna classe sostitutiva**
(`python/tests_rs/test_kombu_identity.py` non si modifica); `celery/`, `celery/t/`, `tools/compat/`
non si modificano; test con broker sempre sotto `tools/dev/with-brokers`.

## Misura (fallo per primo e alla fine)
`bench/bench.py --pool prefork --concurrency 10 --n 30000` per `.venv-ref` e `.venv-rs`, Redis e
AMQP (`BENCH_BROKER=amqp://guest:guest@127.0.0.1:5673//`), con `--task bench` e `--task pure`.
Riporta in tabella, ref vs rs, con **risparmio in MB e %**:
- PSS totale dell'albero: idle, picco, dopo il carico;
- PSS per processo (master, zygote, ogni figlio): idle e dopo il carico; Private_Dirty per figlio;
- task/s end-to-end e latenza p50/p99;
- risvegli/s da idle (facoltativo).
Ripeti ogni misura almeno 2 volte (varianza). Riferimento con 4 processi (08b): ref 145,7 MB idle,
rs 72,3 MB; figlio rs 11,2 MB idle, 23,5 MB dopo il carico col task del bench (redis-py importato
dal task), 12,9 MB con `--task pure`; ref ~31 MB per figlio.

## Dove guadagnare (verifica con i numeri, adotta solo ciò che il bench conferma)
1. **Memoria privata per figlio** (con 10 figli domina tutto):
   - tutto ciò che il figlio importa/alloca al primo task va fatto nello zygote prima del fork e di
     `gc.freeze()` (moduli dei task dell'utente: `app.loader.import_default_modules()` già c'è;
     moduli importati *dai* task al primo uso, es. redis-py nel bench: misura se un precarico
     esplicito dei moduli già presenti nel master — come fa di fatto il riferimento, che ha tutto
     nel master — riduce il PSS totale);
   - copy-on-write: verifica quante pagine ereditate diventano private dopo N task (refcount, GC,
     allocatore); `gc.freeze()` dopo il warm-up, soglie GC, `PYTHONMALLOC`/arene: solo se
     invisibile al codice dell'utente (stessi `gc.get_threshold()`/`gc.isenabled()` del riferimento
     visti dal task);
   - buffer/strutture del figlio (IPC, `_rs_child`): dimensioni e crescita.
2. **Master e zygote**: ciò che non serve al supervisore fuori dallo zygote; lo zygote non deve
   tenere memoria che i figli non usano.
3. **Throughput con 10 figli**: profila il master (dispatch, IPC, ack) a c10; verifica che il
   throughput cresca da c4 a c10 (ref su Redis non scala: 852 → 914 da solo a c4). Prefetch e
   batch di invio/ack solo se il traffico sul broker resta identico (`compat wire-worker`).
4. **Crescita nel tempo**: dopo 30000 task la PSS dei figli non deve continuare a salire
   (nessuna crescita illimitata); `max_tasks_per_child`/`max_memory_per_child` come il riferimento.

## Test
- Test in `python/tests_rs/` per ogni cambio di comportamento (es. precarico nello zygote,
  configurazione GC vista dal task, nessuna crescita di PSS oltre una soglia dopo N task).
- Nessun test esistente indebolito.

## Gate (esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs
./compat wire-worker --pool solo && ./compat wire-worker --pool prefork
./compat wire-beat && ./compat cli
```
Se interrompi processi worker durante le prove, assicurati che non restino orfani connessi ai
broker (falsano i gate degli altri).

## Messaggio finale
Tabella c10 ref vs rs (Redis/AMQP, bench/pure) con risparmio di RAM in MB e %, task/s, latenza;
cosa hai cambiato e perché; varianti provate e scartate con i numeri; gate con esito reale; cosa
resta aperto. Committa nel branch (`08d: …`).
