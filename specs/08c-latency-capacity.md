# 08c — Latenze e capacità reale del worker, senza aumentare la RAM

Priorità del progetto: **RAM di worker e beat** prima di tutto; qui si migliorano latenze e
throughput **solo con modifiche che non aumentano la PSS** (worker c10 idle/carico, beat, client):
ogni modifica riporta la PSS prima/dopo; se cresce oltre il rumore (> 0,5 MB) va scartata o
motivata con un guadagno di prestazioni grande.
Leggi `AGENTS.md` (anche "Fallimenti noti"), `PLAN.md`, `specs/08d-worker-c10.md`.
Vincoli invariati: API, comportamento e traffico sul broker identici al riferimento
(`compat wire*`), oggetti kombu veri, nessun nome riscritto, `test_kombu_identity.py` intoccabile,
`celery/`, `celery/t/`, `tools/compat/` non si modificano.

## 1. Misure mancanti (prima di ottimizzare)
- **Capacità reale del worker c10**: oggi `bench/bench.py` misura il throughput mentre un solo
  client pubblica, e con celery-rs la coda si svuota alla stessa velocità della pubblicazione
  (limite del client, non del worker). Aggiungi a `bench/bench.py` una modalità `--prefill`:
  pubblica N task con il worker fermo, poi avvia il worker e misura il tempo di svuotamento
  (task/s del worker) e la PSS di picco. Misura ref vs rs, c4 e c10, Redis e AMQP, `--task pure` e
  `bench`.
- **App realistica**: variante del task del bench con `import redis` **in cima al modulo** dei
  task (come in un'app vera, quindi condiviso via zygote) invece che dentro la funzione:
  `--task bench-toplevel`. Misura PSS c10 ref vs rs (dovrebbe stare vicino a `pure`).

## 2. Latenze da migliorare
| misura | ref | rs oggi |
|---|---|---|
| backend rpc, `delay().get()` p50 | 3,25 ms | 3,40 ms |
| `inspect ping` in-process verso worker AMQP | — | +1,5 ms rispetto al ref (fase 07) |
| `inspect ping` worker AMQP prefork, p50 | 9,72 ms | 10,66 ms |
| `delay().get()` Redis c10 p50 | 3,01 ms | 1,94 ms |
| `delay().get()` AMQP c10 p50 | 3,28 ms | 2,66 ms |

Obiettivi: rpc e `inspect ping` AMQP **non peggiori del riferimento** (meglio se ≥ 20% più
veloci); capacità c10 col `--prefill` la più alta possibile; p99 non peggiore del riferimento.
Profila prima (dove va il tempo: client, broker, master, IPC, figlio, risposta) e riporta la
scomposizione. Esempi da verificare con i numeri: attese/poll con timeout nel percorso di
risposta, Nagle/flush sui socket AMQP, batch degli ack, wake-up del loop, serializzazioni
ripetute, prefetch con c10.

## Test
Test in `python/tests_rs/` per ogni cambio di comportamento; nessun test esistente indebolito.

## Gate
`tools/dev/gates` (tutti verdi salvo i "Fallimenti noti" di `AGENTS.md`). Verifiche intermedie con
`tools/dev/gates --only <nome>` o singoli test sotto `tools/dev/with-brokers`.

## Regole di lavoro (quota del modello limitata)
Esegui i comandi lunghi **una volta e in modo bloccante**, non in background con controlli
periodici: ogni controllo costa una chiamata al modello. Non indagare i "Fallimenti noti".
Rapporto finale breve: tabelle prima/dopo (latenze, capacità `--prefill`, PSS c10 `pure`/`bench`/
`bench-toplevel`, beat), cosa hai cambiato e perché, varianti scartate con i numeri, gate con esito
reale, cosa resta aperto. Committa nel branch (`08c: …`).
