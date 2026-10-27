# 10b — RAM dei figli prefork sotto carico

Priorità del progetto: **occupare meno RAM possibile** (la velocità conta solo se non costa
memoria). Con molti processi la RAM di un worker è quasi tutta nei figli: un figlio celery-rs pesa
~6 MB a riposo ma sale a 11–15 MB dopo il carico (`bench/results/scale-*.json`,
`final-c10-*.json`), contro ~28 MB del riferimento. Obiettivo: **figlio dopo il carico ≤ 9 MB**
con `--task pure` e ≤ 12 MB con `--task bench-toplevel` (c10 e c50), senza aumentare master,
zygote, beat o client.

Leggi `AGENTS.md`, `PLAN.md`, `specs/08d-worker-c10.md`, `specs/09b-soak-fuzz.md`,
`specs/09c-memory-growth.md`, `specs/10a-master-beat-restart.md`. Vincoli invariati: API,
comportamento e traffico identici al riferimento; oggetti kombu/billiard veri, nessun nome
riscritto; `test_kombu_identity.py`, `celery/`, `celery/t/`, `tools/compat/` intoccabili; nessuna
modifica visibile al codice dei task (configurazione GC vista dal task, `sys.modules` utente,
logging, segnali identici al riferimento).

## Diagnosi
Per un figlio, dal fork ai primi N task (1, 10, 100, 1000) e poi a regime: `Private_Dirty`,
`Shared_*` (smaps per mappatura: heap, arene pymalloc, librerie, stack, mmap anonime),
`tracemalloc` (via `CELERY_RS_MEMORY_PROBE`), heap Rust (feature `memory-probe`). Separa:
- **copy-on-write eroso**: pagine dello zygote scritte dal figlio (refcount e campi GC di oggetti
  ereditati, dict/cache modificati, moduli che mutano al primo uso);
- **inizializzazioni pigre al primo task** (percorsi di trace, serializzazione, backend, logging,
  `re`/`functools` cache, `linecache`): spostale nello **zygote prima del fork** con un
  riscaldamento interno che non esegue task utente né produce effetti esterni (nessun messaggio,
  segnale, log o scrittura sul backend);
- **allocazioni che restano** (arene pymalloc/glibc non restituite, buffer IPC/Rust
  sovradimensionati, oggetti trattenuti tra un task e l'altro).
Riporta una tabella "causa → MB per figlio".

## Correzioni
Solo con evidenza dai numeri: riscaldamento nello zygote, `gc.freeze()` dopo il riscaldamento,
buffer IPC/Rust ridimensionati o rilasciati, rilascio delle arene dopo picchi, meno oggetti vivi
per figlio. Ogni correzione con un test in `python/tests_rs` (PSS/Private_Dirty del figlio oltre
soglia dopo N task; nessun effetto visibile del riscaldamento).

## Verifica
Misure brevi tue, bloccanti (≤ 5 minuti): `bench/bench.py --pool prefork --concurrency 10 e 50
--task pure|bench-toplevel --prefill`, `bench/fleet.py --budget-mb 2048`, prima/dopo. **Non**
eseguire `tools/dev/gates`, soak o `./compat integration`: li esegue l'orchestratore.

## Regole di lavoro (quota del modello limitata)
Comandi bloccanti, niente messaggi di attesa o controlli dell'avanzamento, niente comandi in
background. Rapporto finale breve: tabella cause → MB, correzioni, misure prima/dopo (figlio,
totale c10/c50, worker in 8 GB stimati), varianti scartate, test eseguiti. Committa nel branch
(`10b: …`).
