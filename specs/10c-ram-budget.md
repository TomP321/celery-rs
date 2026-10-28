# 10c — RAM: ogni MB del worker c10 e di beat

Priorità assoluta del proprietario: **occupare meno RAM possibile** (la velocità conta solo se
resta migliore del riferimento). Stato dopo 10b (`bench/fleet.py`, 8 GB): worker `-c 10` rs
**77,4 MB a riposo, 85,2 MB sotto carico** (ref 300 MB); 105 worker in 8 GB a riposo, 96 sotto
carico (ref 27). Per worker: master ~18 MB, zygote ~8 MB, figli 5,9–8 MB (c50/c10) a carico
`pure`, ~9–11 MB con `bench-toplevel`; beat ~28 MB a 3 voci.

Obiettivi: worker c10 **≤ 65 MB a riposo e ≤ 72 MB sotto carico** (`fleet.py`), master ≤ 15 MB,
beat ≤ 24 MB, senza aumentare il client e con latenza p50 sotto il riferimento.

Leggi `AGENTS.md`, `PLAN.md` (fase 10), `specs/10a-master-beat-restart.md`,
`specs/10b-child-memory.md`. Vincoli invariati: API, comportamento e traffico identici; oggetti
kombu/billiard veri, nessun nome riscritto; `test_kombu_identity.py`, `celery/`, `celery/t/`,
`tools/compat/` intoccabili; nessun effetto visibile al codice dei task.

## 1. Bilancio (prima di correggere)
Tabella "dove va ogni MB" per master, zygote, figlio (a riposo e dopo il carico) e beat:
privato vs condiviso (`smaps` per mappatura), heap Python per modulo (tracemalloc, `sys.modules`,
dimensione dei code object e delle docstring), arene pymalloc/glibc libere, librerie mappate,
stack dei thread, heap Rust (`memory-probe`). Indica per ciascuna voce se è condivisa fra i figli,
fra worker diversi, o privata.

## 2. Esperimenti da misurare (adotta solo se il bench conferma)
- **Moduli nel master e nello zygote**: cosa serve davvero a ciascuno; spostare nello zygote ciò
  che serve solo ai figli, togliere dal master ciò che serve solo allo zygote, import pigri onesti.
- **Zygote**: memoria privata dello zygote che i figli non usano (può liberarla prima di forkare?).
- **Allocatore**: tunable glibc impostati dal worker per sé (`M_TRIM_THRESHOLD`,
  `M_MMAP_THRESHOLD`, `M_ARENA_MAX` per i thread Rust), `PYTHONMALLOC`, rilascio delle arene dopo
  l'avvio e dopo i picchi; solo se invisibili al codice utente.
- **Thread**: numero e dimensione degli stack dei thread Rust/Python nel master, nello zygote, nei
  figli e in beat (stack più piccoli dove sicuro).
- **Docstring/annotazioni/code object** non necessari a runtime: solo se il comportamento resta
  identico (`__doc__` pubblici devono restare uguali al riferimento: niente `-OO`).
- **Beat**: moduli e strutture rimaste dopo 08f/10a.

## Test
Un test in `python/tests_rs` per ogni modifica (soglie di PSS per processo, moduli assenti,
nessun effetto visibile). Nessun test esistente indebolito.

## Verifica
Misure brevi tue, bloccanti (≤ 5 minuti ciascuna): `bench/fleet.py --budget-mb 2048` (rs, prima e
dopo), `bench/bench.py --pool prefork --concurrency 10 --task pure|bench-toplevel --prefill`,
`bench/bench_beat.py`, `bench/bench_client.py`. **Non** eseguire `tools/dev/gates`, soak o
`./compat integration`: li esegue l'orchestratore dopo il tuo commit.

## Regole di lavoro (quota del modello limitata)
Comandi bloccanti, niente messaggi di attesa o controlli dell'avanzamento, niente comandi in
background. Rapporto finale breve: tabella del bilancio, esperimenti con i numeri (adottati e
scartati), misure prima/dopo (worker c10 a riposo e sotto carico, worker in 8 GB stimati, master,
zygote, figlio, beat, client, latenza), test eseguiti. Committa nel branch (`10c: …`).
