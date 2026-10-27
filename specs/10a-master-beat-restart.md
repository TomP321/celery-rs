# 10a — RAM: master del worker, beat a riposo, salto al riavvio del broker

Priorità del progetto: **RAM di worker e beat** rispetto al Celery originale (poi prestazioni).
Le fasi 0–9 sono chiuse (`PLAN.md`). Restano tre punti di memoria. Leggi `AGENTS.md`, `PLAN.md`,
`specs/08b-memory.md`, `specs/08f-beat-memory.md`, `specs/09c-memory-growth.md`. Vincoli
invariati: API, comportamento e traffico identici al riferimento; **oggetti kombu/billiard veri,
nessun nome riscritto, nessuna classe sostitutiva**; `test_kombu_identity.py`, `celery/`,
`celery/t/`, `tools/compat/` intoccabili.

## Stato (main @ a28e412)
| | ref | rs | obiettivo |
|---|---|---|---|
| master worker prefork c10, idle | 27,6 MB | 18,3 MB | **≤ 13 MB** |
| zygote, idle | — | 6,1 MB (con preload 9b: ~11 MB) | non crescere |
| beat 3 voci, idle | 43–51 MB | 26–28 MB | **≤ 22 MB** |
| salto al riavvio del broker (soak c10 Redis, totale) | +7 MB | +14 MB | **≤ +5 MB** |

Scomposizione nota (08b, 08f): nel master e in beat l'heap Rust è < 0,2 MB; dominano heap Python
(moduli importati) e librerie mappate; beat carica ~364 moduli.

## Lavoro
1. **Master**: elenca moduli e costo (`python -X importtime`, `tracemalloc`, `sys.modules`) del
   master con pool Rust; tutto ciò che serve solo ai figli (trace, canvas, result, backend…) va
   caricato nello zygote, non nel master; ciò che non serve a nessuno va caricato solo quando
   usato (import pigri onesti, PEP 562). Verifica che lo zygote non trattenga memoria che i figli
   non usano.
2. **Beat**: stessa analisi sul processo `celery beat` (Redis e AMQP); obiettivo ≤ 22 MB a 3 voci.
3. **Riavvio del broker**: cosa viene caricato o allocato in master, zygote e figli alla
   riconnessione (errori, retry, ricostruzione di connessioni/canali/consumer, buffer) rispetto al
   riferimento; caricalo prima del fork o evitalo. Prova mirata: worker c10 idle o con poco carico,
   PSS per processo prima e dopo un `docker restart` dei broker della **tua** corsia.
4. Ogni modifica con un test in `python/tests_rs` (moduli assenti dal master/beat, nessuna
   crescita oltre soglia al riavvio) e misure prima/dopo (`bench/bench.py --pool prefork
   --concurrency 10 --task pure`, `bench/bench_beat.py`, `bench/bench_client.py`): nessuna
   crescita di PSS altrove, throughput e latenza entro il rumore.

## Verifica
Misure brevi tue (≤ 5 minuti ciascuna, bloccanti). **Non** eseguire `tools/dev/gates`, soak o
`./compat integration`: li esegue l'orchestratore dopo il tuo commit.

## Regole di lavoro (quota del modello limitata)
Comandi bloccanti, niente messaggi di attesa o controlli dell'avanzamento; niente comandi in
background (muoiono con la sessione). Rapporto finale breve: tabella prima/dopo (master, zygote,
figli, beat, client, salto al riavvio), moduli tolti/spostati, varianti scartate con i numeri,
test eseguiti, cosa resta. Committa nel branch (`10a: …`).
