# 09c — Crescita della memoria nel tempo: non peggiore del riferimento

Priorità del progetto: **RAM di worker e beat**. Dopo la 09b il worker prefork c10 di celery-rs
usa molta meno memoria del riferimento, ma **cresce più in fretta** sotto carico prolungato
(`bench/soak.py`, 60 minuti, carico vario, riavvio del broker al minuto 30):

| PSS totale worker c10 (senza beat) | 5 min | 31 min | 59 min | crescita |
|---|---|---|---|---|
| ref Redis | 352 | 366 | 378 | +27 MB |
| rs Redis | 230 | 259 | 286 | +57 MB |
| ref AMQP | 381 | 408 | 422 | +41 MB |
| rs AMQP | 225 | 260 | 284 | +60 MB |

Per figlio rs: +4–8 MB/h, tutti i figli (nel ref la crescita è concentrata in pochi figli, altri
restano fermi). Al riavvio del broker rs salta di ~+10 MB, il ref di ~+2 MB. Dati:
`bench/results/soak-20261027-051343-rs-*.json`, `soak-20261027-010658-ref-*.json`,
`09b-soak-report-final.md`, `09b-redis-short-report.md`.

Leggi `AGENTS.md`, `PLAN.md`, `specs/09b-soak-fuzz.md`. Vincoli invariati (API, traffico, oggetti
kombu veri, nessun nome riscritto, test e `celery/` intoccabili).

## Obiettivo
In un soak di 60 minuti: crescita del totale rs **≤ quella del riferimento** sullo stesso broker
(meglio se tende a un plateau) e salto al riavvio del broker ≤ +3 MB; nessun aumento della PSS
iniziale né peggioramento di throughput/latenza oltre il rumore.

## Diagnosi (prima di correggere)
Per ogni figlio distingui, nel tempo:
- **erosione del copy-on-write** (pagine condivise con lo zygote che diventano private:
  `Shared_*` → `Private_*`, RSS costante) — causata da refcount/GC che toccano oggetti ereditati,
  o da strutture ereditate modificate;
- **nuove allocazioni** (RSS che sale): heap Python (`tracemalloc` nel figlio, attivabile con una
  variabile d'ambiente di diagnosi, snapshot a intervalli e diff per traceback), heap Rust
  (feature `memory-probe`), arene glibc / frammentazione (`malloc_info`), cache che crescono
  (es. cache di import, `linecache`, registri, code interne, `revoked`, `active_requests`,
  buffer IPC, log).
Per il salto al riavvio del broker: cosa viene caricato o allocato nei figli e nel master alla
riconnessione (confronta con il riferimento).
Usa soak brevi (`--minutes 15`) e prove mirate; riporta una tabella "causa → MB/h".

## Correzioni
Solo con evidenza dai numeri e senza cambiare il comportamento visibile: es. caricare nello
zygote ciò che i figli caricano dopo, `gc.freeze()` degli oggetti stabili dopo il warm-up se
dimostrato utile, limitare cache e strutture interne con limiti come il riferimento, rilascio di
buffer/arene. Ogni correzione ha un test in `python/tests_rs`.

## Verifica
Soak brevi tuoi prima/dopo (15 min, Redis e AMQP). **Non** eseguire soak da 60 minuti né
`tools/dev/gates`: li esegue l'orchestratore.

## Regole di lavoro (quota del modello limitata)
Comandi in modo bloccante, niente messaggi di attesa o controlli dell'avanzamento. Se un comando
supera il limite del tuo strumento, lancialo in background con output su file e termina il turno
indicando i file da leggere. Rapporto finale breve: tabella cause → MB/h, correzioni, soak brevi
prima/dopo, cosa resta. Committa nel branch (`09c: …`).
