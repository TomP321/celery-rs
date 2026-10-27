# 09b — Hardening: memoria stabile nel tempo, fuzz del parser, segfault billiard

Ultima spec della fase 9. Priorità del progetto: **RAM di worker e beat** (stabile e minima),
poi prestazioni. Leggi `AGENTS.md` (anche "Fallimenti noti"), `PLAN.md`. Vincoli invariati: API,
comportamento e traffico identici al riferimento; oggetti kombu veri, nessun nome riscritto;
`test_kombu_identity.py`, `celery/`, `celery/t/` intoccabili.

## 1. Memoria stabile nel tempo (soak)
Nuovo `bench/soak.py`: worker `-P prefork -c 10` (Redis e AMQP) e `celery beat` (1000 voci, di cui
molte dovute ogni secondo) per una durata configurabile (`--minutes`, default 60), con carico
continuo e vario: task `pure`, task che falliscono, retry, `countdown`/ETA, revoche, chord/group,
risultati grandi (1 MB), `inspect`/`control` periodici, riavvio del broker della corsia una volta a
metà. Campiona ogni 30 s la PSS di ogni processo (master, zygote, figli, beat) e salva una serie
in `bench/results/soak-*.json`. Criterio: dopo il warm-up (primi 5 minuti) la pendenza della PSS
di ogni processo è **< 1 MB/ora** e nessun figlio supera la propria PSS a 5 minuti di oltre 3 MB;
fd aperti e thread costanti. Esegui ref e rs per confronto (60 minuti ciascuno, in parallelo su
corsie diverse). Se trovi crescite, trova la causa e correggila (con test).

## 2. Fuzz del parser dei messaggi
Il core Rust decodifica messaggi dal broker (protocollo Celery v1/v2, header, body JSON/pickle-
free, compressione, envelope kombu per Redis, proprietà AMQP). Aggiungi target di fuzz
(`cargo fuzz` con libFuzzer, o `proptest`/`arbitrary` se `cargo fuzz` non è disponibile:
motiva la dipendenza nel messaggio finale) per i punti d'ingresso del parsing in
`crates/celery-proto` e `crates/celery-transport`. Esegui ogni target ≥ 10 minuti; nessun panic,
nessuna allocazione proporzionale a lunghezze dichiarate dal messaggio senza limite (difese contro
messaggi ostili: limiti espliciti), nessun loop infinito. Ogni crash trovato diventa un test di
regressione. Un messaggio malformato deve produrre lo stesso esito del riferimento (reject/log),
mai la caduta del worker.

## 3. Segfault billiard nella suite completa
`python/tests_rs/test_worker_pool.py::test_worker_process_signals[prefork]` (vedi "Fallimenti
noti"): nella suite completa i figli del pool billiard (percorso del riferimento) muoiono con
SIGSEGV durante il GC. Trova la causa (probabile: fork di un processo pytest che ha thread/stato
Rust vivi lasciati da test precedenti; finalizzatori di oggetti `celery._rs` ereditati) e
correggila nel codice di celery-rs (fork safety: `register_at_fork`, oggetti Rust orfani dopo il
fork non devono essere toccati dai finalizzatori). Poi togli la voce da "Fallimenti noti" in
`AGENTS.md`.

## Gate
`tools/dev/gates` (tutti verdi salvo i "Fallimenti noti" rimasti), più i soak e i fuzz sopra.

## Regole di lavoro (quota del modello limitata)
- Comandi lunghi (soak, fuzz, gate) **in modo bloccante**, una volta sola. **Non scrivere messaggi
  di attesa né controllare l'avanzamento**: ogni messaggio è una chiamata al modello.
- Se un comando dura più del limite del tuo strumento, lancialo in background con output su file e
  **termina il turno** scrivendo nel rapporto quali file leggere: l'orchestratore ti riprenderà
  quando ha finito.
- Non indagare i "Fallimenti noti" diversi dal punto 3.
Rapporto finale breve: grafici/tabella PSS nel tempo (pendenza per processo, ref vs rs), target
di fuzz con durata ed esiti, causa e correzione del segfault, gate con esito reale, cosa resta
aperto. Committa nel branch (`09b: …`).
