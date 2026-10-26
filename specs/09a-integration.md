# 09a — Hardening: suite di integrazione del Celery originale (`t/integration`) su celery-rs

Fase 9 di `PLAN.md`. Gate di uscita della fase: **`t/integration` completa**, eseguita con
celery-rs, con gli stessi esiti del riferimento. Priorità del progetto invariata: RAM di worker e
beat, poi prestazioni; nessuna correzione può aumentare la PSS oltre il rumore (> 0,5 MB) senza
motivarlo.
Leggi `AGENTS.md` (anche "Fallimenti noti"), `PLAN.md`. Vincoli invariati: API, comportamento e
traffico identici; oggetti kombu veri, nessun nome riscritto; `test_kombu_identity.py`,
`celery/`, `celery/t/` intoccabili. Questa spec ti autorizza ad **aggiungere** a `tools/compat/`
il sottocomando `integration` (senza modificare quelli esistenti).

## 1. `./compat integration`
Esegue `celery/t/integration` (copia in `tools/compat/out/integration-src` come fa `upstream`, mai
modificando l'originale) con `.venv-ref` e con `.venv-rs`, sulle corsie di `tools/dev/with-brokers`,
in due configurazioni:
- `TEST_BROKER=redis://…` `TEST_BACKEND=redis://…`
- `TEST_BROKER=pyamqp://…` (AMQP della corsia) `TEST_BACKEND=redis://…`

Esito per test (passato/fallito/saltato/errore) per ref e rs; **regressione = fallisce in rs e
passa in ref**; i test instabili si ripetono (come `upstream`) prima di essere contati. Report in
`tools/compat/out/integration-report.md` (+ JSON) e riga di sintesi come gli altri gate.
I test che richiedono servizi assenti (database, memcached, Django, security con certificati…)
saltano o falliscono allo stesso modo in ref e rs: non sono regressioni; elencali.
Aggiungi `integration` a `tools/dev/gates`.

## 2. Correzioni
Porta le regressioni a **0**. Le aree attese sono il worker incorporato nei test
(`celery.contrib.testing.worker.start_worker`, worker in thread con pool `solo`), canvas/chord,
`test_connection_recovery.py` (broker che si riavvia: per riavviare il broker usa solo i container
della **tua** corsia), `test_inspect.py`, `test_beat.py`, i test di leak
(`test_mem_leak_in_exception_handling.py`, `test_events_state_memory_leak.py`,
`test_backend_leak_in_fixture_teardown.py`). Per ogni correzione: test in `python/tests_rs/` che la
copre, e verifica che la PSS del worker c10 e di beat non cresca (`bench/bench.py --pool prefork
--concurrency 10 --task pure`, `bench/bench_beat.py`).

## Gate
`tools/dev/gates` (tutti verdi salvo i "Fallimenti noti"), compreso il nuovo `integration` con
0 regressioni in entrambe le configurazioni.

## Regole di lavoro (quota del modello limitata)
Comandi lunghi **una volta e in modo bloccante**, niente controlli periodici dell'avanzamento.
Non indagare i "Fallimenti noti". Rapporto finale breve: tabella esiti integration ref vs rs per
configurazione (passati/falliti/saltati, regressioni prima/dopo), correzioni fatte e perché, PSS
prima/dopo, gate con esito reale, cosa resta aperto. Committa nel branch (`09a: …`).
