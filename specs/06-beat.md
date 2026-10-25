# 06 — Beat: pubblicazione Rust, crontab nel core, persistenza compatibile

Fase 6 di `PLAN.md`. Oggi `python/celery/beat.py`, `schedules.py` e `apps/beat.py` sono copie del
riferimento: `celery beat` funziona ma pubblica con un `kombu.Producer` (redis-py/py-amqp) e
calcola i crontab in Python. Obiettivo: `celery beat` di celery-rs con comportamento, messaggi
sul broker e file di schedule identici al riferimento, ma con meno memoria e meno CPU per tick.

## Riferimenti
- `celery/celery/beat.py`: `ScheduleEntry` (`is_due`, `_next_instance`, `update`, `editable_fields`,
  `__reduce__`), `Scheduler` (`tick`, `populate_heap`, `apply_entry`, `apply_async`, `_do_send`,
  `send_task`, `should_sync`/`sync`, `_ensure_connected` con il log
  `beat: Connection error: %s. Trying again in %s seconds...`, `connection`, `producer`,
  `Producer`, `adjust`, `_when`, `merge_inplace`, `install_default_entries`, `close`),
  `PersistentScheduler` (shelve, `_store`, chiavi `entries`/`__version__`/`tz`/`utc_enabled`,
  reset del db se cambiano versione/tz, `_destroy_open_corrupted_schedule`), `Service`
  (`start`, `sync`, `stop`, `get_scheduler`), `EmbeddedService`.
- `celery/celery/schedules.py`: `schedule`, `crontab_parser` (messaggi di `ParseException` e
  `ValueError`), `crontab` (`_expand_cronspec`, `remaining_delta`, `_delta_to_next`,
  `remaining_estimate`, `is_due` con `beat_cron_starting_deadline`, `__reduce__`, `__setstate__`,
  `__eq__`, `from_string`), `solar`.
- `celery/celery/utils/time.py`: `remaining`, `ffwd`, `maybe_make_aware`, `localize`, DST.
- `python/celery/app/_rs_producer.py` (fase 3b): `FastConnection`, `RsProducer`, condizioni di
  attivazione (`CELERY_RS_PRODUCER`), mappatura degli errori.
- Test upstream: `celery/t/unit/app/test_beat.py`, `celery/t/unit/app/test_schedules.py`,
  `celery/t/unit/bin/test_beat.py`, `celery/t/unit/apps/test_beat.py`.

## Progetto
1. **Pubblicazione con il producer Rust.** `Scheduler.connection`/`Scheduler.producer` usano la
   `FastConnection`/`RsProducer` della fase 3b quando il trasporto è idoneo (stesse condizioni di
   `use_fast_producer`, `CELERY_RS_PRODUCER=0` torna a kombu) **e** `Producer` non è stato
   ridefinito da una sottoclasse (django-celery-beat, redbeat e simili devono continuare a
   funzionare col percorso kombu). Restano identici: `_ensure_connected` (retry,
   `broker_connection_max_retries`, testo del log), errore di pubblicazione in `apply_entry`
   (log `Message Error: ...` e beat che prosegue), riconnessione dopo broker perso, chiusura in
   `close()`. Il processo beat non deve più importare redis-py né py-amqp sul percorso Rust
   (verificalo con `sys.modules` in un test). Gate: `./compat wire-beat` → 0 differenze.
2. **Crontab nel core Rust.** Nuovo modulo `crates/celery-beat` (+ binding in `celery-py`):
   - parser dei campi con la semantica esatta di `crontab_parser` (range, step, `*/n`, wrap
     `fr > to`, nomi di giorni/mesi, numeri negativi, stessi messaggi d'errore e stesse classi di
     eccezione lato Python);
   - calcolo del prossimo istante della parte **di calendario** di `remaining_delta` e
     `_delta_to_next` (campi come bitmask: minuti `u64`, ore/giorni/mesi in interi), che
     restituisce gli stessi argomenti di `ffwd` del riferimento (incluso il `RuntimeError` dopo
     2000 tentativi). Fusi orari, DST, `maybe_make_aware` e `remaining()` restano il codice Python
     del riferimento: il Rust non gestisce tz e non aggiunge dipendenze tz.
   - API pubblica invariata: `crontab.minute`/`hour`/`day_of_week`/`day_of_month`/`month_of_year`
     restano `set` di `int`, `_orig_*` invariati, `__reduce__`/`__setstate__`/`__eq__`/`repr`
     identici, così pickle e shelve restano compatibili in entrambe le direzioni.
   - `schedule` (intervallo) e `solar` restano Python.
3. **Persistenza compatibile.** `PersistentScheduler` resta su `shelve` con lo stesso contenuto.
   Test: il file scritto da beat ref è letto da beat rs (stesse voci, `last_run_at`,
   `total_run_count`, niente reset) e viceversa; db corrotto → stesso recupero del riferimento.
4. **Memoria e CPU del processo beat.** Riduci gli import al minimo necessario sul percorso Rust
   (nessuna modifica a nomi pubblici: gli attributi restano accessibili, al più caricati in modo
   pigro). Misura con un nuovo `bench/bench_beat.py` (venv ref e rs, Redis e AMQP):
   PSS del processo beat idle con 3 voci e con 1000 voci (500 interval 1 s + 500 crontab misti),
   CPU per tick (`Scheduler.tick` con 1000 voci, µs), invii/s sostenuti, e microbench di
   `crontab.remaining_estimate`/`is_due` (ns/chiamata). Riporta i numeri ref vs rs. Obiettivo:
   PSS rs < ref, `remaining_estimate` almeno 5× più veloce.
5. Niente modifiche a firme, nomi, default pubblici o testi di log; nessuna nuova impostazione.

## Test richiesti
- `crates/celery-beat`: test unitari del parser e del calcolo calendario (anni bisestili,
  29/30/31 del mese, `day_of_week` + `day_of_month` insieme, mesi senza giorni validi →
  `RuntimeError`).
- `crates/celery-py/tests/test_cron.py` (`.venv-ref`, differenziale nello stesso processo:
  `celery.schedules` del riferimento vs funzioni `_celery_rs`): ≥ 200.000 casi casuali con seme
  fisso su parser (stringhe valide e non valide, stessi errori) e su `remaining_delta`/
  `remaining_estimate`/`is_due` con fusi `UTC`, `Europe/Rome`, `America/New_York`,
  `Australia/Lord_Howe`, `Asia/Kolkata`, `enable_utc` vero/falso, `last_run_at` naive e aware in
  un fuso diverso, istanti attorno ai cambi DST, `beat_cron_starting_deadline`. Zero differenze.
- `python/tests_rs/test_beat.py` (`.venv-rs`): `celery beat` come sottoprocesso con un worker del
  riferimento: interval, crontab (una voce che scatta al minuto: test lento ammesso, < 75 s),
  `relative=True`, `options` (queue, priority, expires, headers), task inesistente, broker non
  raggiungibile all'avvio poi disponibile (usa un proxy TCP controllato dal test, **non** fermare i
  broker condivisi), broker perso durante l'esecuzione, file di schedule ref↔rs, `-S` scheduler
  personalizzato con `Producer` ridefinito (percorso kombu), `CELERY_RS_PRODUCER=0`, beat embedded
  (`celery worker -B`, con il worker rs). Ogni scenario confrontato con beat del riferimento.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressioni
./compat wire-worker --pool prefork                                    # 0 differenze
./compat wire-beat                                                     # 0 differenze
```
Inoltre, nel messaggio finale, i numeri di `bench/bench_beat.py` (ref vs rs).

## Fuori scope
CLI (`celery beat` resta quella di click del riferimento, fase 7), django-celery-beat, `solar` in
Rust, ottimizzazioni del worker.
