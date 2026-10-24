# 05b — Worker: pool di processi Rust (zygote + figli Python)

Fase 5 di `PLAN.md` (architettura ibrida). Sostituisce billiard/`AsynPool` (pool prefork) con un
supervisore Rust nel master e figli Python che eseguono `trace_task_ret`/`fast_trace_task` come
oggi. I figli nascono per `fork()` da uno **zygote**: un processo Python a thread singolo, creato
prima che partano i thread Rust del master, con app e task già importati e `gc.freeze()`, così
le pagine restano condivise (copy-on-write) tra tutti i figli. Obiettivo: stessa semantica di
`-P prefork`, PSS totale del worker molto più bassa.
In parallelo un altro agente scrive il consumer Rust e il loop principale (spec 05a): **non
toccare** `celery/worker/` né `crates/celery-worker/src/consumer*`. Il Waker condiviso esiste già:
`crates/celery-worker/src/wake.rs` e `_celery_rs.Waker` (non cambiarne l'interfaccia): il
supervisore chiama `Waker::notify()` quando ha risultati/eventi pronti.

## Riferimenti
- `python/celery/concurrency/base.py` (`BasePool`: attributi `signal_safe`, `is_green`,
  `uses_semaphore`, `task_join_will_block`, `body_can_be_buffer`, `apply_async`, `on_start/stop/
  terminate/close`, `terminate_job`, `restart`, `grow/shrink`, `num_processes`, `info`,
  `_get_info`, `did_start_ok`, `register_with_event_loop`, `flush`, `maintain_pool`),
  `concurrency/prefork.py` (`TaskPool`, `process_initializer`, `process_destructor`, segnali da
  resettare/ignorare, `worker_process_init`/`worker_process_shutdown`), `concurrency/asynpool.py`
  (semantica di consegna ai figli, `on_job_ready`, `_on_soft_timeout`/`_on_hard_timeout`, flush
  allo shutdown, `WorkerLostError`, figli che muoiono durante un job, `did_start_ok`).
- billiard (`.venv-ref/lib/python3.13/site-packages/billiard/pool.py`): `Worker` (loop del
  figlio, `soft_timeout_sighandler` su SIGUSR1, `maxtasksperchild`, `max_memory_per_child` in
  KB con `resource.getrusage`), `ApplyResult` (`_set`, `_ack`, `_set_terminated`,
  `_timeout_callback`, `_accept_callback(pid, time_accepted)`, `_callback`, `_error_callback`,
  `ExceptionInfo`), `Pool._on_hard_timeout`, `terminate_job`.
- `python/celery/worker/request.py::execute_using_pool` (come il pool viene chiamato) e
  `on_accepted`, `on_timeout`, `on_success`, `on_failure`.
- `python/celery/app/trace.py` (`trace_task_ret`, `fast_trace_task`, `setup_worker_optimizations`).

## Progetto
1. **Supervisore Rust** (`crates/celery-worker/src/pool/`): gestisce N figli via `socketpair`
   (framing binario a lunghezza, `Bytes`), coda dei job limitata, assegnazione al primo figlio
   libero, eventi verso il master (accepted, ready con payload risultato, figlio morto con job,
   soft/hard timeout) in una coda + `Waker::notify()`; time limit con timer: soft → SIGUSR1 al
   figlio, hard → callback di timeout poi SIGKILL; figlio morto/ucciso → job in errore con
   `WorkerLostError` (stessi messaggi di billiard) e figlio rimpiazzato; `maxtasksperchild` e
   `max_memory_per_child` (il figlio esce dopo il job, il supervisore lo rimpiazza);
   `terminate_job(pid, signal)`; `grow/shrink`; shutdown: chiusura ordinata (attende i job,
   `process_destructor`), terminate (SIGTERM/SIGKILL). Nessun `unsafe` non motivato; `fork()` si fa
   **solo nello zygote**, mai nel master con thread attivi.
2. **Zygote** (`python/celery/concurrency/_rs_zygote.py` + parte Rust): processo forkato dal master
   all'avvio del pool (prima dei thread Rust del pool; il master può avere già il runtime del
   producer: lo zygote chiama gli `after_fork` del pacchetto e non usa mai socket del padre),
   `gc.freeze()`, poi resta in attesa di comandi "crea figlio" su un socket di controllo; per
   ogni figlio crea il `socketpair`, fa `fork()`, passa l'estremità del master via `SCM_RIGHTS`
   e riporta il pid. Lo zygote non esegue task. Se muore, il pool lo ricrea (o fallisce con
   `WorkerLostError` sui job in volo se non può: documenta).
3. **Figlio Python** (`python/celery/concurrency/_rs_child.py`): `process_initializer(app,
   hostname)` del riferimento (segnali reset/ignore, `worker_process_init`, `set_default_app`,
   logging, `_set_task_join_will_block(True)`), handler SIGUSR1 → `SoftTimeLimitExceeded` come
   billiard, loop: legge un job, notifica "accepted" (pid, `time.monotonic()` come billiard),
   esegue `target(*args, **kwargs)` (target e argomenti serializzati con `pickle` come billiard:
   il target è la funzione `trace_task_ret`/`fast_trace_task`), invia il risultato
   (`pickle`; eccezioni come `ExceptionInfo` di billiard) e ricomincia. Alla fine
   `process_destructor` + `worker_process_shutdown`.
4. **`TaskPool` Python** (`python/celery/concurrency/rs_prefork.py`): sottoclasse di `BasePool` con
   la stessa interfaccia e gli stessi attributi di `concurrency.prefork.TaskPool`
   (`apply_async(target, args, kwargs, callback, error_callback, accept_callback,
   timeout_callback, soft_timeout, timeout, correlation_id, ...)` restituisce un oggetto con
   l'interfaccia usata da Celery di `ApplyResult`; `info`/`_get_info` con le stesse chiavi di
   prefork — `max-concurrency`, `processes`, `max-tasks-per-child`, `put-guarded-by-semaphore`,
   `timeouts`, `writes`; `num_processes`; `did_start_ok`). I callback vengono chiamati nel thread
   del loop del master quando il loop chiama `pool.process_events()` (non bloccante) dopo un
   risveglio del `Waker` passato al costruttore (`waker=`). Per i test di questa spec usa un
   piccolo driver (`pool.wait(timeout)` = `waker.wait` + `process_events`).
   L'integrazione con `celery worker -P prefork` e con il loop Rust è della spec 05c: qui il
   pool **non** sostituisce ancora `prefork` nella CLI.
5. Segnali, fork safety, nessun thread nel processo che fa `fork()`; niente modifiche a firme,
   nomi o default pubblici.

## Gate (tutti obbligatori, esito reale nel messaggio finale)
```
cargo build --release && cargo clippy --all-targets -- -D warnings
tools/dev/with-brokers cargo test
tools/dev/build-package && tools/dev/build-native
tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q
tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q
./compat api && ./compat wire && ./compat upstream --venv .venv-rs   # 0 / 0 / 0 regressioni
```
Inoltre `python/tests_rs/test_worker_pool.py` (`.venv-rs`), pool Rust guidato direttamente con
`Request` reali del riferimento (costruite da messaggi `as_task_v2`, come in
`t/unit/worker/test_request.py`) e un backend Redis reale:
- esecuzione di N task concorrenti su `-c 4`, risultati e callback identici a billiard
  (stesso test eseguito anche con `concurrency.prefork.TaskPool` come riferimento);
- soft time limit → `SoftTimeLimitExceeded` nel task; hard time limit → `TimeLimitExceeded`,
  figlio rimpiazzato; figlio ucciso (`os.kill` SIGKILL) durante un job → `WorkerLostError`
  con lo stesso messaggio di billiard; `reject_on_worker_lost`;
- `maxtasksperchild=2` e `max_memory_per_child` (task che alloca) → rimpiazzo dei figli;
- `terminate_job` (revoke con terminate=True) con SIGTERM/SIGKILL;
- `grow`/`shrink`; shutdown ordinato con job in corso; `worker_process_init`/`shutdown` chiamati
  nei figli; nessun processo orfano dopo lo stop (verifica con `/proc`).
- Memoria: con un'app che importa ~30 MB di moduli, PSS totale (master+zygote+4 figli) del pool
  Rust vs `concurrency.prefork.TaskPool` del riferimento, idle e dopo 1000 task. Riporta i numeri.
- Benchmark: job/s con task vuoti (1000 task, `-c 4`) pool Rust vs prefork del riferimento.

## Fuori scope
Consumer/loop (05a), integrazione nella CLI `celery worker` (05c), autoscale, `-P threads`,
eventlet/gevent, Windows.
