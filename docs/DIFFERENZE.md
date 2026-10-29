# Differenze note rispetto a Celery 5.7.0rc1

celery-rs mantiene API, comportamento e traffico sul broker identici a Celery: lo verificano
`./compat api|wire|wire-worker|wire-beat|cli|upstream|integration`. Restano queste differenze,
volute e documentate. Riguardano introspezione e dettagli interni, non il risultato dei task.

## Introspezione statica di alcuni attributi di classe
Per non caricare redis-py, billiard e asynpool in processi che non li usano, quattro attributi
di classe sono descrittori risolti al primo accesso:

| attributo | al primo accesso diventa |
|---|---|
| `celery.backends.redis.RedisBackend.connection_class_ssl` | `redis.SSLConnection` |
| `celery.backends.redis.SentinelBackend.connection_class_ssl` | `SentinelManagedSSLConnection` |
| `celery.concurrency.prefork.TaskPool.Pool` | `celery.concurrency.asynpool.AsynPool` |
| `celery.concurrency.prefork.TaskPool.BlockingPool` | `billiard.pool.Pool` |

L'accesso dalla classe o da un'istanza restituisce l'oggetto vero; prima del primo accesso
`vars(cls)` / `inspect.getattr_static` vedono il descrittore. Elencate in `tools/compat` fra le
differenze accettate.

## Annotazioni di `celery.utils.nodenames.worker_direct`
`kombu.entity.Queue` è importata solo per il controllo dei tipi (non a runtime, per non caricare
kombu): `typing.get_type_hints(worker_direct)` solleva `NameError`, mentre `inspect.signature` e
le annotazioni come stringhe sono identiche al riferimento.

## Traceback dei task falliti (pool prefork Rust)
Dopo che il figlio ha inviato il risultato di un task fallito, i frame del traceback ormai
terminati vengono svuotati (`frame.clear()`), come Celery tenta già di fare durante il tracing.
Evita cicli di riferimenti che trattengono memoria fino al GC. I segnali (`task_failure`, …) e gli
errback ricevono il traceback completo come nel riferimento; solo chi conserva un traceback oltre
la fine del task (es. in una variabile globale per leggerlo dopo) ne trova le variabili locali
vuote.

## Garbage collector nei figli
Lo zygote esegue `gc.freeze()` prima di creare i figli (le pagine restano condivise in
copy-on-write): nei figli `gc.get_freeze_count()` è diverso da zero. `gc.isenabled()` e
`gc.get_threshold()` visti dai task sono quelli del riferimento.

## Rilascio della memoria (Linux/glibc)
Dopo l'avvio e dopo task con payload ≥ 1 MiB i processi restituiscono al sistema le pagine libere
dell'allocatore (`malloc_trim`). Nessun effetto funzionale; costo di CPU proporzionale all'heap.

## Processi
Il pool prefork ha un processo in più: lo zygote, da cui nascono i figli (il master contiene i
thread del runtime Rust e non può forkare in sicurezza). Con `-c 10`: master, zygote, 10 figli.

## Oggetti immortali nel pool prefork Rust
Su CPython 3.12–3.14 con GIL, a 64 bit little-endian, il master e lo zygote
rendono immortali gli oggetti già presenti prima del fork e i loro referenti.
Si riducono le pagine copiate per aggiornare i refcount nei figli. La visita usa
`tp_traverse` anche sui contenitori non tracciati, con profondità massima 128;
il refcount stesso interrompe i cicli, senza grandi strutture temporanee.
Sono esclusi gli oggetti già immortali, quelli con `tp_finalize`/`tp_del` e le
istanze native `_celery_rs` (devono poter chiudere socket e runtime).

Questi oggetti non vengono più liberati: i weakref, le `WeakValueDictionary` e
i receiver deboli dei segnali possono restare vivi fino alla fine del processo;
`sys.getrefcount()` restituisce valori enormi. Le istanze con finalizzatore di tipo
sono escluse, ma possono restare vive se trattenute da un contenitore immortale;
un `__del__` aggiunto al tipo dopo la visita non farà
liberare le istanze già immortali. Gli oggetti creati dopo restano normali.
Questo compromesso è accettato per il pool prefork, dove il patrimonio iniziale
è precaricato per tutta la vita del worker e il risparmio di RAM è prioritario.
`CELERY_RS_IMMORTALIZE=0` ripristina il comportamento precedente. Sugli altri
interpreti, build free-threaded, architetture e versioni la funzione è un no-op.

## Initializer dei figli preparato nello zygote
Con il pool prefork Rust, le parti di `process_initializer` che danno lo stesso stato in ogni
figlio (`app.log.setup`, `set_current`/`finalize`, i tracer dei task) vengono eseguite una volta
nello zygote prima del fork: i figli le ereditano condivise invece di ricrearle (circa −1 MB
privato per figlio). Restano nel figlio, nell'ordine del riferimento, `set_pdeathsig`, segnali,
titolo del processo, `init_worker`/`init_worker_process` del loader, `reset_state` e
`worker_process_init`. Il figlio ricostruisce i tracer dei task aggiunti o con tracer sostituito
dall'hook del loader, e rifà `log.setup` se i parametri di logging sono cambiati o se il file di
log contiene `%i` (un file per figlio). Unica differenza: un `on_worker_process_init` del loader
che sostituisce `task.run` di un task già registrato non viene visto dal tracer preparato.
