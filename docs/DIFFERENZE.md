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
