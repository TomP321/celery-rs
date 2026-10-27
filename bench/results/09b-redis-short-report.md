09b — diagnosi Redis e confronto breve (27 ottobre 2026)

Il salto dopo il riavvio nasce soprattutto dagli import tardivi delle classi di errore: `RedisBackend.connection_errors` carica redis-py, asyncio e kombu nei singoli figli. Lo zygote non alloca: la sua RSS rimane costante, ma la PSS aumenta quando i figli modificano pagine condivise. Il secondo contributo sono le pagine libere delle arene glibc dopo le copie dei body da 1 MiB nei buffer RESP. Nel probe di 500 risultati la memoria Python viva dopo GC cresce di soli 16 KiB, mentre `malloc_trim` recupera circa 10 MiB di PSS.

Correzione: il grafo delle eccezioni reali viene caricato nello zygote prima di `gc.freeze()` e dei fork; dopo SET/SETEX/PUBLISH grandi si restituiscono le pagine libere a glibc, anche al comando successivo e alla fine del task, quando le copie Python non sono più vive. Il figlio libera inoltre risultato, argomenti e frame IPC prima di attendere il prossimo job. La reclamazione è limitata a Linux/glibc e ai payload >=1 MiB; `libc` era già nelle dipendenze del workspace.

Quattro soak rs/Redis da 12 minuti, bloccanti, con riavvio al minuto 6. Qui il confronto fra codice iniziale e codice finale, entrambi con 220 cicli completati e ripresa del carico verificata. PSS in MiB; crescita dal campione a 5 minuti; ultimo campione a 11,5 minuti.

| Misura | Prima | Dopo |
|---|---:|---:|
| PSS finale worker + zygote + 10 figli, escluso beat | 414,61 | 253,67 |
| Mediana della crescita massima dei figli | 19,51 | 1,43 |
| Peggiore crescita massima di un figlio | 20,24 | 3,81 |
| Crescita massima PSS zygote | 2,69 | 0,37 |
| RSS zygote | 32,1 costanti | 42,6 costanti |

Serie originali: [prima](soak-20261027-040541-rs-redis-390ea448d0.json), [dopo](soak-20261027-045508-rs-redis-90758db30b.json); [tabella per PID, pendenze, fd e thread](09b-redis-short-pss.md). Le due prove intermedie e tutti i log di questa diagnosi restano in `target/09b/`.

Probe isolato, 100 risultati da 1 MiB: stesso guscio Python rs, binding precedente di `.venv-ref` contro binding nuovo; incremento PSS **12,29 -> 2,30 MiB**. Il test della pipeline fallisce col binding precedente (+4,32 MiB) e passa col nuovo (limite +3 MiB). Altre regressioni verificano le pagine dell'encoding Python, l'assenza di nuovi import degli errori nei figli e la distruzione dell'ultimo risultato mentre il figlio è inattivo.

Verifiche finali: `cargo build --release` e `cargo clippy --all-targets -- -D warnings` OK; `tools/dev/with-brokers cargo test`: **240 passati, 0 falliti, 4 fuzz ignorati**; pytest sotto lock su `test_backend_redis.py`, `test_worker_pool.py`, `test_redis_memory.py` e `bench/test_soak.py`: **49 passati**. Nessun gate, fuzz lungo o soak da 60 minuti rieseguito.

**Resta aperta la soglia della spec:** anche il soak breve finale esce 1. Due figli superano ancora +3 MiB (+3,81 e +3,06); le pendenze OLS dei figli sono 5,01–48,10 MiB/h, quella dello zygote 3,19 MiB/h; alcuni fd cambiano alla riconnessione. Il salto principale è corretto, ma questo confronto non certifica stabilità <1 MiB/h. Gli import/preload e la reclamazione riducono la memoria; non si modificano warm-up, analisi o soglie per dichiarare il soak verde. La verifica lunga resta all'utente.
