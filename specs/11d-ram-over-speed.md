# 11d — RAM prima della velocità (worker `-c 10`, anche Python 3.11)

Leggi `AGENTS.md`, `docs/DIFFERENZE.md`, `bench/results/11c-immortal-load.md`. Base: 11c
(oggetti immortali). Regola dell'utente: **tutto sul risparmio di memoria; la base è il Celery originale.**
Il throughput può scendere fino a quello del Celery originale misurato su questa macchina con
la stessa misura (`loadpeak.py` con `.venv-ref`: Redis `-c 10` ~820 task/s, RabbitMQ ~3350;
rimisuralo tu all'inizio e usa quei numeri come pavimento, non sotto). Ogni MB conta più di
qualsiasi task/s sopra il pavimento: preferisci sempre la variante che usa meno RAM. Conta la
PSS totale del worker **a riposo e al picco sotto carico** (`bench/memprobe/loadpeak.py`,
30.000 task, interi e `--payload-bytes 16384`, Redis e RabbitMQ).

Punto di partenza (11c, Python 3.13): riposo ~49,5 MB, picco 53,7 (Redis interi) / 58,1
(RabbitMQ 16 KiB). La produzione dell'utente gira su **Python 3.11**, dove l'immortalità è
un no-op: misura anche con 3.11 (`uv python install 3.11`, crea `.venv-rs311` con
`tools/dev/build-package` adattato o `uv pip install` della wheel; non toccare `.venv-ref`) e
riporta prima/dopo per 3.11 e 3.13.

## Piste (ognuna: misura prima/dopo, tieni solo ciò che riduce la PSS)
1. **Master**: runtime tokio con meno thread (anche `current_thread`) per consumer/pool/
   producer; `mallopt(M_ARENA_MAX, …)`/arene glibc per thread; stack dei thread Rust più
   piccoli; capacità iniziali e massime dei buffer (lettura socket, prefetch, frame IPC
   verso i figli) ridotte e restituite quando il carico cala.
2. **Figli sotto carico**: restituire memoria dopo ogni task o al passaggio a riposo
   (`malloc_trim`/rilascio arene pymalloc vuote) invece che solo ai task 1/10/100/1000; buffer
   di lettura/scrittura del socketpair riusati o rilasciati; meno oggetti per task nel percorso
   `_rs_child` → trace → READY.
3. **Python 3.11** (senza immortalità): pagine private dei figli dopo fork e
   `process_initializer` (vedi la diagnosi in `bench/results/11c-immortal-load.md` e
   `bench/memprobe/sd.py`): riduci ciò che il figlio tocca, es. lavoro deterministico identico
   in ogni figlio spostabile nello zygote **solo se il comportamento osservabile resta quello
   del riferimento** (segnali `worker_process_init` ecc. sempre nel figlio); compattazione dei
   pool pymalloc parzialmente liberi nello zygote prima del freeze se le misure lo giustificano.
4. **Inventario dell'immagine condivisa** (~28 MB di RSS condivisa che ogni worker paga una
   volta, più le pagine private che nascono toccandola): elenca i moduli in `sys.modules` di
   master, zygote e figlio con il costo stimato (anon + `.so` mappate, es. `_zstd`, `_brotli`,
   `_yaml`, `_cmsgpack`, `_decimal`, `libcrypto`/`ssl`, `click`, trasporti kombu inutilizzati,
   `redis-py` se il backend è Rust) e chi li importa. Evita gli import che il worker non usa
   **solo dove l'API resta identica** (import pigri nel guscio `python/celery`; kombu resta il
   vero kombu, vedi `python/tests_rs/test_kombu_identity.py`). Misura anche l'heap libero
   ereditato dallo zygote (pymalloc/glibc) e la frammentazione.
5. Qualsiasi altra idea misurata. Non cambiare semantica osservabile (prefetch, ack, numero di
   processi, segnali, opzioni CLI) rispetto al riferimento.

## Obiettivi
Python 3.13: riposo ≤ 40 MB, picco ≤ 44 MB in tutti e quattro gli scenari.
Python 3.11: riduzione ≥ 25% di riposo e picco rispetto a 11c su 3.11.
Sono minimi, non traguardi: continua finché trovi riduzioni misurabili. Throughput ≥ pavimento.

## Verifica
`cargo build --release`, `cargo clippy --all-targets -- -D warnings`,
`tools/dev/with-brokers cargo test`, `tools/dev/with-brokers .venv-rs/bin/python -m pytest
python/tests_rs -q`, `./compat api`. **Non lanciare `tools/dev/gates`, upstream, integration né
soak**: li esegue l'orchestratore. Comandi bloccanti, niente messaggi di attesa, niente push.
Committa nel branch (`11d: …`); rapporto in `bench/results/11d-ram-over-speed.md` con tabella
prima/dopo per ogni scenario e versione, e ogni pista provata (anche quelle scartate, con il
numero).

Nota: l'orchestratore sta eseguendo i gate in parallelo sulle corsie dei broker: le misure di
RAM restano valide, il throughput può risultare più basso del reale (ripeti la misura finale
di task/s due volte e riporta la migliore). I file in `bench/results/11d-before.log` di un
avvio precedente interrotto possono essere riusati o sovrascritti.
