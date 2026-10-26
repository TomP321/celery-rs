# 08e — Gate in parallelo: broker separati per "corsia"

Infrastruttura di test, nessun cambio al codice di celery-rs. Oggi tutti i test che usano broker
passano da un'unica coppia Redis `:6390` / RabbitMQ `:5673` (+ management `:15673`) protetta da
un lock (`tools/dev/with-brokers`): il giro completo dei gate dura ~60 minuti, quasi tutto in fila.
Macchina: 24 core, 117 GB RAM. Obiettivo: **giro completo dei gate ≤ 20 minuti** con lo stesso
esito di oggi, e possibilità per più agenti/worktree di lavorare in parallelo senza disturbarsi.

Leggi `AGENTS.md`. Questa spec ti autorizza esplicitamente a modificare `tools/compat/` e
`tools/dev/` **solo** per rendere configurabili gli indirizzi dei broker (nessun cambio alla
logica dei confronti, ai casi, alle normalizzazioni o alle soglie). `celery/` e `celery/t/` restano
intoccabili.

## Progetto
1. **Corsie**: `infra/docker-compose.yml` parametrizzato (o un file per corsia) per N corsie
   (default 4, configurabile), ognuna con il proprio Redis e RabbitMQ su porte proprie. La corsia 0
   resta sulle porte attuali (`6390`, `5673`, `15673`), così chi non usa le corsie non cambia nulla.
   Avvio/arresto: `tools/dev/lanes up|down|status`.
2. **`tools/dev/with-brokers`**: prende in esclusiva **una corsia libera** (flock non bloccante su
   un file per corsia, attesa se tutte occupate), esporta le variabili d'ambiente della corsia ed
   esegue il comando; rientrante come oggi (dentro un comando già sotto lock riusa la stessa
   corsia). Variabili (nomi unici, documentati in `AGENTS.md`):
   `CELERY_RS_LANE`, `CELERY_RS_REDIS_URL` (es. `redis://127.0.0.1:6390`, senza db),
   `CELERY_RS_AMQP_URL` (es. `amqp://guest:guest@127.0.0.1:5673//`),
   `CELERY_RS_RABBIT_API` (es. `http://127.0.0.1:15673/api`).
3. **Tutti i punti che oggi hanno porte fisse** leggono queste variabili con default sulla corsia 0:
   `python/tests_rs/*`, `crates/*/tests` e test Rust, `crates/celery-py/tests`, `tools/compat/*`,
   `bench/*`. Nessuna porta `6390`/`5673`/`15673` deve restare scritta altrove se non come default
   (verifica con `rg`).
4. **`tools/dev/gates`**: esegue tutti i gate (elenco sotto) **in parallelo sulle corsie**, ognuno
   con log proprio in `target/gates/<nome>.log`, e alla fine stampa una tabella nome / esito /
   durata / riga di sintesi (passati, differenze, regressioni) e termina con codice ≠ 0 se un gate
   fallisce. `tests_rs` va suddiviso in più gruppi (per file, bilanciati per durata) che girano in
   corsie diverse. Opzione `--only nome,...` per un sottoinsieme. È **un unico comando bloccante**:
   chi lo lancia aspetta la fine senza dover controllare l'avanzamento.
5. **Pulizia**: alla fine di ogni gate (anche se interrotto con Ctrl-C/SIGTERM) nessun processo
   worker figlio resta vivo né connesso ai broker della corsia (process group dedicato e kill
   dell'intero gruppo). Prima di usare una corsia, `with-brokers` verifica che non ci siano
   connessioni AMQP residue né code esclusive orfane e, se ci sono, le chiude (via API di
   management della corsia) e lo scrive nel log.

## Gate gestiti da `tools/dev/gates`
```
cargo build --release ; cargo clippy --all-targets -- -D warnings ; cargo test
tools/dev/build-package ; tools/dev/build-native
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
.venv-rs/bin/python -m pytest python/tests_rs -q        (in gruppi su più corsie)
./compat api ; ./compat wire ; ./compat upstream --venv .venv-rs
./compat wire-worker --pool solo ; ./compat wire-worker --pool prefork
./compat wire-beat ; ./compat cli
```
build/clippy/build-package/build-native prima (in sequenza), poi tutto il resto in parallelo.

## Verifica
- `tools/dev/lanes up` poi `tools/dev/gates`: stessi esiti di oggi (nota: in `main` è noto un
  fallimento `test_worker_pool.py::test_worker_process_signals[prefork]` con segfault dei figli
  billiard nella suite completa, già presente prima: riportalo, non correggerlo qui) e durata
  totale ≤ 20 minuti; riporta la tabella.
- Due `tools/dev/gates --only tests_rs` lanciati in contemporanea da due shell non interferiscono.
- Ctrl-C a metà di `tools/dev/gates`: nessun processo orfano (`ps`) né connessione residua.

## Regole di lavoro (quota del modello limitata)
Esegui i comandi lunghi **una volta e in modo bloccante** (un solo comando che aspetta la fine,
con timeout adeguato), non in background con controlli periodici dell'avanzamento: ogni controllo
costa una chiamata al modello. Rapporto finale breve: cosa hai fatto, file toccati, tabella dei
gate con esiti e durate reali, cosa resta aperto. Committa nel branch (`08e: …`).
