# GIF del README — verifica 11b

Rigenerazione: `uvx --with pillow python tools/docs/make_readme_gif.py`.
Richiede i sorgenti in `celery/celery/`, le quattro dipendenze della `.venv-ref`
e i font DejaVu di sistema. Nessuna dipendenza del prodotto aggiunta.

Output: `docs/assets/celery-rs-conversione.gif`, 960 × 680, 201 fotogrammi,
12,5 fps durante i 16 s di avanzamento, pausa finale di 5 s, ciclo infinito.
Dimensione verificata: **646.648 byte**, sotto il limite di 4 MB.
Una sola palette globale a 128 colori, salvataggio con `optimize=True`.
Gli estratti PNG vengono scritti in `target/readme-gif/`.

Griglia di 4 colonne × 3 righe: 11 pannelli nominati e un `porting.log`
compatto. Celle di 8 × 8 px, passo di 10 px (2 px di spazio), a capo.
Solo `amqp/redis-py` raggruppa fino a due file per cella, segnalati nel pannello.
I file misti hanno una sottolineatura blu. Ogni conversione Rust genera un lampo
sulla cella e una particella con scia che viaggia per circa 8 fotogrammi verso
la barra corrispondente. Le particelle evitano le aree delle etichette.

Titolo: «…% del codice di runtime in Rust»; sottotitolo:
«righe del riferimento il cui ruolo è svolto da Rust».

## Percentuali

| Pacchetto | LOC del ruolo sostituito / LOC totali |
|---|---:|
| celery/app (include gli altri moduli Celery, canvas e trace) | 0,34% |
| celery/worker | 5,37% |
| celery/concurrency | 72,31% |
| celery/backends | 3,79% |
| celery/beat+schedules | 6,23% |
| celery/events | 1,27% |
| celery/bin | 0,00% |
| celery/utils | 0,00% |
| kombu | 27,57% |
| billiard | 42,27% |
| amqp/redis-py | 36,67% |

Globale: **46,00%**, escludendo `originale` e `assente` dal denominatore.
Conteggio di righe fisiche, incluse righe vuote e commenti. L'inventario comprende
404 file, anche tutti i moduli Celery esterni ai macro pacchetti richiesti:
questi ultimi sono aggregati ad app per conservare il costo dell'API Python.
Lo script rifiuta file mancanti, duplicati, stati sconosciuti e crate inesistenti.

La percentuale descrive la copertura dei ruoli pesata sulle LOC del riferimento,
non una misura del tempo CPU, né delle righe Rust prodotte. Le fasi e l'ordine di
accensione illustrano il percorso di PLAN.md; non sono una ricostruzione temporale
file per file. I messaggi in basso provengono da `git log --reverse --format=%s`.
Le misure del riepilogo provengono dal README e dal commit della flotta 10b
(105 worker a riposo, 96 sotto carico), già presenti nel repository.

## Casi misti e dubbi esplicitati

- Si usa lo stesso criterio di ruolo per Celery e dipendenze. Un simbolo con
  ruolo I/O scavalcato può mantenere wrapper Python: non equivale a una traduzione
  letterale del suo corpo. Nei simboli che mescolano invio, API, segnali o stato
  si contano solo le righe delle chiamate delegate. Le unioni di intervalli
  evitano il doppio conteggio, anche per chiamate dentro un simbolo già contato.
- `rust_symbols` elenca i simboli interi; `rust_calls` usa `simbolo|chiamata`.
  Il generatore ricalcola le quote con AST dal riferimento e rifiuta frazioni
  incoerenti o simboli/chiamate scomparsi. I commenti TOML indicano numeratore,
  denominatore, punto di deviazione `_rs_*` e comportamento ancora Python.
- Consumer: connessioni, registrazione I/O, sottoscrizioni e QoS passano a
  `RsConnection`/`RsTaskConsumer`. Gossip e pidbox usano `_rs_mailbox`; mingle usa
  `_rs_control`. Segnali, bootstep, callback, Panel e stato dei nodi restano Python.
- `worker/strategy.py` conserva la strategia Python: si conta solo l'invio
  `task-received` via RsProducer. `heartbeat.py` conserva timer, raccolta metriche
  e segnale `heartbeat_sent`: si conta solo l'invio. Il bootstep heart conserva
  la costruzione di Heart. Gli heartbeat AMQP, distinti da quelli del worker,
  sono gestiti dal runtime Rust; synloop e fallback non sono contati in loops.
- Backend Redis: quote per accessi KV, connessione e consumer pubsub delegati
  a `_rs_redis`. Callback, metadati, logica chord e API rimangono Python.
  Backend RPC: consumer I/O `_rs_rpc` e pubblicazione RsProducer; cache,
  conversione dei risultati e callback Python escluse.
- `app/amqp.py`: TaskConsumer, producer pool e chiamata publish, tramite
  `_rs_consumer`/`_rs_producer`; routing, costruzione Task v1/v2 e segnali esclusi.
  `app/control.py`: ruolo I/O broadcast tramite `_rs_control`; le API Inspect,
  gli altri metodi Control e Panel rimangono Python. La percentuale del gruppo
  app è diluita anche da canvas, trace, loader e altri moduli aggregati.
- `events/dispatcher.py`: ruolo I/O `_publish` e creazione RsProducer; clock,
  Event, API send/publish e gestione buffer rimangono Python.
- `kombu/{connection,messaging,message,pools,resource}.py` e trasporti supportati:
  ruolo I/O nativo, pur con tipi pubblici ancora importabili. Billiard:
  supervisione/IPC sostituiti, zygote e trace mantengono codice Python.
  AMQP/Redis: solo il percorso supportato è Rust; funzionalità opzionali come
  cluster, sentinel e asyncio restano `originale`; tipi/eccezioni restano Python.
- `schedules.py` e `kombu/compression.py` mantengono le quote misurate della
  prima versione: crontab e codec nativi; fusi orari, registry e codec custom Python.

### Quote dei file Celery rivisti

| File del riferimento | Quota Rust del ruolo |
|---|---:|
| `celery/app/amqp.py` | 3.89% |
| `celery/app/control.py` | 4.56% |
| `celery/backends/redis.py` | 18.69% |
| `celery/backends/rpc.py` | 20.49% |
| `celery/events/dispatcher.py` | 9.54% |
| `celery/worker/consumer/connection.py` | 24.07% |
| `celery/worker/consumer/consumer.py` | 10.82% |
| `celery/worker/consumer/events.py` | 5.33% |
| `celery/worker/consumer/gossip.py` | 5.34% |
| `celery/worker/consumer/heart.py` | 5.56% |
| `celery/worker/consumer/mingle.py` | 12.50% |
| `celery/worker/consumer/tasks.py` | 9.68% |
| `celery/worker/heartbeat.py` | 9.84% |
| `celery/worker/loops.py` | 50.22% |
| `celery/worker/pidbox.py` | 7.94% |
| `celery/worker/strategy.py` | 4.04% |

## Verifiche eseguite

- Comando di rigenerazione: riuscito; i vincoli di formato, durata, ciclo,
  inventario e dimensione sono controllati dallo script.
- Verifica eseguita nella prima implementazione, invariata in questa revisione:
  avvio e arresto di un worker reale `memory://`, backend memory, pool solo,
  concurrency 1, sotto `tools/dev/with-brokers`, usando `.venv-rs` e il guscio
  di questo worktree via `PYTHONPATH`: exit 0. Snapshot dei moduli in
  `target/readme-gif/imports.json`: presenti `_rs_consumer`, `_rs_mailbox`,
  `_rs_producer`, oltre a moduli API di Kombu, billiard e AMQP. L'importazione
  non implica che il relativo I/O Python venga eseguito. Il probe solo non
  verifica il pool prefork: per quello la mappa usa i punti di deviazione del sorgente.
- Estratti inizio, 25%, 50%, 75%, finale controllati visivamente: testo nei
  nuovi pannelli nominati, nessuna sovrapposizione o testo tagliato; celle da 8 px,
  lampi e particelle visibili; righe lunghe del log abbreviate con ellissi.
- Parsing binario GIF: una palette globale, nessuna palette locale.
- `python -m py_compile tools/docs/make_readme_gif.py`: riuscito.
- `git diff --check`: riuscito. Nessuna modifica a Rust, guscio Python,
  riferimento Celery o strumenti di compatibilità. Cargo non eseguito,
  come prescritto dalla spec. Nessun push.
