# GIF del README — verifica 11b

Rigenerazione: `uvx --with pillow python tools/docs/make_readme_gif.py`.
Richiede i sorgenti in `celery/celery/`, le quattro dipendenze della `.venv-ref`
e i font DejaVu di sistema. Nessuna dipendenza del prodotto aggiunta.

Output: `docs/assets/celery-rs-conversione.gif`, 960 × 580, 201 fotogrammi,
12,5 fps durante i 16 s di avanzamento, pausa finale di 5 s, ciclo infinito.
Dimensione verificata: **897.720 byte**, sotto il limite di 4 MB.
Una sola palette globale a 128 colori, salvataggio con `optimize=True`.
Gli estratti PNG vengono scritti in `target/readme-gif/`.

## Percentuali

| Pacchetto | LOC del ruolo sostituito / LOC totali |
|---|---:|
| celery/app (include gli altri moduli Celery, canvas e trace) | 0,00% |
| celery/worker | 4,16% |
| celery/concurrency | 72,31% |
| celery/backends | 1,50% |
| celery/beat+schedules | 6,23% |
| celery/events | 0,00% |
| celery/bin | 0,00% |
| celery/utils | 0,00% |
| kombu | 27,57% |
| billiard | 42,27% |
| amqp/redis-py | 36,67% |

Globale: **45,65%**, escludendo `originale` e `assente` dal denominatore.
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

- `celery/concurrency/{prefork,asynpool}.py` e `celery/worker/loops.py`:
  il ruolo operativo è scavalcato da rs_prefork, zygote e rsloop. I wrapper Python
  rimangono importabili. La classificazione riguarda il ruolo nel percorso nativo.
- `kombu/{connection,messaging,message,pools,resource}.py` e trasporti supportati:
  il ruolo I/O passa a FastConnection/RsProducer/RsMessage. Le API pubbliche Kombu
  possono ancora essere importate e i fallback restano disponibili.
- `billiard/{pool,queues,connection,process,popen_fork}.py`: supervisione e IPC
  sostituiti; fork e trace mantengono anche codice Python nello zygote e nei figli.
- Client AMQP/Redis: solo protocolli, socket e comandi del percorso supportato
  sono classificati Rust; cluster, sentinel, asyncio e funzionalità opzionali
  Redis restano `originale`. Eccezioni, tipi e utilità restano Python.
- `schedules.py`, `backends/rpc.py`, `kombu/compression.py`: quote parziali
  ricavate dalle righe dei simboli elencati nei commenti TOML. Per la compressione
  si considera il ruolo dei codec nativi; registry e codec personalizzati restano
  Python. Le celle miste hanno entrambi i colori.
- Gli altri file misti, inclusi app, beat, eventi, CLI e backend Redis, sono
  conservativamente Python: delegare chiamate a I/O nativo non converte tutta
  l'orchestrazione. Le loro barre possono quindi terminare a zero.

## Verifiche eseguite

- Comando di rigenerazione: riuscito; i vincoli di formato, durata, ciclo,
  inventario e dimensione sono controllati dallo script.
- Avvio e arresto di un worker reale `memory://`, backend memory, pool solo,
  concurrency 1, sotto `tools/dev/with-brokers`, usando `.venv-rs` e il guscio
  di questo worktree via `PYTHONPATH`: exit 0. Snapshot dei moduli in
  `target/readme-gif/imports.json`: presenti `_rs_consumer`, `_rs_mailbox`,
  `_rs_producer`, oltre a moduli API di Kombu, billiard e AMQP. L'importazione
  non implica che il relativo I/O Python venga eseguito. Il probe solo non
  verifica il pool prefork: per quello la mappa usa i punti di deviazione del sorgente.
- Estratti inizio, 25%, 50%, 75%, finale controllati visivamente: testo nei
  pannelli, nessuna sovrapposizione; righe lunghe abbreviate con ellissi.
- Parsing binario GIF: una palette globale, nessuna palette locale.
- `python -m py_compile tools/docs/make_readme_gif.py`: riuscito.
- `git diff --check`: riuscito. Nessuna modifica a Rust, guscio Python,
  riferimento Celery o strumenti di compatibilità. Cargo non eseguito,
  come prescritto dalla spec. Nessun push.
