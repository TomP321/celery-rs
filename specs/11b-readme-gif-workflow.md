# 11b — GIF del README in stile "dynamic workflow" (sostituisce la 11a)

La GIF della 11a (rettangoli statici con elenchi) non va bene. L'utente vuole un'animazione come
quella del blog di Bun "Rewriting Bun in Rust" (https://bun.com/blog/bun-in-rust, sezione
"Compiler errors as a work queue"): un riquadro scuro che si anima, con i file di ogni macro
pacchetto che si convertono in Rust uno dopo l'altro e una barra di percentuale per pacchetto
che si riempie. Leggi `AGENTS.md`. **Non modificare codice di celery-rs** (`python/celery/`,
`crates/`).

## Com'è il riferimento di Bun (da riprodurre nello stile, non copiare marchi)
Sfondo quasi nero (`#0b0c10`), bordo arrotondato grigio scuro, tutto in monospace.
- In alto a sinistra un badge a pillola arancione (`#e8946f`, bordo `#d97757` semitrasparente)
  con il testo "✻ CLAUDE CODE · DYNAMIC WORKFLOW".
- Un grande contatore bianco in grassetto che cambia ("≈13,376 errors left"), con un'etichetta
  grigia, e accanto una data/ora che avanza.
- A sinistra una colonna stile terminale `errors.txt` con righe `error:` in rosso che scorrono.
- Al centro "DIVVIED UP · 64 CLAUDES": 4 riquadri `worktree 1…4`, ognuno con righe di quadratini
  (fasi fix → review → apply, in giallo, viola e verde) e particelle luminose che si muovono.
- A destra "→ COMMITS LAND PER CRATE": per ogni crate nome, barra colorata che si riempie e numero.
- In basso un log di commit che scorre (`· phase-d(bundler): …`), l'ultima riga più chiara.

## La nostra versione
Badge: "✻ CLAUDE CODE · DYNAMIC WORKFLOW" (celery-rs è stato scritto con Claude Code e codex:
è corretto). Titolo piccolo: "CELERY → RUST". Testi in italiano.

1. **Contatore grande**: percentuale del percorso di esecuzione in Rust che sale da 0 a quella
   finale (es. "62% del runtime in Rust"); accanto la fase corrente (`fase 1 · protocollo`, …,
   `fase 10 · RAM`), dalle fasi di `PLAN.md`.
2. **Colonna sinistra `porting.log`**: righe tipo `port  kombu/transport/redis.py → celery-transport/src/redis/…`
   in arancione, `keep  celery/canvas.py (API Python)` in blu. Devono essere mappature reali
   (vedi sotto).
3. **Centro: una griglia di file per macro pacchetto** (questo è il cuore): ogni quadratino è un
   file `.py` del riferimento. I file che celery-rs sostituisce con Rust si accendono da grigio
   ad arancione uno dopo l'altro (con un'etichetta `.py → .rs` o una particella luminosa che va
   verso la barra del pacchetto); quelli che restano Python diventano blu; quelli non usati da
   celery-rs (altri broker/backend, eventlet/gevent) restano grigio scuro. Macro pacchetti:
   `celery/app`, `celery/worker`, `celery/concurrency`, `celery/backends`, `celery/beat+schedules`,
   `celery/events`, `celery/bin`, `celery/utils`, `kombu` (trasporti, messaggi, serializzazione),
   `billiard` (pool), `amqp`/`redis-py` (client dei broker). Se servono troppi quadratini
   raggruppali, ma resta leggibile.
4. **Colonna destra "→ IN RUST PER PACCHETTO"**: nome del pacchetto, barra che si riempie,
   percentuale finale (righe di codice del pacchetto sostituite da Rust / righe totali).
5. **Log in basso**: messaggi di commit **veri** del repository (`git log --format=%s`, scegli
   quelli significativi, accorciati), che scorrono; l'ultima riga più chiara.
6. **Fotogramma finale** (pausa ~5 s): barre complete e un riepilogo: 23.300 righe di Rust in 6
   crate; worker `-c 10` 300 → 73 MB (−76%); in 8 GB 27 → 96–105 worker; Redis task/s ×3.

## Dati: niente numeri inventati
Crea `tools/docs/conversion_map.toml` che assegna a **ogni** file `.py` dei pacchetti sopra
(da `celery/celery/` e da `.venv-ref/lib/python3.13/site-packages/{kombu,billiard,amqp,redis}`)
uno stato: `rust` (in celery-rs il suo ruolo a runtime è svolto da un crate: indica quale),
`python` (resta Python per API pubblica, eseguito anche in celery-rs), `originale` (percorso non
convertito: altri trasporti/backend, usato solo se l'utente li configura), `assente`
(non supportato: eventlet/gevent). Regola per decidere: guarda in `python/celery/` cosa è stato
sostituito o scavalcato dai moduli `_rs_*` e da `celery._rs`/`_celery_rs`, e cosa è importato da
un worker/beat celery-rs reale (puoi verificarlo con `python -X importtime` o `sys.modules` in un
worker `memory://` avviato e fermato subito, sotto `tools/dev/with-brokers`). Un file parzialmente
sostituito può avere una frazione (`rust = 0.6`), motivata in un commento. Lo script calcola le
percentuali dai conteggi di righe e dalla mappa; il contatore globale = righe `rust` / righe di
`rust`+`python` (escludi `originale` e `assente`). Riporta nel rapporto le percentuali uscite.

## Forma
- Script `tools/docs/make_readme_gif.py` (sostituisci quello della 11a), Pillow, eseguibile con
  `uvx --with pillow python tools/docs/make_readme_gif.py`; font DejaVu Sans Mono / Sans
  (`/usr/share/fonts/truetype/dejavu/`).
- Output `docs/assets/celery-rs-conversione.gif` (sovrascrive), larghezza 960 px, altezza
  ~540–600 px, 12–15 fps, 15–25 s, ciclo infinito, ≤ 4 MB (palette ottimizzata, un'unica palette
  globale, `optimize=True`; se serve riduci fps, non la leggibilità). Animazione fluida: le barre
  e il contatore crescono gradualmente, non a scatti di fase.
- Aggiorna il testo alternativo nel `README.md` (la riga c'è già dopo l'introduzione).
- Controllo visivo obbligatorio: estrai almeno 5 fotogrammi (inizio, 25%, 50%, 75%, finale) in PNG
  e guardali: niente testo tagliato o sovrapposto, contrasto leggibile.

## Regole
Comandi bloccanti, niente messaggi di attesa. Niente cargo build/test: non tocchi Rust (basta
`git diff --stat` che lo dimostri). **Non fare push.** Commit nel branch (`11b: …`), rapporto
breve: file, dimensione/durata/fps della GIF, percentuali per pacchetto e globale, file dubbi
nella mappa.
