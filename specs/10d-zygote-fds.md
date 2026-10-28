# 10d — Zygote: non chiudere i descrittori aperti dal codice utente

Revisione dell'orchestratore. `python/celery/concurrency/_rs_zygote.py::_main` importa i moduli
dell'applicazione (`app.loader.import_default_modules()`, precarichi) e **poi** chiama
`_close_inherited_fds`, che chiude tutti i descrittori tranne stdio, il controllo e gli stream dei
logger. Così chiude anche file, socket e connessioni aperti dal codice utente a livello di modulo
(es. `DATA = open('dati.txt')`, un client creato all'import). Nel riferimento i figli prefork
(billiard, fork dal master) ereditano tutti i descrittori del master e possono usarli nei task:
qui li trovano chiusi. È una differenza di comportamento da eliminare.

Leggi `AGENTS.md`, `specs/05b-worker-pool.md`, `specs/09a-integration.md` (fd di logging),
`specs/10c-ram-budget.md`. Vincoli invariati.

## Lavoro
1. Lo zygote (e i figli) devono chiudere **solo** i descrittori che appartengono al runtime Rust
   del master (epoll, eventfd/waker, socket del broker e del backend Rust, pipe interne), non
   quelli aperti dal codice Python dell'utente o dalle librerie. Strategie possibili, da scegliere
   con i numeri e la sicurezza: il runtime Rust espone l'elenco dei propri fd (o li apre con
   `O_CLOEXEC` e li marca), oppure si registra l'insieme dei fd prima dell'avvio del runtime Rust e
   si chiudono solo quelli creati dopo dal runtime. Gli fd Rust ereditati non devono mai essere
   usati né chiusi due volte dal figlio (fork safety già verificata in 09b).
2. Test differenziale in `python/tests_rs` (worker rs e worker ref, `-P prefork -c 2`): un modulo
   di task apre all'import un file e un socket di loopback; un task li legge/usa; l'esito deve
   essere identico al riferimento.
3. Test dell'invariante della 10c: dopo l'avvio, lo zygote non apre nuovi fd (conteggio di
   `/proc/<zygote>/fd` uguale dopo N spawn e respawn di figli con `max_tasks_per_child`); se
   l'invariante si rompesse, i figli erediterebbero fd che non chiudono più.
4. Nessun aumento di RAM (`bench/bench.py --pool prefork --concurrency 10 --task pure --prefill`,
   prima/dopo) e nessuna regressione negli fd di logging (test di 09a).

## Verifica
Test mirati tuoi, bloccanti. **Non** eseguire `tools/dev/gates`, soak o `./compat integration`:
li esegue l'orchestratore.

## Regole di lavoro (quota del modello limitata)
Comandi bloccanti, niente messaggi di attesa o comandi in background. Rapporto breve: strategia
scelta e perché, file toccati, test eseguiti, RAM prima/dopo. Committa nel branch (`10d: …`).
