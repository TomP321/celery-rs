# Corsie dei broker e gate

Avviare `tools/dev/lanes up`, poi eseguire `tools/dev/gates`. Il comando aspetta tutti
i gate, stampa esiti e durate e restituisce un codice non zero se un gate fallisce.
`tools/dev/lanes status` mostra i container; `tools/dev/lanes down` li arresta e rimuove.

`CELERY_RS_LANES` configura il numero di corsie (default 4); usare lo stesso valore
per avvio, esecuzione e arresto. La corsia N usa Redis `6390+N`, AMQP `5673+N` e
management `15673+N`, pubblicati solo su loopback. La corsia 0 mantiene il progetto
Compose `celery-rs`; le altre usano `celery-rs-lane-N`.

`tools/dev/with-brokers comando ...` acquisisce una corsia libera con `flock`,
attendendo quando sono tutte occupate. I lock sono condivisi tra worktree in
`/tmp/celery-rs-broker-lane-N.lock`. Un comando annidato riusa il lock del padre.
Le variabili esportate sono:

| Variabile | Contenuto |
|---|---|
| `CELERY_RS_LANE` | Indice della corsia |
| `CELERY_RS_REDIS_URL` | URL Redis senza database |
| `CELERY_RS_AMQP_URL` | URL AMQP completo |
| `CELERY_RS_RABBIT_API` | URL management con suffisso `/api` |

Restano disponibili gli alias preesistenti `CELERY_RS_TEST_REDIS`,
`CELERY_RS_TEST_AMQP` e `CELERY_RS_TEST_RABBIT_API`; le nuove variabili hanno
precedenza nei test. Il wrapper assegna entrambi gli insiemi alla stessa corsia.

Il wrapper ripulisce le connessioni AMQP e le code esclusive residue prima e dopo
il comando. Usa un gruppo di processi dedicato e un subreaper Linux: raccoglie i
daemon terminati durante il comando e termina tutti i discendenti all'uscita,
anche su SIGINT/SIGTERM e anche se hanno creato altre sessioni.

`gates` esegue build, clippy e installazioni in sequenza; poi distribuisce gli
altri gate sulle corsie. `tests_rs` è suddiviso per file, bilanciando le durate
registrate e usando un interprete separato per ogni file. Le durate dei gate
servono anche a dare precedenza ai lavori più lunghi. Nessuna esclusione o soglia
di conformità è stata modificata.

Sottoinsiemi: `tools/dev/gates --only tests_rs`, oppure nomi separati da virgola:
`build,clippy,build-package,build-native,cargo-test,native-tests,api,wire,upstream,wire-worker-solo,wire-worker-prefork,wire-beat,cli`.
Si può selezionare anche un gruppo `tests_rs-N`. I sottoinsiemi non ricostruiscono
automaticamente il pacchetto.

Log: `target/gates/<nome>.log` punta all'ultima invocazione di quel gate; le copie
permanenti, gli XML e `results.json` sono in `target/gates/<timestamp>-<pid>/`.
Invocazioni simultanee hanno directory distinte. Le durate sono in
`target/gates/durations.json`.

Test dell'infrastruttura: `python3 tools/dev/tests/test_lanes.py -v` (richiede
almeno due corsie avviate e va lanciato fuori da `with-brokers`).

## Verifica del 26 ottobre 2026

Giro completo dopo le correzioni: `tools/dev/gates`, codice 0, **726,8 s (12m 6,8s)**.
Log: `target/gates/20261026-163735-2144576/`.

| Gate | Esito | Durata | Risultato |
|---|---|---:|---|
| build release | OK | 65,6 s | compilazione riuscita |
| clippy | OK | 1,6 s | nessun warning |
| build-package | OK | 53,8 s | installazione riuscita |
| build-native | OK | 1,3 s | installazione riuscita |
| cargo-test | OK | 14,4 s | 234 passati |
| native-tests | OK | 60,4 s | 81 passati |
| tests_rs-1 | OK | 86,9 s | 12 passati |
| tests_rs-2 | OK | 221,0 s | 89 passati |
| tests_rs-3 | OK | 219,8 s | 52 passati |
| tests_rs-4 | OK | 223,5 s | 51 passati |
| api | OK | 1,9 s | 0 differenze, 137 moduli |
| wire | OK | 58,9 s | 0 differenze, 40 scenari |
| upstream | OK | 65,5 s | 31.691 test, 0 regressioni |
| wire-worker solo | OK | 224,1 s | 0 differenze, 22 scenari |
| wire-worker prefork | OK | 260,3 s | 0 differenze, 22 scenari |
| wire-beat | OK | 18,8 s | 0 differenze, 2 broker |
| cli | OK | 275,3 s | 0 differenze, 190 comandi |

La suite upstream ha un fallimento in
`t/unit/contrib/test_pytest.py::test_pytest_celery_marker_registration`, riprodotto
anche con `tools/dev/with-brokers ./compat upstream --venv .venv-ref`; il gate
lo riconosce dal report reale del riferimento, senza nuove esclusioni.
Il segfault noto dei figli billiard compare nei log del primo giro di
`test_worker_pool`, che conclude comunque con tutti i test passati: non è stato corretto qui.

Il primo giro (846,1 s) aveva fallimenti, poi risolti per gli indirizzi Docker,
la raccolta dei daemon e lo stato Python condiviso tra file. Aveva anche tre
differenze wire transitorie: i corrispondenti confronti mirati con l'harness
originale di `51f5b226223ce03738ad980d5ee17ceb0dfc2687` sono passati, così come il
secondo giro completo. Non sono stati cambiati confronti, normalizzazioni o soglie.
`cargo fmt --check`, eseguito durante la revisione, segnala formattazione
preesistente nel workspace; non è un gate della spec e non è stato applicato un
reformat generale.

Prova Ctrl-C su `tools/dev/gates --only tests_rs` dopo 20 s: codice non zero,
42 processi osservati, nessun PID residuo al controllo con `ps`; su ciascuna delle
quattro corsie, zero connessioni AMQP e zero code esclusive. Artefatti in
`target/gates/acceptance/interrupt.log` e `ps-after-interrupt.txt`.

Test finali dell’infrastruttura: **5 test passati in 6,775 s**.

Due shell `tools/dev/gates --only tests_rs` simultanee: codici **0 / 0**,
**204 test passati ciascuna**, durate **377,6 s / 370,3 s** (tempo complessivo
377,7 s), senza interferenze osservate. Log in `target/gates/acceptance/parallel-1.log`
e `parallel-2.log`; directory dei runner `20261026-165027-2238493` e
`20261026-165027-2238494`. Anche il controllo finale dopo entrambe le shell trova
zero connessioni AMQP e zero code esclusive su tutte le corsie.

Verificata inoltre, sotto lock e con una connessione AMQP reale, la chiusura via
management API e la relativa riga di log della pulizia preventiva.

File interessati: `infra/docker-compose.yml`, `AGENTS.md`, `tools/dev/lanes`,
`with-brokers`, `gates`, `test-group`, `tests/test_lanes.py` e questo documento;
indirizzi nei test Python/Rust, nei benchmark e in `tools/compat/celery_compat/{cli,wire}.py`.
Nessuna dipendenza aggiunta e nessuna modifica al core o ai test upstream.

Il controllo mirato finale ha rilevato che RabbitMQ può pubblicare la coda
esclusiva nel management prima della sua connessione proprietaria: un tentativo
di DELETE in quella finestra restituisce HTTP 400. La pulizia ora ripete la
lettura e la chiusura per un massimo di 15 s, poi segnala un errore se i residui
persistono. Questa ultima correzione, successiva al giro completo, è coperta dal
test reale `test_management_closes_real_exclusive_owner`, dai cinque test
infrastrutturali e dalla ripetizione della prova Ctrl-C.

Ripetizione Ctrl-C dopo la correzione finale: codice 1, **43 processi osservati,
zero residui**, zero connessioni AMQP e zero code esclusive sulle quattro corsie.
