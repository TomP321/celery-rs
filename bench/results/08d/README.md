# 08d — worker prefork c10

Misure su questo worktree, Python 3.13.9, 30.000 task, 500 sonde di latenza,
Redis :6390 e RabbitMQ :5673. Memoria in MiB (1024 KiB), PSS sommata su tutto
l'albero; `after` comprende anche le sonde `add.delay().get()`. I JSON contengono
anche `throughput_processes`, prima delle sonde, quando disponibile.

Ogni combinazione ref/rs × Redis/AMQP × bench/pure ha due esecuzioni prima e due
dopo. La prima coppia Redis/bench della base ha memoria sensibilmente maggiore
della seconda: il rapporto conserva entrambe e il loro intervallo, senza
attribuire tutta la differenza prima/dopo alla modifica. Le prime compilazioni
Rust si sono svolte durante parte della matrice iniziale; la matrice finale è
stata eseguita dopo la fine della compilazione, sotto un unico lock dei broker.

Riproduzione:

```sh
tools/dev/with-brokers .venv-ref/bin/python bench/run_c10.py --phase after
python3 bench/report_c10.py --phase after > bench/results/08d/after.md
```

`before.md` e `after.md` riportano medie/intervalli, risparmio in MiB e %, PSS e
Private_Dirty di master, zygote e di ciascun figlio per ogni esecuzione.

## Modifiche

- Lo zygote riscalda realmente il parser CFFI usato da billiard per `prctl`:
  importare `cffi.cparser` lasciava ancora vuota la cache globale del parser.
  Il riscaldamento avviene prima del congelamento GC e del fork dei figli.
- Il reader IPC Rust trasferisce il buffer ricevuto a `Bytes`, senza copiarne
  il payload. Il figlio Python passa la vista del buffer direttamente a pickle.
  Non cambiano formato dei frame, traffico broker, callback o limiti del pool.
- Nessuna modifica alle soglie o all'abilitazione GC. Una sonda isolata confronta
  questi valori col riferimento, con impostazioni predefinite e personalizzate
  (GC disabilitato), su dieci figli; dopo 2.000 job di warm-up misura altri
  30.000 job, con meno di 2 MiB di crescita PSS ammessa per figlio.
- Il benchmark registra PID, ruolo, PSS, Private_Dirty, broker e fase; usa code
  e contatori unici, senza FLUSHALL. Runner e generatore del rapporto inclusi.

## Varianti

Otto prove isolate (`variant-*.json`), due per variante/task, Redis c10:

| variante | task | idle MiB | picco/dopo MiB | task/s |
|---|---|---:|---:|---:|
| CFFI caldo | bench | 84,0–84,3 | 231,8–231,9 | 2610–2612 |
| CFFI caldo | pure | 84,0–84,1 | 102,3–103,1 | 2601–2608 |
| CFFI + redis-py precaricato | bench | 96,4–97,0 | 158,2–160,1 | 1541–2506 |
| CFFI + redis-py precaricato | pure | 96,2–96,7 | 114,4–114,6 | 2368–2592 |

Adottato il riscaldamento CFFI: circa 3 MiB idle in meno rispetto alle misure
calde della base, senza rallentamento rilevato. Scartato il precarico automatico
di redis-py: risparmia circa 73 MiB col task che lo importa, ma costa circa
12 MiB al task `pure`, e introduce un import che l'applicazione potrebbe non
usare. La prima prova di preload ha inoltre throughput molto più basso della
seconda; non viene presentata come miglioramento prestazionale.
Il benchmark supporta `--include redis` per misurare anche il precarico esplicito
tramite l'opzione già esistente del worker, se l'applicazione usa redis-py.

`pickle-copy.json`: 100 decodifiche di un job con argomento da 8 MiB, con
tracemalloc. Copia bytes: 16.002 MiB temporanei, 0,185 s; vista diretta:
8,003 MiB, 0,082 s. La prova isola la copia evitata; non è una previsione del
throughput del broker. Il test Rust aggiunge un payload da 1 MiB seguito da un
secondo frame e verifica che i payload restino validi dopo le letture successive.

Nessuna nuova dipendenza. Mimalloc/arene non riprovati: la revisione 08b-review2
richiede di conservare la decisione già misurata (mimalloc solo 34→40 MiB,
prefork 71→77 MiB; nessun guadagno dalle arene glibc).

## Risultati finali

Medie di due esecuzioni. PSS in MiB, riferimento → celery-rs:

| broker/task | idle | picco = dopo | risparmio idle | risparmio picco/dopo | task/s ref → rs | p50/p99 ms ref → rs |
|---|---:|---:|---:|---:|---:|---|
| Redis bench | 308,7 → 83,9 | 313,2 → 231,4 | 224,8 (72,8%) | 81,9 (26,1%) | 897 → 2674 | 3,11/4,08 → 1,91/4,28 |
| Redis pure | 308,6 → 84,2 | 310,6 → 101,4 | 224,4 (72,7%) | 209,2 (67,3%) | 916 → 2553 | 3,13/4,34 → 1,91/3,97 |
| AMQP bench | 307,0 → 83,9 | 319,6 → 233,1 | 223,2 (72,7%) | 86,5 (27,1%) | 2921 → 3642 | 3,26/3,99 → 2,63/4,35 |
| AMQP pure | 306,7 → 83,5 | 310,3 → 103,1 | 223,2 (72,8%) | 207,2 (66,8%) | 2970 → 3706 | 3,33/4,39 → 2,51/4,34 |

Rispetto alla base calda (seconda esecuzione Redis/bench), il picco del task
`bench` resta sostanzialmente invariato (231,7 → 231,4 MiB), mentre idle passa
87,4 → 83,9 MiB. `pure` Redis passa da 104,2 MiB medi a 101,4 MiB dopo il carico.
Su AMQP `pure` la base ha throughput 3979–4122 task/s, il finale 3662–3750:
le misure mostrano una variabilità/regressione da riportare, senza attribuirla
come fatto al cambiamento delle copie IPC; il confronto isolato CFFI su Redis
non evidenzia questo rallentamento. Le latenze p99 del finale AMQP restano
circa 4,3 ms. Non si rivendica una scalabilità lineare con dieci figli.

Nei JSON finali rs il drain dopo l'ultimo publish è 0–5 ms e il rate del client
è quasi uguale all'end-to-end (Redis circa 2.600/s, AMQP circa 3.700/s).
Questo carico misura quindi soprattutto il limite di pubblicazione: aumentare
i figli oltre quattro non può produrre scalabilità lineare mentre il client
non alimenta una coda più velocemente. Questo è un limite del benchmark e
non una dimostrazione di saturazione del pool c10.

`bytes-micro.txt` confronta due milioni di buffer piccoli (oppure 1.000 da
1 MiB) in release: copia del payload (`mode=0`), trasferimento con `slice`
(`mode=1`), trasferimento con `advance` (`mode=2`). Sui buffer da 32 byte,
47,7 / 44,0 / 28,2 ns; da 1 MiB, circa 1163 / 20,4 / 19,9 µs. Il percorso
implementato è `slice`, che evita la seconda allocazione/copia del corpo;
questa microprova non dimostra un guadagno end-to-end sul broker.
