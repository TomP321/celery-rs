# Capacità e latenze (08c)

Eseguire sempre sotto `tools/dev/with-brokers`, che assegna una corsia esclusiva.

```sh
tools/dev/with-brokers .venv-ref/bin/python bench/run_08c.py --phase before --n 10000 --repeats 1
tools/dev/with-brokers .venv-ref/bin/python bench/probe_08c.py --phase isolated-before
tools/dev/with-brokers .venv-ref/bin/python bench/memory_08c.py --phase isolated-before
# Dopo la modifica e tools/dev/build-package:
tools/dev/with-brokers .venv-ref/bin/python bench/run_08c.py --phase after --n 10000 --repeats 1
tools/dev/with-brokers .venv-ref/bin/python bench/probe_08c.py --phase isolated-after
tools/dev/with-brokers .venv-ref/bin/python bench/memory_08c.py --phase isolated-after
.venv-ref/bin/python bench/report_08c.py
```

`bench.py --prefill` attende che il worker sia pronto e sospende l'intero gruppo
(master, zygote e figli) con `SIGSTOP`. Pubblica esattamente N task, verifica che
nessuno sia stato completato e riprende il gruppo con `SIGCONT`. Il cronometro
parte immediatamente prima della ripresa. In questo modo avvio, connessioni e
pubblicazione non entrano nella capacità del worker. La risoluzione del conteggio
è 20 ms: usare migliaia di task, non le piccole N dei test funzionali. Il publisher
deve terminare entro il timeout heartbeat del broker, perché durante la pausa
anche gli heartbeat del worker sono sospesi.

La memoria è campionata ogni 50 ms sull'intero albero, includendo zygote e figli;
il picco include anche la successiva sonda `delay().get()`. Sono conservati PSS e
Private_Dirty di ciascun processo. `pure` conta i completamenti con append su file;
`bench` usa INCR Redis e importa redis-py nel figlio; `bench-toplevel` esegue lo
stesso INCR, con redis-py importato nel modulo dei task prima del fork. Il confronto
di capacità fra `pure` e gli altri task include quindi anche il costo del contatore.
Non vengono aggiunti task di warm-up al conteggio del throughput.

`probe_08c.py` incrocia client ref/rs con worker ref/rs AMQP (prefork c10, oppure
`--pool solo`). Le latenze di 1.000 chiamate sono raccolte senza profiler; un secondo
giro di 100 chiamate produce i profili cProfile. Il task `timed` usa l'orologio
monotono comune ai processi della stessa macchina: distingue andata fino all'ingresso
nel figlio, esecuzione e ritorno della risposta. L'andata aggrega pubblicazione,
broker, master e IPC; non permette di attribuire separatamente a ciascuno il tempo
speso. `publish_ms` misura la durata della sola chiamata `delay()` sul client.

I JSON sono in `bench/results/08c/`; log e profili binari restano locali. Il report
conserva gli esiti reali dei gate e le eventuali limitazioni della misura. Le PSS
vanno confrontate a parità di altre sonde attive: processi Python aggiuntivi cambiano
la ripartizione delle pagine condivise.
