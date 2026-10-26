# Implementazione e interpretazione

- `--prefill` separa la capacità del worker dalla pubblicazione: worker pronto,
  gruppo sospeso, N pubblicazioni, verifica zero completamenti, ripresa e drain.
  Rimossi i task di warm-up dal conteggio; 66 prove da 10.000 task hanno verificato
  esattamente N completamenti e zero completamenti prima della ripresa.
- `bench-toplevel` importa redis-py nel modulo dei task prima del fork e usa lo
  stesso INCR della variante con import nel figlio.
- AMQP riusa il canale delle dichiarazioni solo per il consumer temporaneo pidbox.
  I consumer ordinari mantengono canali, QoS e tag indipendenti. Il controllo riusa
  il descrittore del pool esistente, rispettando fallback, override e disabilitazione.
- Eliminato il `queue.delete` esplicito dopo il ping AMQP: nel riferimento
  `kombu.transport.base.StdChannel.after_reply_message_received` è vuoto.
  Il broker esegue l'auto-delete; una reply queue durevole deve invece sopravvivere.
  Quest'ultimo comportamento ha un test differenziale con il client di riferimento.
- L'attesa dei risultati continua a cedere la CPU quando il risultato è pendente;
  evita il `sleep(0)` successivo alla risposta finale.

## Profilazione e varianti

La profilazione è separata dalle latenze. Nel master rs Redis c10, su 10.101 task,
ack sincroni: 2,355 s (233 µs/task); pickle IPC: 0,180 s (18 µs/task);
submit al pool: 0,064 s (6 µs/task). L'attesa epoll include avvio, idle e pausa del
prefill, quindi non è interpretabile come costo CPU del task. I profili testuali
sono conservati insieme ai JSON.

Il solo riuso del canale (`after-latency-*.json`, variante intermedia) non bastava:
ping p50 rs 9,68/9,26 ms contro 8,79/8,82 del ref, su worker ref/rs.
Aggiungendo pool e rimozione del yield finale, ma conservando ancora il delete
(`candidate-latency-*.json`), ping 8,89/9,07 ms; rimaneva un p99 di 12,48 ms
contro 11,23 del riferimento sul worker rs. Queste soluzioni parziali non sono
state selezionate. La versione finale è documentata dai file `isolated-*`.
TCP_NODELAY era già attivo in tcp-stream: nessuna modifica Nagle da adottare.
Non sono stati cambiati prefetch o raggruppati gli ack: il profilo identifica il
costo, ma non dimostra una variante compatibile e più veloce da adottare.

## RAM e compromesso

Le sonde isolate mostrano un aumento massimo di 0,14 MiB per client/worker rs e
nessuna crescita del beat oltre 0,03 MiB. Nei benchmark c10 con 10.000 task i
picchi mediani crescono in alcuni casi oltre 0,5 MiB: fino a circa 1,6 MiB
(AMQP bench), comunque meno dello 0,8% della PSS. Redis bench-toplevel cresce
circa 1,1 MiB; pure Redis circa 0,5 MiB. Sono valori esplicitamente conservati
nella tabella, non considerati zero. La stessa versione mostra una variazione
fra ripetizioni di 2,4 MiB (AMQP bench) e 0,8 MiB (Redis bench-toplevel).

La scelta finale accetta conservativamente anche questi piccoli incrementi per
il miglioramento misurato rispetto alla versione iniziale: RPC p50 sul worker ref
3,326 → 2,803 ms (−15,7%); ping sul worker rs 9,916 → 8,487 ms (−14,4%).
RPC è anche il 20,4% più veloce del client ref sul worker ref. Il confronto isolato
non indica una crescita strutturale sopra la soglia di 0,5 MB; i picchi dei task
con import/connessioni Redis restano sensibili al copy-on-write e al GC.

## Limiti rimasti

- Le sonde distinguono publish client, andata fino al figlio, task e risposta;
  broker, ricezione master e IPC non sono cronometrati separatamente.
- `bench-toplevel` risparmia circa 76–80 MiB di picco rispetto all'import nel
  figlio, ma non raggiunge `pure`: rimangono client Redis, connessioni e COW.
- La capacità Redis c10 resta limitata soprattutto dagli ack sincroni; nessuna
  variazione di protocollo o traffico del broker è stata introdotta per aggirarli.
- Il publisher prefill deve finire entro l'heartbeat del broker (vedi README).

Nessuna dipendenza aggiunta; sorgenti di riferimento, test upstream e tool di
conformità non modificati.
