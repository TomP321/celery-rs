# 08b — revisione della sessione 1 (da applicare prima di dichiarare finito)

Il lavoro della sessione 1 è nel commit `f4f90b9` (wip): dieta degli import di client, worker e
beat, `upstream` a 0 regressioni, `compat wire/wire-worker/wire-beat/cli` a 0. Restano i problemi
qui sotto. Leggi anche `specs/08b-memory.md`.

## Decisione del proprietario del progetto: oggetti kombu veri
Dove il riferimento usa o espone un oggetto di kombu/billiard (classi, funzioni, istanze), celery-rs
deve usare **quell'oggetto vero o una sua sottoclasse con il proprio nome onesto**, anche se costa
memoria (misurato: circa +5 MB nel client per `kombu.utils`, che porta `amqp`, `billiard`,
`importlib.metadata`; +1,5 MB per `kombu.entity`/`kombu.serialization`). Quindi:

1. **Vietato riscrivere `__module__`, `__qualname__`, `__name__`, `__class__`** per far sembrare
   un oggetto quello di kombu/billiard o un altro nome di celery. Togli tutte le occorrenze
   introdotte (in `_compat.py`, `utils/imports.py`, `app/trace.py`, `utils/functional.py`, i
   segnaposto di `_lazy.py`…) e quella già presente in `main` in `app/_fast_mailbox.py`
   (`FastMailbox` → deve essere una sottoclasse di `kombu.pidbox.Mailbox` con il proprio nome).
2. **Niente classi sostitutive**: `celery/app/_fast_entities.py` (Queue/Exchange finte) va
   rimosso; `app.amqp.queues`, `default_exchange`, router, `Queues.new_missing` restituiscono
   `kombu.Queue`/`kombu.Exchange` veri. `producer.connection` deve essere istanza di
   `kombu.connection.PooledConnection` come nel riferimento (prima della 08b `FastConnection`
   derivava da `kombu.Connection`: torna a quel modello, con pool come il riferimento).
   Segnaposto che "fingono" di essere classi di kombu/billiard (`lazy_class_attr` e simili in
   `_lazy.py`) non sono ammessi: o l'oggetto vero, o un `__getattr__` di modulo (PEP 562) che
   restituisce l'oggetto vero al primo accesso.
3. Import pigri **onesti** restano benvenuti: un modulo che usa kombu solo in un ramo raro lo
   importa in quel ramo; un nome re-esportato si risolve con `__getattr__` di modulo; un oggetto
   costoso si crea al primo accesso. Ma tutto ciò che una classe usa nella sua definizione (basi,
   decoratori come `cached_property`) va importato davvero.
4. **Bug trovato**: con `app.conf.task_queues = [kombu.Queue('extra', kombu.Exchange('extra',
   type='direct'), routing_key='extra')]`, `app.amqp.queues['extra']` solleva `TypeError`.

## Test di identità (scritto dall'orchestratore: NON modificarlo)
`python/tests_rs/test_kombu_identity.py`:
- `test_no_masqueraded_names`: ogni classe/funzione raggiungibile dai moduli `celery.*` deve
  essere l'oggetto che si trova davvero a `__module__` + `__qualname__` dichiarati;
- `test_runtime_objects_are_kombu_objects[redis|amqp]`: per code, exchange, connessioni,
  producer, mailbox, dispatcher… le classi kombu della MRO del riferimento devono comparire
  (`isinstance` sulla classe vera) anche in celery-rs.
Oggi fallisce con 19 nomi camuffati e ~20 oggetti a runtime: deve passare tutto.

## Test da ripristinare
- `test_import_diet.py`: rimetti i controlli d'identità veri (`issubclass(mlazy,
  kombu.utils.functional.lazy)`, `is`), non quelli sul nome. Aggiorna l'elenco dei moduli vietati
  a ciò che resta onestamente evitabile (per esempio `yaml`/`msgpack`/`kombu.transport.*`/`redis`/
  `click`/`greenlet` se non servono davvero), motivando ogni modulo rimasto nel messaggio finale.
- `test_producer.py::test_producer_exposes_kombu_connection_without_opening_it`: torna come in
  `main` (connessione kombu, non aperta).

## Obiettivi ancora aperti della spec 08b
- **Client**: nuovo obiettivo PSS dopo `get()` ≤ 26 MB (ref 34,6) con oggetti veri.
- **Figli prefork**: con `--task pure` i figli restano a ~11 MB finché non salvano un risultato;
  dopo i task `add` (risultato nel backend) salgono a ~24 MB ciascuno. Trova cosa importa il
  percorso del risultato nel figlio e caricalo nello zygote prima di `gc.freeze()` o evitalo.
  Obiettivo `--task pure` ≤ 60 MB di picco e dopo il carico.
- **Master**: prefork 19,4 MB (obiettivo ≤ 16), `-P solo` Redis 35,4 MB (obiettivo ≤ 30): con la
  regola qui sopra alcuni MB torneranno; misura e riporta la scomposizione richiesta dal punto 3.
- **mimalloc / arene glibc** (punto 4): non ancora provato. Provalo e riporta i numeri anche se lo
  scarti.
- **Beat**: invii/s scesi da 497→464 (Redis) e 553→510 (AMQP) rispetto a `main`: ripeti la misura
  3 volte; se è reale, trova la causa.

## Fine
Tutti i gate di `specs/08b-memory.md` + `python/tests_rs/test_kombu_identity.py`, con l'esito
reale. Committa nel branch al termine (messaggio `08b: …`).
