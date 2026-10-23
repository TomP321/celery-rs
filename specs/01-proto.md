# 01 — Protocollo: JSON compatibile con Python, saferepr, compressione, messaggio task v2

Fase 1 di `PLAN.md`. Obiettivo: il percorso di creazione di un messaggio task lato client
(`as_task_v2` + serializzazione + compressione) in Rust, **byte-identico** al riferimento.

## Riferimenti (sola lettura)
- `celery/celery/app/amqp.py` — `AMQP.as_task_v2` (righe ~325–430)
- `celery/celery/utils/saferepr.py` — `saferepr` (tutto il file) e `celery/celery/utils/text.py::truncate`
- `.venv-ref/lib/python3.13/site-packages/kombu/utils/json.py` — `JSONEncoder`, `dumps`, `loads`, `object_hook`
- `.venv-ref/lib/python3.13/site-packages/kombu/serialization.py` — `register_json`
- `.venv-ref/lib/python3.13/site-packages/kombu/compression.py` — content type e alias
- CPython `json` (encoder C): regole di `ensure_ascii`, float repr, chiavi dict, rilevamento cicli

## Nome del modulo nativo
Il crate `crates/celery-py` produce il modulo top-level **`_celery_rs`** (non `celery._rs`): così
può essere installato anche in `.venv-ref` accanto a Celery originale per i test differenziali.
Il guscio Python (fase 3) lo ri-esporterà. Aggiorna `[lib] name` e `#[pymodule]` di conseguenza.
Build/installazione: `maturin` (aggiungilo a `.venv-ref` con `uv pip install maturin`), poi
`VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml`.

## A. `crates/celery-proto` (Rust puro, nessuna dipendenza da PyO3)

1. `json::write` — primitive di scrittura su `Vec<u8>` identiche a CPython `json.dumps` con i default
   di kombu (`ensure_ascii=True`, separatori `', '` e `': '`, `allow_nan=True`):
   - stringhe: escape `\"` `\\` `\n` `\r` `\t` `\b` `\f`, altri `< 0x20` come `\u00XX`, tutto ciò che
     non è ASCII come `\uXXXX` minuscolo, code point > U+FFFF come coppia surrogata. L'input è una
     sequenza di code point (deve gestire anche surrogati isolati, che le `str` Python possono contenere).
   - float: identico a `float.__repr__` (shortest round-trip; notazione esponenziale quando
     l'esponente decimale è < -4 o ≥ 16, formato `1e+16`, `1.5e-05`; `.0` per valori interi;
     `-0.0`), con `Infinity`, `-Infinity`, `NaN`.
   - interi: i64/u64 nativi; gli interi arbitrari arrivano già come stringa decimale.
2. `json::read` — parser a eventi (pull/visitor, nessun albero intermedio) con la semantica di
   `json.loads` di CPython: accetta `NaN`, `Infinity`, `-Infinity`; interi di qualsiasi grandezza
   (restituiti come slice di testo quando non stanno in i64); float; escape `\uXXXX` inclusi
   surrogati isolati; chiavi duplicate (vince l'ultima, lo fa il builder); spazi bianchi JSON;
   errore con posizione (offset in caratteri come CPython) su input non valido o con dati in coda.
3. `compression` — `compress(name_or_alias, &[u8]) -> (Vec<u8>, content_type)` e
   `decompress(content_type, &[u8])`. Tabella identica a kombu:
   `application/x-gzip` (alias `gzip`, `zlib`; formato zlib, non gzip), `application/x-bz2`
   (`bzip2`, `bzip`), `application/x-lzma` (`lzma`, `xz`; formato xz come `lzma.compress`),
   `application/x-brotli` (`brotli`), `application/zstd` (`zstd`, `zstandard`).
   Requisito: interoperabilità in entrambe le direzioni con le librerie Python
   (`zlib`, `bz2`, `lzma`, `brotli` se installato, `backports.zstd`); i byte compressi NON devono
   essere identici. Nome sconosciuto: kombu solleva `KeyError` da `get_encoder`; il modulo Python
   deve sollevare lo stesso `KeyError`.
4. Test unitari Rust: tabella di float (almeno 200 valori, inclusi limiti e subnormali) con
   l'output atteso generato una volta con Python e salvato come fixture; escape delle stringhe;
   parser su input validi e non validi.

## B. `crates/celery-py` → modulo `_celery_rs`

Tutte le funzioni costruiscono/leggono oggetti Python **direttamente** (nessuna struttura intermedia
tipo `serde_json::Value`) e rilasciano il GIL solo dove non toccano oggetti Python (es. compressione).

1. `json_dumps(obj, default) -> bytes` — identico a `kombu.utils.json.dumps(obj).encode('utf-8')`.
   `default` è il callable di fallback (il guscio passerà `kombu.utils.json.JSONEncoder().default`).
   - Percorso veloce nativo per: `str`, `int`, `float`, `bool`, `None`, `list`, `tuple`, `dict` e
     **loro sottoclassi** con le stesse regole dell'encoder C di CPython (es. `Signature` è una
     sottoclasse di `dict` → oggetto JSON; `IntEnum` → numero via `int.__repr__`; sottoclassi di
     `float` via `float.__repr__`; `namedtuple` → array).
   - Chiavi dict: `str` così com'è; `int`/`float`/`bool`/`None` convertiti come CPython
     (`true`/`false`/`null`, repr dei numeri); altri tipi → `TypeError` con lo stesso messaggio
     di CPython (`keys must be str, int, float, bool or None, not tuple`).
   - Qualsiasi altro oggetto (datetime, Decimal, UUID, bytes, oggetti con `__json__`, tipi
     registrati dall'utente con `register_type`, …): chiama `default(o)` e codifica il risultato.
     Se `default` solleva, l'eccezione si propaga invariata.
   - Riferimenti circolari → `ValueError('Circular reference detected')`.
2. `json_loads(data, object_hook) -> object` — identico a `kombu.utils.json.loads(data)`:
   accetta `bytes`, `bytearray`, `memoryview`, `str`; costruisce gli oggetti durante il parsing;
   per ogni dict chiama `object_hook(d)` (il guscio passerà `kombu.utils.json.object_hook`; per
   prestazioni: chiama l'hook solo per i dict con esattamente le chiavi `__type__` e `__value__`,
   e documenta che è equivalente perché l'hook di kombu restituisce gli altri dict invariati).
   Errori → `json.JSONDecodeError(msg, doc, pos)` (stesso tipo e stessa posizione di CPython;
   il messaggio deve essere uguale almeno per i casi comuni: `Expecting value`, `Extra data`,
   `Unterminated string starting at`, `Expecting ',' delimiter`, `Expecting property name enclosed in double quotes`).
3. `saferepr(obj, maxlen=None, maxlevels=3) -> str` — identico a
   `celery.utils.saferepr.saferepr`, inclusi troncamento (`truncate`), livelli, `', ...'`,
   parentesi pendenti, set/frozenset vuoti e sottoclassi, tuple di un elemento, bytes non UTF-8
   (esadecimale), protezione dalla ricorsione (`<Recursion on list with id=...>` come
   `pprint._recursion`). Per oggetti diversi da quelli gestiti nativamente in `reprstream`
   (Number non builtin, Decimal, range, oggetti arbitrari) chiama `repr()` di Python, con lo
   stesso fallback `<Unrepresentable ...>` del riferimento.
4. `compress(body: bytes, name: str) -> tuple[bytes, str]` e `decompress(body: bytes, content_type: str) -> bytes`.
5. `encode_task_v2(...)` — equivalente a `AMQP.as_task_v2` seguito da serializzazione JSON e
   compressione opzionale. Firma (parametri keyword, stessi nomi di `as_task_v2`):
   `task_id, name, args, kwargs, eta, expires, group_id, group_index, retries, chord, callbacks,
   errbacks, reply_to, time_limit, soft_time_limit, create_sent_event, root_id, parent_id, shadow,
   chain, origin, ignore_result, argsrepr, kwargsrepr, stamped_headers, stamps,
   replaced_task_nesting, argsrepr_maxsize, kwargsrepr_maxsize, repr_maxlevels, compression, default`.
   - `eta`/`expires` arrivano già come stringa ISO o `None` (la conversione da countdown/datetime
     resta nel guscio Python, perché dipende da `app.now()` e dal timezone dell'app).
   - `origin` arriva già calcolato (il guscio passa `origin or anon_nodename()`).
   - Stessa validazione: `args` non lista/tupla → `TypeError('task args must be a list or tuple')`;
     `kwargs` non `Mapping` → `TypeError('task keyword arguments must be a mapping')`.
   - Ritorna `(headers: dict, properties: dict, body: bytes, content_type: str,
     content_encoding: str, sent_event: dict | None)` dove `headers`, `properties` e `sent_event`
     sono **uguali** (==, stesso ordine delle chiavi) a quelli di `as_task_v2`, e `body` è
     identico a `json.dumps((args, kwargs, embed))` di kombu (poi compresso se richiesto; in quel caso
     `headers['compression']` è impostato come fa `Producer._prepare`).
   - Serializer diversi da JSON non sono in questa spec: il guscio userà kombu per pickle/yaml/msgpack.

## C. Test differenziali — `crates/celery-py/tests/test_differential.py`

Eseguiti con `.venv-ref/bin/python -m pytest crates/celery-py/tests -q` (Celery originale e
`_celery_rs` nello stesso processo). Usa un generatore casuale con seed fisso (stdlib `random`),
nessuna dipendenza nuova. Ogni test confronta il riferimento con `_celery_rs`:

1. `json_dumps`: ≥ 50.000 valori annidati casuali (profondità ≤ 6) che includano: str con ASCII,
   BMP, astrali, caratteri di controllo, surrogati isolati; int piccoli, negativi, > 2**64;
   float limite (`0.1`, `1e16`, `1e-5`, `-0.0`, `5e-324`, `1.7976931348623157e308`, `nan`, `inf`);
   bool; None; list/tuple/dict annidati; dict con chiavi int/float/bool/None; `Signature`
   (`celery.signature('x', args=(1,))`); namedtuple; IntEnum; datetime aware/naive, date, time,
   Decimal, UUID, bytes UTF-8 e non; oggetti con `__json__`. Output **byte-identico**.
   Casi di errore (chiave tuple, oggetto non serializzabile, ciclo): stesso tipo di eccezione e
   stesso messaggio.
2. `json_loads`: round-trip di tutti i valori sopra (risultato `==` e stessi tipi) + un corpus di
   ≥ 60 stringhe JSON scritte a mano, valide e non valide (stesso risultato o stesso tipo di errore e
   stessa `pos`).
3. `saferepr`: ≥ 20.000 valori × `maxlen ∈ {None, 0, 1, 5, 64, 1024}` × `maxlevels ∈ {0, 1, 3, 10}`,
   inclusi strutture ricorsive, set/frozenset (usa elementi con ordine di iterazione deterministico),
   sottoclassi di list/dict/tuple, bytes. Output identico.
4. `encode_task_v2`: ≥ 500 combinazioni di opzioni (tutte le opzioni di `as_task_v2`, args/kwargs
   casuali, con e senza compressione zlib). Confronto: `headers`, `properties`, `sent_event`
   uguali al risultato di `app.amqp.as_task_v2(...)` con gli stessi `eta`/`expires`/`origin`;
   `body` identico a `kombu.serialization.dumps(body, 'json')[2]` (decompresso, se compresso).
5. Compressione: per ogni algoritmo disponibile, Rust→Python e Python→Rust su payload casuali
   da 0 B a 1 MB.
6. Micro-benchmark informativo (non un gate), stampato con `-s`: `json_dumps` e `encode_task_v2`
   vs riferimento su un payload tipico. Riporta i numeri nel messaggio finale.

## Criteri di accettazione (eseguili tutti e riporta l'esito reale)
```
cargo build --release
cargo clippy --all-targets -- -D warnings
cargo test
VIRTUAL_ENV=.venv-ref .venv-ref/bin/maturin develop --release -m crates/celery-py/Cargo.toml
.venv-ref/bin/python -m pytest crates/celery-py/tests -q
```
Tutti verdi, nessun test saltato o marcato xfail per far passare la suite.

## Fuori scope
Trasporti, result backend, guscio Python `celery/`, protocollo v1, eventi. Non toccare
`celery/`, `tools/compat/`, `bench/`.
