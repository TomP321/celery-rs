"""Cattura e confronto del traffico sul broker (solo stdlib: gira nel Python dell'harness).

Redis: connessione MONITOR grezza → ogni comando ricevuto dal server.
RabbitMQ: firehose tracer (`amq.rabbitmq.trace`, `publish.#`) letto dall'API di management HTTP.
  L'API restituisce le tabelle AMQP come oggetti JSON senza garanzia d'ordine: su AMQP l'ordine
  delle chiavi negli header non è verificato (non è semantico per i consumer); su Redis sì.
I messaggi vengono decodificati (envelope kombu, base64, compressione) e normalizzati
(id, timestamp, hostname, pid) prima del confronto.
"""
import base64
import bz2
import json
import lzma
import re
import socket
import threading
import time
import urllib.request
import zlib

REDIS = ('127.0.0.1', 6390)
RABBIT_API = 'http://127.0.0.1:15673/api'
RABBIT_AUTH = 'Basic ' + base64.b64encode(b'guest:guest').decode()
TRACE_QUEUE = 'compat.trace'

# comandi di gestione della connessione: dipendono dalla libreria client, non dal protocollo Celery
REDIS_IGNORED = {'HELLO', 'CLIENT', 'PING', 'INFO', 'AUTH', 'QUIT', 'COMMAND', 'MONITOR', 'FLUSHALL',
                 'SELECT', 'RESET'}


# ------------------------------------------------------------------ normalizzazione

UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
# timestamp ISO calcolati da "now" (countdown/expires); quelli fissi degli scenari (2030) restano
ISO_RE = re.compile(r'(?!2030)\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:[+-]\d\d:\d\d|Z)?')
UNIX_TS_RE = re.compile(r'(?<![\d.])1[5-9]\d{8}\.\d+')
UNIX_MS_RE = re.compile(r'(?<![\d.])1[5-9]\d{11}(?![\d.])')
HEX32_RE = re.compile(r'(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])')
# il JSON del body è annidato come stringa dentro l'envelope: le virgolette possono essere escaped
PID_RE = re.compile(r'(\\?"pid\\?":\s*)\d+')
# TTL in ms calcolato da `expires - now`
EXPIRATION_RE = re.compile(r'(\\?"expiration\\?":\s*\\?")\d+')
ORIGIN_RE = re.compile(r'gen\d+@')


class Normalizer:
    """Sostituzioni coerenti all'interno di uno scenario: lo stesso id → stesso segnaposto."""

    def __init__(self):
        self.ids = {}
        self.host = socket.gethostname()

    def _id(self, m):
        return self.ids.setdefault(m.group(0), f'<id{len(self.ids) + 1}>')

    def __call__(self, text):
        text = UUID_RE.sub(self._id, text)
        text = HEX32_RE.sub(self._id, text)
        text = ISO_RE.sub('<now-iso>', text)
        text = UNIX_TS_RE.sub('<now-unix>', text)
        text = UNIX_MS_RE.sub('<now-ms>', text)
        text = PID_RE.sub(r'\1<pid>', text)
        text = EXPIRATION_RE.sub(r'\1<ttl-ms>', text)
        text = ORIGIN_RE.sub('gen<pid>@', text)
        return text.replace(self.host, '<host>')


def decompress(body, content_type):
    if content_type in ('application/x-gzip',):
        return zlib.decompress(body)
    if content_type == 'application/x-bz2':
        return bz2.decompress(body)
    if content_type == 'application/x-lzma':
        return lzma.decompress(body)
    return body


def body_text(body, content_type, compression):
    """Il body come testo confrontabile. I byte compressi non devono coincidere (cambia la
    libreria), il contenuto decompresso sì; è indicato esplicitamente che era compresso."""
    prefix = ''
    if compression:
        body = decompress(body, compression)
        prefix = f'<compressed {compression}> '
    if content_type in ('application/json', 'application/x-yaml'):
        return prefix + body.decode('utf-8')
    return prefix + f'<{content_type}> ' + body.hex()


def decode_kombu_envelope(raw):
    """Envelope JSON del trasporto Redis di kombu → testo normalizzabile, con il body decodificato."""
    try:
        env = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(env, dict) or 'body' not in env or 'properties' not in env:
        return None
    props = env.get('properties') or {}
    body = env['body']
    if props.get('body_encoding') == 'base64':
        body = base64.b64decode(body)
    elif isinstance(body, str):
        body = body.encode('utf-8')
    headers = env.get('headers') or {}
    env['body'] = body_text(body, env.get('content-type'), headers.get('compression'))
    # l'envelope stesso va confrontato anche come testo (ordine chiavi, spazi): lo ricodifichiamo
    # sostituendo solo il body, mantenendo l'ordine originale delle chiavi.
    return json.dumps(env, ensure_ascii=False)


# ------------------------------------------------------------------------ Redis

def parse_monitor_line(line):
    """`<ts> [db addr] "CMD" "arg" ...` → [db, CMD, arg bytes...]."""
    head, _, rest = line.partition('] ')
    db = head.split('[', 1)[1].split()[0]
    args, i, n = [], 0, len(rest)
    while i < n:
        if rest[i] != '"':
            i += 1
            continue
        i += 1
        buf = bytearray()
        while rest[i] != '"':
            c = rest[i]
            if c == '\\':
                nxt = rest[i + 1]
                if nxt == 'x':
                    buf.append(int(rest[i + 2:i + 4], 16))
                    i += 4
                    continue
                buf += {'n': b'\n', 'r': b'\r', 't': b'\t', 'a': b'\a', 'b': b'\b',
                        '"': b'"', '\\': b'\\'}.get(nxt, nxt.encode('latin1'))
                i += 2
                continue
            buf += c.encode('latin1')
            i += 1
        args.append(bytes(buf))
        i += 1
    return db, args


class RedisCapture:
    def __enter__(self):
        flush = socket.create_connection(REDIS)
        flush.sendall(b'FLUSHALL\r\n')
        flush.recv(64)
        flush.close()
        self.sock = socket.create_connection(REDIS)
        self.sock.sendall(b'MONITOR\r\n')
        self.buf = b''
        while b'\r\n' not in self.buf:
            self.buf += self.sock.recv(4096)
        self.buf = self.buf.split(b'\r\n', 1)[1]
        self.lines = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()
        return self

    def _read(self):
        self.sock.settimeout(0.1)
        while not self.stop.is_set():
            try:
                chunk = self.sock.recv(65536)
            except TimeoutError:
                continue
            if not chunk:
                break
            self.buf += chunk
            *done, self.buf = self.buf.split(b'\r\n')
            self.lines += [d.decode('latin1') for d in done if d.startswith(b'+')]

    def __exit__(self, *exc):
        time.sleep(0.3)  # lascia arrivare i comandi in volo
        self.stop.set()
        self.thread.join()
        self.sock.close()

    def messages(self, norm):
        out = []
        for line in self.lines:
            db, args = parse_monitor_line(line[1:])
            if not args:
                continue
            cmd = args[0].decode('latin1').upper()
            if cmd in REDIS_IGNORED:
                continue
            parts = [f'db{db}', cmd]
            for a in args[1:]:
                text = a.decode('utf-8', errors='replace')
                env = decode_kombu_envelope(text) if text.startswith('{') else None
                parts.append(env if env is not None else text)
            out.append(norm(' '.join(parts)))
        return out


# --------------------------------------------------------------------- RabbitMQ

def api(method, path, data=None):
    req = urllib.request.Request(f'{RABBIT_API}{path}', method=method,
                                 data=json.dumps(data).encode() if data is not None else None,
                                 headers={'Authorization': RABBIT_AUTH,
                                          'content-type': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


class RabbitCapture:
    def __enter__(self):
        # stato iniziale identico per ref e rs: nessuna coda/exchange creati da run precedenti
        for q in api('GET', '/queues/%2F'):
            if q['name'] != TRACE_QUEUE:
                api('DELETE', f'/queues/%2F/{urllib.request.quote(q["name"], safe="")}')
        for ex in api('GET', '/exchanges/%2F'):
            if ex['name'] and not ex['name'].startswith('amq.'):
                api('DELETE', f'/exchanges/%2F/{urllib.request.quote(ex["name"], safe="")}')
        api('PUT', f'/queues/%2F/{TRACE_QUEUE}', {'durable': False, 'auto_delete': False})
        api('POST', f'/bindings/%2F/e/amq.rabbitmq.trace/q/{TRACE_QUEUE}', {'routing_key': 'publish.#'})
        api('DELETE', f'/queues/%2F/{TRACE_QUEUE}/contents')
        return self

    def __exit__(self, *exc):
        time.sleep(0.5)  # il tracer è asincrono

    def messages(self, norm):
        got = api('POST', f'/queues/%2F/{TRACE_QUEUE}/get',
                  {'count': 100000, 'ackmode': 'ack_requeue_false', 'encoding': 'base64'})
        out = []
        for m in got:
            if m['properties'].get('headers', {}).get('exchange_name') == 'amq.rabbitmq.trace':
                continue
            h = m['properties']['headers']
            props = dict(h.get('properties') or {})
            body = base64.b64decode(m['payload'])
            hdrs = props.get('headers') or {}
            text = body_text(body, props.get('content_type'), hdrs.get('compression'))
            rec = {'exchange': h.get('exchange_name'), 'routing_keys': h.get('routing_keys'),
                   'routed_queues': sorted(h.get('routed_queues') or []),
                   'properties': props, 'body': text}
            out.append(norm(json.dumps(rec, ensure_ascii=False)))
        return out


def capture(kind):
    return RedisCapture() if kind == 'redis' else RabbitCapture()


def enable_rabbit_trace():
    import subprocess
    subprocess.run(['docker', 'exec', 'celery-rs-rabbitmq-1', 'rabbitmqctl', 'trace_on'],
                   check=True, capture_output=True)
