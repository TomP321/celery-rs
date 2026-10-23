"""Filtra lo stream JSON di `opencode run --format json`: testo e chiamate tool nel log leggibile,
id di sessione salvato per poter continuare la conversazione."""
import json
import sys

sid_file, log_path = sys.argv[1], sys.argv[2]
sid = None
with open(log_path, 'a') as log:
    for line in sys.stdin:
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        part = ev.get('part') or {}
        sid = sid or ev.get('sessionID') or part.get('sessionID')
        if part.get('type') == 'text' and part.get('text'):
            log.write(part['text'] + '\n')
        elif part.get('type') == 'tool':
            st = part.get('state') or {}
            title = str(st.get('title') or '')[:200]
            log.write(f"[tool {part.get('tool')}] {st.get('status', '')} {title}\n")
        log.flush()
if sid:
    with open(sid_file, 'w') as fh:
        fh.write(sid)
