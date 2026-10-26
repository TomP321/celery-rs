"""Filtra lo stream JSON di `codex exec --json`: messaggi e comandi nel log leggibile,
id del thread salvato per poter continuare la conversazione (`codex exec resume`)."""
import json
import sys

sid_file, log_path = sys.argv[1], sys.argv[2]
with open(log_path, 'a') as log:
    for line in sys.stdin:
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        kind = ev.get('type')
        if kind == 'thread.started' and ev.get('thread_id'):
            # salvato subito: se la sessione si blocca si può riprendere lo stesso
            with open(sid_file, 'w') as fh:
                fh.write(ev['thread_id'])
        item = ev.get('item') or {}
        if kind == 'item.completed':
            if item.get('type') == 'agent_message' and item.get('text'):
                log.write(item['text'] + '\n')
            elif item.get('type') == 'command_execution':
                cmd = str(item.get('command', '')).replace('\n', ' ')[:200]
                log.write(f"[cmd exit={item.get('exit_code')}] {cmd}\n")
            elif item.get('type') == 'file_change':
                files = ', '.join(c.get('path', '') for c in item.get('changes', []))[:200]
                log.write(f"[file] {files}\n")
        elif kind in ('turn.failed', 'error'):
            log.write(f"[errore] {json.dumps(ev)[:500]}\n")
        log.flush()
