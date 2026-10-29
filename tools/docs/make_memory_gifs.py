#!/usr/bin/env python3
"""GIF dei meccanismi di memoria: uvx --with pillow python tools/docs/make_memory_gifs.py.

Numeri: `bench/memprobe/loadpeak.py`, worker `-c 10`, Redis, 30.000 task (0.1.3).
"""
import random
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'docs/assets'
WIDTH, HEIGHT = 960, 540
BG, PANEL, BORDER = '#0b0c10', '#11141b', '#2c303b'
WHITE, MUTED, DARK = '#f3f1ed', '#a0a7b7', '#303642'
ORANGE, BLUE = '#e8946f', '#78b6ed'
SHARED, PRIVATE = '#5fae7c', '#e0675f'

# Misure (MB, PSS) del picco sotto carico.
REF = {'master': 28.5, 'figli': 324.7, 'zygote': 0.0}
RS311 = {'master': 15.0, 'figli': 57.5, 'zygote': 5.6}
RS313 = {'master': 11.4, 'figli': 37.3, 'zygote': 3.7}


@lru_cache(maxsize=None)
def font(size, bold=False):
    name = 'DejaVuSans-Bold.ttf' if bold else 'DejaVuSans.ttf'
    return ImageFont.truetype('/usr/share/fonts/truetype/dejavu/' + name, size)


def text(d, x, y, value, size=14, color=MUTED, bold=False, anchor='la'):
    face = font(size, bold)
    box = d.textbbox((x, y), value, font=face, anchor=anchor)
    if box[0] < 4 or box[1] < 4 or box[2] > WIDTH - 4 or box[3] > HEIGHT - 4:
        raise ValueError(f'Testo fuori dal fotogramma: {value!r} {box}')
    d.text((x, y), value, fill=color, font=face, anchor=anchor)


def frame(title, subtitle):
    image = Image.new('RGB', (WIDTH, HEIGHT), BG)
    d = ImageDraw.Draw(image)
    text(d, 28, 22, title, 24, WHITE, True)
    text(d, 28, 56, subtitle, 14, MUTED)
    return image, d


def box(d, xy, fill=PANEL, outline=BORDER, width=2, radius=10):
    d.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def panel(d, x0, x1, label, color):
    box(d, (x0, 90, x1, 512))
    text(d, x0 + 16, 102, label, 16, color, True)


def arrow(d, a, b, color=MUTED):
    d.line([a, b], fill=color, width=2)
    (x0, y0), (x1, y1) = a, b
    if abs(x1 - x0) >= abs(y1 - y0):
        s = 1 if x1 > x0 else -1
        d.polygon([(x1, y1), (x1 - 8 * s, y1 - 5), (x1 - 8 * s, y1 + 5)], fill=color)
    else:
        s = 1 if y1 > y0 else -1
        d.polygon([(x1, y1), (x1 - 5, y1 - 8 * s), (x1 + 5, y1 - 8 * s)], fill=color)


def save(name, frames):
    images = [f for f, _ in frames]
    durations = [ms for _, ms in frames]
    path = OUT / name
    images[0].save(path, save_all=True, append_images=images[1:], duration=durations,
                   loop=0, optimize=True)
    print(f'{path.relative_to(ROOT)}: {len(images)} fotogrammi, '
          f'{sum(durations) / 1000:.1f} s, {path.stat().st_size // 1024} KB')


# 1 ─ architettura ───────────────────────────────────────────────────────────
def children(d, x0, y0, size_mb, label, scale, color, n=10):
    """Dieci figli con area proporzionale ai MB."""
    side = int((size_mb * scale) ** 0.5)
    for i in range(n):
        cx = x0 + (i % 5) * (side + 10)
        cy = y0 + (i // 5) * (side + 10)
        d.rectangle((cx, cy, cx + side, cy + side), fill=color, outline=BORDER)
    text(d, x0, y0 + 2 * (side + 10) + 6, label, 13, MUTED)


def architecture():
    frames = []
    steps = 4
    for step in range(steps):
        image, d = frame('1 · Dove gira il lavoro e da dove nascono i figli',
                         'Worker prefork -c 10: Celery originale a sinistra, celery-rs a destra')
        panel(d, 24, 470, 'Celery originale', MUTED)
        box(d, (44, 140, 450, 250), DARK)
        text(d, 60, 150, 'master Python  · 28,5 MB', 15, WHITE, True)
        for i, line in enumerate(['consumer kombu, QoS, ack, eventi, heartbeat',
                                  'timer ETA, rate limit, controllo remoto',
                                  'pool billiard: thread di supervisione']):
            text(d, 60, 178 + i * 22, line, 13, MUTED)
        if step >= 1:
            arrow(d, (247, 250), (247, 290))
            text(d, 258, 260, 'fork() dal master già carico', 13, MUTED)
            children(d, 60, 300, 32.5, '10 figli · 32,5 MB ciascuno · 325 MB', 40, BLUE)
        panel(d, 490, 936, 'celery-rs', ORANGE)
        if step >= 2:
            box(d, (510, 140, 916, 250), DARK, ORANGE)
            text(d, 526, 150, 'master  · 15 MB', 15, WHITE, True)
            text(d, 526, 178, 'Rust: consumer, QoS, ack, eventi, heartbeat,', 13, ORANGE)
            text(d, 526, 200, 'timer, rate limit, supervisore del pool', 13, ORANGE)
            text(d, 526, 222, 'Python: bootsteps, segnali, comandi Panel', 13, BLUE)
        if step >= 3:
            arrow(d, (713, 250), (713, 272))
            box(d, (560, 274, 866, 330), DARK, SHARED)
            text(d, 576, 282, 'zygote · 5,6 MB', 14, WHITE, True)
            text(d, 576, 304, 'app e task precaricati, nessun thread', 12, MUTED)
            arrow(d, (713, 330), (713, 350))
            children(d, 560, 356, 5.75, '10 figli · 5,7 MB ciascuno · 57,5 MB', 40, BLUE)
            text(d, 510, 478, 'i figli nascono dallo zygote, non dal master', 13, SHARED)
        frames.append((image, 2200 if step < steps - 1 else 5000))
    save('memoria-1-architettura.gif', frames)


# 2 ─ copy-on-write ───────────────────────────────────────────────────────────
COLS, ROWS, CELL = 20, 11, 17
# Figlio dopo 2000 task (breakdown.py): pagine residenti e quota privata.
REF_CHILD = (49.8, 26.8)
RS_CHILD = (28.0, 3.7)


def grid(d, x0, y0, rows, private):
    for i in range(COLS * rows):
        cx, cy = x0 + (i % COLS) * (CELL + 2), y0 + (i // COLS) * (CELL + 2)
        d.rectangle((cx, cy, cx + CELL, cy + CELL), fill=PRIVATE if i in private else SHARED)


def legend(d, y):
    d.rectangle((28, y, 42, y + 14), fill=SHARED)
    text(d, 50, y - 1, 'pagina condivisa con il padre', 13)
    d.rectangle((300, y, 314, y + 14), fill=PRIVATE)
    text(d, 322, y - 1, 'pagina copiata nel figlio (RAM in più)', 13)


def copy_on_write():
    ref_rows = ROWS
    rs_rows = round(ROWS * RS_CHILD[0] / REF_CHILD[0])
    ref_steps = [('subito dopo fork(): tutto condiviso', 0),
                 ('ogni oggetto letto riceve refcount += 1: la pagina si copia', 0.25),
                 ('il GC percorre e marca gli oggetti ereditati', 0.42),
                 ('dopo 2000 task', REF_CHILD[1] / REF_CHILD[0])]
    rs_steps = [('subito dopo fork(): tutto condiviso', 0),
                ('gc.freeze(): il GC non tocca più gli oggetti ereditati', 0.03),
                ('init preparato nello zygote; immortali su Python 3.12+', 0.08),
                ('dopo 2000 task', RS_CHILD[1] / RS_CHILD[0])]
    frames = []
    for n, ((ref_label, ref_q), (rs_label, rs_q)) in enumerate(zip(ref_steps, rs_steps)):
        last = n == len(ref_steps) - 1
        image, d = frame('2 · Copy-on-write: perché i figli di Celery diventano pesanti',
                         'Dopo fork() il figlio condivide le pagine del padre finché qualcuno '
                         'non le scrive · 1 quadrato ≈ 0,23 MB')
        for x0, label, color, rows, step_label, q, child in (
                (28, 'Celery originale', MUTED, ref_rows, ref_label, ref_q, REF_CHILD),
                (500, 'celery-rs (Python 3.11)', ORANGE, rs_rows, rs_label, rs_q, RS_CHILD)):
            text(d, x0, 96, label, 16, color, True)
            order = list(range(COLS * rows))
            random.Random(7).shuffle(order)
            private = set(order[:round(COLS * rows * q)])
            grid(d, x0, 126, rows, private)
            text(d, x0, 344, step_label, 13, WHITE)
            if last:
                text(d, x0, 368, f'{child[1]:.1f} MB copiati su {child[0]:.1f} MB residenti'
                     .replace('.', ','), 13, PRIVATE, True)
        legend(d, 410)
        text(d, 28, 448, 'In Python anche leggere un oggetto lo scrive (contatore dei riferimenti):',
             13, MUTED)
        text(d, 28, 470, 'celery-rs congela il GC, rende immortali gli oggetti precaricati e prepara',
             13, MUTED)
        text(d, 28, 492, 'una volta sola nello zygote ciò che ogni figlio rifarebbe uguale.',
             13, MUTED)
        frames.append((image, 5000 if last else 2400))
    save('memoria-2-copy-on-write.gif', frames)


# 3 ─ inizializzazione e memoria dei task ───────────────────────────────────
def lane(d, x, y, w, label, color):
    box(d, (x, y, x + w, y + 26), DARK, color, 1, 6)
    text(d, x + 8, y + 5, label, 12, WHITE)


def initialization():
    frames = []
    for step in range(4):
        image, d = frame('3 · Lavoro fatto una volta e memoria restituita',
                         'Inizializzazione dei figli (sopra) e memoria dopo un task pesante (sotto)')
        panel(d, 24, 470, 'Celery originale', MUTED)
        panel(d, 490, 936, 'celery-rs', ORANGE)
        text(d, 40, 132, 'ogni figlio, 10 volte:', 13, MUTED)
        for i in range(min(step + 1, 3)):
            y = 156 + i * 34
            lane(d, 40, y, 120, f'figlio {i + 1}', BLUE)
            lane(d, 168, y, 130, 'log.setup', PRIVATE)
            lane(d, 306, y, 148, 'tracer dei task', PRIVATE)
        text(d, 506, 132, 'una volta nello zygote, prima del fork:', 13, MUTED)
        lane(d, 506, 156, 120, 'zygote', SHARED)
        lane(d, 634, 156, 130, 'log.setup', SHARED)
        lane(d, 772, 156, 148, 'tracer dei task', SHARED)
        if step >= 1:
            text(d, 506, 196, 'i figli ereditano lo stato già pronto, condiviso;', 13, MUTED)
            text(d, 506, 216, 'nel figlio restano worker_process_init e segnali', 13, MUTED)
        if step >= 2:
            d.line((40, 290, 454, 290), fill=BORDER)
            d.line((506, 290, 920, 290), fill=BORDER)
            text(d, 40, 302, 'dopo un task con un payload grande:', 13, MUTED)
            text(d, 506, 302, 'dopo ogni task:', 13, MUTED)
            for x0, release in ((40, False), (506, True)):
                base, peak = 474, 340
                d.line((x0, base, x0 + 400, base), fill=BORDER)
                points = [(x0, base - 40), (x0 + 110, base - 40), (x0 + 150, peak),
                          (x0 + 230, peak)]
                points += [(x0 + 270, base - 40), (x0 + 400, base - 40)] if release else \
                          [(x0 + 400, peak)]
                d.line(points, fill=PRIVATE if not release else SHARED, width=3)
                text(d, x0 + 150, peak - 22, 'task', 12, MUTED)
        if step >= 3:
            text(d, 40, 484, 'glibc spesso trattiene le pagine: il figlio resta gonfio', 13, PRIVATE)
            text(d, 506, 484, 'malloc_trim restituisce subito le pagine libere', 13, SHARED)
        frames.append((image, 2400 if step < 3 else 5000))
    save('memoria-3-inizializzazione.gif', frames)


# 4 ─ bilancio ────────────────────────────────────────────────────────────────
def balance():
    rows = [('Celery originale', REF, MUTED), ('celery-rs, Python 3.11', RS311, ORANGE),
            ('celery-rs, Python 3.13', RS313, ORANGE)]
    scale = 560 / 360
    colors = {'master': ORANGE, 'zygote': SHARED, 'figli': BLUE}
    frames = []
    for step in range(len(rows) + 1):
        image, d = frame('4 · Dove vanno i MB: worker -c 10 al picco sotto carico',
                         'PSS misurata con bench/memprobe/loadpeak.py · Redis · 30.000 task')
        for i, (label, data, color) in enumerate(rows[:max(1, step)]):
            y = 120 + i * 110
            text(d, 28, y, label, 16, color, True)
            x = 28
            for part in ('master', 'zygote', 'figli'):
                width = data[part] * scale
                if width:
                    d.rectangle((x, y + 30, x + width, y + 66), fill=colors[part])
                    x += width
            total = sum(data.values())
            text(d, x + 10, y + 38, f'{total:.0f} MB'.replace('.', ','), 16, WHITE, True)
            parts = '   '.join(f'{p} {v:.1f}'.replace('.', ',') for p, v in data.items() if v)
            text(d, 28, y + 74, parts + ' MB', 12, MUTED)
        for i, part in enumerate(('master', 'zygote', 'figli')):
            d.rectangle((28 + i * 140, 470, 42 + i * 140, 484), fill=colors[part])
            text(d, 50 + i * 140, 469, part, 13)
        if step == len(rows):
            text(d, 470, 470, '−78% su 3.11, −85% su 3.13 · throughput ×3', 14, WHITE, True)
        frames.append((image, 2000 if step < len(rows) else 5000))
    save('memoria-4-bilancio.gif', frames)


if __name__ == '__main__':
    OUT.mkdir(parents=True, exist_ok=True)
    architecture()
    copy_on_write()
    initialization()
    balance()
