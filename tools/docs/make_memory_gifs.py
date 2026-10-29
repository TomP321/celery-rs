#!/usr/bin/env python3
"""Render memory mechanism GIFs with uvx --with pillow python.

PSS figures use Redis workers with concurrency 10, measured by
bench/memprobe/loadpeak.py and bench/memprobe/breakdown.py."""
import math
import random
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'docs/assets'
WIDTH, HEIGHT, FPS = 960, 540, 10
SCALE = 2  # Logical 960×540 coordinates render at 1920×1080.
BG, PANEL, BORDER = '#0b0c10', '#11141b', '#2c303b'
WHITE, MUTED, DARK = '#f3f1ed', '#a0a7b7', '#303642'
ORANGE, BLUE = '#e8946f', '#78b6ed'
SHARED, PRIVATE = '#5fae7c', '#e0675f'


REF_CHILD_PRIVATE, RS_CHILD_PRIVATE = 26.8, 3.7        # MB copied per child after 2000 tasks.
REF_MASTER, REF_CHILD = 28.5, 32.47                     # Reference Celery peak under load.
RS_MASTER, RS_ZYGOTE, RS_CHILD = 15.0, 5.6, 5.75        # celery-rs peak under load, Python 3.11.
IDLE_BEFORE, IDLE_AFTER = 72.5, 62.1                    # Idle before/after preparing initialization.
TOTALS = [('Reference Celery', 353.2, MUTED), ('celery-rs · Python 3.11', 78.4, ORANGE),
          ('celery-rs · Python 3.13', 52.4, ORANGE)]


@lru_cache(maxsize=None)
def font(size, bold=False):
    name = 'DejaVuSans-Bold.ttf' if bold else 'DejaVuSans.ttf'
    return ImageFont.truetype('/usr/share/fonts/truetype/dejavu/' + name, size * SCALE)


class ScaledDraw:
    """Draw at logical 960×540 coordinates multiplied by SCALE."""
    def __init__(self, image):
        self._d = ImageDraw.Draw(image)

    @staticmethod
    def _xy(xy):
        return [v * SCALE for v in xy] if isinstance(xy[0], (int, float)) else \
               [(x * SCALE, y * SCALE) for x, y in xy]

    def _kw(self, kw):
        for key in ('width', 'radius'):
            if key in kw:
                kw[key] = kw[key] * SCALE
        return kw

    def text(self, xy, value, **kw):
        self._d.text(self._xy(xy), value, **kw)

    def textbbox(self, xy, value, **kw):
        return [v / SCALE for v in self._d.textbbox(self._xy(xy), value, **kw)]

    def textlength(self, value, **kw):
        return self._d.textlength(value, **kw) / SCALE

    def __getattr__(self, name):  # line, rectangle, rounded_rectangle, ellipse, arc, polygon
        draw = getattr(self._d, name)
        return lambda xy, *args, **kw: draw(self._xy(xy), *args, **self._kw(kw))


def text(d, x, y, value, size=14, color=MUTED, bold=False, anchor='la'):
    face = font(size, bold)
    box = d.textbbox((x, y), value, font=face, anchor=anchor)
    if box[0] < 4 or box[1] < 4 or box[2] > WIDTH - 4 or box[3] > HEIGHT - 4:
        raise ValueError(f'text outside frame: {value!r} {box}')
    d.text((x, y), value, fill=color, font=face, anchor=anchor)


def mb(value):
    return f'{value:.1f} MB'


def ease(t):
    t = min(1.0, max(0.0, t))
    return t * t * (3 - 2 * t)


def lerp(a, b, t):
    return a + (b - a) * t


def mix(c1, c2, t):
    a = [int(c1[i:i + 2], 16) for i in (1, 3, 5)]
    b = [int(c2[i:i + 2], 16) for i in (1, 3, 5)]
    return '#' + ''.join(f'{round(lerp(x, y, ease(t))):02x}' for x, y in zip(a, b))


def canvas(title, subtitle, label=None, label_color=MUTED):
    """Draw a title and subtitle with an optional coloured prefix."""
    image = Image.new('RGB', (WIDTH * SCALE, HEIGHT * SCALE), BG)
    d = ScaledDraw(image)
    text(d, 28, 20, title, 24, WHITE, True)
    x = 28
    if label:
        text(d, x, 54, label, 14, label_color, True)
        x += d.textlength(label + '  ·  ', font=font(14, True))
        if subtitle:
            text(d, x - d.textlength('·  ', font=font(14)), 54, '·', 14, MUTED)
    if subtitle:
        text(d, x, 54, subtitle, 14, MUTED)
    return image, d


def arrow(d, a, b, color=MUTED, width=2):
    d.line([a, b], fill=color, width=width)
    angle = math.atan2(b[1] - a[1], b[0] - a[0])
    tip = [(b[0] - 9 * math.cos(angle + s), b[1] - 9 * math.sin(angle + s)) for s in (-.45, .45)]
    d.polygon([b, *tip], fill=color)


def meter(d, y, label, value, top, color, note=''):
    text(d, 60, y, label, 13, MUTED)
    d.rounded_rectangle((60, y + 22, 900, y + 46), radius=6, fill=DARK)
    width = 840 * min(1.0, value / top)
    if width > 2:
        d.rounded_rectangle((60, y + 22, 60 + width, y + 46), radius=6, fill=color)
    text(d, min(60 + width + 10, 790), y + 25, mb(value), 15, WHITE, True)
    if note:
        text(d, 900, y, note, 12, MUTED, anchor='ra')


def render(name, scenes):
    """Render (seconds, draw(t) -> Image) scenes."""
    frames, durations = [], []
    for seconds, draw in scenes:
        count = max(1, round(seconds * FPS))
        for i in range(count):
            frames.append(draw(i / max(1, count - 1)))
            durations.append(1000 // FPS)
    durations[-1] = 4000
    path = OUT / name
    frames[0].save(path, save_all=True, append_images=frames[1:], duration=durations,
                   loop=0, optimize=True)
    print(f'{path.relative_to(ROOT)}: {len(frames)} frames, '
          f'{sum(durations) / 1000:.1f} s, {path.stat().st_size // 1024} KB')



COLS, ROWS, CELL, GAP = 6, 4, 38, 8
LEFT, RIGHT, TOP = (80, 600, 120)
PAGES = COLS * ROWS
ORDER = random.Random(3).sample(range(PAGES), PAGES)
REF_COPIED = round(PAGES * 26.8 / 49.8)                 # 54% of resident pages.
RS_COPIED = round(PAGES * 3.7 / 28.0)                   # 13%


def cell(x0, i):
    return x0 + (i % COLS) * (CELL + GAP), TOP + (i // COLS) * (CELL + GAP)


def page(d, x0, i, fill, outline=BORDER):
    x, y = cell(x0, i)
    d.rounded_rectangle((x, y, x + CELL, y + CELL), radius=4, fill=fill, outline=outline, width=2)
    for k in range(3):
        d.ellipse((x + 7 + k * 10, y + 15, x + 13 + k * 10, y + 21), fill=BG)


def cow_base(t_child, original, copied, highlight=None):
    image, d = canvas('Copy-on-write: why reading an object costs RAM',
                      'After fork(), parent and child share pages until a process '
                      'writes to them')
    prefix, parent = ('Reference Celery · ', 'master') if original else ('celery-rs · ', 'zygote')
    text(d, LEFT, 94, prefix, 15, MUTED if original else ORANGE, True)
    text(d, LEFT + d.textlength(prefix, font=font(15, True)), 94, parent, 15, WHITE, True)
    text(d, RIGHT, 94, 'child', 15, WHITE, True)
    for i in range(PAGES):
        page(d, LEFT, i, SHARED)
        if i in copied:
            fill = PRIVATE
        else:
            fill = mix(PANEL, SHARED, t_child)
        page(d, RIGHT, i, fill, WHITE if i == highlight else BORDER)
    arrow(d, (LEFT + 290, 205), (LEFT + 290 + 200 * ease(t_child), 205), MUTED, 3)
    text(d, 480, 180, 'fork()', 15, MUTED, True, anchor='ma')
    return image, d


def cow_status(d, copied, caption, value, color):
    text(d, RIGHT, 312, f'shared {PAGES - len(copied)} · copied {len(copied)}', 13, MUTED)
    text(d, 60, 352, caption, 15, WHITE, True)
    meter(d, 400, 'Child private RAM (copied pages)', value, 30, color,
          'per child, after 2000 tasks')


def cow_reads(original):
    events = ORDER[:3] if original else ORDER[:2]

    def draw(t):
        copied = set()
        k = min(len(events) - 1, int(t * len(events)))
        s = t * len(events) - k
        for j in range(k):
            if original:
                copied.add(events[j])
        target = events[k]
        if original and s > .65:
            copied.add(target)
        image, d = cow_base(1, original, copied, target)
        tx, ty = cell(RIGHT, target)
        tx, ty = tx + CELL / 2, ty + CELL / 2
        if s < .35:
            p = ease(s / .35)
            d.ellipse((lerp(RIGHT + 300, tx, p) - 7, lerp(330, ty, p) - 7,
                       lerp(RIGHT + 300, tx, p) + 7, lerp(330, ty, p) + 7), fill=WHITE)
        elif original:
            text(d, tx + CELL / 2 + 4, ty - CELL / 2 - 16, '+1', 15, PRIVATE, True)
            if s > .65:
                p = ease((s - .65) / .35)
                px, py = cell(LEFT, target)
                x, y = lerp(px, tx - CELL / 2, p), lerp(py, ty - CELL / 2, p)
                d.rounded_rectangle((x, y, x + CELL, y + CELL), radius=4, fill=PRIVATE)
        else:
            r = 6 + 18 * ease((s - .35) / .65)
            d.ellipse((tx - r, ty - r, tx + r, ty + r), outline=SHARED, width=3)
        n = len(copied)
        if original:
            caption = ('reading an object writes refcount += 1 → '
                       'the kernel copies the page')
        else:
            caption = ['gc.freeze(): GC leaves inherited objects untouched; no copy',
                       'immortal object (Python 3.12+): no refcount write'][k]
        cow_status(d, copied, caption,
                   n * REF_CHILD_PRIVATE / REF_COPIED if original else 0,
                   PRIVATE if original else SHARED)
        return image
    return draw


def cow_many(original):
    def draw(t):
        total = REF_COPIED if original else RS_COPIED
        start = 3 if original else 0
        n = round(lerp(start, total, ease(t)))
        copied = set(ORDER[:n])
        image, d = cow_base(1, original, copied)
        value = n * (REF_CHILD_PRIVATE / REF_COPIED if original else RS_CHILD_PRIVATE / RS_COPIED)
        caption = ('then GC, imports and tasks write and copy more pages' if original
                   else 'only actual writes remain (new objects, task data)')
        cow_status(d, copied, caption, value, PRIVATE if original else ORANGE)
        return image
    return draw


def cow_fork(original):
    def draw(t):
        image, d = cow_base(t, original, set())
        cow_status(d, set(), 'fork(): the child shares parent pages without copying',
                   0, SHARED)
        return image
    return draw


def cow_final(t):
    image, d = canvas('Copy-on-write: why reading an object costs RAM',
                      'RAM copied in each of 10 children after 2000 tasks (Python 3.11)')
    p = ease(t)
    for i, (label, value, color) in enumerate((('Reference Celery', REF_CHILD_PRIVATE, PRIVATE),
                                               ('celery-rs', RS_CHILD_PRIVATE, ORANGE))):
        meter(d, 130 + i * 90, label, value * p, 30, color)
    text(d, 60, 330, 'Reading a Python object writes to it: its reference count',
         15, WHITE)
    text(d, 60, 356, 'changes and the kernel copies the page. celery-rs freezes GC,',
         15, WHITE)
    text(d, 60, 382, 'immortalizes preloaded objects and runs master I/O in Rust.',
         15, WHITE)
    text(d, 60, 440, f'{mb(REF_CHILD_PRIVATE)} → {mb(RS_CHILD_PRIVATE)} per child · '
         f'×10 children = {mb((REF_CHILD_PRIVATE - RS_CHILD_PRIVATE) * 10)} fewer copies',
         16, SHARED, True)
    return image


def copy_on_write():
    render('memoria-1-copy-on-write.gif', [
        (2.5, cow_fork(True)), (6.0, cow_reads(True)), (3.5, cow_many(True)),
        (1.0, lambda t: cow_many(True)(1)),
        (2.0, cow_fork(False)), (4.0, cow_reads(False)), (2.5, cow_many(False)),
        (1.0, lambda t: cow_many(False)(1)), (3.0, cow_final)])



def child_box(d, i, size_mb, color, t):
    x, y = 40 + i * 90, 300
    d.rounded_rectangle((x, y, x + 78, y + 84), radius=6, fill=PANEL, outline=BORDER, width=2)
    h = 84 * min(1.0, size_mb / REF_CHILD) * ease(t)
    if h > 6:
        d.rounded_rectangle((x + 3, y + 84 - h, x + 75, y + 81), radius=4, fill=color)
    text(d, x + 39, y + 92, mb(size_mb).replace(' MB', ''), 12, MUTED, anchor='ma')


def zygote_scene(original, phase):
    def draw(t):
        title = 'The zygote: where children start'
        image, d = canvas(title, 'how each prefork pool child starts',
                          'Reference Celery' if original else 'celery-rs',
                          MUTED if original else ORANGE)

        d.rounded_rectangle((300, 92, 660, 178), radius=10, fill=DARK,
                            outline=MUTED if original else ORANGE, width=2)
        if original:
            text(d, 316, 102, f'Python master · {mb(REF_MASTER)}', 15, WHITE, True)
            text(d, 316, 128, 'kombu consumer, message buffers,', 13, MUTED)
            text(d, 316, 148, 'events, timers, billiard pool threads', 13, MUTED)
            for k in range(3):
                a = t * 6.28 * 2 + k * 2.1
                cx, cy = 620, 120 + k * 20
                d.arc((cx - 7, cy - 7, cx + 7, cy + 7), math.degrees(a), math.degrees(a) + 250,
                      fill=BLUE, width=2)
        else:
            text(d, 316, 102, f'master · {mb(RS_MASTER)}', 15, WHITE, True)
            text(d, 316, 128, 'Rust: consumer, buffers, events, timers, pool', 13, ORANGE)
            text(d, 316, 148, 'Python: bootsteps, signals, Panel', 13, BLUE)
        source_y = 178
        if not original:
            z = ease(t) if phase == 'start' else 1
            if z > 0:
                d.rounded_rectangle((360, 196, 600, 252), radius=8, fill=DARK,
                                    outline=mix(BG, SHARED, z), width=2)
                text(d, 376, 204, f'zygote · {mb(RS_ZYGOTE)}', 14, mix(BG, WHITE, z), True)
                text(d, 376, 226, 'app and tasks, no threads', 12, mix(BG, MUTED, z))
                arrow(d, (480, 178), (480, 194), mix(BG, MUTED, z))
            source_y = 252
        n = 0
        if phase in ('forks', 'hold'):
            n = 10 * t if phase == 'forks' else 10
        size = REF_CHILD if original else RS_CHILD
        for i in range(10):
            grow = min(1.0, max(0.0, n - i))
            if grow > 0:
                arrow(d, (480, source_y), (79 + i * 90, 296), BORDER, 1)
                child_box(d, i, size, BLUE, grow)
        base = REF_MASTER if original else RS_MASTER + RS_ZYGOTE
        total = base + size * min(10, n)
        caption = ('each child forks from the busy master, inherits its heap '
                   'and keeps dirtying it' if original else
                   'the zygote forks before threads start; children inherit its small heap')
        text(d, 60, 412, caption, 14, WHITE, True)
        meter(d, 444, 'Total worker -c 10 RAM (peak under load)', total, 360,
              PRIVATE if original else ORANGE)
        return image
    return draw


def zygote_final(t):
    image, d = canvas('The zygote: where children start',
                      'Total worker -c 10 RAM at peak load (Python 3.11)')
    ref = REF_MASTER + REF_CHILD * 10
    rs = RS_MASTER + RS_ZYGOTE + RS_CHILD * 10
    meter(d, 130, 'Reference Celery: children forked from the master', ref * ease(t), 360, PRIVATE)
    meter(d, 220, 'celery-rs: Rust master I/O, children forked from the zygote', rs * ease(t), 360,
          ORANGE)
    text(d, 60, 330, f'child {mb(REF_CHILD)} → {mb(RS_CHILD)} · master {mb(REF_MASTER)} → '
         f'{mb(RS_MASTER)} (+ zygote {mb(RS_ZYGOTE)})', 15, WHITE, True)
    return image


def zygote():
    render('memoria-2-zygote.gif', [
        (2.0, zygote_scene(True, 'master')), (5.0, zygote_scene(True, 'forks')),
        (1.5, zygote_scene(True, 'hold')),
        (2.0, zygote_scene(False, 'start')), (4.0, zygote_scene(False, 'forks')),
        (1.5, zygote_scene(False, 'hold')), (3.0, zygote_final)])



BLOCKS = ['log.setup', 'tracer', 'stdout']


def mini_child(d, i, fill_blocks, color):
    x, y = 40 + (i % 5) * 180, 196 + (i // 5) * 92
    d.rounded_rectangle((x, y, x + 168, y + 78), radius=8, fill=PANEL, outline=BORDER, width=2)
    text(d, x + 10, y + 6, f'child {i + 1}', 12, MUTED)
    for k in range(3):
        bx = x + 10 + k * 52
        if k < fill_blocks:
            d.rounded_rectangle((bx, y + 30, bx + 46, y + 66), radius=4, fill=color)
        else:
            d.rounded_rectangle((bx, y + 30, bx + 46, y + 66), radius=4, outline=BORDER)


def init_scene(original):
    def draw(t):
        image, d = canvas('Done once, not ten times',
                          'logging, task tracers and stdout: identical in every child',
                          'Reference Celery' if original else 'celery-rs',
                          MUTED if original else ORANGE)
        if original:
            progress = t * 10 * 3
            for i in range(10):
                mini_child(d, i, int(min(3, max(0, progress - i * 3))), PRIVATE)
            done = min(10, progress / 3)
            caption = 'each child builds the same initialization: 10 private copies'
            value = IDLE_AFTER + (IDLE_BEFORE - IDLE_AFTER) * done / 10
        else:
            built = min(3, int(t * 12))
            d.rounded_rectangle((360, 92, 600, 168), radius=8, fill=DARK, outline=SHARED, width=2)
            text(d, 372, 98, 'zygote, before fork', 12, MUTED)
            for k in range(3):
                bx = 372 + k * 74
                if k < built:
                    d.rounded_rectangle((bx, 122, bx + 66, 158), radius=4, fill=SHARED)
                    text(d, bx + 33, 133, BLOCKS[k], 11, BG, True, anchor='ma')
            shown = max(0.0, t - .3) / .7 * 10
            visible = [i for i in range(10) if i < shown]
            for i in visible:  # Draw links first so children remain on top.
                d.line((40 + (i % 5) * 180 + 84, 196 + (i // 5) * 92, 480, 168),
                       fill=SHARED, width=1)
            for i in visible:
                mini_child(d, i, 3, mix(PANEL, SHARED, .45))
            caption = 'the zygote prepares it once; children share the inherited state'
            value = IDLE_AFTER
        text(d, 60, 390, caption, 14, WHITE, True)
        meter(d, 430, 'Idle worker -c 10 RAM (Python 3.11)', value, 80,
              PRIVATE if original else ORANGE)
        return image
    return draw


def init_final(t):
    image, d = canvas('Done once, not ten times', 'Idle worker -c 10 RAM (Python 3.11)')
    meter(d, 130, 'each child initializes itself', IDLE_BEFORE * ease(t), 80, PRIVATE)
    meter(d, 220, 'initialization prepared in the zygote', IDLE_AFTER * ease(t), 80, ORANGE)
    text(d, 60, 330, f'{mb(IDLE_BEFORE - IDLE_AFTER)} less per worker · children still run '
         'worker_process_init and signals', 15, WHITE, True)
    return image


def initialization():
    render('memoria-3-inizializzazione.gif', [
        (6.0, init_scene(True)), (1.0, lambda t: init_scene(True)(1)),
        (5.0, init_scene(False)), (1.0, lambda t: init_scene(False)(1)), (3.0, init_final)])



def balance_scene(t):
    image, d = canvas('Memory budget: worker -c 10 at peak load',
                      'Total PSS · Redis · 30,000 tasks (bench/memprobe/loadpeak.py)')
    for i, (label, value, color) in enumerate(TOTALS):
        p = ease(min(1.0, max(0.0, t * 1.6 - i * .3)))
        text(d, 60, 110 + i * 100, label, 16, color, True)
        d.rounded_rectangle((60, 140 + i * 100, 900, 176 + i * 100), radius=6, fill=DARK)
        width = 840 * value / 360 * p
        if width > 2:
            d.rounded_rectangle((60, 140 + i * 100, 60 + width, 176 + i * 100), radius=6,
                                fill=PRIVATE if i == 0 else color)
        text(d, min(70 + width, 800), 147 + i * 100, f'{value * p:.0f} MB', 16, WHITE, True)
    if t > .8:
        text(d, 60, 440, '−78% on Python 3.11 · −85% on Python 3.13 · throughput ×3', 17,
             WHITE, True)
    return image


def balance():
    render('memoria-4-bilancio.gif', [(5.0, balance_scene)])


if __name__ == '__main__':
    OUT.mkdir(parents=True, exist_ok=True)
    copy_on_write()
    zygote()
    initialization()
    balance()
