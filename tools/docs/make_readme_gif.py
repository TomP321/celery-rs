#!/usr/bin/env python3
"""Generate the GIF: uvx --with pillow python tools/docs/make_readme_gif.py."""
import ast
import subprocess
import tomllib
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'docs/assets/celery-rs-conversione.gif'
SITE = ROOT / '.venv-ref/lib/python3.13/site-packages'
WIDTH, HEIGHT = 960, 680
BG, PANEL, BORDER = '#0b0c10', '#11141b', '#2c303b'
WHITE, MUTED, ORANGE, BLUE = '#f3f1ed', '#a0a7b7', '#e8946f', '#78b6ed'
DARK = '#303642'
GROUPS = ['celery/app', 'celery/worker', 'celery/concurrency', 'celery/backends',
          'celery/beat+schedules', 'celery/events', 'celery/bin', 'celery/utils',
          'kombu', 'billiard', 'amqp/redis-py']
PHASES = ['protocol', 'transports', 'Python client', 'result backend', 'worker',
          'beat', 'CLI', 'optimization', 'hardening', 'RAM']


@lru_cache(maxsize=None)
def font(size, bold=False):
    name = 'DejaVuSansMono-Bold.ttf' if bold else 'DejaVuSansMono.ttf'
    return ImageFont.truetype('/usr/share/fonts/truetype/dejavu/' + name, size)


def text(d, x, y, value, size=11, color=MUTED, bold=False, width=None):
    face = font(size, bold)
    if width is not None:
        while d.textlength(value, font=face) > width:
            value = value[:-2] + '…'
    box = d.textbbox((x, y), value, font=face)
    if box[2] > WIDTH - 14 or box[3] > HEIGHT - 12:
        raise ValueError(f'text outside frame: {value}')
    d.text((x, y), value, fill=color, font=face)


def symbol_lines(source, row):
    symbols = {}

    def visit(node, prefix=''):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                name = prefix + child.name
                symbols[name] = child
                visit(child, name + '.')
            else:
                visit(child, prefix)

    visit(ast.parse(source.read_text()))
    covered = set()
    for name in row['rust_symbols']:
        node = symbols[name]
        covered.update(range(node.lineno, node.end_lineno + 1))
    for selector in row['rust_calls']:
        symbol, call = selector.split('|')
        matches = [n for n in ast.walk(symbols[symbol]) if isinstance(n, ast.Call)
                   and (ast.unparse(n.func) == call or ast.unparse(n.func).endswith('.' + call))]
        assert matches, f'missing call: {selector}'
        for node in matches:
            covered.update(range(node.lineno, node.end_lineno + 1))
    return len(covered)


def load_map():
    data = tomllib.loads((ROOT / 'tools/docs/conversion_map.toml').read_text())
    expected = {'celery/' + p.relative_to(ROOT / 'celery/celery').as_posix()
                for p in (ROOT / 'celery/celery').rglob('*.py')}
    for package in ['kombu', 'billiard', 'amqp', 'redis']:
        expected.update(package + '/' + p.relative_to(SITE / package).as_posix()
                        for p in (SITE / package).rglob('*.py'))
    rows = data['files']
    assert len({r['path'] for r in rows}) == len(rows), 'duplicate files'
    assert {r['path'] for r in rows} == expected, 'incomplete or changed inventory'
    for row in rows:
        assert row['group'] in GROUPS
        assert row['status'] in {'rust', 'python', 'original', 'absent'}
        package, rel = row['path'].split('/', 1)
        source = ROOT / 'celery/celery' / rel if package == 'celery' else SITE / package / rel
        row['lines'] = len(source.read_bytes().splitlines())
        fraction = row.get('rust', 1 if row['status'] == 'rust' else 0)
        assert 0 <= fraction <= 1
        if 'rust_symbols' in row:
            assert abs(symbol_lines(source, row) - fraction * row['lines']) < 1e-8, row['path']
        if fraction:
            assert (ROOT / 'crates' / row['crate']).is_dir()
        row['weight'] = fraction * row['lines']
    return rows


def percentages(rows):
    result = {}
    for group in GROUPS:
        subset = [r for r in rows if r['group'] == group]
        result[group] = sum(r['weight'] for r in subset) / max(1, sum(r['lines'] for r in subset))
    active = [r for r in rows if r['status'] in {'rust', 'python'}]
    total = sum(r['weight'] for r in active) / sum(r['lines'] for r in active)
    return result, total


def commits():
    subjects = subprocess.check_output(['git', 'log', '--reverse', '--format=%s'], cwd=ROOT, text=True).splitlines()
    chosen = [s for s in subjects if s.startswith(('01:', '02', '03:', '04:', '05', '06:', '07:', '08', '09', '10'))
              and not s.startswith('spec')]
    assert chosen, 'no matching commits'
    labels = {
        '05c:': ['prefork Rust pool, epoll loop, Redis bindings and zygote recovery'],
        '06:': ['Rust beat producer, native crontab and embedded beat thread'],
        '07:': ['Rust remote control, CLI and lazy Redis imports'],
        '08a:': ['Rust pidbox/gossip, Redis bindings and lazy imports'],
        '08b:': ['review session 1 and kombu identity tests', 'review 2 and Codex wrapper',
                 'static Redis __all__ and documented lazy TLS descriptor'],
        '08d:': ['preload cffi in zygote, remove an IPC copy, c10 tests and benchmarks'],
        '08f:': ['compact crontabs and reuse persistent beat cache'],
        '08c:': ['measure prefilled capacity and reduce AMQP latency'],
        '09a:': ['add integration suite comparison', 'disable internal retries; 120 s timeout',
                 'parallel integration, one broker per lane',
                 'fix revocations, Redis TTL, pubsub and AMQP properties',
                 'preserve logging descriptors in zygote'],
        '09b:': ['parser limits, proptest fuzzing, fork-safe destructors and soak',
                 '60-minute reference/Rust soak results',
                 'share Redis errors and release large task buffers',
                 '60-minute Rust soak after fixes'],
        '09c:': ['15-minute baseline soak (--compare)',
                 'release traceback cycles after READY and measure child heaps',
                 '15-minute comparison soak after traceback fix',
                 '60-minute Redis reference/Rust soak'],
        '10a:': ['limit Redis restart growth and defer diagnostics',
                 'lazy pools and compact build with optimized crontab'],
    }
    positions = {}
    translated = []
    for subject in chosen:
        prefix = subject.split(' ', 1)[0]
        index = positions.get(prefix, 0)
        if prefix in labels and index < len(labels[prefix]):
            translated.append(prefix + ' ' + labels[prefix][index])
            positions[prefix] = index + 1
        else:
            translated.append(subject)
    return translated


def render(progress, rows, ratios, total, subjects):
    im = Image.new('RGB', (WIDTH, HEIGHT), BG)
    d = ImageDraw.Draw(im)
    d.rounded_rectangle((7, 7, 952, HEIGHT - 8), radius=16, outline=BORDER, width=2)
    d.rounded_rectangle((24, 20, 429, 46), radius=13, fill='#35241f', outline='#80513f')
    text(d, 36, 25, '✻ CLAUDE CODE · DYNAMIC WORKFLOW', 13, ORANGE, True)
    text(d, 773, 27, 'CELERY → RUST', 16, WHITE, True)
    text(d, 24, 61, f'{100 * total * progress:4.1f}% of runtime code in Rust', 25, WHITE, True)
    text(d, 25, 99, 'reference lines whose role is handled by Rust', 13)
    phase = min(9, int(progress * 10))
    text(d, 25, 126, f'phase {phase + 1} · {PHASES[phase]}', 14, ORANGE)
    d.line((24, 154, 935, 154), fill=BORDER)
    text(d, 25, 164, 'FILE · .py → .rs', 12, WHITE, True)
    text(d, 693, 164, '→ RUST BY PACKAGE', 12, WHITE, True)
    particles = []
    protected = [(507, 446, 660, 567)]
    for gi, group in enumerate(GROUPS):
        px, py = 24 + (gi % 4) * 161, 190 + (gi // 4) * 128
        pw, ph = 153, 121
        protected.extend([(px, py, px + pw, py + 22), (px, py + 104, px + pw, py + ph)])
        d.rounded_rectangle((px, py, px + pw, py + ph), radius=7, fill=PANEL, outline=BORDER)
        text(d, px + 7, py + 6, group, 10, WHITE, True)
        subset = [r for r in rows if r['group'] == group]
        # Only client cells group two files, preserving the full inventory.
        batch = (len(subset) + 103) // 104
        for first in range(0, len(subset), batch):
            cell = subset[first:first + batch]
            index = first // batch
            x, y = px + 8 + (index % 13) * 10, py + 27 + (index // 13) * 10
            assert y + 7 <= py + 104, f'too many cells: {group}'
            for j, row in enumerate(cell):
                # Each file has a distinct activation time and an eight-frame trail.
                moment = .025 + gi * .004 + .87 * (first + j + 1) / (len(subset) + 1)
                age = progress - moment
                converted = age >= 0
                color = ORANGE if row['status'] == 'rust' else BLUE if row['status'] == 'python' else DARK
                left = x + j * 8 // len(cell)
                right = x + (j + 1) * 8 // len(cell) - 1
                d.rectangle((left, y, right, y + 7), fill=color if converted else DARK)
                if converted and row['status'] == 'rust' and row.get('rust', 1) < 1:
                    d.line((left, y + 6, right, y + 6), fill=BLUE, width=2)
                if row['status'] == 'rust' and 0 <= age < .04:
                    d.rectangle((x - 1, y - 1, x + 8, y + 8), outline='#ffe2b4')
                    if age < .02:
                        d.rectangle((left, y, right, y + 3), fill='#ffe2b4')
                    particles.append((x + 4, y + 4, gi, age / .04))
        suffix = ' · cell ≤2 files' if batch > 1 else ' · 1 cell/file'
        text(d, px + 7, py + 106, f'{len(subset)} files' + suffix, 9)
        by = 196 + gi * 33
        protected.append((690, by - 2, 936, by + 14))
        text(d, 693, by, group, 11, WHITE)
        d.rounded_rectangle((693, by + 17, 884, by + 23), radius=3, fill=DARK)
        fill = ratios[group] * progress
        if fill:
            d.rectangle((693, by + 17, 693 + round(191 * fill), by + 23), fill=ORANGE)
        text(d, 892, by + 13, f'{100 * fill:.1f}%', 11, ORANGE)
    # Panel twelve holds a compact trace of actual files.
    px, py = 507, 446
    d.rounded_rectangle((px, py, px + 153, py + 121), radius=7, fill=PANEL, outline=BORDER)
    text(d, px + 7, py + 6, 'porting.log', 11, WHITE, True)
    stream = [r for r in rows if r['status'] in {'rust', 'python'}]
    end = min(len(stream), 2 + int(progress * (len(stream) - 2)))
    for i, row in enumerate(stream[max(0, end - 2):end]):
        y = py + 27 + i * 42
        rust = row['status'] == 'rust'
        text(d, px + 7, y, ('port ' if rust else 'keep ') + row['path'], 9,
             ORANGE if rust else BLUE, width=139)
        destination = '→ ' + row['crate'] if rust else 'Python API / runtime'
        text(d, px + 7, y + 14, destination, 9, width=139)
    # Curve particles above panels toward their destination bars.
    for x, y, gi, t in particles:
        ex = 693 + 191 * ratios[GROUPS[gi]] * min(1, progress + .04)
        ey = 216 + gi * 33
        cx, cy = (x + ex) / 2, min(y, ey) - 38
        def point(u):
            return ((1-u)**2*x + 2*(1-u)*u*cx + u*u*ex,
                    (1-u)**2*y + 2*(1-u)*u*cy + u*u*ey)
        for offset, radius, color in [(.12, 1, '#80513f'), (.06, 2, ORANGE), (0, 3, '#ffe2b4')]:
            tx, ty = point(max(0, t - offset))
            if not any(x0 - radius <= tx <= x1 + radius and y0 - radius <= ty <= y1 + radius
                       for x0, y0, x1, y1 in protected):
                d.ellipse((tx-radius, ty-radius, tx+radius, ty+radius), fill=color)
    for x, color, label in [(25, ORANGE, 'Rust'), (118, BLUE, 'Python'), (224, DARK, 'original / absent')]:
        d.rectangle((x, 580, x + 7, 587), fill=color)
        text(d, x + 14, 577, label, 11)
    text(d, 508, 577, 'mixed cells: blue underline', 10)
    d.line((24, 604, 935, 604), fill=BORDER)
    if progress >= 1:
        text(d, 25, 614, '23,300 Rust lines · 6 crates    worker -c 10: 300 → 73 MB (−76%)', 15, WHITE, True)
        text(d, 25, 643, 'In 8 GB: 27 → 96–105 workers     Redis: task/s ×3', 15, ORANGE)
    else:
        end = min(len(subjects), 3 + int(progress * (len(subjects) - 3)))
        for i, subject in enumerate(subjects[max(0, end - 3):end]):
            text(d, 25, 614 + i * 17, '· ' + subject, 11, WHITE if i == 2 else MUTED, width=903)
    return im


def main():
    rows = load_map()
    ratios, total = percentages(rows)
    subjects = commits()
    # Sample grey cells and intermediate colours in the global palette.
    sample = Image.new('RGB', (WIDTH, HEIGHT * 3))
    for i, p in enumerate([0, .5, 1]):
        sample.paste(render(p, rows, ratios, total, subjects), (0, HEIGHT * i))
    palette = sample.quantize(colors=128)
    frames = [render(i / 200, rows, ratios, total, subjects).quantize(palette=palette, dither=Image.Dither.NONE)
              for i in range(201)]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(OUTPUT, save_all=True, append_images=frames[1:],
                   duration=[80] * 200 + [5000], loop=0, optimize=True,
                   palette=palette.getpalette())
    with Image.open(OUTPUT) as gif:
        duration = 0
        for i in range(gif.n_frames):
            gif.seek(i)
            duration += gif.info['duration']
        assert gif.size == (WIDTH, HEIGHT) and gif.info['loop'] == 0
        assert 15000 <= duration <= 25000
        # Extract encoded frames to check quantization too.
        check = ROOT / 'target/readme-gif'
        check.mkdir(parents=True, exist_ok=True)
        for name, index in [('start', 0), ('25', 50), ('50', 100), ('75', 150), ('final', gif.n_frames - 1)]:
            gif.seek(index)
            gif.convert('RGB').save(check / f'{name}.png')
    assert OUTPUT.stat().st_size <= 4_000_000
    print(f'{len(rows)} files; {WIDTH}×{HEIGHT}; {duration / 1000:g} s; 12.5 fps; {OUTPUT.stat().st_size:,} bytes')
    for group, ratio in ratios.items():
        print(f'{group}: {ratio:.2%}')
    print(f'Global: {total:.2%}')


if __name__ == '__main__':
    main()
