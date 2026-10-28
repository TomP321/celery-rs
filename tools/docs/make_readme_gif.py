#!/usr/bin/env python3
"""Genera la GIF: uvx --with pillow python tools/docs/make_readme_gif.py."""
import subprocess
import tomllib
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'docs/assets/celery-rs-conversione.gif'
SITE = ROOT / '.venv-ref/lib/python3.13/site-packages'
WIDTH, HEIGHT = 960, 580
BG, PANEL, BORDER = '#0b0c10', '#11141b', '#2c303b'
WHITE, MUTED, ORANGE, BLUE = '#f3f1ed', '#a0a7b7', '#e8946f', '#78b6ed'
DARK = '#303642'
GROUPS = ['celery/app', 'celery/worker', 'celery/concurrency', 'celery/backends',
          'celery/beat+schedules', 'celery/events', 'celery/bin', 'celery/utils',
          'kombu', 'billiard', 'amqp/redis-py']
PHASES = ['protocollo', 'trasporti', 'client Python', 'result backend', 'worker',
          'beat', 'CLI', 'ottimizzazione', 'hardening', 'RAM']


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
        raise ValueError(f'Testo fuori dal fotogramma: {value}')
    d.text((x, y), value, fill=color, font=face)


def load_map():
    data = tomllib.loads((ROOT / 'tools/docs/conversion_map.toml').read_text())
    expected = {'celery/' + p.relative_to(ROOT / 'celery/celery').as_posix()
                for p in (ROOT / 'celery/celery').rglob('*.py')}
    for package in ['kombu', 'billiard', 'amqp', 'redis']:
        expected.update(package + '/' + p.relative_to(SITE / package).as_posix()
                        for p in (SITE / package).rglob('*.py'))
    rows = data['files']
    assert len({r['path'] for r in rows}) == len(rows), 'File duplicati'
    assert {r['path'] for r in rows} == expected, 'Inventario incompleto o cambiato'
    for row in rows:
        assert row['group'] in GROUPS
        assert row['status'] in {'rust', 'python', 'originale', 'assente'}
        package, rel = row['path'].split('/', 1)
        source = ROOT / 'celery/celery' / rel if package == 'celery' else SITE / package / rel
        row['lines'] = len(source.read_bytes().splitlines())
        fraction = row.get('rust', 1 if row['status'] == 'rust' else 0)
        assert 0 <= fraction <= 1
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
    assert chosen, 'Mancano commit reali'
    return chosen


def render(progress, rows, ratios, total, subjects):
    im = Image.new('RGB', (WIDTH, HEIGHT), BG)
    d = ImageDraw.Draw(im)
    d.rounded_rectangle((7, 7, 952, 572), radius=16, outline=BORDER, width=2)
    d.rounded_rectangle((24, 22, 429, 48), radius=13, fill='#35241f', outline='#80513f')
    text(d, 36, 27, '✻ CLAUDE CODE · DYNAMIC WORKFLOW', 13, ORANGE, True)
    text(d, 773, 29, 'CELERY → RUST', 16, WHITE, True)
    text(d, 24, 63, f'{100 * total * progress:4.1f}% del runtime in Rust', 27, WHITE, True)
    phase = min(9, int(progress * 10))
    text(d, 26, 105, f'fase {phase + 1} · {PHASES[phase]}', 14, ORANGE)
    text(d, 530, 110, 'Pesi LOC · percorso supportato · API Python conservata', 11)
    d.line((24, 140, 935, 140), fill=BORDER)
    text(d, 25, 151, 'porting.log', 12, WHITE, True)
    text(d, 291, 151, 'FILE · .py → .rs', 12, WHITE, True)
    text(d, 694, 151, '→ IN RUST PER PACCHETTO', 12, WHITE, True)
    for x in [274, 679]:
        d.line((x, 151, x, 467), fill=BORDER)
    # Ogni cella corrisponde a un file: nessun file è nascosto dal raggruppamento.
    for gi, group in enumerate(GROUPS):
        y = 184 + gi * 25
        subset = [r for r in rows if r['group'] == group]
        for i, row in enumerate(subset):
            x, cy = 291 + (i % 72) * 5, y + (i // 72) * 6
            threshold = (i + 1) / (len(subset) + 1)
            local = max(0, min(1, (progress - gi * .012) / .88))
            color = DARK
            if local >= threshold:
                color = ORANGE if row['status'] == 'rust' else BLUE if row['status'] == 'python' else DARK
            d.rectangle((x, cy, x + 3, cy + 3), fill=color)
            if color == ORANGE and row.get('rust', 1) < 1:
                d.rectangle((x + 2, cy, x + 3, cy + 3), fill=BLUE)
            if row['status'] == 'rust' and abs(local - threshold) < .018 and progress < 1:
                d.rectangle((x - 1, cy - 1, x + 4, cy + 4), outline='#ffe2b4')
        text(d, 694, y - 5, group, 10, WHITE)
        bar_y = y + 10
        d.rounded_rectangle((694, bar_y, 884, bar_y + 4), radius=2, fill=DARK)
        fill = ratios[group] * progress
        if fill:
            d.rectangle((694, bar_y, 694 + round(190 * fill), bar_y + 4), fill=ORANGE)
        text(d, 893, y, f'{100 * fill:.1f}%', 11, ORANGE)
    # Le righe scorrono in ordine d'inventario; destinazioni di crate reali.
    stream = [r for r in rows if r['status'] in {'rust', 'python'}]
    end = min(len(stream), 5 + int(progress * (len(stream) - 5)))
    for i, row in enumerate(stream[max(0, end - 6):end]):
        y = 183 + i * 45
        rust = row['status'] == 'rust'
        text(d, 25, y, ('port  ' if rust else 'keep  ') + row['path'], 10, ORANGE if rust else BLUE, width=235)
        targets = {'celery-worker': 'pool/…', 'celery-transport': 'redis/…' if 'redis' in row['path'] else 'transport.rs',
                   'celery-backend': 'redis.rs' if 'redis' in row['path'] else 'lib.rs',
                   'celery-beat': 'parser.rs', 'celery-proto': 'compression.rs'}
        destination = '→ ' + row['crate'] + '/src/' + targets[row['crate']] if rust else '(API / runtime Python)'
        text(d, 37, y + 15, destination, 10, MUTED, width=222)
    for x, color, label in [(291, ORANGE, 'Rust'), (385, BLUE, 'Python'), (493, DARK, 'originale/assente')]:
        d.rectangle((x, 463, x + 6, 469), fill=color)
        text(d, x + 12, 458, label, 10)
    d.line((24, 484, 935, 484), fill=BORDER)
    if progress >= 1:
        text(d, 25, 495, '23.300 righe Rust · 6 crate    worker -c 10: 300 → 73 MB (−76%)', 15, WHITE, True)
        text(d, 25, 527, 'In 8 GB: 27 → 96–105 worker     Redis: task/s ×3', 15, ORANGE)
    else:
        end = min(len(subjects), 3 + int(progress * (len(subjects) - 3)))
        for i, subject in enumerate(subjects[max(0, end - 3):end]):
            text(d, 25, 495 + i * 20, '· ' + subject, 11, WHITE if i == 2 else MUTED, width=903)
    return im


def main():
    rows = load_map()
    ratios, total = percentages(rows)
    subjects = commits()
    # Una palette globale campiona anche celle grigie e colori intermedi.
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
        # Estratti dalla GIF codificata, così si controlla anche la quantizzazione.
        check = ROOT / 'target/readme-gif'
        check.mkdir(parents=True, exist_ok=True)
        for name, index in [('inizio', 0), ('25', 50), ('50', 100), ('75', 150), ('finale', gif.n_frames - 1)]:
            gif.seek(index)
            gif.convert('RGB').save(check / f'{name}.png')
    assert OUTPUT.stat().st_size <= 4_000_000
    print(f'{len(rows)} file; {WIDTH}×{HEIGHT}; {duration / 1000:g} s; 12,5 fps; {OUTPUT.stat().st_size:,} byte')
    for group, ratio in ratios.items():
        print(f'{group}: {ratio:.2%}')
    print(f'Globale: {total:.2%}')


if __name__ == '__main__':
    main()
