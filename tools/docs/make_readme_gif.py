#!/usr/bin/env python3
"""Genera la GIF del README: uvx --with pillow python tools/docs/make_readme_gif.py."""
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'docs/assets/celery-rs-conversione.gif'
WIDTH, HEIGHT = 960, 1130
FONT_DIR = Path('/usr/share/fonts/truetype/dejavu')
BG, INK, MUTED = '#f5f7fa', '#172536', '#46566a'
COLORS = ['#a94408', '#185caa', '#52606d', '#b52c36']
FILLS = ['#fff0e4', '#eaf3ff', '#edf0f3', '#fff0f1']


def font(size, bold=False):
    return ImageFont.truetype(str(FONT_DIR / ('DejaVuSans-Bold.ttf' if bold else 'DejaVuSans.ttf')), size)


def label(draw, xy, value, size=19, fill=INK, bold=False):
    face = font(size, bold)
    # Il controllo rende visibili anche le regressioni dovute a nuove etichette.
    box = draw.textbbox(xy, value, font=face)
    if box[2] > WIDTH - 24 or box[3] > HEIGHT - 20:
        raise ValueError(f'Testo fuori dal fotogramma: {value}')
    draw.text(xy, value, font=face, fill=fill)


RUST = [
    ('Protocollo · celery-proto', 'Task v1/v2, header, JSON, compressione'),
    ('Trasporti · celery-transport', 'Redis, RabbitMQ (AMQP 0-9-1), memory://'),
    ('Producer · delay() / apply_async()', 'Serializzazione e invio'),
    ('Risultati · celery-backend', 'Backend Redis e RPC'),
    ('Consumer · celery-worker', 'QoS/prefetch, ack, eventi, heartbeat'),
    ('Timer e stato del worker', 'ETA, rate limit, revoche'),
    ('Controllo remoto · trasporto', 'Pidbox, mingle, gossip; comandi Python'),
    ('Pool prefork', 'Supervisore, zygote, IPC con i figli'),
    ('Beat e client di controllo CLI', 'Crontab e invio; inspect / control'),
]
PYTHON = [
    ('App e API dei task', 'Configurazione, registry, classe Task'),
    ('Canvas e risultati', 'chain, group, chord, AsyncResult'),
    ('Estensioni pubbliche', 'Segnali, bootsteps, comandi Panel'),
    ('Esecuzione nei figli', 'Trace dei task e codice utente'),
    ('Interfaccia CLI e integrazioni', 'click, loader, fixup Django'),
    ('Persistenza di beat', 'Schedule su shelve'),
]


def panel(draw, x, y, w, h, group, title, subtitle, rows, visible):
    color = COLORS[group] if visible else '#77818d'
    draw.rounded_rectangle((x, y, x + w, y + h), radius=16,
                           fill=FILLS[group] if visible else '#eef1f5', outline=color, width=2)
    label(draw, (x + 16, y + 14), title, 23, color, True)
    label(draw, (x + 16, y + 48), subtitle, 17, MUTED)
    if visible:
        for i, (heading, detail) in enumerate(rows):
            ry = y + 88 + i * 54
            for text, size, offset, bold in [(heading, 18, 0, True), (detail, 16, 25, False)]:
                if draw.textlength(text, font=font(size, bold)) > w - 32:
                    raise ValueError(f'Etichetta troppo larga: {text}')
                label(draw, (x + 16, ry + offset), text, size, INK, bold)
    else:
        label(draw, (x + 16, y + 92), 'Segue nell’animazione…', 18, MUTED)


def frame(stage):
    im = Image.new('RGB', (WIDTH, HEIGHT), BG)
    d = ImageDraw.Draw(im)
    label(d, (28, 22), 'celery-rs', 38, INK, True)
    label(d, (28, 73), 'Cosa passa in Rust, cosa resta in Python', 27, INK, True)
    label(d, (28, 114), 'Stessa API pubblica di Celery 5.7.0rc1 · core Rust, task Python', 20, MUTED)
    for i, (x, title) in enumerate([(28, 'Rust'), (192, 'Python'), (376, 'Non convertito'), (610, 'Non supportato / non verificato')]):
        d.rectangle((x, 157, x + 12, 169), fill=COLORS[i])
        label(d, (x + 20, 151), title, 15, COLORS[i], True)
    panel(d, 28, 190, 446, 590, 0, '01  In Rust', 'Core e percorsi di I/O', RUST, stage >= 0)
    panel(d, 486, 190, 446, 590, 1, '02  In Python per scelta', 'API identica, estendibile dall’utente', PYTHON, stage >= 1)
    if stage >= 1:
        label(d, (502, 643), 'celery-beat: crontab in Rust; fusi orari', 17, MUTED)
        label(d, (502, 669), 'e DST restano Python, come shelve.', 17, MUTED)
    panel(d, 28, 795, 446, 171, 2, '03  Non convertito', 'Percorso originale di Celery', [
        ('Altri broker tramite kombu: SQS, …', 'Backend: SQLAlchemy, cache, S3,'),
    ], stage >= 2)
    if stage >= 2:
        label(d, (44, 935), 'MongoDB, DynamoDB, …', 16)
    panel(d, 486, 795, 446, 171, 3, '04  Limiti', 'Non supportato / non verificato', [
        ('Pool eventlet e gevent', 'macOS e Windows · solo Linux'),
    ], stage >= 3)
    if stage == 4:
        d.rounded_rectangle((28, 982, 932, 1103), radius=14, fill=INK)
        for y, text, size in [
            (992, '23.300 righe Rust · 6 crate | guscio Python: 52.600 righe', 21),
            (1022, 'Worker -c 10: 300 → 73 MB (−76%) | in 8 GB: 27 → 96–105 | Redis: task/s ×3', 20),
            (1052, 'Verifiche: API (137 moduli), messaggi sul broker, 190 comandi CLI,', 19),
            (1077, '31.691 unit test e suite t/integration di Celery.', 19),
        ]:
            label(d, (44, y), text, size, '#ffffff', y == 992)
    else:
        label(d, (28, 1001), f'{stage + 1}/5 · ' + ['Il core Rust', 'L’API Python', 'Il percorso originale', 'I limiti della copertura'][stage], 22, INK, True)
        label(d, (28, 1041), 'Segue: riepilogo di dimensioni, memoria, prestazioni e verifiche.', 20, MUTED)
    return im


def main():
    frames = [frame(stage) for stage in range(5)]
    # Una palette comune evita variazioni di colore tra i fotogrammi.
    palette = frames[-1].quantize(colors=128)
    indexed = [im.quantize(palette=palette, dither=Image.Dither.NONE) for im in frames]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    indexed[0].save(OUTPUT, save_all=True, append_images=indexed[1:],
                    duration=[2500, 3000, 2500, 2500, 6500], loop=0, optimize=False)
    with Image.open(OUTPUT) as gif:
        duration = 0
        for i in range(gif.n_frames):
            gif.seek(i)
            duration += gif.info['duration']
        assert gif.size[0] == 960 and gif.info['loop'] == 0
        assert 10000 <= duration <= 20000
    assert OUTPUT.stat().st_size <= 1_500_000
    print(f'{OUTPUT.relative_to(ROOT)}: {OUTPUT.stat().st_size:,} byte, {duration / 1000:g} s')


if __name__ == '__main__':
    main()
