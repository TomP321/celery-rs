"""Compare beat parsing and calendar calculations with reference Celery in .venv-ref."""

from __future__ import annotations

import importlib.util
import random
import sys
from datetime import datetime, timedelta, timezone as dt_timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import _celery_rs as rs
from celery import Celery
from celery.schedules import crontab as RefCrontab
from celery.schedules import crontab_parser as RefCrontabParser
from celery.utils.time import ffwd as RefFfwd

SEED = 20261025
ROOT = Path(__file__).resolve().parents[3]


TOTAL_CASES = 240_000

ZONES = (
    'UTC',
    'Europe/Rome',
    'America/New_York',
    'Australia/Lord_Howe',
    'Asia/Kolkata',
)

# Lord Howe's 2026 DST transitions shift by half an hour.
DST_MOMENTS = {
    'Europe/Rome': [datetime(2026, 3, 29, 1, 59), datetime(2026, 3, 29, 3, 0),
                    datetime(2026, 10, 25, 2, 59), datetime(2026, 10, 25, 3, 0)],
    'America/New_York': [datetime(2026, 3, 8, 1, 59), datetime(2026, 3, 8, 3, 0),
                         datetime(2026, 11, 1, 1, 59), datetime(2026, 11, 1, 2, 0)],
    'Australia/Lord_Howe': [datetime(2026, 4, 5, 1, 59), datetime(2026, 4, 5, 2, 30),
                            datetime(2026, 10, 4, 1, 59), datetime(2026, 10, 4, 2, 30)],
}

FIELD_LIMITS = ((60, 0), (24, 0), (7, 0), (31, 1), (12, 1))
NAMES = ('sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat',
         'jan', 'feb', 'mar', 'apr', 'may', 'jun',
         'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
         'MON', 'Fri', 'December')
WEIRD = ('', 'foo', '-20', '-', '5-', '1-2-3', '1.5', '+5', ' 5', '5 ',
         '1__0', '_1', '1_', '1-3x', '*/', '1,', ',1', '0-0/0')


def _load_rs_schedules():
    """Load the shell under celery without replacing reference modules."""
    name = 'celery._rs_schedules_test'
    path = ROOT / 'python' / 'celery' / 'schedules.py'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


RS = _load_rs_schedules()
RSCrontab = RS.crontab


class Now:
    """Share a mutable clock between schedules."""

    __slots__ = ('value',)

    def __init__(self):
        self.value = datetime(2026, 1, 1, tzinfo=dt_timezone.utc)

    def __call__(self):
        return self.value

    def __eq__(self, other):
        return self is other

    __hash__ = object.__hash__


def _outcome(fn, *args):
    try:
        return ('ok', fn(*args))
    except Exception as exc:  # noqa: BLE001 - compare exception types too
        return ('exc', type(exc).__name__, str(exc))


def _random_number(rng, max_, min_):
    high = max(min_, max_ - 1 + min_)
    choice = rng.random()
    if choice < 0.55:
        return str(rng.randint(min_, high))
    if choice < 0.75:
        return str(rng.randint(-3, max(min_, max_ + 5)))
    return rng.choice(('0', '1', '59', '60', '99', '07', '1_0'))


def _random_part(rng, max_, min_):
    choice = rng.random()
    step = rng.choice(('1', '2', '3', '5', '7', '0', 'x', '1_0', ''))
    if choice < 0.12:
        return '*'
    if choice < 0.22:
        return '*/' + step
    if choice < 0.32:
        return rng.choice(('*3', '*x', '* '))
    if choice < 0.50:
        return f'{_random_number(rng, max_, min_)}-{_random_number(rng, max_, min_)}'
    if choice < 0.68:
        return (f'{_random_number(rng, max_, min_)}-'
                f'{_random_number(rng, max_, min_)}/{step}')
    if choice < 0.78:
        return rng.choice(NAMES)
    if choice < 0.9:
        return rng.choice(WEIRD)
    return _random_number(rng, max_, min_)


def _random_spec(rng, max_, min_):
    return ','.join(_random_part(rng, max_, min_)
                    for _ in range(rng.randint(1, 4)))


def _make_app(tz, utc, deadline):
    app = Celery('cron-test', set_as_current=False)
    app.conf.update(
        timezone=tz,
        enable_utc=utc,
        beat_cron_starting_deadline=deadline,
    )
    return app


def _random_field(rng, max_, min_):
    kind = rng.random()
    if kind < 0.45:
        return '*'
    if kind < 0.7:
        return _random_part(rng, max_, min_)
    if kind < 0.8:
        values = range(min_, max_ + min_)
        if not values:
            return str(min_)
        picked = rng.sample(values, min(len(values), rng.randint(1, 4)))
        return ','.join(str(value) for value in sorted(picked))
    high = max(min_, max_ - 1 + min_)
    start = rng.randint(min_, high)
    return f'{start}-{rng.randint(min_, high)}'


def _random_crontabs(rng, count=120):
    crontabs = []
    attempts = 0
    while len(crontabs) < count and attempts < count * 20:
        attempts += 1
        fields = [_random_field(rng, max_, min_) for max_, min_ in FIELD_LIMITS]
        try:
            ref = RefCrontab(*fields)
        except Exception:  # noqa: BLE001 - random fields may be invalid
            continue
        crontabs.append(fields)
    return crontabs


def _random_last_run(rng, zone):
    choice = rng.random()
    if choice < 0.2:
        # The reference treats naive datetimes as UTC.
        return datetime(2020, 1, 1) + timedelta(
            seconds=rng.randrange(0, 11 * 366 * 86400))
    if choice < 0.35 and zone in DST_MOMENTS:
        base = rng.choice(DST_MOMENTS[zone])
        return base.replace(
            second=rng.randrange(60),
            microsecond=rng.randrange(1_000_000),
            tzinfo=ZoneInfo(zone),
        )
    base = datetime(2020, 1, 1, tzinfo=dt_timezone.utc) + timedelta(
        seconds=rng.randrange(0, 11 * 366 * 86400),
        microseconds=rng.randrange(1_000_000),
    )

    other = rng.choice(ZONES + ('Pacific/Auckland',))
    return base.astimezone(ZoneInfo(other))


def test_parser_matches_reference():
    """Compare results and errors for 120,000 random parser inputs."""
    rng = random.Random(SEED)
    limit_pairs = ((60, 0), (24, 0), (7, 0), (31, 1), (12, 1), (24, 1),
                   (1, 0), (5, 3), (0, 0))
    cases = 0
    for _ in range(120_000):
        max_, min_ = rng.choice(limit_pairs)
        spec = _random_spec(rng, max_, min_)
        expected = _outcome(RefCrontabParser(max_, min_).parse, spec)
        actual = _outcome(RS.crontab_parser(max_, min_).parse, spec)
        assert actual == expected, (spec, max_, min_, expected, actual)
        cases += 1

    for spec in WEIRD + ('1-3x', '1-2/3x', '1-2/0', '55-5/3', '*/1_0',
                         ' 5', '+5', ' -5', 'mon-fri', 'SUN', 'jan-dec/2',
                         '1,2,3', '*/1000'):
        for max_, min_ in limit_pairs:
            expected = _outcome(RefCrontabParser(max_, min_).parse, spec)
            actual = _outcome(RS.crontab_parser(max_, min_).parse, spec)
            assert actual == expected, (spec, max_, min_, expected, actual)
            cases += 1
    assert cases >= 120_000


def test_calendar_matches_reference():
    """Compare remaining_delta, remaining_estimate and is_due over 120,000 cases."""
    rng = random.Random(SEED + 1)
    specs = _random_crontabs(rng)
    apps = {}
    pairs = []
    for zone in ZONES:
        for utc in (True, False):
            for deadline in (None, 0, 60, 3600, 86400):
                app = _make_app(zone, utc, deadline)
                apps[(zone, utc, deadline)] = app
    for key, app in apps.items():
        for fields in specs:
            now = Now()
            ref = RefCrontab(*fields, app=app, nowfun=now)
            ours = RSCrontab(*fields, app=app, nowfun=now)
            assert (ref.minute, ref.hour, ref.day_of_week, ref.day_of_month,
                    ref.month_of_year) == (
                        ours.minute, ours.hour, ours.day_of_week,
                        ours.day_of_month, ours.month_of_year)
            pairs.append((key, key[0], ref, ours, now))

    cases = 0
    while cases < 120_000:
        key, zone, ref, ours, now = rng.choice(pairs)
        last_run_at = _random_last_run(rng, zone)
        now.value = _random_last_run(rng, zone)

        ref_delta = ref.remaining_delta(last_run_at)
        our_delta = ours.remaining_delta(last_run_at)
        assert our_delta[0] == ref_delta[0]
        assert our_delta[2] == ref_delta[2]
        assert our_delta[1]._fields() == ref_delta[1]._fields(), (
            key, ref, last_run_at)
        assert ref_delta[1].__class__ is RefFfwd

        assert ours.remaining_estimate(last_run_at) == \
            ref.remaining_estimate(last_run_at), (key, ref, last_run_at)
        assert ours.is_due(last_run_at) == ref.is_due(last_run_at), (
            key, ref, last_run_at)
        cases += 1
    assert cases >= 120_000

    assert cases + 120_000 >= TOTAL_CASES


def test_cron_parse_functions_are_the_native_ones():
    """Verify the shell delegates to the Rust binding."""
    parser = RS.crontab_parser(60)
    assert parser.parse('*/15') == {0, 15, 30, 45}
    assert rs.cron_parse('*/15', 60, 0) == [0, 15, 30, 45]
    spec = rs.CronSpec([0], [0], [0, 1, 2, 3, 4, 5, 6], [1], [1], False)

    args = spec.next_ffwd(2026, 1, 1, 0, 0)
    assert args == (2027, 1, 0, None, 1, 0, 0)


def test_runtime_error_on_invalid_month_set():
    """Match the reference RuntimeError for months with no valid days."""
    # February 30 is invalid at construction; mutate the validated sets.
    broken = RSCrontab(day_of_month='30', month_of_year='2,3', nowfun=Now())
    broken.day_of_month = {30}
    broken.month_of_year = {2}
    broken._rs_cached = None
    with pytest.raises(RuntimeError, match='unable to rollover'):
        broken.remaining_estimate(datetime(2026, 1, 1, tzinfo=dt_timezone.utc))
