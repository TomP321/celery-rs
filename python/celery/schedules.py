"""Schedules define the intervals at which periodic tasks run."""
from __future__ import annotations

import re
from bisect import bisect, bisect_left
from collections import namedtuple
from datetime import datetime, timedelta, tzinfo
from typing import Any, Callable, Iterable, Mapping, Sequence, Union

import _celery_rs
from kombu.utils.objects import cached_property

from celery import Celery

from . import current_app
from .exceptions import ImproperlyConfigured
from .utils.collections import AttributeDict
from .utils.time import (ffwd, humanize_seconds, is_naive, localize, maybe_make_aware,
                         maybe_timedelta, remaining, subtract_datetimes, timezone, weekday,
                         yearmonth)

__all__ = (
    'ParseException', 'schedule', 'crontab', 'crontab_parser',
    'maybe_schedule', 'solar',
)

schedstate = namedtuple('schedstate', ('is_due', 'next'))

#: Classe `ffwd` globale del modulo: `remaining_estimate` usa il percorso
#: veloce solo se non ne è stata passata una diversa.
_GLOBAL_FFWD = ffwd

#: `timezone.utc` del riferimento è una property: tenerne il valore fisso
#: evita un accesso a ogni chiamata nel percorso caldo.
_UTC = timezone.utc


def _ffwd_from_args(args, ffwd=ffwd):
    """Costruisce l'`ffwd` del riferimento dagli argomenti restituiti da Rust.

    Gli stessi rami di `remaining_delta`: il ramo "_delta_to_next" usa sempre
    la classe globale, gli altri quella eventualmente passata dal chiamante.
    """
    year, month, weeks, weekday, day, hour, minute = args
    if year is not None:
        return _GLOBAL_FFWD(year=year, month=month, day=day, hour=hour,
                            minute=minute, second=0, microsecond=0)
    if weekday is not None:
        return ffwd(weeks=weeks, weekday=weekday, hour=hour,
                    minute=minute, second=0, microsecond=0)
    if hour is not None:
        return ffwd(hour=hour, minute=minute, second=0, microsecond=0)
    return ffwd(minute=minute, second=0, microsecond=0)


def _add_ffwd(start, args):
    """`start + ffwd(...)` per gli argomenti prodotti da Rust."""
    year, month, weeks, weekday, day, hour, minute = args
    if year is not None:
        # Rust produce solo giorni validi: il clamp di `ffwd` è inutile.
        ret = start.replace(year=year, month=month, day=day, hour=hour,
                            minute=minute, second=0, microsecond=0)
    elif weekday is not None:
        ret = start.replace(hour=hour, minute=minute, second=0,
                            microsecond=0)
        ret += timedelta(days=(7 - ret.weekday() + weekday) % 7)
    elif hour is not None:
        ret = start.replace(hour=hour, minute=minute, second=0,
                            microsecond=0)
    else:
        ret = start.replace(minute=minute, second=0, microsecond=0)
    # `ffwd.__radd__` somma sempre `timedelta(days=weeks * 7)`; l'addizione
    # azzera `fold`, cosa che conta sulle ore ambigue del DST.
    if weeks or ret.fold:
        ret += timedelta(days=weeks * 7)
    return ret


CRON_PATTERN_INVALID = """\
Invalid crontab pattern.  Valid range is {min}-{max}. \
'{value}' was found.\
"""

CRON_INVALID_TYPE = """\
Argument cronspec needs to be of any of the following types: \
int, str, or an iterable type. {type!r} was given.\
"""

CRON_INVALID_DATE = """\
Invalid crontab: day_of_month {day_of_month!r} never occurs in \
month_of_year {month_of_year!r}.\
"""

# Number of days in each month, using the longest February (leap years).
CRON_DAYS_IN_MONTH = {
    1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
    7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
}

CRON_REPR = """\
<crontab: {0._orig_minute} {0._orig_hour} {0._orig_day_of_month} {0._orig_month_of_year} \
{0._orig_day_of_week} (m/h/dM/MY/d)>\
"""

SOLAR_INVALID_LATITUDE = """\
Argument latitude {lat} is invalid, must be between -90 and 90.\
"""

SOLAR_INVALID_LONGITUDE = """\
Argument longitude {lon} is invalid, must be between -180 and 180.\
"""

SOLAR_INVALID_EVENT = """\
Argument event "{event}" is invalid, must be one of {all_events}.\
"""

SOLAR_EPHEM_NOT_INSTALLED = """\
You need to install the ephem library to use solar schedules.
Please install by:

    $ pip install celery[solar]
"""


Cronspec = Union[int, str, Iterable[int]]


def cronfield(s: Cronspec | None) -> Cronspec:
    return '*' if s is None else s


class ParseException(Exception):
    """Raised by :class:`crontab_parser` when the input can't be parsed."""


class BaseSchedule:

    def __init__(self, nowfun: Callable | None = None, app: Celery | None = None):
        self.nowfun = nowfun
        self._app = app

    def now(self) -> datetime:
        return (self.nowfun or self.app.now)()

    def remaining_estimate(self, last_run_at: datetime) -> timedelta:
        raise NotImplementedError()

    def is_due(self, last_run_at: datetime) -> tuple[bool, datetime]:
        raise NotImplementedError()

    def maybe_make_aware(
            self, dt: datetime, naive_as_utc: bool = True) -> datetime:
        return maybe_make_aware(dt, self.tz, naive_as_utc=naive_as_utc)

    @property
    def app(self) -> Celery:
        return self._app or current_app._get_current_object()

    @app.setter
    def app(self, app: Celery) -> None:
        self._app = app

    @cached_property
    def tz(self) -> tzinfo:
        return self.app.timezone

    @cached_property
    def utc_enabled(self) -> bool:
        return self.app.conf.enable_utc

    def to_local(self, dt: datetime) -> datetime:
        if not self.utc_enabled:
            return timezone.to_local_fallback(dt)
        return dt

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BaseSchedule):
            return other.nowfun == self.nowfun
        return NotImplemented


class schedule(BaseSchedule):
    """Schedule for periodic task.

    Arguments:
        run_every (float, ~datetime.timedelta): Time interval.
        relative (bool):  If set to True the run time will be rounded to the
            resolution of the interval.
        nowfun (Callable): Function returning the current date and time
            (:class:`~datetime.datetime`).
        app (Celery): Celery app instance.
    """

    relative: bool = False

    def __init__(self, run_every: float | timedelta | None = None,
                 relative: bool = False, nowfun: Callable | None = None, app: Celery
                 | None = None) -> None:
        self.run_every = maybe_timedelta(run_every)
        self.relative = relative
        super().__init__(nowfun=nowfun, app=app)

    def remaining_estimate(self, last_run_at: datetime) -> timedelta:
        return remaining(
            self.maybe_make_aware(last_run_at), self.run_every,
            self.maybe_make_aware(self.now()), self.relative,
        )

    def is_due(self, last_run_at: datetime) -> tuple[bool, datetime]:
        """Return tuple of ``(is_due, next_time_to_check)``.

        Notes:
            - next time to check is in seconds.

            - ``(True, 20)``, means the task should be run now, and the next
                time to check is in 20 seconds.

            - ``(False, 12.3)``, means the task is not due, but that the
              scheduler should check again in 12.3 seconds.

        The next time to check is used to save energy/CPU cycles,
        it does not need to be accurate but will influence the precision
        of your schedule.  You must also keep in mind
        the value of :setting:`beat_max_loop_interval`,
        that decides the maximum number of seconds the scheduler can
        sleep between re-checking the periodic task intervals.  So if you
        have a task that changes schedule at run-time then your next_run_at
        check will decide how long it will take before a change to the
        schedule takes effect.  The max loop interval takes precedence
        over the next check at value returned.

        .. admonition:: Scheduler max interval variance

            The default max loop interval may vary for different schedulers.
            For the default scheduler the value is 5 minutes, but for example
            the :pypi:`django-celery-beat` database scheduler the value
            is 5 seconds.
        """
        last_run_at = self.maybe_make_aware(last_run_at)
        rem_delta = self.remaining_estimate(last_run_at)
        remaining_s = max(rem_delta.total_seconds(), 0)
        if remaining_s == 0:
            next_s = self.seconds
            if self.relative:
                # The next rounded deadline can differ from the interval,
                # especially across DST or after a late run / restart. Realign
                # to the next rounded clock boundary and do not check past it.
                now = self.maybe_make_aware(self.now())
                now = now.astimezone(last_run_at.tzinfo)
                next_s = self.remaining_estimate(now).total_seconds()
                if next_s <= 0:
                    # Preserve the interval for subsecond rounding and
                    # zero or negative intervals with no future boundary.
                    next_s = self.seconds
            return schedstate(is_due=True, next=next_s)
        return schedstate(is_due=False, next=remaining_s)

    def __repr__(self) -> str:
        return f'<freq: {self.human_seconds}>'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, schedule):
            return self.run_every == other.run_every
        return self.run_every == other

    def __reduce__(self) -> tuple[type,
                                  tuple[timedelta, bool, Callable | None]]:
        return self.__class__, (self.run_every, self.relative, self.nowfun)

    @property
    def seconds(self) -> int | float:
        return max(self.run_every.total_seconds(), 0)

    @property
    def human_seconds(self) -> str:
        return humanize_seconds(self.seconds)


class crontab_parser:
    """Parser for Crontab expressions.

    Any expression of the form 'groups'
    (see BNF grammar below) is accepted and expanded to a set of numbers.
    These numbers represent the units of time that the Crontab needs to
    run on:

    .. code-block:: bnf

        digit   :: '0'..'9'
        dow     :: 'a'..'z'
        number  :: digit+ | dow+
        steps   :: number
        range   :: number ( '-' number ) ?
        numspec :: '*' | range
        expr    :: numspec ( '/' steps ) ?
        groups  :: expr ( ',' expr ) *

    The parser is a general purpose one, useful for parsing hours, minutes and
    day of week expressions.  Example usage:

    .. code-block:: pycon

        >>> minutes = crontab_parser(60).parse('*/15')
        [0, 15, 30, 45]
        >>> hours = crontab_parser(24).parse('*/4')
        [0, 4, 8, 12, 16, 20]
        >>> day_of_week = crontab_parser(7).parse('*')
        [0, 1, 2, 3, 4, 5, 6]

    It can also parse day of month and month of year expressions if initialized
    with a minimum of 1.  Example usage:

    .. code-block:: pycon

        >>> days_of_month = crontab_parser(31, 1).parse('*/3')
        [1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31]
        >>> months_of_year = crontab_parser(12, 1).parse('*/2')
        [1, 3, 5, 7, 9, 11]
        >>> months_of_year = crontab_parser(12, 1).parse('2-12/2')
        [2, 4, 6, 8, 10, 12]

    The maximum possible expanded value returned is found by the formula:

        :math:`max_ + min_ - 1`
    """

    ParseException = ParseException

    _range = r'(\w+?)-(\w+)'
    _steps = r'/(\w+)?'
    _star = r'\*'

    def __init__(self, max_: int = 60, min_: int = 0):
        self.max_ = max_
        self.min_ = min_
        self.pats: tuple[tuple[re.Pattern, Callable], ...] = (
            (re.compile(self._range + self._steps), self._range_steps),
            (re.compile(self._range), self._expand_range),
            (re.compile(self._star + self._steps), self._star_steps),
            (re.compile('^' + self._star + '$'), self._expand_star),
        )

    def parse(self, spec: str) -> set[int]:
        try:
            return set(_celery_rs.cron_parse(spec, self.max_, self.min_))
        except _celery_rs.CronParseError as exc:
            raise self.ParseException(str(exc)) from None

    def _parse_part(self, part: str) -> list[int]:
        for regex, handler in self.pats:
            m = regex.match(part)
            if m:
                return handler(m.groups())
        return self._expand_range((part,))

    def _expand_range(self, toks: Sequence[str]) -> list[int]:
        fr = self._expand_number(toks[0])
        if len(toks) > 1:
            to = self._expand_number(toks[1])
            if to < fr:  # Wrap around max_ if necessary
                return (list(range(fr, self.min_ + self.max_)) +
                        list(range(self.min_, to + 1)))
            return list(range(fr, to + 1))
        return [fr]

    def _range_steps(self, toks: Sequence[str]) -> list[int]:
        if len(toks) != 3 or not toks[2]:
            raise self.ParseException('empty filter')
        return self._expand_range(toks[:2])[::int(toks[2])]

    def _star_steps(self, toks: Sequence[str]) -> list[int]:
        if not toks or not toks[0]:
            raise self.ParseException('empty filter')
        return self._expand_star()[::int(toks[0])]

    def _expand_star(self, *args: Any) -> list[int]:
        return list(range(self.min_, self.max_ + self.min_))

    def _expand_number(self, s: str) -> int:
        if isinstance(s, str) and s[0] == '-':
            raise self.ParseException('negative numbers not supported')
        try:
            i = int(s)
        except ValueError:
            try:
                i = yearmonth(s)
            except KeyError:
                try:
                    i = weekday(s)
                except KeyError:
                    raise ValueError(f'Invalid weekday literal {s!r}.')

        max_val = self.min_ + self.max_ - 1
        if i > max_val:
            raise ValueError(
                f'Invalid end range: {i} > {max_val}.')
        if i < self.min_:
            raise ValueError(
                f'Invalid beginning range: {i} < {self.min_}.')

        return i


class crontab(BaseSchedule):
    """Crontab schedule.

    A Crontab can be used as the ``run_every`` value of a
    periodic task entry to add :manpage:`crontab(5)`-like scheduling.

    Like a :manpage:`cron(5)`-job, you can specify units of time of when
    you'd like the task to execute.  It's a reasonably complete
    implementation of :command:`cron`'s features, so it should provide a fair
    degree of scheduling needs.

    You can specify a minute, an hour, a day of the week, a day of the
    month, and/or a month in the year in any of the following formats:

    .. attribute:: minute

        - A (list of) integers from 0-59 that represent the minutes of
          an hour of when execution should occur; or
        - A string representing a Crontab pattern.  This may get pretty
          advanced, like ``minute='*/15'`` (for every quarter) or
          ``minute='1,13,30-45,50-59/2'``.

    .. attribute:: hour

        - A (list of) integers from 0-23 that represent the hours of
          a day of when execution should occur; or
        - A string representing a Crontab pattern.  This may get pretty
          advanced, like ``hour='*/3'`` (for every three hours) or
          ``hour='0,8-17/2'`` (at midnight, and every two hours during
          office hours).

    .. attribute:: day_of_week

        - A (list of) integers from 0-6, where Sunday = 0 and Saturday =
          6, that represent the days of a week that execution should
          occur.
        - A string representing a Crontab pattern.  This may get pretty
          advanced, like ``day_of_week='mon-fri'`` (for weekdays only).
          (Beware that ``day_of_week='*/2'`` does not literally mean
          'every two days', but 'every day that is divisible by two'!)

    .. attribute:: day_of_month

        - A (list of) integers from 1-31 that represents the days of the
          month that execution should occur.
        - A string representing a Crontab pattern.  This may get pretty
          advanced, such as ``day_of_month='2-30/2'`` (for every even
          numbered day) or ``day_of_month='1-7,15-21'`` (for the first and
          third weeks of the month).

    .. attribute:: month_of_year

        - A (list of) integers from 1-12 that represents the months of
          the year during which execution can occur.
        - A string representing a Crontab pattern.  This may get pretty
          advanced, such as ``month_of_year='*/3'`` (for the first month
          of every quarter) or ``month_of_year='2-12/2'`` (for every even
          numbered month).

    .. attribute:: nowfun

        Function returning the current date and time
        (:class:`~datetime.datetime`).

    .. attribute:: app

        The Celery app instance.

    It's important to realize that any day on which execution should
    occur must be represented by entries in all three of the day and
    month attributes.  For example, if ``day_of_week`` is 0 and
    ``day_of_month`` is every seventh day, only months that begin
    on Sunday and are also in the ``month_of_year`` attribute will have
    execution events.  Or, ``day_of_week`` is 1 and ``day_of_month``
    is '1-7,15-21' means every first and third Monday of every month
    present in ``month_of_year``.

    :exc:`ValueError` is raised if none of the days in ``day_of_month``
    occur in any month in ``month_of_year`` (e.g., the 30th of February).
    """

    def __init__(self, minute: Cronspec = '*', hour: Cronspec = '*', day_of_week: Cronspec = '*',
                 day_of_month: Cronspec = '*', month_of_year: Cronspec = '*', **kwargs: Any) -> None:
        self._orig_minute = cronfield(minute)
        self._orig_hour = cronfield(hour)
        self._orig_day_of_week = cronfield(day_of_week)
        self._orig_day_of_month = cronfield(day_of_month)
        self._orig_month_of_year = cronfield(month_of_year)
        self._orig_kwargs = kwargs
        self.hour = self._expand_cronspec(hour, 24)
        self.minute = self._expand_cronspec(minute, 60)
        self.day_of_week = self._expand_cronspec(day_of_week, 7)
        self.day_of_month = self._expand_cronspec(day_of_month, 31, 1)
        self.month_of_year = self._expand_cronspec(month_of_year, 12, 1)
        if not any(day <= CRON_DAYS_IN_MONTH[month]
                   for month in self.month_of_year
                   for day in self.day_of_month):
            raise ValueError(CRON_INVALID_DATE.format(
                day_of_month=self._orig_day_of_month,
                month_of_year=self._orig_month_of_year,
            ))
        super().__init__(**kwargs)
        #: `remaining_estimate` può usare il percorso veloce solo se la
        #: sottoclasse non ridefinisce `remaining_delta`.
        self._fast_estimate = (
            type(self).remaining_delta is crontab.remaining_delta)
        self._rs_cached = None

    @classmethod
    def from_string(cls, crontab: str) -> crontab:
        """
        Create a Crontab from a cron expression string. For example ``crontab.from_string('* * * * *')``.

        .. code-block:: text

            ┌───────────── minute (0–59)
            │ ┌───────────── hour (0–23)
            │ │ ┌───────────── day of the month (1–31)
            │ │ │ ┌───────────── month (1–12)
            │ │ │ │ ┌───────────── day of the week (0–6) (Sunday to Saturday)
            * * * * *
        """
        minute, hour, day_of_month, month_of_year, day_of_week = crontab.split(" ")
        return cls(minute, hour, day_of_week, day_of_month, month_of_year)

    @staticmethod
    def _expand_cronspec(
            cronspec: Cronspec,
            max_: int, min_: int = 0) -> set[Any]:
        """Expand cron specification.

        Takes the given cronspec argument in one of the forms:

        .. code-block:: text

            int         (like 7)
            str         (like '3-5,*/15', '*', or 'monday')
            set         (like {0,15,30,45}
            list        (like [8-17])

        And convert it to an (expanded) set representing all time unit
        values on which the Crontab triggers.  Only in case of the base
        type being :class:`str`, parsing occurs.  (It's fast and
        happens only once for each Crontab instance, so there's no
        significant performance overhead involved.)

        For the other base types, merely Python type conversions happen.

        The argument ``max_`` is needed to determine the expansion of
        ``*`` and ranges.  The argument ``min_`` is needed to determine
        the expansion of ``*`` and ranges for 1-based cronspecs, such as
        day of month or month of year.  The default is sufficient for minute,
        hour, and day of week.
        """
        if isinstance(cronspec, int):
            result = {cronspec}
        elif isinstance(cronspec, str):
            result = crontab_parser(max_, min_).parse(cronspec)
        elif isinstance(cronspec, set):
            result = cronspec
        elif isinstance(cronspec, Iterable):
            result = set(cronspec)  # type: ignore
        else:
            raise TypeError(CRON_INVALID_TYPE.format(type=type(cronspec)))

        # assure the result does not precede the min or exceed the max
        for number in result:
            if number >= max_ + min_ or number < min_:
                raise ValueError(CRON_PATTERN_INVALID.format(
                    min=min_, max=max_ - 1 + min_, value=number))
        return result

    def _delta_to_next(self, last_run_at: datetime, next_hour: int,
                       next_minute: int) -> ffwd:
        """Find next delta.

        Takes a :class:`~datetime.datetime` of last run, next minute and hour,
        and returns a :class:`~celery.utils.time.ffwd` for the next
        scheduled day and time.

        Only called when ``day_of_month`` and/or ``month_of_year``
        cronspec is specified to further limit scheduled task execution.

        Il calcolo è in Rust (`crates/celery-beat`); i fusi orari restano qui.
        """
        return _ffwd_from_args(self._rs_spec().delta_to_next(
            last_run_at.year, last_run_at.month, last_run_at.day,
            next_hour, next_minute))

    def _rs_spec(self):
        """CronSpec Rust per i campi correnti, ricostruito se i set cambiano."""
        cached = self._rs_cached
        minute = self.minute
        hour = self.hour
        day_of_week = self.day_of_week
        day_of_month = self.day_of_month
        month_of_year = self.month_of_year
        all_dom_moy = (self._orig_day_of_month == '*' and
                       self._orig_month_of_year == '*')
        if (cached is None or cached[0] is not minute or
                cached[1] is not hour or cached[2] is not day_of_week or
                cached[3] is not day_of_month or
                cached[4] is not month_of_year or
                cached[5] != all_dom_moy):
            spec = _celery_rs.CronSpec(
                list(minute), list(hour), list(day_of_week),
                list(day_of_month), list(month_of_year), all_dom_moy)
            cached = (minute, hour, day_of_week, day_of_month, month_of_year,
                      all_dom_moy, spec)
            self._rs_cached = cached
        return cached[6]

    def _next_ffwd_args(self, last_run_at):
        return self._rs_spec().next_ffwd(
            last_run_at.year, last_run_at.month, last_run_at.day,
            last_run_at.hour, last_run_at.minute)

    def _remaining_delta_fast(self, last_run_at, tz=None):
        if tz is None:
            # `CachedProperty` di kombu salva il valore in `__dict__['tz']`;
            # può essere una stringa (es. app.timezone impostato dai test).
            cached_tz = self.__dict__.get('tz')
            schedule_tz: tzinfo = (
                cached_tz if isinstance(cached_tz, tzinfo)
                else timezone.get_timezone(cached_tz or self.tz))
        else:
            schedule_tz = timezone.get_timezone(tz)
        current_tz = last_run_at.tzinfo
        if current_tz is None or current_tz.utcoffset(last_run_at) is None:
            last_run_at = self.maybe_make_aware(last_run_at)
        if last_run_at.tzinfo is not schedule_tz:
            last_run_at = last_run_at.astimezone(schedule_tz)
        now = self.now()
        current_tz = now.tzinfo
        if current_tz is None or current_tz.utcoffset(now) is None:
            now = self.maybe_make_aware(now)
        if now.tzinfo is not schedule_tz:
            now = now.astimezone(schedule_tz)
        return last_run_at, self._next_ffwd_args(last_run_at), now

    def __repr__(self) -> str:
        return CRON_REPR.format(self)

    def __reduce__(self) -> tuple[type, tuple[Cronspec, Cronspec, Cronspec, Cronspec, Cronspec], Any]:
        return (self.__class__, (self._orig_minute,
                                 self._orig_hour,
                                 self._orig_day_of_week,
                                 self._orig_day_of_month,
                                 self._orig_month_of_year), self._orig_kwargs)

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        # Calling super's init because the kwargs aren't necessarily passed in
        # the same form as they are stored by the superclass
        super().__init__(**state)
        self._orig_kwargs = dict(state)

    def remaining_delta(self, last_run_at: datetime,
                        tz: str | tzinfo | None = None,
                        ffwd: type = ffwd) -> tuple[datetime, Any, datetime]:
        # caching global ffwd
        # Normalize both datetimes into the schedule's timezone, so that the
        # crontab field matching and the next-run arithmetic below operate in
        # the frame the crontab is defined in. An aware last_run_at may arrive
        # in a different timezone (e.g. from django-celery-beat).
        last_run_at, args, now = self._remaining_delta_fast(last_run_at, tz)
        return last_run_at, _ffwd_from_args(args, ffwd), now

    def remaining_estimate(
            self, last_run_at: datetime, ffwd: type = ffwd) -> timedelta:
        """Estimate of next run time.

        Returns when the periodic task should run next as a
        :class:`~datetime.timedelta`.
        """
        # pylint: disable=redefined-outer-name
        # caching global ffwd
        if (ffwd is _GLOBAL_FFWD and self._fast_estimate and
                'remaining_delta' not in self.__dict__):
            # Stesso calcolo di ``remaining(start, ffwd(...), now)`` senza
            # costruire l'oggetto ffwd: è il percorso caldo di beat.
            cached_tz = self.__dict__.get('tz')
            schedule_tz = (
                cached_tz if isinstance(cached_tz, tzinfo)
                else timezone.get_timezone(cached_tz or self.tz))
            current_tz = last_run_at.tzinfo
            if current_tz is None or current_tz.utcoffset(last_run_at) is None:
                last_run_at = self.maybe_make_aware(last_run_at)
            if last_run_at.tzinfo is not schedule_tz:
                last_run_at = last_run_at.astimezone(schedule_tz)
            now = self.now()
            current_tz = now.tzinfo
            if current_tz is None or current_tz.utcoffset(now) is None:
                now = self.maybe_make_aware(now)
            if now.tzinfo is not schedule_tz:
                now = now.astimezone(schedule_tz)
            args = self._rs_spec().next_ffwd(
                last_run_at.year, last_run_at.month, last_run_at.day,
                last_run_at.hour, last_run_at.minute)
            end = _add_ffwd(last_run_at, args)
            if schedule_tz is not _UTC:
                if end.tzinfo is not _UTC:
                    end = end.astimezone(_UTC)
                if now.tzinfo is not _UTC:
                    now = now.astimezone(_UTC)
            return end - now
        return remaining(*self.remaining_delta(last_run_at, ffwd=ffwd))

    def is_due(self, last_run_at: datetime) -> tuple[bool, datetime]:
        """Return tuple of ``(is_due, next_time_to_run)``.

        If :setting:`beat_cron_starting_deadline`  has been specified, the
        scheduler will make sure that the `last_run_at` time is within the
        deadline. This prevents tasks that could have been run according to
        the crontab, but didn't, from running again unexpectedly.

        Note:
            Next time to run is in seconds.

        SeeAlso:
            :meth:`celery.schedules.schedule.is_due` for more information.
        """

        rem_delta = self.remaining_estimate(last_run_at)
        rem_secs = rem_delta.total_seconds()
        rem = max(rem_secs, 0)
        due = rem == 0

        deadline_secs = self.app.conf.beat_cron_starting_deadline
        has_passed_deadline = False
        if deadline_secs is not None and rem_secs < 0:
            # If no feasible run date falls in [now - deadline_secs, now],
            # the most recent missed run is too stale to catch up.
            now = self.maybe_make_aware(self.now())
            # Subtract 1 microsecond so a run exactly at (now - deadline_secs) is counted within the deadline.
            deadline_since = (
                now.astimezone(timezone.utc) - timedelta(seconds=deadline_secs, microseconds=1)
            ).astimezone(now.tzinfo)
            if self.remaining_estimate(deadline_since).total_seconds() > 0:
                has_passed_deadline = True
                due = False

        if due or has_passed_deadline:
            rem_delta = self.remaining_estimate(self.now())
            rem = max(rem_delta.total_seconds(), 0)
        return schedstate(due, rem)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, crontab):
            return (
                other.month_of_year == self.month_of_year and
                other.day_of_month == self.day_of_month and
                other.day_of_week == self.day_of_week and
                other.hour == self.hour and
                other.minute == self.minute and
                super().__eq__(other)
            )
        return NotImplemented


def maybe_schedule(
        s: int | float | timedelta | BaseSchedule, relative: bool = False,
        app: Celery | None = None) -> float | timedelta | BaseSchedule:
    """Return schedule from number, timedelta, or actual schedule."""
    if s is not None:
        if isinstance(s, (float, int)):
            s = timedelta(seconds=s)
        if isinstance(s, timedelta):
            return schedule(s, relative, app=app)
        else:
            s.app = app
    return s


class solar(BaseSchedule):
    """Solar event.

    A solar event can be used as the ``run_every`` value of a
    periodic task entry to schedule based on certain solar events.

    Notes:

        Available event values are:

            - ``dawn_astronomical``
            - ``dawn_nautical``
            - ``dawn_civil``
            - ``sunrise``
            - ``solar_noon``
            - ``sunset``
            - ``dusk_civil``
            - ``dusk_nautical``
            - ``dusk_astronomical``

    Arguments:
        event (str): Solar event that triggers this task.
            See note for available values.
        lat (float): The latitude of the observer.
        lon (float): The longitude of the observer.
        nowfun (Callable): Function returning the current date and time
            as a class:`~datetime.datetime`.
        app (Celery): Celery app instance.
    """

    _all_events = {
        'dawn_astronomical',
        'dawn_nautical',
        'dawn_civil',
        'sunrise',
        'solar_noon',
        'sunset',
        'dusk_civil',
        'dusk_nautical',
        'dusk_astronomical',
    }
    _horizons = {
        'dawn_astronomical': '-18',
        'dawn_nautical': '-12',
        'dawn_civil': '-6',
        'sunrise': '-0:34',
        'solar_noon': '0',
        'sunset': '-0:34',
        'dusk_civil': '-6',
        'dusk_nautical': '-12',
        'dusk_astronomical': '-18',
    }
    _methods = {
        'dawn_astronomical': 'next_rising',
        'dawn_nautical': 'next_rising',
        'dawn_civil': 'next_rising',
        'sunrise': 'next_rising',
        'solar_noon': 'next_transit',
        'sunset': 'next_setting',
        'dusk_civil': 'next_setting',
        'dusk_nautical': 'next_setting',
        'dusk_astronomical': 'next_setting',
    }
    _use_center_l = {
        'dawn_astronomical': True,
        'dawn_nautical': True,
        'dawn_civil': True,
        'sunrise': False,
        'solar_noon': False,
        'sunset': False,
        'dusk_civil': True,
        'dusk_nautical': True,
        'dusk_astronomical': True,
    }

    #: How many days ahead to search for an event that doesn't happen
    #: every day (see :meth:`_next_event`).
    max_search_days = 366

    def __init__(self, event: str, lat: int | float, lon: int | float, **
                 kwargs: Any) -> None:
        try:
            self.ephem = __import__('ephem')
        except ImportError as exc:
            raise ImproperlyConfigured(SOLAR_EPHEM_NOT_INSTALLED) from exc
        self.event = event
        self.lat = lat
        self.lon = lon
        super().__init__(**kwargs)

        if event not in self._all_events:
            raise ValueError(SOLAR_INVALID_EVENT.format(
                event=event, all_events=', '.join(sorted(self._all_events)),
            ))
        if lat < -90 or lat > 90:
            raise ValueError(SOLAR_INVALID_LATITUDE.format(lat=lat))
        if lon < -180 or lon > 180:
            raise ValueError(SOLAR_INVALID_LONGITUDE.format(lon=lon))

        cal = self.ephem.Observer()
        cal.lat = str(lat)
        cal.lon = str(lon)
        cal.elev = 0
        cal.horizon = self._horizons[event]
        cal.pressure = 0
        self.cal = cal

        self.method = self._methods[event]
        self.use_center = self._use_center_l[event]

    def __reduce__(self) -> tuple[type, tuple[str, int | float, int | float]]:
        return self.__class__, (self.event, self.lat, self.lon)

    def __repr__(self) -> str:
        return '<solar: {} at latitude {}, longitude: {}>'.format(
            self.event, self.lat, self.lon,
        )

    def _next_event(self, start: Any) -> Any:
        """Return the first occurrence of the event after ``start``.

        At high latitudes some events don't happen every day, e.g.
        astronomical dawn in summer or sunrise during the polar night,
        and :mod:`ephem` raises :exc:`~ephem.CircumpolarError` for them.
        In that case the search continues from the next anti-transit
        (one day later), for at most :attr:`max_search_days` days.

        Returns:
            ephem.Date: the time of the event, or, if it doesn't happen
                within :attr:`max_search_days` days, the time at which
                the search stopped, so the schedule is checked again then.
        """
        sun = self.ephem.Sun()
        kwargs = {'use_center': True} if self.use_center else {}
        for _ in range(self.max_search_days):
            try:
                return getattr(self.cal, self.method)(
                    sun, start=start, **kwargs
                )
            except self.ephem.CircumpolarError:
                start = self.cal.next_antitransit(sun, start=start)
        return start

    def remaining_estimate(self, last_run_at: datetime) -> timedelta:
        """Return estimate of next time to run.

        Returns:
            ~datetime.timedelta: when the periodic task should
                run next, or if it shouldn't run today (e.g., the sun does
                not rise today), returns the time when the next check
                should take place.
        """
        last_run_at = self.maybe_make_aware(last_run_at)
        last_run_at_utc = localize(last_run_at, timezone.utc)
        self.cal.date = last_run_at_utc
        next_utc = self._next_event(self.ephem.Date(last_run_at_utc))
        next = self.maybe_make_aware(next_utc.datetime())
        now = self.maybe_make_aware(self.now())
        return subtract_datetimes(next, now)

    def is_due(self, last_run_at: datetime) -> tuple[bool, datetime]:
        """Return tuple of ``(is_due, next_time_to_run)``.

        Note:
            next time to run is in seconds.

        See Also:
            :meth:`celery.schedules.schedule.is_due` for more information.
        """
        rem_delta = self.remaining_estimate(last_run_at)
        rem = max(rem_delta.total_seconds(), 0)
        due = rem == 0
        if due:
            rem_delta = self.remaining_estimate(self.now())
            rem = max(rem_delta.total_seconds(), 0)
        return schedstate(due, rem)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, solar):
            return (
                other.event == self.event and
                other.lat == self.lat and
                other.lon == self.lon
            )
        return NotImplemented
