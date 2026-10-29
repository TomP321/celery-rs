//! Crontab calendar calculations returning Celery ffwd arguments. Python handles
//! time zones and DST; input fields are already local to the schedule's time zone.

use crate::error::Error;
/// Reference ffwd fields; None leaves a field unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FfwdArgs {
    pub year: Option<i32>,
    pub month: Option<u32>,
    pub weeks: u32,
    pub weekday: Option<u32>,
    pub day: Option<u32>,
    pub hour: Option<u32>,
    pub minute: Option<u32>,
}

impl FfwdArgs {
    const fn minute(minute: u32) -> Self {
        Self {
            year: None,
            month: None,
            weeks: 0,
            weekday: None,
            day: None,
            hour: None,
            minute: Some(minute),
        }
    }

    const fn hour_minute(hour: u32, minute: u32) -> Self {
        Self {
            hour: Some(hour),
            ..Self::minute(minute)
        }
    }

    const fn weekday(weeks: u32, weekday: u32, hour: u32, minute: u32) -> Self {
        Self {
            weeks,
            weekday: Some(weekday),
            hour: Some(hour),
            ..Self::minute(minute)
        }
    }

    const fn date(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> Self {
        Self {
            year: Some(year),
            month: Some(month),
            day: Some(day),
            hour: Some(hour),
            ..Self::minute(minute)
        }
    }
}

/// Local last_run_at fields used for calendar calculations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastRun {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    /// Weekday with Sunday as zero, matching the reference.
    pub dow: u32,
}

impl LastRun {
    /// Returns the weekday with Sunday as zero, matching datetime.isoweekday() % 7.
    pub fn new(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> Self {
        Self {
            year,
            month,
            day,
            hour,
            minute,
            dow: isoweekday(year, month, day) % 7,
        }
    }
}

/// Compact, sorted expanded crontab fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSpec {
    minute: Vec<u32>,
    hour: Vec<u32>,
    day_of_week: Vec<u32>,
    day_of_month: Vec<u32>,
    month_of_year: Vec<u32>,
    all_dom_moy: bool,
}

impl CronSpec {
    /// Whether both original day-of-month and month-of-year fields are wildcards.
    pub fn new(
        minute: Vec<i64>,
        hour: Vec<i64>,
        day_of_week: Vec<i64>,
        day_of_month: Vec<i64>,
        month_of_year: Vec<i64>,
        all_dom_moy: bool,
    ) -> Result<Self, Error> {
        Ok(Self {
            minute: sorted_u32(&minute)?,
            hour: sorted_u32(&hour)?,
            day_of_week: sorted_u32(&day_of_week)?,
            day_of_month: sorted_u32(&day_of_month)?,
            month_of_year: sorted_u32(&month_of_year)?,
            all_dom_moy,
        })
    }

    pub fn minute(&self) -> &[u32] {
        &self.minute
    }

    pub fn hour(&self) -> &[u32] {
        &self.hour
    }

    pub fn day_of_week(&self) -> &[u32] {
        &self.day_of_week
    }

    pub fn day_of_month(&self) -> &[u32] {
        &self.day_of_month
    }

    pub fn month_of_year(&self) -> &[u32] {
        &self.month_of_year
    }

    /// Returns the next ffwd from local datetime fields.
    pub fn next_ffwd(&self, last: LastRun) -> Result<FfwdArgs, Error> {
        let execute_this_date = self.month_of_year.contains(&last.month)
            && self.day_of_month.contains(&last.day)
            && self.day_of_week.contains(&last.dow);

        let execute_this_hour = execute_this_date && self.hour.contains(&last.hour);
        if execute_this_hour {
            // The reference evaluates max(minute) only in this branch.
            let minute_max = self.minute.last().copied().ok_or(Error::EmptyMax)?;
            if last.minute < minute_max {
                let next_minute = self
                    .minute
                    .iter()
                    .copied()
                    .find(|m| *m > last.minute)
                    .ok_or(Error::EmptyMin)?;
                return Ok(FfwdArgs::minute(next_minute));
            }
        }
        let next_minute = *self.minute.first().ok_or(Error::EmptyMin)?;

        let execute_today = if execute_this_date {
            last.hour < self.hour_max()?
        } else {
            false
        };
        if execute_today {
            let next_hour = self
                .hour
                .iter()
                .copied()
                .find(|h| *h > last.hour)
                .ok_or(Error::EmptyMin)?;
            return Ok(FfwdArgs::hour_minute(next_hour, next_minute));
        }
        let next_hour = *self.hour.first().ok_or(Error::EmptyMin)?;

        if self.all_dom_moy {
            let next_day = self
                .day_of_week
                .iter()
                .copied()
                .find(|d| *d > last.dow)
                .or_else(|| self.day_of_week.first().copied())
                .ok_or(Error::EmptyMin)?;
            let add_week = u32::from(next_day == last.dow);
            return Ok(FfwdArgs::weekday(
                add_week,
                (next_day + 6) % 7,
                next_hour,
                next_minute,
            ));
        }
        self.delta_to_next(last, next_hour, next_minute)
    }

    fn hour_max(&self) -> Result<u32, Error> {
        self.hour.last().copied().ok_or(Error::EmptyMax)
    }

    /// Finds the next valid date at the given time, matching crontab._delta_to_next.
    pub fn delta_to_next(
        &self,
        last: LastRun,
        next_hour: u32,
        next_minute: u32,
    ) -> Result<FfwdArgs, Error> {
        if self.month_of_year.is_empty() || self.day_of_month.is_empty() {
            // Runtime mutation can create empty sets; the reference raises IndexError.
            return Err(Error::Index);
        }
        let mut roller = Roller {
            year: last.year,
            dom: 0,
            moy: 0,
            last,
            days: &self.day_of_month,
            months: &self.month_of_year,
        };
        if self.month_of_year.contains(&last.month) {
            roller.dom = bisect_right(&self.day_of_month, last.day);
            roller.moy = bisect_left(&self.month_of_year, last.month);
        } else {
            roller.dom = 0;
            roller.moy = bisect_right(&self.month_of_year, last.month);
            if roller.moy == self.month_of_year.len() {
                // The reference leaves year advancement to roll_over.
                roller.moy = 0;
            }
        }
        roller.roll_over()?;

        loop {
            let year = roller.year;
            let month = self.month_of_year[roller.moy];
            let day = self.day_of_month[roller.dom];
            if self
                .day_of_week
                .contains(&(isoweekday(year, month, day) % 7))
            {
                return Ok(FfwdArgs::date(year, month, day, next_hour, next_minute));
            }
            roller.dom += 1;
            roller.roll_over()?;
        }
    }
}

struct Roller<'a> {
    year: i32,
    dom: usize,
    moy: usize,
    last: LastRun,
    days: &'a [u32],
    months: &'a [u32],
}

impl Roller<'_> {
    fn roll_over(&mut self) -> Result<(), Error> {
        for _ in 0..2000 {
            let month = self.months[self.moy];
            let day = self.days.get(self.dom).copied();
            let flag = day.is_none()
                || day_out_of_range(self.year, month, day.unwrap_or_default())
                || self.is_before_last_run(month, day.unwrap_or_default());
            if flag {
                self.dom = 0;
                self.moy += 1;
                if self.moy == self.months.len() {
                    self.moy = 0;
                    self.year += 1;
                }
            } else {
                return Ok(());
            }
        }
        // Match the reference's cycle limit and exception after 2000 steps.
        Err(Error::Rollover)
    }

    // Local dates suffice: bisect_right never selects last_run_at's day, and
    // the reference compares aware datetimes in the same time zone.
    fn is_before_last_run(&self, month: u32, day: u32) -> bool {
        (self.year, month, day) < (self.last.year, self.last.month, self.last.day)
    }
}

fn sorted_u32(values: &[i64]) -> Result<Vec<u32>, Error> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(u32::try_from(*value).map_err(|_| Error::BeginRange {
            value: value.to_string(),
            min: 0,
        })?);
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn bisect_right(values: &[u32], value: u32) -> usize {
    values.partition_point(|v| *v <= value)
}

fn bisect_left(values: &[u32], value: u32) -> usize {
    values.partition_point(|v| *v < value)
}

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn day_out_of_range(year: i32, month: u32, day: u32) -> bool {
    !(1..=9999).contains(&year)
        || day == 0
        || day > days_in_month(year, month)
        || !(1..=12).contains(&month)
}

fn isoweekday(year: i32, month: u32, day: u32) -> u32 {
    let days = days_from_civil(year as i64, month, day);
    (((days + 3).rem_euclid(7)) + 1) as u32
}

// Uses Howard Hinnant's algorithm for days since 1970-01-01.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full(dow: Vec<i64>) -> CronSpec {
        CronSpec::new(
            (0..60).collect(),
            (0..24).collect(),
            dow,
            (1..=31).collect(),
            (1..=12).collect(),
            true,
        )
        .unwrap()
    }

    fn last(year: i32, month: u32, day: u32, hour: u32, minute: u32, dow: u32) -> LastRun {
        LastRun {
            year,
            month,
            day,
            hour,
            minute,
            dow,
        }
    }

    #[test]
    fn isoweekday_matches_python() {
        assert_eq!(isoweekday(2026, 10, 25) % 7, 0);
        assert_eq!(isoweekday(2024, 2, 29), 4);
        assert_eq!(isoweekday(1970, 1, 1), 4);
        assert_eq!(isoweekday(2000, 2, 29), 2);
        assert_eq!(isoweekday(1900, 3, 1), 4);
    }

    #[test]
    fn every_minute_next_minute() {
        let s = full((0..7).collect());
        assert_eq!(
            s.next_ffwd(last(2026, 10, 25, 10, 30, 0)).unwrap(),
            FfwdArgs::minute(31)
        );
    }

    #[test]
    fn last_minute_rolls_to_next_hour() {
        let s = full((0..7).collect());
        assert_eq!(
            s.next_ffwd(last(2026, 10, 25, 10, 59, 0)).unwrap(),
            FfwdArgs::hour_minute(11, 0)
        );
    }

    #[test]
    fn hour_rolls_to_next_day() {
        let s = CronSpec::new(
            vec![0],
            vec![2],
            (0..7).collect(),
            (1..=31).collect(),
            (1..=12).collect(),
            true,
        )
        .unwrap();

        let args = s.next_ffwd(last(2026, 10, 25, 3, 0, 0)).unwrap();
        assert_eq!(args.weeks, 0);
        assert_eq!(args.weekday, Some(0));
        assert_eq!(args.hour, Some(2));
        assert_eq!(args.minute, Some(0));
    }

    #[test]
    fn weekday_rollover_adds_week() {
        let s = CronSpec::new(
            vec![0],
            vec![0],
            vec![1],
            (1..=31).collect(),
            (1..=12).collect(),
            true,
        )
        .unwrap();

        let args = s.next_ffwd(last(2026, 10, 26, 0, 0, 1)).unwrap();
        assert_eq!(args.weeks, 1);
        assert_eq!(args.weekday, Some(0));
    }

    #[test]
    fn leap_day() {
        let s =
            CronSpec::new(vec![0], vec![0], (0..7).collect(), vec![29], vec![2], false).unwrap();
        assert_eq!(
            s.next_ffwd(last(2023, 3, 1, 0, 0, 0)).unwrap(),
            FfwdArgs::date(2024, 2, 29, 0, 0)
        );

        assert_eq!(
            s.next_ffwd(last(2024, 3, 1, 0, 0, 0)).unwrap(),
            FfwdArgs::date(2028, 2, 29, 0, 0)
        );
        // Century years are leap years only when divisible by 400.
        assert_eq!(
            s.next_ffwd(last(2096, 3, 1, 0, 0, 0)).unwrap(),
            FfwdArgs::date(2104, 2, 29, 0, 0)
        );
    }

    #[test]
    fn months_with_31_days() {
        let s = CronSpec::new(
            vec![0],
            vec![12],
            (0..7).collect(),
            vec![31],
            vec![1, 3, 5, 7, 8, 10, 12],
            false,
        )
        .unwrap();
        assert_eq!(
            s.next_ffwd(last(2026, 10, 31, 12, 0, 6)).unwrap(),
            FfwdArgs::date(2026, 12, 31, 12, 0)
        );
        assert_eq!(
            s.next_ffwd(last(2026, 11, 1, 0, 0, 0)).unwrap(),
            FfwdArgs::date(2026, 12, 31, 12, 0)
        );
        assert_eq!(
            s.next_ffwd(last(2027, 1, 1, 0, 0, 0)).unwrap(),
            FfwdArgs::date(2027, 1, 31, 12, 0)
        );
    }

    #[test]
    fn day_of_week_with_day_of_month() {
        let s = CronSpec::new(
            vec![0],
            vec![0],
            vec![1],
            (1..=7).collect(),
            (1..=12).collect(),
            false,
        )
        .unwrap();
        assert_eq!(
            s.next_ffwd(last(2026, 10, 26, 0, 0, 1)).unwrap(),
            FfwdArgs::date(2026, 11, 2, 0, 0)
        );
    }

    #[test]
    fn invalid_month_set_rolls_over_forever() {
        // February 30 never yields a valid day within 2000 attempts.
        let s =
            CronSpec::new(vec![0], vec![0], (0..7).collect(), vec![30], vec![2], false).unwrap();
        assert_eq!(s.next_ffwd(last(2026, 1, 1, 0, 0, 4)), Err(Error::Rollover));
    }

    #[test]
    fn wrap_to_next_year() {
        let s = CronSpec::new(
            vec![0],
            vec![0],
            (0..7).collect(),
            (1..=31).collect(),
            vec![1],
            false,
        )
        .unwrap();
        assert_eq!(
            s.next_ffwd(last(2026, 12, 15, 0, 0, 2)).unwrap(),
            FfwdArgs::date(2027, 1, 1, 0, 0)
        );
    }

    #[test]
    fn empty_day_sets_raise_index_error() {
        // Runtime mutation can create states rejected by the crontab constructor.
        let s = CronSpec::new(vec![0], vec![0], (0..7).collect(), vec![], vec![2], false).unwrap();
        assert_eq!(s.next_ffwd(last(2026, 1, 1, 0, 0, 4)), Err(Error::Index));
    }

    #[test]
    fn empty_sets_error_like_python() {
        let base = |minute: Vec<i64>, hour: Vec<i64>, dow: Vec<i64>| {
            CronSpec::new(
                minute,
                hour,
                dow,
                (1..=31).collect(),
                (1..=12).collect(),
                true,
            )
            .unwrap()
        };
        assert_eq!(
            base(vec![], vec![0], (0..7).collect()).next_ffwd(last(2026, 1, 1, 0, 0, 4)),
            Err(Error::EmptyMax)
        );
        assert_eq!(
            base(vec![0], vec![], (0..7).collect()).next_ffwd(last(2026, 1, 1, 0, 0, 4)),
            Err(Error::EmptyMax)
        );
        assert_eq!(
            base(vec![0], vec![0], vec![]).next_ffwd(last(2026, 1, 1, 0, 0, 4)),
            Err(Error::EmptyMin)
        );
    }
}
