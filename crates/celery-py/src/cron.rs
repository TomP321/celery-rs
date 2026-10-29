//! Python bindings for crontab parsing and ffwd calendar calculations.

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyIndexError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use celery_beat::{CronSpec, Error, FfwdArgs, LastRun};

create_exception!(_celery_rs, CronParseError, PyException);

/// ffwd arguments: year, month, weeks, weekday, day, hour and minute.
type FfwdTuple = (
    Option<i32>,
    Option<u32>,
    u32,
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
);

fn to_py_err(error: Error) -> PyErr {
    let message = error.to_string();
    match error.kind() {
        celery_beat::ErrorKind::Parse => CronParseError::new_err(message),
        celery_beat::ErrorKind::Value => PyValueError::new_err(message),
        celery_beat::ErrorKind::Runtime => PyRuntimeError::new_err(message),
        celery_beat::ErrorKind::Index => PyIndexError::new_err(message),
    }
}

/// Parses a crontab field with the reference's results and errors.
#[pyfunction]
pub fn cron_parse(spec: &str, max_: i64, min_: i64) -> PyResult<Vec<i64>> {
    celery_beat::parse(spec, max_, min_).map_err(to_py_err)
}

/// Compact expanded crontab fields.
#[pyclass(module = "_celery_rs", name = "CronSpec")]
pub struct PyCronSpec {
    inner: CronSpec,
}

#[pymethods]
impl PyCronSpec {
    fn fields_equal(&self, other: &Self) -> bool {
        self.inner.minute() == other.inner.minute()
            && self.inner.hour() == other.inner.hour()
            && self.inner.day_of_week() == other.inner.day_of_week()
            && self.inner.day_of_month() == other.inner.day_of_month()
            && self.inner.month_of_year() == other.inner.month_of_year()
    }

    fn fields(&self) -> [Vec<u32>; 5] {
        [
            self.inner.minute().to_vec(),
            self.inner.hour().to_vec(),
            self.inner.day_of_week().to_vec(),
            self.inner.day_of_month().to_vec(),
            self.inner.month_of_year().to_vec(),
        ]
    }

    #[new]
    fn new(
        minute: Vec<i64>,
        hour: Vec<i64>,
        day_of_week: Vec<i64>,
        day_of_month: Vec<i64>,
        month_of_year: Vec<i64>,
        all_dom_moy: bool,
    ) -> PyResult<Self> {
        let inner = CronSpec::new(
            minute,
            hour,
            day_of_week,
            day_of_month,
            month_of_year,
            all_dom_moy,
        )
        .map_err(to_py_err)?;
        Ok(Self { inner })
    }

    fn next_ffwd(
        &self,
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
    ) -> PyResult<FfwdTuple> {
        let args = self
            .inner
            .next_ffwd(LastRun::new(year, month, day, hour, minute))
            .map_err(to_py_err)?;
        Ok(ffwd_tuple(args))
    }

    /// Returns the next calendar delta using crontab._delta_to_next semantics.
    fn delta_to_next(
        &self,
        year: i32,
        month: u32,
        day: u32,
        next_hour: u32,
        next_minute: u32,
    ) -> PyResult<FfwdTuple> {
        let args = self
            .inner
            .delta_to_next(
                LastRun {
                    year,
                    month,
                    day,
                    hour: next_hour,
                    minute: next_minute,
                    dow: 0,
                },
                next_hour,
                next_minute,
            )
            .map_err(to_py_err)?;
        Ok(ffwd_tuple(args))
    }
}

fn ffwd_tuple(args: FfwdArgs) -> FfwdTuple {
    (
        args.year,
        args.month,
        args.weeks,
        args.weekday,
        args.day,
        args.hour,
        args.minute,
    )
}
