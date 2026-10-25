//! Binding Python del core beat: parser crontab e calcolo del prossimo `ffwd`.

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyIndexError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use celery_beat::{CronSpec, Error, FfwdArgs, LastRun};

create_exception!(_celery_rs, CronParseError, PyException);

/// Argomenti di `ffwd`: `(year, month, weeks, weekday, day, hour, minute)`.
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

/// `crontab_parser(max_, min_).parse(spec)`: stessi risultati e stessi errori.
#[pyfunction]
pub fn cron_parse(spec: &str, max_: i64, min_: i64) -> PyResult<Vec<i64>> {
    celery_beat::parse(spec, max_, min_).map_err(to_py_err)
}

/// Campi di un crontab già espansi, compatti per il percorso caldo.
#[pyclass(module = "_celery_rs", name = "CronSpec")]
pub struct PyCronSpec {
    inner: CronSpec,
}

#[pymethods]
impl PyCronSpec {
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

    /// `crontab._delta_to_next(last_run_at, next_hour, next_minute)`.
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
