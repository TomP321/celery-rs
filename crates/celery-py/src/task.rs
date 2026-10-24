//! `encode_task_v2`: equivalente Rust di `AMQP.as_task_v2` + JSON + compressione.

use pyo3::exceptions::{PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use celery_proto::compression;

use crate::json::json_dumps;
use crate::saferepr::saferepr;

static MAPPING_TYPE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn mapping_type(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let v = MAPPING_TYPE.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("collections.abc")?.getattr("Mapping")?.unbind())
    })?;
    Ok(v.bind(py).clone())
}

fn is_list_or_tuple(obj: &Bound<'_, PyAny>) -> bool {
    // SAFETY: `obj` è vivo; i check di tipo non lo modificano.
    unsafe {
        pyo3::ffi::PyList_Check(obj.as_ptr()) != 0 || pyo3::ffi::PyTuple_Check(obj.as_ptr()) != 0
    }
}

fn empty_tuple(py: Python<'_>) -> Bound<'_, PyAny> {
    PyTuple::empty(py).into_any()
}

fn empty_dict(py: Python<'_>) -> Bound<'_, PyAny> {
    PyDict::new(py).into_any()
}

fn anon_nodename(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    py.import("celery.utils.nodenames")?
        .getattr("anon_nodename")?
        .call0()
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    *,
    task_id,
    name,
    args=None,
    kwargs=None,
    eta=None,
    expires=None,
    group_id=None,
    group_index=None,
    retries=0,
    chord=None,
    callbacks=None,
    errbacks=None,
    reply_to=None,
    time_limit=None,
    soft_time_limit=None,
    create_sent_event=false,
    root_id=None,
    parent_id=None,
    shadow=None,
    chain=None,
    origin=None,
    ignore_result=false,
    argsrepr=None,
    kwargsrepr=None,
    stamped_headers=None,
    stamps=None,
    replaced_task_nesting=0,
    argsrepr_maxsize=1024,
    kwargsrepr_maxsize=1024,
    repr_maxlevels=3,
    compression=None,
    default=None,
))]
pub fn encode_task_v2(
    py: Python<'_>,
    task_id: Bound<'_, PyAny>,
    name: Bound<'_, PyAny>,
    args: Option<Bound<'_, PyAny>>,
    kwargs: Option<Bound<'_, PyAny>>,
    eta: Option<Bound<'_, PyAny>>,
    expires: Option<Bound<'_, PyAny>>,
    group_id: Option<Bound<'_, PyAny>>,
    group_index: Option<Bound<'_, PyAny>>,
    retries: i64,
    chord: Option<Bound<'_, PyAny>>,
    callbacks: Option<Bound<'_, PyAny>>,
    errbacks: Option<Bound<'_, PyAny>>,
    reply_to: Option<Bound<'_, PyAny>>,
    time_limit: Option<Bound<'_, PyAny>>,
    soft_time_limit: Option<Bound<'_, PyAny>>,
    create_sent_event: bool,
    root_id: Option<Bound<'_, PyAny>>,
    parent_id: Option<Bound<'_, PyAny>>,
    shadow: Option<Bound<'_, PyAny>>,
    chain: Option<Bound<'_, PyAny>>,
    origin: Option<Bound<'_, PyAny>>,
    ignore_result: bool,
    argsrepr: Option<Bound<'_, PyAny>>,
    kwargsrepr: Option<Bound<'_, PyAny>>,
    stamped_headers: Option<Bound<'_, PyAny>>,
    stamps: Option<Bound<'_, PyAny>>,
    replaced_task_nesting: i64,
    argsrepr_maxsize: i64,
    kwargsrepr_maxsize: i64,
    repr_maxlevels: Option<i64>,
    compression: Option<String>,
    default: Option<Bound<'_, PyAny>>,
) -> PyResult<Py<PyTuple>> {
    // `args = args or ()`, `kwargs = kwargs or {}`.
    let args_value = match args {
        Some(value) if value.is_truthy()? => {
            if !is_list_or_tuple(&value) {
                return Err(PyTypeError::new_err("task args must be a list or tuple"));
            }
            value
        }
        _ => empty_tuple(py),
    };
    let kwargs_value = match kwargs {
        Some(value) if value.is_truthy()? => {
            if !value.is_instance(&mapping_type(py)?)? {
                return Err(PyTypeError::new_err(
                    "task keyword arguments must be a mapping",
                ));
            }
            value
        }
        _ => empty_dict(py),
    };

    let argsrepr_value = match argsrepr {
        Some(v) => v,
        None => saferepr(py, &args_value, Some(argsrepr_maxsize), repr_maxlevels)?
            .into_bound(py)
            .into_any(),
    };
    let kwargsrepr_value = match kwargsrepr {
        Some(v) => v,
        None => saferepr(py, &kwargs_value, Some(kwargsrepr_maxsize), repr_maxlevels)?
            .into_bound(py)
            .into_any(),
    };

    let root_id_value = match root_id {
        Some(value) if value.is_truthy()? => value,
        _ => task_id.clone(),
    };

    let origin_value = match origin {
        Some(value) if value.is_truthy()? => value,
        _ => anon_nodename(py)?,
    };

    let stamps_value = build_stamps(py, stamped_headers.as_ref(), stamps.as_ref())?;

    let timelimit = PyList::empty(py);
    timelimit.append(opt_value(py, time_limit.as_ref()))?;
    timelimit.append(opt_value(py, soft_time_limit.as_ref()))?;

    let headers = PyDict::new(py);
    headers.set_item("lang", "py")?;
    headers.set_item("task", &name)?;
    headers.set_item("id", &task_id)?;
    headers.set_item("shadow", opt_value(py, shadow.as_ref()))?;
    headers.set_item("eta", opt_value(py, eta.as_ref()))?;
    headers.set_item("expires", opt_value(py, expires.as_ref()))?;
    headers.set_item("group", opt_value(py, group_id.as_ref()))?;
    headers.set_item("group_index", opt_value(py, group_index.as_ref()))?;
    headers.set_item("retries", retries)?;
    headers.set_item("timelimit", timelimit)?;
    headers.set_item("root_id", &root_id_value)?;
    headers.set_item("parent_id", opt_value(py, parent_id.as_ref()))?;
    headers.set_item("argsrepr", &argsrepr_value)?;
    headers.set_item("kwargsrepr", &kwargsrepr_value)?;
    headers.set_item("origin", &origin_value)?;
    headers.set_item("ignore_result", ignore_result)?;
    headers.set_item("replaced_task_nesting", replaced_task_nesting)?;
    headers.set_item("stamped_headers", opt_value(py, stamped_headers.as_ref()))?;
    headers.set_item("stamps", stamps_value)?;

    let properties = PyDict::new(py);
    properties.set_item("correlation_id", &task_id)?;
    let reply_to_value = match reply_to {
        Some(value) if value.is_truthy()? => value,
        _ => PyString::new(py, "").into_any(),
    };
    properties.set_item("reply_to", reply_to_value)?;

    let embed = PyDict::new(py);
    embed.set_item("callbacks", opt_value(py, callbacks.as_ref()))?;
    embed.set_item("errbacks", opt_value(py, errbacks.as_ref()))?;
    embed.set_item("chain", opt_value(py, chain.as_ref()))?;
    embed.set_item("chord", opt_value(py, chord.as_ref()))?;

    let body = PyTuple::new(py, [args_value, kwargs_value, embed.into_any()])?;

    let default_value = match default {
        Some(v) => v,
        None => py
            .import("kombu.utils.json")?
            .getattr("JSONEncoder")?
            .call0()?
            .getattr("default")?,
    };
    let mut body_bytes: Bound<'_, PyBytes> =
        json_dumps(py, body.as_any(), &default_value)?.into_bound(py);
    let mut content_type = String::from("application/json");
    let content_encoding = String::from("utf-8");

    if let Some(name) = compression
        && !name.is_empty()
    {
        let (compressed, ct) = {
            let data = body_bytes.as_bytes();
            let addr = data.as_ptr() as usize;
            let len = data.len();
            // SAFETY: `body_bytes` resta vivo e i bytes sono immutabili; il GIL
            // viene rilasciato ma nessun codice Python può mutare l'oggetto.
            let out = py.detach(move || unsafe {
                compression::compress(
                    name.as_str(),
                    std::slice::from_raw_parts(addr as *const u8, len),
                )
            });
            out.map_err(compression_error)?
        };
        headers.set_item("compression", ct)?;
        content_type = ct.to_string();
        body_bytes = PyBytes::new(py, &compressed);
    }

    let sent_event = if create_sent_event {
        let event = PyDict::new(py);
        event.set_item("uuid", &task_id)?;
        event.set_item("root_id", &root_id_value)?;
        event.set_item("parent_id", opt_value(py, parent_id.as_ref()))?;
        event.set_item("name", &name)?;
        event.set_item("args", &argsrepr_value)?;
        event.set_item("kwargs", &kwargsrepr_value)?;
        event.set_item("retries", retries)?;
        event.set_item("eta", opt_value(py, eta.as_ref()))?;
        event.set_item("expires", opt_value(py, expires.as_ref()))?;
        event.into_any()
    } else {
        py.None().into_bound(py)
    };

    Ok(PyTuple::new(
        py,
        [
            headers.into_any(),
            properties.into_any(),
            body_bytes.into_any(),
            PyString::new(py, &content_type).into_any(),
            PyString::new(py, &content_encoding).into_any(),
            sent_event,
        ],
    )?
    .unbind())
}

fn build_stamps<'py>(
    py: Python<'py>,
    stamped_headers: Option<&Bound<'py, PyAny>>,
    stamps: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let out = PyDict::new(py);
    if let Some(headers) = stamped_headers
        && let Some(stamps) = stamps
    {
        let stamps = stamps.cast::<PyDict>()?;
        for header in headers.try_iter()? {
            let header = header?;
            let value = stamps
                .get_item(&header)?
                .ok_or_else(|| PyKeyError::new_err(header.clone().unbind()))?;
            out.set_item(header, value)?;
        }
    }
    Ok(out.into_any())
}

fn opt_value<'py>(py: Python<'py>, value: Option<&Bound<'py, PyAny>>) -> Bound<'py, PyAny> {
    value.cloned().unwrap_or_else(|| py.None().into_bound(py))
}

fn compression_error(err: compression::CompressionError) -> PyErr {
    match err {
        compression::CompressionError::UnknownMethod(name) => PyKeyError::new_err(name),
        compression::CompressionError::Codec(message) => {
            pyo3::exceptions::PyValueError::new_err(message)
        }
    }
}
