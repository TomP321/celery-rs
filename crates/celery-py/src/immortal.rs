//! Oggetti precaricati condivisi dal master e dai figli (PEP 683).
use pyo3::{ffi, prelude::*, types::PyList};
use std::os::raw::{c_int, c_void};

struct Visitor {
    initial: u32,
    depth: usize,
    count: usize,
    excluded: [*mut ffi::PyTypeObject; 10],
}

unsafe extern "C" fn visit(obj: *mut ffi::PyObject, arg: *mut c_void) -> c_int {
    // SAFETY: tp_traverse passa riferimenti vivi; il chiamante mantiene il GIL.
    // La visita non esegue Python né rimuove riferimenti. Il marcatore precede
    // la ricorsione e interrompe i cicli senza un insieme di visitati.
    unsafe {
        let state = arg.cast::<Visitor>();
        let counter = obj.cast::<u32>();
        if *counter >= (1 << 31) || (*state).depth >= 128 {
            return 0;
        }
        let ty = ffi::Py_TYPE(obj);
        if !ffi::PyType_GetSlot(ty, ffi::Py_tp_finalize).is_null()
            || !ffi::PyType_GetSlot(ty, ffi::Py_tp_del).is_null()
            || (*state)
                .excluded
                .iter()
                .any(|&base| ffi::PyType_IsSubtype(ty, base) != 0)
        {
            return 0;
        }
        // SAFETY: CPython 3.12/3.13 usa UINT32_MAX nei 32 bit bassi
        // di ob_refcnt; 3.14 usa 3<<30 (Include/refcount.h). Solo build
        // GIL, little-endian a 64 bit: questo campo è il primo u32.
        // Non tocchiamo i bit alti, riservati ai flag nelle versioni nuove.
        *counter = (*state).initial;
        (*state).count += 1;
        let slot = ffi::PyType_GetSlot(ty, ffi::Py_tp_traverse);
        if !slot.is_null() {
            let traverse: ffi::traverseproc = std::mem::transmute(slot);
            (*state).depth += 1;
            traverse(obj, visit, arg);
            (*state).depth -= 1;
        }
        0
    }
}

#[pyfunction]
pub fn _immortalize(objects: &Bound<'_, PyList>) -> PyResult<usize> {
    let py = objects.py();
    let sys = py.import("sys")?;
    let version: (u8, u8) = (
        sys.getattr("version_info")?.get_item(0)?.extract()?,
        sys.getattr("version_info")?.get_item(1)?.extract()?,
    );
    if std::env::var_os("CELERY_RS_IMMORTALIZE").is_some_and(|v| v == "0") {
        return Ok(0);
    }
    let config = py.import("sysconfig")?;
    let gil_disabled = config
        .call_method1("get_config_var", ("Py_GIL_DISABLED",))?
        .is_truthy()?;
    let trace_refs = config
        .call_method1("get_config_var", ("Py_TRACE_REFS",))?
        .is_truthy()?;
    let cpython = sys
        .getattr("implementation")?
        .getattr("name")?
        .extract::<&str>()?
        == "cpython";
    let Some(initial) = initial_refcount(version, cpython, gil_disabled, trace_refs) else {
        return Ok(0);
    };
    // Tipi reali: le riesportazioni Python possono essere sostituite dall'utente.
    let excluded = [
        py.get_type::<crate::cron::CronParseError>().as_ptr().cast(),
        py.get_type::<crate::cron::PyCronSpec>().as_ptr().cast(),
        py.get_type::<crate::transport::Connection>()
            .as_ptr()
            .cast(),
        py.get_type::<crate::worker::PyWaker>().as_ptr().cast(),
        py.get_type::<crate::worker::PyWorkerConsumer>()
            .as_ptr()
            .cast(),
        py.get_type::<crate::worker::PyPool>().as_ptr().cast(),
        py.get_type::<crate::worker::PyPoolEvent>().as_ptr().cast(),
        py.get_type::<crate::backend_redis::RedisClient>()
            .as_ptr()
            .cast(),
        py.get_type::<crate::backend_redis::RedisPipeline>()
            .as_ptr()
            .cast(),
        py.get_type::<crate::backend_redis::RedisPubSub>()
            .as_ptr()
            .cast(),
    ];
    let mut state = Visitor {
        initial,
        depth: 0,
        count: 0,
        excluded,
    };
    for obj in objects.iter() {
        // SAFETY: la lista e il GIL mantengono vivi tutti gli oggetti radice.
        unsafe {
            visit(obj.as_ptr(), (&mut state as *mut Visitor).cast());
        }
    }
    Ok(state.count)
}

fn initial_refcount(
    version: (u8, u8),
    cpython: bool,
    gil_disabled: bool,
    trace_refs: bool,
) -> Option<u32> {
    if !cfg!(all(target_pointer_width = "64", target_endian = "little"))
        || !cpython
        || gil_disabled
        || trace_refs
    {
        return None;
    }
    match version {
        (3, 12 | 13) => Some(u32::MAX),
        (3, 14) => Some(3 << 30),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::initial_refcount;

    #[test]
    fn supported_layouts() {
        if cfg!(all(target_pointer_width = "64", target_endian = "little")) {
            assert_eq!(
                initial_refcount((3, 12), true, false, false),
                Some(u32::MAX)
            );
            assert_eq!(
                initial_refcount((3, 13), true, false, false),
                Some(u32::MAX)
            );
            assert_eq!(initial_refcount((3, 14), true, false, false), Some(3 << 30));
        }
        for version in [(3, 10), (3, 11), (3, 15), (4, 0)] {
            assert_eq!(initial_refcount(version, true, false, false), None);
        }
        assert_eq!(initial_refcount((3, 13), false, false, false), None);
        assert_eq!(initial_refcount((3, 13), true, true, false), None);
        assert_eq!(initial_refcount((3, 13), true, false, true), None);
    }
}
