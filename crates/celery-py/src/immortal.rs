//! Preloaded objects shared between master and children using PEP 683 immortality.
use pyo3::{ffi, prelude::*, types::PyList};
use std::os::raw::{c_int, c_void};

struct Visitor {
    initial: u32,
    depth: usize,
    count: usize,
    excluded: [*mut ffi::PyTypeObject; 10],
}

unsafe extern "C" fn visit(obj: *mut ffi::PyObject, arg: *mut c_void) -> c_int {
    // SAFETY: tp_traverse supplies live references under the GIL. Traversal runs no
    // Python code and removes no references. Marking before recursion breaks cycles.
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
        // SAFETY: GIL-enabled, little-endian 64-bit CPython stores the immortal marker
        // in the first u32: UINT32_MAX in 3.12/3.13, 3<<30 in 3.14 (Include/refcount.h).
        // The upper flag bits remain unchanged.
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
    // Use actual types because users can replace Python re-exports.
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
        // SAFETY: the list and GIL keep all root objects alive.
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
