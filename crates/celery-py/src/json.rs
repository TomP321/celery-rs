//! JSON dumps and loads compatible with kombu.utils.json.

use std::collections::HashSet;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyIterator, PyList, PyMemoryView, PyString, PyTuple,
};

use celery_proto::json::read::{Event, JsonStr, Reader};
use celery_proto::json::write::{write_float, write_int, write_str, write_str_code_points};

use crate::util::{PyStrData, decode_utf8_strict, py_str_data, py_str_from_code_points};

/// CPython 3.13's non-debug POSIX encoder allows 10000 C recursion slots; kombu
/// consumes three before user data. A fixed limit matches this across threads and stack sizes.
const MAX_DEPTH: usize = 9_997;

static BUILTIN_INT: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn cached_type<'py>(
    cell: &'static PyOnceLock<Py<PyAny>>,
    py: Python<'py>,
    module: &str,
    name: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let value = cell.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import(module)?.getattr(name)?.unbind())
    })?;
    Ok(value.bind(py).clone())
}

fn builtin_int(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    cached_type(&BUILTIN_INT, py, "builtins", "int")
}

struct Work<'py> {
    obj: Bound<'py, PyAny>,
    depth: usize,
}

enum FrameKind<'py> {
    Array {
        iter: Bound<'py, PyIterator>,
    },
    DictExact {
        dict: Bound<'py, PyDict>,
        pos: ffi::Py_ssize_t,
    },
    DictItems {
        iter: Bound<'py, PyIterator>,
    },
}

struct Frame<'py> {
    kind: FrameKind<'py>,
    id: usize,
    depth: usize,
    first: bool,
}

struct DumpEngine<'py> {
    py: Python<'py>,
    default: Bound<'py, PyAny>,
    out: Vec<u8>,
    markers: HashSet<usize>,
    stack: Vec<Frame<'py>>,
    pending: Option<Work<'py>>,
}

impl<'py> DumpEngine<'py> {
    fn run(mut self) -> PyResult<Vec<u8>> {
        loop {
            if let Some(work) = self.pending.take() {
                self.emit(work)?;
                continue;
            }
            if self.stack.is_empty() {
                break;
            }
            self.step_frame()?;
        }
        Ok(self.out)
    }

    fn emit(&mut self, work: Work<'py>) -> PyResult<()> {
        if work.depth > MAX_DEPTH {
            return Err(pyo3::exceptions::PyRecursionError::new_err(
                "maximum recursion depth exceeded while encoding a JSON object",
            ));
        }
        let obj = &work.obj;
        if obj.is_none() {
            self.out.extend_from_slice(b"null");
            return Ok(());
        }
        let ptr = obj.as_ptr();
        // SAFETY: ptr references a live Python object; type checks read only its header.
        unsafe {
            if ffi::PyBool_Check(ptr) != 0 {
                if obj.is_truthy()? {
                    self.out.extend_from_slice(b"true");
                } else {
                    self.out.extend_from_slice(b"false");
                }
                return Ok(());
            }
            if ffi::PyLong_Check(ptr) != 0 {
                write_py_int(self.py, obj, &mut self.out)?;
                return Ok(());
            }
            if ffi::PyFloat_Check(ptr) != 0 {
                let value: f64 = obj.extract()?;
                write_float(&mut self.out, value);
                return Ok(());
            }
            if ffi::PyUnicode_Check(ptr) != 0 {
                write_py_string(obj, &mut self.out)?;
                return Ok(());
            }
            if ffi::PyList_Check(ptr) != 0 || ffi::PyTuple_Check(ptr) != 0 {
                let id = ptr as usize;
                if !self.markers.insert(id) {
                    return Err(PyValueError::new_err("Circular reference detected"));
                }
                self.out.push(b'[');
                let iter = obj.try_iter()?;
                self.stack.push(Frame {
                    kind: FrameKind::Array { iter },
                    id,
                    depth: work.depth,
                    first: true,
                });
                return Ok(());
            }
            if ffi::PyDict_Check(ptr) != 0 {
                let id = ptr as usize;
                if !self.markers.insert(id) {
                    return Err(PyValueError::new_err("Circular reference detected"));
                }
                self.out.push(b'{');
                // CPython uses items() for dictionary subclasses.
                if ffi::PyDict_CheckExact(ptr) != 0 {
                    let dict = obj.cast::<PyDict>()?.clone();
                    self.stack.push(Frame {
                        kind: FrameKind::DictExact { dict, pos: 0 },
                        id,
                        depth: work.depth,
                        first: true,
                    });
                } else {
                    let items = obj.call_method0("items")?;
                    self.stack.push(Frame {
                        kind: FrameKind::DictItems {
                            iter: items.try_iter()?,
                        },
                        id,
                        depth: work.depth,
                        first: true,
                    });
                }
                return Ok(());
            }
        }

        let value = self.default.call1((obj,))?;
        self.pending = Some(Work {
            obj: value,
            depth: work.depth + 1,
        });
        Ok(())
    }

    fn step_frame(&mut self) -> PyResult<()> {
        let idx = self.stack.len() - 1;
        if matches!(self.stack[idx].kind, FrameKind::Array { .. }) {
            let next: Option<PyResult<Bound<'py, PyAny>>> = match &mut self.stack[idx].kind {
                FrameKind::Array { iter } => iter.next(),
                _ => None,
            };
            match next {
                Some(item) => {
                    let item = item?;
                    self.separator(idx);
                    self.stack[idx].first = false;
                    let depth = self.stack[idx].depth + 1;
                    self.pending = Some(Work { obj: item, depth });
                }
                None => self.close_frame(idx, b']'),
            }
            return Ok(());
        }

        if matches!(self.stack[idx].kind, FrameKind::DictItems { .. }) {
            let next: Option<PyResult<Bound<'py, PyAny>>> = match &mut self.stack[idx].kind {
                FrameKind::DictItems { iter } => iter.next(),
                _ => None,
            };
            match next {
                Some(item) => {
                    let item = item?;
                    let pair = item.cast::<PyTuple>()?;
                    let key = pair.get_item(0)?;
                    let value = pair.get_item(1)?;
                    self.emit_pair(idx, &key, value)?;
                }
                None => self.close_frame(idx, b'}'),
            }
            return Ok(());
        }

        let (has, key_ptr, value_ptr) = match &mut self.stack[idx].kind {
            FrameKind::DictExact { dict, pos } => {
                let mut key: *mut ffi::PyObject = std::ptr::null_mut();
                let mut value: *mut ffi::PyObject = std::ptr::null_mut();
                // SAFETY: dict is alive and pos holds the iteration state.
                let has = unsafe { ffi::PyDict_Next(dict.as_ptr(), pos, &mut key, &mut value) };
                (has, key, value)
            }
            _ => (0, std::ptr::null_mut(), std::ptr::null_mut()),
        };
        if has == 0 {
            self.close_frame(idx, b'}');
            return Ok(());
        }
        // SAFETY: borrowed pointers remain valid while the frame owns the live dictionary.
        let key = unsafe { Bound::from_borrowed_ptr(self.py, key_ptr) };
        let value = unsafe { Bound::from_borrowed_ptr(self.py, value_ptr) };
        self.emit_pair(idx, &key, value)
    }

    fn separator(&mut self, idx: usize) {
        if !self.stack[idx].first {
            self.out.extend_from_slice(b", ");
        }
    }

    fn emit_pair(
        &mut self,
        idx: usize,
        key: &Bound<'py, PyAny>,
        value: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        self.separator(idx);
        self.stack[idx].first = false;
        write_dict_key(self.py, key, &mut self.out)?;
        self.out.extend_from_slice(b": ");
        let depth = self.stack[idx].depth + 1;
        self.pending = Some(Work { obj: value, depth });
        Ok(())
    }

    fn close_frame(&mut self, idx: usize, close: u8) {
        self.out.push(close);
        let id = self.stack[idx].id;
        self.markers.remove(&id);
        self.stack.pop();
    }
}

#[pyfunction]
#[pyo3(signature = (obj, default))]
pub fn json_dumps(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    default: &Bound<'_, PyAny>,
) -> PyResult<Py<PyBytes>> {
    let engine = DumpEngine {
        py,
        default: default.clone(),
        out: Vec::with_capacity(256),
        markers: HashSet::new(),
        stack: Vec::new(),
        pending: Some(Work {
            obj: obj.clone(),
            depth: 0,
        }),
    };
    let out = engine.run()?;
    Ok(PyBytes::new(py, &out).unbind())
}

fn write_py_int(py: Python<'_>, obj: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    let mut overflow: i32 = 0;
    // SAFETY: obj is a live PyLong or subclass and overflow points to a valid integer.
    let v = unsafe { ffi::PyLong_AsLongLongAndOverflow(obj.as_ptr(), &mut overflow) };
    if v == -1
        && let Some(err) = PyErr::take(py)
    {
        return Err(err);
    }
    if overflow == 0 {
        write_int(out, v as i64);
        return Ok(());
    }
    // int.__repr__ bypasses subclass overrides for arbitrary integers.
    let s = builtin_int(py)?.call_method1("__repr__", (obj,))?;
    out.extend_from_slice(s.cast::<PyString>()?.to_str()?.as_bytes());
    Ok(())
}

fn write_py_string(obj: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    let s = obj.cast::<PyString>()?;
    match py_str_data(s)? {
        PyStrData::Utf8(v) => write_str(out, v),
        PyStrData::Code(cps) => write_str_code_points(out, cps),
    }
    Ok(())
}

fn type_name(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(obj.get_type().name()?.to_string_lossy().into_owned())
}

/// Returns CPython's type name, including modules for static types except builtins.
fn dict_key_type_name(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    let ty = obj.get_type();
    // SAFETY: ty is a live Python type; PyType_GetFlags does not modify it.
    let flags = unsafe { ffi::PyType_GetFlags(ty.as_ptr().cast()) };
    let name = ty.name()?.to_string_lossy().into_owned();
    // Python heap types use __name__; C types, including immutable heap types such
    // as Decimal, include the module in tp_name.
    if flags & ffi::Py_TPFLAGS_HEAPTYPE != 0 && flags & ffi::Py_TPFLAGS_IMMUTABLETYPE == 0 {
        return Ok(name);
    }
    let module = ty
        .getattr("__module__")
        .ok()
        .and_then(|m| m.extract::<String>().ok())
        .unwrap_or_default();
    if module.is_empty() || module == "builtins" {
        Ok(name)
    } else {
        Ok(format!("{module}.{name}"))
    }
}

fn write_dict_key(py: Python<'_>, key: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
    let ptr = key.as_ptr();
    // SAFETY: ptr references a live Python key; type checks do not modify it.
    unsafe {
        if ffi::PyUnicode_Check(ptr) != 0 {
            out.push(b'"');
            let s = key.cast::<PyString>()?;
            match py_str_data(s)? {
                PyStrData::Utf8(v) => {
                    let mut escaped = Vec::with_capacity(v.len() + 2);
                    write_str(&mut escaped, v);
                    out.extend_from_slice(&escaped[1..escaped.len() - 1]);
                }
                PyStrData::Code(cps) => {
                    let mut escaped = Vec::with_capacity(cps.len() * 2 + 2);
                    write_str_code_points(&mut escaped, cps);
                    out.extend_from_slice(&escaped[1..escaped.len() - 1]);
                }
            }
            out.push(b'"');
            return Ok(());
        }
        if ffi::PyBool_Check(ptr) != 0 {
            out.push(b'"');
            if key.is_truthy()? {
                out.extend_from_slice(b"true");
            } else {
                out.extend_from_slice(b"false");
            }
            out.push(b'"');
            return Ok(());
        }
        if ffi::PyLong_Check(ptr) != 0 {
            let mut buf = Vec::new();
            write_py_int(py, key, &mut buf)?;
            out.push(b'"');
            out.extend_from_slice(&buf);
            out.push(b'"');
            return Ok(());
        }
        if ffi::PyFloat_Check(ptr) != 0 {
            let value: f64 = key.extract()?;
            let mut buf = Vec::new();
            write_float(&mut buf, value);
            out.push(b'"');
            out.extend_from_slice(&buf);
            out.push(b'"');
            return Ok(());
        }
    }
    if key.is_none() {
        out.extend_from_slice(b"\"null\"");
        return Ok(());
    }

    // Match CPython's invalid key error without calling default.
    let name = dict_key_type_name(key)?;
    Err(PyTypeError::new_err(format!(
        "keys must be str, int, float, bool or None, not {name}"
    )))
}

#[pyfunction]
#[pyo3(signature = (data, object_hook=None))]
pub fn json_loads(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    object_hook: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let doc = prepare_document(py, data)?;
    let string_data = py_str_data(doc.cast::<PyString>()?)?;

    let input_bytes: Vec<u8>;
    let src: &[u8] = match &string_data {
        PyStrData::Utf8(s) => s.as_bytes(),
        PyStrData::Code(_) => {
            input_bytes = string_data.wtf8();
            &input_bytes
        }
    };

    if src.starts_with(&[0xef, 0xbb, 0xbf]) {
        return Err(json_error(
            py,
            "Unexpected UTF-8 BOM (decode using utf-8-sig)",
            0,
            &doc,
        ));
    }

    let mut reader = Reader::new(src);
    let mut builder = Builder::new(py, object_hook);
    loop {
        match reader.next_event() {
            Ok(Some(event)) => builder.handle(event)?,
            Ok(None) => break,
            Err(err) if err.message == "maximum JSON depth exceeded" => {
                return Err(pyo3::exceptions::PyRecursionError::new_err(
                    "maximum recursion depth exceeded while decoding a JSON value",
                ));
            }
            Err(err) => return Err(json_error(py, err.message, err.pos, &doc)),
        }
    }
    Ok(builder
        .root
        .take()
        .unwrap_or_else(|| py.None().into_bound(py))
        .unbind())
}

fn prepare_document<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyString>> {
    if let Ok(s) = data.cast::<PyString>() {
        return Ok(s.clone());
    }
    if data.cast::<PyBytes>().is_ok() || data.cast::<pyo3::types::PyByteArray>().is_ok() {
        return data
            .call_method1("decode", ("utf-8",))?
            .cast_into::<PyString>()
            .map_err(Into::into);
    }
    if let Ok(mv) = data.cast::<PyMemoryView>() {
        let bytes = mv.call_method0("tobytes")?;
        return decode_utf8_strict(py, bytes.cast::<PyBytes>()?.as_bytes());
    }
    let name = type_name(data)?;
    Err(PyTypeError::new_err(format!(
        "the JSON object must be str, bytes or bytearray, not {name}"
    )))
}

fn json_error(py: Python<'_>, msg: &str, pos: usize, doc: &Bound<'_, PyString>) -> PyErr {
    match py
        .import("json")
        .and_then(|m| m.getattr("JSONDecodeError"))
        .and_then(|ty| ty.call1((msg, doc, pos)))
    {
        Ok(exc) => PyErr::from_value(exc),
        Err(err) => err,
    }
}

struct Builder<'py> {
    py: Python<'py>,
    hook: Option<Bound<'py, PyAny>>,
    stack: Vec<Container<'py>>,
    root: Option<Bound<'py, PyAny>>,
}

enum Container<'py> {
    List(Bound<'py, PyList>),
    Dict {
        dict: Bound<'py, PyDict>,
        key: Option<Bound<'py, PyString>>,
    },
}

impl<'py> Builder<'py> {
    fn new(py: Python<'py>, hook: Option<&Bound<'py, PyAny>>) -> Self {
        Self {
            py,
            hook: hook.cloned(),
            stack: Vec::with_capacity(8),
            root: None,
        }
    }

    fn handle(&mut self, event: Event<'_>) -> PyResult<()> {
        match event {
            Event::Null => self.attach(self.py.None().into_bound(self.py)),
            Event::Bool(v) => {
                let obj = PyBool::new(self.py, v).to_owned().into_any();
                self.attach(obj)
            }
            Event::Int(text) => {
                let obj = int_from_text(self.py, text)?;
                self.attach(obj)
            }
            Event::Float(v) => {
                let obj = PyFloat::new(self.py, v).into_any();
                self.attach(obj)
            }
            Event::Str(s) => {
                let obj = json_str_to_py(self.py, s)?;
                self.attach(obj)
            }
            Event::Key(s) => {
                let key = json_str_to_py(self.py, s)?;
                if let Some(Container::Dict { key: slot, .. }) = self.stack.last_mut() {
                    *slot = Some(key.cast_into::<PyString>()?);
                }
                Ok(())
            }
            Event::StartArray => {
                self.stack.push(Container::List(PyList::empty(self.py)));
                Ok(())
            }
            Event::EndArray => {
                let Some(Container::List(list)) = self.stack.pop() else {
                    return Err(PyValueError::new_err("parser: unexpected array"));
                };
                self.attach(list.into_any())
            }
            Event::StartObject => {
                self.stack.push(Container::Dict {
                    dict: PyDict::new(self.py),
                    key: None,
                });
                Ok(())
            }
            Event::EndObject => {
                let Some(Container::Dict { dict, .. }) = self.stack.pop() else {
                    return Err(PyValueError::new_err("parser: unexpected object"));
                };
                let value = self.apply_hook(dict.into_any())?;
                self.attach(value)
            }
        }
    }

    fn apply_hook(&self, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let Some(hook) = &self.hook else {
            return Ok(value);
        };
        let dict = value.cast::<PyDict>()?;
        if dict.len() != 2 || !dict.contains("__type__")? || !dict.contains("__value__")? {
            return Ok(value);
        }
        hook.call1((value,))
    }

    fn attach(&mut self, value: Bound<'py, PyAny>) -> PyResult<()> {
        match self.stack.last_mut() {
            Some(Container::List(list)) => {
                list.append(value)?;
            }
            Some(Container::Dict { dict, key }) => {
                let key = key
                    .take()
                    .ok_or_else(|| PyValueError::new_err("parser: object value without key"))?;
                dict.set_item(key, value)?;
            }
            None => self.root = Some(value),
        }
        Ok(())
    }
}

fn int_from_text<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(v) = text.parse::<i64>() {
        return Ok(v.into_pyobject(py)?.into_any());
    }
    let mut buf = Vec::with_capacity(text.len() + 1);
    buf.extend_from_slice(text.as_bytes());
    buf.push(0);
    // SAFETY: buf is NUL-terminated and remains alive throughout the call.
    let ptr = unsafe { ffi::PyLong_FromString(buf.as_ptr().cast(), std::ptr::null_mut(), 10) };
    if ptr.is_null() {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: ptr is a new owned reference returned by PyLong_FromString.
    Ok(unsafe { Bound::from_owned_ptr(py, ptr) })
}

fn json_str_to_py<'py>(py: Python<'py>, s: JsonStr<'_>) -> PyResult<Bound<'py, PyAny>> {
    Ok(match s {
        JsonStr::Borrowed(v) => PyString::new(py, v).into_any(),
        JsonStr::Owned(v) => PyString::new(py, &v).into_any(),
        JsonStr::Code(cps) => py_str_from_code_points(py, &cps)?.into_any(),
    })
}
