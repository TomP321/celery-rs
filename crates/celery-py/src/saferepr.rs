//! `saferepr` identico a `celery.utils.saferepr.saferepr`.
//!
//! L'implementazione è iterativa (stack esplicito sullo heap) per non esaurire
//! lo stack nativo con strutture profonde. Solo le chiavi dei dict ricorrono
//! come nel riferimento Python, con un limite che produce `RecursionError`.

use std::borrow::Cow;
use std::collections::HashSet;
use std::mem;

use pyo3::exceptions::PyRecursionError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyIterator, PyRange, PyString, PyTuple};
use pyo3::{PyTypeInfo, ffi};

use crate::util::{PyStrData, py_str_data, py_str_from_code_points};

static NUMBER_TYPE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static DECIMAL_TYPE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

const KEY_RECURSION_HARD_CAP: usize = 1000;

fn number_type(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let v = NUMBER_TYPE.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("numbers")?.getattr("Number")?.unbind())
    })?;
    Ok(v.bind(py).clone())
}

/// `decimal.Decimal`, senza importare `decimal` quando non è già caricato.
///
/// Importarlo a ogni `saferepr` costava il modulo `decimal` (e le estensioni
/// C che porta) nel processo client, dove il protocollo è Rust (spec 08b).
/// Un'istanza di Decimal può esistere solo se qualcuno ha già importato
/// `decimal`, quindi la classe si prende da `sys.modules`.
fn decimal_type(py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
    if let Some(v) = DECIMAL_TYPE.get(py) {
        return Ok(Some(v.clone_ref(py)));
    }
    let modules = py.import("sys")?.getattr("modules")?;
    let Some(module) = modules.cast::<PyDict>()?.get_item("decimal")? else {
        return Ok(None);
    };
    let v = module.getattr("Decimal")?.unbind();
    let _ = DECIMAL_TYPE.set(py, v.clone_ref(py));
    Ok(Some(v))
}

/// Limite per le chiavi annidate: il riferimento consuma circa due frame
/// Python per livello, oltre ai frame del chiamante.
fn key_recursion_limit() -> usize {
    // SAFETY: `Py_GetRecursionLimit` non richiede stato aggiuntivo.
    let limit = unsafe { ffi::Py_GetRecursionLimit() }.max(1) as usize;
    ((limit.saturating_sub(8)) / 2).clamp(1, KEY_RECURSION_HARD_CAP)
}

#[pyfunction]
#[pyo3(signature = (obj, maxlen=None, maxlevels=3))]
pub fn saferepr(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    maxlen: Option<i64>,
    maxlevels: Option<i64>,
) -> PyResult<Py<PyString>> {
    let text = render(py, obj, maxlen, maxlevels, 0)?;
    Ok(py_str_from_code_points(py, &text)?.unbind())
}

struct Out {
    buf: Vec<u32>,
    /// Budget residuo in code point; `None` = illimitato.
    maxlen: Option<i64>,
}

impl Out {
    fn exhausted(&self) -> bool {
        matches!(self.maxlen, Some(m) if m <= 0)
    }

    fn push_str(&mut self, s: &str) {
        self.buf.extend(s.chars().map(|c| c as u32));
    }

    fn push_cps(&mut self, cps: &[u32]) {
        self.buf.extend_from_slice(cps);
    }

    fn charge(&mut self, n: usize) {
        if let Some(m) = self.maxlen.as_mut() {
            *m -= n as i64;
        }
    }

    /// Token literal (parentesi, separatori): non viene mai troncato.
    fn token_literal(&mut self, s: &str) -> bool {
        if self.exhausted() {
            self.push_str(", ...");
            return false;
        }
        let before = self.buf.len();
        self.push_str(s);
        self.charge(self.buf.len() - before);
        true
    }

    /// Token di testo: applica `truncate` con il budget residuo.
    fn token_text(&mut self, cps: &[u32]) -> bool {
        if self.exhausted() {
            self.push_str(", ...");
            return false;
        }
        let v = truncate(cps, self.maxlen);
        self.push_cps(&v);
        self.charge(v.len());
        true
    }

    fn token_quoted(&mut self, cps: &[u32]) -> bool {
        if self.exhausted() {
            self.push_str(", ...");
            return false;
        }
        let v = truncate(cps, self.maxlen);
        let before = self.buf.len();
        self.buf.push(b'\'' as u32);
        for cp in v {
            if cp == b'\'' as u32 {
                self.buf.push(b'\\' as u32);
            }
            self.buf.push(cp);
        }
        self.buf.push(b'\'' as u32);
        self.charge(self.buf.len() - before);
        true
    }

    fn token_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.exhausted() {
            self.push_str(", ...");
            return false;
        }
        let (text_cps, ellipsis) = format_binary_bytes(bytes, self.maxlen);
        let before = self.buf.len();
        self.push_str("b'");
        self.push_cps(&text_cps);
        if ellipsis {
            self.push_str("...");
        }
        self.buf.push(b'\'' as u32);
        self.charge(self.buf.len() - before);
        true
    }

    fn push_forced(&mut self, s: &str) {
        self.push_str(s);
    }
}

enum Prepared<'py> {
    Text(Vec<u32>),
    Quoted(Vec<u32>),
    Bytes(Bound<'py, PyBytes>),
    Container(Frame<'py>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Start,
    SeqItems,
    SeqAfter,
    DictKey,
    DictAfter,
    End,
}

enum Kind<'py> {
    Pending(Bound<'py, PyAny>, bool),
    Seq(Bound<'py, PyIterator>),
    Dict(Bound<'py, PyIterator>),
    Done,
}

struct Frame<'py> {
    kind: Kind<'py>,
    state: State,
    start: Cow<'static, str>,
    end: Cow<'static, str>,
    id: usize,
    size: usize,
    index: usize,
}

struct Engine<'py> {
    py: Python<'py>,
    out: Out,
    maxlevels: Option<i64>,
    seen: HashSet<usize>,
    stack: Vec<Frame<'py>>,
    pending: Option<Bound<'py, PyAny>>,
    key_depth: usize,
    stopped: bool,
}

fn render(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    maxlen: Option<i64>,
    maxlevels: Option<i64>,
    key_depth: usize,
) -> PyResult<Vec<u32>> {
    if key_depth > key_recursion_limit() {
        return Err(PyRecursionError::new_err(
            "maximum recursion depth exceeded",
        ));
    }
    let mut engine = Engine {
        py,
        out: Out {
            buf: Vec::with_capacity(64),
            maxlen,
        },
        maxlevels,
        seen: HashSet::new(),
        stack: Vec::new(),
        pending: Some(obj.clone()),
        key_depth,
        stopped: false,
    };
    engine.run()?;
    Ok(engine.out.buf)
}

impl<'py> Engine<'py> {
    fn run(&mut self) -> PyResult<()> {
        loop {
            if let Some(obj) = self.pending.take() {
                match self.prepare(&obj)? {
                    Prepared::Text(cps) => {
                        if !self.out.token_text(&cps) {
                            self.stopped = true;
                        }
                    }
                    Prepared::Quoted(cps) => {
                        if !self.out.token_quoted(&cps) {
                            self.stopped = true;
                        }
                    }
                    Prepared::Bytes(bytes) => {
                        if !self.out.token_bytes(bytes.as_bytes()) {
                            self.stopped = true;
                        }
                    }
                    Prepared::Container(frame) => self.stack.push(frame),
                }
                if self.stopped {
                    break;
                }
                continue;
            }

            if self.stack.is_empty() {
                break;
            }
            let idx = self.stack.len() - 1;
            match self.stack[idx].state {
                State::Start => {
                    let start = self.stack[idx].start.clone();
                    if !self.out.token_literal(&start) {
                        self.stopped = true;
                        break;
                    }
                    let id = self.stack[idx].id;
                    self.seen.insert(id);
                    let kind = mem::replace(&mut self.stack[idx].kind, Kind::Done);
                    match kind {
                        Kind::Pending(obj, is_dict) => {
                            let size = obj.len()?;
                            let iter = if is_dict {
                                obj.call_method0("items")?.try_iter()?
                            } else {
                                obj.try_iter()?
                            };
                            self.stack[idx].size = size;
                            self.stack[idx].kind = if is_dict {
                                Kind::Dict(iter)
                            } else {
                                Kind::Seq(iter)
                            };
                            self.stack[idx].state = if is_dict {
                                State::DictKey
                            } else {
                                State::SeqItems
                            };
                        }
                        other => {
                            self.stack[idx].kind = other;
                            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                                "saferepr: stato interno non valido",
                            ));
                        }
                    }
                }
                State::SeqItems => {
                    let next = match &mut self.stack[idx].kind {
                        Kind::Seq(iter) => iter.next(),
                        _ => None,
                    };
                    match next {
                        Some(item) => {
                            self.stack[idx].state = State::SeqAfter;
                            self.pending = Some(item?);
                        }
                        None => self.stack[idx].state = State::End,
                    }
                }
                State::SeqAfter | State::DictAfter => {
                    self.stack[idx].index += 1;
                    let (index, size) = (self.stack[idx].index, self.stack[idx].size);
                    if index < size && !self.out.token_literal(", ") {
                        self.stopped = true;
                        break;
                    }
                    self.stack[idx].state = if self.stack[idx].state == State::SeqAfter {
                        State::SeqItems
                    } else {
                        State::DictKey
                    };
                }
                State::DictKey => {
                    let next = match &mut self.stack[idx].kind {
                        Kind::Dict(iter) => iter.next(),
                        _ => None,
                    };
                    match next {
                        None => self.stack[idx].state = State::End,
                        Some(item) => {
                            let pair = item?;
                            let pair = pair.cast::<PyTuple>()?;
                            let key = pair.get_item(0)?;
                            let value = pair.get_item(1)?;

                            if self.out.exhausted() {
                                self.out.push_str(", ...");
                                self.stopped = true;
                                break;
                            }
                            let text = render(
                                self.py,
                                &key,
                                self.out.maxlen,
                                self.maxlevels,
                                self.key_depth + 1,
                            )?;
                            self.out.push_cps(&text);
                            self.out.charge(text.len());
                            if !self.out.token_literal(": ") {
                                self.stopped = true;
                                break;
                            }
                            self.stack[idx].state = State::DictAfter;
                            self.pending = Some(value);
                        }
                    }
                }
                State::End => {
                    let Some(frame) = self.stack.pop() else {
                        break;
                    };
                    self.seen.remove(&frame.id);
                    if !self.out.token_literal(&frame.end) {
                        self.stopped = true;
                        break;
                    }
                }
            }
        }

        if self.stopped {
            // Parentesi di chiusura pendenti, dalla più interna alla più esterna.
            for frame in self.stack.iter().rev() {
                self.out.push_forced(&frame.end);
            }
        }
        Ok(())
    }

    fn prepare(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Prepared<'py>> {
        let py = self.py;
        let ptr = obj.as_ptr();

        // Decimal prima di Number, come nel riferimento.
        if let Some(decimal) = decimal_type(py)?
            && obj.is_instance(decimal.bind(py))?
        {
            return Ok(Prepared::Text(text_cps(py, &obj.repr()?)?));
        }

        // SAFETY: `ptr` è un oggetto Python vivo; i check di tipo non lo modificano.
        unsafe {
            if ffi::PyBool_Check(ptr) != 0
                || ffi::PyLong_Check(ptr) != 0
                || ffi::PyFloat_Check(ptr) != 0
                || ffi::PyComplex_Check(ptr) != 0
            {
                return Ok(Prepared::Text(text_cps(py, &obj.str()?)?));
            }
            if ffi::PyUnicode_Check(ptr) != 0 {
                let cps = match py_str_data(obj.cast::<PyString>()?)? {
                    PyStrData::Utf8(s) => str_cps(s),
                    PyStrData::Code(cps) => cps,
                };
                return Ok(Prepared::Quoted(cps));
            }
            if ffi::PyBytes_Check(ptr) != 0 {
                let bytes = obj.cast::<PyBytes>()?;
                return Ok(Prepared::Bytes(bytes.clone()));
            }
        }

        // `numbers.Number` non builtin (es. Fraction).
        if obj.is_instance(&number_type(py)?)? {
            return Ok(Prepared::Text(text_cps(py, &obj.str()?)?));
        }

        let range_ty = PyRange::type_object(py);
        if obj.is_instance(range_ty.as_any())? {
            return Ok(Prepared::Text(text_cps(py, &obj.repr()?)?));
        }

        // SAFETY: `ptr` è un oggetto Python vivo; i check di tipo non lo modificano.
        unsafe {
            if ffi::PySet_Check(ptr) != 0 || ffi::PyFrozenSet_Check(ptr) != 0 {
                return self.prepare_set(obj);
            }
            if ffi::PyTuple_Check(ptr) != 0 {
                let size = obj.len()?;
                let end = if size == 1 {
                    Cow::Borrowed(",)")
                } else {
                    Cow::Borrowed(")")
                };
                return self.prepare_container(obj, false, Cow::Borrowed("("), end);
            }
            if ffi::PyDict_Check(ptr) != 0 {
                return self.prepare_container(obj, true, Cow::Borrowed("{"), Cow::Borrowed("}"));
            }
            if ffi::PyList_Check(ptr) != 0 {
                return self.prepare_container(obj, false, Cow::Borrowed("["), Cow::Borrowed("]"));
            }
        }

        Ok(Prepared::Text(repr_cps(py, obj)?))
    }

    fn prepare_set(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Prepared<'py>> {
        let size = obj.len()?;
        if size == 0 {
            let name = type_name(obj)?;
            return Ok(Prepared::Text(str_cps(&format!("{name}()"))));
        }
        // SAFETY: `obj` è un set/frozenset vivo.
        let exact_set = unsafe { ffi::PySet_CheckExact(obj.as_ptr()) != 0 };
        if exact_set {
            self.prepare_container(obj, false, Cow::Borrowed("{"), Cow::Borrowed("}"))
        } else {
            let name = type_name(obj)?;
            self.prepare_container(
                obj,
                false,
                Cow::Owned(format!("{name}({{")),
                Cow::Borrowed("})"),
            )
        }
    }

    fn prepare_container(
        &mut self,
        obj: &Bound<'py, PyAny>,
        is_dict: bool,
        start: Cow<'static, str>,
        end: Cow<'static, str>,
    ) -> PyResult<Prepared<'py>> {
        let level = self.stack.len() as i64;
        if let Some(maxlevels) = self.maxlevels
            && maxlevels != 0
            && level >= maxlevels
        {
            return Ok(Prepared::Text(str_cps(&format!("{start}...{end}"))));
        }
        let id = obj.as_ptr() as usize;
        if self.seen.contains(&id) {
            return Ok(Prepared::Text(str_cps(&recursion_text(obj)?)));
        }
        Ok(Prepared::Container(Frame {
            kind: Kind::Pending(obj.clone(), is_dict),
            state: State::Start,
            start,
            end,
            id,
            size: 0,
            index: 0,
        }))
    }
}

fn str_cps(s: &str) -> Vec<u32> {
    s.chars().map(|c| c as u32).collect()
}

fn text_cps(_py: Python<'_>, s: &Bound<'_, PyString>) -> PyResult<Vec<u32>> {
    Ok(match py_str_data(s)? {
        PyStrData::Utf8(v) => str_cps(v),
        PyStrData::Code(v) => v,
    })
}

fn type_name(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(obj.get_type().name()?.to_string_lossy().into_owned())
}

fn recursion_text(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(format!(
        "<Recursion on {} with id={}>",
        type_name(obj)?,
        obj.as_ptr() as usize
    ))
}

fn repr_cps(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<u32>> {
    match obj.repr() {
        Ok(s) => text_cps(py, &s),
        Err(exc) => {
            let type_repr = obj.get_type().repr()?.to_string_lossy().into_owned();
            let exc_value = exc.value(py);
            let exc_repr = exc_value.repr()?.to_string_lossy().into_owned();
            let stack = py.import("traceback")?.call_method0("format_stack")?;
            let joined = PyString::new(py, "\n").call_method1("join", (stack,))?;
            let stack_repr = joined.repr()?.to_string_lossy().into_owned();
            let text = format!(
                "<Unrepresentable {} {:#x}: {} {}>",
                type_repr,
                obj.as_ptr() as usize,
                exc_repr,
                stack_repr
            );
            Ok(str_cps(&text))
        }
    }
}

fn truncate(cps: &[u32], maxlen: Option<i64>) -> Vec<u32> {
    let Some(m) = maxlen else {
        return cps.to_vec();
    };
    if m == 0 {
        return cps.to_vec();
    }
    let len = cps.len() as i64;
    if m > 0 && len < m {
        return cps.to_vec();
    }
    let take = if m > 0 {
        (m as usize).min(cps.len())
    } else {
        let k = len + m;
        if k <= 0 { 0 } else { k as usize }
    };
    let head = &cps[..take];
    let cut = head
        .iter()
        .rposition(|&c| c == b' ' as u32)
        .unwrap_or(head.len());
    let mut out = head[..cut].to_vec();
    out.extend("...".chars().map(|c| c as u32));
    out
}

fn format_binary_bytes(bytes: &[u8], maxlen: Option<i64>) -> (Vec<u32>, bool) {
    let mut ellipsis = false;
    let chunk: &[u8] = match maxlen {
        Some(m) if m != 0 && (bytes.len() as i64) > m => {
            ellipsis = true;
            let take = if m > 0 {
                (m as usize).min(bytes.len())
            } else {
                let k = bytes.len() as i64 + m;
                if k <= 0 { 0 } else { k as usize }
            };
            &bytes[..take]
        }
        _ => bytes,
    };
    match std::str::from_utf8(chunk) {
        Ok(s) => (str_cps(s), ellipsis),
        Err(_) => (str_cps(&hex_lower(chunk)), ellipsis),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn troncamento_come_truncate() {
        let s: Vec<u32> = "abc def".chars().map(|c| c as u32).collect();
        assert_eq!(
            String::from_iter(
                truncate(&s, Some(1))
                    .iter()
                    .map(|&c| char::from_u32(c).unwrap())
            ),
            "a..."
        );
        assert_eq!(
            String::from_iter(
                truncate(&s, Some(5))
                    .iter()
                    .map(|&c| char::from_u32(c).unwrap())
            ),
            "abc..."
        );
    }
}
