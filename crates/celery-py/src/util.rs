//! Python string helpers preserving lone surrogates.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

/// Python string contents.
pub enum PyStrData<'a> {
    /// Valid UTF-8 borrowed from Python.
    Utf8(&'a str),
    /// Code points containing lone surrogates.
    Code(Vec<u32>),
}

impl PyStrData<'_> {
    pub fn wtf8(&self) -> Vec<u8> {
        match self {
            Self::Utf8(s) => s.as_bytes().to_vec(),
            Self::Code(cps) => encode_wtf8(cps),
        }
    }
}

/// Extracts string code points without losing lone surrogates.
pub fn py_str_data<'a>(s: &'a Bound<'_, PyString>) -> PyResult<PyStrData<'a>> {
    match s.to_str() {
        Ok(v) => Ok(PyStrData::Utf8(v)),
        Err(_) => {
            // Consume the exception left by to_str before continuing.
            let _ = PyErr::take(s.py());
            // SAFETY: s is a live string; the call returns a new reference or NULL without mutation.
            let encoded = unsafe {
                pyo3::ffi::PyUnicode_AsEncodedString(
                    s.as_ptr(),
                    c"utf-8".as_ptr(),
                    c"surrogatepass".as_ptr(),
                )
            };
            if encoded.is_null() {
                return Err(PyErr::fetch(s.py()));
            }
            // SAFETY: encoded is a non-NULL owned reference.
            let bytes = unsafe { Bound::from_owned_ptr(s.py(), encoded) }.cast_into::<PyBytes>()?;
            Ok(PyStrData::Code(decode_wtf8(bytes.as_bytes())))
        }
    }
}

/// Decodes strict UTF-8 as bytes.decode('utf-8') does.
pub fn decode_utf8_strict<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyString>> {
    PyBytes::new(py, data)
        .call_method1("decode", ("utf-8",))?
        .cast_into::<PyString>()
        .map_err(Into::into)
}

/// Builds a Python string from code points, including lone surrogates.
pub fn py_str_from_code_points<'py>(
    py: Python<'py>,
    cps: &[u32],
) -> PyResult<Bound<'py, PyString>> {
    if let Some(s) = code_points_to_string(cps) {
        return Ok(PyString::new(py, &s));
    }
    let bytes = encode_wtf8(cps);
    PyBytes::new(py, &bytes)
        .call_method1("decode", ("utf-8", "surrogatepass"))?
        .cast_into::<PyString>()
        .map_err(Into::into)
}

/// Decodes UTF-8 or WTF-8 to code points.
pub fn decode_wtf8(src: &[u8]) -> Vec<u32> {
    let mut out = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < src.len() {
        match decode_one(src, i) {
            Some((cp, next)) => {
                out.push(cp);
                i = next;
            }
            None => {
                out.push(0xfffd);
                i += 1;
            }
        }
    }
    out
}

fn decode_one(src: &[u8], i: usize) -> Option<(u32, usize)> {
    let b0 = *src.get(i)?;
    if b0 < 0x80 {
        return Some((b0 as u32, i + 1));
    }
    let (len, init) = match b0 {
        0xc2..=0xdf => (2, (b0 & 0x1f) as u32),
        0xe0..=0xef => (3, (b0 & 0x0f) as u32),
        0xf0..=0xf4 => (4, (b0 & 0x07) as u32),
        _ => return None,
    };
    let mut cp = init;
    for k in 1..len {
        let b = *src.get(i + k)?;
        if b & 0xc0 != 0x80 {
            return None;
        }
        cp = (cp << 6) | (b & 0x3f) as u32;
    }
    Some((cp, i + len))
}

pub fn encode_wtf8(cps: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(cps.len() * 3);
    for &cp in cps {
        if cp < 0x80 {
            out.push(cp as u8);
        } else if cp < 0x800 {
            out.push(0xc0 | (cp >> 6) as u8);
            out.push(0x80 | (cp & 0x3f) as u8);
        } else if cp < 0x1_0000 {
            out.push(0xe0 | (cp >> 12) as u8);
            out.push(0x80 | ((cp >> 6) & 0x3f) as u8);
            out.push(0x80 | (cp & 0x3f) as u8);
        } else {
            out.push(0xf0 | (cp >> 18) as u8);
            out.push(0x80 | ((cp >> 12) & 0x3f) as u8);
            out.push(0x80 | ((cp >> 6) & 0x3f) as u8);
            out.push(0x80 | (cp & 0x3f) as u8);
        }
    }
    out
}

pub fn code_points_to_string(cps: &[u32]) -> Option<String> {
    let mut s = String::with_capacity(cps.len());
    for &cp in cps {
        s.push(char::from_u32(cp)?);
    }
    Some(s)
}
