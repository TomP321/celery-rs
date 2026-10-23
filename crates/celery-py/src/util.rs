//! Helper per leggere/scrivere `str` Python, anche con surrogati isolati.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

/// Contenuto di una `str` Python.
pub enum PyStrData<'a> {
    /// UTF-8 valido, prestato dall'oggetto Python.
    Utf8(&'a str),
    /// Surrogati isolati: sequenza di code point.
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

/// Estrae i code point di una `str`, senza perdere i surrogati isolati.
pub fn py_str_data<'a>(s: &'a Bound<'_, PyString>) -> PyResult<PyStrData<'a>> {
    match s.to_str() {
        Ok(v) => Ok(PyStrData::Utf8(v)),
        Err(_) => {
            // `to_str` ha lasciato un'eccezione attiva: va consumata.
            let _ = PyErr::take(s.py());
            // SAFETY: `s` è una str viva; la chiamata restituisce un nuovo riferimento
            // o NULL, e non modifica l'oggetto.
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
            // SAFETY: `encoded` è un riferimento posseduto non NULL.
            let bytes = unsafe { Bound::from_owned_ptr(s.py(), encoded) }.cast_into::<PyBytes>()?;
            Ok(PyStrData::Code(decode_wtf8(bytes.as_bytes())))
        }
    }
}

/// Decodifica UTF-8 stretto, come `bytes.decode('utf-8')`.
pub fn decode_utf8_strict<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyString>> {
    PyBytes::new(py, data)
        .call_method1("decode", ("utf-8",))?
        .cast_into::<PyString>()
        .map_err(Into::into)
}

/// Costruisce una `str` Python da code point, inclusi surrogati isolati.
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

/// Decodifica UTF-8/WTF-8 in code point.
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
