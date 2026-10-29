//! JSON encoding with kombu's ASCII escaping, default separators and nonfinite floats.

/// Writes a Rust string as JSON.
pub fn write_str(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    for ch in s.chars() {
        write_code_point(out, ch as u32);
    }
    out.push(b'"');
}

/// Writes code points as JSON, accepting Python's lone surrogates.
pub fn write_str_code_points<I>(out: &mut Vec<u8>, code_points: I)
where
    I: IntoIterator<Item = u32>,
{
    out.push(b'"');
    for cp in code_points {
        write_code_point(out, cp);
    }
    out.push(b'"');
}

fn write_code_point(out: &mut Vec<u8>, cp: u32) {
    match cp {
        0x22 => out.extend_from_slice(b"\\\""),
        0x5c => out.extend_from_slice(b"\\\\"),
        0x0a => out.extend_from_slice(b"\\n"),
        0x0d => out.extend_from_slice(b"\\r"),
        0x09 => out.extend_from_slice(b"\\t"),
        0x08 => out.extend_from_slice(b"\\b"),
        0x0c => out.extend_from_slice(b"\\f"),
        0x20..=0x7e => out.push(cp as u8),

        0x0000..=0xffff => write_u_escape(out, cp),
        _ => {
            // Encode astral characters as UTF-16 surrogate pairs, matching CPython's C encoder.
            let v = cp - 0x1_0000;
            write_u_escape(out, 0xd800 + (v >> 10));
            write_u_escape(out, 0xdc00 + (v & 0x3ff));
        }
    }
}

fn write_u_escape(out: &mut Vec<u8>, v: u32) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.extend_from_slice(b"\\u");
    out.push(HEX[((v >> 12) & 0xf) as usize]);
    out.push(HEX[((v >> 8) & 0xf) as usize]);
    out.push(HEX[((v >> 4) & 0xf) as usize]);
    out.push(HEX[(v & 0xf) as usize]);
}

/// Writes an integer as Python json.dumps does.
pub fn write_int(out: &mut Vec<u8>, v: i64) {
    let mut buf = itoa::Buffer::new();
    out.extend_from_slice(buf.format(v).as_bytes());
}

/// Writes a float using Python float representation.
pub fn write_float(out: &mut Vec<u8>, value: f64) {
    if value.is_nan() {
        out.extend_from_slice(b"NaN");
        return;
    }
    if value.is_infinite() {
        if value.is_sign_negative() {
            out.extend_from_slice(b"-Infinity");
        } else {
            out.extend_from_slice(b"Infinity");
        }
        return;
    }
    if value == 0.0 {
        if value.is_sign_negative() {
            out.extend_from_slice(b"-0.0");
        } else {
            out.extend_from_slice(b"0.0");
        }
        return;
    }

    let mut buf = ryu::Buffer::new();
    let repr = buf.format_finite(value);
    write_python_repr(out, repr);
}

// Converts ryu's shortest round-trip output to Python formatting using stack storage.
fn write_python_repr(out: &mut Vec<u8>, repr: &str) {
    let bytes = repr.as_bytes();
    let start = usize::from(bytes.first() == Some(&b'-'));
    let negative = start == 1;

    let (mantissa_end, exp10) = match bytes.iter().position(|&b| b == b'e' || b == b'E') {
        Some(pos) => {
            let mut j = pos + 1;
            let mut sign = 1i32;
            if bytes.get(j) == Some(&b'-') {
                sign = -1;
                j += 1;
            } else if bytes.get(j) == Some(&b'+') {
                j += 1;
            }
            let mut exp = 0i32;
            while let Some(&b) = bytes.get(j) {
                if b.is_ascii_digit() {
                    exp = exp.saturating_mul(10).saturating_add((b - b'0') as i32);
                }
                j += 1;
            }
            (pos, sign * exp)
        }
        None => (bytes.len(), 0),
    };

    let dot = bytes[start..mantissa_end]
        .iter()
        .position(|&b| b == b'.')
        .map(|p| start + p);
    let int_end = dot.unwrap_or(mantissa_end);
    let frac_start = dot.map_or(mantissa_end, |d| d + 1);
    let frac_len = mantissa_end - frac_start;

    let mut digits = [0u8; 32];
    let mut n = 0usize;
    for &b in &bytes[start..int_end] {
        digits[n] = b;
        n += 1;
    }
    for &b in &bytes[frac_start..mantissa_end] {
        digits[n] = b;
        n += 1;
    }
    let mut exp = exp10 - frac_len as i32;
    let mut lead = 0usize;
    while lead < n && digits[lead] == b'0' {
        lead += 1;
    }
    let mut end = n;
    while end > lead + 1 && digits[end - 1] == b'0' {
        end -= 1;
        exp += 1;
    }
    if lead == end {
        out.extend_from_slice(b"0.0");
        return;
    }
    let digits = &digits[lead..end];
    let sci_exp = exp + digits.len() as i32 - 1;

    if negative {
        out.push(b'-');
    }

    if !(-4..16).contains(&sci_exp) {
        out.push(digits[0]);
        if digits.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(&digits[1..]);
        }
        out.push(b'e');
        if sci_exp < 0 {
            out.push(b'-');
        } else {
            out.push(b'+');
        }
        let abs = sci_exp.unsigned_abs();
        if abs < 10 {
            out.push(b'0');
        }
        let mut buf = itoa::Buffer::new();
        out.extend_from_slice(buf.format(abs).as_bytes());
    } else if sci_exp >= 0 {
        let int_len = (sci_exp + 1) as usize;
        if digits.len() <= int_len {
            out.extend_from_slice(digits);
            for _ in digits.len()..int_len {
                out.push(b'0');
            }
            out.extend_from_slice(b".0");
        } else {
            out.extend_from_slice(&digits[..int_len]);
            out.push(b'.');
            out.extend_from_slice(&digits[int_len..]);
        }
    } else {
        out.extend_from_slice(b"0.");
        for _ in 0..(-sci_exp - 1) {
            out.push(b'0');
        }
        out.extend_from_slice(digits);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_match_cpython() {
        let mut out = Vec::new();
        write_str(&mut out, "\"\\\n\r\t\u{8}\u{c}\u{1}\u{7f}\u{e9}\u{1f600}");
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "\"\\\"\\\\\\n\\r\\t\\b\\f\\u0001\\u007f\\u00e9\\ud83d\\ude00\""
        );
    }

    #[test]
    fn lone_surrogates() {
        let mut out = Vec::new();
        write_str_code_points(&mut out, [0xd800, 0x61, 0xdc00]);
        assert_eq!(String::from_utf8(out).unwrap(), "\"\\ud800a\\udc00\"");
    }

    #[test]
    fn float_python_format() {
        let cases = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (0.1, "0.1"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.5e-5, "1.5e-05"),
            (1e-4, "0.0001"),
            (5e-324, "5e-324"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            (f64::NAN, "NaN"),
        ];
        for (v, expected) in cases {
            let mut out = Vec::new();
            write_float(&mut out, v);
            assert_eq!(String::from_utf8(out).unwrap(), expected, "value {v:?}");
        }
    }
}
