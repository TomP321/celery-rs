//! Compares float output with 600 CPython json.dumps fixtures generated using
//! a fixed seed, including boundary values, subnormals and random bit patterns.

use celery_proto::json::write_float;

#[test]
fn float_repr_matches_python_bytes() {
    let fixture = include_str!("fixtures/floats.txt");
    let mut n = 0;
    for line in fixture.lines() {
        let (bits_hex, expected) = line.split_once(' ').expect("fixture row");
        let bits = u64::from_str_radix(bits_hex, 16).expect("hexadecimal bits");
        let value = f64::from_bits(bits);
        let mut out = Vec::new();
        write_float(&mut out, value);
        let got = String::from_utf8(out).expect("utf-8");
        assert_eq!(got, expected, "bits {bits_hex} ({value:?})");
        n += 1;
    }
    assert!(n >= 200, "fixture too small: {n}");
}
