//! Confronta `write_float` con l'output di `json.dumps` di CPython.
//!
//! La fixture è generata una volta con `.venv-ref/bin/python` (seed fisso) e
//! contiene 600 valori: limite, subnormali e bit pattern casuali.

use celery_proto::json::write_float;

#[test]
fn float_repr_byte_identico_a_python() {
    let fixture = include_str!("fixtures/floats.txt");
    let mut n = 0;
    for line in fixture.lines() {
        let (bits_hex, atteso) = line.split_once(' ').expect("riga fixture");
        let bits = u64::from_str_radix(bits_hex, 16).expect("bits esadecimali");
        let value = f64::from_bits(bits);
        let mut out = Vec::new();
        write_float(&mut out, value);
        let got = String::from_utf8(out).expect("utf-8");
        assert_eq!(got, atteso, "bits {bits_hex} ({value:?})");
        n += 1;
    }
    assert!(n >= 200, "fixture troppo piccola: {n}");
}
