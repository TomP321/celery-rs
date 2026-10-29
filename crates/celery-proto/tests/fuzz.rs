#[path = "../../../tools/fuzz/support.rs"]
mod support;
use celery_proto::{compression, json::Reader};

#[test]
#[ignore = "run for at least ten minutes: tools/fuzz/run"]
fn fuzz_json() {
    let seeds = [
        br#"[[1,"text",null],{}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}]"#
            .to_vec(),
        br#"{"task":"t","id":"id","args":[1],"kwargs":{},"retries":0}"#.to_vec(),
        br#"{"task":"t","id":"id","eta":"2026-01-01T00:00:00Z"}"#.to_vec(),
        [vec![b'['; 1024], vec![b']'; 1024]].concat(),
        br#"["\ud800","\udfff",1e9999,NaN,-Infinity]"#.to_vec(),
    ];
    support::campaign(&seeds, |bytes| {
        let mut reader = Reader::with_max_depth(bytes, 64);
        let mut events = 0;
        while let Ok(Some(_)) = reader.next_event() {
            events += 1;
            assert!(events <= bytes.len() + 1);
        }
    });
}

#[test]
#[ignore = "run for at least ten minutes: tools/fuzz/run"]
fn fuzz_compression() {
    let names = ["gzip", "bzip2", "xz", "brotli", "zstd"];
    let seeds: Vec<_> = names
        .iter()
        .map(|name| compression::compress(name, &[0; 8192]).unwrap().0)
        .collect();
    support::campaign(&seeds, |bytes| {
        for name in names {
            if let Ok(body) = compression::decompress(name, bytes) {
                assert!(body.len() <= compression::MAX_DECOMPRESSED_SIZE);
            }
        }
    });
}
