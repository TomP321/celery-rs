#[path = "../../../tools/fuzz/support.rs"]
mod support;
use celery_transport::redis::json;

#[test]
#[ignore = "run for at least ten minutes: tools/fuzz/run"]
fn fuzz_redis_envelope() {
    let seeds = [
        br#"{"body":"W1tdLHt9LHt9XQ==","content-type":"application/json","content-encoding":"utf-8","headers":{"task":"t","id":"id"},"properties":{"delivery_tag":"tag","body_encoding":"base64","delivery_info":{"exchange":"","routing_key":"q"}}}"#.to_vec(),
        br#"[{"body":"","properties":{"delivery_tag":"tag"}},"","q"]"#.to_vec(),
        [vec![b'['; 1024], vec![b']'; 1024]].concat(),
        br#"{"headers":{"__type__":"bytes","__value__":"/w=="},"properties":{}}"#.to_vec(),
    ];
    support::campaign(&seeds, |bytes| {
        let _ = json::decode(bytes);
        let _ = json::mark_redelivered(bytes);
        let _ = json::split_unacked(bytes);
        let _ = json::base64_decode(bytes);
    });
}

#[test]
fn deeply_nested_envelope_is_rejected() {
    let data = [vec![b'['; 100_000], vec![b']'; 100_000]].concat();
    assert!(json::decode(&data).is_err());
    assert!(json::mark_redelivered(&data).is_err());
    assert!(json::split_unacked(&data).is_err());
}

#[test]
fn trailing_data_in_envelope_is_rejected() {
    let valid = br#"{"body":"","properties":{"delivery_tag":"tag"}}"#;
    assert!(json::decode(valid).is_ok());
    let mut invalid = valid.to_vec();
    invalid.extend_from_slice(b"null");
    assert!(json::decode(&invalid).is_err());
    assert!(json::mark_redelivered(&invalid).is_err());
    assert!(json::split_unacked(b"[{},\"\",\"q\"]null").is_err());
}
