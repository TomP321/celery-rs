use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use std::time::{Duration, Instant};

pub fn campaign(seeds: &[Vec<u8>], check: impl Fn(&[u8])) {
    let seconds = std::env::var("CELERY_RS_FUZZ_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let started = Instant::now();
    let mut count = 0_u64;
    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let strategy = (
        any::<usize>(),
        proptest::collection::vec(any::<u8>(), 0..4096),
    );
    loop {
        runner
            .run(&strategy, |(selector, bytes)| {
                check(&bytes);
                let mut mutated = seeds[selector % seeds.len()].clone();
                check(&mutated);
                for &[lo, hi, value] in bytes.as_chunks::<3>().0.iter().take(32) {
                    if !mutated.is_empty() {
                        let index = usize::from(u16::from_le_bytes([lo, hi])) % mutated.len();
                        mutated[index] = value;
                    }
                }
                if selector & 1 != 0 && !mutated.is_empty() {
                    mutated.truncate(bytes.len() % mutated.len());
                }
                check(&mutated);
                Ok(())
            })
            .unwrap();
        count += 256;
        if started.elapsed() >= Duration::from_secs(seconds) {
            break;
        }
    }
    eprintln!(
        "fuzz: {count} cases, {:.3} seconds",
        started.elapsed().as_secs_f64()
    );
}
