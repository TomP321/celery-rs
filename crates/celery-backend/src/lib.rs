//! Result backend: redis, rpc, memory (vedi PLAN.md, fase 4).
//!
//! Il client Redis vive qui: `crates/celery-py` ne espone il binding PyO3 e
//! `celery/backends/redis.py` resta la logica del backend del riferimento.

pub mod redis;

