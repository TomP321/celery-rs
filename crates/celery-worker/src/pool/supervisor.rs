//! Single-threaded process supervisor assigning jobs and time limits. Reader threads
//! queue events for Python callbacks on the master thread; zygote loss prevents new forks.

use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::pool::frame::{Frame, FrameKind, read_frame};
use crate::pool::spawn::{SpawnedChild, Spawner};
use crate::wake::Waker;

const TERM_GRACE: Duration = Duration::from_millis(100);

const ZYGOTE_CHECK: Duration = Duration::from_secs(1);

// Matches billiard's recycling exit code for task or memory limits.
const EX_RECYCLE: i32 = 0x9B;

// Used when zygote loss makes queued jobs impossible to execute.
const POOL_UNAVAILABLE: &str = "pool process unavailable: worker pool cannot create children";

pub const SIGKILL: i32 = libc::SIGKILL;
pub const SIGTERM: i32 = libc::SIGTERM;
pub const SIGUSR1: i32 = libc::SIGUSR1;

/// Process pool configuration.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Number of child processes.
    pub processes: usize,
    /// Maximum pending job count.
    pub queue_limit: usize,
}

impl Config {
    pub fn new(processes: usize) -> Self {
        let processes = processes.max(1);
        Self {
            processes,
            // Bound queued jobs and backpressure the master, matching billiard's putlock.
            queue_limit: processes.saturating_mul(4).max(16),
        }
    }
}

#[derive(Debug)]
struct Job {
    id: u64,
    payload: Bytes,
    soft: Option<Duration>,
    hard: Option<Duration>,
}

/// Job specification submitted by the Python master.
pub struct JobSpec {
    pub id: u64,
    pub payload: Bytes,
    pub soft_timeout: Option<Duration>,
    pub hard_timeout: Option<Duration>,
}

/// Event ready for the master.
#[derive(Debug, Clone)]
pub enum PoolEvent {
    /// Child acceptance, matching billiard's ApplyResult._ack.
    Accepted {
        job: u64,
        pid: i32,
        // Child monotonic timestamp, matching billiard's _time_accepted.
        time_accepted: f64,
    },
    /// Child result with a pickled value or ExceptionInfo payload.
    Ready {
        job: u64,
        success: bool,
        payload: Bytes,
    },
    /// Child lost during execution, reported as WorkerLostError.
    WorkerLost { job: u64, pid: i32, exitcode: i32 },
    /// Job terminated by the master, reported as Terminated.
    Terminated { job: u64, pid: i32, exitcode: i32 },
    /// Soft timeout delivered as SIGUSR1.
    SoftTimeout { job: u64, timeout: f64 },
    /// Hard timeout that terminated the child.
    HardTimeout { job: u64, timeout: f64 },
    /// Zygote unavailable; no new children can be created.
    ZygoteLost,
    /// Job impossible to execute after losing the zygote and all children.
    PoolFailure { job: u64, message: String },
}

/// Process pool operation errors.
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("pool is not running")]
    NotRunning,
    #[error("Can't shrink pool. All processes busy!")]
    ShrinkBusy,
    #[error("{0}")]
    Spawn(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Running,
    Closing,
    Terminating,
    Stopped,
}

#[derive(Debug)]
struct Active {
    id: u64,
    soft: Option<Duration>,
    hard: Option<Duration>,
    soft_at: Option<Instant>,
    hard_at: Option<Instant>,
    accepted: bool,
}

#[derive(Debug)]
struct Child {
    pid: i32,
    writer: crate::fds::Tracked<UnixStream>,
    active: Option<Active>,
    // Escalation deadline when SIGTERM does not stop a timed-out child.
    pending_kill: Option<Instant>,

    job_terminated: bool,
    // Intentional exits without an active job must not report worker loss.
    controlled: bool,

    shutdown_sent: bool,

    retire_after_job: bool,
    writes: u64,
}

#[derive(Debug, Default)]
struct State {
    lifecycle: Option<Lifecycle>,
    target: usize,
    queue_limit: usize,
    children: BTreeMap<u32, Child>,
    idle: VecDeque<u32>,
    pending: VecDeque<Job>,
    events: VecDeque<PoolEvent>,
    next_child: u32,
    zygote_lost: bool,
    zygote_shutdown: bool,
    unexpected_exits: usize,
}

impl State {
    fn active_jobs(&self) -> usize {
        self.children
            .values()
            .filter(|child| child.active.is_some())
            .count()
    }

    fn lifecycle(&self) -> Lifecycle {
        self.lifecycle.unwrap_or(Lifecycle::Running)
    }

    fn running(&self) -> bool {
        matches!(self.lifecycle(), Lifecycle::Running | Lifecycle::Closing)
    }

    fn work_left(&self) -> bool {
        !self.pending.is_empty() || self.active_jobs() > 0
    }
}

struct Inner {
    state: Mutex<State>,
    cvar: Condvar,
    waker: Waker,
    spawner: Arc<dyn Spawner>,
    tx: Sender<Command>,
}

enum Command {
    Wake,
    Grow,
    Shrink(usize, Sender<Result<(), PoolError>>),
    TerminateJob { pid: i32, signal: i32 },
    Flush(Sender<Vec<u64>>),
    Restart,
    ResetBackend(Bytes, Sender<()>),
    Close,
    Terminate,
    Abandon,
    Frame { child: u32, frame: Frame },
    Closed { child: u32, exitcode: i32 },
}

/// Process pool controlled by the Python master.
pub struct Pool {
    inner: Arc<Inner>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Pool {
    /// Creates initial children before starting the supervisor thread. The zygote
    /// must already exist; only it may fork.
    pub fn new(config: Config, spawner: Arc<dyn Spawner>, waker: Waker) -> Self {
        let (tx, rx) = mpsc::channel();
        let inner = Arc::new(Inner {
            state: Mutex::new(State {
                lifecycle: Some(Lifecycle::Running),
                target: config.processes,
                queue_limit: config.queue_limit,
                ..State::default()
            }),
            cvar: Condvar::new(),
            waker,
            spawner,
            tx,
        });
        spawn_to_target(&inner);
        let supervisor_inner = Arc::clone(&inner);
        let thread = thread::Builder::new()
            .name("celery-rs-pool".to_owned())
            .spawn(move || supervisor(supervisor_inner, rx))
            .ok();
        Self {
            inner,
            thread: Mutex::new(thread),
        }
    }

    /// Submits a job, blocking on a full queue as billiard's putlock does.
    pub fn submit(&self, spec: JobSpec) -> Result<(), PoolError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        loop {
            if !state.running() {
                return Err(PoolError::NotRunning);
            }
            if state.zygote_lost && state.children.is_empty() {
                state.events.push_back(PoolEvent::PoolFailure {
                    job: spec.id,
                    message: POOL_UNAVAILABLE.to_owned(),
                });
                drop(state);
                self.inner.waker.notify();
                return Ok(());
            }
            if state.pending.len() < state.queue_limit {
                break;
            }
            state = self
                .inner
                .cvar
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.pending.push_back(Job {
            id: spec.id,
            payload: spec.payload,
            soft: spec.soft_timeout,
            hard: spec.hard_timeout,
        });
        drop(state);
        let _ = self.inner.tx.send(Command::Wake);
        Ok(())
    }

    /// Drains ready events without blocking.
    pub fn drain_events(&self) -> Vec<PoolEvent> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.events.drain(..).collect()
    }

    pub fn grow(&self, n: usize) {
        // Update the target immediately, matching billiard. Autoscaling reads it between
        // grow calls and would otherwise create too many children.
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if state.running() {
                state.target = state.target.saturating_add(n);
            }
        }
        let _ = self.inner.tx.send(Command::Grow);
    }

    /// Removes idle children, returning an error if too few are available.
    pub fn shrink(&self, n: usize) -> Result<(), PoolError> {
        let (tx, rx) = mpsc::channel();
        if self.inner.tx.send(Command::Shrink(n, tx)).is_err() {
            return Err(PoolError::NotRunning);
        }
        rx.recv().unwrap_or(Err(PoolError::NotRunning))
    }

    pub fn terminate_job(&self, pid: i32, signal: i32) {
        let _ = self.inner.tx.send(Command::TerminateJob { pid, signal });
    }

    pub fn restart(&self) {
        let _ = self.inner.tx.send(Command::Restart);
    }

    /// Queues backend reset before subsequent jobs, including on busy children.
    pub fn reset_backend(&self, broker: Bytes) {
        let (tx, rx) = mpsc::channel();
        if self
            .inner
            .tx
            .send(Command::ResetBackend(broker, tx))
            .is_ok()
        {
            let _ = rx.recv();
        }
    }

    /// Terminates children after zygote loss, reporting active jobs as WorkerLost
    /// and queued jobs as PoolFailure.
    pub fn abandon_children(&self) {
        let _ = self.inner.tx.send(Command::Abandon);
    }

    /// Discards unaccepted jobs for broker redelivery.
    pub fn flush(&self) -> Vec<u64> {
        let (tx, rx) = mpsc::channel();
        if self.inner.tx.send(Command::Flush(tx)).is_err() {
            return Vec::new();
        }
        rx.recv().unwrap_or_default()
    }

    /// Closes gracefully after queued jobs finish.
    pub fn close(&self) {
        let _ = self.inner.tx.send(Command::Close);
    }

    /// Terminates the pool immediately.
    pub fn terminate(&self) {
        let _ = self.inner.tx.send(Command::Terminate);
    }

    pub fn num_processes(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .target
    }

    pub fn pids(&self) -> Vec<i32> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .children
            .values()
            .map(|child| child.pid)
            .collect()
    }

    pub fn alive(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .children
            .len()
    }

    pub fn stopped(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .lifecycle()
            == Lifecycle::Stopped
    }

    pub fn did_start_ok(&self) -> bool {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.unexpected_exits == 0 && !state.zygote_lost
    }

    pub fn zygote_pid(&self) -> i32 {
        self.inner.spawner.zygote_pid()
    }

    pub fn zygote_lost(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .zygote_lost
    }

    /// Returns total writes and counts for live children.
    pub fn write_stats(&self) -> (u64, Vec<u64>) {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let per_child: Vec<u64> = state.children.values().map(|child| child.writes).collect();
        (per_child.iter().sum(), per_child)
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        if !self.stopped() {
            self.terminate();
        }
        if let Some(handle) = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = handle.join();
        }
    }
}

fn notify(inner: &Inner) {
    let pending = !inner
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .events
        .is_empty();
    if pending {
        inner.waker.notify();
    }
}

fn supervisor(inner: Arc<Inner>, rx: Receiver<Command>) {
    let mut last_zygote_check = Instant::now();
    loop {
        dispatch(&inner);
        finish_if_done(&inner);
        if inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .lifecycle()
            == Lifecycle::Stopped
        {
            break;
        }
        notify(&inner);

        let wait = next_deadline(&inner, last_zygote_check);
        match rx.recv_timeout(wait) {
            Ok(command) => {
                let wake_zygote_check = matches!(
                    command,
                    Command::Wake
                        | Command::Grow
                        | Command::Shrink(..)
                        | Command::Close
                        | Command::Terminate
                );
                handle(&inner, command);
                if wake_zygote_check {
                    last_zygote_check = Instant::now();
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                fire_timers(&inner);
                check_zygote(&inner);
                last_zygote_check = Instant::now();
            }
            Err(RecvTimeoutError::Disconnected) => {
                terminate_all(&inner);
                break;
            }
        }
    }
    notify(&inner);
}

fn handle(inner: &Arc<Inner>, command: Command) {
    match command {
        Command::Wake => {}
        Command::ResetBackend(broker, reply) => {
            let frame = Frame::new(FrameKind::ResetBackend, 0, broker);
            let header = frame.header();
            let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            for child in state.children.values_mut() {
                if !child.shutdown_sent {
                    let result = child
                        .writer
                        .write_all(&header)
                        .and_then(|()| child.writer.write_all(&frame.payload));
                    if result.is_err() {
                        let _ = child.writer.shutdown(Shutdown::Both);
                    }
                }
            }
            let _ = reply.send(());
        }
        Command::Grow => {
            // Pool::grow has already updated the target.
            spawn_to_target(inner);
        }
        Command::Shrink(n, reply) => {
            let result = shrink(inner, n);
            let _ = reply.send(result);
        }
        Command::TerminateJob { pid, signal } => {
            {
                let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(child) = state.children.values_mut().find(|child| child.pid == pid) {
                    child.controlled = true;
                    if child.active.is_some() {
                        child.job_terminated = true;
                    }
                }
            }
            let _ = inner.spawner.signal(pid, signal);
        }
        Command::Flush(reply) => {
            let dropped: Vec<u64> = {
                let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.pending.drain(..).map(|job| job.id).collect()
            };
            let _ = reply.send(dropped);
        }
        Command::Restart => {
            {
                let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
                for child in state.children.values_mut() {
                    if child.active.is_some() {
                        child.retire_after_job = true;
                    } else {
                        child.controlled = true;
                        child.shutdown_sent = true;
                        let _ = child.writer.shutdown(Shutdown::Write);
                    }
                }
                state.idle.clear();
            }
            spawn_to_target(inner);
        }
        Command::Close => {
            let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.lifecycle() == Lifecycle::Running {
                state.lifecycle = Some(Lifecycle::Closing);
            }
            inner.cvar.notify_all();
        }
        Command::Terminate => {
            {
                let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.lifecycle = Some(Lifecycle::Terminating);
                state.pending.clear();
                state.idle.clear();
                let now = Instant::now();
                for child in state.children.values_mut() {
                    child.controlled = true;
                    child.pending_kill = Some(now + TERM_GRACE);
                }
                inner.cvar.notify_all();
            }
            terminate_all(inner);
        }
        Command::Frame { child, frame } => on_frame(inner, child, frame),
        Command::Closed { child, exitcode } => on_closed(inner, child, exitcode),
        Command::Abandon => {
            let pids: Vec<i32> = inner
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .children
                .values()
                .map(|child| child.pid)
                .collect();
            for pid in pids {
                let _ = inner.spawner.signal(pid, SIGKILL);
            }
        }
    }
}

fn terminate_all(inner: &Arc<Inner>) {
    let pids: Vec<i32> = inner
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .children
        .values()
        .map(|child| child.pid)
        .collect();
    for pid in pids {
        let _ = inner.spawner.signal(pid, SIGTERM);
    }
}

fn shrink(inner: &Arc<Inner>, n: usize) -> Result<(), PoolError> {
    let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
    if state.lifecycle() != Lifecycle::Running {
        return Err(PoolError::NotRunning);
    }
    if state.idle.len() < n {
        return Err(PoolError::ShrinkBusy);
    }
    for _ in 0..n {
        let Some(id) = state.idle.pop_front() else {
            return Err(PoolError::ShrinkBusy);
        };
        if let Some(child) = state.children.get_mut(&id) {
            child.controlled = true;
            child.shutdown_sent = true;
            let _ = child.writer.shutdown(Shutdown::Write);
        }
    }
    state.target = state.target.saturating_sub(n);
    inner.cvar.notify_all();
    Ok(())
}

fn dispatch(inner: &Arc<Inner>) {
    let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
    if !state.running() {
        return;
    }
    if state.zygote_lost && state.children.is_empty() && !state.pending.is_empty() {
        let pending: Vec<Job> = state.pending.drain(..).collect();
        for job in pending {
            state.events.push_back(PoolEvent::PoolFailure {
                job: job.id,
                message: POOL_UNAVAILABLE.to_owned(),
            });
        }
        inner.cvar.notify_all();
        drop(state);
        notify(inner);
        return;
    }
    loop {
        let Some(job) = state.pending.pop_front() else {
            break;
        };
        let Some(child_id) = state.idle.pop_front() else {
            state.pending.push_front(job);
            break;
        };
        let failed = {
            let Some(child) = state.children.get_mut(&child_id) else {
                state.pending.push_front(job);
                continue;
            };
            match write_job(child, &job) {
                Ok(()) => {
                    child.writes += 1;
                    child.active = Some(Active {
                        id: job.id,
                        soft: job.soft,
                        hard: job.hard,
                        soft_at: None,
                        hard_at: None,
                        accepted: false,
                    });
                    false
                }
                Err(_) => true,
            }
        };
        if failed {
            state.pending.push_front(job);
            if let Some(child) = state.children.get_mut(&child_id) {
                child.controlled = true;
                let _ = child.writer.shutdown(Shutdown::Both);
            }
        }
    }
    inner.cvar.notify_all();
}

fn write_job(child: &mut Child, job: &Job) -> io::Result<()> {
    let frame = Frame::new(FrameKind::Task, job.id, job.payload.clone());
    let header = frame.header();
    child.writer.write_all(&header)?;
    child.writer.write_all(&job.payload)?;
    child.writer.flush()
}

fn on_frame(inner: &Arc<Inner>, child_id: u32, frame: Frame) {
    let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
    match frame.kind {
        FrameKind::Ack => {
            let time_accepted = frame
                .payload
                .get(..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(f64::from_be_bytes)
                .unwrap_or(0.0);
            let pid;
            {
                let Some(child) = state.children.get_mut(&child_id) else {
                    return;
                };
                let Some(active) = child.active.as_mut() else {
                    return;
                };
                if active.id != frame.job || active.accepted {
                    return;
                }
                let now = Instant::now();
                active.accepted = true;
                active.soft_at = active.soft.map(|timeout| now + timeout);
                active.hard_at = active.hard.map(|timeout| now + timeout);
                pid = child.pid;
            }
            state.events.push_back(PoolEvent::Accepted {
                job: frame.job,
                pid,
                time_accepted,
            });
        }
        FrameKind::Ready => {
            let flags = frame.payload.first().copied().unwrap_or(0);
            let success = flags & 1 != 0;
            let retire = flags & 2 != 0;
            let payload = frame.payload.slice(1..);
            let mut shutdown = false;
            {
                let Some(child) = state.children.get_mut(&child_id) else {
                    return;
                };
                let Some(active) = child.active.as_ref() else {
                    return;
                };
                if active.id != frame.job {
                    return;
                }
                child.active = None;
                if retire || child.retire_after_job {
                    // A child at its task or memory limit retires itself and must receive no new jobs.
                    child.controlled = true;
                    child.shutdown_sent = true;
                    shutdown = true;
                }
            }
            if shutdown {
                if let Some(child) = state.children.get_mut(&child_id) {
                    let _ = child.writer.shutdown(Shutdown::Write);
                }
            } else {
                state.idle.push_back(child_id);
            }
            state.events.push_back(PoolEvent::Ready {
                job: frame.job,
                success,
                payload,
            });
            inner.cvar.notify_all();
        }
        FrameKind::Task | FrameKind::ResetBackend => {}
    }
}

fn on_closed(inner: &Arc<Inner>, child_id: u32, exitcode: i32) {
    let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(child) = state.children.remove(&child_id) else {
        return;
    };
    state.idle.retain(|id| *id != child_id);

    if let Some(active) = child.active {
        if child.job_terminated {
            state.events.push_back(PoolEvent::Terminated {
                job: active.id,
                pid: child.pid,
                exitcode,
            });
        } else {
            state.events.push_back(PoolEvent::WorkerLost {
                job: active.id,
                pid: child.pid,
                exitcode,
            });
            state.unexpected_exits += 1;
        }
    } else if exitcode != 0 && exitcode != EX_RECYCLE && !child.controlled {
        state.unexpected_exits += 1;
    }

    let lifecycle = state.lifecycle();
    let children = state.children.len();
    let work_left = state.work_left();
    let zygote_lost = state.zygote_lost;
    let target = state.target;
    drop(state);

    match lifecycle {
        Lifecycle::Running => {
            if !zygote_lost {
                spawn_to_target(inner);
            } else if children == 0 {
                fail_unrunnable(inner);
            }
        }
        Lifecycle::Closing => {
            if work_left {
                if !zygote_lost && children < target {
                    spawn_to_target(inner);
                } else if zygote_lost && children == 0 {
                    fail_unrunnable(inner);
                }
            }
        }
        Lifecycle::Terminating | Lifecycle::Stopped => {}
    }
    finish_if_done(inner);
    notify(inner);
}

fn fail_unrunnable(inner: &Arc<Inner>) {
    let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
    if state.children.is_empty() {
        let pending: Vec<Job> = state.pending.drain(..).collect();
        for job in pending {
            state.events.push_back(PoolEvent::PoolFailure {
                job: job.id,
                message: POOL_UNAVAILABLE.to_owned(),
            });
        }
    }
}

fn spawn_to_target(inner: &Arc<Inner>) {
    loop {
        let spawn = {
            let state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            let allowed = state.lifecycle() == Lifecycle::Running
                || (state.lifecycle() == Lifecycle::Closing && state.work_left());
            allowed && !state.zygote_lost && state.children.len() < state.target
        };
        if !spawn {
            return;
        }
        match inner.spawner.spawn() {
            Ok(SpawnedChild { pid, stream }) => {
                let reader = match stream.try_clone() {
                    Ok(reader) => reader,
                    Err(error) => {
                        let _ = inner.spawner.signal(pid, SIGKILL);
                        mark_zygote_lost(inner, Some(error));
                        return;
                    }
                };
                let id = {
                    let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
                    let id = state.next_child;
                    state.next_child += 1;
                    state.children.insert(
                        id,
                        Child {
                            pid,
                            writer: stream,
                            active: None,
                            pending_kill: None,
                            job_terminated: false,
                            controlled: false,
                            shutdown_sent: false,
                            retire_after_job: false,
                            writes: 0,
                        },
                    );
                    state.idle.push_back(id);
                    id
                };
                spawn_reader(
                    id,
                    pid,
                    crate::fds::Tracked::new(reader),
                    inner.spawner.clone(),
                    inner.tx.clone(),
                );
            }
            Err(error) => {
                mark_zygote_lost(inner, Some(error));
                return;
            }
        }
    }
}

fn mark_zygote_lost(inner: &Arc<Inner>, _error: Option<io::Error>) {
    let empty = {
        let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.zygote_lost {
            state.zygote_lost = true;
            state.events.push_back(PoolEvent::ZygoteLost);
        }
        state.children.is_empty()
    };
    if empty {
        fail_unrunnable(inner);
    }
    notify(inner);
}

fn spawn_reader(
    child_id: u32,
    pid: i32,
    reader: crate::fds::Tracked<UnixStream>,
    spawner: Arc<dyn Spawner>,
    tx: Sender<Command>,
) {
    let _ = thread::Builder::new()
        .name(format!("celery-rs-pool-child-{child_id}"))
        .spawn(move || {
            let mut reader = reader;
            while let Ok(Some(frame)) = read_frame(&mut *reader) {
                if tx
                    .send(Command::Frame {
                        child: child_id,
                        frame,
                    })
                    .is_err()
                {
                    return;
                }
            }
            let exitcode = spawner.reap(pid);
            let _ = tx.send(Command::Closed {
                child: child_id,
                exitcode,
            });
        });
}

fn next_deadline(inner: &Arc<Inner>, last_zygote_check: Instant) -> Duration {
    let now = Instant::now();
    let state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
    let mut deadline = last_zygote_check + ZYGOTE_CHECK;
    for child in state.children.values() {
        if let Some(active) = child.active.as_ref() {
            for at in [active.soft_at, active.hard_at].into_iter().flatten() {
                if at < deadline {
                    deadline = at;
                }
            }
        }
        if let Some(at) = child.pending_kill.filter(|at| *at < deadline) {
            deadline = at;
        }
    }
    deadline
        .saturating_duration_since(now)
        .max(Duration::from_millis(1))
}

fn fire_timers(inner: &Arc<Inner>) {
    let now = Instant::now();
    let mut signals: Vec<(i32, i32)> = Vec::new();
    let mut events: Vec<PoolEvent> = Vec::new();
    {
        let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
        for child in state.children.values_mut() {
            let mut soft_hit = None;
            let mut hard_hit = None;
            let mut job_id = None;
            if let Some(active) = child.active.as_ref() {
                job_id = Some(active.id);
                if active.soft_at.is_some_and(|at| at <= now) {
                    soft_hit = active.soft;
                }
                if active.hard_at.is_some_and(|at| at <= now) {
                    hard_hit = active.hard;
                }
            }
            if let Some(timeout) = soft_hit {
                if let Some(active) = child.active.as_mut() {
                    active.soft_at = None;
                }
                if let Some(job) = job_id {
                    events.push(PoolEvent::SoftTimeout {
                        job,
                        timeout: timeout.as_secs_f64(),
                    });
                }
                signals.push((child.pid, SIGUSR1));
            }
            if let Some(timeout) = hard_hit {
                child.active = None;
                child.controlled = true;
                child.pending_kill = Some(now + TERM_GRACE);
                if let Some(job) = job_id {
                    events.push(PoolEvent::HardTimeout {
                        job,
                        timeout: timeout.as_secs_f64(),
                    });
                }
                signals.push((child.pid, SIGTERM));
            }
            if child.pending_kill.is_some_and(|at| at <= now) {
                child.pending_kill = None;
                signals.push((child.pid, SIGKILL));
            }
        }
        state.events.extend(events);
        inner.cvar.notify_all();
    }
    for (pid, signal) in signals {
        let _ = inner.spawner.signal(pid, signal);
    }
}

fn check_zygote(inner: &Arc<Inner>) {
    let lost = {
        let state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.zygote_lost
    };
    if lost {
        return;
    }
    if !inner.spawner.zygote_alive() {
        mark_zygote_lost(inner, None);
    }
}

fn finish_if_done(inner: &Arc<Inner>) {
    let mut state = inner.state.lock().unwrap_or_else(PoisonError::into_inner);
    match state.lifecycle() {
        Lifecycle::Closing => {
            let work_left = state.work_left();
            if !work_left {
                for child in state.children.values_mut() {
                    if !child.shutdown_sent {
                        child.shutdown_sent = true;
                        let _ = child.writer.shutdown(Shutdown::Write);
                    }
                }
            }
            if state.children.is_empty() && !work_left {
                state.lifecycle = Some(Lifecycle::Stopped);
            }
        }
        Lifecycle::Terminating => {
            if state.children.is_empty() {
                state.lifecycle = Some(Lifecycle::Stopped);
            }
        }
        Lifecycle::Running | Lifecycle::Stopped => {}
    }
    let stopped = state.lifecycle() == Lifecycle::Stopped && !state.zygote_shutdown;
    if stopped {
        state.zygote_shutdown = true;
    }
    drop(state);
    if stopped {
        inner.spawner.shutdown();
        let pid = inner.spawner.zygote_pid();
        if pid > 0 {
            let _ = inner.spawner.reap(pid);
        }
        notify(inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    struct FakeExit {
        code: Mutex<Option<i32>>,
        cvar: Condvar,
    }

    impl FakeExit {
        fn new() -> Self {
            Self {
                code: Mutex::new(None),
                cvar: Condvar::new(),
            }
        }

        fn set(&self, code: i32) {
            let mut slot = self.code.lock().expect("exit");
            if slot.is_none() {
                *slot = Some(code);
                self.cvar.notify_all();
            }
        }

        fn get(&self) -> i32 {
            let mut slot = self.code.lock().expect("exit");
            while slot.is_none() {
                slot = self.cvar.wait(slot).expect("exit wait");
            }
            (*slot).unwrap_or(-1)
        }
    }

    struct FakeControl {
        soft: AtomicBool,
        kill: AtomicI32,
        exit: Arc<FakeExit>,
        resets: Mutex<Vec<Vec<u8>>>,
    }

    impl FakeControl {
        fn new() -> Self {
            Self {
                soft: AtomicBool::new(false),
                kill: AtomicI32::new(0),
                exit: Arc::new(FakeExit::new()),
                resets: Mutex::new(Vec::new()),
            }
        }

        fn kill_signal(&self) -> Option<i32> {
            let signal = self.kill.load(Ordering::Acquire);
            (signal != 0).then_some(signal)
        }
    }

    struct FakeChild {
        pid: i32,
        control: Arc<FakeControl>,
    }

    struct FakeState {
        next_pid: i32,
        children: Vec<FakeChild>,
        signals: Vec<(i32, i32)>,
        spawned: usize,
        shutdown: bool,
    }

    struct FakeSpawner {
        state: Mutex<FakeState>,
        alive: AtomicBool,
    }

    impl FakeSpawner {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(FakeState {
                    next_pid: 1000,
                    children: Vec::new(),
                    signals: Vec::new(),
                    spawned: 0,
                    shutdown: false,
                }),
                alive: AtomicBool::new(true),
            })
        }

        fn kill(&self, pid: i32, code: i32) {
            let state = self.state.lock().expect("state");
            if let Some(child) = state.children.iter().find(|child| child.pid == pid) {
                child.control.kill.store(-code, Ordering::Release);
                child.control.exit.set(code);
            }
        }

        fn signals(&self) -> Vec<(i32, i32)> {
            self.state.lock().expect("state").signals.clone()
        }

        fn spawned(&self) -> usize {
            self.state.lock().expect("state").spawned
        }
    }

    fn write_fake_frame(stream: &mut UnixStream, frame: &Frame) -> io::Result<()> {
        stream.write_all(&frame.header())?;
        stream.write_all(&frame.payload)
    }

    fn fake_child_loop(mut stream: UnixStream, control: Arc<FakeControl>) {
        stream
            .set_read_timeout(Some(Duration::from_millis(10)))
            .expect("timeout");
        let mut buffer: Vec<u8> = Vec::new();
        'outer: loop {
            if control.kill_signal().is_some() {
                break;
            }
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(_) => break,
            }
            while buffer.len() >= 4 {
                let total =
                    u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
                if !(9..=1 << 20).contains(&total) {
                    break 'outer;
                }
                if buffer.len() < 4 + total {
                    break;
                }
                let kind = buffer[4];
                let job = u64::from_be_bytes(buffer[5..13].try_into().expect("job"));
                let payload = buffer[13..4 + total].to_vec();
                buffer.drain(..4 + total);
                if kind == 4 {
                    control.resets.lock().expect("resets").push(payload);
                    continue;
                }
                if kind != 1 {
                    continue;
                }
                let mode = payload.first().copied().unwrap_or(0);
                let ack = Frame::new(
                    FrameKind::Ack,
                    job,
                    Bytes::copy_from_slice(&0f64.to_be_bytes()),
                );
                if write_fake_frame(&mut stream, &ack).is_err() {
                    break 'outer;
                }
                match mode {
                    0 => {
                        let ready =
                            Frame::new(FrameKind::Ready, job, Bytes::from_static(b"\x01ok"));
                        if write_fake_frame(&mut stream, &ready).is_err() {
                            break 'outer;
                        }
                    }
                    1 => {
                        while control.kill_signal().is_none() {
                            thread::sleep(Duration::from_millis(5));
                        }
                        break 'outer;
                    }
                    2 => {
                        while control.kill_signal().is_none()
                            && !control.soft.swap(false, Ordering::AcqRel)
                        {
                            thread::sleep(Duration::from_millis(2));
                        }
                        if control.kill_signal().is_some() {
                            break 'outer;
                        }
                        let ready =
                            Frame::new(FrameKind::Ready, job, Bytes::from_static(b"\x00fail"));
                        if write_fake_frame(&mut stream, &ready).is_err() {
                            break 'outer;
                        }
                    }
                    3 => {
                        thread::sleep(Duration::from_millis(30));
                        let ready =
                            Frame::new(FrameKind::Ready, job, Bytes::from_static(b"\x01ok"));
                        if write_fake_frame(&mut stream, &ready).is_err() {
                            break 'outer;
                        }
                    }
                    4 => {
                        let ready =
                            Frame::new(FrameKind::Ready, job, Bytes::from_static(b"\x03ok"));
                        if write_fake_frame(&mut stream, &ready).is_err() {
                            break 'outer;
                        }
                        break 'outer;
                    }
                    _ => {}
                }
            }
        }
        control
            .exit
            .set(control.kill_signal().map(|signal| -signal).unwrap_or(0));
    }

    impl Spawner for FakeSpawner {
        fn spawn(&self) -> io::Result<SpawnedChild> {
            if !self.alive.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "zygote unavailable",
                ));
            }
            let (master, peer) = UnixStream::pair()?;
            let control = Arc::new(FakeControl::new());
            let pid = {
                let mut state = self.state.lock().expect("state");
                state.next_pid += 1;
                let pid = state.next_pid;
                state.spawned += 1;
                state.children.push(FakeChild {
                    pid,
                    control: Arc::clone(&control),
                });
                pid
            };
            thread::spawn(move || fake_child_loop(peer, control));
            Ok(SpawnedChild {
                pid,
                stream: crate::fds::Tracked::new(master),
            })
        }

        fn signal(&self, pid: i32, signal: i32) -> io::Result<()> {
            let mut state = self.state.lock().expect("state");
            state.signals.push((pid, signal));
            if let Some(child) = state.children.iter().find(|child| child.pid == pid) {
                match signal {
                    SIGUSR1 => child.control.soft.store(true, Ordering::Release),
                    SIGTERM | SIGKILL => {
                        child.control.kill.store(signal, Ordering::Release);
                        child.control.exit.set(-signal);
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        fn reap(&self, pid: i32) -> i32 {
            let exit = {
                let state = self.state.lock().expect("state");
                state
                    .children
                    .iter()
                    .find(|child| child.pid == pid)
                    .map(|child| Arc::clone(&child.control.exit))
            };
            exit.map(|exit| exit.get()).unwrap_or(-1)
        }

        fn shutdown(&self) {
            self.state.lock().expect("state").shutdown = true;
        }

        fn zygote_alive(&self) -> bool {
            self.alive.load(Ordering::Acquire)
        }

        fn zygote_pid(&self) -> i32 {
            4242
        }
    }

    fn spec(id: u64, mode: u8) -> JobSpec {
        JobSpec {
            id,
            payload: Bytes::copy_from_slice(&[mode]),
            soft_timeout: None,
            hard_timeout: None,
        }
    }

    fn collect(pool: &Pool, wanted: usize, timeout: Duration) -> Vec<PoolEvent> {
        let deadline = Instant::now() + timeout;
        let mut events = Vec::new();
        while events.len() < wanted && Instant::now() < deadline {
            events.extend(pool.drain_events());
            if events.len() < wanted {
                thread::sleep(Duration::from_millis(2));
            }
        }
        events
    }

    fn kinds(events: &[PoolEvent]) -> Vec<&'static str> {
        events
            .iter()
            .map(|event| match event {
                PoolEvent::Accepted { .. } => "accepted",
                PoolEvent::Ready { .. } => "ready",
                PoolEvent::WorkerLost { .. } => "lost",
                PoolEvent::Terminated { .. } => "terminated",
                PoolEvent::SoftTimeout { .. } => "soft",
                PoolEvent::HardTimeout { .. } => "hard",
                PoolEvent::ZygoteLost => "zygote-lost",
                PoolEvent::PoolFailure { .. } => "failure",
            })
            .collect()
    }

    #[test]
    fn backend_reset_reaches_every_child_without_submitting_jobs() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(2), spawner.clone(), Waker::new());
        let broker = Bytes::from_static(b"redis://localhost/1");
        pool.reset_backend(broker.clone());
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let done = spawner
                .state
                .lock()
                .expect("state")
                .children
                .iter()
                .all(|child| {
                    child.control.resets.lock().expect("resets").as_slice() == [broker.to_vec()]
                });
            if done {
                break;
            }
            assert!(Instant::now() < deadline, "reset not received");
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(pool.write_stats().0, 0);
        assert!(pool.drain_events().is_empty());
        pool.terminate();
    }

    #[test]
    fn job_roundtrip() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(2), spawner.clone(), Waker::new());
        pool.submit(spec(1, 0)).expect("submit");
        let events = collect(&pool, 3, Duration::from_secs(2));
        // Filter by job because the next acceptance can arrive in the same iteration.
        let mut seen = Vec::new();
        for event in events {
            if let PoolEvent::Accepted { job, .. } = event {
                assert_eq!(job, 1);
                seen.push("accepted");
            } else if let PoolEvent::Ready {
                job,
                success,
                payload,
            } = event
            {
                assert_eq!(job, 1);
                assert!(success);
                assert_eq!(&payload[..], b"ok");
                seen.push("ready");
                break;
            }
        }
        assert_eq!(seen, ["accepted", "ready"]);
        assert_eq!(pool.alive(), 2);
        let (total, per_child) = pool.write_stats();
        assert_eq!(total, 1);
        assert_eq!(per_child.len(), 2);
        pool.close();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    fn wait_stopped(pool: &Pool, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pool.stopped() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        pool.stopped()
    }

    #[test]
    fn soft_then_ready() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner.clone(), Waker::new());
        let mut job = spec(7, 2);
        job.soft_timeout = Some(Duration::from_millis(30));
        job.hard_timeout = Some(Duration::from_secs(5));
        pool.submit(job).expect("submit");
        let events = collect(&pool, 3, Duration::from_secs(2));
        let kinds = kinds(&events);
        assert_eq!(kinds, ["accepted", "soft", "ready"]);
        assert!(spawner.signals().iter().any(|(_, sig)| *sig == SIGUSR1));
        if let PoolEvent::Ready { success, .. } = &events[2] {
            assert!(!success);
        }
        pool.close();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn hard_timeout_kills_and_replaces() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner.clone(), Waker::new());
        let pid = pool.pids()[0];
        let mut job = spec(9, 1);
        job.hard_timeout = Some(Duration::from_millis(30));
        pool.submit(job).expect("submit");
        let events = collect(&pool, 2, Duration::from_secs(2));
        assert_eq!(kinds(&events), ["accepted", "hard"]);
        assert!(
            spawner
                .signals()
                .iter()
                .any(|(who, sig)| *who == pid && *sig == SIGTERM)
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut replaced = false;
        while Instant::now() < deadline {
            let pids = pool.pids();
            if pids.len() == 1 && pids[0] != pid {
                replaced = true;
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert!(replaced, "child not replaced: {:?}", pool.pids());
        assert_eq!(spawner.spawned(), 2);
        pool.close();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn worker_lost_reports_exit_code() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner.clone(), Waker::new());
        let pid = pool.pids()[0];
        pool.submit(spec(11, 1)).expect("submit");
        let accepted = collect(&pool, 1, Duration::from_secs(2));
        assert_eq!(kinds(&accepted), ["accepted"]);
        spawner.kill(pid, -SIGKILL);
        let events = collect(&pool, 1, Duration::from_secs(2));
        match &events[0] {
            PoolEvent::WorkerLost {
                job,
                pid: who,
                exitcode,
            } => {
                assert_eq!(*job, 11);
                assert_eq!(*who, pid);
                assert_eq!(*exitcode, -SIGKILL);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert_eq!(spawner.spawned(), 2);
        pool.close();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn terminated_job_is_not_worker_lost() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner.clone(), Waker::new());
        let pid = pool.pids()[0];
        pool.submit(spec(12, 1)).expect("submit");
        assert_eq!(
            kinds(&collect(&pool, 1, Duration::from_secs(2))),
            ["accepted"]
        );
        pool.terminate_job(pid, SIGTERM);
        let events = collect(&pool, 1, Duration::from_secs(2));
        assert_eq!(kinds(&events), ["terminated"]);
        match &events[0] {
            PoolEvent::Terminated { exitcode, .. } => assert_eq!(*exitcode, -SIGTERM),
            other => panic!("unexpected event: {other:?}"),
        }
        pool.close();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn grow_and_shrink() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner, Waker::new());
        assert_eq!(pool.pids().len(), 1);
        pool.grow(2);
        // Expose the target immediately so autoscaling does not overshoot during spawning.
        assert_eq!(pool.num_processes(), 3);
        let deadline = Instant::now() + Duration::from_secs(2);
        while pool.pids().len() < 3 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(pool.num_processes(), 3);
        assert_eq!(pool.pids().len(), 3);
        pool.shrink(2).expect("shrink");
        assert_eq!(pool.num_processes(), 1);
        let deadline = Instant::now() + Duration::from_secs(2);
        while pool.pids().len() > 1 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(pool.pids().len(), 1);
        pool.close();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn shrink_with_busy_pool_fails() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner, Waker::new());
        pool.submit(spec(13, 1)).expect("submit");
        assert_eq!(
            kinds(&collect(&pool, 1, Duration::from_secs(2))),
            ["accepted"]
        );
        match pool.shrink(1) {
            Err(PoolError::ShrinkBusy) => {}
            other => panic!("expected ShrinkBusy, got {other:?}"),
        }
        pool.terminate();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn flush_drops_only_pending_jobs() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner, Waker::new());
        pool.submit(spec(14, 1)).expect("submit");
        assert_eq!(
            kinds(&collect(&pool, 1, Duration::from_secs(2))),
            ["accepted"]
        );
        pool.submit(spec(15, 1)).expect("submit");
        pool.submit(spec(16, 1)).expect("submit");
        let dropped = pool.flush();
        assert_eq!(dropped, vec![15, 16]);
        pool.terminate();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn close_finishes_running_jobs() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(2), spawner.clone(), Waker::new());
        pool.submit(spec(17, 3)).expect("submit");
        pool.submit(spec(18, 3)).expect("submit");
        pool.close();
        let events = collect(&pool, 4, Duration::from_secs(3));
        assert_eq!(kinds(&events), ["accepted", "accepted", "ready", "ready"]);
        assert!(wait_stopped(&pool, Duration::from_secs(3)));
        assert!(spawner.state.lock().expect("state").shutdown);
    }

    #[test]
    fn abandon_children_reports_worker_lost() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner.clone(), Waker::new());
        pool.submit(spec(30, 1)).expect("submit");
        let events = collect(&pool, 1, Duration::from_secs(2));
        assert_eq!(kinds(&events), ["accepted"]);
        let pid = pool.pids()[0];

        spawner.alive.store(false, Ordering::Release);
        pool.abandon_children();
        let events = collect(&pool, 1, Duration::from_secs(2));
        assert!(kinds(&events).contains(&"lost"), "{events:?}");
        assert!(spawner.signals().contains(&(pid, SIGKILL)));
    }

    #[test]
    fn zygote_lost_fails_unrunnable_jobs() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner.clone(), Waker::new());
        spawner.alive.store(false, Ordering::Release);

        pool.grow(1);
        let events = collect(&pool, 1, Duration::from_secs(2));
        assert_eq!(kinds(&events), ["zygote-lost"]);
        let pid = pool.pids()[0];
        pool.submit(spec(20, 1)).expect("submit");
        pool.submit(spec(21, 1)).expect("submit");
        let events = collect(&pool, 1, Duration::from_secs(2));
        assert_eq!(kinds(&events), ["accepted"]);
        spawner.kill(pid, -SIGKILL);
        let events = collect(&pool, 2, Duration::from_secs(2));
        let mut names = kinds(&events);
        names.sort_unstable();
        assert_eq!(names, ["failure", "lost"]);

        pool.submit(spec(22, 0)).expect("submit");
        let events = collect(&pool, 1, Duration::from_secs(2));
        assert_eq!(kinds(&events), ["failure"]);
        match &events[0] {
            PoolEvent::PoolFailure { job, .. } => assert_eq!(*job, 22),
            other => panic!("unexpected event: {other:?}"),
        }
        pool.terminate();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }

    #[test]
    fn retiring_child_is_not_reused() {
        let spawner = FakeSpawner::new();
        let pool = Pool::new(Config::new(1), spawner, Waker::new());
        let first = pool.pids()[0];
        pool.submit(spec(40, 4)).expect("submit");
        let events = collect(&pool, 2, Duration::from_secs(2));
        assert_eq!(kinds(&events), ["accepted", "ready"]);

        pool.submit(spec(41, 0)).expect("submit");
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut pids = Vec::new();
        while Instant::now() < deadline {
            pids = pool.pids();
            if pids.len() == 1 && pids[0] != first {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(pids.len(), 1);
        assert_ne!(pids[0], first);
        let events = collect(&pool, 2, Duration::from_secs(2));
        match &events[1] {
            PoolEvent::Ready { job, success, .. } => {
                assert_eq!(*job, 41);
                assert!(success);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        pool.close();
        assert!(wait_stopped(&pool, Duration::from_secs(2)));
    }
}
