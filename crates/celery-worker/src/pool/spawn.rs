//! Creates and controls children through a single-threaded Python zygote forked
//! before Rust threads start. The supervisor never forks; it passes socketpair descriptors via SCM_RIGHTS.

use std::io::{self, Read, Write};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// New child PID and master socketpair endpoint.
pub struct SpawnedChild {
    pub pid: i32,
    pub stream: crate::fds::Tracked<UnixStream>,
}

/// Creates, signals and reaps child processes.
pub trait Spawner: Send + Sync {
    /// Creates a child and returns its master endpoint.
    fn spawn(&self) -> io::Result<SpawnedChild>;

    /// Sends a process signal as os.kill does.
    fn signal(&self, pid: i32, signal: i32) -> io::Result<()>;

    /// Waits for process exit and returns a Popen-style code, negative for signal termination.
    fn reap(&self, pid: i32) -> i32;

    /// Closes the control channel; the zygote exits after its children finish.
    fn shutdown(&self);

    /// Returns whether the zygote is alive.
    fn zygote_alive(&self) -> bool;

    /// Returns the zygote PID, or zero when absent.
    fn zygote_pid(&self) -> i32;
}

const CONTROL_SPAWN: u8 = 1;
const CONTROL_SHUTDOWN: u8 = 2;
const CONTROL_REAP: u8 = 3;
const CONTROL_FRAME_LEN: usize = 9;

/// Spawner communicating with a Python zygote process.
pub struct ZygoteSpawner {
    control: Mutex<crate::fds::Tracked<UnixStream>>,
    zygote_pid: i32,
    maxtasks: u32,
    max_memory_kb: u32,
    lost: AtomicBool,
}

impl ZygoteSpawner {
    /// Duplicates the descriptor, leaving the original owned by the caller.
    pub fn new(
        fd: RawFd,
        zygote_pid: i32,
        maxtasks: Option<u32>,
        max_memory_kb: Option<u32>,
    ) -> io::Result<Self> {
        // SAFETY: dup returns a new independent descriptor owned exclusively here;
        // Python retains ownership of the original.
        let dup = unsafe { libc::dup(fd) };
        if dup < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: dup is a valid, exclusively owned descriptor.
        let control = unsafe { UnixStream::from_raw_fd(dup) };
        control.set_nonblocking(false)?;
        Ok(Self {
            control: Mutex::new(crate::fds::Tracked::new(control)),
            zygote_pid,
            maxtasks: maxtasks.unwrap_or(0),
            max_memory_kb: max_memory_kb.unwrap_or(0),
            lost: AtomicBool::new(false),
        })
    }

    fn send_control(&self, kind: u8, fd: Option<RawFd>) -> io::Result<()> {
        let mut payload = [0u8; CONTROL_FRAME_LEN];
        payload[0] = kind;
        payload[1..5].copy_from_slice(&self.maxtasks.to_be_bytes());
        payload[5..9].copy_from_slice(&self.max_memory_kb.to_be_bytes());
        let stream = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match fd {
            Some(fd) => send_with_fd(&stream, &payload, fd),
            None => {
                let mut stream = stream;
                stream.write_all(&payload)
            }
        }
    }

    fn send_reap(&self, pid: i32) -> io::Result<i32> {
        let mut payload = [0u8; CONTROL_FRAME_LEN];
        payload[0] = CONTROL_REAP;
        payload[1..5].copy_from_slice(&(pid as u32).to_be_bytes());
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        control.write_all(&payload)?;
        Self::read_reply(&mut control)
    }

    fn read_reply(stream: &mut UnixStream) -> io::Result<i32> {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len)?;
        let length = u32::from_be_bytes(len) as usize;
        if length > 4096 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zygote response too large",
            ));
        }
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body)?;
        if body.len() < 9 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "short zygote response",
            ));
        }
        let status = body[0];
        let mut pid = [0u8; 4];
        pid.copy_from_slice(&body[1..5]);
        let pid = i32::from_be_bytes(pid);
        let mut errlen = [0u8; 4];
        errlen.copy_from_slice(&body[5..9]);
        let errlen = u32::from_be_bytes(errlen) as usize;
        let message =
            String::from_utf8_lossy(&body[9..9 + errlen.min(body.len() - 9)]).into_owned();
        if status != 0 {
            return Err(io::Error::other(if message.is_empty() {
                "zygote failed to create child".to_owned()
            } else {
                message
            }));
        }
        Ok(pid)
    }
}

impl Spawner for ZygoteSpawner {
    fn spawn(&self) -> io::Result<SpawnedChild> {
        if self.lost.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pool zygote unavailable",
            ));
        }
        let mut fds = [0 as RawFd; 2];
        // SAFETY: fds is an array of two integers, both initialized by socketpair on success.
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both descriptors were just created and are exclusively owned here.
        let master = crate::fds::Tracked::new(unsafe { UnixStream::from_raw_fd(fds[0]) });
        // SAFETY: this descriptor is newly created and owned here; it closes after transfer
        // unless the zygote adopts it.
        let child = crate::fds::Tracked::new(unsafe { OwnedFd::from_raw_fd(fds[1]) });
        use std::os::fd::AsRawFd;

        if let Err(error) = self.send_control(CONTROL_SPAWN, Some(child.as_raw_fd())) {
            self.lost.store(true, Ordering::Release);
            return Err(error);
        }
        // The zygote duplicated the descriptor; our copy is no longer needed.
        drop(child);

        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match Self::read_reply(&mut control) {
            Ok(pid) => Ok(SpawnedChild {
                pid,
                stream: master,
            }),
            Err(error) => {
                self.lost.store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    fn signal(&self, pid: i32, signal: i32) -> io::Result<()> {
        // SAFETY: kill accesses no process memory; the PID comes from the spawn protocol or Python master.
        let rc = unsafe { libc::kill(pid, signal) };
        if rc != 0 {
            let error = io::Error::last_os_error();
            // ESRCH means the process already exited, which is harmless to the caller.
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(());
            }
            return Err(error);
        }
        Ok(())
    }

    fn reap(&self, pid: i32) -> i32 {
        if pid == self.zygote_pid {
            // The master can reap the zygote because it is a direct child.
            return waitpid(pid);
        }
        // Only the zygote can waitpid its children; request their exit status through it.
        self.send_reap(pid).unwrap_or(-1)
    }

    fn shutdown(&self) {
        if self.zygote_pid <= 0 {
            return;
        }
        let _ = self.send_control(CONTROL_SHUTDOWN, None);
    }

    fn zygote_alive(&self) -> bool {
        if self.lost.load(Ordering::Acquire) || self.zygote_pid <= 0 {
            return false;
        }
        // SAFETY: kill(pid, 0) checks existence without sending a signal or accessing memory.
        let rc = unsafe { libc::kill(self.zygote_pid, 0) };
        rc == 0
    }

    fn zygote_pid(&self) -> i32 {
        self.zygote_pid
    }
}

// Sends a payload and descriptor with SCM_RIGHTS in one sendmsg call.
fn send_with_fd(stream: &UnixStream, payload: &[u8], fd: RawFd) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr().cast_mut().cast(),
        iov_len: payload.len(),
    };
    // SAFETY: CMSG_SPACE is a pure libc calculation; four bytes hold one descriptor integer.
    let space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut control = [0u8; 64];
    debug_assert!(space <= control.len());
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = space;
    // SAFETY: the control buffer fits a cmsghdr and one descriptor.
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(io::Error::other("null SCM_RIGHTS control buffer"));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as u32) as _;
        std::ptr::copy_nonoverlapping(
            &fd as *const RawFd as *const u8,
            libc::CMSG_DATA(cmsg),
            std::mem::size_of::<RawFd>(),
        );
    }
    let sent = loop {
        // SAFETY: msg points to iov and control, both alive throughout the call.
        let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
        if sent >= 0 {
            break sent;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    if (sent as usize) != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "partial zygote command write",
        ));
    }
    Ok(())
}

fn waitpid(pid: i32) -> i32 {
    loop {
        let mut status = 0;
        // SAFETY: waitpid writes to the valid local status integer and waits for a direct child.
        let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
        if rc < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return -1;
        }
        if libc::WIFEXITED(status) {
            return libc::WEXITSTATUS(status);
        }
        if libc::WIFSIGNALED(status) {
            return -libc::WTERMSIG(status);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicates_the_control_fd() {
        let (a, _b) = UnixStream::pair().expect("pair");
        use std::os::fd::AsRawFd;
        let spawner = ZygoteSpawner::new(a.as_raw_fd(), 0, None, None).expect("spawner");

        drop(a);
        assert!(!spawner.zygote_alive());
    }
}
