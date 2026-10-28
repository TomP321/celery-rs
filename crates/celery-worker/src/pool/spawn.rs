//! Creazione e controllo dei figli del pool.
//!
//! Il supervisore non fa mai `fork()`: chiede i figli allo zygote Python
//! (processo a thread singolo forkato dal master prima che partano i thread
//! Rust) e riceve l'estremità master del `socketpair` via `SCM_RIGHTS`.
//! Il protocollo di controllo è:
//!
//! * master → zygote: `[u8 tipo][u32 a][u32 b]`
//!   (tipo 1 = crea figlio, con un fd in `SCM_RIGHTS`; tipo 2 = esci;
//!   tipo 3 = raccogli il figlio `a` e restituiscine l'exit code);
//! * zygote → master: `[u32 lunghezza][u8 stato][i32 pid_o_exitcode]
//!   [u32 lunghezza errore][errore]`.
//!
//! Il trait [`Spawner`] isola questa parte per poter testare il supervisore
//! senza processi reali.

use std::io::{self, Read, Write};
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Un figlio appena creato: pid e estremità master dello `socketpair`.
pub struct SpawnedChild {
    pub pid: i32,
    pub stream: crate::fds::Tracked<UnixStream>,
}

/// Chi crea, segnala e raccoglie i figli.
pub trait Spawner: Send + Sync {
    /// Crea un figlio e restituisce la sua estremità master.
    fn spawn(&self) -> io::Result<SpawnedChild>;

    /// Invia un segnale al processo (come `os.kill`).
    fn signal(&self, pid: i32, signal: i32) -> io::Result<()>;

    /// Attende l'uscita del processo e restituisce l'exit code in stile
    /// `Popen.returncode` (negativo se terminato da un segnale).
    fn reap(&self, pid: i32) -> i32;

    /// Chiude il canale di controllo: lo zygote esce appena non ha più figli.
    fn shutdown(&self);

    /// Lo zygote è ancora vivo?
    fn zygote_alive(&self) -> bool;

    /// Pid dello zygote (0 se non c'è).
    fn zygote_pid(&self) -> i32;
}

const CONTROL_SPAWN: u8 = 1;
const CONTROL_SHUTDOWN: u8 = 2;
const CONTROL_REAP: u8 = 3;
const CONTROL_FRAME_LEN: usize = 9;

/// Spawner reale: parla con il processo zygote Python.
pub struct ZygoteSpawner {
    control: Mutex<crate::fds::Tracked<UnixStream>>,
    zygote_pid: i32,
    maxtasks: u32,
    max_memory_kb: u32,
    lost: AtomicBool,
}

impl ZygoteSpawner {
    /// `fd` resta di proprietà del chiamante: qui viene duplicato.
    pub fn new(
        fd: RawFd,
        zygote_pid: i32,
        maxtasks: Option<u32>,
        max_memory_kb: Option<u32>,
    ) -> io::Result<Self> {
        // SAFETY: `dup` restituisce un nuovo descrittore indipendente, di cui
        // questo oggetto è l'unico proprietario; l'originale resta a Python.
        let dup = unsafe { libc::dup(fd) };
        if dup < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `dup` è un fd valido e non condiviso.
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
                "risposta zygote troppo grande",
            ));
        }
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body)?;
        if body.len() < 9 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "risposta zygote corta",
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
                "lo zygote non ha creato il figlio".to_owned()
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
                "zygote del pool non disponibile",
            ));
        }
        let mut fds = [0 as RawFd; 2];
        // SAFETY: `fds` è un array di due interi; la chiamata riempie entrambi.
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
        // SAFETY: entrambi i descrittori sono appena stati creati e di nostra proprietà.
        let master = crate::fds::Tracked::new(unsafe { UnixStream::from_raw_fd(fds[0]) });
        // SAFETY: idem; viene chiuso dopo l'invio a meno che lo zygote lo adotti.
        let child = crate::fds::Tracked::new(unsafe { OwnedFd::from_raw_fd(fds[1]) });
        use std::os::fd::AsRawFd;

        if let Err(error) = self.send_control(CONTROL_SPAWN, Some(child.as_raw_fd())) {
            self.lost.store(true, Ordering::Release);
            return Err(error);
        }
        // Lo zygote ha duplicato il fd: la nostra copia non serve più.
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
        // SAFETY: `kill` non tocca memoria del processo; il pid viene dal
        // protocollo di spawn o dal master Python.
        let rc = unsafe { libc::kill(pid, signal) };
        if rc != 0 {
            let error = io::Error::last_os_error();
            // ESRCH == processo già uscito: non è un errore per il chiamante.
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(());
            }
            return Err(error);
        }
        Ok(())
    }

    fn reap(&self, pid: i32) -> i32 {
        if pid == self.zygote_pid {
            // Lo zygote è figlio diretto del master: si raccoglie qui.
            return waitpid(pid);
        }
        // I figli del pool sono figli dello zygote: solo lui può fare
        // `waitpid`. Il supervisore gli chiede lo stato di uscita.
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
        // SAFETY: `kill(pid, 0)` non invia nulla: verifica solo l'esistenza.
        let rc = unsafe { libc::kill(self.zygote_pid, 0) };
        rc == 0
    }

    fn zygote_pid(&self) -> i32 {
        self.zygote_pid
    }
}

/// Invia `payload` e `fd` con `SCM_RIGHTS` in un'unica `sendmsg`.
fn send_with_fd(stream: &UnixStream, payload: &[u8], fd: RawFd) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr().cast_mut().cast(),
        iov_len: payload.len(),
    };
    // SAFETY: `CMSG_SPACE(4)` è una funzione pura della libc; 4 è la dimensione
    // di un `int` (un fd).
    let space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut control = [0u8; 64];
    debug_assert!(space <= control.len());
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = space;
    // SAFETY: il buffer di controllo è abbastanza grande per un cmsghdr + un fd.
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(io::Error::other("buffer di controllo SCM_RIGHTS nullo"));
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
        // SAFETY: `msg` punta a `iov` e `control`, vivi per tutta la chiamata.
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
            "invio del comando zygote parziale",
        ));
    }
    Ok(())
}

fn waitpid(pid: i32) -> i32 {
    loop {
        let mut status = 0;
        // SAFETY: `waitpid` scrive solo in `status`, un intero locale, e
        // attende un figlio diretto del processo.
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
        // Chiudere l'originale non deve rompere il duplicato.
        drop(a);
        assert!(!spawner.zygote_alive());
    }
}
