//! Safe process operations shared by the pool and Python bindings.

use std::io;

pub fn signal(pid: i32, signal: i32) -> io::Result<()> {
    // SAFETY: kill accepts integers and accesses no caller memory.
    if unsafe { libc::kill(pid, signal) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub fn wait(pid: i32) -> io::Result<i32> {
    loop {
        let mut status = 0;
        // SAFETY: status is writable for the call; waitpid retains no pointer.
        if unsafe { libc::waitpid(pid, &mut status, 0) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if libc::WIFEXITED(status) {
            return Ok(libc::WEXITSTATUS(status));
        }
        if libc::WIFSIGNALED(status) {
            return Ok(-libc::WTERMSIG(status));
        }
    }
}

#[cfg(target_os = "linux")]
pub fn set_parent_death_signal(signal: i32) -> io::Result<()> {
    // SAFETY: PR_SET_PDEATHSIG accepts an integer signal and retains no pointer.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, signal, 0, 0, 0) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
pub fn parent_death_signal() -> io::Result<i32> {
    let mut signal = 0;
    // SAFETY: PR_GET_PDEATHSIG writes one integer into the live local destination.
    if unsafe { libc::prctl(libc::PR_GET_PDEATHSIG, &mut signal, 0, 0, 0) } == 0 {
        Ok(signal)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_process_exists() {
        assert!(signal(std::process::id() as i32, 0).is_ok());
    }

    #[test]
    fn waits_for_exit_status() {
        let mut child = std::process::Command::new("sh")
            .args(["-c", "exit 7"])
            .spawn()
            .unwrap();
        assert_eq!(wait(child.id() as i32).unwrap(), 7);
        let _ = child.wait();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parent_death_signal_round_trip() {
        let old = parent_death_signal().unwrap();
        set_parent_death_signal(libc::SIGKILL).unwrap();
        assert_eq!(parent_death_signal().unwrap(), libc::SIGKILL);
        set_parent_death_signal(old).unwrap();
        assert!(set_parent_death_signal(-1).is_err());
    }
}
