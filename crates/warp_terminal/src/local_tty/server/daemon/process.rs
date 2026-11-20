//! Process-level setup of the daemon: resource limits, core dumps, and the
//! client context a shell is spawned in.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use super::super::api::SpawnEnvironment;

/// The highest file descriptor limit the daemon raises itself to, and the
/// most fds closed one by one before an exec.
const FD_LIMIT_CAP: libc::rlim_t = 10240;
/// Used when the fd limit cannot be read.
const FALLBACK_FD_LIMIT: libc::rlim_t = 1024;

/// Resource limits the daemon inherited before hardening itself, restored for
/// every shell it spawns.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InheritedLimits {
    nofile_soft: libc::rlim_t,
    core_soft: libc::rlim_t,
}

fn get_rlimit(resource: libc::c_int) -> Option<libc::rlimit> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is valid for writes.
    let res = unsafe { libc::getrlimit(resource as _, &mut limit) };
    (res == 0).then_some(limit)
}

fn set_soft_rlimit(resource: libc::c_int, soft: libc::rlim_t) -> std::io::Result<()> {
    let Some(mut limit) = get_rlimit(resource) else {
        return Err(std::io::Error::last_os_error());
    };
    limit.rlim_cur = soft;
    // SAFETY: `limit` is a valid rlimit.
    if unsafe { libc::setrlimit(resource as _, &limit) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// The soft limit to raise `RLIMIT_NOFILE` to: the hard limit, capped.
pub(crate) fn raised_fd_limit(soft: libc::rlim_t, hard: libc::rlim_t) -> libc::rlim_t {
    let target = if hard == libc::RLIM_INFINITY {
        FD_LIMIT_CAP
    } else {
        hard.min(FD_LIMIT_CAP)
    };
    target.max(soft)
}

/// Raises the fd limit, disables core dumps and, on Linux, marks the process
/// non-dumpable so its memory (scrollback) cannot be read through core files
/// or `ptrace`. Returns the limits to restore for spawned shells.
pub(crate) fn harden_daemon_process() -> Option<InheritedLimits> {
    let nofile = get_rlimit(libc::RLIMIT_NOFILE as libc::c_int)?;
    let core = get_rlimit(libc::RLIMIT_CORE as libc::c_int)?;
    let raised = raised_fd_limit(nofile.rlim_cur, nofile.rlim_max);
    if raised > nofile.rlim_cur
        && let Err(err) = set_soft_rlimit(libc::RLIMIT_NOFILE as libc::c_int, raised)
    {
        log::warn!("Failed to raise the fd limit: {err}");
    }
    if let Err(err) = set_soft_rlimit(libc::RLIMIT_CORE as libc::c_int, 0) {
        log::warn!("Failed to disable core dumps: {err}");
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // SAFETY: PR_SET_DUMPABLE takes a plain integer argument.
        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
            log::warn!(
                "Failed to mark the daemon non-dumpable: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    Some(InheritedLimits {
        nofile_soft: nofile.rlim_cur,
        core_soft: core.rlim_cur,
    })
}

/// The exclusive upper bound of fds to close before an exec: the soft fd
/// limit, capped so an unlimited or huge limit stays cheap.
pub(crate) fn max_fd_to_close() -> i32 {
    let soft = get_rlimit(libc::RLIMIT_NOFILE as libc::c_int)
        .map(|limit| limit.rlim_cur)
        .unwrap_or(FALLBACK_FD_LIMIT);
    soft.clamp(3, FD_LIMIT_CAP) as i32
}

fn is_valid_var(key: &OsStr, value: &OsStr) -> bool {
    let key = key.as_bytes();
    !key.is_empty() && !key.contains(&b'=') && !key.contains(&0) && !value.as_bytes().contains(&0)
}

/// Replaces the process environment with `vars`, skipping entries that cannot
/// be set. Returns the previous environment.
fn replace_environment(vars: &[(OsString, OsString)]) -> Vec<(OsString, OsString)> {
    let previous: Vec<_> = std::env::vars_os().collect();
    if previous.as_slice() == vars {
        return previous;
    }
    let wanted: HashMap<&OsStr, &OsStr> = vars
        .iter()
        .filter(|(key, value)| is_valid_var(key, value))
        .map(|(key, value)| (key.as_os_str(), value.as_os_str()))
        .collect();
    let current: HashMap<&OsStr, &OsStr> = previous
        .iter()
        .map(|(key, value)| (key.as_os_str(), value.as_os_str()))
        .collect();
    for key in current.keys() {
        if !wanted.contains_key(key) {
            // SAFETY: the daemon runs its event loop on a single thread and
            // spawns no other threads, so nothing reads the environment
            // concurrently.
            unsafe { std::env::remove_var(key) };
        }
    }
    for (key, value) in &wanted {
        if current.get(key) != Some(value) {
            // SAFETY: as above.
            unsafe { std::env::set_var(key, value) };
        }
    }
    previous
}

/// Process state changed for the duration of a spawn.
struct Restore {
    environment: Option<Vec<(OsString, OsString)>>,
    cwd: Option<PathBuf>,
    nofile_soft: Option<libc::rlim_t>,
    core_soft: Option<libc::rlim_t>,
}

impl Drop for Restore {
    fn drop(&mut self) {
        if let Some(cwd) = self.cwd.take() {
            let _ = std::env::set_current_dir(cwd);
        }
        if let Some(environment) = self.environment.take() {
            replace_environment(&environment);
        }
        if let Some(soft) = self.nofile_soft {
            let _ = set_soft_rlimit(libc::RLIMIT_NOFILE as libc::c_int, soft);
        }
        if let Some(soft) = self.core_soft {
            let _ = set_soft_rlimit(libc::RLIMIT_CORE as libc::c_int, soft);
        }
    }
}

/// Runs `spawn` with the client's environment and working directory and the
/// resource limits the daemon inherited, so the shell starts as if the client
/// had spawned it. Everything is restored afterwards.
pub(crate) fn with_spawn_context<T>(
    environment: Option<&SpawnEnvironment>,
    limits: Option<InheritedLimits>,
    spawn: impl FnOnce() -> T,
) -> T {
    let mut restore = Restore {
        environment: None,
        cwd: None,
        nofile_soft: None,
        core_soft: None,
    };
    if let Some(limits) = limits {
        if let Some(current) = get_rlimit(libc::RLIMIT_NOFILE as libc::c_int)
            && current.rlim_cur != limits.nofile_soft
            && set_soft_rlimit(libc::RLIMIT_NOFILE as libc::c_int, limits.nofile_soft).is_ok()
        {
            restore.nofile_soft = Some(current.rlim_cur);
        }
        if let Some(current) = get_rlimit(libc::RLIMIT_CORE as libc::c_int)
            && current.rlim_cur != limits.core_soft
            && set_soft_rlimit(libc::RLIMIT_CORE as libc::c_int, limits.core_soft).is_ok()
        {
            restore.core_soft = Some(current.rlim_cur);
        }
    }
    if let Some(environment) = environment {
        restore.environment = Some(replace_environment(&environment.vars));
        if let Some(cwd) = environment
            .cwd
            .as_ref()
            .map(PathBuf::from)
            .filter(|cwd| cwd.is_dir())
            && let Ok(current) = std::env::current_dir()
            && current != *cwd
            && std::env::set_current_dir(&cwd).is_ok()
        {
            restore.cwd = Some(current);
        }
    }
    spawn()
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
