//! Filesystem layout, single-instance locking, and the listening socket of the
//! terminal daemon.

use std::fs::{File, OpenOptions, Permissions};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use super::super::api::DAEMON_PROTOCOL_VERSION;

/// Socket paths longer than this fall back to a directory under `$TMPDIR`.
const MAX_SOCKET_PATH_LEN: usize = 100;

/// The files used by one daemon protocol version.
#[derive(Debug, Clone)]
pub(crate) struct DaemonPaths {
    pub dir: PathBuf,
    pub socket: PathBuf,
    pub lock: PathBuf,
    pub log: PathBuf,
}

impl DaemonPaths {
    pub fn in_dir(dir: PathBuf) -> Self {
        let version = DAEMON_PROTOCOL_VERSION;
        Self {
            socket: dir.join(format!("v{version}.sock")),
            lock: dir.join(format!("v{version}.lock")),
            log: dir.join(format!("v{version}.log")),
            dir,
        }
    }

    /// The paths used by the app and the daemon it spawns: `<state dir>/ts`,
    /// or `$TMPDIR/warp-ts-<uid>` if that would make the socket path too long.
    pub fn default_for_user() -> Self {
        let state_dir = warp_core::paths::state_dir();
        if !state_dir.as_os_str().is_empty() {
            let paths = Self::in_dir(state_dir.join("ts"));
            if paths.socket.as_os_str().len() <= MAX_SOCKET_PATH_LEN {
                return paths;
            }
        }
        let tmp = std::env::var_os("TMPDIR")
            .map(PathBuf::from)
            .filter(|dir| dir.is_absolute())
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        Self::in_dir(tmp.join(format!("warp-ts-{}", nix::unistd::geteuid())))
    }

    /// Creates the directory with mode 0700 and checks that it belongs to the
    /// current user.
    pub fn ensure_private_dir(&self) -> Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("Failed to create {}", self.dir.display()))?;
        let metadata = std::fs::symlink_metadata(&self.dir)?;
        if !metadata.is_dir() || metadata.uid() != nix::unistd::geteuid().as_raw() {
            bail!(
                "{} is not a directory owned by the current user",
                self.dir.display()
            );
        }
        std::fs::set_permissions(&self.dir, Permissions::from_mode(0o700))?;
        Ok(())
    }
}

/// An exclusive `flock` held for the lifetime of the daemon.
pub(crate) struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    /// Takes the lock without blocking. Returns `None` if another process
    /// holds it.
    pub fn try_acquire(path: &Path) -> Result<Option<Self>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("Failed to open {}", path.display()))?;
        // SAFETY: `file` is a valid open fd.
        let res = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if res == 0 {
            return Ok(Some(Self { _file: file }));
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Ok(None)
        } else {
            Err(err).context("flock failed")
        }
    }
}

/// Binds the daemon socket with mode 0600, replacing any stale socket file.
pub(crate) fn bind_socket(path: &Path) -> Result<UnixListener> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).context("Failed to remove stale socket"),
    }
    // SAFETY: umask has no memory-safety requirements.
    let previous = unsafe { libc::umask(0o077) };
    let result = UnixListener::bind(path);
    // SAFETY: as above.
    unsafe { libc::umask(previous) };
    let listener = result.with_context(|| format!("Failed to bind {}", path.display()))?;
    std::fs::set_permissions(path, Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// Returns the effective uid of the process on the other end of `fd`.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn peer_uid(fd: RawFd) -> std::io::Result<u32> {
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `cred` and `len` are valid for writes of the sizes passed.
    let res = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut libc::ucred as *mut libc::c_void,
            &mut len,
        )
    };
    if res == 0 {
        Ok(cred.uid)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Returns the effective uid of the process on the other end of `fd`.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(crate) fn peer_uid(fd: RawFd) -> std::io::Result<u32> {
    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    // SAFETY: `uid` and `gid` are valid for writes.
    let res = unsafe { libc::getpeereid(fd, &mut uid, &mut gid) };
    if res == 0 {
        Ok(uid)
    } else {
        Err(std::io::Error::last_os_error())
    }
}
