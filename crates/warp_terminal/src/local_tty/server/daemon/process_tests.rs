use std::os::unix::ffi::OsStrExt;

use super::*;

#[test]
fn raised_fd_limit_is_the_hard_limit_capped() {
    assert_eq!(raised_fd_limit(256, 4096), 4096);
    assert_eq!(raised_fd_limit(256, 1 << 20), FD_LIMIT_CAP);
    assert_eq!(raised_fd_limit(256, libc::RLIM_INFINITY), FD_LIMIT_CAP);
}

#[test]
fn raised_fd_limit_never_lowers_the_soft_limit() {
    assert_eq!(raised_fd_limit(1 << 20, libc::RLIM_INFINITY), 1 << 20);
}

#[test]
fn max_fd_to_close_is_bounded() {
    let max = max_fd_to_close();
    assert!((3..=FD_LIMIT_CAP as i32).contains(&max));
}

#[test]
fn invalid_variables_are_skipped() {
    let value = OsStr::new("value");
    assert!(is_valid_var(OsStr::new("EXAMPLE"), value));
    assert!(!is_valid_var(OsStr::new(""), value));
    assert!(!is_valid_var(OsStr::new("EXA=MPLE"), value));
    assert!(!is_valid_var(OsStr::from_bytes(b"EXA\0MPLE"), value));
    assert!(!is_valid_var(
        OsStr::new("EXAMPLE"),
        OsStr::from_bytes(b"va\0lue")
    ));
}

#[test]
fn spawn_context_without_changes_runs_the_closure() {
    assert_eq!(with_spawn_context(None, None, || 7), 7);
}
