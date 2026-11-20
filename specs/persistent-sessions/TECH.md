# Persistent local sessions (unix)

## Context
Quitting Warp kills every local shell. `on_will_terminate` (`app/src/lib.rs:2860`) calls `PtySpawner::prepare_for_app_termination`, which drops `TerminalServer` (`crates/warp_terminal/src/local_tty/spawner.rs:174`, `server/mod.rs:219`); the `Children` drop sends SIGHUP to each shell (`server/event_loop.rs:66`). The server also exits when its parent pid changes or its socketpair closes, and it hands the PTY leader fd to the app and closes its own copy (`server/event_loop.rs:275-301`). Session restoration only restores layout and finished blocks, with a fresh shell.

Goal: shells keep running after quitting Warp and reattach on relaunch, like a terminal-multiplexer server. Closing a tab or pane still kills its process. The feature is a user-facing setting (on by default) that can be toggled from Settings.

Other facts this design relies on:
- The app uses the leader fd beyond read/write: `TIOCSWINSZ` on resize (panics on failure, `unix.rs:717`), `TerminalAttributesPoller` (`tcgetattr`), and `tcgetpgrp` (`ai/agent_sdk/driver/harness/process_control.rs:56`, `lrc_activity/sampler.rs:220`).
- Every DCS hook is validated against `registered_session_ids` (`model/ansi/mod.rs:536-562`); the app registers the id at shell spawn (`terminal_manager.rs:1091`).
- Replaying `InitShell` makes `PtyController::initialize_shell` write the bootstrap script (`writeable_pty/pty_controller.rs:325`).
- Hook wire format: `ESC P $` + `d` + hex(JSON) + `ESC \` (`assets/bundled/bootstrap/zsh_body.sh:19-25`, `model/ansi/dcs_hooks.rs:17`).
- `terminal_panes.uuid` (`crates/persistence/src/schema.rs:405`) is stable across restore (`pane_group/mod.rs:1639` → `create_session`, `:6111`); new panes use `Uuid::new_v4()` (`:2180`). It does not reach `TerminalManager::create_model` yet.
- The current framing does a single `recvmsg` per payload (`server/protocol.rs:153-168`) and carries one fd (`AuxData = [RawFd; 1]`).

## Decision
1. A per-user, per-protocol-version terminal daemon **owns the PTY**: it always reads it and keeps a 10 MiB ring buffer per session, so processes never block while no app is attached.
2. Per session the app receives two fds over SCM_RIGHTS: `data` (one end of a daemon-created socketpair, bytes in both directions) and `leader` (a dup of the PTY leader, used by the app **only for ioctls**: resize, termios, tcgetpgrp). No `Resize` message is needed.
3. Session id = `terminal_panes.uuid`.
4. Reattach: the daemon sends a **preamble** (raw `InitShell` + `Bootstrapped` DCS of the login shell) followed by the **body** (bytes since the `CommandFinished` that immediately precedes the last `Precmd` seen before detach). The app registers the original `hook_session_id`, does not resend the bootstrap script, and parses the replay **before** the live loop, discarding terminal-query replies. This rebuilds `SessionInfo` naturally.
5. Closing a pane sends `Kill`. Quitting the app detaches. An app crash is a detach.

Rejected: handing only the PTY fd and reading in the daemon while detached (incomplete ring, a crash loses the running block); replay over the control channel (framing cannot carry MBs, small macOS socket buffers); suppressing replies inside `pty_read` by byte counting (chunks mix replay and live bytes).

## 1. Transport
- Directory: `warp_core::paths::state_dir().join("ts")` (`crates/warp_core/src/paths.rs:260`). Files: `v{DAEMON_PROTOCOL_VERSION}.sock`, `.lock`, `.log`. If the socket path exceeds 100 bytes, fall back to `$XDG_RUNTIME_DIR/warp-ts/` (Linux, when set and absolute), else `$TMPDIR/warp-ts-<uid>/`, else `/tmp/warp-ts-<uid>/`.
- The directory is created with `DirBuilder::mode(0o700)` (never world-readable, not even briefly) and must be a real directory (not a symlink) owned by `geteuid()`. `connect_or_spawn` and the daemon tighten an existing one we own to 0700; `connect_existing` only validates it (missing → no daemon; symlink, other owner or group/other bits → error) and never creates it.
- `umask(0o077)` before `bind`, then `chmod 0600`. Both ends check the peer uid against `geteuid()` (`getpeereid` on macOS, `SO_PEERCRED` on Linux): the daemon on accept (close silently on mismatch) and the client right after `connect`, before sending `Hello` (error on mismatch), so neither side talks to another user's process even if the socket path was planted.
- Framing: the `protocol.rs` format (BE `usize` header + bincode + SCM_RIGHTS), generalized: `send_frame<T: Serialize>(fd, &T, fds: &[RawFd])`, `cmsg_space!([RawFd; 2])`, a blocking reader that loops until `payload_size`, and a non-blocking `FrameReader` on the daemon side that accumulates bytes and fds. Legacy `api::Message` stays intact.
- One full-duplex control connection; responses correlate by `request_id`; a client reader thread dispatches responses and events.
- The daemon never blocks on a client: every frame it sends goes through a per-connection outbound queue (`FrameWriter`, 8 MiB cap) written when the socket is writable; a client whose queue overflows is dropped. Fds attached to a frame stay open for 5 s after their first byte is written (macOS in-flight fd GC).
- Limits on the control socket: frames from a client are capped at 4 MiB (frames from the daemon at 64 MiB); each read pass takes at most 256 KiB per connection before yielding to the event loop; at most 16 concurrent connections; a connection must send `Hello` within 5 s or it is closed.
- Received fds are close-on-exec immediately: `MSG_CMSG_CLOEXEC` on Linux, `FD_CLOEXEC` right after `recvmsg` on macOS. Daemon socketpairs use `SOCK_CLOEXEC` on Linux.
- Single client: a valid `Hello` evicts the previous client (equivalent to a detach for it).

## 2. Daemon lifecycle
- CLI: `warp_cli::TerminalServerArgs` (`crates/warp_cli/src/lib.rs:734`) gains `#[clap(long)] pub daemon: bool`; `--parent-pid` is not required in daemon mode. `run_terminal_server` dispatches on the flag.
- Startup with `--daemon`, before any thread: double fork (`_exit(0)` in the parent), `setsid()`, `SIGHUP` → `SIG_IGN`, `umask(0o077)`, `chdir("/")`, stdin → `/dev/null`, stdout/stderr → `v{N}.log` (append; truncate at start if > 5 MB), then hardening: `RLIMIT_NOFILE` soft raised to the hard limit (capped at 10240), `RLIMIT_CORE` soft = 0, and on Linux `prctl(PR_SET_DUMPABLE, 0)`; then `flock(v{N}.lock, LOCK_EX|LOCK_NB)` (on failure `exit(0)`; held for life), unlink stale socket, `bind`, `listen`, file logger, `init_platform()` and feature flags as today.
- Log rotation while running: every tick, if `v{N}.log` exceeds 5 MB it is renamed to `v{N}.log.old` and stdout/stderr are pointed at a fresh file.
- Connect-or-spawn (client): `connect` + peer uid check + `Hello`; on `ENOENT`/`ECONNREFUSED`, or `EPIPE`/`ECONNRESET`/EOF during the handshake (a daemon that is idle-exiting), run `current_exe() <terminal-server subcommand> --daemon` with null stdio and fds ≥ 3 closed in `pre_exec` (up to the soft `RLIMIT_NOFILE`, capped at 10240; never `sysconf`, which can be -1 or huge), `wait()` the short-lived child, retry `connect` every 50 ms up to 3 s. The daemon's flock serializes races.
- Shells are spawned with `local_tty::spawn` and `close_fds = true`; all daemon fds are `CLOEXEC`. `pre_exec` already resets signal dispositions (`unix.rs:502-521`).
- Spawn context: the daemon must not hand shells its own stale state (the environment of whichever app launched it, its `/` cwd, its raised and hardened limits). `Spawn` carries a `SpawnEnvironment { vars, cwd }` captured by the client (`std::env::vars_os()`, lossless `OsString`s, plus `current_dir()`). For the duration of `local_tty::spawn` the single-threaded daemon replaces its process environment with exactly `vars` (so `env_clear()` + client env; `unix.rs` then applies its Warp overlay and `PtyOptions::env_vars` on top, as in the legacy path), switches its cwd to the client's cwd, and restores the inherited soft `RLIMIT_NOFILE` and `RLIMIT_CORE`; everything is restored afterwards. The shell's initial directory is `start_dir`, else the client's `$HOME` (same as the legacy path), never the daemon's `/`. If a spawn with the inherited fd limit fails (the daemon may be using fds above it), it is retried once with the raised limit. Variables that cannot be set (empty key, `=` or NUL) are skipped. `PR_SET_DUMPABLE` is reset by `execve`.
- Idle exit: no sessions (alive or exited-pending) and no client for 60 s (overridable by env var for tests).
- Control EOF from the app = detach all its sessions; nothing is killed.
- `Kill`: close the leader, SIGKILL the pid, one `waitpid(WNOHANG)`, reply `Done` right away; SIGCHLD reaps it. The event loop never sleeps or blocks on a single client or process.
- Shell exit: drain the leader until `EIO`/`EAGAIN`. If attached: `shutdown(SHUT_WR)` on data, emit `SessionExited`, drop the session. If detached: keep `Exited{status}` + ring until the next `Attach` (delivered, then dropped), `Prune`, or 24 h.

## 3. Ring buffer and replay (daemon)
- `RING_CAPACITY = 10 MiB` per session, lazily grown, absolute offsets (`total: u64`, `ring_start = total - len`).
- Incremental DCS scanner: `ESC P $` + `d|f` + payload + `ESC \` (or 0x9C; CAN/SUB abort), payload capped at 8 MiB (a warning is logged when a hook is dropped for size), tolerant of splits across reads. Sizing: the largest hook, `Bootstrapped`, carries aliases, function names, builtins, keywords, `setopt` and exported variable names; a measured zsh setup sends about 16 KiB of JSON (32 KiB hex), and heavy plugin setups a few hundred KiB. For `d`, hex-decode and parse only `{hook, value.session_id, value.is_subshell}`. `CommandFinished`, `Precmd` and `Preexec` count only when `session_id` equals the session's `hook_session_id`; hooks from warpified subshells or SSH sessions (other ids) or without an id never move `replay_from`:
  - `CommandFinished`: keep it as pending (`last_command_finished_raw`); a `Preexec` clears a pending one.
  - `Precmd`: a prompt starts at the pending `CommandFinished` (if any, it is consumed) or else at the `Precmd` itself. Record that start offset in `prompt_starts` (pruned on eviction) and keep `last_command_finished_raw` + `last_precmd_raw` for the last prompt.
  - `Preexec`: keep `last_preexec_raw`.
  - Login `InitShell` (`is_subshell == false`) followed by the first `Bootstrapped`: store both raw DCS as `preamble`; a new login `InitShell` replaces the pair.
- On detach (explicit or EOF): `replay_from` = offset of the `CommandFinished` immediately before the last `Precmd`, or that `Precmd`'s offset if there is none (0 if no prompt yet). Starting at `CommandFinished` lets the restored model leave the hidden bootstrap block (risk 7.1).
- Replay = `preamble` + body, where body is the first that applies:
  1. `ring[replay_from..]` if `replay_from >= ring_start`;
  2. `ring[oldest_prompt_start..]` if any remains;
  3. `last_command_finished_raw` (if paired with the last prompt) + `last_precmd_raw` + `last_preexec_raw` (if later) + `b"\r\n[output truncated]\r\n"` + `ring[ring_start..]`.
- On `Attach` the daemon enqueues the replay on the data queue **before** resuming live forwarding, then replies with `replay_len`.
- Backpressure: per-session outbound queue capped at 4 MiB; above it, stop reading the leader until below 1 MiB. With no client attached, write only to the ring and never block the process.

## 4. Contract (`crates/warp_terminal/src/local_tty/server/api.rs`, next to legacy `Message`)
```rust
pub const DAEMON_PROTOCOL_VERSION: u32 = 1;
pub const RING_CAPACITY: usize = 10 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistentSessionId(pub [u8; 16]);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub hook_session_id: u64,
    pub shell_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnEnvironment {
    pub vars: Vec<(OsString, OsString)>,
    pub cwd: Option<OsString>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    Hello { protocol_version: u32, client_pid: u32, app_version: Option<String> },
    Request { request_id: u64, request: DaemonRequest },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DaemonRequest {
    List,
    Spawn { session_id: PersistentSessionId, metadata: SessionMetadata, options: Box<PtyOptions>, environment: Option<SpawnEnvironment> },
    Attach { session_id: PersistentSessionId },
    Kill { session_id: PersistentSessionId },
    Prune { keep: Vec<PersistentSessionId> },
    DetachAll,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DaemonMessage {
    HelloAck { protocol_version: u32, daemon_pid: u32 },
    HelloReject { protocol_version: u32, reason: String },
    Response { request_id: u64, response: DaemonResponse },
    Event(DaemonEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DaemonResponse {
    Sessions(Vec<SessionSummary>),
    Attached(AttachInfo),
    Done,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: PersistentSessionId,
    pub pid: u32,
    pub metadata: SessionMetadata,
    pub exit_status: Option<ObservedExitStatus>,
    pub attached: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachInfo {
    pub session_id: PersistentSessionId,
    pub pid: u32,
    pub metadata: SessionMetadata,
    pub replay_len: u64,
    pub has_bootstrap_preamble: bool,
    pub exit_status: Option<ObservedExitStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DaemonEvent {
    SessionExited { session_id: PersistentSessionId, pid: u32, status: ObservedExitStatus },
}
```
Rules:
- `Attached` always carries fds via SCM_RIGHTS in order `[data, leader]`; `leader` is absent if the session already exited.
- `Spawn` replies `Attached` with `replay_len = 0`. `DaemonClient::spawn` always sends `environment: Some(SpawnEnvironment::capture())`; `None` keeps the daemon's own environment (tests only).
- `Spawn` with an id the daemon already has replaces it: the old session is killed with `Kill` semantics (attached or not, alive or exited), then the new shell is spawned under the same id. The app relies on this when `attach()` fails during restore, since `DaemonClient` has no `kill(id)`.
- `Hello` must be the first frame; on version mismatch the daemon replies `HelloReject` and closes.

### 4.1 Client API (side A, `server/daemon_client.rs`, re-exported from `server/mod.rs`)
```rust
pub struct DaemonClient { /* … */ }
impl DaemonClient {
    pub fn connect_or_spawn() -> anyhow::Result<Arc<Self>>;
    pub fn connect_existing() -> anyhow::Result<Option<Arc<Self>>>;
    pub fn list(&self) -> anyhow::Result<Vec<SessionSummary>>;
    pub fn spawn(&self, id: PersistentSessionId, metadata: SessionMetadata, options: PtyOptions) -> anyhow::Result<AttachedSession>;
    pub fn attach(&self, id: PersistentSessionId) -> anyhow::Result<AttachedSession>;
    pub fn prune(&self, keep: Vec<PersistentSessionId>) -> anyhow::Result<()>;
    pub fn detach_all(&self);
    pub fn kill_all_attached(&self);
    pub fn is_alive(&self) -> bool;
}
pub struct AttachedSession {
    pub info: AttachInfo,
    pub data: OwnedFd,
    pub leader: Option<OwnedFd>,
    pub handle: Box<dyn PtyHandle>,
}
```
Client semantics:
- The reader thread turns `SessionExited` into a `pid → status` entry (same pattern as `server/mod.rs:71-111`) and raises `SIGCHLD` on itself, so `Pty::next_child_event` (`unix.rs:693-710`) works unchanged. The same applies when `attach()` returns `exit_status: Some`.
- Daemon EOF: every session becomes `Unavailable`, SIGCHLD is raised, `is_alive()` returns false.
- `DaemonPtyHandle::kill()` is a no-op after `detach_all()`; otherwise it sends `Kill` and waits for `Done`.

### 4.2 Trait change (side B, `local_tty/mod.rs:66`)
```rust
fn take_replay(&mut self) -> Option<Vec<u8>> { None }
```
In `local_tty/event_loop.rs`, right after `State::default()` and before registering fds: if `take_replay()` returns bytes, run them through `state.parser.parse_bytes` with a reply buffer that is **discarded**, then `send_wakeup_event()`.

### 4.3 Post-replay nudge (side B)
At the end of `Pty::attach`, `TIOCSWINSZ` with `(rows, cols - 1)` then `(rows, cols)` on `leader`, so TUIs redraw after the replay.

### 4.4 Commit order
First commit: these types plus `unimplemented!()` stubs for `DaemonClient` and `AttachedSession`, so both sides can build in parallel.

## 5. App rules
- Setting in `app/src/terminal/general_settings.rs`, after `restore_session`: `persistent_sessions: PersistentSessions`, `bool`, default `true`, mac + linux, `toml_path: "general.persistent_sessions"`, description "Whether shells keep running in the background after quitting Warp and reattach on relaunch."
- Settings UI in `app/src/settings_view/features_page.rs`: `PersistentSessionsWidget` right after `SessionRestorationWidget`; `FeaturesPageAction::TogglePersistentSessions` handled next to `ToggleRestoreSession` with `toggle_and_save_value`. Label "Keep sessions running after quitting Warp"; description "Shells keep running in the background when you quit Warp and reattach when you reopen it. Closing a tab or pane still ends its process. Requires restoring windows, tabs, and panes on startup." Rendered disabled when `restore_session` is off.
- Effective value: `persist = cfg!(unix) && persistent_sessions && restore_session && APPIMAGE unset`.
- Readers: `TerminalManager::create_pty` per pane (daemon vs legacy), startup restore (attach vs `Prune`), `on_will_terminate` (detach vs kill).
- ON → OFF: new panes use legacy; daemon panes keep working until closed; on quit `kill_all_attached()` instead of `detach_all()`. OFF → ON: new panes only. No restart needed.
- Uuid plumbing: `create_session` → `create_model`/`create_model_with_manager` gain `persistent_session_id: Option<PersistentSessionId>` → `on_shell_determined` → `create_pty`. `create_tui_model` and `spawn_replacement_shell` pass `None`.
- New pane with `persist`: lazy `connect_or_spawn` stored in `PtySpawner` as `Option<Arc<DaemonClient>>`; `spawn(uuid, {hook_session_id, shell_type}, options)`; `Pty::from_attached`. On failure: report once, mark the daemon unavailable for this run, fall back to legacy `spawn_pty`.
- Restore: `list()` once (cached). If the uuid exists with `has_bootstrap_preamble`: keep `set_login_shell_spawned`/`set_pending_shell_launch_data`, `register_session_id(SessionId::from(hook_session_id))` instead of the generated id, skip `enqueue_init_script`, set `PtyController::skip_next_bootstrap_write()` (consumed by `initialize_shell`), store `replay_len` in `Pty` for `take_replay` (blocking read, 5 s timeout), use `leader` for `ShellProcessInfo` and `TerminalAttributesPoller`. If attach fails or there is no preamble: `Kill` + `spawn` with the same uuid and normal bootstrap.
- Prune after windows are restored: if `connect_existing()` finds a daemon, `prune(terminal uuids in the snapshot)`; with `persist` off, `prune(vec![])`. Never start a daemon just to prune.
- `Pty` (`unix.rs:207`): new `leader: Option<File>` and `replay: Option<u64>`; `get_fd()` returns the leader if present; `on_resize` ioctls on `get_fd()`; `kill` drops `fd` and `leader` **before** `pty_handle.kill()` (keeps the macOS 'E' state fix, `unix.rs:727-736`).
- Quit: `prepare_for_app_termination(detach_sessions: bool)` with `persist`; calls `detach_all()` or `kill_all_attached()`, then drops the legacy server as today.
- Quit warning (`quit_warning/mod.rs:353`): with `persist`, say running commands "will keep running in the background".

## 6. Fallbacks
| Case | Behavior |
|---|---|
| `persist` off | Full legacy path; `Prune(vec![])` at startup only if a daemon exists. |
| Non-unix | Everything under `cfg(unix)`; Windows unchanged. |
| Cannot connect/spawn in 3 s | One `report_error`, legacy for the run, restored panes get fresh shells. |
| `HelloReject` | Same as above; the old daemon is left running with its sessions (documented limitation). |
| Daemon dies | Panes end as `Unavailable`; the next new pane retries `connect_or_spawn` once. |
| AppImage (`APPIMAGE` set) | Feature disabled (binary unmount can SIGBUS the daemon). |

## 6.1 Threat model
- Trust boundary: the user account. Any process running as the same uid can connect to the socket, list, attach to, write into and kill every persistent session, and read its scrollback. This is an accepted risk, the same as tmux, screen and similar multiplexer servers: such a process can already `ptrace` the user's shells (where permitted) or read their files.
- Other users are kept out: the directory is 0700 and owned by the user (not a symlink), the socket is 0600, and both the daemon and the client enforce the peer uid; the client never sends anything to a daemon owned by another uid.
- Scrollback lives only in daemon memory (10 MiB ring per session): nothing is written to disk. Core dumps are disabled (`RLIMIT_CORE` = 0) and on Linux the daemon is non-dumpable, so its memory is not exposed through core files or same-uid `ptrace`/`/proc/<pid>/mem`. The log records lifecycle events only, never terminal bytes.
- Resource abuse by a same-uid client is bounded (frame size, per-read budget, connection count, `Hello` timeout, outbound queue) so a misbehaving client cannot stall or exhaust the daemon for the app.

## 7. Risks
1. Rebuilt bootstrap: the preamble moves the model from `WarpInput` straight to `Bootstrapped` and the replayed hook triggers follow-up events. Side B validates this with a model test first; plan B is `set_bootstrapped()` plus a synthetic `Sessions` entry.
2. Warpified subshells/SSH use other session ids and are rejected on replay; shown as plain text in v1.
3. If the app crashed before persisting, blocks before `replay_from` are lost (never duplicated).
4. macOS: no app sandbox found; hardened runtime allows fork/exec/setsid. Bundle replacement is safe with atomic rename.
5. Linux: systemd `KillUserProcesses` kills the daemon on logout (accepted).
6. Extra hop PTY → daemon → socket → app; measure with `yes | head -c 1G`.
7. Up to 10 MiB per busy session.

## 8. Tests
Side A: round-trip of every message over a socketpair (2 fds, payloads > 1 MiB split); scanner byte-by-byte with hex CommandFinished/Precmd/Preexec/InitShell/Bootstrapped, foreign DCS ignored, CAN abort, payload cap; ring eviction, `prompt_starts` pruning and the three body cases (starting at `CommandFinished` when present); integration in a temp dir with `/bin/sh`: spawn → detach → output → attach with correct replay, Kill, `Spawn` over an existing id replaces it, `SessionExited` while attached, exited session delivered on Attach, Prune, short idle exit, second daemon exits on held lock, socket 0600 / dir 0700, `HelloReject`, 50 MB while detached without blocking the child.

Side B: event loop with a mock `EventedPty` whose replay contains `ESC[c` leaves the write queue empty and live bytes are processed afterwards; `Pty` over socketpair + openpty resizes via the leader; model test: restored `TerminalModel` + `register_session_id` + preamble + CommandFinished + Precmd + Preexec + output (with both 0x9C and `ESC \` terminators) → bootstrapped, running active block with that output, no bootstrap write; setting default `true` and key `general.persistent_sessions`; `prepare_for_app_termination(true)` makes `kill()` a no-op, `false` sends `Kill`.

## Consequences
Sessions survive quitting and app crashes. One resident process per user, exiting after 60 s idle. Closing a pane still kills its process. Persistence requires session restoration. A new protocol version does not reattach sessions from the previous one.

## Manual verification
Automated daemon checks against the real binary (headless, isolated `HOME`):
`cargo build --bin warp-oss --features gui && WARP_DAEMON_E2E_BIN=$PWD/target/debug/warp-oss cargo test -p warp_terminal daemon_e2e -- --test-threads=1`.

GUI checklist (build and run `warp-oss`; settings live at Settings > Features, "Keep sessions running after quitting Warp"). Use `pgrep -fl 'terminal-server --daemon'` and `ps -o pid,ppid,command -p <shell pid>` to observe processes.

Setting on (default, "Restore windows, tabs, and panes on startup" on):
1. Start `i=0; while true; do echo $i; i=$((i+1)); sleep 0.5; done` in a tab, quit with Cmd+Q and accept the quit warning, which should say commands keep running in the background. The shell and one `terminal-server --daemon` (ppid 1) stay alive.
2. Relaunch: the tab is restored with the same shell pid, the counter output continues without a gap, the prompt and block history are intact, and typing works.
3. Run `vim` or `top` and quit/relaunch: the TUI redraws at the right size; resize the window and confirm it follows.
4. Force-quit the app (`kill -9` the GUI process): relaunch and confirm the shells are still there.
5. Close a tab or pane (not quit): its process ends (`ps` shows it gone) and it is not restored.
6. Open several windows, quit, relaunch: each pane reattaches to its own shell.
7. Produce about 100 MB of output (`head -c 100000000 /dev/zero | tr '\0' y`) and quit mid-way: the command finishes in the background and the relaunch shows its tail without freezing.
8. Quit and leave Warp closed with no shells running: the daemon exits after about 60 s (`pgrep` is empty).

Toggle scenarios:
9. On to off with a daemon tab open: the tab keeps working; open a new tab (legacy shell, no new daemon session); quit: every shell, including the daemon one, is killed and nothing survives (`pgrep`, `ps`).
10. Off to on: only new tabs persist; old legacy tabs die on quit; no restart needed.
11. With the setting off at launch while a daemon from an earlier run still holds sessions: after restore the stale sessions are pruned (their shells die) and the app uses fresh shells.
12. Turn off "Restore windows, tabs, and panes on startup": the persistence toggle renders disabled and behaves as off.
13. Launch with `APPIMAGE` set (Linux): the feature is disabled and quitting kills shells.
14. Delete or rename the daemon socket while Warp runs and open a new tab: one error is reported and the tab falls back to a normal shell.
