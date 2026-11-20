use anyhow::Result;
#[cfg(unix)]
use warp_errors::report_error;
use warpui_core::{AppContext, Entity, SingletonEntity};
#[cfg(unix)]
use {
    crate::local_tty::server::{
        AttachedSession, DaemonClient, PersistentSessionId, SessionMetadata, SessionSummary,
        TerminalServer,
    },
    anyhow::bail,
    std::cmp::Reverse,
    std::collections::HashMap,
    std::ffi::OsString,
    std::process::Child,
    std::sync::Arc,
};

#[cfg(target_os = "windows")]
use super::PseudoConsoleChild;
use super::{PtyOptions, PtySpawnResult};
use crate::event::ObservedExitStatus;
use crate::local_tty::{self};

#[derive(Clone, Copy, Debug)]
pub enum PtySpawnMode {
    TerminalServer,
    FallbackToDirect,
    Direct,
}

pub trait PtySpawnHooks {
    fn before_spawn(&self);
    fn after_spawn(&self);
    fn spawned(&self, mode: PtySpawnMode, ctx: &mut AppContext);
}
/// A handle that can be used to interact with a pty process.
pub trait PtyHandle: Send + Sync {
    /// Returns the pty's process ID.
    fn pid(&self) -> u32;

    /// Returns the observed status when the child has terminated. This may
    /// return `None` for an exited child (e.g. before a server-hosted PTY
    /// reports it), but never returns a status for a living child.
    fn process_termination_status(&mut self) -> Result<Option<ObservedExitStatus>>;

    /// Kills the pty process and waits for its successful termination.
    fn kill(&mut self) -> Result<()>;
}

/// A handle for a pty that is a direct child of the current process.
#[cfg(unix)]
struct DirectPtyHandle {
    child: Child,
}

#[cfg(unix)]
impl PtyHandle for DirectPtyHandle {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn process_termination_status(&mut self) -> Result<Option<ObservedExitStatus>> {
        use std::os::unix::process::ExitStatusExt;
        self.child
            .try_wait()
            .map(|status| {
                status.map(|status| {
                    status
                        .code()
                        .map(ObservedExitStatus::Code)
                        .or_else(|| status.signal().map(ObservedExitStatus::Signal))
                        .unwrap_or(ObservedExitStatus::Unavailable)
                })
            })
            .map_err(anyhow::Error::from)
    }

    fn kill(&mut self) -> Result<()> {
        self.child.kill()?;
        match self.child.wait() {
            Ok(_) => Ok(()),
            Err(err) => bail!(err),
        }
    }
}

#[cfg(target_os = "windows")]
struct DirectPtyHandle {
    child: PseudoConsoleChild,
}

#[cfg(target_os = "windows")]
impl PtyHandle for DirectPtyHandle {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn process_termination_status(&mut self) -> Result<Option<ObservedExitStatus>> {
        Ok(self
            .child
            .is_terminated()
            .then_some(ObservedExitStatus::Unavailable))
    }

    fn kill(&mut self) -> Result<()> {
        self.child.kill()
    }
}
/// Invokes the provided callback function without crash reporting enabled.
fn invoke_without_crash_reporting<T>(hooks: &dyn PtySpawnHooks, func: impl FnOnce() -> T) -> T {
    // Uninitialize cocoa-sentry before spawning the shell process to avoid passing any custom state
    // (such as BSD signal handlers and mach exception handlers) into the shell process. This means
    // we lose all Cocoa crash reports from now until when the session is successfully spawned,
    // which is not ideal but allows us to fully ensure that we don't improperly leak any Sentry state
    // into the child processes.
    hooks.before_spawn();

    let retval = func();

    // Now that the child has spawned--reinitialize cocoa sentry.
    hooks.after_spawn();

    retval
}

pub(super) struct PtySpawnInfo {
    pub result: PtySpawnResult,
    #[cfg(unix)]
    pub child: Child,
    #[cfg(windows)]
    pub child: PseudoConsoleChild,
}

/// A global singleton that provides the ability to spawn ptys.
///
/// This abstracts away from callers the manner in which the pty is spawned -
/// depending on configuration, the pty might be spawned as a child of the
/// current process, or it may be spawned by a subprocess that is responsible
/// for owning and managing ptys.
pub struct PtySpawner {
    #[cfg(unix)]
    server: Option<TerminalServer>,
    #[cfg(unix)]
    daemon: PersistentSessions,
}

/// The operations the app needs from the persistent session daemon.
#[cfg(unix)]
pub(crate) trait PersistentSessionDaemon: Send + Sync {
    fn list(&self) -> Result<Vec<SessionSummary>>;
    fn spawn(
        &self,
        id: PersistentSessionId,
        metadata: SessionMetadata,
        options: PtyOptions,
    ) -> Result<AttachedSession>;
    fn attach(&self, id: PersistentSessionId) -> Result<AttachedSession>;
    fn prune(&self, keep: Vec<PersistentSessionId>) -> Result<()>;
    fn detach_all(&self);
    fn kill_all_attached(&self);
    fn is_alive(&self) -> bool;
}

#[cfg(unix)]
impl PersistentSessionDaemon for DaemonClient {
    fn list(&self) -> Result<Vec<SessionSummary>> {
        DaemonClient::list(self)
    }

    fn spawn(
        &self,
        id: PersistentSessionId,
        metadata: SessionMetadata,
        options: PtyOptions,
    ) -> Result<AttachedSession> {
        DaemonClient::spawn(self, id, metadata, options)
    }

    fn attach(&self, id: PersistentSessionId) -> Result<AttachedSession> {
        DaemonClient::attach(self, id)
    }

    fn prune(&self, keep: Vec<PersistentSessionId>) -> Result<()> {
        DaemonClient::prune(self, keep)
    }

    fn detach_all(&self) {
        DaemonClient::detach_all(self)
    }

    fn kill_all_attached(&self) {
        DaemonClient::kill_all_attached(self)
    }

    fn is_alive(&self) -> bool {
        DaemonClient::is_alive(self)
    }
}

#[cfg(unix)]
type DaemonHandle = Arc<dyn PersistentSessionDaemon>;

#[cfg(unix)]
pub(crate) struct DaemonConnector {
    pub connect_or_spawn: Box<dyn Fn() -> Result<DaemonHandle>>,
    pub connect_existing: Box<dyn Fn() -> Result<Option<DaemonHandle>>>,
}

#[cfg(unix)]
impl Default for DaemonConnector {
    fn default() -> Self {
        Self {
            connect_or_spawn: Box::new(|| {
                DaemonClient::connect_or_spawn().map(|client| client as DaemonHandle)
            }),
            connect_existing: Box::new(|| {
                DaemonClient::connect_existing()
                    .map(|client| client.map(|client| client as DaemonHandle))
            }),
        }
    }
}

#[cfg(unix)]
enum DaemonState {
    NotConnected,
    Connected(DaemonHandle),
    Unavailable,
}

/// Connection state for the persistent session daemon, established lazily.
#[cfg(unix)]
struct PersistentSessions {
    connector: DaemonConnector,
    state: DaemonState,
    reconnected: bool,
    /// Sessions the daemon reported the first time a restored pane asked to
    /// reattach.
    listed_sessions: Option<HashMap<PersistentSessionId, SessionSummary>>,
}

#[cfg(unix)]
impl PersistentSessions {
    fn new(connector: DaemonConnector) -> Self {
        Self {
            connector,
            state: DaemonState::NotConnected,
            reconnected: false,
            listed_sessions: None,
        }
    }

    /// Returns the connected daemon, starting it if needed.
    fn connect_or_spawn(&mut self, hooks: &dyn PtySpawnHooks) -> Option<DaemonHandle> {
        if let DaemonState::Connected(daemon) = &self.state {
            if daemon.is_alive() {
                return Some(daemon.clone());
            }
            if self.reconnected {
                self.state = DaemonState::Unavailable;
                return None;
            }
            self.reconnected = true;
            self.state = DaemonState::NotConnected;
        }
        if !matches!(self.state, DaemonState::NotConnected) {
            return None;
        }

        match invoke_without_crash_reporting(hooks, || (self.connector.connect_or_spawn)()) {
            Ok(daemon) => {
                self.state = DaemonState::Connected(daemon.clone());
                Some(daemon)
            }
            Err(err) => {
                report_error!(err.context(
                    "Failed to connect to the persistent session daemon; sessions will not persist",
                ));
                self.state = DaemonState::Unavailable;
                None
            }
        }
    }

    /// Returns the connected daemon only if one is already running.
    fn connect_existing(&mut self) -> Option<DaemonHandle> {
        match &self.state {
            DaemonState::Connected(daemon) => return Some(daemon.clone()),
            DaemonState::Unavailable => return None,
            DaemonState::NotConnected => {}
        }
        match (self.connector.connect_existing)() {
            Ok(Some(daemon)) => {
                self.state = DaemonState::Connected(daemon.clone());
                Some(daemon)
            }
            Ok(None) => None,
            Err(err) => {
                report_error!(err.context(
                    "Failed to connect to the persistent session daemon; sessions will not persist",
                ));
                self.state = DaemonState::Unavailable;
                None
            }
        }
    }

    fn spawn(
        &mut self,
        id: PersistentSessionId,
        metadata: SessionMetadata,
        options: PtyOptions,
        hooks: &dyn PtySpawnHooks,
    ) -> Result<AttachedSession> {
        let Some(daemon) = self.connect_or_spawn(hooks) else {
            bail!("persistent session daemon is unavailable");
        };
        daemon.spawn(id, metadata, options)
    }

    fn attach(&mut self, id: PersistentSessionId) -> Option<AttachedSession> {
        if self
            .listed_sessions
            .as_ref()
            .is_some_and(|sessions| sessions.is_empty())
        {
            return None;
        }
        let Some(daemon) = self.connect_existing() else {
            self.listed_sessions = Some(HashMap::new());
            return None;
        };
        let listed_sessions = self
            .listed_sessions
            .get_or_insert_with(|| match daemon.list() {
                Ok(sessions) => sessions
                    .into_iter()
                    .map(|session| (session.session_id, session))
                    .collect(),
                Err(err) => {
                    log::warn!("Failed to list persistent sessions: {err:#}");
                    HashMap::new()
                }
            });
        listed_sessions.remove(&id)?;

        let mut session = match daemon.attach(id) {
            Ok(session) => session,
            Err(err) => {
                log::warn!("Failed to reattach persistent session: {err:#}");
                return None;
            }
        };
        if !session.info.has_bootstrap_preamble {
            log::info!("Persistent session has no bootstrap preamble; starting a fresh shell");
            if let Err(err) = session.handle.kill() {
                log::warn!("Failed to kill persistent session without preamble: {err:#}");
            }
            return None;
        }
        Some(session)
    }

    fn prune(&mut self, keep: Vec<PersistentSessionId>) {
        let Some(daemon) = self.connect_existing() else {
            return;
        };
        if let Err(err) = daemon.prune(keep) {
            log::warn!("Failed to prune persistent sessions: {err:#}");
        }
    }

    fn release(&mut self, detach_sessions: bool) {
        let DaemonState::Connected(daemon) = &self.state else {
            return;
        };
        if detach_sessions {
            log::info!("Detaching from persistent sessions...");
            daemon.detach_all();
        } else {
            log::info!("Killing attached persistent sessions...");
            daemon.kill_all_attached();
        }
    }
}

impl PtySpawner {
    /// Creates a new PtySpawner.
    ///
    /// This should be called extremely early in the application startup
    /// process - we want to minimize the number of already-obtained resources
    /// that could leak into forked subprocesses (e.g.: file descriptors).
    pub fn new() -> Result<Self> {
        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                let server = super::server::TerminalServer::new()?;
                Ok(Self {
                    server: Some(server),
                    daemon: PersistentSessions::new(DaemonConnector::default()),
                })
            } else if #[cfg(target_os = "windows")] {
                Ok(Self {})
            } else {
                unreachable!("Spawning a PTY is not supported on this platform.");
            }
        }
    }

    /// Creates a new PtySpanwer that is configured for unit test purposes.
    pub fn new_for_test() -> Self {
        cfg_if::cfg_if! {
            if #[cfg(unix)] {
                Self {
                    server: None,
                    daemon: PersistentSessions::new(DaemonConnector {
                        connect_or_spawn: Box::new(|| bail!("no persistent session daemon in tests")),
                        connect_existing: Box::new(|| Ok(None)),
                    }),
                }
            } else if #[cfg(target_os = "windows")] {
                Self {}
            } else {
                unreachable!("Spawning a PTY for tests is not supported on this platform.");
            }
        }
    }

    #[cfg(all(unix, test))]
    pub(crate) fn new_for_test_with_daemon(connector: DaemonConnector) -> Self {
        Self {
            server: None,
            daemon: PersistentSessions::new(connector),
        }
    }

    /// Does any work necessary to clean up state in advance of the app
    /// terminating.
    ///
    /// With `detach_sessions`, shells owned by the persistent session daemon
    /// keep running; otherwise the attached ones are killed.
    pub fn prepare_for_app_termination(
        &mut self,
        #[cfg_attr(not(unix), allow(unused_variables))] detach_sessions: bool,
    ) {
        #[cfg(unix)]
        self.daemon.release(detach_sessions);

        // Drop the backing `TerminalServer`, if one exists, killing the child
        // process.
        #[cfg(unix)]
        if let Some(server) = self.server.take() {
            log::info!("Tearing down terminal server...");
            drop(server);
        }
    }

    /// Spawns a shell owned by the persistent session daemon, connecting to (or
    /// starting) the daemon on first use.
    #[cfg(unix)]
    pub fn spawn_persistent_session(
        &mut self,
        id: PersistentSessionId,
        metadata: SessionMetadata,
        options: PtyOptions,
        hooks: &dyn PtySpawnHooks,
    ) -> Result<AttachedSession> {
        self.daemon.spawn(id, metadata, options, hooks)
    }

    /// Reattaches to a session that survived a previous run of the app.
    ///
    /// Returns `None` when there is no running daemon, the daemon does not know
    /// `id`, or the session cannot be resumed without bootstrapping again (in
    /// which case it is killed so a fresh shell can take its id).
    #[cfg(unix)]
    pub fn attach_persistent_session(
        &mut self,
        id: PersistentSessionId,
    ) -> Option<AttachedSession> {
        self.daemon.attach(id)
    }

    /// Kills daemon sessions that are not in `keep`. Does nothing if no daemon
    /// is running.
    #[cfg(unix)]
    pub fn prune_persistent_sessions(&mut self, keep: Vec<PersistentSessionId>) {
        self.daemon.prune(keep);
    }

    /// Spawns a pty, returning information about the pty and a handle that can
    /// be used to interact with the pty process.
    pub(super) fn spawn_pty(
        &self,
        options: PtyOptions,
        hooks: &dyn PtySpawnHooks,
        #[cfg(windows)] event_loop_tx: super::mio_channel::Sender<crate::writeable_pty::Message>,
        ctx: &mut AppContext,
    ) -> Result<(PtySpawnResult, Box<dyn PtyHandle>)> {
        #[cfg(not(unix))]
        let is_fallback = false;
        #[cfg(unix)]
        let mut is_fallback = false;

        #[cfg(unix)]
        if let Some(server) = &self.server {
            let result = Self::spawn_pty_via_server(server, options.clone());
            if let Err(err) = result {
                // Log env var names + sizes for any terminal server failure.
                // Large env vars are the most common cause (E2BIG on Linux,
                // socket overflow on macOS), so logging them on every failure
                // makes both cases diagnosable from the logs.
                log_env_var_diagnostics(&options.env_vars);

                // E2BIG is deterministic — retrying from the main process with
                // the same PtyOptions would hit the same limit — so fail
                // immediately rather than falling back.
                if is_e2big(&err) {
                    // err already contains the original message. We add some context on how to fix.
                    return Err(err.context(
                        "This can happen when env vars in the image or Oz secrets are \
                         too long.",
                    ));
                }
                report_error!(err.context(
                    "Failed to spawn pty via terminal server; falling back to spawning locally...",
                ));
                is_fallback = true;
            } else {
                hooks.spawned(PtySpawnMode::TerminalServer, ctx);
                return result;
            }
        }

        let mode = if is_fallback {
            PtySpawnMode::FallbackToDirect
        } else {
            PtySpawnMode::Direct
        };
        hooks.spawned(mode, ctx);

        Self::spawn_pty_directly(
            options,
            #[cfg(windows)]
            event_loop_tx,
            hooks,
        )
    }

    fn spawn_pty_directly(
        options: PtyOptions,
        #[cfg(windows)] event_loop_tx: super::mio_channel::Sender<crate::writeable_pty::Message>,
        hooks: &dyn PtySpawnHooks,
    ) -> Result<(PtySpawnResult, Box<dyn PtyHandle>)> {
        let pty_spawn_info = invoke_without_crash_reporting(hooks, move || {
            local_tty::spawn(
                options,
                #[cfg(windows)]
                event_loop_tx,
            )
        })?;
        let direct_pty_handle = Box::new(DirectPtyHandle {
            child: pty_spawn_info.child,
        });
        Ok((pty_spawn_info.result, direct_pty_handle))
    }

    #[cfg(unix)]
    fn spawn_pty_via_server(
        server: &TerminalServer,
        options: PtyOptions,
    ) -> Result<(PtySpawnResult, Box<dyn PtyHandle>)> {
        use crate::local_tty::server::ServerOwnedPtyHandle;

        let client = server.client().clone();
        let result = client.spawn_pty(options)?;
        let handle = Box::new(ServerOwnedPtyHandle {
            pid: result.pid,
            client,
        });
        Ok((result, handle))
    }
}

impl Entity for PtySpawner {
    type Event = ();
}

impl SingletonEntity for PtySpawner {}

#[cfg(all(test, unix))]
#[path = "spawner_tests.rs"]
mod tests;

/// Returns `true` if the error (or any cause in its chain) indicates that
/// `execve` failed with `ENAMETOOLONG` / E2BIG ("Argument list too long").
///
/// The terminal-server IPC path stringifies the raw `io::Error`, so we cannot
/// reliably downcast; instead we check the formatted error message as a
/// fallback.
#[cfg(unix)]
fn is_e2big(err: &anyhow::Error) -> bool {
    // Try the "real" io::Error in the chain first (direct-spawn path).
    if err.chain().any(|e| {
        e.downcast_ref::<std::io::Error>()
            .and_then(|e| e.raw_os_error())
            .is_some_and(|code| code == libc::E2BIG)
    }) {
        return true;
    }
    // Fall back to string matching for the IPC path where the io::Error
    // was serialised to a String before being sent back to the client.
    let msg = format!("{err:#}");
    msg.contains("os error 7") || msg.contains("Argument list too long")
}

/// Logs the names and byte-lengths (not values) of env vars passed to the
/// shell. Called on any terminal server failure to aid diagnosis of oversized
/// env var / secret configurations (E2BIG on Linux, socket overflow on macOS).
#[cfg(unix)]
fn log_env_var_diagnostics(extra_env_vars: &HashMap<OsString, OsString>) {
    log::error!("Shell spawn env var diagnostics (names and sizes only, no values):");

    // Log the additional env vars supplied via PtyOptions.
    let mut extra: Vec<(&OsString, usize)> = extra_env_vars
        .iter()
        .map(|(k, v)| (k, k.len() + v.len() + 2))
        .collect();
    extra.sort_by_key(|(_, size)| Reverse(*size));
    log::error!("PtyOptions env_vars entries={}", extra_env_vars.len());
    for (key, size) in extra.iter().take(20) {
        log::error!("PtyOptions env var entry key={key:?} bytes={size}");
    }

    // Log the largest vars from the inherited process environment.
    let mut inherited: Vec<(OsString, usize)> = std::env::vars_os()
        .map(|(k, v)| {
            let size = k.len() + v.len() + 2;
            (k, size)
        })
        .collect();
    inherited.sort_by_key(|(_, size)| Reverse(*size));
    let total: usize = inherited.iter().map(|(_, s)| s).sum();
    log::error!(
        "Inherited process env summary vars={} bytes={total}",
        inherited.len()
    );
    for (key, size) in inherited.iter().take(20) {
        log::error!("Inherited process env entry key={key:?} bytes={size}");
    }
}
