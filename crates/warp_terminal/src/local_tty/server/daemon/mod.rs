//! The persistent terminal daemon.
//!
//! A per-user, per-protocol-version process that owns PTYs so shells outlive
//! the app. It always reads every PTY into a ring buffer, and forwards bytes to
//! the single attached client over a per-session data socketpair.

mod listener;
mod ring;
mod scanner;
mod session;

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use instant::Instant;

use anyhow::{Context, Result, bail};
use mio::Interest;
use mio::net::{UnixListener, UnixStream};
use nix::sys::socket;
use signal_hook_mio::v1_0::Signals;

pub(crate) use self::listener::DaemonPaths;
use self::listener::{InstanceLock, bind_socket, peer_uid};
use self::session::{Attachment, PumpOutcome, Session, observed_exit_status};
use super::api::{
    AttachInfo, ClientMessage, DAEMON_PROTOCOL_VERSION, DaemonEvent, DaemonMessage, DaemonRequest,
    DaemonResponse, PersistentSessionId, RING_CAPACITY, SessionMetadata,
};
use super::protocol::{self, FillResult, FrameReader};
use crate::local_tty::{self, PtyOptions};

/// Overrides the idle timeout, in seconds.
pub(crate) const IDLE_TIMEOUT_ENV: &str = "WARP_TERMINAL_DAEMON_IDLE_SECS";
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const EXITED_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const KILL_WAIT: Duration = Duration::from_millis(500);
/// How long the daemon keeps its copies of fds sent to a client. Closing a
/// socket while it is still in flight over `SCM_RIGHTS` can get it flushed by
/// the macOS unix socket garbage collector, which the receiver then sees as
/// EOF.
const IN_FLIGHT_FD_GRACE: Duration = Duration::from_secs(5);
const TICK: Duration = Duration::from_secs(1);
const MAX_LOG_SIZE: u64 = 5 * 1024 * 1024;

const LISTENER_TOKEN: mio::Token = mio::Token(0);
const SIGNALS_TOKEN: mio::Token = mio::Token(1);
const WAKER_TOKEN: mio::Token = mio::Token(2);
const FIRST_DYNAMIC_TOKEN: usize = 3;

#[derive(Debug, Clone)]
pub(crate) struct DaemonConfig {
    pub paths: DaemonPaths,
    pub idle_timeout: Duration,
    pub exited_session_ttl: Duration,
    pub ring_capacity: usize,
}

impl DaemonConfig {
    pub fn new(paths: DaemonPaths) -> Self {
        let idle_timeout = std::env::var(IDLE_TIMEOUT_ENV)
            .ok()
            .and_then(|secs| secs.parse::<f64>().ok())
            .filter(|secs| secs.is_finite() && *secs >= 0.0)
            .map(Duration::from_secs_f64)
            .unwrap_or(DEFAULT_IDLE_TIMEOUT);
        Self {
            paths,
            idle_timeout,
            exited_session_ttl: EXITED_SESSION_TTL,
            ring_capacity: RING_CAPACITY,
        }
    }
}

/// Entry point for `terminal-server --daemon`: detaches from the caller, takes
/// the instance lock, and serves until idle.
pub(crate) fn run_daemon_process(
    init_feature_flags: impl FnOnce(),
    init_platform: impl FnOnce() -> anyhow::Result<()>,
) {
    let paths = DaemonPaths::default_for_user();
    if let Err(err) = paths.ensure_private_dir() {
        eprintln!("Failed to prepare terminal daemon directory: {err:#}");
        std::process::exit(1);
    }
    if let Err(err) = daemonize(&paths.log) {
        eprintln!("Failed to daemonize terminal daemon: {err:#}");
        std::process::exit(1);
    }

    let daemon = match Daemon::bind(DaemonConfig::new(paths)) {
        Ok(Some(daemon)) => daemon,
        Ok(None) => std::process::exit(0),
        Err(err) => {
            eprintln!("Failed to start terminal daemon: {err:#}");
            std::process::exit(1);
        }
    };

    if log::set_boxed_logger(Box::new(StderrLogger)).is_ok() {
        log::set_max_level(log::LevelFilter::Info);
    }
    init_feature_flags();
    if let Err(err) = init_platform() {
        log::error!("Failed to initialize platform: {err:#}");
    }
    log::info!("Terminal daemon running with pid {}", std::process::id());
    daemon.run();
    log::info!("Terminal daemon exiting");
}

/// Double-forks into a new session, ignores SIGHUP, and redirects stdio.
fn daemonize(log_path: &Path) -> Result<()> {
    // SAFETY: called before any thread is spawned; the children only continue
    // executing this single-threaded program.
    unsafe {
        match libc::fork() {
            -1 => return Err(std::io::Error::last_os_error()).context("fork failed"),
            0 => {}
            _ => libc::_exit(0),
        }
        if libc::setsid() < 0 {
            return Err(std::io::Error::last_os_error()).context("setsid failed");
        }
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
        match libc::fork() {
            -1 => return Err(std::io::Error::last_os_error()).context("fork failed"),
            0 => {}
            _ => libc::_exit(0),
        }
        libc::umask(0o077);
    }
    std::env::set_current_dir("/")?;

    let dev_null = File::open("/dev/null")?;
    let truncate = std::fs::metadata(log_path).is_ok_and(|m| m.len() > MAX_LOG_SIZE);
    let mut options = OpenOptions::new();
    options.create(true).mode(0o600);
    if truncate {
        options.write(true).truncate(true);
    } else {
        options.append(true);
    }
    let log = options.open(log_path)?;
    for (fd, target) in [
        (dev_null.as_raw_fd(), libc::STDIN_FILENO),
        (log.as_raw_fd(), libc::STDOUT_FILENO),
        (log.as_raw_fd(), libc::STDERR_FILENO),
    ] {
        // SAFETY: both fds are valid.
        if unsafe { libc::dup2(fd, target) } < 0 {
            return Err(std::io::Error::last_os_error()).context("dup2 failed");
        }
    }
    Ok(())
}

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        eprintln!(
            "{timestamp} {} {}: {}",
            record.level(),
            record.target(),
            record.args()
        );
    }

    fn flush(&self) {}
}

enum Source {
    Leader(PersistentSessionId),
    Data(PersistentSessionId),
}

struct Connection {
    stream: UnixStream,
    reader: FrameReader,
    said_hello: bool,
}

/// Stops a running daemon from another thread.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct StopHandle {
    stop: Arc<AtomicBool>,
    waker: Arc<mio::Waker>,
}

#[cfg(test)]
impl StopHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.waker.wake();
    }
}

pub(crate) struct Daemon {
    config: DaemonConfig,
    _lock: InstanceLock,
    listener: UnixListener,
    poll: mio::Poll,
    signals: Signals,
    stop: Arc<AtomicBool>,
    #[cfg_attr(not(test), allow(dead_code))]
    waker: Arc<mio::Waker>,
    next_token: usize,
    sources: HashMap<mio::Token, Source>,
    connections: HashMap<mio::Token, Connection>,
    active: Option<mio::Token>,
    sessions: HashMap<PersistentSessionId, Session>,
    orphans: Vec<std::process::Child>,
    in_flight: Vec<InFlightFds>,
    ready: HashSet<PersistentSessionId>,
    idle_since: Option<Instant>,
}

/// Copies of fds sent to a client, kept open for [`IN_FLIGHT_FD_GRACE`].
struct InFlightFds {
    sent_at: Instant,
    _fds: Vec<OwnedFd>,
}

impl Daemon {
    /// Takes the instance lock and binds the socket. Returns `None` if another
    /// daemon holds the lock.
    pub fn bind(config: DaemonConfig) -> Result<Option<Self>> {
        config.paths.ensure_private_dir()?;
        let Some(lock) = InstanceLock::try_acquire(&config.paths.lock)? else {
            return Ok(None);
        };
        let listener = bind_socket(&config.paths.socket)?;
        let mut listener = UnixListener::from_std(listener);
        let poll = mio::Poll::new()?;
        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;
        let mut signals = Signals::new([signal_hook::consts::SIGCHLD])?;
        poll.registry()
            .register(&mut signals, SIGNALS_TOKEN, Interest::READABLE)?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), WAKER_TOKEN)?);
        Ok(Some(Self {
            config,
            _lock: lock,
            listener,
            poll,
            signals,
            stop: Arc::new(AtomicBool::new(false)),
            waker,
            next_token: FIRST_DYNAMIC_TOKEN,
            sources: HashMap::new(),
            connections: HashMap::new(),
            active: None,
            sessions: HashMap::new(),
            orphans: Vec::new(),
            in_flight: Vec::new(),
            ready: HashSet::new(),
            idle_since: None,
        }))
    }

    #[cfg(test)]
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            stop: self.stop.clone(),
            waker: self.waker.clone(),
        }
    }

    /// Serves until idle for the configured timeout or stopped.
    pub fn run(mut self) {
        let mut events = mio::Events::with_capacity(128);
        let mut last_tick = Instant::now();
        self.idle_since = Some(Instant::now());
        loop {
            let timeout = if self.ready.is_empty() {
                let idle_left = self
                    .idle_since
                    .map(|since| self.config.idle_timeout.saturating_sub(since.elapsed()))
                    .unwrap_or(TICK);
                TICK.min(idle_left)
            } else {
                Duration::ZERO
            };
            if let Err(err) = self.poll.poll(&mut events, Some(timeout))
                && err.kind() != std::io::ErrorKind::Interrupted
            {
                log::error!("Daemon poll failed: {err}");
                break;
            }

            for event in events.iter() {
                match event.token() {
                    LISTENER_TOKEN => self.accept_connections(),
                    SIGNALS_TOKEN => {
                        if self.signals.pending().next().is_some() {
                            self.reap_children();
                        }
                    }
                    WAKER_TOKEN => {}
                    token if self.connections.contains_key(&token) => {
                        self.on_connection_readable(token)
                    }
                    token => match self.sources.get(&token) {
                        Some(Source::Leader(id) | Source::Data(id)) => {
                            let id = *id;
                            self.pump(id);
                        }
                        None => {}
                    },
                }
            }

            for id in std::mem::take(&mut self.ready) {
                self.pump(id);
            }

            if self.stop.load(Ordering::SeqCst) {
                break;
            }

            if last_tick.elapsed() >= TICK {
                last_tick = Instant::now();
                self.reap_children();
                self.expire_exited_sessions();
                self.in_flight
                    .retain(|entry| entry.sent_at.elapsed() < IN_FLIGHT_FD_GRACE);
            }

            if self.sessions.is_empty() && self.connections.is_empty() {
                let since = *self.idle_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= self.config.idle_timeout {
                    log::info!("Daemon idle; shutting down");
                    let _ = std::fs::remove_file(&self.config.paths.socket);
                    break;
                }
            } else {
                self.idle_since = None;
            }
        }
    }

    fn allocate_token(&mut self) -> mio::Token {
        let token = mio::Token(self.next_token);
        self.next_token += 1;
        token
    }

    fn accept_connections(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((mut stream, _)) => {
                    let our_uid = nix::unistd::geteuid().as_raw();
                    match peer_uid(stream.as_raw_fd()) {
                        Ok(uid) if uid == our_uid => {}
                        _ => {
                            log::warn!("Rejected connection from a different user");
                            continue;
                        }
                    }
                    let token = self.allocate_token();
                    if let Err(err) =
                        self.poll
                            .registry()
                            .register(&mut stream, token, Interest::READABLE)
                    {
                        log::warn!("Failed to register connection: {err}");
                        continue;
                    }
                    self.connections.insert(
                        token,
                        Connection {
                            stream,
                            reader: FrameReader::new(),
                            said_hello: false,
                        },
                    );
                    // Data may have arrived before registration.
                    self.on_connection_readable(token);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    log::warn!("Failed to accept connection: {err}");
                    return;
                }
            }
        }
    }

    fn on_connection_readable(&mut self, token: mio::Token) {
        let Some(connection) = self.connections.get_mut(&token) else {
            return;
        };
        let fill = connection.reader.fill(connection.stream.as_raw_fd());
        let mut messages = Vec::new();
        let mut broken = false;
        loop {
            match connection.reader.next_frame::<ClientMessage>() {
                Ok(Some((message, _fds))) => messages.push(message),
                Ok(None) => break,
                Err(err) => {
                    log::warn!("Received malformed frame: {err:#}");
                    broken = true;
                    break;
                }
            }
        }
        for message in messages {
            if !self.connections.contains_key(&token) {
                return;
            }
            self.handle_message(token, message);
        }
        if broken || !matches!(fill, Ok(FillResult::WouldBlock)) {
            self.close_connection(token);
        }
    }

    fn close_connection(&mut self, token: mio::Token) {
        let Some(mut connection) = self.connections.remove(&token) else {
            return;
        };
        let _ = self.poll.registry().deregister(&mut connection.stream);
        if self.active == Some(token) {
            self.active = None;
            self.detach_all();
        }
    }

    fn send(&mut self, token: mio::Token, message: &DaemonMessage, fds: &[RawFd]) {
        let Some(connection) = self.connections.get(&token) else {
            return;
        };
        if let Err(err) = protocol::send_frame(connection.stream.as_raw_fd(), message, fds) {
            log::warn!("Failed to send message to client: {err:#}");
            self.close_connection(token);
        }
    }

    fn handle_message(&mut self, token: mio::Token, message: ClientMessage) {
        let said_hello = self
            .connections
            .get(&token)
            .is_some_and(|connection| connection.said_hello);
        match message {
            ClientMessage::Hello {
                protocol_version,
                client_pid,
                ..
            } => {
                if said_hello {
                    self.close_connection(token);
                    return;
                }
                if protocol_version != DAEMON_PROTOCOL_VERSION {
                    log::warn!(
                        "Rejecting client {client_pid} with protocol version {protocol_version}"
                    );
                    self.send(
                        token,
                        &DaemonMessage::HelloReject {
                            protocol_version: DAEMON_PROTOCOL_VERSION,
                            reason: format!(
                                "unsupported protocol version {protocol_version}; expected {DAEMON_PROTOCOL_VERSION}"
                            ),
                        },
                        &[],
                    );
                    self.close_connection(token);
                    return;
                }
                if let Some(previous) = self.active.filter(|previous| *previous != token) {
                    log::info!("New client {client_pid} replaces the attached client");
                    self.close_connection(previous);
                }
                if let Some(connection) = self.connections.get_mut(&token) {
                    connection.said_hello = true;
                }
                self.active = Some(token);
                self.send(
                    token,
                    &DaemonMessage::HelloAck {
                        protocol_version: DAEMON_PROTOCOL_VERSION,
                        daemon_pid: std::process::id(),
                    },
                    &[],
                );
            }
            ClientMessage::Request { .. } if !said_hello => self.close_connection(token),
            ClientMessage::Request {
                request_id,
                request,
            } => {
                let (response, fds) = self.handle_request(request);
                let raw_fds: Vec<RawFd> = fds.iter().map(|fd| fd.as_raw_fd()).collect();
                self.send(
                    token,
                    &DaemonMessage::Response {
                        request_id,
                        response,
                    },
                    &raw_fds,
                );
                if !fds.is_empty() {
                    self.in_flight.push(InFlightFds {
                        sent_at: Instant::now(),
                        _fds: fds,
                    });
                }
            }
        }
    }

    fn handle_request(&mut self, request: DaemonRequest) -> (DaemonResponse, Vec<OwnedFd>) {
        match request {
            DaemonRequest::List => {
                let sessions = self.sessions.values().map(Session::summary).collect();
                (DaemonResponse::Sessions(sessions), Vec::new())
            }
            DaemonRequest::Spawn {
                session_id,
                metadata,
                options,
            } => match self.spawn_session(session_id, metadata, *options) {
                Ok(()) => self.attach_session(session_id, false),
                Err(err) => (DaemonResponse::Error(format!("{err:#}")), Vec::new()),
            },
            DaemonRequest::Attach { session_id } => self.attach_session(session_id, true),
            DaemonRequest::Kill { session_id } => {
                self.kill_session(session_id);
                (DaemonResponse::Done, Vec::new())
            }
            DaemonRequest::Prune { keep } => {
                let keep: HashSet<_> = keep.into_iter().collect();
                let doomed: Vec<_> = self
                    .sessions
                    .values()
                    .filter(|session| session.attachment.is_none() && !keep.contains(&session.id))
                    .map(|session| session.id)
                    .collect();
                for id in doomed {
                    self.kill_session(id);
                }
                (DaemonResponse::Done, Vec::new())
            }
            DaemonRequest::DetachAll => {
                self.detach_all();
                (DaemonResponse::Done, Vec::new())
            }
        }
    }

    fn spawn_session(
        &mut self,
        id: PersistentSessionId,
        metadata: SessionMetadata,
        mut options: PtyOptions,
    ) -> Result<()> {
        if self.sessions.contains_key(&id) {
            log::info!("Replacing an existing session on spawn");
            self.kill_session(id);
        }
        options.close_fds = true;
        let info = local_tty::spawn(options)?;
        // SAFETY: the spawn result transfers ownership of the leader fd.
        let leader = unsafe { OwnedFd::from_raw_fd(info.result.leader_fd) };
        protocol::set_nonblocking(leader.as_raw_fd())?;
        let token = self.allocate_token();
        let session = match Session::new(
            id,
            metadata,
            info.child,
            leader,
            token,
            self.config.ring_capacity,
            self.poll.registry(),
        ) {
            Ok(session) => session,
            Err(err) => bail!("Failed to watch pty: {err}"),
        };
        log::info!("Spawned session with pid {}", session.pid);
        self.sources.insert(token, Source::Leader(id));
        self.sessions.insert(id, session);
        Ok(())
    }

    fn attach_session(
        &mut self,
        id: PersistentSessionId,
        with_replay: bool,
    ) -> (DaemonResponse, Vec<OwnedFd>) {
        match self.try_attach_session(id, with_replay) {
            Ok(result) => result,
            Err(err) => (DaemonResponse::Error(format!("{err:#}")), Vec::new()),
        }
    }

    fn try_attach_session(
        &mut self,
        id: PersistentSessionId,
        with_replay: bool,
    ) -> Result<(DaemonResponse, Vec<OwnedFd>)> {
        if !self.sessions.contains_key(&id) {
            bail!("unknown session");
        }
        self.detach_session(id);

        let (daemon_end, app_end) = socket::socketpair(
            socket::AddressFamily::Unix,
            socket::SockType::Stream,
            None,
            socket::SockFlag::empty(),
        )?;
        // SAFETY: socketpair returned two fresh fds that we now own.
        let (daemon_end, app_end) = unsafe {
            (
                OwnedFd::from_raw_fd(daemon_end),
                OwnedFd::from_raw_fd(app_end),
            )
        };
        protocol::set_cloexec(daemon_end.as_raw_fd())?;
        protocol::set_cloexec(app_end.as_raw_fd())?;
        protocol::set_nonblocking(daemon_end.as_raw_fd())?;

        let token = self.allocate_token();
        self.poll.registry().register(
            &mut mio::unix::SourceFd(&daemon_end.as_raw_fd()),
            token,
            Interest::READABLE | Interest::WRITABLE,
        )?;
        self.sources.insert(token, Source::Data(id));

        let session = self.sessions.get_mut(&id).expect("checked above");
        let (replay, has_bootstrap_preamble) = if with_replay {
            let replay = session.history.replay();
            (replay.bytes, replay.has_bootstrap_preamble)
        } else {
            (Vec::new(), session.history.has_bootstrap_preamble())
        };
        let replay_len = replay.len() as u64;
        let mut attachment = Attachment::new(daemon_end, token, replay);
        if session.exit_status.is_some() {
            attachment.close_after_flush = Some(false);
        }
        session.attachment = Some(attachment);

        let mut fds = vec![app_end];
        if let Some(leader) = &session.leader {
            fds.push(leader.try_clone()?);
        }
        let info = AttachInfo {
            session_id: id,
            pid: session.pid,
            metadata: session.metadata.clone(),
            replay_len,
            has_bootstrap_preamble,
            exit_status: session.exit_status,
        };
        self.ready.insert(id);
        Ok((DaemonResponse::Attached(info), fds))
    }

    fn unregister_attachment(&mut self, attachment: Attachment) {
        let _ = self
            .poll
            .registry()
            .deregister(&mut mio::unix::SourceFd(&attachment.data.as_raw_fd()));
        self.sources.remove(&attachment.token);
    }

    fn detach_session(&mut self, id: PersistentSessionId) {
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        if session.attachment.is_none() {
            return;
        }
        let attachment = session.detach();
        if let Some(attachment) = attachment {
            self.unregister_attachment(attachment);
        }

        self.ready.insert(id);
    }

    fn detach_all(&mut self) {
        let ids: Vec<_> = self.sessions.keys().copied().collect();
        for id in ids {
            self.detach_session(id);
        }
    }

    /// Removes a session and kills its process.
    fn kill_session(&mut self, id: PersistentSessionId) {
        let Some(mut session) = self.remove_session(id) else {
            return;
        };
        let Some(mut child) = session.child.take() else {
            return;
        };
        let _ = child.kill();
        let deadline = Instant::now() + KILL_WAIT;
        loop {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) if Instant::now() >= deadline => break,
                Ok(None) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        self.orphans.push(child);
    }

    /// Removes a session, closing its leader and data socket.
    fn remove_session(&mut self, id: PersistentSessionId) -> Option<Session> {
        let mut session = self.sessions.remove(&id)?;
        self.ready.remove(&id);

        if let Some(attachment) = session.attachment.take() {
            self.unregister_attachment(attachment);
        }
        session.close_leader(self.poll.registry());
        self.sources.remove(&session.leader_token);
        Some(session)
    }

    fn pump(&mut self, id: PersistentSessionId) {
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        match session.pump(self.poll.registry()) {
            PumpOutcome::Idle => {}
            PumpOutcome::MoreWork => {
                self.ready.insert(id);
            }
            PumpOutcome::DataClosed => self.detach_session(id),
            PumpOutcome::Finished { emit_event } => {
                let Some(session) = self.remove_session(id) else {
                    return;
                };
                if emit_event
                    && let (Some(token), Some(status)) = (self.active, session.exit_status)
                {
                    self.send(
                        token,
                        &DaemonMessage::Event(DaemonEvent::SessionExited {
                            session_id: id,
                            pid: session.pid,
                            status,
                        }),
                        &[],
                    );
                }
            }
        }
    }

    fn reap_children(&mut self) {
        self.orphans
            .retain_mut(|child| !matches!(child.try_wait(), Ok(Some(_)) | Err(_)));

        let mut exited = Vec::new();
        for session in self.sessions.values_mut() {
            if let Some(child) = &mut session.child
                && let Ok(Some(status)) = child.try_wait()
            {
                exited.push((session.id, observed_exit_status(status)));
            }
        }
        for (id, status) in exited {
            self.on_child_exit(id, status);
        }
    }

    fn on_child_exit(&mut self, id: PersistentSessionId, status: crate::event::ObservedExitStatus) {
        let registry = self.poll.registry();
        let Some(session) = self.sessions.get_mut(&id) else {
            return;
        };
        log::info!("Session with pid {} exited: {status:?}", session.pid);
        session.child = None;
        session.exit_status = Some(status);
        session.drain_leader(registry);
        session.close_leader(registry);
        self.sources.remove(&session.leader_token);
        match &mut session.attachment {
            Some(attachment) => {
                attachment.close_after_flush = Some(true);
                self.ready.insert(id);
            }
            None => session.exited_at = Some(Instant::now()),
        }
    }

    fn expire_exited_sessions(&mut self) {
        let ttl = self.config.exited_session_ttl;
        let expired: Vec<_> = self
            .sessions
            .values()
            .filter(|session| {
                session.attachment.is_none()
                    && session.exited_at.is_some_and(|at| at.elapsed() >= ttl)
            })
            .map(|session| session.id)
            .collect();
        for id in expired {
            self.remove_session(id);
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let ids: Vec<_> = self.sessions.keys().copied().collect();
        for id in ids {
            self.kill_session(id);
        }
        for mut child in self.orphans.drain(..) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
#[path = "daemon_tests.rs"]
mod tests;
