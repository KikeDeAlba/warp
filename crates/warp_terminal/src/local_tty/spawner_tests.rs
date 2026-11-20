use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::*;
use crate::SizeInfo;
use crate::local_tty::server::AttachInfo;
use crate::local_tty::shell::{DirectShellStarter, ShellStarter};
use crate::shell::ShellType;

#[derive(Default)]
struct FakeDaemonState {
    calls: Vec<&'static str>,
    dead: bool,
    sessions: Vec<SessionSummary>,
    has_bootstrap_preamble: bool,
    kills: usize,
}

#[derive(Clone, Default)]
struct FakeDaemon(Arc<Mutex<FakeDaemonState>>);

impl FakeDaemon {
    fn calls(&self) -> Vec<&'static str> {
        self.0.lock().calls.clone()
    }

    fn attached_session(&self, id: PersistentSessionId) -> AttachedSession {
        let (data, _peer) = UnixStream::pair().unwrap();
        AttachedSession {
            info: AttachInfo {
                session_id: id,
                pid: 4242,
                metadata: metadata(),
                replay_len: 0,
                has_bootstrap_preamble: self.0.lock().has_bootstrap_preamble,
                exit_status: None,
            },
            data: OwnedFd::from(data),
            leader: None,
            handle: Box::new(FakeHandle(self.0.clone())),
        }
    }
}

impl PersistentSessionDaemon for FakeDaemon {
    fn list(&self) -> Result<Vec<SessionSummary>> {
        let mut state = self.0.lock();
        state.calls.push("list");
        Ok(state.sessions.clone())
    }

    fn spawn(
        &self,
        id: PersistentSessionId,
        _metadata: SessionMetadata,
        _options: PtyOptions,
    ) -> Result<AttachedSession> {
        self.0.lock().calls.push("spawn");
        Ok(self.attached_session(id))
    }

    fn attach(&self, id: PersistentSessionId) -> Result<AttachedSession> {
        self.0.lock().calls.push("attach");
        Ok(self.attached_session(id))
    }

    fn prune(&self, _keep: Vec<PersistentSessionId>) -> Result<()> {
        self.0.lock().calls.push("prune");
        Ok(())
    }

    fn detach_all(&self) {
        self.0.lock().calls.push("detach_all");
    }

    fn kill_all_attached(&self) {
        self.0.lock().calls.push("kill_all_attached");
    }

    fn is_alive(&self) -> bool {
        !self.0.lock().dead
    }
}

struct FakeHandle(Arc<Mutex<FakeDaemonState>>);

impl PtyHandle for FakeHandle {
    fn pid(&self) -> u32 {
        4242
    }

    fn process_termination_status(&mut self) -> Result<Option<ObservedExitStatus>> {
        Ok(None)
    }

    fn kill(&mut self) -> Result<()> {
        self.0.lock().kills += 1;
        Ok(())
    }
}

struct NoopHooks;

impl PtySpawnHooks for NoopHooks {
    fn before_spawn(&self) {}

    fn after_spawn(&self) {}

    fn spawned(&self, _mode: PtySpawnMode, _ctx: &mut AppContext) {}
}

fn metadata() -> SessionMetadata {
    SessionMetadata {
        hook_session_id: 7,
        shell_type: "zsh".to_owned(),
    }
}

fn session_id(byte: u8) -> PersistentSessionId {
    PersistentSessionId([byte; 16])
}

fn summary(id: PersistentSessionId) -> SessionSummary {
    SessionSummary {
        session_id: id,
        pid: 4242,
        metadata: metadata(),
        exit_status: None,
        attached: false,
    }
}

fn options() -> PtyOptions {
    PtyOptions {
        size: SizeInfo::new_without_font_metrics(24, 80),
        window_id: None,
        shell_starter: ShellStarter::Direct(DirectShellStarter::new_for_test(
            ShellType::Zsh,
            PathBuf::from("/bin/zsh"),
            Vec::new(),
        )),
        start_dir: None,
        env_vars: Default::default(),
        enable_ssh_wrapper: false,
        reuse_ssh_control_master: false,
        shell_debug_mode: false,
        honor_ps1: false,
        node_version_chip_enabled: false,
        close_fds: true,
    }
}

struct Connections {
    spawned: Arc<AtomicUsize>,
    existing: Arc<AtomicUsize>,
}

fn spawner_with(
    daemon: &FakeDaemon,
    can_spawn: bool,
    is_running: bool,
) -> (PtySpawner, Connections) {
    let spawned = Arc::new(AtomicUsize::new(0));
    let existing = Arc::new(AtomicUsize::new(0));
    let connector = DaemonConnector {
        connect_or_spawn: Box::new({
            let daemon = daemon.clone();
            let spawned = spawned.clone();
            move || {
                spawned.fetch_add(1, Ordering::SeqCst);
                if can_spawn {
                    Ok(Arc::new(daemon.clone()) as DaemonHandle)
                } else {
                    bail!("cannot start daemon")
                }
            }
        }),
        connect_existing: Box::new({
            let daemon = daemon.clone();
            let existing = existing.clone();
            move || {
                existing.fetch_add(1, Ordering::SeqCst);
                Ok(is_running.then(|| Arc::new(daemon.clone()) as DaemonHandle))
            }
        }),
    };
    (
        PtySpawner::new_for_test_with_daemon(connector),
        Connections { spawned, existing },
    )
}

#[test]
fn termination_detaches_sessions_when_persisting() {
    let daemon = FakeDaemon::default();
    let (mut spawner, _) = spawner_with(&daemon, true, true);
    spawner
        .spawn_persistent_session(session_id(1), metadata(), options(), &NoopHooks)
        .unwrap();

    spawner.prepare_for_app_termination(true);

    assert_eq!(daemon.calls(), vec!["spawn", "detach_all"]);
}

#[test]
fn termination_kills_attached_sessions_when_not_persisting() {
    let daemon = FakeDaemon::default();
    let (mut spawner, _) = spawner_with(&daemon, true, true);
    spawner
        .spawn_persistent_session(session_id(1), metadata(), options(), &NoopHooks)
        .unwrap();

    spawner.prepare_for_app_termination(false);

    assert_eq!(daemon.calls(), vec!["spawn", "kill_all_attached"]);
}

#[test]
fn termination_without_daemon_does_not_connect() {
    let daemon = FakeDaemon::default();
    let (mut spawner, connections) = spawner_with(&daemon, true, true);

    spawner.prepare_for_app_termination(true);

    assert!(daemon.calls().is_empty());
    assert_eq!(connections.spawned.load(Ordering::SeqCst), 0);
    assert_eq!(connections.existing.load(Ordering::SeqCst), 0);
}

#[test]
fn spawn_connects_lazily_once() {
    let daemon = FakeDaemon::default();
    let (mut spawner, connections) = spawner_with(&daemon, true, false);
    assert_eq!(connections.spawned.load(Ordering::SeqCst), 0);

    for byte in 0..2 {
        spawner
            .spawn_persistent_session(session_id(byte), metadata(), options(), &NoopHooks)
            .unwrap();
    }

    assert_eq!(connections.spawned.load(Ordering::SeqCst), 1);
    assert_eq!(daemon.calls(), vec!["spawn", "spawn"]);
}

#[test]
fn failed_connection_marks_daemon_unavailable_for_the_run() {
    let daemon = FakeDaemon::default();
    let (mut spawner, connections) = spawner_with(&daemon, false, false);

    for byte in 0..3 {
        assert!(
            spawner
                .spawn_persistent_session(session_id(byte), metadata(), options(), &NoopHooks)
                .is_err()
        );
    }

    assert_eq!(connections.spawned.load(Ordering::SeqCst), 1);
    assert!(daemon.calls().is_empty());
}

#[test]
fn dead_daemon_is_reconnected_only_once() {
    let daemon = FakeDaemon::default();
    let (mut spawner, connections) = spawner_with(&daemon, true, false);
    spawner
        .spawn_persistent_session(session_id(1), metadata(), options(), &NoopHooks)
        .unwrap();

    daemon.0.lock().dead = true;
    spawner
        .spawn_persistent_session(session_id(2), metadata(), options(), &NoopHooks)
        .unwrap();
    assert!(
        spawner
            .spawn_persistent_session(session_id(3), metadata(), options(), &NoopHooks)
            .is_err()
    );

    assert_eq!(connections.spawned.load(Ordering::SeqCst), 2);
}

#[test]
fn prune_never_starts_a_daemon() {
    let daemon = FakeDaemon::default();
    let (mut spawner, connections) = spawner_with(&daemon, true, false);

    spawner.prune_persistent_sessions(vec![session_id(1)]);

    assert_eq!(connections.spawned.load(Ordering::SeqCst), 0);
    assert_eq!(connections.existing.load(Ordering::SeqCst), 1);
    assert!(daemon.calls().is_empty());
}

#[test]
fn prune_uses_running_daemon() {
    let daemon = FakeDaemon::default();
    let (mut spawner, _) = spawner_with(&daemon, true, true);

    spawner.prune_persistent_sessions(Vec::new());

    assert_eq!(daemon.calls(), vec!["prune"]);
}

#[test]
fn attach_lists_sessions_once_and_skips_unknown_ids() {
    let daemon = FakeDaemon::default();
    {
        let mut state = daemon.0.lock();
        state.sessions = vec![summary(session_id(1)), summary(session_id(2))];
        state.has_bootstrap_preamble = true;
    }
    let (mut spawner, connections) = spawner_with(&daemon, true, true);

    assert!(spawner.attach_persistent_session(session_id(9)).is_none());
    let attached = spawner.attach_persistent_session(session_id(1)).unwrap();
    assert_eq!(attached.info.session_id, session_id(1));
    assert!(spawner.attach_persistent_session(session_id(2)).is_some());
    assert!(spawner.attach_persistent_session(session_id(1)).is_none());

    assert_eq!(daemon.calls(), vec!["list", "attach", "attach"]);
    assert_eq!(connections.existing.load(Ordering::SeqCst), 1);
    assert_eq!(connections.spawned.load(Ordering::SeqCst), 0);
}

#[test]
fn attach_without_preamble_kills_session() {
    let daemon = FakeDaemon::default();
    daemon.0.lock().sessions = vec![summary(session_id(1))];
    let (mut spawner, _) = spawner_with(&daemon, true, true);

    assert!(spawner.attach_persistent_session(session_id(1)).is_none());

    assert_eq!(daemon.calls(), vec!["list", "attach"]);
    assert_eq!(daemon.0.lock().kills, 1);
}

#[test]
fn attach_without_running_daemon_returns_none() {
    let daemon = FakeDaemon::default();
    let (mut spawner, connections) = spawner_with(&daemon, true, false);

    assert!(spawner.attach_persistent_session(session_id(1)).is_none());

    assert_eq!(connections.spawned.load(Ordering::SeqCst), 0);
    assert!(daemon.calls().is_empty());
}
