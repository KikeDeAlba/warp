use std::os::fd::OwnedFd;
use std::sync::Arc;

use super::api::{AttachInfo, PersistentSessionId, SessionMetadata, SessionSummary};
use crate::local_tty::PtyOptions;
use crate::local_tty::spawner::PtyHandle;

pub struct DaemonClient {}

impl DaemonClient {
    pub fn connect_or_spawn() -> anyhow::Result<Arc<Self>> {
        unimplemented!()
    }

    pub fn connect_existing() -> anyhow::Result<Option<Arc<Self>>> {
        unimplemented!()
    }

    pub fn list(&self) -> anyhow::Result<Vec<SessionSummary>> {
        unimplemented!()
    }

    pub fn spawn(
        &self,
        _id: PersistentSessionId,
        _metadata: SessionMetadata,
        _options: PtyOptions,
    ) -> anyhow::Result<AttachedSession> {
        unimplemented!()
    }

    pub fn attach(&self, _id: PersistentSessionId) -> anyhow::Result<AttachedSession> {
        unimplemented!()
    }

    pub fn prune(&self, _keep: Vec<PersistentSessionId>) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn detach_all(&self) {
        unimplemented!()
    }

    pub fn kill_all_attached(&self) {
        unimplemented!()
    }

    pub fn is_alive(&self) -> bool {
        unimplemented!()
    }
}

pub struct AttachedSession {
    pub info: AttachInfo,
    pub data: OwnedFd,
    pub leader: Option<OwnedFd>,
    pub handle: Box<dyn PtyHandle>,
}
