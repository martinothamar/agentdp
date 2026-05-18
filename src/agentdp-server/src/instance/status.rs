use agentdp_core::platform::{self, ProcessStatus};
use agentdp_protocol::InstanceStatusResult;

use super::{Instance, network_result, readiness_result};

impl Instance {
    pub fn status(&self) -> InstanceStatusResult {
        self.status_with_process_status(platform::process_status)
    }

    pub(super) fn status_with_process_status(
        &self,
        process_status: impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
    ) -> InstanceStatusResult {
        let status = self
            .backend()
            .status_with_process_status(self.state.status, &self.state.backend, process_status);

        InstanceStatusResult {
            name: self.name(),
            state: self.runtime_path(),
            status: self.state.status.to_string(),
            stale: status.stale,
            process: status.process,
            backend: status.details,
            network: network_result(&self.state.network),
            readiness: self.state.readiness.as_ref().map(readiness_result),
        }
    }
}
