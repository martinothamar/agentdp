use std::time::Duration;

use agentdp_core::Context;
use agentdp_core::platform::{self, ProcessStatus};
use agentdp_protocol::{InstanceDownResult, ProcessResult};

use crate::runtime;

use super::{Error, Instance, state};

const DOWN_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

impl Instance {
    pub fn down(&mut self, context: &Context) -> Result<InstanceDownResult, Error> {
        self.down_with_process_control(context, platform::process_status, platform::terminate_process, |pid| {
            platform::wait_for_process_exit(pid, DOWN_WAIT_TIMEOUT)
        })
    }

    pub(super) fn down_with_process_control(
        &mut self,
        context: &Context,
        process_status: impl FnMut(u32) -> Result<ProcessStatus, platform::ProcessStatusError>,
        terminate: impl FnMut(u32) -> Result<(), platform::TerminateProcessError>,
        wait_for_exit: impl FnMut(u32) -> Result<bool, platform::ProcessStatusError>,
    ) -> Result<InstanceDownResult, Error> {
        let _lock = self.acquire_lock()?;
        self.reload_state()?;

        let name = self.name();
        let previous_status = self.state.status;
        let status = self.state.status;
        let output = self.backend().down_with_process_control(
            context,
            runtime::DownInput { name: &name, status },
            &mut self.state.backend,
            process_status,
            terminate,
            wait_for_exit,
        )?;

        match status {
            state::InstanceStatus::Running => {
                self.state.status = state::InstanceStatus::Stopped;
                self.mark_not_ready();
                self.write_runtime()?;
            }
            state::InstanceStatus::Stopped => {
                if self.mark_not_ready() {
                    self.write_runtime()?;
                }
            }
            state::InstanceStatus::Created => {}
        }

        Ok(InstanceDownResult {
            name: self.name(),
            state: self.runtime_path(),
            status: self.state.status.to_string(),
            previous_status: previous_status.to_string(),
            terminated_pid: output.terminated_pid,
            process: ProcessResult {
                pid: None,
                status: output.process_status.to_owned(),
                message: None,
            },
        })
    }

    const fn mark_not_ready(&mut self) -> bool {
        let Some(readiness) = self.state.readiness.as_mut() else {
            return false;
        };
        if !readiness.ready {
            return false;
        }
        readiness.ready = false;
        true
    }
}
