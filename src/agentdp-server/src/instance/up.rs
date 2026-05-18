use agentdp_core::Context;
use agentdp_core::platform::{self, ProcessStatus};
use agentdp_protocol::{InstanceUpResult, ProcessResult};

use crate::progress::Progress;
use crate::runtime;

use super::{Error, Instance, state};

impl Instance {
    pub fn up(&mut self, context: &Context, progress: &mut dyn Progress) -> Result<InstanceUpResult, Error> {
        self.up_with_start(
            context,
            |context, manifest, state| runtime::Backend::from_state(&state.backend).start(context, manifest, state),
            |context, state, progress| {
                runtime::Backend::from_state(&state.backend).wait_provisioned(context, state, progress)
            },
            progress,
        )
    }

    pub(super) fn up_with_start(
        &mut self,
        context: &Context,
        start: impl FnOnce(
            &Context,
            &agentdp_core::manifest::AgentManifest,
            &mut state::InstanceState,
        ) -> Result<runtime::StartOutput, runtime::Error>,
        wait_provisioned: impl FnOnce(&Context, &state::InstanceState, &mut dyn Progress) -> Result<(), runtime::Error>,
        progress: &mut dyn Progress,
    ) -> Result<InstanceUpResult, Error> {
        let _lock = self.acquire_lock()?;
        self.reload_state()?;

        let started = match self.state.status {
            state::InstanceStatus::Created | state::InstanceStatus::Stopped => {
                let started = start(context, &self.manifest, &mut self.state)?;
                match started.process.pid {
                    Some(pid) => progress.info(format!("qemu started pid {pid}")),
                    None => progress.info("qemu started".to_owned()),
                }
                started
            }
            state::InstanceStatus::Running => {
                let started = self.running_output()?;
                match started.process.pid {
                    Some(pid) => progress.info(format!("qemu already running pid {pid}")),
                    None => progress.info("qemu already running".to_owned()),
                }
                started
            }
        };

        self.state.status = state::InstanceStatus::Running;
        self.state.readiness = None;
        self.write_runtime()?;
        progress.info("waiting for cloud-init".to_owned());
        wait_provisioned(context, &self.state, progress)?;
        progress.info("cloud-init completed".to_owned());
        let readiness = self.wait_ready(context, progress)?;
        self.state.readiness = Some(state::ReadinessState {
            ready: true,
            last_success_unix_seconds: current_unix_seconds(),
            result: readiness.clone(),
        });
        self.write_runtime()?;

        Ok(InstanceUpResult {
            name: self.name(),
            state: self.runtime_path(),
            process: started.process,
            readiness,
            backend: started.details,
        })
    }

    fn running_output(&self) -> Result<runtime::StartOutput, Error> {
        let summary = self.backend().runtime_summary(&self.state.backend);
        if let Some(pid) = summary.pid {
            match platform::process_status(pid)? {
                ProcessStatus::Running => {}
                ProcessStatus::NotFound => {
                    return Err(Error::InvalidStatus {
                        name: self.name(),
                        status: "running with stale process".to_owned(),
                    });
                }
            }
        }

        Ok(runtime::StartOutput {
            process: ProcessResult {
                status: "running".to_owned(),
                pid: summary.pid,
                message: None,
            },
            details: self.backend().runtime_details(&self.state.backend),
        })
    }
}

fn current_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}
