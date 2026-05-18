use std::time::Duration;

use agentdp_core::Context;
use agentdp_protocol::{InstanceExecParams, InstanceExecResult, InstanceShellResult};

use super::{Error, Instance};

impl Instance {
    pub fn exec(&self, context: &Context, params: &InstanceExecParams) -> Result<InstanceExecResult, Error> {
        validate_exec_params(params)?;
        self.ensure_running_for_ssh()?;
        let timeout = Duration::from_secs(params.timeout_seconds.unwrap_or(300));
        let output = self
            .backend()
            .run_user_command(context, &self.state, &params.command, timeout)?;

        Ok(InstanceExecResult {
            name: self.name(),
            command: params.command.clone(),
            exit_status: u64::try_from(output.status).unwrap_or(1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    pub fn shell(&self) -> Result<InstanceShellResult, Error> {
        self.ensure_running_for_ssh()?;
        let command = self.backend().shell_command(&self.state)?;

        Ok(InstanceShellResult {
            name: self.name(),
            command,
        })
    }
}

fn validate_exec_params(params: &InstanceExecParams) -> Result<(), Error> {
    if params.command.is_empty() {
        return Err(Error::EmptyExecCommand);
    }
    if params.timeout_seconds == Some(0) {
        return Err(Error::InvalidExecTimeout);
    }
    Ok(())
}
