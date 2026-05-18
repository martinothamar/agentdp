use agentdp_protocol::InstanceRmResult;

use super::{Error, Instance, path_text, state};

impl Instance {
    pub fn rm(mut self) -> Result<InstanceRmResult, Error> {
        let _lock = self.acquire_lock()?;
        self.reload_state()?;

        let name = self.name();
        if self.state.status == state::InstanceStatus::Running {
            return Err(Error::RemoveRunning {
                name,
                instance: self.state.instance,
            });
        }
        self.backend().cleanup(&self.state.backend)?;
        state::remove(&self.files)?;

        Ok(InstanceRmResult {
            name: self.name(),
            removed: path_text(&self.files.instance_dir),
        })
    }
}
