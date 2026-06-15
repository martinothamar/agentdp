use std::time::Duration;

use agentdp_network::{InstanceNetworkError, InstanceNetworkState, InstanceNetworkStatus};
use tokio::sync::watch;

use super::InstanceNetworkObservation;

#[derive(Debug, Clone)]
pub(crate) struct InstanceNetworkHandle {
    pub(super) label: String,
    observation: watch::Receiver<InstanceNetworkObservation>,
}

impl InstanceNetworkHandle {
    pub(super) const fn new(label: String, observation: watch::Receiver<InstanceNetworkObservation>) -> Self {
        Self { label, observation }
    }

    #[must_use]
    pub(crate) fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub(crate) fn status(&self) -> InstanceNetworkStatus {
        self.observation.borrow().status.clone()
    }

    #[must_use]
    pub(super) fn observation(&self) -> InstanceNetworkObservation {
        self.observation.borrow().clone()
    }

    pub(crate) async fn wait_ready(&self, timeout: Duration) -> Result<(), InstanceNetworkError> {
        let mut observation = self.observation.clone();
        let label = self.label.clone();
        match tokio::time::timeout(timeout, async {
            loop {
                let current = observation.borrow().status.clone();
                if current.is_ready() {
                    return Ok(());
                }
                match current.state {
                    InstanceNetworkState::Stopped => {
                        return Err(InstanceNetworkError::StoppedBeforeReady { label: label.clone() });
                    }
                    InstanceNetworkState::Failed { error } => {
                        return Err(InstanceNetworkError::TaskFailed {
                            label: label.clone(),
                            message: error,
                        });
                    }
                    _ => {}
                }
                if observation.changed().await.is_err() {
                    return Err(InstanceNetworkError::StoppedBeforeReady { label: label.clone() });
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_elapsed) => Err(InstanceNetworkError::ReadyTimeout { label, timeout }),
        }
    }
}
