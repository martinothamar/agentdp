use std::path::PathBuf;

use crate::Result;

#[derive(Debug)]
pub(crate) struct Config {
    pub instance_spec: PathBuf,
}

pub(crate) async fn run(config: Config) -> Result<()> {
    Box::pin(crate::system::run(config.into())).await
}

impl From<Config> for crate::system::Config {
    fn from(config: Config) -> Self {
        Self {
            instance_spec: config.instance_spec,
        }
    }
}
