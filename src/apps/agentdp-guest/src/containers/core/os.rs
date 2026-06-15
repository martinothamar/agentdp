use std::ffi::OsStr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub(crate) struct CliConfig {
    real_cli: &'static str,
    ca_dir: &'static str,
    shim_executable_name: Option<&'static str>,
}

impl CliConfig {
    pub(crate) const fn new(
        real_cli: &'static str,
        ca_dir: &'static str,
        shim_executable_name: Option<&'static str>,
    ) -> Self {
        Self {
            real_cli,
            ca_dir,
            shim_executable_name,
        }
    }

    pub(crate) fn is_shim_executable_name(self, name: &OsStr) -> bool {
        self.shim_executable_name.is_some_and(|expected| name == expected)
    }

    pub(crate) fn real_cli_path(self) -> &'static Path {
        Path::new(self.real_cli)
    }

    pub(crate) fn ca_dir(self) -> &'static Path {
        Path::new(self.ca_dir)
    }

    pub(crate) fn ca_bundle_path(self) -> PathBuf {
        self.ca_dir().join("ca-bundle.pem")
    }
}
