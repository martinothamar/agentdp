use serde::{Deserialize, Serialize};

use crate::manifest::{AgentManifest, GuestOs};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Qemu,
}

impl BackendKind {
    #[must_use]
    pub const fn local_default() -> Self {
        Self::Qemu
    }

    #[must_use]
    pub const fn for_manifest(manifest: &AgentManifest) -> Self {
        match manifest.image.os {
            GuestOs::Archlinux => Self::Qemu,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Qemu => "qemu",
        }
    }
}
