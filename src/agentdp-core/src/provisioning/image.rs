use crate::manifest::{AgentManifest, GuestOs};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageRequest {
    pub os: GuestOs,
}

impl ImageRequest {
    pub(crate) const fn from_manifest(manifest: &AgentManifest) -> Self {
        Self { os: manifest.image.os }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogImage {
    pub os: GuestOs,
    pub architecture: ImageArchitecture,
    pub variant: ImageVariant,
}

impl CatalogImage {
    #[must_use]
    pub const fn os_name(self) -> &'static str {
        match self.os {
            GuestOs::Archlinux => "archlinux",
        }
    }

    #[must_use]
    pub const fn architecture_name(self) -> &'static str {
        self.architecture.name()
    }

    #[must_use]
    pub const fn variant_name(self) -> &'static str {
        self.variant.name()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageArchitecture {
    X86_64,
}

impl ImageArchitecture {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageVariant {
    Cloud,
}

impl ImageVariant {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Cloud => "cloud",
        }
    }
}

pub struct ImageCatalog;

impl ImageCatalog {
    #[must_use]
    pub const fn resolve(request: ImageRequest) -> CatalogImage {
        match request.os {
            GuestOs::Archlinux => CatalogImage {
                os: GuestOs::Archlinux,
                architecture: ImageArchitecture::X86_64,
                variant: ImageVariant::Cloud,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::GuestOs;
    use crate::provisioning::image::{ImageArchitecture, ImageCatalog, ImageRequest, ImageVariant};

    #[test]
    fn resolves_archlinux_to_cloud_x86_64_catalog_image() {
        let image = ImageCatalog::resolve(ImageRequest { os: GuestOs::Archlinux });

        assert_eq!(image.os, GuestOs::Archlinux);
        assert_eq!(image.architecture, ImageArchitecture::X86_64);
        assert_eq!(image.variant, ImageVariant::Cloud);
        assert_eq!(image.os_name(), "archlinux");
        assert_eq!(image.architecture_name(), "x86_64");
        assert_eq!(image.variant_name(), "cloud");
    }
}
