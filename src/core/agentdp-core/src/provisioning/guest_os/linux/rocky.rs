use crate::manifest::{HostAlias, User};
use crate::provisioning::guest_os::{GuestBootOptions, linux};
use crate::provisioning::image::{CatalogImage, ImageArchitecture, ImageVariant};

pub(in crate::provisioning::guest_os) fn base_packages(needs_git: bool) -> Vec<String> {
    let mut packages = vec![
        "sudo".to_owned(),
        "cloud-utils-growpart".to_owned(),
        "gdisk".to_owned(),
        "gzip".to_owned(),
        "hostname".to_owned(),
        "tmux".to_owned(),
        "policycoreutils".to_owned(),
        "policycoreutils-python-utils".to_owned(),
    ];
    if needs_git {
        packages.push("git".to_owned());
    }
    packages
}

pub(in crate::provisioning::guest_os) const fn catalog_image() -> CatalogImage {
    CatalogImage {
        os: crate::manifest::GuestOs::Rocky9,
        architecture: ImageArchitecture::X86_64,
        variant: ImageVariant::Cloud,
    }
}

pub(in crate::provisioning::guest_os) fn boot_commands(options: GuestBootOptions) -> Vec<String> {
    if options.install_ca {
        vec![options.ca_bundle_command]
    } else {
        Vec::new()
    }
}

pub(in crate::provisioning::guest_os) fn root_setup(user: &User, host_aliases: &[HostAlias]) -> Vec<String> {
    linux::root_setup(user, host_aliases)
}

pub(in crate::provisioning::guest_os) const fn ca_bundle_install() -> &'static str {
    "if [ -f /var/lib/agentdp/ca/ca-bundle.pem ]; then\n  install -d -m 0755 /etc/pki/ca-trust/source/anchors\n  install -m 0644 /var/lib/agentdp/ca/ca-bundle.pem /etc/pki/ca-trust/source/anchors/agentdp-ca-bundle.crt\n  if command -v update-ca-trust >/dev/null 2>&1; then update-ca-trust extract || true; fi\nfi"
}
