use std::sync::Once;

/// Installs the workspace's ring-backed rustls provider once for the current process.
pub fn install_default_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _installed = rustls::crypto::ring::default_provider().install_default();
    });
}
