use std::sync::Once;

pub(crate) fn install_default_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _installed = rustls::crypto::ring::default_provider().install_default();
    });
}
