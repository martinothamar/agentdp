#![no_main]

use agentdp_core::provisioning::secrets::SecretBindings;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let allowed_hosts = ["api.github.com".to_owned(), "api.openai.com".to_owned()];
    let Ok(bindings) = SecretBindings::from_env_bytes(input, &allowed_hosts) else {
        return;
    };

    let guest_env = bindings.guest_env_contents();
    let _ = format!("{bindings:?}");
    let _ = String::from_utf8_lossy(&guest_env);
});
