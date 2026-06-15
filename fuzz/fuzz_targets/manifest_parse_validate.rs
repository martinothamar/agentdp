#![no_main]

use agentdp_core::manifest::AgentManifest;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    if let Ok(contents) = std::str::from_utf8(input)
        && let Ok(manifest) = serde_yaml::from_str::<AgentManifest>(contents)
    {
        let _ = manifest.validate();
    }
});
