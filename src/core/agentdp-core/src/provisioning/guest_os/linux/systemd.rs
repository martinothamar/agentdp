pub(in crate::provisioning) fn enable_service_if_present(service: &str) -> String {
    format!(
        "if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files {service} >/dev/null 2>&1; then\n  systemctl enable --now {service}\nfi"
    )
}
