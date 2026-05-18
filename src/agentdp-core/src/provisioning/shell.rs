#[derive(Debug, Default)]
pub(super) struct ShellScript {
    contents: String,
}

impl ShellScript {
    pub(super) fn new() -> Self {
        Self {
            contents: String::with_capacity(4096),
        }
    }

    pub(super) fn line(&mut self, line: impl AsRef<str>) {
        self.contents.push_str(line.as_ref());
        self.contents.push('\n');
    }

    pub(super) fn blank(&mut self) {
        self.contents.push('\n');
    }

    pub(super) fn block(&mut self, block: &str) {
        self.contents.push_str(block.trim_end_matches('\n'));
        self.contents.push('\n');
    }

    pub(super) fn render(mut self) -> String {
        if self.contents.ends_with('\n') {
            self.contents.pop();
        }
        self.contents
    }
}

pub(super) fn single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub(super) fn double_quoted_fragment(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
        .replace('`', "\\`")
}

pub(super) fn render_template(template: &str, replacements: &[(&str, String)]) -> String {
    let mut rendered = template.to_owned();
    for (placeholder, value) in replacements {
        rendered = rendered.replace(placeholder, value);
    }
    rendered
}

pub(super) fn enable_systemd_service_if_present(service: &str) -> String {
    format!(
        "if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files {service} >/dev/null 2>&1; then\n  systemctl enable --now {service}\nfi"
    )
}
