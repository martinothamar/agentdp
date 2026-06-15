#[derive(Debug, Default)]
pub(in crate::provisioning) struct ShellScript {
    contents: String,
}

impl ShellScript {
    pub(in crate::provisioning) fn new() -> Self {
        Self {
            contents: String::with_capacity(4096),
        }
    }

    pub(in crate::provisioning) fn line(&mut self, line: impl AsRef<str>) {
        self.contents.push_str(line.as_ref());
        self.contents.push('\n');
    }

    pub(in crate::provisioning) fn blank(&mut self) {
        self.contents.push('\n');
    }

    pub(in crate::provisioning) fn block(&mut self, block: &str) {
        self.contents.push_str(block.trim_end_matches('\n'));
        self.contents.push('\n');
    }

    pub(in crate::provisioning) fn render(mut self) -> String {
        if self.contents.ends_with('\n') {
            self.contents.pop();
        }
        self.contents
    }
}

pub(in crate::provisioning) fn single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub(in crate::provisioning) fn double_quoted_fragment(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
        .replace('`', "\\`")
}
