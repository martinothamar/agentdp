use std::fmt::Write as _;

use agentdp_platform::ca::{CA_ENV_VARS_KEY, ca_env_vars_csv, ca_env_vars_from_env};

pub(crate) const INJECTION_MARKER: &str = "# agentdp CA bundle";
pub(crate) const CA_CONTAINER_PATH: &str = "/tmp/agentdp-ca-bundle.crt";

pub(crate) fn inject_ca(input: &str, copy_instruction: &str) -> String {
    if input.contains(INJECTION_MARKER) {
        return input.to_owned();
    }
    let ca_snippet = ca_snippet(copy_instruction);
    let lines = input.lines().collect::<Vec<_>>();
    let stages = stages_with_run(&lines);
    let mut stage_index = 0usize;
    let mut output = String::with_capacity(input.len() + ca_snippet.len());
    for line in &lines {
        output.push_str(line);
        output.push('\n');
        if from_image(line).is_some() {
            let inject = stages.get(stage_index).copied().unwrap_or(false);
            stage_index += 1;
            if inject {
                output.push_str(&ca_snippet);
            }
        }
    }
    if input.is_empty() {
        return output;
    }
    if !input.ends_with('\n') && output.ends_with('\n') {
        output.pop();
    }
    output
}

fn stages_with_run(lines: &[&str]) -> Vec<bool> {
    let mut stages = Vec::new();
    for line in lines {
        if let Some(image) = from_image(line) {
            stages.push((!image.eq_ignore_ascii_case("scratch"), false));
        } else if is_run(line)
            && let Some(stage) = stages.last_mut()
        {
            stage.1 = true;
        }
    }
    stages
        .into_iter()
        .map(|(non_scratch, has_run)| non_scratch && has_run)
        .collect()
}

fn from_image(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if !trimmed
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("FROM"))
    {
        return None;
    }
    let rest = trimmed[4..].trim_start();
    if rest.is_empty() {
        return None;
    }
    let mut parts = rest.split_whitespace().peekable();
    while parts.peek().is_some_and(|part| part.starts_with("--")) {
        let _ = parts.next();
    }
    parts.next()
}

fn is_run(line: &str) -> bool {
    let trimmed = line.trim_start();
    if !trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("RUN"))
    {
        return false;
    }
    trimmed.as_bytes().get(3).is_none_or(u8::is_ascii_whitespace)
}

fn ca_snippet(copy_instruction: &str) -> String {
    let mut snippet = format!(
        "\
{INJECTION_MARKER}
{copy_instruction}
"
    );
    let env_vars = ca_env_vars_from_env();
    for key in &env_vars {
        let _ = writeln!(snippet, "ENV {key}={CA_CONTAINER_PATH}");
    }
    let _ = writeln!(snippet, "ENV {CA_ENV_VARS_KEY}={}", ca_env_vars_csv(&env_vars));
    let _ = write!(
        snippet,
        "\
RUN set -u; \\
    (mkdir -p /usr/local/share/ca-certificates && \\
     cp {CA_CONTAINER_PATH} /usr/local/share/ca-certificates/agentdp-ca-bundle.crt) || \\
      echo 'agentdp docker proxy: Debian CA source install failed'; \\
    (mkdir -p /etc/pki/ca-trust/source/anchors && \\
     cp {CA_CONTAINER_PATH} /etc/pki/ca-trust/source/anchors/agentdp-ca-bundle.crt) || \\
      echo 'agentdp docker proxy: RHEL CA source install failed'; \\
    if command -v update-ca-certificates >/dev/null 2>&1; then \\
      update-ca-certificates || echo 'agentdp docker proxy: update-ca-certificates failed'; \\
    elif command -v update-ca-trust >/dev/null 2>&1; then \\
      update-ca-trust extract || echo 'agentdp docker proxy: update-ca-trust failed'; \\
    elif [ -w /etc/ssl/certs/ca-certificates.crt ]; then \\
      cat {CA_CONTAINER_PATH} >> /etc/ssl/certs/ca-certificates.crt || echo 'agentdp docker proxy: CA bundle append failed'; \\
    elif [ -w /etc/ssl/cert.pem ]; then \\
      cat {CA_CONTAINER_PATH} >> /etc/ssl/cert.pem || echo 'agentdp docker proxy: CA bundle append failed'; \\
    else \\
      echo 'agentdp docker proxy: no active CA trust store found; installed CA source anchors for later package setup'; \\
    fi
"
    );
    snippet
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONTEXT_COPY: &str = "COPY .agentdp/ca-bundle.crt /tmp/agentdp-ca-bundle.crt";
    const NAMED_CONTEXT_COPY: &str = "COPY --from=agentdp_ca_bundle ca-bundle.pem /tmp/agentdp-ca-bundle.crt";

    #[test]
    fn injects_each_non_scratch_stage_with_run() {
        let input = "\
FROM alpine AS build
RUN echo build
FROM scratch
COPY --from=build /out /out
FROM --platform=linux/amd64 debian:stable
RUN echo final
";

        let output = inject_ca(input, CONTEXT_COPY);

        assert_eq!(output.matches(INJECTION_MARKER).count(), 2);
        assert!(output.contains(CONTEXT_COPY));
        assert!(output.contains("update-ca-certificates"));
        assert!(output.contains("update-ca-trust extract"));
        assert!(output.contains("cat /tmp/agentdp-ca-bundle.crt >> /etc/ssl/certs/ca-certificates.crt"));
        assert!(
            output.find("/usr/local/share/ca-certificates/agentdp-ca-bundle.crt")
                < output.find("command -v update-ca-certificates")
        );
        assert!(
            output.find("/etc/pki/ca-trust/source/anchors/agentdp-ca-bundle.crt")
                < output.find("command -v update-ca-trust")
        );
        assert!(output.find("ENV SSL_CERT_FILE=/tmp/agentdp-ca-bundle.crt") < output.find("RUN set -u"));
        for key in agentdp_platform::ca::DEFAULT_CA_ENV_VARS {
            assert!(output.contains(&format!("ENV {key}=/tmp/agentdp-ca-bundle.crt")));
        }
        assert!(output.contains(&format!(
            "ENV {CA_ENV_VARS_KEY}={}",
            agentdp_platform::ca::default_ca_env_vars_csv()
        )));
    }

    #[test]
    fn skips_stages_without_run() {
        let input = "\
FROM alpine AS build
RUN echo build
FROM gcr.io/distroless/static-debian12
COPY --from=build /out /out
";

        let output = inject_ca(input, CONTEXT_COPY);

        assert_eq!(output.matches(INJECTION_MARKER).count(), 1);
        assert!(output.contains("FROM alpine AS build\n# agentdp CA bundle"));
        assert!(!output.contains("FROM gcr.io/distroless/static-debian12\n# agentdp CA bundle"));
    }

    #[test]
    fn skips_scratch_even_with_run() {
        let input = "\
FROM scratch
RUN echo impossible
";

        let output = inject_ca(input, CONTEXT_COPY);

        assert!(!output.contains(INJECTION_MARKER));
    }

    #[test]
    fn uses_caller_copy_instruction() {
        let input = "\
FROM alpine AS build
RUN echo build
";

        let output = inject_ca(input, NAMED_CONTEXT_COPY);

        assert_eq!(output.matches(INJECTION_MARKER).count(), 1);
        assert!(output.contains(NAMED_CONTEXT_COPY));
    }
}
