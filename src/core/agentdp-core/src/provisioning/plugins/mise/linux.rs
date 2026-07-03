use crate::manifest::GuestOs;
use crate::provisioning::bootstrap::ProvisioningBuilder;
use crate::provisioning::guest_os::linux::shell;
use agentdp_protocol::server_guest::BootstrapStepResource;

const MISE_SHIMS_SUFFIX: &str = ".local/share/mise/shims";
const MISE_DOTNET_INSTALLS_SUFFIX: &str = ".local/share/mise/installs/dotnet";

pub(super) fn apply_requirements(builder: &mut ProvisioningBuilder<'_>) {
    match builder.guest_os() {
        GuestOs::Archlinux => builder.add_package("mise"),
        GuestOs::Rocky9 => builder.add_package("curl"),
    }
    builder.add_agent_shell_env(render_mise_agent_env(builder.guest_layout().agent_home));

    builder.add_base_user_step(
        "plugin.mise",
        "Install mise runtimes",
        ["system.agent_user"],
        [BootstrapStepResource::AgentHome, BootstrapStepResource::Mise],
        render_mise_setup(builder.guest_os(), &dedupe(builder.mise_packages())),
    );
}

fn render_mise_setup(guest_os: GuestOs, packages: &[String]) -> String {
    let mut lines = Vec::new();
    if guest_os == GuestOs::Rocky9 {
        lines.push("if ! command -v mise >/dev/null 2>&1; then".to_owned());
        lines.push("  curl https://mise.run | sh".to_owned());
        lines.push("fi".to_owned());
    }
    lines.push("if command -v mise >/dev/null 2>&1; then".to_owned());
    for package in packages {
        lines.push(format!("  mise use --global {}", shell::single_quote(package)));
    }
    lines.push("  mise reshim --force".to_owned());
    let goenv = format!("${{{}:-}}", "GOENV");
    let xdg_config_home = format!("${{{}:-$HOME/.config}}", "XDG_CONFIG_HOME");
    lines.push(format!("  if [ \"{goenv}\" != \"off\" ]; then"));
    lines.push(format!("    agentdp_goenv=\"{goenv}\""));
    lines.push("    if [ -z \"$agentdp_goenv\" ]; then".to_owned());
    lines.push(format!("      agentdp_goenv=\"{xdg_config_home}/go/env\""));
    lines.push("    fi".to_owned());
    lines.push("    if [ -f \"$agentdp_goenv\" ]; then".to_owned());
    lines.push("      agentdp_goenv_tmp=\"$agentdp_goenv.agentdp.$$.tmp\"".to_owned());
    lines.push(
        "      grep -Ev '^(GOBIN|GOROOT|GOTOOLCHAIN)=' \"$agentdp_goenv\" >\"$agentdp_goenv_tmp\" || true".to_owned(),
    );
    lines.push("      install -m 0600 \"$agentdp_goenv_tmp\" \"$agentdp_goenv\"".to_owned());
    lines.push("      rm -f \"$agentdp_goenv_tmp\"".to_owned());
    lines.push("    fi".to_owned());
    lines.push("    unset agentdp_goenv agentdp_goenv_tmp".to_owned());
    lines.push("  fi".to_owned());
    lines.push("  if command -v go >/dev/null 2>&1; then".to_owned());
    lines.push("    go env -u GOBIN || true".to_owned());
    lines.push("    go env -u GOROOT || true".to_owned());
    lines.push("    go env -u GOTOOLCHAIN || true".to_owned());
    lines.push("  fi".to_owned());
    lines.push("fi".to_owned());
    lines.join("\n")
}

fn render_mise_agent_env(agent_home: &str) -> String {
    let shims = format!("{agent_home}/{MISE_SHIMS_SUFFIX}");
    let dotnet = format!("{agent_home}/{MISE_DOTNET_INSTALLS_SUFFIX}");
    format!(
        concat!(
            "export MISE_GO_SET_GOBIN=false\n",
            "export MISE_GO_SET_GOROOT=false\n",
            "agentdp_prepend_path {shims}\n",
            "unset GOBIN GOROOT GOTOOLCHAIN\n",
            "\n",
            "if [ -d {dotnet} ]; then\n",
            "  agentdp_dotnet_root=\"$(find {dotnet} -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1)\"\n",
            "  if [ -n \"$agentdp_dotnet_root\" ]; then\n",
            "    export DOTNET_ROOT=\"$agentdp_dotnet_root\"\n",
            "    export DOTNET_ROOT_X64=\"$DOTNET_ROOT\"\n",
            "  fi\n",
            "  unset agentdp_dotnet_root\n",
            "fi"
        ),
        shims = shell::single_quote(&shims),
        dotnet = shell::single_quote(&dotnet),
    )
}

fn dedupe(packages: &[String]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    packages
        .iter()
        .filter(|package| seen.insert((*package).clone()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;

    use crate::manifest::GuestOs;
    use crate::provisioning::guest_os::linux::shell;

    #[test]
    fn mise_agent_env_keeps_normal_shims_and_disables_go_override_exports() {
        let root = std::env::temp_dir().join(format!("agentdp-mise-env-test-{}", std::process::id()));
        let home = root.join("home");
        fs::create_dir_all(home.join(super::MISE_SHIMS_SUFFIX)).expect("create shims");

        let rendered = super::render_mise_agent_env(home.to_str().expect("home is utf8"));
        let script = format!(
            r#"
PATH=/usr/bin:/bin
agentdp_prepend_path() {{
  agentdp_path_value=
  agentdp_old_ifs=$IFS
  IFS=:
  for agentdp_path_entry in ${{PATH:-}}; do
    if [ -n "$agentdp_path_entry" ] && [ "$agentdp_path_entry" != "$1" ]; then
      agentdp_path_value="${{agentdp_path_value:+$agentdp_path_value:}}$agentdp_path_entry"
    fi
  done
  IFS=$agentdp_old_ifs
  PATH="$1${{agentdp_path_value:+:$agentdp_path_value}}"
}}
export GOBIN=/bad/go/bin
export GOROOT=/bad/go/root
export GOTOOLCHAIN=local
{rendered}
printf '%s\n' "$PATH"
printf 'MISE_GO_SET_GOBIN=%s\n' "${{MISE_GO_SET_GOBIN-}}"
printf 'MISE_GO_SET_GOROOT=%s\n' "${{MISE_GO_SET_GOROOT-}}"
printf 'GOBIN=%s\n' "${{GOBIN-}}"
printf 'GOROOT=%s\n' "${{GOROOT-}}"
printf 'GOTOOLCHAIN=%s\n' "${{GOTOOLCHAIN-}}"
"#
        );
        let path = root.join("env.sh");
        fs::write(&path, script).expect("write env script");
        let output = Command::new("sh")
            .arg("-c")
            .arg(format!(". {}", shell::single_quote(&path.display().to_string())))
            .output()
            .expect("run shell");
        let _ = fs::remove_dir_all(&root);
        assert!(
            output.status.success(),
            "shell failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        let mut lines = stdout.lines();
        let path = lines.next().expect("path line");
        let entries = path.split(':').collect::<Vec<_>>();
        let shims = entries
            .iter()
            .position(|entry| entry.ends_with(super::MISE_SHIMS_SUFFIX))
            .expect("PATH contains mise shims");
        assert_eq!(shims, 0, "expected mise shims first in {path}");
        assert!(stdout.contains("MISE_GO_SET_GOBIN=false\n"));
        assert!(stdout.contains("MISE_GO_SET_GOROOT=false\n"));
        assert!(stdout.contains("GOBIN=\n"));
        assert!(stdout.contains("GOROOT=\n"));
        assert!(stdout.contains("GOTOOLCHAIN=\n"));
    }

    #[test]
    fn mise_setup_scrubs_persisted_go_overrides_before_invoking_go() {
        let root = std::env::temp_dir().join(format!(
            "agentdp-mise-setup-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time after epoch")
                .as_nanos()
        ));
        let bin = root.join("bin");
        let config = root.join("config");
        fs::create_dir_all(&bin).expect("create bin");
        fs::create_dir_all(config.join("go")).expect("create go config");
        let goenv = config.join("go/env");
        fs::write(
            &goenv,
            "GOBIN=/bad/go/bin\nGOROOT=/bad/go/root\nGOTOOLCHAIN=local\nGOPATH=/keep\n",
        )
        .expect("write go env");
        write_executable(&bin.join("mise"), "#!/bin/sh\nexit 0\n");
        write_executable(&bin.join("go"), "#!/bin/sh\nexit 9\n");

        let rendered = format!(
            "set -eu\n{}",
            super::render_mise_setup(GuestOs::Archlinux, &["go@latest".to_owned()])
        );
        let output = Command::new("sh")
            .arg("-c")
            .arg(&rendered)
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .env("HOME", root.join("home"))
            .env("XDG_CONFIG_HOME", &config)
            .env_remove("GOENV")
            .output()
            .expect("run mise setup");
        assert!(
            output.status.success(),
            "mise setup failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let goenv = fs::read_to_string(&goenv).expect("read go env");
        let _ = fs::remove_dir_all(&root);

        assert_eq!(goenv, "GOPATH=/keep\n");
        assert!(rendered.contains("mise reshim --force"));
    }

    fn write_executable(path: &std::path::Path, contents: &str) {
        fs::write(path, contents).expect("write executable");
        let status = Command::new("chmod").arg("755").arg(path).status().expect("run chmod");
        assert!(status.success(), "chmod failed for {}", path.display());
    }
}
