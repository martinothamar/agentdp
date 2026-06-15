use crate::manifest::{HostAlias, User};

use super::{AGENT_HOME, shell, templates};

pub(in crate::provisioning::guest_os) fn root_setup(user: &User, host_aliases: &[HostAlias]) -> Vec<String> {
    let mut setup = vec![
        render_grow_root_filesystem(),
        render_default_tmux_config(user),
        render_hostname_sync_service(),
    ];
    if !host_aliases.is_empty() {
        setup.push(render_host_aliases(host_aliases));
    }
    setup
}

pub(in crate::provisioning::guest_os) fn pre_user_boot_commands(user: &User) -> Vec<String> {
    let mut commands = Vec::new();
    let linux_user = user.linux();
    if linux_user.uid.is_some_and(|uid| uid > 60000) || linux_user.gid.is_some_and(|gid| gid > 60000) {
        commands.push(
            "sed -i -E 's/^UID_MAX.*/UID_MAX 2147483647/; s/^GID_MAX.*/GID_MAX 2147483647/' /etc/login.defs".to_owned(),
        );
    }
    if linux_user.group.is_some() || linux_user.gid.is_some() {
        let group = linux_user.group.as_deref().unwrap_or(&user.name);
        let gid_arg = linux_user.gid.map_or(String::new(), |gid| format!(" -g {gid}"));
        commands.push(format!(
            "getent group {group} >/dev/null 2>&1 || groupadd{gid_arg} {group}",
            group = shell::single_quote(group),
        ));
    }
    commands
}

fn render_default_tmux_config(user: &User) -> String {
    let primary_group = user.linux().group.as_deref().unwrap_or(&user.name);
    let mut script = shell::ShellScript::new();
    script.line(format!(
        "install -d -o {} -g {} -m 0700 {}",
        shell::single_quote(&user.name),
        shell::single_quote(primary_group),
        shell::single_quote(AGENT_HOME)
    ));
    script.line(format!(
        "if [ ! -e {} ]; then",
        shell::single_quote(&format!("{AGENT_HOME}/.tmux.conf"))
    ));
    script.line(format!(
        "  cat >{} <<'EOF'",
        shell::single_quote(&format!("{AGENT_HOME}/.tmux.conf"))
    ));
    script.block(templates::TMUX_CONF);
    script.line("EOF");
    script.line(format!(
        "  chown {}:{} {}",
        shell::single_quote(&user.name),
        shell::single_quote(primary_group),
        shell::single_quote(&format!("{AGENT_HOME}/.tmux.conf"))
    ));
    script.line(format!(
        "  chmod 0600 {}",
        shell::single_quote(&format!("{AGENT_HOME}/.tmux.conf"))
    ));
    script.line("fi");
    script.render()
}

fn render_host_aliases(aliases: &[HostAlias]) -> String {
    let mut script = shell::ShellScript::new();
    for alias in aliases {
        let names = alias.names.iter().map(String::as_str).collect::<Vec<_>>().join(" ");
        let pattern = alias
            .names
            .iter()
            .map(|name| format!("(^|[[:space:]]){}([[:space:]]|$)", regex_escape_for_grep(name)))
            .collect::<Vec<_>>()
            .join("|");
        script.line(format!(
            "if ! grep -Eq {} /etc/hosts; then",
            shell::single_quote(&pattern)
        ));
        script.line(format!(
            "  printf '%s\\n' {} >>/etc/hosts",
            shell::single_quote(&format!("{} {}", alias.address, names))
        ));
        script.line("fi");
    }
    script.render()
}

fn regex_escape_for_grep(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '.' | '[' | ']' | '(' | ')' | '{' | '}' | '*' | '+' | '?' | '^' | '$' | '|' | '\\' => {
                vec!['\\', character]
            }
            other => vec![other],
        })
        .collect()
}

fn render_grow_root_filesystem() -> String {
    let mut script = shell::ShellScript::new();
    script.line("grow_agentdp_root() {");
    script.line("  local root_source root_fstype parent partnum grow_output");
    script.line("  root_source=\"$(findmnt -no SOURCE / || true)\"");
    script.line("  root_fstype=\"$(findmnt -no FSTYPE / || true)\"");
    script.line("  if [ -z \"$root_source\" ] || [ ! -b \"$root_source\" ]; then");
    script.line("    echo \"agentdp grow-root skipped: root source is not a block device: $root_source\"");
    script.line("    return 0");
    script.line("  fi");
    script.line("  parent=\"$(lsblk -no PKNAME \"$root_source\" | head -n1 || true)\"");
    script.line("  partnum=\"$(lsblk -no PARTN \"$root_source\" | head -n1 || true)\"");
    script.line("  if [ -z \"$parent\" ] || [ -z \"$partnum\" ]; then");
    script.line("    echo \"agentdp grow-root skipped: root source is not a partition: $root_source\"");
    script.line("    return 0");
    script.line("  fi");
    script.line("  echo \"Growing root partition $root_source on /dev/$parent\"");
    script.line("  if ! grow_output=\"$(growpart \"/dev/$parent\" \"$partnum\" 2>&1)\"; then");
    script.line("    if ! printf '%s\\n' \"$grow_output\" | grep -qi 'NOCHANGE'; then");
    script.line("      printf '%s\\n' \"$grow_output\" >&2");
    script.line("      return 1");
    script.line("    fi");
    script.line("  fi");
    script.line("  if [ -n \"$grow_output\" ]; then");
    script.line("    printf '%s\\n' \"$grow_output\"");
    script.line("  fi");
    script.line("  udevadm settle || true");
    script.line("  case \"$root_fstype\" in");
    script.line("    ext2|ext3|ext4)");
    script.line("      resize2fs \"$root_source\"");
    script.line("      ;;");
    script.line("    xfs)");
    script.line("      if command -v xfs_growfs >/dev/null 2>&1; then");
    script.line("        xfs_growfs /");
    script.line("      else");
    script.line("        echo \"agentdp grow-root skipped: xfs_growfs is not installed\"");
    script.line("      fi");
    script.line("      ;;");
    script.line("    btrfs)");
    script.line("      if command -v btrfs >/dev/null 2>&1; then");
    script.line("        btrfs filesystem resize max /");
    script.line("      else");
    script.line("        echo \"agentdp grow-root skipped: btrfs is not installed\"");
    script.line("      fi");
    script.line("      ;;");
    script.line("    *)");
    script.line("      echo \"agentdp grow-root skipped: unsupported root filesystem: $root_fstype\"");
    script.line("      ;;");
    script.line("  esac");
    script.line("}");
    script.line("grow_agentdp_root");
    script.line("unset -f grow_agentdp_root");
    script.render()
}

fn render_hostname_sync_service() -> String {
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 /usr/local/lib/agentdp");
    script.line("cat >/usr/local/lib/agentdp/sync-hostname-from-seed.sh <<'EOF'");
    script.line("#!/usr/bin/env sh");
    script.line("set -eu");
    script.line("device=\"$(blkid -L CIDATA 2>/dev/null || blkid -L cidata 2>/dev/null || true)\"");
    script.line("[ -n \"$device\" ] || exit 0");
    script.line("mount_dir=\"$(mktemp -d)\"");
    script.line("cleanup() {");
    script.line("  umount \"$mount_dir\" >/dev/null 2>&1 || true");
    script.line("  rmdir \"$mount_dir\" >/dev/null 2>&1 || true");
    script.line("}");
    script.line("trap cleanup EXIT");
    script.line("mount -o ro \"$device\" \"$mount_dir\" >/dev/null 2>&1 || exit 0");
    script.line(
        "hostname=\"$(sed -n 's/^local-hostname:[[:space:]]*//p' \"$mount_dir/meta-data\" 2>/dev/null | head -n1 | tr -d \"\\\"'\")\"",
    );
    script.line("case \"$hostname\" in");
    script.line("  \"\"|.*|-*|*-|*[!A-Za-z0-9.-]*) exit 0 ;;");
    script.line("esac");
    script.line("hostnamectl set-hostname \"$hostname\" || printf '%s\\n' \"$hostname\" >/etc/hostname");
    script.line("EOF");
    script.line("chmod 0755 /usr/local/lib/agentdp/sync-hostname-from-seed.sh");
    script.line("cat >/etc/systemd/system/agentdp-hostname.service <<'EOF'");
    script.line("[Unit]");
    script.line("Description=Sync agentdp guest hostname from cloud-init seed");
    script.line("After=local-fs.target");
    script.line("");
    script.line("[Service]");
    script.line("Type=oneshot");
    script.line("ExecStart=/usr/local/lib/agentdp/sync-hostname-from-seed.sh");
    script.line("");
    script.line("[Install]");
    script.line("WantedBy=multi-user.target");
    script.line("EOF");
    script.line("systemctl daemon-reload");
    script.line("systemctl enable agentdp-hostname.service");
    script.line("/usr/local/lib/agentdp/sync-hostname-from-seed.sh || true");
    script.render()
}
