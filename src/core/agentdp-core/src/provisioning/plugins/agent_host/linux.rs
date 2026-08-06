use agentdp_protocol::server_guest::BootstrapStepResource;

use crate::manifest::plugins::agent_host::SERVICE;
use crate::provisioning::bootstrap::{HealthcheckKind, HealthcheckPlan, ProvisioningBuilder};
use crate::provisioning::guest_os::linux::{paths, shell};

// Agent Host is an AgentDP-managed runtime, not user-facing manifest configuration.
// When bumping this pin, update both values and verify every AGENT_HOST_PATCHES
// entry against the new bundle.
const PINNED_AGENT_HOST_URL: &str =
    "https://update.code.visualstudio.com/commit:780ea331b2861816fe6bb8215d812933c81df83b/server-linux-x64/insider";
const PINNED_AGENT_HOST_SHA256: &str = "126cf3b0b7bccbc30b97d74446d29604f69fcf216f5ae4d24b117194bea2698f";

pub(super) fn apply(builder: &mut ProvisioningBuilder<'_>) {
    let Some(guest_port) = builder.guest_port(SERVICE) else {
        return;
    };
    let user = builder.agent_user();
    let user_name = user.name.clone();
    let group = user.linux().group.clone().unwrap_or_else(|| user_name.clone());
    let agent_home = builder.guest_layout().agent_home;
    let code_dir = builder.code_dir();
    builder.add_package("curl");
    builder.add_package("jq");
    builder.add_base_system_step(
        "plugin.agent_host.install",
        "Install VS Code Agent Host",
        ["system.agent_user"],
        [],
        render_install(),
    );
    builder.add_base_system_step(
        "plugin.agent_host.service",
        "Configure VS Code Agent Host service",
        ["plugin.agent_host.install", "system.agent_user"],
        [BootstrapStepResource::Systemd],
        render_service(&user_name, &group, agent_home, code_dir, guest_port),
    );
    builder.add_instance_user_step(
        "plugin.agent_host.config",
        "Configure VS Code Agent Host",
        [],
        [BootstrapStepResource::AgentHome, BootstrapStepResource::Systemd],
        render_config(agent_home),
    );
    builder.add_healthcheck_if_absent(HealthcheckPlan {
        name: SERVICE.to_owned(),
        kind: HealthcheckKind::Tcp {
            target: format!("127.0.0.1:{guest_port}"),
        },
        timeout: Some("5m".to_owned()),
    });
}

fn render_install() -> String {
    let mut script = shell::ShellScript::new();
    let install_dir = "/opt/agentdp/vscode-agent-host";
    let server_dir = format!("{install_dir}/server");
    script.line("archive=\"$(mktemp)\"");
    script.line("trap 'rm -f \"$archive\"' EXIT");
    script.line(format!(
        "curl -fL --retry 3 --output \"$archive\" {}",
        shell::single_quote(PINNED_AGENT_HOST_URL)
    ));
    script.line(format!(
        "printf '%s  %s\\n' {} \"$archive\" | sha256sum --check --status",
        shell::single_quote(PINNED_AGENT_HOST_SHA256)
    ));
    script.line(format!("install -d -m 0755 {}", shell::single_quote(&server_dir)));
    script.line(format!(
        "tar --extract --gzip --file \"$archive\" --directory {} --strip-components=1",
        shell::single_quote(&server_dir)
    ));
    script.line(format!(
        "bundle={}",
        shell::single_quote(&format!("{server_dir}/out/vs/platform/agentHost/node/agentHostMain.js"))
    ));
    script.block(
        "patch_bundle() {\n\
         \x20 description=$1\n\
         \x20 before=$2\n\
         \x20 after=$3\n\
         \x20 count=\"$(grep -oF \"$before\" \"$bundle\" | wc -l)\"\n\
         \x20 if [ \"$count\" -ne 1 ]; then\n\
         \x20   echo \"expected exactly one patch location to $description; found $count\" >&2\n\
         \x20   return 1\n\
         \x20 fi\n\
         \x20 jq --raw-input --raw-output --slurp --arg before \"$before\" --arg after \"$after\" \
         \x20   'split($before) | if length == 2 then join($after) else error(\"unexpected patch count\") end' \
         \x20   \"$bundle\" >\"$bundle.tmp\"\n\
         \x20 mv \"$bundle.tmp\" \"$bundle\"\n\
         }",
    );
    for (before, after, description) in AGENT_HOST_PATCHES {
        script.line(format!(
            "patch_bundle {} {} {}",
            shell::single_quote(description),
            shell::single_quote(before),
            shell::single_quote(after)
        ));
    }
    script.line(format!(
        "chmod 0755 {}",
        shell::single_quote(&format!("{server_dir}/bin/code-server-insiders"))
    ));
    script.render()
}

const AGENT_HOST_PATCHES: &[(&str, &str, &str)] = &[
    (
        "f.registerProvider(h.createInstance(Qe)),Ug(process.env[tP],!0)&&(!o.isBuilt||ne.isAvailable(pc))&&f.registerProvider(h.createInstance(Ac)),",
        "Ug(process.env[tP],!1)&&(!o.isBuilt||ne.isAvailable(pc))&&f.registerProvider(h.createInstance(Ac)),",
        "disable the bundled Copilot provider and default Claude provider",
    ),
    (
        "protectedResources:a.length>0?a:void 0",
        "protectedResources:void 0",
        "hide protected resources from desktop clients",
    ),
    (
        r"async authenticate(n,e){this._logService.trace(`[AgentHostAuthenticationService] authenticate called: resource=${n.resource}`);",
        r"async authenticate(n,e){return{authenticated:!1};this._logService.trace(`[AgentHostAuthenticationService] authenticate called: resource=${n.resource}`);",
        "reject protected-resource bearer tokens",
    ),
    (
        ",u={web_search:AU(o[\"codex.webSearchMode\"])??co[\"codex.webSearchMode\"],...c.config},p=Object.keys(d);",
        ",u={...c.config},p=Object.keys(d);",
        "inherit web_search from the guest Codex config",
    ),
    (
        "var rS=[\"default\",\"auto-review\",\"full-access\"],Dc=\"default\";",
        "var rS=[\"default\",\"auto-review\",\"full-access\"],Dc=\"full-access\";",
        "default new sessions to Full Access",
    ),
    (
        "\"codex.permissionsPreset\":Dc,\"codex.approvalPolicy\":\"on-request\",\"codex.sandboxMode\":\"workspace-write\"",
        "\"codex.permissionsPreset\":Dc,\"codex.approvalPolicy\":\"never\",\"codex.sandboxMode\":\"danger-full-access\"",
        "default new sessions to unrestricted Codex permissions",
    ),
    (
        "enumDescriptions:Oc.map(r=>Ra(r)??\"\"),default:\"medium\",sessionMutable:!0",
        "enumDescriptions:Oc.map(r=>Ra(r)??\"\"),default:\"high\",sessionMutable:!0",
        "advertise high as the default reasoning effort",
    ),
    (
        "\"codex.modelReasoningEffort\":\"medium\"",
        "\"codex.modelReasoningEffort\":\"high\"",
        "resolve high as the default reasoning effort",
    ),
    (
        "description:g(973,null),default:\"medium\",enum:[...Oc]",
        "description:g(973,null),default:\"high\",enum:[...Oc]",
        "default the model picker to high reasoning effort",
    ),
    // `ae` is this pinned bundle's parseRequiredSessionUriFromChatUri helper.
    // Normalizing here matters because providers may invoke server tools on a
    // chat URI while guestctl stores the owning session URI.
    (
        "function l_(r){return[v0,P0(r)]}",
        r#"function l_(r){return[v0,P0(r),{definitions:[{name:"agentdp_register_pr",title:"Register Pull Request",description:"Register a GitHub pull request for event notifications in this Agent Host session. Call this once after creating each pull request.",inputSchema:{type:"object",properties:{url:{type:"string",description:"Full GitHub pull request URL."}},required:["url"]},annotations:{readOnlyHint:!1,idempotentHint:!0}},{name:"agentdp_unregister_pr",title:"Unregister Pull Request",description:"Stop pull request event notifications previously registered by this Agent Host session.",inputSchema:{type:"object",properties:{url:{type:"string",description:"Full GitHub pull request URL."}},required:["url"]},annotations:{readOnlyHint:!1,idempotentHint:!0}}],execute(n,e,t,r){if(t!=="agentdp_register_pr"&&t!=="agentdp_unregister_pr")throw new Error(`Unknown AgentDP server tool: ${t}`);if(r===null||typeof r!=="object"||Array.isArray(r)||typeof r.url!=="string"||r.url.length===0)throw new Error(`${t} requires a pull request URL`);let i=t==="agentdp_register_pr"?"register-agent-host":"unregister-agent-host",l=e.toString(),d=l.startsWith("ahp-chat:")?ae(l):l;return new Promise((s,a)=>{process.getBuiltinModule("child_process").execFile("/usr/local/bin/guestctl",["pr",i,d,r.url],{encoding:"utf8",timeout:6e4},(l,c,d)=>{if(l){a(new Error((d.trim()||c.trim()||l.message)));return}s(c.trim()||r.url)})})}}]}"#,
        "add session-bound AgentDP PR server tools",
    ),
];

fn render_service(user: &str, group: &str, agent_home: &str, code_dir: &str, guest_port: u16) -> String {
    let unit = format!(
        "[Unit]\n\
         Description=agentdp VS Code Agent Host\n\
         After=network-online.target agentdp-runtime-env.service\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         User={}\n\
         Group={}\n\
         WorkingDirectory={}\n\
         Environment=HOME={}\n\
         Environment=CODEX_HOME={}/.codex\n\
         Environment=VSCODE_AGENT_HOST_CLAUDE_AGENT_ENABLED=false\n\
         Environment=VSCODE_AGENT_HOST_CODEX_AGENT_ENABLED=true\n\
         Environment=VSCODE_AGENT_HOST_CODEX_SDK_ROOT={}/.local/share/agentdp/codex\n\
         RuntimeDirectory=agentdp-agent-host\n\
         ExecStart={} /opt/agentdp/vscode-agent-host/server/bin/code-server-insiders --start-server --accept-server-license-terms --socket-path /run/agentdp-agent-host/code-server.sock --agent-host-port {} --host 0.0.0.0 --without-connection-token --server-data-dir {}/.local/share/agentdp/vscode-agent-host/data\n\
         Restart=always\n\
         RestartSec=2\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target",
        user,
        group,
        code_dir,
        agent_home,
        agent_home,
        agent_home,
        paths::AGENT_ENV_PATH,
        guest_port,
        agent_home,
    );
    let mut script = shell::ShellScript::new();
    script.line("install -d -m 0755 /etc/systemd/user/guestd.service.d");
    script.line("cat >/etc/systemd/user/guestd.service.d/agent-host.conf <<'EOF'");
    script.line("[Service]");
    script.line(format!(
        "Environment=AGENTDP_AGENT_HOST_URL=ws://127.0.0.1:{guest_port}"
    ));
    script.line("EOF");
    script.line("cat >/etc/systemd/system/agent-host.service <<'EOF'");
    script.block(&unit);
    script.line("EOF");
    script.line("systemctl daemon-reload");
    script.line("systemctl enable --now agent-host.service");
    script.render()
}

fn render_config(agent_home: &str) -> String {
    let config_dir = format!("{agent_home}/.local/share/agentdp/vscode-agent-host/data/data/User/globalStorage");
    let config = format!("{config_dir}/agent-host-config.json");
    let mut script = shell::ShellScript::new();
    script.line(format!("install -d -m 0700 {}", shell::single_quote(&config_dir)));
    script.line(format!("config={}", shell::single_quote(&config)));
    script.line("tmp=\"$config.tmp.$$\"");
    script.line("trap 'rm -f \"$tmp\"' EXIT");
    script.line("if [ -f \"$config\" ]; then");
    script.line("  jq '.codexUsageSource = \"openai\"' \"$config\" >\"$tmp\"");
    script.line("else");
    script.line("  jq --null-input '{ codexUsageSource: \"openai\" }' >\"$tmp\"");
    script.line("fi");
    script.line("chmod 0600 \"$tmp\"");
    script.line("mv \"$tmp\" \"$config\"");
    script.line("trap - EXIT");
    script.line("sudo -n systemctl restart agent-host.service");
    script.render()
}

#[cfg(test)]
mod tests {
    #[test]
    fn install_disables_unintended_providers_and_protected_resource_authentication() {
        let script = super::render_install();

        assert!(script.contains("disable the bundled Copilot provider and default Claude provider"));
        assert!(script.contains("hide protected resources from desktop clients"));
        assert!(script.contains("reject protected-resource bearer tokens"));
        assert!(script.contains("protectedResources:void 0"));
        assert!(script.contains("return{authenticated:!1}"));
        assert!(script.contains("add session-bound AgentDP PR server tools"));
        assert!(script.contains("agentdp_register_pr"));
        assert!(script.contains("agentdp_unregister_pr"));
    }

    #[test]
    fn service_waits_for_the_runtime_environment() {
        let script = super::render_service("agent", "agent", "/data/home", "/data/home/code", 18_765);

        assert!(script.contains("After=network-online.target agentdp-runtime-env.service"));
        assert!(script.contains("Environment=VSCODE_AGENT_HOST_CLAUDE_AGENT_ENABLED=false"));
    }
}
