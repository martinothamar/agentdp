use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::command::{CommandSnapshot, TestContext, agentctl_path_for_support};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
pub enum QemuSystemMode {
    StaticPid,
    SpawnSleep,
    DelayStart,
    DelayBootstrapFinished,
}

pub struct ServerFixture {
    real_server: PathBuf,
    wrapper: PathBuf,
    guest_tool_dir: PathBuf,
    codex_auth: PathBuf,
    pid_file: PathBuf,
    _guard: ServerProcessGuard,
    runtime: ShortRuntimeDir,
}

impl ServerFixture {
    pub fn new(context: &TestContext) -> Self {
        let real_server = agentdp_server_path();
        let wrapper = write_server_wrapper(context);
        let guest_tool_dir = write_fake_guest_tool_binaries(context);
        let codex_auth = write_fake_codex_auth(context);
        let pid_file = context.path().join("server.pid");
        let runtime = ShortRuntimeDir::create();
        let guard = ServerProcessGuard::new(pid_file.clone());
        Self {
            real_server,
            wrapper,
            guest_tool_dir,
            codex_auth,
            pid_file,
            runtime,
            _guard: guard,
        }
    }

    pub fn run_doctor(&self, context: &TestContext) -> CommandSnapshot {
        let mut command = self.command(context);
        command.arg("doctor");
        self.run(context, command, "run agentctl doctor")
    }

    fn command(&self, context: &TestContext) -> Command {
        let mut command = context.agentctl_in_temp_home(self.runtime.path());
        self.add_env(&mut command);
        command
    }

    fn run(&self, context: &TestContext, command: Command, expectation: &str) -> CommandSnapshot {
        context.run_command(command, expectation, &[(self.runtime.path(), "$TMP/runtime")])
    }

    fn add_env(&self, command: &mut Command) {
        command
            .env("AGENTDP_SERVER_PATH", &self.wrapper)
            .env("AGENTDP_REAL_SERVER_PATH", &self.real_server)
            .env("AGENTDP_GUEST_TOOL_DIR", &self.guest_tool_dir)
            .env("AGENTDP_CODEX_AUTH_PATH", &self.codex_auth)
            .env("AGENTDP_TEST_SERVER_PID", &self.pid_file);
    }

    fn try_stop(&self) -> Result<(), String> {
        let Ok(pid) = fs::read_to_string(&self.pid_file) else {
            return Ok(());
        };
        let pid = pid
            .trim()
            .parse()
            .map_err(|error| format!("parse test agentdp-server pid: {error}"))?;
        stop_process(pid)?;
        let _result = fs::remove_file(&self.pid_file);
        Ok(())
    }
}

pub struct AgentFixture {
    server: ServerFixture,
    context: TestContext,
    manifest: PathBuf,
    qemu_img: PathBuf,
    qemu_system: Option<PathBuf>,
}

impl AgentFixture {
    pub fn new(name: &str, manifest: &str) -> Self {
        let context = TestContext::new(name);
        let manifest = context.write("agent.yaml", manifest);
        let server = ServerFixture::new(&context);
        let qemu_img = write_fake_qemu_img(&context);
        let qemu_system = write_fake_qemu_system(&context, QemuSystemMode::SpawnSleep);
        write_fake_custom_env(&context);
        write_cached_base_image(&context);
        Self {
            server,
            context,
            manifest,
            qemu_img,
            qemu_system: Some(qemu_system),
        }
    }

    #[must_use]
    pub fn with_qemu_system(mut self, mode: QemuSystemMode) -> Self {
        self.qemu_system = Some(write_fake_qemu_system(&self.context, mode));
        self
    }

    pub fn apply_agent(&self) -> CommandSnapshot {
        self.apply_agent_with_options(false)
    }

    pub fn apply_agent_wait(&self) -> CommandSnapshot {
        self.apply_agent_with_options(true)
    }

    fn apply_agent_with_options(&self, wait: bool) -> CommandSnapshot {
        let ssh_keygen = self.context.write_fake_ssh_keygen();
        let ssh = self.context.write_fake_ssh();
        let mut command = self.command();
        command
            .args(["apply", "-f"])
            .arg(&self.manifest)
            .env("AGENTDP_QEMU_IMG_PATH", &self.qemu_img)
            .env("AGENTDP_SSH_KEYGEN_PATH", ssh_keygen)
            .env("AGENTDP_SSH_PATH", ssh);
        if let Some(qemu_system) = &self.qemu_system {
            command.env("AGENTDP_QEMU_SYSTEM_PATH", qemu_system);
        }
        if wait {
            command.arg("--wait");
        }
        self.run(command, "run agentctl apply")
    }

    pub fn update_manifest(&self, manifest: &str) {
        fs::write(&self.manifest, manifest).expect("update test manifest");
    }

    pub fn wait_observed(&self) -> CommandSnapshot {
        self.wait_observed_with_timeout(10)
    }

    pub fn wait_observed_with_timeout(&self, timeout_seconds: u64) -> CommandSnapshot {
        let mut command = self.command();
        command
            .args(["wait", "-f"])
            .arg(&self.manifest)
            .args(["--for", "observed", "--timeout-seconds"])
            .arg(timeout_seconds.to_string());
        self.run(command, "run agentctl wait observed")
    }

    pub fn wait_ready(&self) -> CommandSnapshot {
        let mut command = self.command();
        command
            .args(["wait", "-f"])
            .arg(&self.manifest)
            .args(["--for", "ready", "--timeout-seconds", "10"])
            .env("AGENTDP_QEMU_SYSTEM_PATH", self.qemu_system());
        self.run(command, "run agentctl wait ready")
    }

    pub fn status(&self) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["status", "0", "-f"]).arg(&self.manifest);
        if let Some(qemu_system) = &self.qemu_system {
            command.env("AGENTDP_QEMU_SYSTEM_PATH", qemu_system);
        }
        self.run(command, "run agentctl status")
    }

    pub fn agent_status(&self) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["status", "-f"]).arg(&self.manifest);
        if let Some(qemu_system) = &self.qemu_system {
            command.env("AGENTDP_QEMU_SYSTEM_PATH", qemu_system);
        }
        self.run(command, "run agentctl agent status")
    }

    pub fn wait_agent_status_contains(&self, needle: &str, timeout: Duration) -> CommandSnapshot {
        let deadline = Instant::now() + timeout;
        let mut output = self.agent_status();
        while !output.stdout().contains(needle) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
            output = self.agent_status();
        }
        output
    }

    pub fn wait_status_contains(&self, needle: &str, timeout: Duration) -> CommandSnapshot {
        let deadline = Instant::now() + timeout;
        let mut output = self.status();
        while !output.stdout().contains(needle) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
            output = self.status();
        }
        output
    }

    pub fn logs(&self, qemu: bool, lines: Option<usize>) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["logs", "0", "-f"]).arg(&self.manifest);
        if qemu {
            command.arg("--qemu");
        }
        if let Some(lines) = lines {
            command.arg("--lines").arg(lines.to_string());
        }
        self.run(command, "run agentctl logs")
    }

    pub fn network_logs(&self, lines: Option<usize>) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["logs", "0", "-f"]).arg(&self.manifest).arg("--network");
        if let Some(lines) = lines {
            command.arg("--lines").arg(lines.to_string());
        }
        self.run(command, "run agentctl network logs")
    }

    pub fn wait_network_logs_contains(&self, needle: &str, timeout: Duration) -> CommandSnapshot {
        let deadline = Instant::now() + timeout;
        let mut output = self.network_logs(Some(20));
        while !output.stdout().contains(needle) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
            output = self.network_logs(Some(20));
        }
        output
    }

    pub fn exec(&self, args: &[&str]) -> CommandSnapshot {
        let ssh = self.context.write_fake_ssh();
        let mut command = self.command();
        command
            .args(["exec", "0", "-f"])
            .arg(&self.manifest)
            .arg("--")
            .args(args)
            .env("AGENTDP_SSH_PATH", ssh);
        self.run(command, "run agentctl exec")
    }

    pub fn ps(&self) -> CommandSnapshot {
        let mut command = self.command();
        command.arg("ps").arg("-f").arg(&self.manifest);
        self.run(command, "run agentctl ps")
    }

    pub fn scale_agent(&self, replicas: u16, wait: bool) -> CommandSnapshot {
        let mut command = self.command();
        command
            .arg("scale")
            .arg(replicas.to_string())
            .arg("-f")
            .arg(&self.manifest);
        if wait {
            command.arg("--wait");
        }
        self.run(command, "run agentctl scale")
    }

    pub fn delete_agent(&self) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["delete", "-f"]).arg(&self.manifest);
        self.run(command, "run agentctl delete")
    }

    pub fn wait_deleted(&self) -> CommandSnapshot {
        let mut command = self.command();
        command
            .args(["wait", "-f"])
            .arg(&self.manifest)
            .args(["--for", "deleted", "--timeout-seconds", "10"]);
        self.run(command, "run agentctl wait deleted")
    }

    pub fn watch_agent_json_for(&self, duration: Duration) -> CommandSnapshot {
        let mut command = self.command();
        command
            .args(["watch", "-f"])
            .arg(&self.manifest)
            .arg("--json")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("start agentctl watch");
        std::thread::sleep(duration);
        let _result = child.kill();
        let output = child.wait_with_output().expect("wait for agentctl watch");
        self.context
            .snapshot_output(&output, &[(self.server.runtime.path(), "$TMP/runtime")])
    }

    pub fn instance_dir(&self) -> PathBuf {
        self.context
            .path()
            .join("home/.agentdp/agents/altinn-studio/instances/0")
    }

    pub fn target_instance_dir(&self, target: &str) -> PathBuf {
        self.context
            .path()
            .join("home/.agentdp/agents/altinn-studio/instances")
            .join(target)
    }

    pub fn target_instance_file(&self, target: &str, suffix: &str) -> PathBuf {
        self.target_instance_dir(target).join(suffix)
    }

    pub fn instance_file(&self, suffix: &str) -> PathBuf {
        self.instance_dir().join(suffix)
    }

    pub fn write_serial_log(&self, contents: &str) {
        fs::write(self.instance_file("logs/serial.log"), contents).expect("write serial log");
    }

    fn command(&self) -> Command {
        self.server.command(&self.context)
    }

    fn run(&self, command: Command, expectation: &str) -> CommandSnapshot {
        self.server.run(&self.context, command, expectation)
    }

    fn qemu_system(&self) -> &Path {
        self.qemu_system
            .as_deref()
            .expect("fixture must be created with a fake qemu-system")
    }
}

impl Drop for AgentFixture {
    fn drop(&mut self) {
        let _result = self.server.try_stop();
        stop_qemu_processes(self.context.path());
    }
}

fn stop_qemu_processes(root: &Path) {
    for pid_file in qemu_pid_files(root) {
        let Ok(pid) = fs::read_to_string(&pid_file) else {
            continue;
        };
        let Ok(pid) = pid.trim().parse() else {
            continue;
        };
        let _result = stop_process(pid);
    }
}

fn qemu_pid_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_qemu_pid_files(root, &mut files);
    files
}

fn collect_qemu_pid_files(path: &Path, files: &mut Vec<PathBuf>) {
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if metadata.is_file() {
        if path.file_name() == Some(OsStr::new("qemu.pid")) {
            files.push(path.to_path_buf());
        }
        return;
    }
    if !metadata.is_dir() {
        return;
    }
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        collect_qemu_pid_files(&entry.path(), files);
    }
}

fn agentdp_server_path() -> PathBuf {
    let agentctl = agentctl_path_for_support();
    let server = agentctl.with_file_name(format!("agentdp-server{}", std::env::consts::EXE_SUFFIX));
    assert!(
        server.is_file(),
        "agentdp-server binary was not built at {}; run workspace tests through `make test`",
        server.display()
    );
    server
}

fn write_server_wrapper(context: &TestContext) -> PathBuf {
    let wrapper = context.write(
        "agentdp-server-wrapper",
        r#"#!/bin/sh
printf '%s\n' "$$" > "$AGENTDP_TEST_SERVER_PID"
exec "$AGENTDP_REAL_SERVER_PATH" "$@"
"#,
    );
    set_executable(&wrapper);
    wrapper
}

fn write_cached_base_image(context: &TestContext) {
    let image = context
        .path()
        .join("home/.agentdp/cache/images/archlinux-x86_64-cloudimg.qcow2");
    fs::create_dir_all(image.parent().expect("cached image parent")).expect("create image cache");
    fs::write(image, b"cached image").expect("write cached image");
}

fn write_fake_qemu_img(context: &TestContext) -> PathBuf {
    let qemu_img = context.write(
        "qemu-img",
        r#"#!/bin/sh
printf 'fake qcow2\n' > "$8"
"#,
    );
    set_executable(&qemu_img);
    qemu_img
}

fn write_fake_guest_tool_binaries(context: &TestContext) -> PathBuf {
    let dir = context.path().join("guest-tools");
    fs::create_dir_all(&dir).expect("create fake guest tool directory");
    for name in ["guestd", "guestctl"] {
        let path = dir.join(name);
        fs::write(&path, "#!/bin/sh\nexit 0\n").expect("write fake guest tool");
        set_executable(&path);
    }
    dir
}

fn write_fake_codex_auth(context: &TestContext) -> PathBuf {
    context.write(
        "codex-auth.json",
        r#"{"tokens":{"access_token":"test-access","refresh_token":"test-refresh"}}
"#,
    )
}

fn write_fake_custom_env(context: &TestContext) {
    context.write(
        ".env",
        r"GITHUB_PAT=test-github
OPENAI_API_KEY=test-openai
",
    );
}

fn write_fake_qemu_system(context: &TestContext, mode: QemuSystemMode) -> PathBuf {
    let script = fake_qemu_system_script(mode);
    let qemu_system = context.write("qemu-system-x86_64", &script);
    set_executable(&qemu_system);
    qemu_system
}

fn fake_qemu_system_script(mode: QemuSystemMode) -> String {
    format!("{}{}", FAKE_QEMU_STREAM_HELPER, fake_qemu_system_mode_script(mode))
}

const FAKE_QEMU_STREAM_HELPER: &str = r#"#!/bin/sh
start_fake_qemu_stream() {
  socket=$1
  if [ -z "$socket" ]; then
    return 0
  fi
  python3 - "$socket" >/dev/null 2>&1 <<'PY' &
import os
import socket
import struct
import sys

path = sys.argv[1]
os.makedirs(os.path.dirname(path), exist_ok=True)
try:
    os.unlink(path)
except FileNotFoundError:
    pass

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(path)
server.listen(1)
server.settimeout(120)
try:
    conn, _ = server.accept()
except TimeoutError:
    server.close()
    raise SystemExit(0)

with conn, server:
    conn.sendall(struct.pack(">I", 1) + b"\0")
    conn.settimeout(120)
    while True:
        try:
            if not conn.recv(4096):
                break
        except TimeoutError:
            break
PY
}

start_fake_guest_control() {
  socket=$1
  finish_delay_seconds=${2:-0}
  manifest=${3:-altinn-studio}
  instance=${4:-replica-0}
  if [ -z "$socket" ]; then
    return 0
  fi
  python3 - "$socket" "$finish_delay_seconds" "$manifest" "$instance" >/dev/null 2>&1 <<'PY' &
import json
import os
import socket
import sys
import time

path = sys.argv[1]
finish_delay_seconds = float(sys.argv[2])
manifest = sys.argv[3]
instance = sys.argv[4]
os.makedirs(os.path.dirname(path), exist_ok=True)
try:
    os.unlink(path)
except FileNotFoundError:
    pass

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(path)
server.listen(1)
server.settimeout(120)
try:
    conn, _ = server.accept()
except TimeoutError:
    server.close()
    raise SystemExit(0)

messages = [
    {
        "id": "msg_0",
        "type": "guest.hello",
        "payload": {
            "protocol_version": 1,
            "guestd_role": "system",
            "guestd_version": "0.1.0",
            "manifest": manifest,
            "instance": instance,
            "os": "linux",
            "hostname": instance,
            "user": "agent",
        },
    },
    {
        "id": "bootstrap_0",
        "type": "bootstrap.status",
        "payload": {
            "plan_id": f"{manifest}/{instance}",
            "plan_hash": "sha256:test",
            "phase": "system",
            "status": "passed",
            "completed_steps": [],
            "pending_steps": [],
        },
    },
    {
        "id": "bootstrap_1",
        "type": "bootstrap.finished",
        "payload": {
            "plan_hash": "sha256:test",
            "status": "passed",
        },
    },
]

with conn, server:
    for index, message in enumerate(messages):
        if index == len(messages) - 1 and finish_delay_seconds > 0:
            time.sleep(finish_delay_seconds)
        conn.sendall(json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n")
PY
}

"#;

fn fake_qemu_system_mode_script(mode: QemuSystemMode) -> String {
    let bootstrap_delay_seconds = match mode {
        QemuSystemMode::StaticPid | QemuSystemMode::SpawnSleep | QemuSystemMode::DelayStart => "0",
        QemuSystemMode::DelayBootstrapFinished => "30",
    };
    let start_delay_seconds = match mode {
        QemuSystemMode::DelayStart => "2",
        QemuSystemMode::StaticPid | QemuSystemMode::SpawnSleep | QemuSystemMode::DelayBootstrapFinished => "0",
    };
    let process = if matches!(mode, QemuSystemMode::StaticPid) {
        "printf '4242\\n' > \"$pidfile\"\n".to_owned()
    } else {
        String::from(
            r#"(
  cleanup() {
    for child in $(jobs -p); do
      kill "$child" >/dev/null 2>&1 || true
    done
  }
  trap 'cleanup; exit 0' INT TERM
  trap 'cleanup' EXIT
  start_fake_qemu_stream "$socket"
  start_fake_guest_control "$control_socket" "$bootstrap_delay_seconds" "$manifest" "$instance"
  sleep 60 >/dev/null 2>&1 &
  wait "$!"
) >/dev/null 2>&1 &
qemu_pid=$!
printf '%s\n' "$qemu_pid" > "$pidfile"
if [ "$start_delay_seconds" != "0" ]; then
  sleep "$start_delay_seconds"
fi
"#,
        )
    };
    let mut script = String::from(
        r"pidfile=
socket=
control_socket=
manifest=altinn-studio
instance=replica-0
",
    );
    script.push_str("bootstrap_delay_seconds=");
    script.push_str(bootstrap_delay_seconds);
    script.push('\n');
    script.push_str("start_delay_seconds=");
    script.push_str(start_delay_seconds);
    script.push_str(
        r#"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-pidfile" ]; then
    shift
    pidfile=$1
  elif [ "$1" = "-netdev" ]; then
    shift
    socket=${1##*addr.path=}
  elif [ "$1" = "-chardev" ]; then
    shift
    control_socket=${1#*path=}
    control_socket=${control_socket%%,*}
  fi
  shift
done
case "$pidfile" in
  */agents/*/instances/*/run/qemu.pid)
    rest=${pidfile#*/agents/}
    manifest=${rest%%/*}
    rest=${rest#*/instances/}
    instance=${rest%%/*}
    case "$instance" in
      ''|*[!0-9]*) ;;
      *) instance="replica-$instance" ;;
    esac
    ;;
  */agents/*/.run/b/*/qemu.pid)
    rest=${pidfile#*/agents/}
    manifest=${rest%%/*}
    instance=agent-base
    ;;
  */agents/*/bases/*/run/qemu.pid)
    rest=${pidfile#*/agents/}
    manifest=${rest%%/*}
    instance=agent-base
    ;;
esac
"#,
    );
    script.push_str(
        r#"if [ "$instance" = "agent-base" ]; then
  bootstrap_delay_seconds=0
  start_delay_seconds=0
fi
"#,
    );
    script.push_str(&process);
    script
}

#[cfg(unix)]
fn set_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = fs::metadata(path).expect("read test helper permissions").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make test helper executable");
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) {}

struct ServerProcessGuard {
    pid_file: PathBuf,
}

impl ServerProcessGuard {
    const fn new(pid_file: PathBuf) -> Self {
        Self { pid_file }
    }
}

impl Drop for ServerProcessGuard {
    fn drop(&mut self) {
        if let Some(root) = self.pid_file.parent() {
            stop_qemu_processes(root);
        }
        let Ok(pid) = fs::read_to_string(&self.pid_file) else {
            return;
        };
        let result = pid
            .trim()
            .parse()
            .map_err(|error| format!("parse test agentdp-server pid: {error}"))
            .and_then(stop_process);
        if !std::thread::panicking() {
            result.expect("stop test agentdp-server");
        }
    }
}

fn stop_process(pid: u32) -> Result<(), String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(async move {
            match agentdp_platform::process::process_status(pid)
                .await
                .map_err(|error| error.to_string())?
            {
                agentdp_platform::process::ProcessStatus::NotFound => return Ok(()),
                agentdp_platform::process::ProcessStatus::Running => {}
            }

            match agentdp_platform::process::terminate_process(pid).await {
                Ok(()) => {}
                Err(error) => match agentdp_platform::process::process_status(pid)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    agentdp_platform::process::ProcessStatus::NotFound => return Ok(()),
                    agentdp_platform::process::ProcessStatus::Running => return Err(error.to_string()),
                },
            }

            if agentdp_platform::process::wait_for_process_exit(pid, Duration::from_secs(5))
                .await
                .map_err(|error| error.to_string())?
            {
                Ok(())
            } else {
                Err(format!("test agentdp-server pid {pid} did not exit"))
            }
        })
}

struct ShortRuntimeDir {
    path: PathBuf,
}

impl ShortRuntimeDir {
    fn create() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("adp-{}-{timestamp}-{id}", std::process::id()));
        fs::create_dir(&path).expect("create short runtime directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ShortRuntimeDir {
    fn drop(&mut self) {
        let _result = fs::remove_dir_all(&self.path);
    }
}
