use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::command::{CommandSnapshot, TestContext};
use super::runtime;

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
pub enum QemuSystemMode {
    StaticPid,
    SpawnSleep,
}

pub struct ServerFixture {
    real_server: PathBuf,
    wrapper: PathBuf,
    pid_file: PathBuf,
    _guard: ServerProcessGuard,
    runtime: ShortRuntimeDir,
}

impl ServerFixture {
    pub fn new(context: &TestContext) -> Self {
        let real_server = agentdp_server_path();
        let wrapper = write_server_wrapper(context);
        let pid_file = context.path().join("server.pid");
        let runtime = ShortRuntimeDir::create();
        let guard = ServerProcessGuard::new(pid_file.clone());
        Self {
            real_server,
            wrapper,
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
            .env("AGENTDP_TEST_SERVER_PID", &self.pid_file);
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
        write_cached_base_image(&context);
        Self {
            server,
            context,
            manifest,
            qemu_img,
            qemu_system: None,
        }
    }

    #[must_use]
    pub fn with_qemu_system(mut self, mode: QemuSystemMode) -> Self {
        self.qemu_system = Some(write_fake_qemu_system(&self.context, mode));
        self
    }

    pub fn create_instance(&self, ports: &[&str]) -> CommandSnapshot {
        let ssh_keygen = self.context.write_fake_ssh_keygen();
        let ssh = self.context.write_fake_ssh();
        let mut command = self.command();
        command
            .args(["create", "pr-0", "-f"])
            .arg(&self.manifest)
            .env("AGENTDP_QEMU_IMG_PATH", &self.qemu_img)
            .env("AGENTDP_SSH_KEYGEN_PATH", ssh_keygen)
            .env("AGENTDP_SSH_PATH", ssh);
        if let Some(qemu_system) = &self.qemu_system {
            command.env("AGENTDP_QEMU_SYSTEM_PATH", qemu_system);
        }
        for port in ports {
            command.arg("--port").arg(port);
        }
        self.run(command, "run agentctl create")
    }

    pub fn clone_instance(&self, target: &str) -> CommandSnapshot {
        self.clone_instance_with_ports(target, &[])
    }

    pub fn clone_instance_with_ports(&self, target: &str, ports: &[&str]) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["clone", "pr-0", target, "-f"]).arg(&self.manifest);
        for port in ports {
            command.arg("--port").arg(port);
        }
        self.run(command, "run agentctl clone")
    }

    pub fn up(&self) -> CommandSnapshot {
        let mut command = self.command();
        command
            .args(["up", "pr-0", "-f"])
            .arg(&self.manifest)
            .env("AGENTDP_QEMU_SYSTEM_PATH", self.qemu_system());
        self.run(command, "run agentctl up")
    }

    pub fn down(&self) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["down", "pr-0", "-f"]).arg(&self.manifest);
        self.run(command, "run agentctl down")
    }

    pub fn status(&self) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["status", "pr-0", "-f"]).arg(&self.manifest);
        self.run(command, "run agentctl status")
    }

    pub fn logs(&self, qemu: bool, lines: Option<usize>) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["logs", "pr-0", "-f"]).arg(&self.manifest);
        if qemu {
            command.arg("--qemu");
        }
        if let Some(lines) = lines {
            command.arg("--lines").arg(lines.to_string());
        }
        self.run(command, "run agentctl logs")
    }

    pub fn exec(&self, args: &[&str]) -> CommandSnapshot {
        let ssh = self.context.write_fake_ssh();
        let mut command = self.command();
        command
            .args(["exec", "pr-0", "-f"])
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

    pub fn rm(&self) -> CommandSnapshot {
        let mut command = self.command();
        command.args(["rm", "pr-0", "-f"]).arg(&self.manifest);
        self.run(command, "run agentctl rm")
    }

    pub fn instance_dir(&self) -> PathBuf {
        self.context.path().join("data/agentdp/instances/altinn-studio/pr-0")
    }

    pub fn target_instance_dir(&self, target: &str) -> PathBuf {
        self.context
            .path()
            .join("data/agentdp/instances/altinn-studio")
            .join(target)
    }

    pub fn target_instance_file(&self, target: &str, suffix: &str) -> PathBuf {
        self.target_instance_dir(target).join(suffix)
    }

    pub fn instance_file(&self, suffix: &str) -> PathBuf {
        self.instance_dir().join(suffix)
    }

    pub fn runtime_state(&self) -> runtime::RuntimeState {
        runtime::read(&self.instance_file("runtime.json"))
    }

    pub fn write_runtime_state(&self, state: &runtime::RuntimeState) {
        runtime::write(&self.instance_file("runtime.json"), state);
    }

    pub fn mark_running(&self) {
        let mut state = self.runtime_state();
        "running".clone_into(&mut state.status);
        self.write_runtime_state(&state);
    }

    pub fn mark_running_with_missing_pid(&self) {
        let mut state = self.runtime_state();
        let pid_file = state.qemu().pid_file.clone();
        "running".clone_into(&mut state.status);
        state.qemu_mut().pid = Some(u32::MAX);
        fs::create_dir_all(Path::new(&pid_file).parent().expect("pid file parent")).expect("create pid file parent");
        fs::write(pid_file, format!("{}\n", u32::MAX)).expect("write stale pid file");
        self.write_runtime_state(&state);
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

fn agentdp_server_path() -> PathBuf {
    let agentctl = Path::new(env!("CARGO_BIN_EXE_agentctl"));
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
        .join("cache/agentdp/images/archlinux-x86_64-cloudimg.qcow2");
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

fn write_fake_qemu_system(context: &TestContext, mode: QemuSystemMode) -> PathBuf {
    let qemu_system = context.write("qemu-system-x86_64", fake_qemu_system_script(mode));
    set_executable(&qemu_system);
    qemu_system
}

const fn fake_qemu_system_script(mode: QemuSystemMode) -> &'static str {
    match mode {
        QemuSystemMode::StaticPid => {
            r#"#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-pidfile" ]; then
    shift
    printf '4242\n' > "$1"
  fi
  shift
done
"#
        }
        QemuSystemMode::SpawnSleep => {
            r#"#!/bin/sh
pidfile=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-pidfile" ]; then
    shift
    pidfile=$1
  fi
  shift
done
sleep 60 >/dev/null 2>&1 &
printf '%s\n' "$!" > "$pidfile"
"#
        }
    }
}

fn set_executable(path: &Path) {
    agentdp_core::platform::set_executable(path).expect("make test helper executable");
}

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
    match agentdp_core::platform::process_status(pid).map_err(|error| error.to_string())? {
        agentdp_core::platform::ProcessStatus::NotFound => return Ok(()),
        agentdp_core::platform::ProcessStatus::Running => {}
    }

    match agentdp_core::platform::terminate_process(pid) {
        Ok(()) => {}
        Err(error) => match agentdp_core::platform::process_status(pid).map_err(|error| error.to_string())? {
            agentdp_core::platform::ProcessStatus::NotFound => return Ok(()),
            agentdp_core::platform::ProcessStatus::Running => return Err(error.to_string()),
        },
    }

    if agentdp_core::platform::wait_for_process_exit(pid, Duration::from_secs(5)).map_err(|error| error.to_string())? {
        Ok(())
    } else {
        Err(format!("test agentdp-server pid {pid} did not exit"))
    }
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
