use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use agentdp_core::Context;
use agentdp_core::platform::ssh::SshKeygen;
use agentdp_core::platform::{PlatformPaths, ProcessStatus};
use agentdp_protocol::{
    InstanceCloneParams, InstanceCloneResult, InstanceCreateParams, InstanceCreateResult, InstanceRef,
    InstanceUpResult, ReadinessResult,
};

use super::state::{self, InstanceState, ReadinessState};
use crate::progress::{NoopProgress, Progress};
use crate::runtime;

const MANIFEST_NAME: &str = "altinn-studio";
const INSTANCE_NAME: &str = "pr-0";
const FULL_INSTANCE_NAME: &str = "altinn-studio/pr-0";
const TEST_PID: u32 = 4242;
const TEST_HOST_SSH_PORT: u16 = 2222;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn create_writes_seed_media_and_runtime_state() {
    let fixture = InstanceFixture::create("instance-create");

    let result = fixture.create_instance();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert_eq!(PathBuf::from(result.state), fixture.state_path());
    let state = fixture.read_state();
    assert_eq!(state.status, state::InstanceStatus::Created);
    let qemu = state.backend.qemu();
    assert_eq!(qemu.image.cache_key, "archlinux-x86_64-cloudimg.qcow2");
    assert_eq!(state.network.ports["ssh"].host, TEST_HOST_SSH_PORT);
    assert_eq!(state.network.ports["ssh"].guest, 22);
    assert_eq!(
        fs::read_to_string(&qemu.disk).unwrap().replace("\r\n", "\n"),
        "fake qcow2\n"
    );
    assert_eq!(fs::metadata(&qemu.seed_media).unwrap().len(), 4 * 1024 * 1024);
    assert!(PathBuf::from(&qemu.seed_media).ends_with("generated/qemu/seed.img"));
    assert!(PathBuf::from(&qemu.monitor_socket).ends_with("runtime/instances/altinn-studio/pr-0/qemu/monitor.sock"));
    assert!(PathBuf::from(&qemu.serial_log).ends_with("logs/serial.log"));
    assert!(PathBuf::from(state.manifest.copy).is_file());
}

#[test]
fn rm_removes_instance_directory() {
    let fixture = InstanceFixture::create("instance-rm");
    fixture.create_instance();

    let result = fixture.load().rm().unwrap();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert!(!fixture.instance_dir().exists());
}

#[test]
fn rm_refuses_running_instance() {
    let fixture = InstanceFixture::create("instance-rm-running");
    fixture.create_instance();
    fixture.mark_running();

    let error = fixture.load().rm().unwrap_err();

    assert!(
        error
            .to_string()
            .contains("run `agentctl down pr-0` before removing it")
    );
    assert!(fixture.state_path().exists());
}

#[test]
fn clone_copies_stopped_instance_with_target_runtime_state() {
    let fixture = InstanceFixture::create("instance-clone");
    fixture.create_instance();
    fixture.mark_stopped_ready();

    let result = fixture.clone_instance("pr-1");

    assert_eq!(result.source, FULL_INSTANCE_NAME);
    assert_eq!(result.name, "altinn-studio/pr-1");
    let target_state = fixture.read_target_state("pr-1");
    assert_eq!(target_state.instance, "pr-1");
    assert_eq!(target_state.status, state::InstanceStatus::Stopped);
    assert!(target_state.readiness.is_none());
    assert!(PathBuf::from(&target_state.manifest.copy).ends_with("altinn-studio/pr-1/manifest.yaml"));
    assert!(PathBuf::from(&target_state.backend.qemu().disk).ends_with("altinn-studio/pr-1/disk.qcow2"));
    assert!(
        PathBuf::from(&target_state.backend.qemu().monitor_socket)
            .ends_with("runtime/instances/altinn-studio/pr-1/qemu/monitor.sock")
    );
    assert!(
        PathBuf::from(&target_state.guest_access.as_ref().unwrap().private_key)
            .ends_with("altinn-studio/pr-1/generated/qemu/ssh/agentdp_ed25519")
    );
    assert_ne!(target_state.network.ports["ssh"].host, TEST_HOST_SSH_PORT);
    assert_eq!(
        fs::read_to_string(target_state.backend.qemu().disk.clone())
            .unwrap()
            .replace("\r\n", "\n"),
        "fake qcow2\n"
    );
}

#[test]
fn clone_accepts_target_port_overrides() {
    let fixture = InstanceFixture::create("instance-clone-port");
    fixture.create_instance();
    fixture.mark_stopped_ready();

    fixture.clone_instance_with_ports("pr-1", BTreeMap::from([("ssh".to_owned(), 4091)]));

    let target_state = fixture.read_target_state("pr-1");
    assert_eq!(target_state.network.ports["ssh"].host, 4091);
}

#[test]
fn clone_refuses_running_instance() {
    let fixture = InstanceFixture::create("instance-clone-running");
    fixture.create_instance();
    fixture.mark_running();

    let Err(error) = super::Instance::clone_existing(&Context::quiet(), &fixture.clone_params("pr-1"), &fixture.paths)
    else {
        panic!("running source should not clone");
    };

    assert!(error.to_string().contains("run `agentctl down pr-0` before cloning it"));
    assert!(!fixture.target_instance_dir("pr-1").exists());
}

#[test]
fn up_starts_qemu_and_updates_runtime_state() {
    let fixture = InstanceFixture::create("instance-up");
    fixture.create_instance();

    let result = fixture.up_with_fake_qemu().unwrap();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    let state = fixture.read_state();
    assert_eq!(state.status, state::InstanceStatus::Running);
    #[cfg(unix)]
    assert_eq!(result.process.pid, Some(TEST_PID));
    #[cfg(windows)]
    assert!(result.process.pid.is_some());
    assert_eq!(state.backend.qemu().pid, result.process.pid);
    assert!(state.backend.qemu().last_start_unix_seconds.is_some());
}

#[test]
fn up_waits_for_provisioning_before_marking_ready() {
    let fixture = InstanceFixture::create("instance-up-provisioning");
    fixture.create_instance();
    let waited = Cell::new(false);

    let result = fixture
        .up_with_fake_qemu_and_wait(|_context, state, _progress| {
            assert_eq!(state.status, state::InstanceStatus::Running);
            assert!(state.readiness.is_none());
            waited.set(true);
            Ok(())
        })
        .unwrap();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert!(waited.get());
    let state = fixture.read_state();
    assert!(state.readiness.as_ref().unwrap().ready);
}

#[test]
fn up_resumes_running_instance_wait() {
    let fixture = InstanceFixture::create("instance-up-running");
    fixture.create_instance();
    fixture.mark_running_ready();
    let waited = Cell::new(false);
    let mut instance = fixture.load();

    let result = instance
        .up_with_start(
            &Context::quiet(),
            |_context, _manifest, _state| unreachable!("running instance should not start again"),
            |_context, state, _progress| {
                assert_eq!(state.status, state::InstanceStatus::Running);
                assert!(state.readiness.is_none());
                waited.set(true);
                Ok(())
            },
            &mut NoopProgress,
        )
        .unwrap();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert_eq!(result.process.status, "running");
    assert!(waited.get());
    let state = fixture.read_state();
    assert!(state.readiness.as_ref().unwrap().ready);
}

#[test]
fn down_running_instance_terminates_and_updates_runtime_state() {
    let fixture = InstanceFixture::create("instance-down-running");
    fixture.create_instance();
    fixture.write_ready_running_qemu_runtime_files();

    let mut instance = fixture.load();
    let result = instance
        .down_with_process_control(
            &Context::quiet(),
            |pid| {
                assert_eq!(pid, TEST_PID);
                Ok(ProcessStatus::Running)
            },
            |pid| {
                assert_eq!(pid, TEST_PID);
                Ok(())
            },
            |pid| {
                assert_eq!(pid, TEST_PID);
                Ok(true)
            },
        )
        .unwrap();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert_eq!(result.status, "stopped");
    assert_eq!(result.previous_status, "running");
    assert_eq!(result.terminated_pid, Some(TEST_PID));
    assert_eq!(result.process.status, "terminated");
    let state = fixture.read_state();
    assert_eq!(state.status, state::InstanceStatus::Stopped);
    let qemu = state.backend.qemu();
    assert_eq!(qemu.pid, None);
    let readiness = state.readiness.as_ref().unwrap();
    assert!(!readiness.ready);
    assert_eq!(readiness.last_success_unix_seconds, 123);
    assert!(!PathBuf::from(&qemu.pid_file).exists());
    assert!(!PathBuf::from(&qemu.monitor_socket).exists());
    assert!(!PathBuf::from(&qemu.qmp_socket).exists());
}

#[test]
fn down_created_instance_is_idempotent() {
    let fixture = InstanceFixture::create("instance-down-created");
    fixture.create_instance();

    let mut instance = fixture.load();
    let result = instance
        .down_with_process_control(
            &Context::quiet(),
            |_pid| unreachable!("created instance should not probe process status"),
            |_pid| unreachable!("created instance should not terminate a process"),
            |_pid| unreachable!("created instance should not wait for process exit"),
        )
        .unwrap();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert_eq!(result.status, "created");
    assert_eq!(result.previous_status, "created");
}

#[test]
fn down_stale_running_instance_marks_stopped_without_terminating() {
    let fixture = InstanceFixture::create("instance-down-stale");
    fixture.create_instance();
    fixture.write_running_qemu_pid_file();

    let mut instance = fixture.load();
    let result = instance
        .down_with_process_control(
            &Context::quiet(),
            |pid| {
                assert_eq!(pid, TEST_PID);
                Ok(ProcessStatus::NotFound)
            },
            |_pid| unreachable!("stale instance should not terminate a missing process"),
            |_pid| unreachable!("stale instance should not wait for a missing process"),
        )
        .unwrap();

    assert_eq!(result.status, "stopped");
    assert_eq!(result.terminated_pid, None);
    assert_eq!(result.process.status, "missing");
    let state = fixture.read_state();
    assert_eq!(state.status, state::InstanceStatus::Stopped);
    assert_eq!(state.backend.qemu().pid, None);
}

#[test]
fn status_reports_stale_running_process() {
    let fixture = InstanceFixture::create("instance-status-stale");
    fixture.create_instance();
    fixture.mark_running_with_pid();

    let instance = fixture.load();
    let result = instance.status_with_process_status(|pid| {
        assert_eq!(pid, TEST_PID);
        Ok(ProcessStatus::NotFound)
    });

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert_eq!(result.status, "running");
    assert!(result.stale);
    assert_eq!(result.process.status, "missing");
}

#[test]
fn status_does_not_take_instance_lock() {
    let fixture = InstanceFixture::create("instance-status-lock-free");
    fixture.create_instance();
    fixture.write_lock_file_for_current_process();

    let instance = fixture.load();
    let result = instance.status();

    assert_eq!(result.name, FULL_INSTANCE_NAME);
    assert_eq!(result.status, "created");
}

struct InstanceFixture {
    temp: TestTempDir,
    paths: PlatformPaths,
    manifest: PathBuf,
    qemu_img: PathBuf,
}

impl InstanceFixture {
    fn create(name: &str) -> Self {
        let temp = TestTempDir::create(name);
        let manifest = temp.write("agent.yaml", valid_manifest());
        let paths = temp.platform_paths();
        TestTempDir::write_cached_base_image(&paths);
        let qemu_img = temp.write_fake_qemu_img();
        Self {
            temp,
            paths,
            manifest,
            qemu_img,
        }
    }

    fn create_instance(&self) -> InstanceCreateResult {
        let ssh_keygen = SshKeygen::new(self.temp.write_fake_ssh_keygen());
        let create_backend = crate::qemu::runtime::QemuCreateBackend::new(self.qemu_img.clone(), ssh_keygen);
        let instance = super::Instance::create_new_with_backend(
            &Context::quiet(),
            &self.create_params(),
            &self.paths,
            |context, input| create_backend.create(context, input).map_err(runtime::Error::Qemu),
        )
        .unwrap();
        instance.create_result()
    }

    fn clone_instance(&self, target: &str) -> InstanceCloneResult {
        self.clone_instance_with_ports(target, BTreeMap::default())
    }

    fn clone_instance_with_ports(&self, target: &str, ports: BTreeMap<String, u16>) -> InstanceCloneResult {
        let mut params = self.clone_params(target);
        params.ports = ports;
        let instance = super::Instance::clone_existing(&Context::quiet(), &params, &self.paths).unwrap();
        instance.clone_result(FULL_INSTANCE_NAME)
    }

    fn up_with_fake_qemu(&self) -> Result<InstanceUpResult, super::Error> {
        self.up_with_fake_qemu_and_wait(|_context, _state, _progress| Ok(()))
    }

    fn up_with_fake_qemu_and_wait(
        &self,
        wait_provisioned: impl FnOnce(&Context, &InstanceState, &mut dyn Progress) -> Result<(), runtime::Error>,
    ) -> Result<InstanceUpResult, super::Error> {
        let runtime_backend = crate::qemu::runtime::QemuRuntimeBackend::new(self.temp.write_fake_qemu_system());
        let mut instance = self.load();
        instance.up_with_start(
            &Context::quiet(),
            |context, manifest, state| {
                let manifest_name = state.manifest_name.clone();
                let instance = state.instance.clone();
                let network = state.network.clone();
                match &mut state.backend {
                    runtime::BackendState::Qemu(qemu_state) => runtime_backend
                        .start(context, manifest, &manifest_name, &instance, &network, qemu_state)
                        .map_err(runtime::Error::Qemu),
                }
            },
            wait_provisioned,
            &mut NoopProgress,
        )
    }

    fn load(&self) -> super::Instance {
        super::Instance::load_existing(&Context::quiet(), &self.instance_ref(), &self.paths).unwrap()
    }

    fn create_params(&self) -> InstanceCreateParams {
        InstanceCreateParams {
            manifest: self.manifest.clone(),
            instance: INSTANCE_NAME.to_owned(),
            ports: BTreeMap::from([("ssh".to_owned(), TEST_HOST_SSH_PORT)]),
        }
    }

    fn clone_params(&self, target: &str) -> InstanceCloneParams {
        InstanceCloneParams {
            manifest: self.manifest.clone(),
            source: INSTANCE_NAME.to_owned(),
            target: target.to_owned(),
            ports: BTreeMap::default(),
        }
    }

    fn instance_ref(&self) -> InstanceRef {
        InstanceRef {
            manifest: self.manifest.clone(),
            instance: INSTANCE_NAME.to_owned(),
        }
    }

    fn instance_dir(&self) -> PathBuf {
        self.paths
            .data
            .join(format!("instances/{MANIFEST_NAME}/{INSTANCE_NAME}"))
    }

    fn target_instance_dir(&self, instance: &str) -> PathBuf {
        self.paths.data.join("instances").join(MANIFEST_NAME).join(instance)
    }

    fn state_path(&self) -> PathBuf {
        self.instance_dir().join("runtime.json")
    }

    fn read_state(&self) -> InstanceState {
        serde_json::from_str(&fs::read_to_string(self.state_path()).unwrap()).unwrap()
    }

    fn read_target_state(&self, instance: &str) -> InstanceState {
        serde_json::from_str(&fs::read_to_string(self.target_instance_dir(instance).join("runtime.json")).unwrap())
            .unwrap()
    }

    fn write_state(&self, state: &InstanceState) {
        fs::write(self.state_path(), serde_json::to_string_pretty(state).unwrap()).unwrap();
    }

    fn update_state(&self, update: impl FnOnce(&mut InstanceState)) {
        let mut state = self.read_state();
        update(&mut state);
        self.write_state(&state);
    }

    fn mark_running(&self) {
        self.update_state(|state| {
            state.status = state::InstanceStatus::Running;
        });
    }

    fn mark_running_ready(&self) {
        self.update_state(|state| {
            state.status = state::InstanceStatus::Running;
            state.readiness = Some(ready_state());
        });
    }

    fn mark_stopped_ready(&self) {
        self.update_state(|state| {
            state.status = state::InstanceStatus::Stopped;
            state.readiness = Some(ready_state());
        });
    }

    fn mark_running_with_pid(&self) {
        self.update_state(|state| {
            state.status = state::InstanceStatus::Running;
            state.backend.qemu_mut().pid = Some(TEST_PID);
        });
    }

    fn write_ready_running_qemu_runtime_files(&self) {
        self.update_state(|state| {
            state.status = state::InstanceStatus::Running;
            state.backend.qemu_mut().pid = Some(TEST_PID);
            state.readiness = Some(ready_state());
            write_qemu_runtime_files(state);
        });
    }

    fn write_running_qemu_pid_file(&self) {
        self.update_state(|state| {
            state.status = state::InstanceStatus::Running;
            state.backend.qemu_mut().pid = Some(TEST_PID);
            write_qemu_pid_file(state);
        });
    }

    fn write_lock_file_for_current_process(&self) {
        fs::write(
            self.instance_dir()
                .parent()
                .unwrap()
                .join(format!("{INSTANCE_NAME}.lock")),
            format!("pid={}\n", std::process::id()),
        )
        .unwrap();
    }
}

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn create(name: &str) -> Self {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("agentdp-{name}-{}-{timestamp}-{id}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self { path }
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.path.join(name);
        fs::write(&path, contents).unwrap();
        path
    }

    fn write_fake_tool(&self, name: &str, contents: &str) -> PathBuf {
        let executable = executable_script_name(name);
        let tool = self.write(&executable, contents);
        agentdp_core::platform::set_executable(&tool).unwrap();
        tool
    }

    fn write_cached_base_image(paths: &PlatformPaths) {
        let image = paths.cache.join("images/archlinux-x86_64-cloudimg.qcow2");
        fs::create_dir_all(image.parent().unwrap()).unwrap();
        fs::write(image, b"cached image").unwrap();
    }

    fn write_fake_qemu_img(&self) -> PathBuf {
        self.write_fake_tool("qemu-img", fake_qemu_img_script())
    }

    fn write_fake_qemu_system(&self) -> PathBuf {
        self.write_fake_tool("qemu-system-x86_64", fake_qemu_system_script())
    }

    fn write_fake_ssh_keygen(&self) -> PathBuf {
        self.write_fake_tool("ssh-keygen", fake_ssh_keygen_script())
    }

    fn platform_paths(&self) -> PlatformPaths {
        PlatformPaths {
            data: self.path.join("data"),
            config: self.path.join("config"),
            cache: self.path.join("cache"),
            runtime: self.path.join("runtime"),
            logs: self.path.join("logs"),
        }
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _result = fs::remove_dir_all(&self.path);
    }
}

fn ready_state() -> ReadinessState {
    ReadinessState {
        ready: true,
        last_success_unix_seconds: 123,
        result: ReadinessResult {
            ready: true,
            services: BTreeMap::default(),
            healthchecks: Vec::new(),
        },
    }
}

fn write_qemu_runtime_files(state: &InstanceState) {
    let qemu = state.backend.qemu();
    write_qemu_pid_file(state);
    fs::write(&qemu.monitor_socket, "").unwrap();
    fs::write(&qemu.qmp_socket, "").unwrap();
}

fn write_qemu_pid_file(state: &InstanceState) {
    let qemu = state.backend.qemu();
    fs::create_dir_all(Path::new(&qemu.pid_file).parent().unwrap()).unwrap();
    fs::write(&qemu.pid_file, format!("{TEST_PID}\n")).unwrap();
}

const fn valid_manifest() -> &'static str {
    agentdp_test_support::manifest::minimal()
}

#[cfg(unix)]
const fn fake_qemu_img_script() -> &'static str {
    "#!/bin/sh\nprintf 'fake qcow2\\n' > \"$8\"\n"
}

#[cfg(windows)]
const fn fake_qemu_img_script() -> &'static str {
    "@echo off\r\necho fake qcow2> %8\r\n"
}

#[cfg(unix)]
const fn fake_qemu_system_script() -> &'static str {
    "#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"-pidfile\" ]; then\n    shift\n    printf '4242\\n' > \"$1\"\n  fi\n  shift\ndone\n"
}

#[cfg(windows)]
const fn fake_qemu_system_script() -> &'static str {
    "@echo off\r\n:loop\r\nif \"%~1\"==\"\" exit /b 0\r\nif \"%~1\"==\"-pidfile\" goto found\r\nshift\r\ngoto loop\r\n:found\r\nshift\r\necho 4242> \"%~1\"\r\nexit /b 0\r\n"
}

#[cfg(unix)]
const fn fake_ssh_keygen_script() -> &'static str {
    "#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"-f\" ]; then\n    shift\n    printf 'private key\\n' > \"$1\"\n    printf 'ssh-ed25519 AAAATEST agentdp\\n' > \"$1.pub\"\n    exit 0\n  fi\n  shift\ndone\nexit 1\n"
}

#[cfg(windows)]
const fn fake_ssh_keygen_script() -> &'static str {
    "@echo off\r\necho private key> \"%~8\"\r\necho ssh-ed25519 AAAATEST agentdp> \"%~8.pub\"\r\nexit /b 0\r\n"
}

#[cfg(windows)]
fn executable_script_name(name: &str) -> String {
    format!("{name}.cmd")
}

#[cfg(unix)]
fn executable_script_name(name: &str) -> String {
    name.to_owned()
}
