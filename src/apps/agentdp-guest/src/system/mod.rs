mod bootstrap;
mod control;
mod os;
mod seed;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use crate::Result;

use self::bootstrap::BootstrapExecutor;
use self::control::{ControlChannelSink, HostCommandContext, open_control_channel, wait_for_host_messages};
use self::seed::SeedSpec;

const HOST_CONTROL_RECONNECT_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub(crate) struct Config {
    pub instance_spec: PathBuf,
}

pub(crate) async fn run(config: Config) -> Result<()> {
    eprintln!("guestd system: loading local instance spec");
    let initial_seed = SeedSpec::load_local(&config).await?;
    eprintln!("guestd system: opening control channel");
    let control = open_control_channel(&initial_seed.control_path()).await?;
    let mut sink = ControlChannelSink::new(control);
    eprintln!("guestd system: refreshing seeded instance spec");
    let seed = match SeedSpec::load(&config).await {
        Ok(seed) => seed,
        Err(error) => {
            sink.emit_error("seed_load_failed", &error.to_string()).await?;
            return Err(error);
        }
    };
    let hello = seed.hello_message();
    eprintln!("guestd system: sending hello");
    sink.emit_message(&hello).await?;
    let plan_id = seed.instance.plan_id();
    let bootstrap_state_path = seed.bootstrap_state_path();
    let bootstrap_root_path = seed.bootstrap_root_path();
    let control_path = seed.control_path();
    let host_command_context = HostCommandContext::from_seed(&seed);
    eprintln!("guestd system: running bootstrap");
    Box::pin(BootstrapExecutor::new(seed.plan, plan_id, bootstrap_state_path, bootstrap_root_path).run(&mut sink))
        .await?;
    eprintln!("guestd system: bootstrap finished");
    serve_host_control_sessions(
        sink.into_inner(),
        host_command_context,
        ControlPathOpener { path: control_path },
    )
    .await
}

async fn serve_host_control_sessions<W, O>(control: W, context: HostCommandContext, mut opener: O) -> Result<()>
where
    W: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    O: HostControlOpener<W>,
{
    let mut session = control;
    loop {
        if let Err(error) = wait_for_host_messages(&mut session, &context).await {
            eprintln!("guestd system: host control session failed: {error}");
        }
        drop(session);
        eprintln!("guestd system: host control session closed; reopening control channel");
        tokio::time::sleep(HOST_CONTROL_RECONNECT_DELAY).await;
        session = opener.open().await?;
    }
}

trait HostControlOpener<W> {
    fn open(&mut self) -> Pin<Box<dyn Future<Output = Result<W>> + Send + '_>>;
}

struct ControlPathOpener {
    path: PathBuf,
}

impl HostControlOpener<tokio::fs::File> for ControlPathOpener {
    fn open(&mut self) -> Pin<Box<dyn Future<Output = Result<tokio::fs::File>> + Send + '_>> {
        Box::pin(open_control_channel(&self.path))
    }
}

#[cfg(test)]
mod tests {
    use agentdp_protocol::server_guest::{
        BootstrapLifecycleStatus, BootstrapPlan, BootstrapStep, BootstrapStepPhase, GUEST_INSTANCE_SPEC_VERSION,
        GuestInstancePaths, GuestInstanceSpec, GuestInstanceUser, GuestMessageKind, GuestPlatform, HostCommand,
        HostMessage, HostMessageKind, WRITE_USER_FILE_COMMAND, WriteUserFileCommand, decode_guest_message_line,
        encode_host_message_line,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::{
        Config,
        control::HostCommandContext,
        seed::{SeedSpec, validate_bootstrap_plan},
    };

    #[test]
    fn bootstrap_plan_accepts_relative_seed_scripts() {
        validate_bootstrap_plan(&plan("phases/040-packages.sh"), "/run/agentdp/bootstrap")
            .expect("valid bootstrap plan");
    }

    #[test]
    fn bootstrap_plan_rejects_absolute_scripts() {
        let error = validate_bootstrap_plan(&plan("/tmp/bootstrap.sh"), "/run/agentdp/bootstrap")
            .expect_err("invalid bootstrap plan");
        assert!(error.to_string().contains("relative"));
    }

    #[test]
    fn bootstrap_plan_rejects_parent_traversal() {
        let error = validate_bootstrap_plan(&plan("../bootstrap.sh"), "/run/agentdp/bootstrap")
            .expect_err("invalid bootstrap plan");
        assert!(error.to_string().contains("path components"));
    }

    #[tokio::test]
    async fn seed_spec_loads_paths_from_instance_spec() {
        let paths = SeedFiles::write(plan("phases/040-packages.sh")).await;

        let seed = SeedSpec::load(&Config {
            instance_spec: paths.instance_spec.clone(),
        })
        .await
        .expect("load seed spec");

        assert_eq!(seed.instance.plan_id(), "basic/basic-0");
        assert_eq!(seed.control_path(), paths.control);
        assert_eq!(seed.bootstrap_state_path(), paths.bootstrap_state);
        assert_eq!(seed.bootstrap_root_path(), paths.bootstrap_root);
        assert_eq!(seed.hello_message().kind.user(), Some("agent"));
    }

    #[tokio::test]
    async fn system_run_keeps_serving_after_bootstrap_control_eof() {
        let paths = SeedFiles::write(BootstrapPlan {
            steps: Vec::new(),
            ..plan("phases/040-packages.sh")
        })
        .await;
        tokio::fs::File::create(&paths.control)
            .await
            .expect("create control file");

        let instance_spec = paths.instance_spec.clone();
        let task = tokio::spawn(async move { super::run(Config { instance_spec }).await });

        wait_for_control_lines(&paths.control, 4).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !task.is_finished(),
            "guestd system exited after bootstrap control EOF instead of staying alive for host commands"
        );
        task.abort();

        let messages = decode_lines(&tokio::fs::read(&paths.control).await.expect("read control lines"));
        assert_eq!(messages.len(), 4);
        assert!(matches!(&messages[0].kind, GuestMessageKind::Hello(_)));
        assert!(matches!(
            &messages[1].kind,
            GuestMessageKind::BootstrapStatus(status)
                if status.status == BootstrapLifecycleStatus::Pending
        ));
        assert!(matches!(
            &messages[2].kind,
            GuestMessageKind::BootstrapStatus(status)
                if status.status == BootstrapLifecycleStatus::Passed
        ));
        assert!(matches!(&messages[3].kind, GuestMessageKind::BootstrapFinished(_)));
    }

    #[tokio::test]
    async fn host_control_reopen_serves_command_without_bootstrap_replay() {
        let temp = TestTempDir::create("guestd-control-reopen");
        let (mut first_host, first_guest) = tokio::io::duplex(8192);
        let (mut second_host, second_guest) = tokio::io::duplex(8192);
        let context = HostCommandContext {
            user: current_user(),
            home: temp.path.display().to_string(),
        };
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let mut opener = TestControlOpener {
            next: Some(second_guest),
            opened: Some(signal_tx),
        };
        let task =
            tokio::spawn(async move { super::serve_host_control_sessions(first_guest, context, &mut opener).await });

        first_host.shutdown().await.expect("close first host session");
        signal_rx.await.expect("control session reopened");

        let response = write_user_file_over_host(&mut second_host).await;
        task.abort();

        assert_user_file_written_response(&response);
        assert_eq!(
            tokio::fs::read(temp.path.join(".codex/auth.json")).await.unwrap(),
            b"{\"tokens\":\"placeholder\"}\n"
        );
    }

    #[tokio::test]
    async fn host_control_session_error_reopens_without_bootstrap_replay() {
        let temp = TestTempDir::create("guestd-control-reopen-after-error");
        let (mut first_host, first_guest) = tokio::io::duplex(8192);
        let (mut second_host, second_guest) = tokio::io::duplex(8192);
        let context = HostCommandContext {
            user: current_user(),
            home: temp.path.display().to_string(),
        };
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let mut opener = TestControlOpener {
            next: Some(second_guest),
            opened: Some(signal_tx),
        };
        let task =
            tokio::spawn(async move { super::serve_host_control_sessions(first_guest, context, &mut opener).await });

        first_host
            .write_all(b"{not valid host json}\n")
            .await
            .expect("write invalid host frame");
        first_host.shutdown().await.expect("close first host session");
        signal_rx.await.expect("control session reopened after invalid frame");

        let response = write_user_file_over_host(&mut second_host).await;
        task.abort();

        assert_user_file_written_response(&response);
        assert_eq!(
            tokio::fs::read(temp.path.join(".codex/auth.json")).await.unwrap(),
            b"{\"tokens\":\"placeholder\"}\n"
        );
    }

    async fn write_user_file_over_host(host: &mut tokio::io::DuplexStream) -> Vec<u8> {
        host.write_all(
            &encode_host_message_line(&write_command(
                "cmd_1",
                ".codex/auth.json",
                b"{\"tokens\":\"placeholder\"}\n",
                "0600",
            ))
            .expect("encode command"),
        )
        .await
        .expect("write command");
        host.shutdown().await.expect("close host write side");

        let mut response = Vec::new();
        host.read_to_end(&mut response).await.expect("read command response");
        response
    }

    fn assert_user_file_written_response(response: &[u8]) {
        let messages = decode_lines(response);
        assert_eq!(messages.len(), 1);
        assert!(matches!(
            &messages[0].kind,
            GuestMessageKind::CommandResult(result)
                if result.command == WRITE_USER_FILE_COMMAND && result.updated
        ));
    }

    async fn wait_for_control_lines(path: &std::path::Path, count: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            let contents = tokio::fs::read(path).await.expect("read control file");
            if decode_lines(&contents).len() >= count {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {count} control messages");
    }

    fn plan(script: &str) -> BootstrapPlan {
        BootstrapPlan {
            plan_version: 1,
            user: "agent".to_owned(),
            home: "/data/home".to_owned(),
            code_dir: "/data/home/code".to_owned(),
            steps: vec![BootstrapStep {
                id: "system.packages".to_owned(),
                label: "Install manifest packages".to_owned(),
                phase: BootstrapStepPhase::System,
                depends_on: vec!["system.prep".to_owned()],
                resources: Vec::new(),
                script: script.to_owned(),
                working_directory: "/".to_owned(),
                timeout_seconds: 900,
            }],
        }
    }

    fn write_command(id: &str, path: &str, contents: &[u8], permissions: &str) -> HostMessage {
        HostMessage::new(
            id,
            HostMessageKind::Command(HostCommand {
                command: WRITE_USER_FILE_COMMAND.to_owned(),
                payload: serde_json::to_value(WriteUserFileCommand {
                    path: path.to_owned(),
                    contents: contents.to_vec(),
                    permissions: permissions.to_owned(),
                })
                .unwrap(),
            }),
        )
    }

    fn current_user() -> String {
        std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "agent".to_owned())
    }

    struct TestControlOpener {
        next: Option<tokio::io::DuplexStream>,
        opened: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl super::HostControlOpener<tokio::io::DuplexStream> for &mut TestControlOpener {
        fn open(
            &mut self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<tokio::io::DuplexStream>> + Send + '_>>
        {
            Box::pin(async move {
                if let Some(opened) = self.opened.take() {
                    let _ = opened.send(());
                }
                self.next
                    .take()
                    .ok_or_else(|| crate::Error::Message("no test control session available".to_owned()))
            })
        }
    }

    struct TestTempDir {
        path: std::path::PathBuf,
    }

    impl TestTempDir {
        fn create(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "agentdp-{name}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct SeedFiles {
        root: std::path::PathBuf,
        instance_spec: std::path::PathBuf,
        control: std::path::PathBuf,
        bootstrap_root: std::path::PathBuf,
        bootstrap_state: std::path::PathBuf,
    }

    impl SeedFiles {
        async fn write(plan: BootstrapPlan) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "agentdp-guest-system-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system time")
                    .as_nanos()
            ));
            let spec_dir = dir.join("spec");
            let bootstrap_root = dir.join("bootstrap");
            let manifest = spec_dir.join("agent-manifest.yaml");
            let bootstrap_plan = spec_dir.join("bootstrap-plan.json");
            let instance_spec = spec_dir.join("instance.json");
            let bootstrap_state = dir.join("state/bootstrap-state.json");
            let control = dir.join("agentdp.control");
            tokio::fs::create_dir_all(&spec_dir).await.expect("create spec dir");
            tokio::fs::create_dir_all(&bootstrap_root)
                .await
                .expect("create bootstrap dir");
            tokio::fs::write(&manifest, "name: basic\n")
                .await
                .expect("write manifest");
            tokio::fs::write(&bootstrap_plan, serde_json::to_vec(&plan).expect("serialize plan"))
                .await
                .expect("write plan");
            tokio::fs::write(
                &instance_spec,
                serde_json::to_vec(&GuestInstanceSpec {
                    schema_version: GUEST_INSTANCE_SPEC_VERSION,
                    manifest: "basic".to_owned(),
                    instance: "basic-0".to_owned(),
                    hostname: "basic-0".to_owned(),
                    platform: GuestPlatform::Linux,
                    user: GuestInstanceUser {
                        name: "agent".to_owned(),
                        home: "/data/home".to_owned(),
                        code_dir: "/data/home/code".to_owned(),
                    },
                    paths: GuestInstancePaths {
                        spec_dir: spec_dir.display().to_string(),
                        instance_spec: instance_spec.display().to_string(),
                        manifest: manifest.display().to_string(),
                        bootstrap_plan: bootstrap_plan.display().to_string(),
                        bootstrap_root: bootstrap_root.display().to_string(),
                        bootstrap_state: bootstrap_state.display().to_string(),
                        control: control.display().to_string(),
                    },
                })
                .expect("serialize instance spec"),
            )
            .await
            .expect("write instance spec");
            Self {
                root: dir,
                instance_spec,
                control,
                bootstrap_root,
                bootstrap_state,
            }
        }
    }

    impl Drop for SeedFiles {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn decode_lines(bytes: &[u8]) -> Vec<agentdp_protocol::server_guest::GuestMessage> {
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| decode_guest_message_line(line).expect("decode line"))
            .collect()
    }

    trait GuestMessageKindExt {
        fn user(&self) -> Option<&str>;
    }

    impl GuestMessageKindExt for GuestMessageKind {
        fn user(&self) -> Option<&str> {
            match self {
                Self::Hello(hello) => Some(&hello.user),
                _ => None,
            }
        }
    }
}
