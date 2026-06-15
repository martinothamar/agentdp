mod bootstrap;
mod control;
mod os;
mod seed;

use std::path::PathBuf;

use crate::Result;

use self::bootstrap::BootstrapExecutor;
use self::control::{ControlChannelSink, open_control_channel, wait_for_host_messages};
use self::seed::SeedSpec;

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
    eprintln!("guestd system: running bootstrap");
    Box::pin(BootstrapExecutor::new(seed.plan, plan_id, bootstrap_state_path, bootstrap_root_path).run(&mut sink))
        .await?;
    eprintln!("guestd system: bootstrap finished");
    wait_for_host_messages(sink.into_inner()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use agentdp_protocol::server_guest::{
        BootstrapLifecycleStatus, BootstrapPlan, BootstrapStep, BootstrapStepPhase, GUEST_INSTANCE_SPEC_VERSION,
        GuestInstancePaths, GuestInstanceSpec, GuestInstanceUser, GuestMessageKind, GuestPlatform,
        decode_guest_message_line,
    };

    use super::{
        Config,
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
    async fn system_run_writes_hello_and_bootstrap_lines_to_control_channel() {
        let paths = SeedFiles::write(BootstrapPlan {
            steps: Vec::new(),
            ..plan("phases/040-packages.sh")
        })
        .await;
        tokio::fs::File::create(&paths.control)
            .await
            .expect("create control file");

        super::run(Config {
            instance_spec: paths.instance_spec.clone(),
        })
        .await
        .expect("run system daemon");

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
