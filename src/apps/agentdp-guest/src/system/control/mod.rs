mod channel;
mod commands;
mod user_file;

pub(super) use channel::{ControlChannelSink, open_control_channel};
pub(super) use commands::{HostCommandContext, HostControlAction, HostMessageWait, wait_for_host_messages};

#[cfg(test)]
mod tests {
    use agentdp_protocol::server_guest::{
        GuestMessage, GuestMessageKind, HostCommand, HostMessage, HostMessageKind, RETRY_BOOTSTRAP_COMMAND,
        RetryBootstrapCommand, WRITE_USER_FILE_COMMAND, WriteUserFileCommand, decode_guest_message_line,
        encode_host_message_line,
    };
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

    use super::{HostCommandContext, HostControlAction, wait_for_host_messages};

    #[cfg(unix)]
    #[tokio::test]
    async fn host_command_launches_user_file_worker_subprocess() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = TestTempDir::create("guestd-write-user-file-subprocess");
        let worker = temp.path.join("worker");
        std::fs::write(
            &worker,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$5/worker-arguments\"\ncat > \"$5/worker-stdin\"\nprintf 'updated\\n'\n",
        )
        .unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let user = current_unix_user();
        let context = HostCommandContext {
            user: user.clone(),
            home: temp.path.display().to_string(),
            bootstrap_plan_hash: "plan-hash".to_owned(),
            worker_executable: worker,
            worker_timeout: std::time::Duration::from_secs(1),
        };
        let contents = b"worker input\n";
        let message = write_command("cmd_subprocess", ".codex/auth.json", contents, "0600");

        let result = run_host_command(message, context).await;

        assert!(matches!(
            result.kind,
            GuestMessageKind::CommandResult(result)
                if result.command == WRITE_USER_FILE_COMMAND && result.updated
        ));
        assert_eq!(std::fs::read(temp.path.join("worker-stdin")).unwrap(), contents);
        assert_eq!(
            std::fs::read_to_string(temp.path.join("worker-arguments")).unwrap(),
            format!(
                "write-user-file\n--user\n{user}\n--home\n{}\n--path=.codex/auth.json\n--permissions\n0600\n",
                temp.path.display()
            )
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn hung_user_file_worker_is_reaped_and_next_command_is_served() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = TestTempDir::create("guestd-hung-user-file-worker");
        let worker = temp.path.join("worker");
        std::fs::write(
            &worker,
            "#!/bin/sh\npath=${6#--path=}\nif [ \"$path\" = .codex/hang ]; then\n  printf '%s\\n' \"$$\" > \"$5/worker-pid\"\n  while :; do :; done\nfi\ncat >/dev/null\nprintf 'updated\\n'\n",
        )
        .unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let context = HostCommandContext {
            user: current_unix_user(),
            home: temp.path.display().to_string(),
            bootstrap_plan_hash: "plan-hash".to_owned(),
            worker_executable: worker,
            worker_timeout: std::time::Duration::from_millis(500),
        };
        let (host, mut guest) = tokio::io::duplex(8192);
        let guest_task = tokio::spawn(async move { wait_for_host_messages(&mut guest, &context).await });
        let mut host = tokio::io::BufReader::new(host);

        host.get_mut()
            .write_all(
                &encode_host_message_line(&write_command("cmd_hung", ".codex/hang", b"blocked", "0600")).unwrap(),
            )
            .await
            .unwrap();
        let first = match tokio::time::timeout(std::time::Duration::from_secs(1), read_guest_message(&mut host)).await {
            Ok(message) => message,
            Err(elapsed) => {
                let pid = std::fs::read_to_string(temp.path.join("worker-pid")).unwrap();
                let _status = std::process::Command::new("kill")
                    .arg(pid.trim())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
                panic!("hung worker should be bounded: {elapsed}");
            }
        };
        assert!(matches!(
            first.kind,
            GuestMessageKind::Error(error)
                if error.code == "host_command_failed" && error.message.contains("timed out")
        ));

        let pid = std::fs::read_to_string(temp.path.join("worker-pid")).unwrap();
        let status = std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.trim())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "timed-out worker {pid:?} was not reaped");

        host.get_mut()
            .write_all(
                &encode_host_message_line(&write_command("cmd_next", ".codex/auth.json", b"fresh", "0600")).unwrap(),
            )
            .await
            .unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(1), read_guest_message(&mut host))
            .await
            .expect("next command should be served");
        assert!(
            matches!(
                &second.kind,
                GuestMessageKind::CommandResult(result)
                    if result.command == WRITE_USER_FILE_COMMAND && result.updated
            ),
            "unexpected response to command after timed-out worker: {second:?}"
        );

        host.get_mut().shutdown().await.unwrap();
        assert_eq!(guest_task.await.unwrap().unwrap().handled, 2);
    }

    #[tokio::test]
    async fn host_command_rejects_parent_traversal() {
        let temp = TestTempDir::create("guestd-write-user-file-traversal");
        let context = test_context(&temp);
        let message = write_command("cmd_2", "../auth.json", b"nope\n", "0600");

        let result = run_host_command(message, context).await;

        assert!(matches!(
            result.kind,
            GuestMessageKind::Error(error)
                if error.code == "host_command_failed" && error.message.contains("must not contain")
        ));
        assert!(!temp.path.join("auth.json").exists());
    }

    #[tokio::test]
    async fn bootstrap_retry_command_defers_response_to_durable_executor() {
        let temp = TestTempDir::create("guestd-bootstrap-retry-command");
        let context = test_context(&temp);
        let message = HostMessage::new(
            "retry_1",
            HostMessageKind::Command(HostCommand {
                command: RETRY_BOOTSTRAP_COMMAND.to_owned(),
                payload: serde_json::to_value(RetryBootstrapCommand {
                    plan_hash: "plan-hash".to_owned(),
                    attempt_epoch: 4,
                })
                .unwrap(),
            }),
        );
        let (mut host, mut guest) = tokio::io::duplex(8192);
        let guest_task = tokio::spawn(async move { wait_for_host_messages(&mut guest, &context).await });

        host.write_all(&encode_host_message_line(&message).unwrap())
            .await
            .unwrap();
        let wait = guest_task.await.unwrap().unwrap();

        assert!(matches!(
            wait.action,
            Some(HostControlAction::RetryBootstrap { id, request })
                if id == "retry_1" && request.attempt_epoch == 4
        ));
    }

    #[tokio::test]
    async fn unterminated_host_command_is_not_dispatched() {
        let temp = TestTempDir::create("guestd-unterminated-host-command");
        let context = test_context(&temp);
        let message = write_command("cmd_3", ".codex/auth.json", b"must not be written\n", "0600");
        let mut frame = encode_host_message_line(&message).unwrap();
        assert_eq!(frame.pop(), Some(b'\n'));
        let (mut host, guest) = tokio::io::duplex(8192);
        let guest_task = tokio::spawn(async move {
            let mut guest = guest;
            wait_for_host_messages(&mut guest, &context).await
        });

        host.write_all(&frame).await.unwrap();
        host.shutdown().await.unwrap();
        let mut response = Vec::new();
        host.read_to_end(&mut response).await.unwrap();

        assert_eq!(guest_task.await.unwrap().unwrap().handled, 0);
        assert!(response.is_empty());
        assert!(!temp.path.join(".codex/auth.json").exists());
    }

    async fn run_host_command(message: HostMessage, context: HostCommandContext) -> GuestMessage {
        let (mut host, guest) = tokio::io::duplex(8192);
        let guest_task = tokio::spawn(async move {
            let mut guest = guest;
            wait_for_host_messages(&mut guest, &context).await
        });
        host.write_all(&encode_host_message_line(&message).unwrap())
            .await
            .unwrap();
        host.shutdown().await.unwrap();

        let mut line = Vec::new();
        host.read_to_end(&mut line).await.unwrap();
        guest_task.await.unwrap().unwrap();
        decode_guest_message_line(&line).unwrap()
    }

    async fn read_guest_message<R>(reader: &mut R) -> GuestMessage
    where
        R: tokio::io::AsyncBufRead + Unpin,
    {
        let mut line = Vec::new();
        reader.read_until(b'\n', &mut line).await.unwrap();
        assert!(!line.is_empty(), "guest closed before responding");
        decode_guest_message_line(&line).unwrap()
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

    fn test_context(temp: &TestTempDir) -> HostCommandContext {
        HostCommandContext {
            user: current_user(),
            home: temp.path.display().to_string(),
            bootstrap_plan_hash: "plan-hash".to_owned(),
            worker_executable: std::path::PathBuf::from("unused-user-file-worker"),
            worker_timeout: std::time::Duration::from_secs(1),
        }
    }

    #[cfg(unix)]
    fn current_unix_user() -> String {
        let output = std::process::Command::new("id").arg("-un").output().unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
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
}
