mod channel;
mod commands;
mod user_file;

pub(super) use channel::{ControlChannelSink, open_control_channel};
pub(super) use commands::{HostCommandContext, wait_for_host_messages};

#[cfg(test)]
mod tests {
    use agentdp_protocol::server_guest::{
        GuestMessage, GuestMessageKind, HostCommand, HostMessage, HostMessageKind, WRITE_USER_FILE_COMMAND,
        WriteUserFileCommand, decode_guest_message_line, encode_host_message_line,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::{HostCommandContext, wait_for_host_messages};

    #[tokio::test]
    async fn host_command_writes_user_file() {
        let temp = TestTempDir::create("guestd-write-user-file");
        let context = HostCommandContext {
            user: current_user(),
            home: temp.path.display().to_string(),
        };
        let message = write_command("cmd_1", ".codex/auth.json", b"{\"tokens\":\"placeholder\"}\n", "0600");

        let result = run_host_command(message, context).await;

        assert!(matches!(
            result.kind,
            GuestMessageKind::CommandResult(result)
                if result.command == WRITE_USER_FILE_COMMAND && result.updated
        ));
        assert_eq!(
            tokio::fs::read(temp.path.join(".codex/auth.json")).await.unwrap(),
            b"{\"tokens\":\"placeholder\"}\n"
        );
    }

    #[tokio::test]
    async fn host_command_rejects_parent_traversal() {
        let temp = TestTempDir::create("guestd-write-user-file-traversal");
        let context = HostCommandContext {
            user: current_user(),
            home: temp.path.display().to_string(),
        };
        let message = write_command("cmd_2", "../auth.json", b"nope\n", "0600");

        let result = run_host_command(message, context).await;

        assert!(matches!(
            result.kind,
            GuestMessageKind::Error(error)
                if error.code == "host_command_failed" && error.message.contains("must not contain")
        ));
        assert!(!temp.path.join("auth.json").exists());
    }

    async fn run_host_command(message: HostMessage, context: HostCommandContext) -> GuestMessage {
        let (mut host, guest) = tokio::io::duplex(8192);
        let guest_task = tokio::spawn(async move { wait_for_host_messages(guest, &context).await });
        host.write_all(&encode_host_message_line(&message).unwrap())
            .await
            .unwrap();
        host.shutdown().await.unwrap();

        let mut line = Vec::new();
        host.read_to_end(&mut line).await.unwrap();
        guest_task.await.unwrap().unwrap();
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
