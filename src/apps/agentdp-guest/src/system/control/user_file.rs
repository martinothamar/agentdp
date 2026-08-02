use agentdp_protocol::server_guest::WriteUserFileCommand;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::Command;

use crate::Error;
use crate::daemon::user_file as worker;

use super::commands::{HostCommandContext, HostCommandFailure};

pub(super) async fn write(
    payload: serde_json::Value,
    context: &HostCommandContext,
) -> Result<bool, HostCommandFailure> {
    let request = serde_json::from_value::<WriteUserFileCommand>(payload).map_err(Error::from)?;
    worker::validate_relative_path(&request.path)?;
    worker::parse_octal_mode(&request.permissions)?;
    execute(context, request.path, request.permissions, &request.contents).await
}

async fn execute(
    context: &HostCommandContext,
    path: String,
    permissions: String,
    contents: &[u8],
) -> Result<bool, HostCommandFailure> {
    match tokio::fs::metadata(&context.home).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(HostCommandFailure::not_ready("agent home is not a directory")),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(HostCommandFailure::not_ready("agent home does not exist"));
        }
        Err(source) => return Err(Error::from(source).into()),
    }

    let mut command = Command::new(&context.worker_executable);
    command
        .arg("write-user-file")
        .arg("--user")
        .arg(&context.user)
        .arg("--home")
        .arg(&context.home)
        .arg(format!("--path={path}"))
        .arg("--permissions")
        .arg(permissions);
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    agentdp_platform::user::run_as_user(&mut command, &context.user)
        .map_err(|source| HostCommandFailure::not_ready(source.to_string()))?;
    let mut child = command.spawn().map_err(Error::from)?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::Message("user file worker stdin was not piped".to_owned()))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Message("user file worker stdout was not piped".to_owned()))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::Message("user file worker stderr was not piped".to_owned()))?;
    let exchange = async {
        let write_stdin = async move {
            stdin.write_all(contents).await?;
            stdin.shutdown().await?;
            drop(stdin);
            Ok::<_, std::io::Error>(())
        };
        let read_stdout = async {
            let mut output = Vec::new();
            stdout.read_to_end(&mut output).await?;
            Ok::<_, std::io::Error>(output)
        };
        let read_stderr = async {
            let mut output = Vec::new();
            stderr.read_to_end(&mut output).await?;
            Ok::<_, std::io::Error>(output)
        };
        let ((), status, stdout, stderr) = tokio::try_join!(write_stdin, child.wait(), read_stdout, read_stderr)?;
        Ok::<_, std::io::Error>((status, stdout, stderr))
    };
    let (status, stdout, stderr) = match tokio::time::timeout(context.worker_timeout, exchange).await {
        Ok(Ok(output)) => output,
        Ok(Err(source)) => {
            terminate_and_reap(&mut child).await;
            return Err(Error::from(source).into());
        }
        Err(_elapsed) => {
            terminate_and_reap(&mut child).await;
            return Err(Error::Message(format!(
                "user file worker timed out after {}s",
                context.worker_timeout.as_secs_f64()
            ))
            .into());
        }
    };
    if !status.success() {
        return Err(Error::Message(format!(
            "user file worker failed: {}",
            String::from_utf8_lossy(&stderr).trim()
        ))
        .into());
    }
    match String::from_utf8_lossy(&stdout).trim() {
        "updated" => Ok(true),
        "unchanged" => Ok(false),
        output => Err(Error::Message(format!("user file worker returned invalid output {output:?}")).into()),
    }
}

async fn terminate_and_reap(child: &mut tokio::process::Child) {
    let _kill_result = child.start_kill();
    let _wait_result = child.wait().await;
}
