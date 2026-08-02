#![cfg(unix)]
#![allow(clippy::expect_used, clippy::panic)]

use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::AsyncWriteExt as _;

static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(1);

#[tokio::test]
async fn user_file_worker_writes_owned_file_and_reports_idempotence() {
    let user = current_user();
    let owner = agentdp_platform::user::UnixUser::resolve(&user).expect("resolve current user");
    let home = std::env::temp_dir().join(format!(
        "agentdp-user-file-worker-{}-{}",
        std::process::id(),
        NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed)
    ));
    let _result = tokio::fs::remove_dir_all(&home).await;
    tokio::fs::create_dir_all(&home).await.expect("create temporary home");
    let contents = br#"{"auth":"fresh"}"#;

    let first = run_worker(&user, &home, contents).await;
    assert!(
        first.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(first.stdout, b"updated\n");

    let target = home.join(".codex/auth.json");
    assert_eq!(tokio::fs::read(&target).await.expect("read written file"), contents);
    let metadata = tokio::fs::metadata(&target).await.expect("read file metadata");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!((metadata.uid(), metadata.gid()), (owner.uid(), owner.gid()));
    let parent_metadata = tokio::fs::metadata(target.parent().expect("target parent"))
        .await
        .expect("read parent metadata");
    assert_eq!(parent_metadata.permissions().mode() & 0o777, 0o700);
    assert_eq!(
        (parent_metadata.uid(), parent_metadata.gid()),
        (owner.uid(), owner.gid())
    );

    let second = run_worker(&user, &home, contents).await;
    assert!(
        second.status.success(),
        "idempotent worker failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(second.stdout, b"unchanged\n");

    tokio::fs::remove_dir_all(home).await.expect("remove temporary home");
}

async fn run_worker(user: &str, home: &Path, contents: &[u8]) -> std::process::Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_guestd"));
    command
        .arg("write-user-file")
        .arg("--user")
        .arg(user)
        .arg("--home")
        .arg(home)
        .arg("--path=.codex/auth.json")
        .arg("--permissions")
        .arg("0600")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn guestd user-file worker");
    child
        .stdin
        .take()
        .expect("worker stdin")
        .write_all(contents)
        .await
        .expect("write worker stdin");
    child.wait_with_output().await.expect("wait for worker")
}

fn current_user() -> String {
    let output = std::process::Command::new("id")
        .arg("-un")
        .output()
        .expect("resolve current user");
    assert!(output.status.success(), "id -un failed");
    String::from_utf8(output.stdout)
        .expect("current user is utf8")
        .trim()
        .to_owned()
}
