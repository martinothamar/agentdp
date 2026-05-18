use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

pub(super) struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    pub(super) fn create(root: &Path, name: &str) -> Self {
        fs::create_dir_all(root).expect("create e2e temp root");

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!("{}-{}-{}-{}", name, std::process::id(), timestamp, id));
        fs::create_dir(&path).expect("create e2e temp dir");

        Self { path }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _result = fs::remove_dir_all(&self.path);
    }
}
