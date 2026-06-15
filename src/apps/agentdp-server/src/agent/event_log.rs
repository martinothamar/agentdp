use std::io::SeekFrom;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use agentdp_core::Context;
use agentdp_ds::local::spsc;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _, BufWriter};

const EVENT_LOG_WRITE_CAPACITY: usize = 4096;
const EVENT_LOG_SEQUENCE_TAIL_WINDOW: u64 = 64 * 1024;
const EVENT_LOG_SEQUENCE_MAX_TAIL_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("failed to read event log {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create event log directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize event log record: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("failed to parse final event sequence in {path}: {source}")]
    ParseSequence {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "failed to find final event sequence in {path}; final record is larger than {max_bytes} bytes or malformed"
    )]
    MissingSequence { path: PathBuf, max_bytes: u64 },
    #[error("failed to append event log {path}: {source}")]
    Append {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("event log writer queue is full: {path}")]
    QueueFull { path: PathBuf },
    #[error("event log writer has stopped: {path}")]
    Stopped { path: PathBuf },
}

#[derive(Debug)]
pub(crate) struct EventLogWriter<T> {
    path: PathBuf,
    writes: spsc::Sender<T>,
    _record: PhantomData<fn() -> T>,
}

impl<T: Serialize + 'static> EventLogWriter<T> {
    pub(crate) fn spawn(context: &Context, path: PathBuf) -> Self {
        let (writes, receiver) = spsc::bounded(EVENT_LOG_WRITE_CAPACITY);
        tokio::task::spawn_local(run_event_log_writer(context.clone(), path.clone(), receiver));
        Self {
            path,
            writes,
            _record: PhantomData,
        }
    }

    pub(crate) fn append(&mut self, record: T) -> Result<(), Error> {
        self.writes.try_send(record).map_err(|error| match error {
            spsc::TrySendError::Full(_) => Error::QueueFull {
                path: self.path.clone(),
            },
            spsc::TrySendError::Disconnected(_) => Error::Stopped {
                path: self.path.clone(),
            },
        })
    }
}

struct EventLogFile {
    path: PathBuf,
    writer: BufWriter<tokio::fs::File>,
    buffer: Vec<u8>,
}

impl EventLogFile {
    async fn open(path: PathBuf) -> Result<Self, Error> {
        let writer = open_writer(&path).await?;
        Ok(Self {
            path,
            writer,
            buffer: Vec::new(),
        })
    }

    async fn append<T: Serialize>(&mut self, records: &[T]) -> Result<(), Error> {
        if records.is_empty() {
            return Ok(());
        }
        self.buffer.clear();
        for record in records {
            serde_json::to_writer(&mut self.buffer, record).map_err(Error::Serialize)?;
            self.buffer.push(b'\n');
        }
        self.writer
            .write_all(&self.buffer)
            .await
            .map_err(|source| Error::Append {
                path: self.path.clone(),
                source,
            })?;
        self.writer.flush().await.map_err(|source| Error::Append {
            path: self.path.clone(),
            source,
        })
    }
}

async fn run_event_log_writer<T: Serialize>(context: Context, path: PathBuf, mut receiver: spsc::Receiver<T>) {
    let mut file = match EventLogFile::open(path).await {
        Ok(file) => file,
        Err(error) => {
            context.logger().warn(format!("{error}"));
            return;
        }
    };
    let mut batch = Vec::new();
    loop {
        match receiver.recv().await {
            Ok(record) => {
                batch.push(record);
                receiver.drain(|record| batch.push(record));
                if let Err(error) = file.append(&batch).await {
                    context.logger().warn(format!("{error}"));
                }
                batch.clear();
            }
            Err(spsc::TryRecvError::Empty) => {}
            Err(spsc::TryRecvError::Disconnected) => break,
        }
    }
}

async fn open_writer(path: &Path) -> Result<BufWriter<tokio::fs::File>, Error> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
    }
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|source| Error::Append {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(BufWriter::new(file))
}

pub(crate) async fn next_sequence(path: &Path) -> Result<u64, Error> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(1),
        Err(source) => {
            return Err(Error::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let file_len = file
        .metadata()
        .await
        .map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    if file_len == 0 {
        return Ok(1);
    }

    let max_window = file_len.min(EVENT_LOG_SEQUENCE_MAX_TAIL_BYTES);
    let mut window = file_len.min(EVENT_LOG_SEQUENCE_TAIL_WINDOW);
    loop {
        let start = file_len - window;
        file.seek(SeekFrom::Start(start)).await.map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let Ok(window_len) = usize::try_from(window) else {
            return Err(Error::MissingSequence {
                path: path.to_path_buf(),
                max_bytes: EVENT_LOG_SEQUENCE_MAX_TAIL_BYTES,
            });
        };
        let mut buffer = vec![0; window_len];
        file.read_exact(&mut buffer).await.map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;

        if start == 0 && buffer.iter().all(u8::is_ascii_whitespace) {
            return Ok(1);
        }
        if let Some(line) = final_complete_line(&buffer, start == 0) {
            let record = serde_json::from_slice::<SequenceRecord>(line).map_err(|source| Error::ParseSequence {
                path: path.to_path_buf(),
                source,
            })?;
            return Ok(record.sequence.saturating_add(1));
        }

        if window == max_window {
            return Err(Error::MissingSequence {
                path: path.to_path_buf(),
                max_bytes: EVENT_LOG_SEQUENCE_MAX_TAIL_BYTES,
            });
        }
        window = window.saturating_mul(2).min(max_window);
    }
}

#[derive(Debug, Deserialize)]
struct SequenceRecord {
    sequence: u64,
}

fn final_complete_line(buffer: &[u8], includes_file_start: bool) -> Option<&[u8]> {
    let mut end = buffer.len();
    while end > 0 && buffer[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end == 0 {
        return None;
    }
    let line_start = buffer[..end]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |position| position + 1);
    if line_start == 0 && !includes_file_start {
        return None;
    }
    let line = &buffer[line_start..end];
    (!line.iter().all(u8::is_ascii_whitespace)).then_some(line)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::next_sequence;

    static NEXT_TEST_LOG: AtomicU64 = AtomicU64::new(1);

    #[tokio::test(flavor = "local")]
    async fn next_sequence_returns_one_for_missing_log() {
        let path = test_path("missing");

        assert_eq!(next_sequence(&path).await.expect("next sequence"), 1);
    }

    #[tokio::test(flavor = "local")]
    async fn next_sequence_uses_last_non_empty_jsonl_record() {
        let path = test_path("small");
        write_log(
            &path,
            b"{\"sequence\":1,\"kind\":\"first\"}\n\n{\"sequence\":41,\"kind\":\"last\"}\n\n",
        )
        .await
        .expect("write test log");

        assert_eq!(next_sequence(&path).await.expect("next sequence"), 42);
    }

    #[tokio::test(flavor = "local")]
    async fn next_sequence_expands_tail_window_for_large_final_record() {
        let path = test_path("large-final-record");
        let padding = "x".repeat(80 * 1024);
        let contents = format!("{{\"sequence\":1}}\n{{\"sequence\":7,\"padding\":\"{padding}\"}}\n");
        write_log(&path, contents).await.expect("write test log");

        assert_eq!(next_sequence(&path).await.expect("next sequence"), 8);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join("agentdp-event-log-tests").join(format!(
            "{}-{}-{name}.jsonl",
            std::process::id(),
            NEXT_TEST_LOG.fetch_add(1, Ordering::Relaxed)
        ))
    }

    async fn write_log(path: &std::path::Path, contents: impl AsRef<[u8]>) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, contents).await
    }
}
