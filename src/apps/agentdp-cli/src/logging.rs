use std::sync::Arc;

use agentdp_core::Context;
use agentdp_core::logging::{LogRecord, LogSink, Logger};

#[must_use]
pub(crate) fn context(verbose: bool) -> Context {
    Context::new(Logger::new(Arc::new(StderrSink), verbose))
}

#[derive(Debug)]
struct StderrSink;

impl LogSink for StderrSink {
    fn write(&self, record: LogRecord) {
        eprintln!("{}", format_record(&record));
    }
}

fn format_record(record: &LogRecord) -> String {
    let message = record.message.trim_end_matches(['\r', '\n']);
    format!("{}: {message}", record.level.label())
}

#[cfg(test)]
mod tests {
    use agentdp_core::logging::{LogLevel, LogRecord};

    use super::format_record;

    #[test]
    fn log_record_format_trims_trailing_newline_from_streamed_chunks() {
        let record = LogRecord {
            level: LogLevel::Info,
            message: "line from guest\n".to_owned(),
        };

        assert_eq!(format_record(&record), "info: line from guest");
    }

    #[test]
    fn log_record_format_preserves_internal_newlines() {
        let record = LogRecord {
            level: LogLevel::Info,
            message: "first\nsecond\n".to_owned(),
        };

        assert_eq!(format_record(&record), "info: first\nsecond");
    }
}
