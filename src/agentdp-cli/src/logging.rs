use std::sync::Arc;

use agentdp_core::Context;
use agentdp_core::logging::{LogRecord, LogSink, Logger};

#[must_use]
pub fn context(verbose: bool) -> Context {
    Context::new(Logger::new(Arc::new(StderrSink), verbose))
}

#[derive(Debug)]
struct StderrSink;

impl LogSink for StderrSink {
    fn write(&self, record: LogRecord) {
        eprintln!("{}: {}", record.level.label(), record.message);
    }
}
