use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Verbose,
}

impl LogLevel {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Verbose => "verbose",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub level: LogLevel,
    pub message: String,
}

pub trait LogSink: Send + Sync {
    fn write(&self, record: LogRecord);
}

#[derive(Debug, Default)]
pub struct NoopSink;

impl LogSink for NoopSink {
    fn write(&self, _record: LogRecord) {}
}

#[derive(Clone)]
pub struct Logger {
    sink: Arc<dyn LogSink>,
    verbose: bool,
}

impl Logger {
    #[must_use]
    pub fn new(sink: Arc<dyn LogSink>, verbose: bool) -> Self {
        Self { sink, verbose }
    }

    #[must_use]
    pub fn quiet() -> Self {
        Self::new(Arc::new(NoopSink), false)
    }

    pub fn info(&self, message: impl Into<String>) {
        self.emit(LogLevel::Info, message.into());
    }

    pub fn warn(&self, message: impl Into<String>) {
        self.emit(LogLevel::Warn, message.into());
    }

    pub fn error(&self, message: impl Into<String>) {
        self.emit(LogLevel::Error, message.into());
    }

    pub fn verbose(&self, message: impl Into<String>) {
        if self.verbose {
            self.emit(LogLevel::Verbose, message.into());
        }
    }

    pub fn verbose_with(&self, message: impl FnOnce() -> String) {
        if self.verbose {
            self.emit(LogLevel::Verbose, message());
        }
    }

    #[must_use]
    pub const fn verbose_enabled(&self) -> bool {
        self.verbose
    }

    fn emit(&self, level: LogLevel, message: String) {
        self.sink.write(LogRecord { level, message });
    }
}

impl Default for Logger {
    fn default() -> Self {
        Self::quiet()
    }
}
