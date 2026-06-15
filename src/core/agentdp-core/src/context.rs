use crate::logging::Logger;

#[derive(Clone)]
pub struct Context {
    logger: Logger,
}

impl Context {
    #[must_use]
    pub const fn new(logger: Logger) -> Self {
        Self { logger }
    }

    #[must_use]
    pub const fn from_parts(logger: Logger) -> Self {
        Self { logger }
    }

    #[must_use]
    pub fn quiet() -> Self {
        Self::new(Logger::quiet())
    }

    #[must_use]
    pub const fn logger(&self) -> &Logger {
        &self.logger
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::quiet()
    }
}
