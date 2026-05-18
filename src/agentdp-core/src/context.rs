use crate::logging::Logger;
use crate::platform::{self, HostTarget, PlatformPaths};

#[derive(Clone)]
pub struct Context {
    logger: Logger,
    host_target: HostTarget,
    paths: Result<PlatformPaths, platform::Error>,
}

impl Context {
    #[must_use]
    pub fn new(logger: Logger) -> Self {
        Self::from_parts(logger, platform::host_target(), PlatformPaths::resolve())
    }

    #[must_use]
    pub const fn from_parts(
        logger: Logger,
        host_target: HostTarget,
        paths: Result<PlatformPaths, platform::Error>,
    ) -> Self {
        Self {
            logger,
            host_target,
            paths,
        }
    }

    #[must_use]
    pub fn quiet() -> Self {
        Self::new(Logger::quiet())
    }

    #[must_use]
    pub const fn logger(&self) -> &Logger {
        &self.logger
    }

    #[must_use]
    pub const fn host_target(&self) -> HostTarget {
        self.host_target
    }

    /// Returns the process-local platform paths resolved when the context was
    /// created.
    ///
    /// # Errors
    ///
    /// Returns the stored platform path resolution error when required
    /// per-host environment variables were unavailable.
    pub const fn paths(&self) -> Result<&PlatformPaths, &platform::Error> {
        match &self.paths {
            Ok(paths) => Ok(paths),
            Err(error) => Err(error),
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::quiet()
    }
}
