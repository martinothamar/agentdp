#![cfg_attr(any(target_os = "linux", target_os = "macos", windows), allow(unsafe_code))]

use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("OS random request exceeds the platform API length limit")]
    BufferTooLarge,
    #[error("OS random source failed with code {0}")]
    Os(i32),
    #[error("OS random source is not supported on this host")]
    Unsupported,
}

/// Fills `buffer` with bytes from the operating system CSPRNG.
///
/// # Errors
///
/// Returns an error when the host OS random source is unavailable or unsupported.
pub fn fill(buffer: &mut [u8]) -> Result<(), Error> {
    platform::fill(buffer)
}

#[cfg(windows)]
mod platform {
    use windows_sys::Win32::Security::Cryptography::{BCRYPT_USE_SYSTEM_PREFERRED_RNG, BCryptGenRandom};

    use super::Error;

    pub(super) fn fill(buffer: &mut [u8]) -> Result<(), Error> {
        for chunk in buffer.chunks_mut(max_chunk_len()) {
            let len = u32::try_from(chunk.len()).map_err(|_| Error::BufferTooLarge)?;
            let status = unsafe {
                BCryptGenRandom(
                    std::ptr::null_mut(),
                    chunk.as_mut_ptr(),
                    len,
                    BCRYPT_USE_SYSTEM_PREFERRED_RNG,
                )
            };
            if status < 0 {
                return Err(Error::Os(status));
            }
        }
        Ok(())
    }

    fn max_chunk_len() -> usize {
        usize::try_from(u32::MAX).unwrap_or(usize::MAX)
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::io::Error as IoError;

    use super::Error;

    pub(super) fn fill(mut buffer: &mut [u8]) -> Result<(), Error> {
        while !buffer.is_empty() {
            let written = unsafe { libc::getrandom(buffer.as_mut_ptr().cast(), buffer.len(), 0) };
            if written > 0 {
                let written = usize::try_from(written).map_err(|_| Error::Os(libc::EOVERFLOW))?;
                buffer = &mut buffer[written..];
                continue;
            }
            let code = errno();
            if code != libc::EINTR {
                return Err(Error::Os(code));
            }
        }
        Ok(())
    }

    fn errno() -> i32 {
        IoError::last_os_error().raw_os_error().unwrap_or(0)
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::io::Error as IoError;

    use super::Error;

    const GETENTROPY_MAX: usize = 256;

    pub(super) fn fill(buffer: &mut [u8]) -> Result<(), Error> {
        for chunk in buffer.chunks_mut(GETENTROPY_MAX) {
            loop {
                let result = unsafe { libc::getentropy(chunk.as_mut_ptr().cast(), chunk.len()) };
                if result == 0 {
                    break;
                }
                let code = errno();
                if code != libc::EINTR {
                    return Err(Error::Os(code));
                }
            }
        }
        Ok(())
    }

    fn errno() -> i32 {
        IoError::last_os_error().raw_os_error().unwrap_or(0)
    }
}

#[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
mod platform {
    use super::Error;

    pub(super) fn fill(_buffer: &mut [u8]) -> Result<(), Error> {
        Err(Error::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::fill;

    #[test]
    fn fill_accepts_empty_buffer() {
        assert!(fill(&mut []).is_ok());
    }

    #[test]
    fn fill_returns_bytes_for_non_empty_buffer() {
        let mut bytes = [0_u8; 32];
        assert!(fill(&mut bytes).is_ok());
    }
}
