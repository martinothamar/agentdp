use std::collections::VecDeque;

use crate::buffers::{BufferPool, ByteBuf};
use smoltcp::socket::tcp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpStep {
    Progress,
    Blocked,
}

impl PumpStep {
    pub(crate) const fn made_progress(self) -> bool {
        matches!(self, Self::Progress)
    }
}

#[derive(Debug)]
pub(crate) struct WriteQueue {
    first: Option<PendingWrite>,
    rest: VecDeque<PendingWrite>,
}

impl WriteQueue {
    pub(crate) const fn new() -> Self {
        Self {
            first: None,
            rest: VecDeque::new(),
        }
    }

    pub(crate) fn push(&mut self, bytes: ByteBuf) {
        if bytes.is_empty() {
            return;
        }
        let write = PendingWrite { bytes, offset: 0 };
        if self.first.is_none() {
            self.first = Some(write);
        } else {
            self.rest.push_back(write);
        }
    }

    pub(crate) fn push_front(&mut self, write: PendingWrite) {
        if write.is_empty() {
            return;
        }
        if let Some(first) = self.first.replace(write) {
            self.rest.push_front(first);
        }
    }

    pub(crate) fn pop_front(&mut self) -> Option<PendingWrite> {
        let write = self.first.take()?;
        self.first = self.rest.pop_front();
        Some(write)
    }

    pub(crate) fn front_slice(&self) -> Option<&[u8]> {
        self.first.as_ref().and_then(PendingWrite::remaining)
    }

    pub(crate) fn advance_front(&mut self, len: usize) -> bool {
        let Some(write) = self.first.as_mut() else {
            return true;
        };
        write.offset += len;
        if write.offset >= write.bytes.len() {
            self.first = self.rest.pop_front();
            true
        } else {
            false
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    pub(crate) fn clear(&mut self) {
        self.first = None;
        self.rest.clear();
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn pending_bytes(&self) -> usize {
        self.first
            .iter()
            .chain(self.rest.iter())
            .map(PendingWrite::remaining_len)
            .sum()
    }

    pub(crate) fn flush_to_std<W: std::io::Write>(&mut self, writer: &mut W) -> std::io::Result<PumpStep> {
        let mut made_progress = false;
        loop {
            self.discard_exhausted_front();
            let Some(write) = self.first.as_mut() else {
                return Ok(if made_progress {
                    PumpStep::Progress
                } else {
                    PumpStep::Blocked
                });
            };
            let remaining = &write.bytes.as_slice()[write.offset..];
            match writer.write(remaining) {
                Ok(0) => {
                    return if remaining.is_empty() {
                        Ok(if made_progress {
                            PumpStep::Progress
                        } else {
                            PumpStep::Blocked
                        })
                    } else {
                        Err(std::io::ErrorKind::WriteZero.into())
                    };
                }
                Ok(written) => {
                    made_progress = true;
                    write.offset += written;
                    if write.offset >= write.bytes.len() {
                        self.first = self.rest.pop_front();
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(if made_progress {
                        PumpStep::Progress
                    } else {
                        PumpStep::Blocked
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn flush_to_guest_socket(&mut self, socket: &mut tcp::Socket<'_>) -> PumpStep {
        let mut made_progress = false;
        while socket.can_send() {
            self.discard_exhausted_front();
            let Some(write) = self.first.as_mut() else {
                return if made_progress {
                    PumpStep::Progress
                } else {
                    PumpStep::Blocked
                };
            };
            match socket.send_slice(&write.bytes.as_slice()[write.offset..]) {
                Ok(0) | Err(_) => {
                    return if made_progress {
                        PumpStep::Progress
                    } else {
                        PumpStep::Blocked
                    };
                }
                Ok(written) => {
                    made_progress = true;
                    write.offset += written;
                    if write.offset >= write.bytes.len() {
                        self.first = self.rest.pop_front();
                    }
                }
            }
        }
        if made_progress {
            PumpStep::Progress
        } else {
            PumpStep::Blocked
        }
    }

    fn discard_exhausted_front(&mut self) {
        while self.first.as_ref().is_some_and(PendingWrite::is_empty) {
            self.first = self.rest.pop_front();
        }
    }
}

impl Default for WriteQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct PendingWrite {
    pub(crate) bytes: ByteBuf,
    pub(crate) offset: usize,
}

impl PendingWrite {
    const fn is_empty(&self) -> bool {
        self.offset >= self.bytes.len()
    }

    fn remaining(&self) -> Option<&[u8]> {
        (!self.is_empty()).then(|| &self.bytes.as_slice()[self.offset..])
    }

    #[cfg(any(test, feature = "simulation"))]
    const fn remaining_len(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    pub(crate) fn into_remaining(self, buffers: &BufferPool) -> Result<ByteBuf, crate::buffers::PoolExhausted> {
        if self.offset == 0 {
            return Ok(self.bytes);
        }
        let remaining = &self.bytes.as_slice()[self.offset..];
        let mut bytes = buffers.try_byte_with_capacity(remaining.len())?;
        bytes.extend_from_slice(remaining);
        Ok(bytes)
    }
}
