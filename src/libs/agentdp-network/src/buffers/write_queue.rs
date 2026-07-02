use std::collections::VecDeque;

use crate::buffers::{BufferPool, ByteBuf};

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

    pub(crate) fn pending_bytes(&self) -> usize {
        self.first
            .iter()
            .chain(self.rest.iter())
            .map(PendingWrite::remaining_len)
            .sum()
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
