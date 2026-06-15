#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seed(u64);

impl Seed {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn derive(self, name: &str) -> Self {
        let mut state = mix(self.0 ^ 0x9e37_79b9_7f4a_7c15);
        for byte in name.bytes() {
            state = mix(state ^ u64::from(byte));
        }
        Self(state)
    }
}

impl Display for Seed {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "0x{:016x}", self.0)
    }
}

impl FromStr for Seed {
    type Err = ParseSeedError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(ParseSeedError);
        }
        let parsed = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
            .map_or_else(|| trimmed.parse::<u64>(), |hex| u64::from_str_radix(hex, 16));
        parsed.map(Self).map_err(|_error| ParseSeedError)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseSeedError;

impl Display for ParseSeedError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid deterministic seed")
    }
}

impl std::error::Error for ParseSeedError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    #[must_use]
    pub const fn from_seed(seed: Seed) -> Self {
        Self { state: mix(seed.get()) }
    }

    #[must_use]
    pub const fn state(self) -> u64 {
        self.state
    }

    #[must_use]
    pub fn fork(self, name: &str) -> Self {
        Self::from_seed(Seed(self.state).derive(name))
    }

    #[must_use]
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.state)
    }

    #[must_use]
    pub const fn below(&mut self, upper: u64) -> Option<u64> {
        if upper == 0 {
            return None;
        }
        let zone = u64::MAX - (u64::MAX % upper);
        loop {
            let value = self.next_u64();
            if value < zone {
                return Some(value % upper);
            }
        }
    }

    #[must_use]
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        if denominator == 0 {
            return false;
        }
        if numerator >= denominator {
            return true;
        }
        self.below(denominator).is_some_and(|value| value < numerator)
    }

    pub fn fill_bytes(&mut self, output: &mut [u8]) {
        for chunk in output.chunks_mut(std::mem::size_of::<u64>()) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    pub fn shuffle<T>(&mut self, values: &mut [T]) {
        for index in (1..values.len()).rev() {
            let Some(selected) = self.below(u64::try_from(index + 1).unwrap_or(u64::MAX)) else {
                continue;
            };
            let selected = usize::try_from(selected).unwrap_or(0);
            values.swap(index, selected);
        }
    }

    pub fn choice<'a, T>(&mut self, values: &'a [T]) -> Option<&'a T> {
        let index = self.below(u64::try_from(values.len()).ok()?)?;
        values.get(usize::try_from(index).ok()?)
    }
}

// SplitMix64 finalizer constants from Steele, Lea, and Flood's SplitMix
// generator. This crate uses the generator for deterministic simulation and
// replay only; it is not suitable for cryptographic randomness.
const fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{DeterministicRng, Seed};

    #[test]
    fn parses_and_formats_hex_seed() -> Result<(), Box<dyn std::error::Error>> {
        let seed: Seed = "0x000000000000002a".parse()?;

        assert_eq!(seed.get(), 42);
        assert_eq!(seed.to_string(), "0x000000000000002a");
        Ok(())
    }

    #[test]
    fn produces_replayable_sequence() {
        let mut first = DeterministicRng::from_seed(Seed::new(7));
        let mut second = DeterministicRng::from_seed(Seed::new(7));

        let first_values = [first.next_u64(), first.next_u64(), first.next_u64()];
        let second_values = [second.next_u64(), second.next_u64(), second.next_u64()];

        assert_eq!(first_values, second_values);
    }

    #[test]
    fn derives_named_streams() {
        let root = Seed::new(9);

        assert_eq!(root.derive("link").derive("rx"), root.derive("link").derive("rx"));
        assert_ne!(root.derive("link").derive("rx"), root.derive("link").derive("tx"));
    }

    #[test]
    fn shuffles_deterministically() {
        let mut first = [1, 2, 3, 4, 5];
        let mut second = [1, 2, 3, 4, 5];
        DeterministicRng::from_seed(Seed::new(11)).shuffle(&mut first);
        DeterministicRng::from_seed(Seed::new(11)).shuffle(&mut second);

        assert_eq!(first, second);
    }
}
