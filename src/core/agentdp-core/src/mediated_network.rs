use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediatedNetworkProfile {
    pub guest_mac: MacAddress,
    pub gateway_mac: MacAddress,
    pub guest_ipv4: Ipv4Addr,
    pub gateway_ipv4: Ipv4Addr,
    pub ipv4_cidr_prefix: u8,
    pub guest_ipv6: Ipv6Addr,
    pub gateway_ipv6: Ipv6Addr,
    pub ipv6_cidr_prefix: u8,
}

impl Default for MediatedNetworkProfile {
    fn default() -> Self {
        DEFAULT_PROFILE
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MacAddress([u8; 6]);

impl MacAddress {
    #[must_use]
    pub const fn new(octets: [u8; 6]) -> Self {
        Self(octets)
    }

    #[must_use]
    pub const fn octets(self) -> [u8; 6] {
        self.0
    }
}

impl fmt::Display for MacAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5]
        )
    }
}

pub const DEFAULT_PROFILE: MediatedNetworkProfile = MediatedNetworkProfile {
    guest_mac: MacAddress::new([0x52, 0x54, 0x00, 0x65, 0x43, 0x21]),
    gateway_mac: MacAddress::new([0x52, 0x54, 0x00, 0x73, 0x00, 0x01]),
    guest_ipv4: Ipv4Addr::new(10, 73, 0, 10),
    gateway_ipv4: Ipv4Addr::new(10, 73, 0, 1),
    ipv4_cidr_prefix: 24,
    guest_ipv6: Ipv6Addr::new(0xfd42, 0x6175, 0x6469, 0x006f, 0, 0, 0, 0x0010),
    gateway_ipv6: Ipv6Addr::new(0xfd42, 0x6175, 0x6469, 0x006f, 0, 0, 0, 0x0001),
    ipv6_cidr_prefix: 64,
};
