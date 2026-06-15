#![cfg_attr(windows, allow(unsafe_code))]

#[cfg(windows)]
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr};

#[cfg(windows)]
type AdapterAddresses = windows_sys::Win32::NetworkManagement::IpHelper::IP_ADAPTER_ADDRESSES_LH;
#[cfg(windows)]
type SockAddr = windows_sys::Win32::Networking::WinSock::SOCKADDR;

/// Returns host DNS servers in priority order.
///
/// # Errors
///
/// Returns an error when the host DNS configuration cannot be queried.
#[cfg(unix)]
pub async fn system_dns_servers() -> std::io::Result<Vec<IpAddr>> {
    let contents = tokio::fs::read_to_string("/etc/resolv.conf").await?;
    Ok(contents
        .lines()
        .filter_map(|line| line.trim().strip_prefix("nameserver"))
        .filter_map(|rest| rest.split_whitespace().next())
        .filter_map(|address| address.parse::<IpAddr>().ok())
        .collect())
}

/// Returns host DNS servers in priority order.
///
/// # Errors
///
/// Returns an error when the host DNS configuration cannot be queried.
#[cfg(windows)]
#[allow(clippy::unused_async)]
pub async fn system_dns_servers() -> std::io::Result<Vec<IpAddr>> {
    windows_dns_servers()
}

#[cfg(windows)]
fn windows_dns_servers() -> std::io::Result<Vec<IpAddr>> {
    use std::ffi::c_void;
    use std::ptr;

    use windows_sys::Win32::Foundation::{ERROR_BUFFER_OVERFLOW, NO_ERROR};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_FRIENDLY_NAME, GAA_FLAG_SKIP_MULTICAST, GAA_FLAG_SKIP_UNICAST,
        GetAdaptersAddresses,
    };
    use windows_sys::Win32::Networking::WinSock::AF_UNSPEC;

    const INITIAL_BUFFER_LEN: u32 = 15 * 1024;
    let flags = GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_FRIENDLY_NAME | GAA_FLAG_SKIP_UNICAST;
    let mut buffer_len = INITIAL_BUFFER_LEN;
    let mut buffer = adapter_buffer(buffer_len);

    for _attempt in 0..2 {
        // SAFETY: `buffer` is writable for `buffer_len` bytes and is only interpreted
        // by Windows as `AdapterAddresses` for the duration of the call.
        let result = unsafe {
            GetAdaptersAddresses(
                u32::from(AF_UNSPEC),
                flags,
                ptr::null::<c_void>(),
                buffer.as_mut_ptr().cast(),
                &raw mut buffer_len,
            )
        };
        if result == ERROR_BUFFER_OVERFLOW {
            buffer = adapter_buffer(buffer_len);
            continue;
        }
        if result != NO_ERROR {
            return Err(std::io::Error::from_raw_os_error(
                i32::try_from(result).unwrap_or(i32::MAX),
            ));
        }

        let mut servers = Vec::new();
        let mut adapter = buffer.as_ptr().cast::<AdapterAddresses>();
        while !adapter.is_null() {
            // SAFETY: `adapter` points into the `GetAdaptersAddresses` output buffer
            // and remains valid while `buffer` is alive.
            unsafe {
                if adapter_is_dns_candidate(&*adapter) {
                    collect_adapter_dns_servers(&*adapter, &mut servers);
                }
                adapter = (*adapter).Next;
            }
        }
        servers.sort_by_key(|server| (matches!(server.address, IpAddr::V6(_)), server.metric));
        servers.dedup_by_key(|server| server.address);
        return Ok(servers.into_iter().map(|server| server.address).collect());
    }

    Err(std::io::Error::other("GetAdaptersAddresses buffer changed repeatedly"))
}

#[cfg(windows)]
#[derive(Clone, Copy)]
struct DnsServer {
    metric: u32,
    address: IpAddr,
}

#[cfg(windows)]
fn adapter_buffer(byte_len: u32) -> Vec<MaybeUninit<AdapterAddresses>> {
    let item_len = std::mem::size_of::<AdapterAddresses>();
    let len = (byte_len as usize).div_ceil(item_len).max(1);
    vec![MaybeUninit::zeroed(); len]
}

#[cfg(windows)]
const fn adapter_is_dns_candidate(adapter: &AdapterAddresses) -> bool {
    use windows_sys::Win32::NetworkManagement::IpHelper::{IF_TYPE_SOFTWARE_LOOPBACK, IF_TYPE_TUNNEL};
    use windows_sys::Win32::NetworkManagement::Ndis::IfOperStatusUp;

    adapter.OperStatus == IfOperStatusUp
        && adapter.IfType != IF_TYPE_SOFTWARE_LOOPBACK
        && adapter.IfType != IF_TYPE_TUNNEL
        && !adapter.FirstDnsServerAddress.is_null()
}

#[cfg(windows)]
unsafe fn collect_adapter_dns_servers(adapter: &AdapterAddresses, servers: &mut Vec<DnsServer>) {
    let mut server = adapter.FirstDnsServerAddress;
    let metric = adapter_metric(adapter);
    while !server.is_null() {
        // SAFETY: `server` is part of the adapter-address linked list returned by
        // `GetAdaptersAddresses`; callers keep the backing buffer alive.
        unsafe {
            if let Some(address) = sockaddr_ip((*server).Address.lpSockaddr) {
                servers.push(DnsServer { metric, address });
            }
            server = (*server).Next;
        }
    }
}

#[cfg(windows)]
const fn adapter_metric(adapter: &AdapterAddresses) -> u32 {
    let metric = match (adapter.Ipv4Metric, adapter.Ipv6Metric) {
        (0, ipv6) => ipv6,
        (ipv4, 0) => ipv4,
        (ipv4, ipv6) if ipv4 < ipv6 => ipv4,
        (_, ipv6) => ipv6,
    };
    if metric == 0 { u32::MAX } else { metric }
}

#[cfg(windows)]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn sockaddr_ip(sockaddr: *const SockAddr) -> Option<IpAddr> {
    use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6, SOCKADDR_IN, SOCKADDR_IN6};

    if sockaddr.is_null() {
        return None;
    }
    // SAFETY: `sockaddr` comes from Windows `SOCKET_ADDRESS`; the family tag
    // determines which concrete sockaddr layout is valid.
    unsafe {
        match (*sockaddr).sa_family {
            AF_INET => {
                let addr = sockaddr.cast::<SOCKADDR_IN>();
                let bytes = (*addr).sin_addr.S_un.S_un_b;
                let address = Ipv4Addr::new(bytes.s_b1, bytes.s_b2, bytes.s_b3, bytes.s_b4);
                usable_dns_address(IpAddr::V4(address))
            }
            AF_INET6 => {
                let addr = sockaddr.cast::<SOCKADDR_IN6>();
                let address = std::net::Ipv6Addr::from((*addr).sin6_addr.u.Byte);
                usable_dns_address(IpAddr::V6(address))
            }
            _ => None,
        }
    }
}

#[cfg(windows)]
const fn usable_dns_address(address: IpAddr) -> Option<IpAddr> {
    match address {
        IpAddr::V4(address)
            if address.is_unspecified()
                || address.is_loopback()
                || address.is_multicast()
                || address.is_link_local() =>
        {
            None
        }
        IpAddr::V6(address)
            if address.is_unspecified()
                || address.is_loopback()
                || address.is_multicast()
                || address.is_unicast_link_local()
                || is_ipv6_site_local(address) =>
        {
            None
        }
        address => Some(address),
    }
}

#[cfg(windows)]
const fn is_ipv6_site_local(address: std::net::Ipv6Addr) -> bool {
    let octets = address.octets();
    octets[0] == 0xfe && (octets[1] & 0xc0) == 0xc0
}

#[cfg(all(test, windows))]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::usable_dns_address;

    #[test]
    fn windows_dns_filters_unusable_addresses() {
        assert_eq!(
            usable_dns_address(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1))),
            Some(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)))
        );
        assert_eq!(usable_dns_address(IpAddr::V4(Ipv4Addr::LOCALHOST)), None);
        assert_eq!(usable_dns_address(IpAddr::V6(Ipv6Addr::LOCALHOST)), None);
        assert_eq!(
            usable_dns_address(IpAddr::V6("fec0:0:0:ffff::1".parse().unwrap())),
            None
        );
        assert_eq!(usable_dns_address(IpAddr::V6("fe80::1".parse().unwrap())), None);
    }
}

/// Returns host DNS servers in priority order.
///
/// # Errors
///
/// Returns an error when the host DNS configuration cannot be queried.
#[cfg(not(any(unix, windows)))]
pub async fn system_dns_servers() -> std::io::Result<Vec<IpAddr>> {
    Ok(Vec::new())
}

#[must_use]
#[cfg(not(windows))]
pub const fn fallback_dns_server() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))
}

#[must_use]
#[cfg(windows)]
pub const fn fallback_dns_server() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))
}
