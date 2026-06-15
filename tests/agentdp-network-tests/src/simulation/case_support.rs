use agentdp_network::{
    EgressPolicy, InstanceAddresses, InstanceMacAddresses, InstanceNetworkConfig, Ipv4AddressText, MacAddress,
};
use smoltcp::wire::Ipv4Address;

use super::{DriveBudget, GuestLink};
use super::{Result, ScenarioReport, Simulator, SmolTcpGuest, SteppedNetwork, TcpHandle};

pub(super) const fn allow_all_network_config() -> InstanceNetworkConfig {
    InstanceNetworkConfig::new(
        mediated_network_addresses(),
        mediated_network_mac(),
        EgressPolicy::allow_all(),
    )
}

pub(super) const fn mediated_network_addresses() -> InstanceAddresses {
    let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
    InstanceAddresses {
        gateway: ipv4_address(profile.gateway_ipv4),
        address: ipv4_address(profile.guest_ipv4),
        cidr_prefix: profile.ipv4_cidr_prefix,
    }
}

pub(super) const fn mediated_network_mac() -> InstanceMacAddresses {
    let profile = agentdp_core::mediated_network::DEFAULT_PROFILE;
    InstanceMacAddresses {
        gateway: MacAddress::new(profile.gateway_mac.octets()),
        guest: MacAddress::new(profile.guest_mac.octets()),
    }
}

const fn ipv4_address(address: std::net::Ipv4Addr) -> Ipv4AddressText {
    let [a, b, c, d] = address.octets();
    Ipv4AddressText(Ipv4Address::new(a, b, c, d))
}

pub(super) fn stop_tcp_report<N>(
    name: &'static str,
    mut sim: Simulator,
    mut guest: SmolTcpGuest,
    mut running: N,
    guest_link: &GuestLink,
    tcp: TcpHandle,
    clean_close: bool,
) -> Result<ScenarioReport>
where
    N: SteppedNetwork,
{
    if clean_close {
        guest.close(&mut running, tcp)?;
    } else {
        let _closed = guest.close(&mut running, tcp);
    }
    let quiescence = sim.drive_guest_network_until_quiescent(
        &mut guest,
        &mut running,
        guest_link,
        name,
        DriveBudget {
            max_steps: 4096,
            ..DriveBudget::default()
        },
    )?;
    let seed = sim.seed();
    let simulator_trace = sim.trace().to_vec();
    let stop = running.stop()?;
    Ok(ScenarioReport::new(
        name,
        seed,
        stop.final_status,
        quiescence,
        simulator_trace,
        guest_link.trace(),
        stop.network_events,
    ))
}

pub(super) fn stop_network_report<N>(
    name: &'static str,
    mut sim: Simulator,
    mut running: N,
    guest_link: &GuestLink,
) -> Result<ScenarioReport>
where
    N: SteppedNetwork,
{
    let quiescence = sim.drive_until_quiescent(&mut running, guest_link, name, DriveBudget::default())?;
    let seed = sim.seed();
    let simulator_trace = sim.trace().to_vec();
    let stop = running.stop()?;
    Ok(ScenarioReport::new(
        name,
        seed,
        stop.final_status,
        quiescence,
        simulator_trace,
        guest_link.trace(),
        stop.network_events,
    ))
}

pub(super) fn repeated_bytes(bytes: &[u8], iterations: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(bytes.len() * iterations);
    for _index in 0..iterations {
        output.extend_from_slice(bytes);
    }
    output
}
