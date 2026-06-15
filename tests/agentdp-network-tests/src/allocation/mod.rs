use std::alloc::System;
use std::path::Path;

use agentdp_network_tests::{AsyncDataplane, Result, payload};
use agentdp_test_support::{allocation::ReportingAllocator, snapshot};

#[global_allocator]
static ALLOCATOR: ReportingAllocator<System> = ReportingAllocator::new(System);

#[tokio::test(flavor = "current_thread")]
async fn established_tcp_roundtrip_reuses_hot_path_allocations() -> Result<()> {
    let payload = payload(256 * 1024);
    let mut response = Vec::with_capacity(payload.len());
    let mut dataplane = AsyncDataplane::start_with_established_tcp_server().await?;

    for _ in 0..16 {
        dataplane
            .established_tcp_roundtrip_into(&payload, &mut response)
            .await?;
        assert_eq!(response, payload);
    }

    let before = ALLOCATOR.begin_report();
    for _ in 0..8 {
        dataplane
            .established_tcp_roundtrip_into(&payload, &mut response)
            .await?;
        assert_eq!(response, payload);
    }
    let report = ALLOCATOR.report_since(before);

    snapshot::assert_file(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/allocation/snapshots")
            .join("established_tcp_roundtrip_reuses_hot_path_allocations.snap"),
        &format!("{report:#?}\n"),
    );
    dataplane.shutdown().await?;
    Ok(())
}
