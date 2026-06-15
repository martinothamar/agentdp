mod dataplane;

pub use dataplane::{
    AgentWorkflowHarness, AsyncDataplane, Result, agent_https_request, agent_https_response, http_get_request, payload,
};

#[tokio::test(flavor = "current_thread")]
async fn mediated_https_http1_roundtrip_observes_clean_guest_tls_close() -> Result<()> {
    let mut harness = AgentWorkflowHarness::start_https_http1(16 * 1024).await?;
    let request = agent_https_request(harness.host(), 1024);
    let response = harness.https_http1_roundtrip(&request).await?;
    assert_eq!(response, agent_https_response(16 * 1024));
    harness.shutdown().await
}
