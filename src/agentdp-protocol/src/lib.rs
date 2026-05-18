#![forbid(unsafe_code)]

mod error;
mod framing;
mod message;
mod params;
mod request;
mod response;
mod results;

pub use agentdp_core::backend::BackendKind;
pub use error::Error;
pub use framing::{decode_request, decode_server_message, encode_line};
pub use message::{Event, EventKind, EventLevel, ServerMessage, ServerMessageType};
pub use params::{
    InstanceCreateParams, InstanceExecParams, InstanceLogsParams, InstancePsParams, InstanceRef, LogFile,
    ProvisioningPlanParams, ServerDoctorParams,
};
pub use request::{Request, RequestFactory, RequestKind, request};
pub use response::{ErrorObject, Response, invalid_request};
pub use results::{
    BackendCreateResult, BackendProvisioningResult, BackendRuntimeResult, BackendStatusResult, DoctorCheckResult,
    GuestAccessResult, HealthcheckResult, HostCommandResult, ImageResult, InstanceCreateResult, InstanceDownResult,
    InstanceExecResult, InstanceListItem, InstanceLogsResult, InstancePsResult, InstanceRmResult, InstanceShellResult,
    InstanceStatusResult, InstanceUpResult, ManifestResult, NetworkResult, PingResult, PortMappingResult,
    PortProtocolResult, ProcessResult, ProvisioningImageResult, ProvisioningPlanResult, QemuCreateResult,
    QemuImageResult, QemuProvisioningResult, QemuRuntimeResult, QemuStatusResult, ReadinessResult,
    ReadinessStateResult, SeedResult, ServerDoctorResult, ServiceResult, ShutdownResult,
};

#[cfg(test)]
mod tests {
    use super::{
        Event, InstanceCreateParams, PingResult, RequestFactory, RequestKind, Response, ServerMessage, decode_request,
        decode_server_message, encode_line,
    };

    #[test]
    fn request_round_trips_as_json_line() {
        let line = encode_line(&RequestFactory::new(7).request(RequestKind::ServerPing)).expect("encode request");
        assert!(line.ends_with('\n'));

        let decoded = decode_request(&line).expect("decode request");
        assert_eq!(decoded.id, "cmd_7_0");
        assert!(matches!(decoded.kind, RequestKind::ServerPing));
    }

    #[test]
    fn response_round_trips_as_server_message_json_line() {
        let response = Response::success(
            "cmd_1",
            PingResult {
                service: "agentdp-server".to_owned(),
                pid: 123,
                version: None,
                executable: None,
            },
        );
        let line = encode_line(&ServerMessage::response(response)).expect("encode response");

        let decoded = decode_server_message(&line).expect("decode response");
        let response = decoded.response.expect("expected response");
        assert!(response.ok);
        assert_eq!(response.id, "cmd_1");
    }

    #[test]
    fn event_round_trips_as_server_message_json_line() {
        let line =
            encode_line(&ServerMessage::event(Event::info("cmd_1", "waiting for cloud-init"))).expect("encode event");

        let decoded = decode_server_message(&line).expect("decode event");
        let event = decoded.event.expect("expected event");
        assert_eq!(event.id, "cmd_1");
        assert_eq!(event.message, "waiting for cloud-init");
    }

    #[test]
    fn request_with_params_round_trips_as_json_line() {
        let line = encode_line(
            &RequestFactory::new(7).request(RequestKind::InstanceCreate(InstanceCreateParams {
                manifest: "/tmp/agent.yaml".into(),
                instance: "pr-0".to_owned(),
                ports: std::collections::BTreeMap::default(),
            })),
        )
        .expect("encode request");

        let decoded = decode_request(&line).expect("decode request");
        assert_eq!(decoded.id, "cmd_7_0");
        let RequestKind::InstanceCreate(params) = decoded.kind else {
            panic!("expected instance.create");
        };
        assert_eq!(params.instance, "pr-0");
    }
}
