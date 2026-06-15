use serde::Serialize;

use crate::{Error, jsonl};

use super::{Request, ServerMessage};

/// Encodes a protocol value as one JSON object followed by a newline.
///
/// # Errors
///
/// Returns an error when the value cannot be serialized.
pub fn encode_line(value: &impl Serialize) -> Result<String, Error> {
    let line = jsonl::encode(value)?;
    String::from_utf8(line).map_err(|source| Error::Frame(format!("encoded JSONL frame was not UTF-8: {source}")))
}

/// Decodes a JSONL request line.
///
/// # Errors
///
/// Returns an error when the line is not a valid request object.
pub fn decode_request(line: &str) -> Result<Request, Error> {
    jsonl::decode(line.as_bytes())
}

/// Decodes a JSONL server message line.
///
/// # Errors
///
/// Returns an error when the line is not a valid server message object.
pub fn decode_server_message(line: &str) -> Result<ServerMessage, Error> {
    jsonl::decode(line.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use proptest::prelude::*;

    use crate::client_server::{
        AgentApplyParams, AgentInstanceExecParams, AgentInstanceListParams, AgentInstanceLogsParams,
        AgentInstanceSelector, AgentScaleParams, AgentSelector, AgentWaitCondition, AgentWaitParams, AgentWatchParams,
        BackendKind, Event, EventKind, EventLevel, LogFile, PingResult, Request, RequestFactory, RequestKind, Response,
        ServerDoctorParams, ServerMessage,
    };

    use super::{decode_request, decode_server_message, encode_line};

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
        let response = decoded.into_response().expect("expected response");
        assert!(response.is_ok());
        assert_eq!(response.id(), "cmd_1");
    }

    #[test]
    fn event_round_trips_as_server_message_json_line() {
        let line = encode_line(&ServerMessage::event(Event::diagnostic(
            "cmd_1",
            EventLevel::Info,
            "waiting for cloud-init",
        )))
        .expect("encode event");

        let decoded = decode_server_message(&line).expect("decode event");
        let event = decoded.into_event().expect("expected event");
        assert_eq!(event.id, "cmd_1");
        assert_eq!(
            event.event,
            EventKind::Diagnostic {
                level: EventLevel::Info,
                message: "waiting for cloud-init".to_owned()
            }
        );
    }

    #[test]
    fn exec_output_events_round_trip_as_server_message_json_line() {
        for event in [
            Event::session_stdout("cmd_1", "out\n"),
            Event::session_stderr("cmd_1", "err\n"),
        ] {
            let line = encode_line(&ServerMessage::event(event.clone())).expect("encode event");

            let decoded = decode_server_message(&line).expect("decode event");

            assert_eq!(decoded.into_event(), Some(event));
        }
    }

    #[test]
    fn agent_document_event_round_trips_as_server_message_json_line() {
        let document = serde_json::json!({
            "apiVersion": "agentdp.dev/v1alpha1",
            "kind": "Agent",
            "metadata": { "name": "altinn-studio" }
        });
        let event = Event::agent_document_value_changed("cmd_1", document.clone());
        let line = encode_line(&ServerMessage::event(event)).expect("encode event");

        let decoded = decode_server_message(&line).expect("decode event");

        assert_eq!(
            decoded.into_event(),
            Some(Event::agent_document_value_changed("cmd_1", document))
        );
    }

    #[test]
    fn request_with_params_round_trips_as_json_line() {
        let line = encode_line(
            &RequestFactory::new(7).request(RequestKind::AgentApply(AgentApplyParams {
                manifest: "/tmp/agent.yaml".into(),
            })),
        )
        .expect("encode request");

        let decoded = decode_request(&line).expect("decode request");
        assert_eq!(decoded.id, "cmd_7_0");
        let RequestKind::AgentApply(params) = decoded.kind else {
            panic!("expected agent.apply");
        };
        assert_eq!(params.manifest, PathBuf::from("/tmp/agent.yaml"));
    }

    #[test]
    fn request_params_reject_unknown_fields() {
        let line = r#"{"id":"cmd_1","method":"agent.instance.status","params":{"agent":"altinn-studio","instance_id":0,"extra":true}}"#;

        assert!(decode_request(line).is_err());
    }

    #[test]
    fn agent_apply_request_accepts_manifest_only() {
        let line = r#"{"id":"cmd_1","method":"agent.apply","params":{"manifest":"/tmp/agent.yaml"}}"#;

        let decoded = decode_request(line).expect("decode apply request");
        let RequestKind::AgentApply(params) = decoded.kind else {
            panic!("expected apply request");
        };
        assert_eq!(params.manifest, PathBuf::from("/tmp/agent.yaml"));
    }

    #[test]
    fn agent_scale_request_round_trips_as_json_line() {
        let line = encode_line(
            &RequestFactory::new(7).request(RequestKind::AgentScale(AgentScaleParams {
                agent: "altinn-studio".to_owned(),
                replicas: 0,
            })),
        )
        .expect("encode request");

        let decoded = decode_request(&line).expect("decode request");
        let RequestKind::AgentScale(params) = decoded.kind else {
            panic!("expected agent.scale");
        };
        assert_eq!(params.agent, "altinn-studio");
        assert_eq!(params.replicas, 0);
    }

    #[test]
    fn request_envelope_rejects_unknown_fields() {
        let line = r#"{"id":"cmd_1","method":"server.ping","extra":true}"#;

        assert!(decode_request(line).is_err());
    }

    #[test]
    fn request_without_params_method_rejects_params() {
        let line = r#"{"id":"cmd_1","method":"server.ping","params":{}}"#;

        assert!(decode_request(line).is_err());
    }

    #[test]
    fn server_message_rejects_unknown_fields() {
        let line = r#"{"type":"event","event":{"id":"cmd_1","event":"diagnostic","level":"info","message":"ok"},"extra":true}"#;

        assert!(decode_server_message(line).is_err());
    }

    #[test]
    fn server_message_rejects_missing_payload_for_type() {
        assert!(decode_server_message(r#"{"type":"response"}"#).is_err());
        assert!(decode_server_message(r#"{"type":"event"}"#).is_err());
    }

    #[test]
    fn server_message_rejects_mismatched_payload_for_type() {
        let response_as_event =
            r#"{"type":"event","response":{"id":"cmd_1","ok":true,"result":{"service":"agentdp-server","pid":123}}}"#;
        let event_as_response =
            r#"{"type":"response","event":{"id":"cmd_1","event":"diagnostic","level":"info","message":"ok"}}"#;

        assert!(decode_server_message(response_as_event).is_err());
        assert!(decode_server_message(event_as_response).is_err());
    }

    #[test]
    fn server_message_rejects_both_payloads() {
        let line = r#"{"type":"event","response":{"id":"cmd_1","ok":true,"result":{"service":"agentdp-server","pid":123}},"event":{"id":"cmd_1","event":"diagnostic","level":"info","message":"ok"}}"#;

        assert!(decode_server_message(line).is_err());
    }

    #[test]
    fn event_message_rejects_unknown_fields() {
        let line = r#"{"type":"event","event":{"id":"cmd_1","event":"diagnostic","level":"info","message":"ok","extra":true}}"#;

        assert!(decode_server_message(line).is_err());
    }

    #[test]
    fn typed_response_result_rejects_unknown_fields() {
        let line = r#"{"type":"response","response":{"id":"cmd_1","ok":true,"result":{"service":"agentdp-server","pid":123,"extra":true}}}"#;

        let decoded = decode_server_message(line).expect("decode response envelope");
        let response = decoded.into_response().expect("expected response");

        assert!(response.result::<PingResult>().is_err());
    }

    #[test]
    fn response_rejects_missing_payload_for_status() {
        let success = r#"{"type":"response","response":{"id":"cmd_1","ok":true}}"#;
        let failure = r#"{"type":"response","response":{"id":"cmd_1","ok":false}}"#;

        assert!(decode_server_message(success).is_err());
        assert!(decode_server_message(failure).is_err());
    }

    #[test]
    fn response_rejects_mismatched_payload_for_status() {
        let success_with_error =
            r#"{"type":"response","response":{"id":"cmd_1","ok":true,"error":{"code":"boom","message":"bad"}}}"#;
        let failure_with_result = r#"{"type":"response","response":{"id":"cmd_1","ok":false,"result":{"service":"agentdp-server","pid":123}}}"#;

        assert!(decode_server_message(success_with_error).is_err());
        assert!(decode_server_message(failure_with_result).is_err());
    }

    #[test]
    fn response_rejects_both_payloads() {
        let line = r#"{"type":"response","response":{"id":"cmd_1","ok":true,"result":{"service":"agentdp-server","pid":123},"error":{"code":"boom","message":"bad"}}}"#;

        assert!(decode_server_message(line).is_err());
    }

    proptest! {
        #[test]
        fn arbitrary_requests_round_trip(id in bounded_text(), kind in request_kind()) {
            let request = Request::new(id, kind);
            let line = encode_line(&request).expect("encode request");

            prop_assert!(line.ends_with('\n'));
            prop_assert_eq!(decode_request(&line).expect("decode request"), request);
        }

        #[test]
        fn success_responses_round_trip(
            id in bounded_text(),
            service in bounded_text(),
            pid in any::<u32>(),
            version in prop::option::of(bounded_text()),
            executable in prop::option::of(bounded_text()),
        ) {
            let result = PingResult {
                service,
                pid,
                version,
                executable,
            };
            let line = encode_line(&ServerMessage::response(Response::success(id.clone(), &result)))
                .expect("encode response");
            let decoded = decode_server_message(&line).expect("decode response");
            let response = decoded.into_response().expect("response message");

            prop_assert!(response.is_ok());
            prop_assert_eq!(response.id(), id);
            prop_assert_eq!(response.result::<PingResult>().expect("decode result"), result);
        }

        #[test]
        fn diagnostic_events_round_trip(id in bounded_text(), message in bounded_text()) {
            let event = Event::diagnostic(id.clone(), EventLevel::Info, message.clone());
            let line = encode_line(&ServerMessage::event(event)).expect("encode event");
            let decoded = decode_server_message(&line).expect("decode event");

            prop_assert_eq!(
                decoded.into_event().expect("event message"),
                Event::diagnostic(id, EventLevel::Info, message)
            );
        }

        #[test]
        fn exec_output_events_round_trip(
            id in bounded_text(),
            message in bounded_text(),
            stdout in any::<bool>(),
        ) {
            let event = if stdout {
                Event::session_stdout(id.clone(), message.clone())
            } else {
                Event::session_stderr(id.clone(), message.clone())
            };
            let line = encode_line(&ServerMessage::event(event)).expect("encode event");
            let decoded = decode_server_message(&line).expect("decode event");
            let decoded = decoded.into_event().expect("event message");

            prop_assert_eq!(decoded.id, id);
            match decoded.event {
                EventKind::SessionOutput { chunk, .. } => {
                    prop_assert_eq!(chunk, message);
                }
                EventKind::Diagnostic { .. } | EventKind::AgentDocumentChanged { .. } | EventKind::AgentEvent { .. } => {
                    prop_assert!(false, "expected exec output event");
                }
            }
        }
    }

    fn request_kind() -> impl Strategy<Value = RequestKind> {
        prop_oneof![
            Just(RequestKind::ServerPing),
            Just(RequestKind::ServerShutdown),
            Just(RequestKind::ServerDoctor(ServerDoctorParams {
                backend: BackendKind::Qemu,
            })),
            path().prop_map(|manifest| RequestKind::AgentApply(AgentApplyParams { manifest })),
            (bounded_text(), any::<u16>())
                .prop_map(|(agent, replicas)| RequestKind::AgentScale(AgentScaleParams { agent, replicas })),
            bounded_text().prop_map(|agent| RequestKind::AgentDelete(AgentSelector { agent })),
            bounded_text().prop_map(|agent| RequestKind::AgentStatus(AgentSelector { agent })),
            (
                bounded_text(),
                any::<u64>(),
                wait_condition(),
                prop::option::of(1_u64..3600)
            )
                .prop_map(|(agent, generation, condition, timeout_seconds)| {
                    RequestKind::AgentWait(AgentWaitParams {
                        agent,
                        generation,
                        condition,
                        timeout_seconds,
                    })
                }),
            bounded_text().prop_map(|agent| RequestKind::AgentWatch(AgentWatchParams { agent })),
            instance_selector().prop_map(RequestKind::AgentInstanceStatus),
            instance_selector().prop_map(RequestKind::AgentInstanceShell),
            (
                bounded_text(),
                any::<u32>(),
                prop::collection::vec(bounded_text(), 0..8),
                prop::option::of(1_u64..3600)
            )
                .prop_map(|(agent, instance_id, command, timeout_seconds)| {
                    RequestKind::AgentInstanceExec(AgentInstanceExecParams {
                        agent,
                        instance_id,
                        command,
                        timeout_seconds,
                    })
                }),
            (bounded_text(), any::<u32>(), log_file(), 0_usize..1000).prop_map(|(agent, instance_id, file, lines)| {
                RequestKind::AgentInstanceLogs(AgentInstanceLogsParams {
                    agent,
                    instance_id,
                    file,
                    lines,
                })
            }),
            prop::option::of(bounded_text())
                .prop_map(|agent| RequestKind::AgentInstanceList(AgentInstanceListParams { agent })),
        ]
    }

    fn instance_selector() -> impl Strategy<Value = AgentInstanceSelector> {
        (bounded_text(), any::<u32>()).prop_map(|(agent, instance_id)| AgentInstanceSelector { agent, instance_id })
    }

    fn path() -> impl Strategy<Value = PathBuf> {
        bounded_text().prop_map(PathBuf::from)
    }

    fn log_file() -> impl Strategy<Value = LogFile> {
        prop_oneof![Just(LogFile::Serial), Just(LogFile::Qemu), Just(LogFile::Events)]
    }

    fn wait_condition() -> impl Strategy<Value = AgentWaitCondition> {
        prop_oneof![
            Just(AgentWaitCondition::Accepted),
            Just(AgentWaitCondition::Observed),
            Just(AgentWaitCondition::Ready),
            Just(AgentWaitCondition::Paused),
            Just(AgentWaitCondition::Stopped),
            Just(AgentWaitCondition::Deleted),
        ]
    }

    fn bounded_text() -> impl Strategy<Value = String> {
        "[A-Za-z0-9._/ -]{0,48}"
    }
}
