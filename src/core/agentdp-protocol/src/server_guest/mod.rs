mod framing;
mod message;
mod spec;

pub use crate::Error;
pub use framing::{
    decode_guest_message_line, decode_host_message_line, encode_guest_message_line, encode_host_message_line,
};
pub use message::{
    BOOTSTRAP_PLAN_VERSION, BootstrapFailed, BootstrapFinished, BootstrapLifecycleStatus, BootstrapOutput,
    BootstrapOutputStream, BootstrapPlan, BootstrapStatusReport, BootstrapStep, BootstrapStepFinished,
    BootstrapStepPhase, BootstrapStepResource, BootstrapStepStarted, BootstrapStepStatus,
    GUEST_CONTROL_PROTOCOL_VERSION, GuestCommandResult, GuestError, GuestHello, GuestMessage, GuestMessageKind,
    GuestdRole, HostCommand, HostMessage, HostMessageKind, RETRY_BOOTSTRAP_COMMAND, RetryBootstrapCommand,
    WRITE_USER_FILE_COMMAND, WriteUserFileCommand,
};
pub use spec::{GUEST_INSTANCE_SPEC_VERSION, GuestInstancePaths, GuestInstanceSpec, GuestInstanceUser, GuestPlatform};
