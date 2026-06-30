use crate::RuntimeSecrets;

#[derive(Debug)]
pub enum NetworkCommand {
    UpdateSecrets(RuntimeSecrets),
    Stop,
}

pub trait NetworkCommandSource: Send + 'static {
    fn try_recv(&mut self) -> Option<NetworkCommand>;
}
