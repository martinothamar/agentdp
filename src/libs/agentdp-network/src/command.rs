#[derive(Debug)]
pub enum NetworkCommand {
    Stop,
}

pub trait NetworkCommandSource: Send + 'static {
    fn try_recv(&mut self) -> Option<NetworkCommand>;
}
