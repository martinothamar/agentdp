pub trait Progress {
    fn info(&mut self, message: String);
}

#[cfg(test)]
#[derive(Debug, Default)]
pub struct NoopProgress;

#[cfg(test)]
impl Progress for NoopProgress {
    fn info(&mut self, _message: String) {}
}
