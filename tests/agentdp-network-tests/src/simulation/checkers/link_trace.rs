use std::time::Duration;

use super::super::{LinkDirection, LinkTraceEventKind, ScenarioReport};
use super::{Checker, Result};

#[derive(Debug, Clone)]
pub(crate) struct LinkTraceContains {
    direction: LinkDirection,
    event: LinkTraceEventKind,
    at: Option<Duration>,
    sequence: Option<u64>,
}

impl LinkTraceContains {
    pub(crate) const fn new(direction: LinkDirection, event: LinkTraceEventKind) -> Self {
        Self {
            direction,
            event,
            at: None,
            sequence: None,
        }
    }

    pub(crate) const fn at(mut self, at: Duration) -> Self {
        self.at = Some(at);
        self
    }

    pub(crate) const fn sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    fn matches(&self, event: &super::super::LinkTraceEvent) -> bool {
        event.direction == self.direction
            && event.event == self.event
            && self.at.is_none_or(|at| event.at == at)
            && self.sequence.is_none_or(|sequence| event.sequence == sequence)
    }
}

impl Checker for LinkTraceContains {
    fn name(&self) -> &'static str {
        "link_trace_contains"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        let found = report.link_trace.iter().any(|event| self.matches(event));
        if found {
            return Ok(());
        }

        Err(report.error(format!(
            "missing link trace event direction={} event={:?} at={:?} sequence={:?}",
            self.direction, self.event, self.at, self.sequence
        )))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LinkTracePrecedes {
    first: LinkTraceContains,
    second: LinkTraceContains,
}

impl LinkTracePrecedes {
    pub(crate) const fn new(first: LinkTraceContains, second: LinkTraceContains) -> Self {
        Self { first, second }
    }
}

impl Checker for LinkTracePrecedes {
    fn name(&self) -> &'static str {
        "link_trace_precedes"
    }

    fn check(&self, report: &ScenarioReport) -> Result<()> {
        let first = report.link_trace.iter().position(|event| self.first.matches(event));
        let second = report.link_trace.iter().position(|event| self.second.matches(event));
        if first.zip(second).is_some_and(|(first, second)| first < second) {
            return Ok(());
        }

        Err(report.error(format!(
            "missing ordered link trace events first={:?} second={:?}",
            self.first, self.second
        )))
    }
}
