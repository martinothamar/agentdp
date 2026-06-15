use std::collections::{BTreeMap, BTreeSet};

use agentdp_protocol::server_guest::{BootstrapPlan, BootstrapStep, BootstrapStepPhase};

use super::BootstrapState;
use crate::{Error, Result};

#[derive(Debug)]
pub(super) struct BootstrapScheduler {
    steps: Vec<BootstrapStep>,
    running: BTreeSet<String>,
    max_parallelism: usize,
    stopping: bool,
}

impl BootstrapScheduler {
    pub(super) fn new(plan: &BootstrapPlan) -> Result<Self> {
        Ok(Self {
            steps: ordered_steps(plan)?,
            running: BTreeSet::new(),
            max_parallelism: bootstrap_parallelism(),
            stopping: false,
        })
    }

    pub(super) fn steps(&self) -> &[BootstrapStep] {
        &self.steps
    }

    pub(super) fn can_start(&self) -> bool {
        !self.stopping && self.running.len() < self.max_parallelism
    }

    pub(super) fn next_ready(&self, state: &BootstrapState) -> Option<BootstrapStep> {
        self.steps
            .iter()
            .find(|step| {
                !state.step_passed(&step.id)
                    && !self.running.contains(&step.id)
                    && self.phase_ready(step, state)
                    && dependencies_passed(step, state)
                    && self.resources_available(step)
            })
            .cloned()
    }

    pub(super) fn mark_running(&mut self, step: &BootstrapStep) {
        self.running.insert(step.id.clone());
    }

    pub(super) fn mark_finished(&mut self, step: &BootstrapStep) {
        self.running.remove(&step.id);
    }

    pub(super) const fn stop(&mut self) {
        self.stopping = true;
    }

    pub(super) fn is_drained(&self) -> bool {
        self.running.is_empty()
    }

    pub(super) const fn is_stopping(&self) -> bool {
        self.stopping
    }

    fn phase_ready(&self, step: &BootstrapStep, state: &BootstrapState) -> bool {
        step.phase == BootstrapStepPhase::System
            || self
                .steps
                .iter()
                .filter(|candidate| candidate.phase == BootstrapStepPhase::System)
                .all(|candidate| state.step_passed(&candidate.id))
    }

    fn resources_available(&self, step: &BootstrapStep) -> bool {
        if step.resources.is_empty() {
            return true;
        }
        !self
            .steps
            .iter()
            .filter(|candidate| self.running.contains(&candidate.id))
            .any(|candidate| {
                candidate
                    .resources
                    .iter()
                    .any(|resource| step.resources.contains(resource))
            })
    }
}

pub(super) fn ordered_steps(plan: &BootstrapPlan) -> Result<Vec<BootstrapStep>> {
    let mut steps_by_id = BTreeMap::new();
    for step in &plan.steps {
        if steps_by_id.insert(step.id.clone(), step.clone()).is_some() {
            return Err(Error::Message(format!("duplicate bootstrap step id {}", step.id)));
        }
    }

    let mut dependents = BTreeMap::<String, Vec<String>>::new();
    let mut remaining_dependencies = BTreeMap::<String, usize>::new();
    for step in &plan.steps {
        remaining_dependencies.insert(step.id.clone(), step.depends_on.len());
        for dependency in &step.depends_on {
            let Some(dependency_step) = steps_by_id.get(dependency) else {
                return Err(Error::Message(format!(
                    "bootstrap step {} depends on unknown step {dependency}",
                    step.id
                )));
            };
            if step.phase == BootstrapStepPhase::System && dependency_step.phase == BootstrapStepPhase::User {
                return Err(Error::Message(format!(
                    "system bootstrap step {} must not depend on user bootstrap step {dependency}",
                    step.id
                )));
            }
            dependents.entry(dependency.clone()).or_default().push(step.id.clone());
        }
    }

    let mut ready_system = BTreeSet::new();
    let mut ready_user = BTreeSet::new();
    for step in &plan.steps {
        if step.depends_on.is_empty() {
            ready_insert(step, &mut ready_system, &mut ready_user);
        }
    }

    let mut ordered = Vec::with_capacity(plan.steps.len());
    while let Some(step_id) = pop_next_ready(&mut ready_system, &mut ready_user) {
        let step = steps_by_id
            .get(&step_id)
            .ok_or_else(|| Error::Message(format!("bootstrap step {step_id} disappeared during ordering")))?;
        ordered.push(step.clone());
        for dependent in dependents.get(&step_id).into_iter().flatten() {
            let count = remaining_dependencies.get_mut(dependent).ok_or_else(|| {
                Error::Message(format!(
                    "bootstrap step {dependent} disappeared during dependency ordering"
                ))
            })?;
            *count = count.saturating_sub(1);
            if *count == 0 {
                let step = steps_by_id.get(dependent).ok_or_else(|| {
                    Error::Message(format!("bootstrap step {dependent} disappeared during ready ordering"))
                })?;
                ready_insert(step, &mut ready_system, &mut ready_user);
            }
        }
    }

    if ordered.len() != plan.steps.len() {
        return Err(Error::Message(
            "bootstrap step dependency graph contains a cycle".to_owned(),
        ));
    }
    Ok(ordered)
}

fn dependencies_passed(step: &BootstrapStep, state: &BootstrapState) -> bool {
    step.depends_on.iter().all(|dependency| state.step_passed(dependency))
}

fn ready_insert(step: &BootstrapStep, ready_system: &mut BTreeSet<String>, ready_user: &mut BTreeSet<String>) {
    match step.phase {
        BootstrapStepPhase::System => {
            ready_system.insert(step.id.clone());
        }
        BootstrapStepPhase::User => {
            ready_user.insert(step.id.clone());
        }
    }
}

fn pop_next_ready(ready_system: &mut BTreeSet<String>, ready_user: &mut BTreeSet<String>) -> Option<String> {
    pop_first(ready_system).or_else(|| pop_first(ready_user))
}

fn pop_first(values: &mut BTreeSet<String>) -> Option<String> {
    let value = values.first().cloned()?;
    values.remove(&value);
    Some(value)
}

fn bootstrap_parallelism() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

#[cfg(test)]
mod tests {
    use agentdp_protocol::server_guest::BootstrapStepResource;

    use super::*;

    #[test]
    fn orders_steps_topologically_with_system_before_user() {
        let ordered = ordered_steps(&plan(vec![
            user_step("user.ready", []),
            system_step("system.after", ["system.before"]),
            system_step("system.before", []),
        ]))
        .expect("ordered steps");

        let ids = ordered.into_iter().map(|step| step.id).collect::<Vec<_>>();

        assert_eq!(ids, ["system.before", "system.after", "user.ready"]);
    }

    #[test]
    fn rejects_duplicate_step_ids() {
        let error = ordered_steps(&plan(vec![
            system_step("system.same", []),
            system_step("system.same", []),
        ]))
        .expect_err("duplicate id");

        assert!(error.to_string().contains("duplicate"));
    }

    #[test]
    fn rejects_unknown_dependencies() {
        let error = ordered_steps(&plan(vec![system_step("system.after", ["system.missing"])]))
            .expect_err("unknown dependency");

        assert!(error.to_string().contains("unknown"));
    }

    #[test]
    fn rejects_dependency_cycles() {
        let error = ordered_steps(&plan(vec![
            system_step("system.first", ["system.second"]),
            system_step("system.second", ["system.first"]),
        ]))
        .expect_err("cycle");

        assert!(error.to_string().contains("cycle"));
    }

    #[test]
    fn rejects_system_steps_that_depend_on_user_steps() {
        let error = ordered_steps(&plan(vec![
            system_step("system.root", []),
            user_step("user.first", ["system.root"]),
            system_step("system.after", ["user.first"]),
        ]))
        .expect_err("system depends on user");

        assert!(error.to_string().contains("must not depend on user"));
    }

    #[test]
    fn does_not_start_user_phase_until_all_system_steps_pass() {
        let system_done = system_step("system.done", []);
        let system_pending = system_step("system.pending", []);
        let mut state = state();
        state.mark_passed(&system_done, 0, 1);
        let scheduler = BootstrapScheduler::new(&plan(vec![
            system_done,
            user_step("user.ready", ["system.done"]),
            system_pending,
        ]))
        .expect("scheduler");

        let ready = scheduler.next_ready(&state).expect("ready step");

        assert_eq!(ready.id, "system.pending");
    }

    #[test]
    fn does_not_start_steps_with_running_resource_conflicts() {
        let mut scheduler = BootstrapScheduler::new(&plan(vec![
            system_step_with_resources("system.package_a", [], [BootstrapStepResource::PackageManager]),
            system_step_with_resources("system.package_b", [], [BootstrapStepResource::PackageManager]),
        ]))
        .expect("scheduler");
        let state = state();
        let running = scheduler.next_ready(&state).expect("first ready step");
        scheduler.mark_running(&running);

        assert!(scheduler.next_ready(&state).is_none());
    }

    #[test]
    fn allows_parallel_steps_with_disjoint_or_empty_resources() {
        let mut scheduler = BootstrapScheduler::new(&plan(vec![
            system_step_with_resources("system.package", [], [BootstrapStepResource::PackageManager]),
            system_step_with_resources("system.service", [], [BootstrapStepResource::Systemd]),
            system_step("system.unlocked", []),
        ]))
        .expect("scheduler");
        let state = state();
        let running = scheduler.next_ready(&state).expect("first ready step");
        scheduler.mark_running(&running);

        let ready = scheduler.next_ready(&state).expect("second ready step");

        assert_eq!(ready.id, "system.service");
    }

    #[test]
    fn stop_prevents_new_steps_but_allows_running_drain() {
        let mut scheduler = BootstrapScheduler::new(&plan(vec![
            system_step("system.first", []),
            system_step("system.second", []),
        ]))
        .expect("scheduler");
        let state = state();
        let running = scheduler.next_ready(&state).expect("running step");
        scheduler.mark_running(&running);

        scheduler.stop();

        assert!(!scheduler.can_start());
        assert!(!scheduler.is_drained());
        scheduler.mark_finished(&running);
        assert!(scheduler.is_drained());
    }

    fn plan(steps: Vec<BootstrapStep>) -> BootstrapPlan {
        BootstrapPlan {
            plan_version: 1,
            user: "agent".to_owned(),
            home: "/data/home".to_owned(),
            code_dir: "/data/home/code".to_owned(),
            steps,
        }
    }

    fn state() -> BootstrapState {
        BootstrapState {
            plan_hash: "test".to_owned(),
            steps: BTreeMap::default(),
        }
    }

    fn system_step(id: &str, depends_on: impl IntoIterator<Item = &'static str>) -> BootstrapStep {
        step(id, BootstrapStepPhase::System, depends_on, [])
    }

    fn user_step(id: &str, depends_on: impl IntoIterator<Item = &'static str>) -> BootstrapStep {
        step(id, BootstrapStepPhase::User, depends_on, [])
    }

    fn system_step_with_resources(
        id: &str,
        depends_on: impl IntoIterator<Item = &'static str>,
        resources: impl IntoIterator<Item = BootstrapStepResource>,
    ) -> BootstrapStep {
        step(id, BootstrapStepPhase::System, depends_on, resources)
    }

    fn step(
        id: &str,
        phase: BootstrapStepPhase,
        depends_on: impl IntoIterator<Item = &'static str>,
        resources: impl IntoIterator<Item = BootstrapStepResource>,
    ) -> BootstrapStep {
        BootstrapStep {
            id: id.to_owned(),
            label: id.to_owned(),
            phase,
            depends_on: depends_on.into_iter().map(str::to_owned).collect(),
            resources: resources.into_iter().collect(),
            script: format!("steps/{id}.sh"),
            working_directory: "/".to_owned(),
            timeout_seconds: 30,
        }
    }
}
