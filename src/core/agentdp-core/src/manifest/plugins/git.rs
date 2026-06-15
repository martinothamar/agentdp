use serde::{Deserialize, Serialize};

use crate::provisioning::host_input::HostInputRequirements;

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Git {
    pub user: Option<User>,
    #[serde(default)]
    pub defaults: Defaults,
}

impl Git {
    pub(super) fn validate(&self, errors: &mut Vec<String>) {
        if let Some(user) = &self.user {
            user.validate(errors);
        }
        self.defaults.validate(errors);
    }

    pub(super) fn host_input_requirements(&self, requirements: &mut HostInputRequirements) {
        let Some(user) = &self.user else {
            return;
        };
        requirements.copy_custom_env([user.name.from_env.clone(), user.email.from_env.clone()]);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct User {
    pub name: EnvValue,
    pub email: EnvValue,
}

impl User {
    fn validate(&self, errors: &mut Vec<String>) {
        self.name.validate("plugins.git.user.name", errors);
        self.email.validate("plugins.git.user.email", errors);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvValue {
    pub from_env: String,
}

impl EnvValue {
    fn validate(&self, field: &str, errors: &mut Vec<String>) {
        super::super::validate_env_name(&format!("{field}.from_env"), &self.from_env, errors);
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    pub init_default_branch: Option<String>,
    pub autocrlf: Option<bool>,
}

impl Defaults {
    fn validate(&self, errors: &mut Vec<String>) {
        if let Some(init_default_branch) = &self.init_default_branch {
            super::super::validate_identifier("plugins.git.defaults.init_default_branch", init_default_branch, errors);
        }
    }
}
