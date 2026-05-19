use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortOverride {
    name: String,
    host: u16,
}

impl FromStr for PortOverride {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (name, host) = value
            .split_once(':')
            .ok_or_else(|| "expected NAME:HOST_PORT".to_owned())?;
        validate_port_name(name)?;
        let host = host
            .parse::<u16>()
            .map_err(|_| "host port must be a number from 1 to 65535".to_owned())?;
        if host == 0 {
            return Err("host port must be greater than zero".to_owned());
        }
        Ok(Self {
            name: name.to_owned(),
            host,
        })
    }
}

pub fn port_overrides(ports: &[PortOverride]) -> Result<BTreeMap<String, u16>, Error> {
    let mut values = BTreeMap::new();
    for port in ports {
        if values.insert(port.name.clone(), port.host).is_some() {
            return Err(Error::DuplicatePort(port.name.clone()));
        }
    }
    Ok(values)
}

fn validate_port_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("port name must not be empty".to_owned());
    }
    if name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err("port name may contain only ASCII letters, digits, '.', '_', and '-'".to_owned())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("port override was provided more than once: {0}")]
    DuplicatePort(String),
}
