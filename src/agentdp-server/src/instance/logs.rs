use std::path::Path;

use agentdp_protocol::{InstanceLogsParams, InstanceLogsResult};

use super::{Error, Instance, path_text};

impl Instance {
    pub fn logs(&self, params: &InstanceLogsParams) -> Result<InstanceLogsResult, Error> {
        if params.lines == 0 {
            return Err(Error::InvalidLogLines);
        }
        let path = self.backend().log_path(&self.state.backend, params.file);
        let contents = read_tail(&path, params.lines)?;

        Ok(InstanceLogsResult {
            name: self.name(),
            file: params.file.as_str().to_owned(),
            path: path_text(&path),
            lines: params.lines,
            contents,
        })
    }
}

fn read_tail(path: &Path, line_count: usize) -> Result<String, Error> {
    let contents = std::fs::read_to_string(path).map_err(|source| Error::ReadLog {
        path: path.to_path_buf(),
        source,
    })?;
    let lines = contents.split_inclusive('\n').collect::<Vec<_>>();
    let start = lines.len().saturating_sub(line_count);
    Ok(lines[start..].concat())
}
