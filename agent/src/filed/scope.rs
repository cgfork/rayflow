//! Agent-side construction of run/version/revision-scoped file keys.

use catalog::path::{PathError, validate_relative_path};
use uuid::Uuid;

/// The execution and pinned node version that own an artifact.
#[derive(Clone, Debug)]
pub(super) struct Scope {
    pub run_id: Uuid,
    pub graph_name: String,
    pub version: u64,
    pub node_name: String,
    pub revision: u64,
}

impl Scope {
    /// Object key relative to the server's configured storage prefix.
    pub fn key(&self, filename: &str) -> Result<String, PathError> {
        validate_relative_path(filename)?;
        let name = format!(
            "{}/{}/{}/{}/{}/{filename}",
            self.run_id,
            name_segment(&self.graph_name)?,
            self.version,
            name_segment(&self.node_name)?,
            self.revision
        );
        if name.chars().count() > 512 {
            return Err(PathError);
        }
        Ok(name)
    }
}

fn name_segment(name: &str) -> Result<String, PathError> {
    if name.trim().is_empty() || matches!(name, "." | "..") || name.chars().any(char::is_control) {
        return Err(PathError);
    }
    const ENCODING: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    Ok(percent_encoding::utf8_percent_encode(name, ENCODING).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keys_include_run_and_pinned_names_version_revision() {
        let scope = Scope {
            run_id: Uuid::nil(),
            graph_name: "flow/a".into(),
            version: 7,
            node_name: "make data".into(),
            revision: 3,
        };
        assert_eq!(
            scope.key("dir/file.csv").unwrap(),
            "00000000-0000-0000-0000-000000000000/flow%2Fa/7/make%20data/3/dir/file.csv"
        );
        for path in [
            "/absolute",
            "../outside",
            "a//b",
            "a/./b",
            "a\\b",
            "C:/file",
            "",
        ] {
            assert!(scope.key(path).is_err());
        }
    }
}
