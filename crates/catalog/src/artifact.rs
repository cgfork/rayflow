//! Shared job artifact paths, directory manifests, and transfer API payloads.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{PresignedUrl, model::Catalog};

pub const DIRECTORY_MANIFEST: &str = ".rayflow-directory.json";
pub const DIRECTORY_FORMAT: &str = "application/vnd.rayflow.directory+json";
pub const MAX_MANIFEST_BYTES: u64 = 2 * 1024 * 1024;

/// The execution and pinned node version that own an artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Scope {
    pub run_id: Uuid,
    pub graph_name: String,
    pub version: u64,
    pub node_name: String,
    pub revision: u64,
}

impl Scope {
    pub fn fullpath(&self, prefix: &str, filename: &str) -> Result<String, PathError> {
        let prefix = prefix.trim_matches('/');
        validate_relative_path(prefix)?;
        let name = format!("/{prefix}/{}", self.key(filename)?);
        if name.chars().count() > 512 {
            return Err(PathError);
        }
        Ok(name)
    }

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

#[derive(Debug, thiserror::Error)]
#[error(
    "artifact paths must be nonempty relative paths without empty, dot, parent, backslash, or control components"
)]
pub struct PathError;

pub fn validate_relative_path(path: &str) -> Result<(), PathError> {
    if path.is_empty()
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".." || part.contains(':'))
    {
        return Err(PathError);
    }
    Ok(())
}

/// Directory entries are relative to the declared output, never host paths.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryManifest {
    pub version: u32,
    pub entries: Vec<DirectoryEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub path: String,
    pub kind: EntryKind,
    pub size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
}

impl DirectoryManifest {
    pub fn validate(&self) -> Result<(), PathError> {
        if self.version != 1 || self.entries.len() > 10_000 {
            return Err(PathError);
        }
        let mut names = std::collections::BTreeMap::new();
        for entry in &self.entries {
            validate_relative_path(&entry.path)?;
            if entry.path.split('/').any(|part| part == DIRECTORY_MANIFEST)
                || (entry.kind == EntryKind::Directory && entry.size != 0)
                || names.insert(entry.path.as_str(), entry.kind).is_some()
            {
                return Err(PathError);
            }
        }
        for entry in &self.entries {
            let mut parent = entry.path.as_str();
            while let Some((path, _)) = parent.rsplit_once('/') {
                if names.get(path) != Some(&EntryKind::Directory) {
                    return Err(PathError);
                }
                parent = path;
            }
        }
        Ok(())
    }
}

/// Prepare an upload. The server creates or reuses the catalog by object URL.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareUploadRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    /// Complete object key relative to the configured storage prefix.
    pub key: String,
    pub size: u64,
    pub format: String,
}

/// Commit an upload with the catalog UUID returned by prepare.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitUploadRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub key: String,
    pub catalog_uuid: Uuid,
    pub size: u64,
    pub format: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub job_id: Uuid,
    pub agent_id: Uuid,
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transfer {
    pub catalog: Catalog,
    pub presigned: PresignedUrl,
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
            scope.fullpath("/prefix/", "dir/file.csv").unwrap(),
            "/prefix/00000000-0000-0000-0000-000000000000/flow%2Fa/7/make%20data/3/dir/file.csv"
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
            assert!(scope.fullpath("prefix", path).is_err());
        }
    }

    #[test]
    fn manifests_validate_tree_and_reject_escaping_or_conflicting_entries() {
        let directory = DirectoryEntry {
            path: "dir".into(),
            kind: EntryKind::Directory,
            size: 0,
        };
        let file = DirectoryEntry {
            path: "dir/a".into(),
            kind: EntryKind::File,
            size: 12,
        };
        assert!(
            DirectoryManifest {
                version: 1,
                entries: vec![file.clone(), directory.clone()]
            }
            .validate()
            .is_ok()
        );
        for entries in [
            vec![file.clone()],
            vec![directory.clone(), directory],
            vec![DirectoryEntry {
                path: "../outside".into(),
                ..file.clone()
            }],
            vec![DirectoryEntry {
                path: DIRECTORY_MANIFEST.into(),
                ..file
            }],
        ] {
            assert!(
                DirectoryManifest {
                    version: 1,
                    entries
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            DirectoryManifest {
                version: 1,
                entries: vec![]
            }
            .validate()
            .is_ok()
        );
    }
}
