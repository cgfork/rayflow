//! Directory metadata shared by filesystem and transfer implementations.

use crate::path::{PathError, validate_relative_path};
use serde::{Deserialize, Serialize};

pub const DIRECTORY_MANIFEST: &str = ".rayflow-directory.json";
pub const DIRECTORY_FORMAT: &str = "application/vnd.rayflow.directory+json";
pub const MAX_MANIFEST_BYTES: u64 = 2 * 1024 * 1024;

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

#[cfg(test)]
mod tests {
    use super::*;
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
