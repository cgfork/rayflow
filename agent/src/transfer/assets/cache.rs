use std::{
    fs::{self, File, Metadata, TryLockError},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use sha2::{Digest, Sha256};
use tokio::{io::AsyncReadExt, task::spawn_blocking, time};
use uuid::Uuid;

use super::super::check_digest;
use crate::{errors::Error, settings::Settings};

const LOCK_RETRY_DELAY: Duration = Duration::from_millis(25);

pub(super) struct AssetCache<'a> {
    asset_dir: &'a Path,
    max_bytes: u64,
}

pub(super) struct LockedAsset {
    directory: PathBuf,
    lock: Arc<File>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CacheFileKind {
    // Reclaim these before applying the byte budget to verified files.
    Retired,
    Download,
    Verified,
}

struct CacheFile {
    asset_id: Uuid,
    path: PathBuf,
    kind: CacheFileKind,
    modified: SystemTime,
    size: u64,
}

impl<'a> AssetCache<'a> {
    pub(super) fn new(settings: &'a Settings) -> Self {
        Self {
            asset_dir: &settings.asset_dir,
            max_bytes: settings.asset_cache_max_bytes,
        }
    }

    pub(super) async fn ensure_job_dir(&self, job_dir: PathBuf) -> Result<(), Error> {
        let asset_dir: PathBuf = self.asset_dir.components().collect();
        spawn_blocking(move || {
            ensure_asset_root(&asset_dir)?;
            ensure_directory(&asset_dir.join("jobs"))?;
            ensure_directory(&job_dir)
        })
        .await?
    }

    pub(super) async fn initialize(&self) -> Result<(), Error> {
        let asset_dir: PathBuf = self.asset_dir.components().collect();
        let root = self.root();
        spawn_blocking(move || {
            ensure_asset_root(&asset_dir)?;
            ensure_directory(&root)
        })
        .await?
    }

    fn root(&self) -> PathBuf {
        self.asset_dir.join("cache")
    }

    pub(super) async fn lock_asset(&self, asset_id: Uuid) -> Result<LockedAsset, Error> {
        let root = self.root();
        let lock = wait_for_lock(asset_lock_path(&root, asset_id)).await?;
        let directory = root.join(asset_id.to_string());
        spawn_blocking(move || {
            ensure_directory(&directory)?;
            Ok(LockedAsset {
                directory,
                lock: Arc::new(lock),
            })
        })
        .await?
    }

    pub(super) async fn trim(&self) -> Result<(), Error> {
        let root = self.root();
        let parent: PathBuf = self.asset_dir.components().collect();
        let exists = {
            let cache_dir = root.clone();
            spawn_blocking(move || {
                Ok::<_, Error>(directory_exists(&parent)? && directory_exists(&cache_dir)?)
            })
            .await??
        };
        if !exists {
            return Ok(());
        }

        // Serialize scans and evictions. Downloads and links use asset locks
        // alone, so an unrelated asset can still stage while this scan runs.
        let sweep = wait_for_lock(root.join(".trim.lock")).await?;
        let max_bytes = self.max_bytes;
        spawn_blocking(move || {
            let _sweep = sweep;
            evict_files(&root, max_bytes)
        })
        .await?
    }
}

impl LockedAsset {
    pub(super) async fn verified_file(&self) -> Result<Option<PathBuf>, Error> {
        let mut files = tokio::fs::read_dir(&self.directory).await?;
        while let Some(file) = files.next_entry().await? {
            let name = file.file_name();
            let Some(digest) = name.to_str().and_then(verified_digest) else {
                continue;
            };
            let path = file.path();
            let metadata = checked_file(&path, tokio::fs::symlink_metadata(&path).await)?;
            let Some(metadata) = metadata else { continue };
            if digest_matches(&path, metadata.len(), digest).await? {
                return Ok(Some(path));
            }
            let retired = self
                .directory
                .join(format!("retired-{}.blob", Uuid::new_v4()));
            self.mutate(move || {
                // Leave the old inode in the cache until the last job releases
                // its link. A new download publishes a different inode.
                if is_unpinned(&path, &fs::symlink_metadata(&path)?) {
                    fs::remove_file(&path)?;
                } else {
                    fs::rename(&path, retired)?;
                }
                Ok(())
            })
            .await?;
        }
        Ok(None)
    }

    pub(super) fn staging_path(&self) -> PathBuf {
        // An interrupted transfer can only rename into its own staging path.
        self.directory.join(format!(".{}.download", Uuid::new_v4()))
    }

    pub(super) fn verified_path(&self, sha256: &str) -> PathBuf {
        self.directory
            .join(format!("{}.blob", sha256.to_ascii_lowercase()))
    }

    pub(super) async fn link(
        &self,
        source: PathBuf,
        publish_to: Option<PathBuf>,
        destination: PathBuf,
    ) -> Result<(), Error> {
        self.mutate(move || {
            let source = if let Some(verified) = publish_to {
                fs::rename(&source, &verified)?;
                verified
            } else {
                source
            };
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::hard_link(&source, destination)?;
            if let Err(error) = fs::OpenOptions::new()
                .write(true)
                .open(&source)
                .and_then(|file| file.set_modified(SystemTime::now()))
            {
                tracing::warn!(%error, path = %source.display(), "failed to update asset cache recency");
            }
            Ok(())
        })
        .await
    }

    async fn mutate(
        &self,
        operation: impl FnOnce() -> Result<(), Error> + Send + 'static,
    ) -> Result<(), Error> {
        // Keep the lock until the syscall finishes even if the caller's
        // staging future is canceled during agent shutdown.
        let lock = Arc::clone(&self.lock);
        spawn_blocking(move || {
            let _lock = lock;
            operation()
        })
        .await?
    }
}

fn evict_files(root: &Path, max_bytes: u64) -> Result<(), Error> {
    let mut files = list_cache_files(root)?;
    files.sort_unstable_by(|left, right| {
        (left.kind, left.modified, &left.path).cmp(&(right.kind, right.modified, &right.path))
    });
    let mut total: u128 = files.iter().map(|file| u128::from(file.size)).sum();

    for file in files {
        if file.kind == CacheFileKind::Verified && max_bytes != 0 && total <= u128::from(max_bytes)
        {
            break;
        }
        let lock = open_lock(&asset_lock_path(root, file.asset_id))?;
        if !try_lock(&lock)? {
            // A successful staging will run its own sweep and job cleanup
            // will run one again after releasing its links.
            continue;
        }
        let Some(metadata) = file_metadata(&file.path)? else {
            total -= u128::from(file.size);
            continue;
        };
        if file.kind == CacheFileKind::Verified
            && (metadata.modified()? != file.modified || metadata.len() != file.size)
        {
            // A job touched or replaced this blob after the snapshot.
            continue;
        }
        if is_unpinned(&file.path, &metadata) {
            fs::remove_file(&file.path)?;
            total -= u128::from(file.size);
            remove_empty_directory(&root.join(file.asset_id.to_string()))?;
        }
    }
    Ok(())
}

fn list_cache_files(root: &Path) -> Result<Vec<CacheFile>, Error> {
    let mut result = Vec::new();
    for asset in fs::read_dir(root)? {
        let asset = asset?;
        let name = asset.file_name();
        let Some(asset_id) = name.to_str().and_then(|name| {
            let id = Uuid::parse_str(name).ok()?;
            (id.to_string() == name).then_some(id)
        }) else {
            continue;
        };
        let directory = asset.path();
        if !directory_exists(&directory)? {
            continue;
        }
        let mut empty = true;
        for file in fs::read_dir(&directory)? {
            empty = false;
            let file = file?;
            let Some(kind) = file.file_name().to_str().and_then(cache_file_kind) else {
                continue;
            };
            let path = file.path();
            let Some(metadata) = file_metadata(&path)? else {
                continue;
            };
            result.push(CacheFile {
                asset_id,
                path,
                kind,
                modified: metadata.modified()?,
                size: if kind == CacheFileKind::Download {
                    0
                } else {
                    metadata.len()
                },
            });
        }
        if empty {
            let lock = open_lock(&asset_lock_path(root, asset_id))?;
            if try_lock(&lock)? {
                remove_empty_directory(&directory)?;
            }
        }
    }
    Ok(result)
}

fn cache_file_kind(name: &str) -> Option<CacheFileKind> {
    if verified_digest(name).is_some() {
        return Some(CacheFileKind::Verified);
    }
    if name
        .strip_prefix("retired-")
        .and_then(|name| name.strip_suffix(".blob"))
        .is_some_and(|id| Uuid::parse_str(id).is_ok())
    {
        return Some(CacheFileKind::Retired);
    }
    if name
        .strip_prefix('.')
        .and_then(|name| name.strip_suffix(".download"))
        .is_some_and(|id| Uuid::parse_str(id).is_ok())
    {
        return Some(CacheFileKind::Download);
    }
    None
}

fn verified_digest(name: &str) -> Option<&str> {
    name.strip_suffix(".blob")
        .filter(|digest| is_sha256(digest))
}

pub(super) fn is_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn digest_matches(path: &Path, size: u64, expected: &str) -> Result<bool, Error> {
    let mut file = tokio::fs::File::open(path)
        .await?
        .take(size.saturating_add(1));
    let mut buffer = vec![0; 64 * 1024];
    let mut digest = Sha256::new();
    let mut total = 0;
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total += read as u64;
        digest.update(&buffer[..read]);
    }
    Ok(total == size && check_digest(Some(digest), Some(expected)).is_ok())
}

#[cfg(unix)]
fn is_unpinned(_path: &Path, metadata: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    metadata.nlink() == 1
}

#[cfg(windows)]
fn is_unpinned(path: &Path, _metadata: &Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    File::open(path)
        .and_then(|file| file.metadata())
        .is_ok_and(|metadata| metadata.number_of_links() == Some(1))
}

#[cfg(not(any(unix, windows)))]
fn is_unpinned(_path: &Path, _metadata: &Metadata) -> bool {
    // Keep files whenever their hard-link count is unavailable.
    false
}

fn asset_lock_path(root: &Path, asset_id: Uuid) -> PathBuf {
    // Stable lock inodes coordinate separate managers sharing this data root.
    // Sharding caps the number of lockfiles independently of the cache size.
    root.join(format!(".asset-{:02x}.lock", asset_id.as_bytes()[0]))
}

fn open_lock(path: &Path) -> Result<File, Error> {
    file_metadata(path)?;
    Ok(fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?)
}

fn try_lock(file: &File) -> Result<bool, Error> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

async fn wait_for_lock(path: PathBuf) -> Result<File, Error> {
    let file = spawn_blocking(move || open_lock(&path)).await??;
    while !try_lock(&file)? {
        time::sleep(LOCK_RETRY_DELAY).await;
    }
    Ok(file)
}

fn ensure_directory(path: &Path) -> Result<(), Error> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && directory_exists(path)? => {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn ensure_asset_root(path: &Path) -> Result<(), Error> {
    fs::create_dir_all(path)?;
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err(invalid_cache_path(path));
    }
    Ok(())
}

fn directory_exists(path: &Path) -> Result<bool, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(true),
        Ok(_) => Err(invalid_cache_path(path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn file_metadata(path: &Path) -> Result<Option<Metadata>, Error> {
    checked_file(path, fs::symlink_metadata(path))
}

fn checked_file(path: &Path, metadata: io::Result<Metadata>) -> Result<Option<Metadata>, Error> {
    match metadata {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Err(invalid_cache_path(path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn remove_empty_directory(path: &Path) -> Result<(), Error> {
    match fs::remove_dir(path) {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn invalid_cache_path(path: &Path) -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid asset path: {}", path.display()),
    )
    .into()
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trims_abandoned_downloads_after_the_asset_lock_is_released() {
        let asset_dir = tempfile::tempdir().unwrap();
        let cache = AssetCache {
            asset_dir: asset_dir.path(),
            max_bytes: u64::MAX,
        };
        cache.initialize().await.unwrap();
        let entry = cache.lock_asset(Uuid::new_v4()).await.unwrap();
        let staging = entry.staging_path();
        tokio::fs::write(&staging, b"partial download")
            .await
            .unwrap();

        cache.trim().await.unwrap();
        assert!(staging.is_file());

        drop(entry);
        cache.trim().await.unwrap();
        assert!(!staging.exists());
    }
}
