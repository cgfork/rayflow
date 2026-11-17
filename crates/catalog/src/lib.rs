//! Object metadata and logical file names, independent of the storage provider.
//!
//! Register [`models`] with Toasty and apply Rayflow's migrations before using
//! the MySQL [`CatalogSystem`] and [`FileSystem`] implementations on a database
//! or transaction.

pub mod directory;
pub mod metadata;
pub mod model;
pub mod path;
#[cfg(feature = "s3")]
pub mod s3;
pub mod store;

use std::{collections::BTreeMap, time::Duration};

use jiff::Timestamp;
use uuid::Uuid;

pub use metadata::{
    Catalog, CatalogInput, CatalogOptionalData, File, FileInput, FileOptionalData, FileWithCatalog,
    Id,
};
pub use model::models;
pub use store::Error;

/// Lookup a logical file by its identity or full path.
#[derive(Clone, Copy, Debug)]
pub enum FileKey<'a, I = Uuid> {
    Id(I),
    Fullpath(&'a str),
}

/// Object metadata with identities assigned by a store.
/// Upserts match catalog keys, returning the canonical ID and preserving creation timestamps.
/// Catalog writes persist the supplied upload acknowledgement without uploading bytes.
#[allow(async_fn_in_trait)]
pub trait CatalogSystem {
    type Err: std::error::Error;
    type CatalogId<'a>: Id + 'a
    where
        Self: 'a;
    type CatalogData<'a>: Catalog + 'a
    where
        Self: 'a;
    type CatalogOptionalData<'a>: 'a
    where
        Self: 'a;

    /// Apply supplied fields by key. New catalogs require URL, size, and format;
    /// omitted uploaded defaults to false only when inserting.
    async fn upsert_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        data: C,
    ) -> Result<Self::CatalogId<'a>, Self::Err>
    where
        Self: 'a;

    /// Update supplied fields by identity; a missing identity is an error.
    async fn update_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        id: Self::CatalogId<'a>,
        data: C,
    ) -> Result<(), Self::Err>
    where
        Self: 'a;

    async fn get_catalog<'a>(
        &'a mut self,
        id: Self::CatalogId<'a>,
    ) -> Result<Option<Self::CatalogData<'a>>, Self::Err>
    where
        Self: 'a;
}

/// Logical files referencing catalogs managed by [`CatalogSystem`].
/// Upserts match full paths, returning the canonical ID and preserving creation timestamps.
#[allow(async_fn_in_trait)]
pub trait FileSystem: CatalogSystem {
    type FileId<'a>: Id + 'a
    where
        Self: 'a;
    type FileData<'a>: File<CatalogId = Self::CatalogId<'a>> + 'a
    where
        Self: 'a;
    type FileOptionalData<'a>: 'a
    where
        Self: 'a;

    /// Apply supplied fields by full path. New files require filename and catalog ID.
    async fn upsert_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        data: F,
    ) -> Result<Self::FileId<'a>, Self::Err>
    where
        Self: 'a;

    /// Update supplied fields by identity; a missing identity is an error.
    async fn update_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        id: Self::FileId<'a>,
        data: F,
    ) -> Result<(), Self::Err>
    where
        Self: 'a;

    /// Returns None if either the file or its catalog is missing.
    async fn get_file<'a>(
        &'a mut self,
        key: FileKey<'_, Self::FileId<'a>>,
    ) -> Result<Option<FileWithCatalog<Self::FileData<'a>, Self::CatalogData<'a>>>, Self::Err>
    where
        Self: 'a;
}

/// HTTP operation authorized by an object storage signature.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresignOperation {
    Upload,
    Download,
    CreateMultipart,
    UploadPart { upload_id: String, part_number: u32 },
    CompleteMultipart { upload_id: String },
    AbortMultipart { upload_id: String },
}

impl PresignOperation {
    pub const fn method(&self) -> &'static str {
        match self {
            Self::Upload | Self::UploadPart { .. } => "PUT",
            Self::Download => "GET",
            Self::CreateMultipart | Self::CompleteMultipart { .. } => "POST",
            Self::AbortMultipart { .. } => "DELETE",
        }
    }

    pub const fn is_multipart(&self) -> bool {
        !matches!(self, Self::Upload | Self::Download)
    }

    pub fn is_valid(&self) -> bool {
        let id = match self {
            Self::UploadPart {
                upload_id,
                part_number,
            } => {
                if !(1..=10_000).contains(part_number) {
                    return false;
                }
                upload_id
            }
            Self::CompleteMultipart { upload_id } | Self::AbortMultipart { upload_id } => upload_id,
            _ => return true,
        };
        !id.is_empty() && id.len() <= 2048 && !id.chars().any(char::is_control)
    }
}

/// A temporary URL and any headers required by the provider's signature.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PresignedUrl {
    pub url: String,
    pub operation: PresignOperation,
    pub headers: BTreeMap<String, String>,
    pub expires_at: Timestamp,
}

/// Provider adapters add signing to a metadata filesystem.
///
/// Implementations resolve a catalog's URL, select the corresponding
/// provider, and sign using that provider's configured endpoint and credentials.
/// Reject zero or unsupported lifetimes, missing catalogs, and schemes
/// that cannot be signed (such as `file://`). Never return the stored object URL
/// as if it were a signed HTTP URL. The returned expiry is the actual expiry,
/// including any limit imposed by temporary credentials.
#[allow(async_fn_in_trait)]
pub trait ObjectStorage: FileSystem {
    /// Sign metadata that the caller has already loaded, including records in
    /// an uncommitted transaction. This operation performs no persistence I/O.
    fn presign_catalog(
        &self,
        catalog: &(impl Catalog + ?Sized),
        operation: PresignOperation,
        expires_in: Duration,
    ) -> Result<PresignedUrl, Self::Err>;

    fn supports_multipart(&self) -> bool {
        false
    }

    async fn presign<'a>(
        &'a mut self,
        catalog_id: Self::CatalogId<'a>,
        operation: PresignOperation,
        expires_in: Duration,
    ) -> Result<PresignedUrl, Self::Err>
    where
        Self: 'a;
}
