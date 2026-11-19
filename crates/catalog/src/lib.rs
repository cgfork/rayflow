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

pub use metadata::{
    Catalog, CatalogOptionalData, File, FileInput, FileOptionalData, FileWithCatalog, Id,
};
pub use model::models;
pub use store::Error;

/// Object metadata with identities assigned by a store.
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
    /// Inserts a catalog or updates the existing record identified by its key.
    async fn upsert_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        data: C,
    ) -> Result<Self::CatalogId<'a>, Self::Err>
    where
        Self: 'a;
    /// Applies partial metadata changes to an existing catalog identity.
    async fn update_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        id: Self::CatalogId<'a>,
        data: C,
    ) -> Result<(), Self::Err>
    where
        Self: 'a;
    /// Loads a catalog by its store-specific identity.
    async fn get_catalog<'a>(
        &'a mut self,
        id: Self::CatalogId<'a>,
    ) -> Result<Option<Self::CatalogData<'a>>, Self::Err>
    where
        Self: 'a;
    /// Loads a catalog by its globally unique logical key.
    async fn get_catalog_by_key<'a>(
        &'a mut self,
        key: &str,
    ) -> Result<Option<Self::CatalogData<'a>>, Self::Err>
    where
        Self: 'a;
}

/// Logical files referencing catalogs managed by [`CatalogSystem`].
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
    /// Inserts or updates a file identified by its node-run and filename pair.
    async fn upsert_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        data: F,
    ) -> Result<Self::FileId<'a>, Self::Err>
    where
        Self: 'a;
    /// Applies partial metadata changes to an existing file identity.
    async fn update_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        id: Self::FileId<'a>,
        data: F,
    ) -> Result<(), Self::Err>
    where
        Self: 'a;
    /// Loads a file by identity, including its optional catalog edge.
    async fn get_file<'a>(
        &'a mut self,
        id: Self::FileId<'a>,
    ) -> Result<Option<FileWithCatalog<Self::FileData<'a>, Option<Self::CatalogData<'a>>>>, Self::Err>
    where
        Self: 'a;
    /// Loads a file by its unique `(node_run_id, filename)` key.
    async fn get_file_by_node_run_filename<'a>(
        &'a mut self,
        node_run_id: uuid::Uuid,
        filename: &str,
    ) -> Result<Option<FileWithCatalog<Self::FileData<'a>, Option<Self::CatalogData<'a>>>>, Self::Err>
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
