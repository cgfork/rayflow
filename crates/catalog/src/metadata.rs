//! Data contracts shared by owned records and borrowed views.

use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

/// Filesystem identities may be owned values or borrowed keys.
pub trait Id: Clone + Eq + Hash + Debug + Display {}
impl<T: Clone + Eq + Hash + Debug + Display> Id for T {}

/// Object data; the filesystem assigns its identity separately.
pub trait Catalog {
    fn key(&self) -> &str;
    fn url(&self) -> &str;
    fn size(&self) -> u64;
    fn format(&self) -> &str;
    fn uploaded(&self) -> bool;
}

/// A logical file owned by a node run; its path is the referenced catalog key.
/// Its catalog reference uses the filesystem's ID type.
pub trait File {
    type CatalogId: Id;
    fn node_run_id(&self) -> uuid::Uuid;
    fn filename(&self) -> &str;
    fn catalog_id(&self) -> Self::CatalogId;
}

/// Sparse catalog data. `None` leaves an existing field unchanged.
/// Upserts require a key; new rows also require URL, size, and format.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CatalogOptionalData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uploaded: Option<bool>,
}

impl<C: Catalog + ?Sized> From<&C> for CatalogOptionalData {
    fn from(data: &C) -> Self {
        Self {
            key: Some(data.key().to_owned()),
            url: Some(data.url().to_owned()),
            size: Some(data.size()),
            format: Some(data.format().to_owned()),
            uploaded: Some(data.uploaded()),
        }
    }
}

impl From<&CatalogOptionalData> for CatalogOptionalData {
    fn from(data: &CatalogOptionalData) -> Self {
        data.clone()
    }
}

/// A logical file payload without its own identity or persistence fields.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileInput<I> {
    pub node_run_id: uuid::Uuid,
    pub filename: String,
    pub catalog_id: I,
}

impl<I> FileInput<I> {
    pub fn new(node_run_id: uuid::Uuid, filename: impl Into<String>, catalog_id: I) -> Self {
        Self {
            node_run_id,
            filename: filename.into(),
            catalog_id,
        }
    }
}

impl<I: Id> File for FileInput<I> {
    type CatalogId = I;
    fn node_run_id(&self) -> uuid::Uuid {
        self.node_run_id
    }
    fn filename(&self) -> &str {
        &self.filename
    }
    fn catalog_id(&self) -> I {
        self.catalog_id.clone()
    }
}

/// Sparse file data. `None` leaves an existing field unchanged.
/// Upserts require a catalog ID; new rows also require filename and node-run UUID.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileOptionalData<I> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_run_id: Option<uuid::Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_id: Option<I>,
}

impl<F: File + ?Sized> From<&F> for FileOptionalData<F::CatalogId> {
    fn from(data: &F) -> Self {
        Self {
            node_run_id: Some(data.node_run_id()),
            filename: Some(data.filename().to_owned()),
            catalog_id: Some(data.catalog_id()),
        }
    }
}

impl<I: Clone> From<&FileOptionalData<I>> for FileOptionalData<I> {
    fn from(data: &FileOptionalData<I>) -> Self {
        data.clone()
    }
}

/// A logical file and its catalog in the filesystem's data representations.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileWithCatalog<F, C> {
    pub file: F,
    pub catalog: C,
}

impl<T: Catalog + ?Sized> Catalog for &T {
    fn key(&self) -> &str {
        T::key(self)
    }

    fn url(&self) -> &str {
        T::url(self)
    }
    fn size(&self) -> u64 {
        T::size(self)
    }
    fn format(&self) -> &str {
        T::format(self)
    }
    fn uploaded(&self) -> bool {
        T::uploaded(self)
    }
}

impl<T: File + ?Sized> File for &T {
    type CatalogId = T::CatalogId;
    fn node_run_id(&self) -> uuid::Uuid {
        T::node_run_id(self)
    }
    fn filename(&self) -> &str {
        T::filename(self)
    }
    fn catalog_id(&self) -> Self::CatalogId {
        T::catalog_id(self)
    }
}
