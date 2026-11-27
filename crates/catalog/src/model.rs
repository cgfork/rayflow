//! Owned Toasty records implementing the shared metadata traits.

use jiff::Timestamp;
use uuid::Uuid;

/// A physical object, addressed by a provider URL such as `s3://bucket/key`,
/// `oss://bucket/key`, or `file://host/path`. Size is measured in bytes.
/// Object keys and URLs are unique.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "catalog"]
#[unique(uuid)]
#[unique(key)]
#[unique(url)]
pub struct Catalog {
    #[key]
    #[auto]
    pub id: u64,
    pub uuid: Uuid,
    #[column(type = varchar(750))]
    pub key: String,
    #[column(type = varchar(750))]
    pub url: String,
    pub size: u64,
    #[column(type = varchar(255))]
    pub format: String,
    /// True after the object upload has been acknowledged.
    #[default(false)]
    #[serde(default)]
    pub uploaded: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Catalog {
    /// Constructs unacknowledged metadata. Persistence assigns ID and audit times.
    pub fn new(
        uuid: Uuid,
        key: impl Into<String>,
        url: impl Into<String>,
        size: u64,
        format: impl Into<String>,
    ) -> Self {
        Self {
            id: 0,
            uuid,
            key: key.into(),
            url: url.into(),
            size,
            format: format.into(),
            uploaded: false,
            created_at: Timestamp::UNIX_EPOCH,
            updated_at: Timestamp::UNIX_EPOCH,
        }
    }
}

impl crate::Catalog for Catalog {
    fn key(&self) -> &str {
        &self.key
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn format(&self) -> &str {
        &self.format
    }

    fn uploaded(&self) -> bool {
        self.uploaded
    }
}

/// A node-run-owned file pointing to a catalog object. Its path lives in the catalog.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, toasty::Model)]
#[table = "file"]
#[unique(uuid)]
#[unique(node_run_id, filename)]
pub struct File {
    #[key]
    #[auto]
    pub id: u64,
    pub uuid: Uuid,
    #[index]
    pub node_run_id: Uuid,
    #[column(type = varchar(255))]
    pub filename: String,
    pub catalog_uuid: Uuid,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[belongs_to(key = catalog_uuid, references = uuid)]
    #[serde(default, skip_serializing_if = "toasty::Deferred::is_unloaded")]
    pub catalog: toasty::Deferred<Catalog>,
}

impl File {
    /// Constructs a logical file. Names are preserved as supplied.
    pub fn new(
        uuid: Uuid,
        node_run_id: Uuid,
        filename: impl Into<String>,
        catalog_uuid: Uuid,
    ) -> Self {
        Self {
            id: 0,
            uuid,
            node_run_id,
            filename: filename.into(),
            catalog_uuid,
            created_at: Timestamp::UNIX_EPOCH,
            updated_at: Timestamp::UNIX_EPOCH,
            catalog: toasty::Deferred::default(),
        }
    }
}

impl crate::File for File {
    type CatalogId = Uuid;

    fn node_run_id(&self) -> Uuid {
        self.node_run_id
    }

    fn filename(&self) -> &str {
        &self.filename
    }

    fn catalog_id(&self) -> Uuid {
        self.catalog_uuid
    }
}

/// A reserved or published version of a reusable asset.
#[derive(Clone, Debug, toasty::Model)]
#[table = "asset"]
#[unique(uuid)]
#[unique(name, version)]
pub struct Asset {
    #[key]
    #[auto]
    pub id: u64,
    pub uuid: Uuid,
    #[column(type = varchar(255))]
    pub name: String,
    pub version: u64,
    #[unique]
    pub catalog_uuid: Option<Uuid>,
    // Migration 27 stores exactly 32 bytes. Toasty uses its bytes mapping
    // here because its binary length override is incompatible with this derive.
    pub sha256: Vec<u8>,
    pub size: u64,
    #[column(type = varchar(255))]
    pub format: String,
    pub upload_id: Option<Uuid>,
    #[column(type = varchar(750))]
    pub source_url: String,
    #[column(type = varchar(64))]
    pub created_by: String,
    pub created_at: Timestamp,
    pub published_at: Option<Timestamp>,
}

pub fn models() -> toasty::ModelSet {
    toasty::models!(Catalog, File, Asset)
}
