//! Immutable asset versions and direct object-store uploads.

use std::{collections::BTreeSet, sync::LazyLock, time::Duration};

use catalog::{
    CatalogOptionalData, CatalogSystem, ObjectStorage, PresignOperation, PresignedUrl,
    directory::DIRECTORY_FORMAT,
    model::{Asset, AssetPromotion, Catalog},
    path::validate_relative_path,
};
use futures_util::StreamExt;
use graph::{Graph, GraphStore, types::DAG};
use jiff::Timestamp;
use runtime::command::NodeRun;
use scheduler::Job;
use sha2::{Digest, Sha256};
use toasty::{Executor, Transaction, schema::Model};
use uuid::Uuid;

use super::{App, AppError, Page, PageQuery};
use crate::{
    api::asset::{
        AssetDescriptor, AssetTransfer, ImportAssetRequest, PrepareAssetRequest,
        PrepareAssetResponse, PresignJobAssetRequest,
    },
    config::StorageConfig,
};

// Single-request S3 copy supports objects up to five gigabytes.
const MAX_ASSET_BYTES: u64 = 5_000_000_000;
const OBJECT_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
static OBJECTS: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(OBJECT_REQUEST_TIMEOUT)
        .build()
        .expect("asset storage client")
});

struct AssetVersion {
    name: String,
    version: u64,
    sha256: Vec<u8>,
    format: String,
}

impl AssetVersion {
    fn new(name: String, version: u64, sha256: String, format: String) -> Result<Self, AppError> {
        validate_version(&name, version)?;
        if format.is_empty()
            || format.len() > 255
            || !format.bytes().all(|byte| matches!(byte, b' '..=b'~'))
            || format.trim() != format
            || format.eq_ignore_ascii_case(DIRECTORY_FORMAT)
        {
            return Err(AppError::InvalidRequest("invalid asset format".into()));
        }
        let sha256 = decode_digest(&sha256).ok_or_else(|| {
            AppError::InvalidRequest("asset SHA-256 must contain 64 hexadecimal digits".into())
        })?;
        Ok(Self {
            name,
            version,
            sha256,
            format,
        })
    }

    fn verify_metadata(&self, asset: &Asset) -> Result<(), AppError> {
        if asset.sha256 != self.sha256 || asset.format != self.format {
            return Err(AppError::AssetConflict);
        }
        Ok(())
    }
}

fn validate_version(name: &str, version: u64) -> Result<(), AppError> {
    if name.is_empty()
        || name.len() > 255
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(AppError::InvalidRequest(
            "asset name must start with an ASCII letter or digit and contain at most 255 ASCII letters, digits, dots, underscores, or hyphens".into(),
        ));
    }
    if version == 0 {
        return Err(AppError::InvalidRequest(
            "asset version must be positive".into(),
        ));
    }
    Ok(())
}

fn decode_digest(digest: &str) -> Option<Vec<u8>> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    (0..64)
        .step_by(2)
        .map(|index| u8::from_str_radix(&digest[index..index + 2], 16).ok())
        .collect()
}

fn digest_string(digest: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for &byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 15)]));
    }
    encoded
}

fn validate_size(size: u64) -> Result<(), AppError> {
    if size > MAX_ASSET_BYTES {
        return Err(AppError::InvalidRequest(format!(
            "asset size must be at most {MAX_ASSET_BYTES} bytes"
        )));
    }
    Ok(())
}

fn asset_descriptor(asset: Asset) -> Result<AssetDescriptor, AppError> {
    let published_at = asset
        .published_at
        .ok_or(AppError::AssetNotFound(asset.uuid))?;
    Ok(AssetDescriptor {
        asset_id: asset.uuid,
        name: asset.name,
        version: asset.version,
        size: asset.size,
        format: asset.format,
        sha256: digest_string(&asset.sha256),
        created_by: asset.created_by,
        created_at: asset.created_at,
        published_at,
    })
}

async fn load_asset(db: &mut impl Executor, asset_id: Uuid) -> Result<Asset, AppError> {
    Asset::filter_by_uuid(asset_id)
        .first()
        .exec(db)
        .await?
        .ok_or(AppError::AssetNotFound(asset_id))
}

async fn lock_promotion(
    tx: &mut Transaction<'_>,
    uuid: Uuid,
) -> Result<Option<AssetPromotion>, AppError> {
    let table = tx
        .schema()
        .table_for(AssetPromotion::id())
        .name
        .replace('`', "``");
    // Lock the record across the publication or reclamation decision. Toasty
    // does not expose SELECT FOR UPDATE on model queries.
    toasty::sql::query(format!(
        "SELECT id FROM `{table}` WHERE uuid = ? FOR UPDATE"
    ))
    .bind(uuid)
    .column_types([toasty::stmt::Type::U64])
    .exec(&mut *tx)
    .await?;
    Ok(AssetPromotion::filter_by_uuid(uuid)
        .first()
        .exec(tx)
        .await?)
}

async fn published_catalog(db: &mut impl Executor, asset: &Asset) -> Result<Catalog, AppError> {
    let catalog_id = asset
        .catalog_uuid
        .filter(|_| asset.published_at.is_some())
        .ok_or(AppError::AssetNotFound(asset.uuid))?;
    db.get_catalog(catalog_id)
        .await?
        .filter(|catalog| catalog.uploaded)
        .ok_or(AppError::AssetNotFound(asset.uuid))
}

pub(super) async fn validate_graph_assets(
    db: &mut impl Executor,
    graph: &DAG,
) -> Result<(), AppError> {
    let assets = graph
        .nodes
        .iter()
        .flat_map(|node| node.assets.iter().map(|reference| reference.asset_id))
        .collect::<BTreeSet<_>>();
    for asset_id in assets {
        let asset = load_asset(db, asset_id).await?;
        published_catalog(db, &asset).await?;
    }
    Ok(())
}

impl<S> App<S> {
    pub(crate) async fn list_assets(
        &self,
        page: PageQuery,
        name: Option<String>,
    ) -> Result<Page<AssetDescriptor>, AppError> {
        let page = page.request()?;
        let mut db = self.db.clone();
        let mut query = Asset::filter(Asset::fields().published_at().is_some());
        if let Some(name) = name {
            query = query.filter(Asset::fields().name().eq(name));
        }
        if let Some(before) = page.before() {
            query = query.filter(Asset::fields().id().lt(before));
        }
        let rows = query
            .order_by(Asset::fields().id().desc())
            .limit(page.limit() + 1)
            .exec(&mut db)
            .await?;
        let page = Page::new(rows, page.limit(), |asset| asset.id);
        Ok(Page {
            items: page
                .items
                .into_iter()
                .map(asset_descriptor)
                .collect::<Result<_, _>>()?,
            next_cursor: page.next_cursor,
        })
    }

    pub(crate) async fn read_asset(&self, asset_id: Uuid) -> Result<AssetDescriptor, AppError> {
        asset_descriptor(load_asset(&mut self.db.clone(), asset_id).await?)
    }

    pub(crate) async fn prepare_asset(
        &self,
        request: PrepareAssetRequest,
        actor: &str,
    ) -> Result<PrepareAssetResponse, AppError> {
        validate_size(request.size)?;
        let version = AssetVersion::new(
            request.name,
            request.version,
            request.sha256,
            request.format,
        )?;
        let config = self.asset_storage()?;
        let asset_id = Uuid::new_v4();
        let upload_id = Uuid::new_v4();
        let staging = asset_key(config, &format!("asset-staging/{asset_id}/{upload_id}"));
        let source_url = config.object_url(&staging)?;
        let asset = self
            .reserve_asset(
                &version,
                asset_id,
                Some(upload_id),
                source_url,
                request.size,
                actor,
            )
            .await?;
        if asset.published_at.is_some() {
            return Ok(PrepareAssetResponse::Published {
                asset: asset_descriptor(asset)?,
            });
        }
        verify_upload_owner(&asset, actor)?;
        let upload_id = asset.upload_id.ok_or(AppError::AssetConflict)?;
        let object = Catalog::new(asset.uuid, "", &asset.source_url, asset.size, &asset.format);
        let mut upload = self.sign_asset(
            config,
            &object,
            PresignOperation::UploadSized { size: asset.size },
            config.presign_ttl,
        )?;
        upload.operation = PresignOperation::Upload;
        // Fetch derives the signed Content-Length from the selected File.
        upload.headers.remove("content-length");
        Ok(PrepareAssetResponse::Pending {
            asset_id: asset.uuid,
            upload_id,
            upload,
        })
    }

    pub(crate) async fn commit_asset(
        &self,
        asset_id: Uuid,
        upload_id: Uuid,
        actor: &str,
    ) -> Result<AssetDescriptor, AppError> {
        let asset = load_asset(&mut self.db.clone(), asset_id).await?;
        verify_upload_owner(&asset, actor)?;
        if asset.upload_id != Some(upload_id) {
            return Err(AppError::AssetConflict);
        }
        if asset.published_at.is_some() {
            return asset_descriptor(asset);
        }
        self.finalize_asset(asset).await
    }

    pub(crate) async fn cancel_asset(
        &self,
        name: &str,
        version: u64,
        actor: &str,
    ) -> Result<(), AppError> {
        validate_version(name, version)?;
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let table = tx.schema().table_for(Asset::id()).name.replace('`', "``");
        // Serialize cancellation with both reservation and publication.
        toasty::sql::query(format!(
            "SELECT id FROM `{table}` WHERE name = ? AND version = ? FOR UPDATE"
        ))
        .bind(name)
        .bind(version)
        .column_types([toasty::stmt::Type::U64])
        .exec(&mut tx)
        .await?;
        let asset = Asset::filter_by_name_and_version(name, version)
            .first()
            .exec(&mut tx)
            .await?;
        if let Some(asset) = &asset {
            if asset.published_at.is_some() {
                return Err(AppError::AssetConflict);
            }
            verify_upload_owner(asset, actor)?;
            Asset::filter_by_id(asset.id).delete().exec(&mut tx).await?;
        }
        tx.commit().await?;

        if let Some((asset, config)) = asset.zip(self.config.storage.as_ref())
            && asset.upload_id.is_some()
        {
            self.remove_staged_upload(config, &asset).await;
        }
        Ok(())
    }

    pub(crate) async fn import_asset(
        &self,
        name: String,
        version: u64,
        request: ImportAssetRequest,
        actor: &str,
    ) -> Result<AssetDescriptor, AppError> {
        let version = AssetVersion::new(name, version, request.sha256, request.format)?;
        validate_relative_path(&request.source_key)?;
        let config = self.asset_storage()?;
        let import = config
            .asset_import_prefix
            .as_deref()
            .ok_or_else(|| AppError::InvalidRequest("asset import is not configured".into()))?;
        let key = format!("/{}/{}", import.trim_matches('/'), request.source_key);
        let source_url = config.object_url(&key)?;
        if source_url.len() > 750 {
            return Err(AppError::InvalidRequest(
                "asset source URL is too long".into(),
            ));
        }
        let asset = if let Some(asset) =
            Asset::filter_by_name_and_version(&version.name, version.version)
                .first()
                .exec(&mut self.db.clone())
                .await?
        {
            version.verify_metadata(&asset)?;
            asset
        } else {
            let mut source = Catalog::new(Uuid::nil(), &key, &source_url, 0, &version.format);
            let size = self.asset_object_length(config, &source).await?;
            validate_size(size)?;
            // Import requests identify expected bytes, but do not declare their size.
            // Verify the source before reserving a length for subsequent retries.
            source.size = size;
            self.verify_asset_object(config, &source, &version.sha256)
                .await?;
            self.reserve_asset(
                &version,
                Uuid::new_v4(),
                None,
                source_url.clone(),
                size,
                actor,
            )
            .await?
        };
        if asset.published_at.is_some() {
            return asset_descriptor(asset);
        }
        verify_upload_owner(&asset, actor)?;
        if asset.upload_id.is_some() || asset.source_url != source_url {
            return Err(AppError::AssetConflict);
        }
        self.finalize_asset(asset).await
    }

    pub(crate) async fn download_asset(&self, asset_id: Uuid) -> Result<AssetTransfer, AppError> {
        let mut db = self.db.clone();
        let asset = load_asset(&mut db, asset_id).await?;
        let catalog = published_catalog(&mut db, &asset).await?;
        let config = self.asset_storage()?;
        let operation = PresignOperation::DownloadAttachment {
            filename: asset.name.clone(),
        };
        self.asset_transfer(config, asset, catalog, operation, config.presign_ttl)
    }

    pub(crate) async fn presign_job_asset(
        &self,
        request: PresignJobAssetRequest,
    ) -> Result<AssetTransfer, AppError>
    where
        S: GraphStore<Uuid, G = DAG> + Clone,
        S::load(..): Send,
        S::Err: Send + Sync + 'static,
    {
        let PresignJobAssetRequest {
            job_id,
            agent_id,
            filename,
        } = request;
        validate_relative_path(&filename)?;
        let mut db = self.db.clone();
        let job = Job::filter_by_uuid(job_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::JobNotFound(job_id))?;
        Self::validate_job_claim(&job, agent_id)?;
        let node_run = NodeRun::filter_by_uuid(job.node_run_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::NodeRunNotFound(job.node_run_id))?;
        let graph = self
            .read_graph(node_run.graph_id, Some(node_run.version))
            .await?;
        let node = graph
            .node(node_run.node_id)
            .ok_or(AppError::NodeNotFound(node_run.node_id))?;
        let reference = node
            .data
            .assets
            .iter()
            .find(|asset| asset.file_name == filename)
            .ok_or(AppError::ArtifactNotFound(filename))?;
        let asset = load_asset(&mut db, reference.asset_id).await?;
        let catalog = published_catalog(&mut db, &asset).await?;
        let config = self.asset_storage()?;
        let current = Job::filter_by_uuid(job_id)
            .first()
            .exec(&mut db)
            .await?
            .ok_or(AppError::JobNotFound(job_id))?;
        Self::validate_job_claim(&current, agent_id)?;
        let ttl = current
            .expired_at
            .and_then(|expiry| active_grant_ttl(Timestamp::now(), expiry))
            .ok_or(AppError::JobConflict(job_id))?;
        self.asset_transfer(
            config,
            asset,
            catalog,
            PresignOperation::Download,
            ttl.min(config.presign_ttl),
        )
    }

    async fn reserve_asset(
        &self,
        version: &AssetVersion,
        asset_id: Uuid,
        upload_id: Option<Uuid>,
        source_url: String,
        size: u64,
        actor: &str,
    ) -> Result<Asset, AppError> {
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let table = tx.schema().table_for(Asset::id()).name.replace('`', "``");
        // Toasty does not expose insert-on-conflict with a no-op. This locks the
        // canonical name/version across simultaneous reservations.
        toasty::sql::statement(format!(
            "INSERT INTO `{table}` (uuid, name, version, sha256, size, format, upload_id, source_url, created_by) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE id = id",
        ))
        .bind(asset_id)
        .bind(version.name.as_str())
        .bind(version.version)
        .bind(version.sha256.clone())
        .bind(size)
        .bind(version.format.as_str())
        .bind_typed(
            upload_id.map(|id| id.to_string()),
            toasty::schema::db::Type::VarChar(36),
        )
        .bind(source_url)
        .bind(actor)
        .exec(&mut tx)
        .await?;
        let asset = Asset::filter_by_name_and_version(&version.name, version.version)
            .get(&mut tx)
            .await?;
        version.verify_metadata(&asset)?;
        if asset.size != size {
            return Err(AppError::AssetConflict);
        }
        tx.commit().await?;
        Ok(asset)
    }

    async fn finalize_asset(&self, asset: Asset) -> Result<AssetDescriptor, AppError> {
        let config = self.asset_storage()?;
        if asset.upload_id.is_some() {
            let staging =
                Catalog::new(asset.uuid, "", &asset.source_url, asset.size, &asset.format);
            let error = match self.asset_object_length(config, &staging).await {
                Ok(size) if size == asset.size => None,
                Ok(_) => Some(AppError::AssetIntegrity),
                Err(error) => Some(error),
            };
            if let Some(error) = error {
                return self.published_or_error(asset.uuid, error).await;
            }
        }

        // Commit attempts own separate object keys. A racing copy can never
        // overwrite a candidate that another request has checked or published.
        let candidate_id = Uuid::new_v4();
        let key = asset_key(config, &format!("assets/{}/{candidate_id}", asset.uuid));
        let url = config.object_url(&key)?;
        let candidate = Catalog::new(candidate_id, &key, &url, asset.size, &asset.format);
        let operation = PresignOperation::Copy {
            source: asset.source_url.clone(),
        };
        let signed = self.sign_asset(
            config,
            &candidate,
            operation,
            config.presign_ttl.min(OBJECT_REQUEST_TIMEOUT),
        )?;
        // Account for a copy started at the end of the signature lifetime and
        // a subsequent verification GET before allowing cleanup to take over.
        let expires_at = signed
            .expires_at
            .checked_add(OBJECT_REQUEST_TIMEOUT * 2)
            .map_err(|error| AppError::Storage(Box::new(error)))?;
        AssetPromotion::create()
            .uuid(candidate_id)
            .url(&url)
            .expires_at(Some(expires_at))
            .exec(&mut self.db.clone())
            .await?;
        let copy = async {
            let response = asset_storage_request(signed).await?;
            // CopyObject can send HTTP 200 before completing the copy and
            // report an error in the response body.
            let body = response.text().await.map_err(storage_transport_error)?;
            if !body.trim_end().ends_with("</CopyObjectResult>") {
                return Err(AppError::StorageUnavailable);
            }
            Ok(())
        };
        if let Err(error) = copy.await {
            // The provider could have started the copy before the response was
            // lost. Leave its record for the delayed sweep.
            return self.published_or_error(asset.uuid, error).await;
        }
        if let Err(error) = self
            .verify_asset_object(config, &candidate, &asset.sha256)
            .await
        {
            self.discard_promotion(config, &candidate).await;
            return self.published_or_error(asset.uuid, error).await;
        }

        // The promotion record stays durable after every SQL error. If COMMIT
        // succeeds but its response is lost, the record was removed in the
        // same transaction that made the copy canonical.
        let mut db = self.db.clone();
        let mut tx = db.transaction().await?;
        let table = tx.schema().table_for(Asset::id()).name.replace('`', "``");
        // Toasty cannot currently lock a selected record. Serialize the final
        // catalog link before any snapshot reads in this transaction.
        toasty::sql::query(format!(
            "SELECT id FROM `{table}` WHERE uuid = ? FOR UPDATE"
        ))
        .bind(asset.uuid)
        .column_types([toasty::stmt::Type::U64])
        .exec(&mut tx)
        .await?;
        let promotion = lock_promotion(&mut tx, candidate_id).await?;
        let current = Asset::filter_by_uuid(asset.uuid)
            .first()
            .exec(&mut tx)
            .await?;
        match current {
            Some(current) if current.published_at.is_some() => {
                tx.commit().await?;
                self.discard_promotion(config, &candidate).await;
                return asset_descriptor(current);
            }
            None => {
                tx.commit().await?;
                self.discard_promotion(config, &candidate).await;
                return Err(AppError::AssetNotFound(asset.uuid));
            }
            Some(_) => {}
        }
        if promotion
            .and_then(|row| row.expires_at)
            .is_none_or(|expires_at| expires_at <= Timestamp::now())
        {
            return Err(AppError::AssetUploadIncomplete);
        }
        let catalog_id = tx
            .upsert_catalog(CatalogOptionalData {
                key: Some(key),
                url: Some(url),
                size: Some(asset.size),
                format: Some(asset.format.clone()),
                uploaded: Some(true),
            })
            .await?;
        Asset::filter_by_uuid(asset.uuid)
            .update()
            .catalog_uuid(Some(catalog_id))
            .published_at(Some(Timestamp::now()))
            .exec(&mut tx)
            .await?;
        let published = load_asset(&mut tx, asset.uuid).await?;
        AssetPromotion::filter_by_uuid(candidate_id)
            .delete()
            .exec(&mut tx)
            .await?;
        tx.commit().await?;
        if asset.upload_id.is_some() {
            self.remove_staged_upload(config, &asset).await;
        }
        asset_descriptor(published)
    }

    async fn published_or_error(
        &self,
        uuid: Uuid,
        error: AppError,
    ) -> Result<AssetDescriptor, AppError> {
        let current = load_asset(&mut self.db.clone(), uuid).await?;
        if current.published_at.is_some() {
            return asset_descriptor(current);
        }
        Err(error)
    }

    async fn asset_object_length(
        &self,
        config: &StorageConfig,
        object: &Catalog,
    ) -> Result<u64, AppError> {
        let signed = self.sign_asset(config, object, PresignOperation::Head, config.presign_ttl)?;
        let response = asset_storage_request(signed).await?;
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .ok_or(AppError::AssetIntegrity)
    }

    async fn verify_asset_object(
        &self,
        config: &StorageConfig,
        candidate: &Catalog,
        digest: &[u8],
    ) -> Result<(), AppError> {
        let signed = self.sign_asset(
            config,
            candidate,
            PresignOperation::Download,
            config.presign_ttl,
        )?;
        let response = asset_storage_request(signed).await?;
        if response
            .content_length()
            .is_some_and(|length| length != candidate.size)
        {
            return Err(AppError::AssetIntegrity);
        }
        let mut stream = response.bytes_stream();
        let mut hash = Sha256::new();
        let mut size = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(storage_transport_error)?;
            size += chunk.len() as u64;
            if size > candidate.size {
                return Err(AppError::AssetIntegrity);
            }
            hash.update(&chunk);
        }
        if size != candidate.size || &hash.finalize()[..] != digest {
            return Err(AppError::AssetIntegrity);
        }
        Ok(())
    }

    async fn delete_asset_object(
        &self,
        config: &StorageConfig,
        object: &Catalog,
    ) -> Result<(), AppError> {
        let signed =
            self.sign_asset(config, object, PresignOperation::Delete, config.presign_ttl)?;
        match asset_storage_request(signed).await {
            Ok(_) | Err(AppError::AssetUploadIncomplete) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn remove_staged_upload(&self, config: &StorageConfig, asset: &Asset) {
        let staging = Catalog::new(asset.uuid, "", &asset.source_url, asset.size, &asset.format);
        if let Err(error) = self.delete_asset_object(config, &staging).await {
            tracing::warn!(%error, "could not remove staged asset upload");
        }
    }

    async fn discard_promotion(&self, config: &StorageConfig, candidate: &Catalog) {
        let result = async {
            self.delete_asset_object(config, candidate).await?;
            AssetPromotion::filter_by_uuid(candidate.uuid)
                .delete()
                .exec(&mut self.db.clone())
                .await?;
            Ok::<(), AppError>(())
        };
        if let Err(error) = result.await {
            tracing::warn!(%error, "could not remove unpublished asset copy");
        }
    }

    /// Reconciles expired private copies against ongoing publication transactions.
    pub async fn reap_asset_promotions(&self) -> Result<(), AppError> {
        let Some(config) = &self.config.storage else {
            return Ok(());
        };
        let mut db = self.db.clone();
        let cutoff = Timestamp::now();
        let mut after = 0;
        loop {
            let fields = AssetPromotion::fields();
            let page = AssetPromotion::filter(
                fields
                    .expires_at()
                    .le(Some(cutoff))
                    .or(fields.expires_at().is_none()),
            )
            .filter(fields.id().gt(after))
            .order_by(fields.id().asc())
            .limit(64)
            .exec(&mut db)
            .await?;
            if page.is_empty() {
                return Ok(());
            }
            for candidate in page {
                after = candidate.id;
                let mut tx = db.transaction().await?;
                let Some(row) = lock_promotion(&mut tx, candidate.uuid)
                    .await?
                    .filter(|row| row.expires_at.is_none_or(|expires_at| expires_at <= cutoff))
                else {
                    tx.commit().await?;
                    continue;
                };
                // Publication removes the record atomically. Clearing the
                // expiration under lock fences surviving records on every server.
                if row.expires_at.is_some() {
                    AssetPromotion::filter_by_uuid(row.uuid)
                        .update()
                        .expires_at(None)
                        .exec(&mut tx)
                        .await?;
                }
                tx.commit().await?;
                let object = Catalog::new(row.uuid, "", row.url, 0, "application/octet-stream");
                self.discard_promotion(config, &object).await;
            }
        }
    }

    fn asset_storage(&self) -> Result<&StorageConfig, AppError> {
        self.config
            .storage
            .as_ref()
            .ok_or(AppError::StorageUnavailable)
    }

    fn sign_asset(
        &self,
        config: &StorageConfig,
        object: &Catalog,
        operation: PresignOperation,
        ttl: Duration,
    ) -> Result<PresignedUrl, AppError> {
        config
            .object_storage(self.db.clone())?
            .presign_catalog(object, operation, ttl)
            .map_err(|error| AppError::Storage(Box::new(error)))
    }

    fn asset_transfer(
        &self,
        config: &StorageConfig,
        asset: Asset,
        catalog: Catalog,
        operation: PresignOperation,
        ttl: Duration,
    ) -> Result<AssetTransfer, AppError> {
        let storage = config.object_storage(self.db.clone())?;
        let mut presigned = storage
            .presign_catalog(&catalog, operation, ttl)
            .map_err(|error| AppError::Storage(Box::new(error)))?;
        presigned.operation = PresignOperation::Download;
        Ok(AssetTransfer {
            asset_id: asset.uuid,
            sha256: digest_string(&asset.sha256),
            catalog,
            presigned,
            multipart: config.multipart_enabled && storage.supports_multipart(),
        })
    }
}

fn verify_upload_owner(asset: &Asset, actor: &str) -> Result<(), AppError> {
    if asset.created_by != actor {
        return Err(AppError::AssetUploadForbidden);
    }
    Ok(())
}

fn active_grant_ttl(now: Timestamp, expiry: Timestamp) -> Option<Duration> {
    let remaining = expiry.duration_since(now);
    if !remaining.is_positive() {
        return None;
    }
    // S3 only accepts whole seconds. An active claim in its final fraction
    // of a second still gets one signing second.
    let seconds = remaining.as_secs() as u64 + u64::from(remaining.subsec_nanos() != 0);
    Some(Duration::from_secs(seconds))
}

fn asset_key(config: &StorageConfig, relative: &str) -> String {
    format!("/{}/{}", config.prefix.trim_matches('/'), relative)
}

async fn asset_storage_request(signature: PresignedUrl) -> Result<reqwest::Response, AppError> {
    let method = reqwest::Method::from_bytes(signature.operation.method().as_bytes())
        .expect("known HTTP method");
    let mut request = OBJECTS.request(method, signature.url);
    for (name, value) in signature.headers {
        request = request.header(name, value);
    }
    if matches!(signature.operation, PresignOperation::Copy { .. }) {
        request = request.body("");
    }
    let response = request.send().await.map_err(storage_transport_error)?;
    match response.status() {
        status if status.is_success() => Ok(response),
        reqwest::StatusCode::NOT_FOUND => Err(AppError::AssetUploadIncomplete),
        status => {
            tracing::warn!(%status, "asset storage request failed");
            Err(AppError::StorageUnavailable)
        }
    }
}

fn storage_transport_error(error: reqwest::Error) -> AppError {
    tracing::warn!(error = %error.without_url(), "asset storage request failed");
    AppError::StorageUnavailable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_claims_get_a_grant_even_with_a_fraction_of_a_second_remaining() {
        let now: Timestamp = "2026-11-27T10:00:00.100Z".parse().unwrap();
        let expiry = now.checked_add(Duration::from_millis(250)).unwrap();
        assert_eq!(active_grant_ttl(now, expiry), Some(Duration::from_secs(1)));
        assert_eq!(active_grant_ttl(expiry, expiry), None);
        assert_eq!(active_grant_ttl(expiry, now), None);
    }
}
