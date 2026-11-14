//! MySQL persistence on Toasty databases and transactions. Nested savepoints
//! leave ownership of an outer commit or rollback with the caller.

use crate::{
    CatalogOptionalData, CatalogSystem, FileKey, FileOptionalData, FileSystem, FileWithCatalog,
    model::{Catalog, File},
};
use jiff::Timestamp;
use toasty::Executor;
use url::Url;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] toasty::Error),
    #[error(
        "invalid catalog URL: expected an absolute object URL with a path and no credentials or fragment"
    )]
    InvalidUrl,
    #[error("{0} must be nonempty, contain no NUL, and fit its database column")]
    InvalidField(&'static str),
    #[error("field is required: {0}")]
    MissingField(&'static str),
    #[error("catalog was not found: {0}")]
    CatalogNotFound(Uuid),
    #[error("file was not found: {0}")]
    FileNotFound(Uuid),
}

impl<B: Executor> CatalogSystem for B {
    type Err = Error;
    type CatalogId<'a>
        = Uuid
    where
        Self: 'a;
    type CatalogData<'a>
        = Catalog
    where
        Self: 'a;

    type CatalogOptionalData<'a>
        = CatalogOptionalData
    where
        Self: 'a;

    async fn upsert_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        data: C,
    ) -> Result<Uuid, Error>
    where
        Self: 'a,
    {
        let mut data = data.into();
        let key = data.key.as_deref().ok_or(Error::MissingField("key"))?;
        validate_field("key", key, 750)?;
        if let Some(url) = &data.url {
            validate_url(url)?;
            validate_field("url", url, 750)?;
        }
        if let Some(format) = &data.format {
            validate_field("format", format, 255)?;
        }
        let mut tx = self.transaction().await?;
        let existing = Catalog::filter_by_key(key).first().exec(&mut tx).await?;
        let id = if let Some(record) = existing {
            data.key = None;
            update_catalog_fields(&mut tx, record.uuid, data).await?;
            record.uuid
        } else {
            let url = data.url.as_deref().ok_or(Error::MissingField("url"))?;
            let size = data.size.ok_or(Error::MissingField("size"))?;
            let format = data.format.ok_or(Error::MissingField("format"))?;
            let now = Timestamp::now();
            let uuid = Uuid::new_v4();
            let temporary = format!("file:///{}", uuid);
            Catalog::create()
                .uuid(uuid)
                .key(insert_varchar(key, 750, &uuid.to_string()))
                .url(insert_varchar(url, 750, &temporary))
                .size(size)
                .format(insert_varchar(&format, 255, "bin"))
                .uploaded(data.uploaded.unwrap_or(false))
                .created_at(now)
                .updated_at(now)
                .exec(&mut tx)
                .await?;
            if key.len() > 750 || url.len() > 750 || format.len() > 255 {
                Catalog::filter_by_uuid(uuid)
                    .update()
                    .key(key)
                    .url(url)
                    .format(format)
                    .exec(&mut tx)
                    .await?;
            }
            uuid
        };
        tx.commit().await?;
        Ok(id)
    }

    async fn update_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        id: Uuid,
        data: C,
    ) -> Result<(), Error>
    where
        Self: 'a,
    {
        let data = data.into();
        if let Some(key) = &data.key {
            validate_field("key", key, 750)?;
        }
        if let Some(url) = &data.url {
            validate_url(url)?;
            validate_field("url", url, 750)?;
        }
        if let Some(format) = &data.format {
            validate_field("format", format, 255)?;
        }
        let mut tx = self.transaction().await?;
        if tx.get_catalog(id).await?.is_none() {
            return Err(Error::CatalogNotFound(id));
        }
        update_catalog_fields(&mut tx, id, data).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get_catalog<'a>(&'a mut self, id: Uuid) -> Result<Option<Catalog>, Error>
    where
        Self: 'a,
    {
        Ok(Catalog::filter_by_uuid(id).first().exec(self).await?)
    }
}

impl<B: Executor> FileSystem for B {
    type FileId<'a>
        = Uuid
    where
        Self: 'a;
    type FileData<'a>
        = File
    where
        Self: 'a;

    type FileOptionalData<'a>
        = FileOptionalData<Uuid>
    where
        Self: 'a;

    async fn upsert_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        data: F,
    ) -> Result<Uuid, Error>
    where
        Self: 'a,
    {
        let mut data = data.into();
        let fullpath = data
            .fullpath
            .as_deref()
            .ok_or(Error::MissingField("fullpath"))?;
        validate_field("fullpath", fullpath, 512)?;
        if let Some(filename) = &data.filename {
            validate_field("filename", filename, 255)?;
        }
        let mut tx = self.transaction().await?;
        let existing = File::filter_by_fullpath(fullpath)
            .first()
            .exec(&mut tx)
            .await?;
        if let Some(catalog_id) = data.catalog_id
            && tx.get_catalog(catalog_id).await?.is_none()
        {
            return Err(Error::CatalogNotFound(catalog_id));
        }
        let id = if let Some(record) = existing {
            data.fullpath = None;
            update_file_fields(&mut tx, record.uuid, data).await?;
            record.uuid
        } else {
            let filename = data.filename.ok_or(Error::MissingField("filename"))?;
            let catalog_id = data.catalog_id.ok_or(Error::MissingField("catalog_id"))?;
            let now = Timestamp::now();
            let uuid = Uuid::new_v4();
            let temporary = uuid.to_string();
            File::create()
                .uuid(uuid)
                .fullpath(insert_varchar(fullpath, 512, &temporary))
                .filename(insert_varchar(&filename, 255, "file"))
                .catalog_uuid(catalog_id)
                .created_at(now)
                .updated_at(now)
                .exec(&mut tx)
                .await?;
            if fullpath.len() > 512 || filename.len() > 255 {
                File::filter_by_uuid(uuid)
                    .update()
                    .fullpath(fullpath)
                    .filename(filename)
                    .exec(&mut tx)
                    .await?;
            }
            uuid
        };
        tx.commit().await?;
        Ok(id)
    }

    async fn update_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        id: Uuid,
        data: F,
    ) -> Result<(), Error>
    where
        Self: 'a,
    {
        let data = data.into();
        if let Some(fullpath) = &data.fullpath {
            validate_field("fullpath", fullpath, 512)?;
        }
        if let Some(filename) = &data.filename {
            validate_field("filename", filename, 255)?;
        }
        let mut tx = self.transaction().await?;
        if File::filter_by_uuid(id)
            .first()
            .exec(&mut tx)
            .await?
            .is_none()
        {
            return Err(Error::FileNotFound(id));
        }
        if let Some(catalog_id) = data.catalog_id
            && tx.get_catalog(catalog_id).await?.is_none()
        {
            return Err(Error::CatalogNotFound(catalog_id));
        }
        update_file_fields(&mut tx, id, data).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get_file<'a>(
        &'a mut self,
        key: FileKey<'_, Uuid>,
    ) -> Result<Option<FileWithCatalog<File, Catalog>>, Error>
    where
        Self: 'a,
    {
        let mut tx = self.transaction().await?;
        let query = match key {
            FileKey::Id(id) => File::filter(File::fields().uuid().eq(id)),
            FileKey::Fullpath(name) => File::filter(File::fields().fullpath().eq(name)),
        };
        let result = match query.first().exec(&mut tx).await? {
            Some(file) => tx
                .get_catalog(file.catalog_uuid)
                .await?
                .map(|catalog| FileWithCatalog { file, catalog }),
            None => None,
        };
        tx.commit().await?;
        Ok(result)
    }
}

// Toasty 0.11's insert validator counts UTF-8 bytes, while MySQL VARCHAR counts
// characters. For already-validated values exceeding that byte limit, insert a
// temporary value and replace it with an ORM update in the same transaction.
// Unique fields use the new row's UUID so no temporary name is shared.
fn insert_varchar<'a>(value: &'a str, max_chars: usize, temporary: &'a str) -> &'a str {
    if value.len() > max_chars {
        temporary
    } else {
        value
    }
}

async fn update_catalog_fields(
    tx: &mut impl Executor,
    id: Uuid,
    data: CatalogOptionalData,
) -> Result<(), Error> {
    if data == CatalogOptionalData::default() {
        return Ok(());
    }
    let mut update = Catalog::filter_by_uuid(id)
        .update()
        .updated_at(Timestamp::now());
    if let Some(key) = data.key {
        update = update.key(key);
    }
    if let Some(url) = data.url {
        update = update.url(url);
    }
    if let Some(size) = data.size {
        update = update.size(size);
    }
    if let Some(format) = data.format {
        update = update.format(format);
    }
    if let Some(uploaded) = data.uploaded {
        update = update.uploaded(uploaded);
    }
    update.exec(tx).await?;
    Ok(())
}

async fn update_file_fields(
    tx: &mut impl Executor,
    id: Uuid,
    data: FileOptionalData<Uuid>,
) -> Result<(), Error> {
    if data == FileOptionalData::default() {
        return Ok(());
    }
    let mut update = File::filter_by_uuid(id)
        .update()
        .updated_at(Timestamp::now());
    if let Some(fullpath) = data.fullpath {
        update = update.fullpath(fullpath);
    }
    if let Some(filename) = data.filename {
        update = update.filename(filename);
    }
    if let Some(catalog_id) = data.catalog_id {
        update = update.catalog_uuid(catalog_id);
    }
    update.exec(tx).await?;
    Ok(())
}

fn validate_url(value: &str) -> Result<(), Error> {
    let url = Url::parse(value).map_err(|_| Error::InvalidUrl)?;
    if value.trim() != value
        || value.chars().any(char::is_control)
        || url.cannot_be_a_base()
        || (url.host_str().is_none() && url.scheme() != "file")
        || url.path().is_empty()
        || url.path() == "/"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::InvalidUrl);
    }
    Ok(())
}

fn validate_field(name: &'static str, value: &str, max_chars: usize) -> Result<(), Error> {
    if value.is_empty() || value.contains('\0') || value.chars().count() > max_chars {
        return Err(Error::InvalidField(name));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_provider_and_local_object_urls() {
        for value in [
            "s3://a/b/c",
            "oss://a/b/c",
            "file://host/path",
            "file:///tmp/a",
            "s3://bucket/key",
        ] {
            assert!(validate_url(value).is_ok(), "{value}");
        }
        for value in [
            "relative/path",
            "s3://a",
            "oss:///key",
            "file:///",
            "mailto:a@b",
            "oss://user:password@a/key",
            "oss://a/key#fragment",
            " oss://a/key",
        ] {
            assert!(validate_url(value).is_err(), "{value}");
        }
    }
}
