//! Non-UUID identities, borrowed data, and borrowed inputs exercise the filesystem GATs.
use catalog::{
    Catalog, CatalogInput, CatalogOptionalData, CatalogSystem, File, FileInput, FileOptionalData,
    FileSystem, FileWithCatalog,
};
use std::io;

struct BorrowingStore<'data> {
    catalog: &'data mut CatalogInput,
    file: &'data mut FileInput<u64>,
    file_id: &'data str,
}

impl CatalogSystem for BorrowingStore<'_> {
    type Err = io::Error;
    type CatalogId<'a>
        = u64
    where
        Self: 'a;
    type CatalogData<'a>
        = &'a CatalogInput
    where
        Self: 'a;

    type CatalogOptionalData<'a>
        = &'a CatalogOptionalData
    where
        Self: 'a;

    async fn upsert_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        data: C,
    ) -> Result<u64, io::Error>
    where
        Self: 'a,
    {
        let data = data.into();
        assert_eq!(data.key.as_deref(), Some(self.catalog.key.as_str()));
        if let Some(url) = &data.url {
            self.catalog.url = url.clone();
        }
        if let Some(size) = data.size {
            self.catalog.size = size;
        }
        if let Some(format) = &data.format {
            self.catalog.format = format.clone();
        }
        if let Some(uploaded) = data.uploaded {
            self.catalog.uploaded = uploaded;
        }
        Ok(7)
    }
    async fn update_catalog<'a, C: Into<Self::CatalogOptionalData<'a>>>(
        &'a mut self,
        id: Self::CatalogId<'a>,
        data: C,
    ) -> Result<(), io::Error>
    where
        Self: 'a,
    {
        if id != 7 {
            return Err(io::ErrorKind::NotFound.into());
        }
        let data = data.into();
        if let Some(key) = &data.key {
            self.catalog.key = key.clone();
        }
        if let Some(url) = &data.url {
            self.catalog.url = url.clone();
        }
        if let Some(size) = data.size {
            self.catalog.size = size;
        }
        if let Some(format) = &data.format {
            self.catalog.format = format.clone();
        }
        if let Some(uploaded) = data.uploaded {
            self.catalog.uploaded = uploaded;
        }
        Ok(())
    }

    async fn get_catalog<'a>(
        &'a mut self,
        id: Self::CatalogId<'a>,
    ) -> Result<Option<Self::CatalogData<'a>>, io::Error>
    where
        Self: 'a,
    {
        Ok((id == 7).then_some(&*self.catalog))
    }
    async fn get_catalog_by_key<'a>(
        &'a mut self,
        key: &str,
    ) -> Result<Option<Self::CatalogData<'a>>, io::Error>
    where
        Self: 'a,
    {
        Ok((key == self.catalog.key).then_some(&*self.catalog))
    }
}

impl FileSystem for BorrowingStore<'_> {
    type FileId<'a>
        = &'a str
    where
        Self: 'a;
    type FileData<'a>
        = &'a FileInput<u64>
    where
        Self: 'a;

    type FileOptionalData<'a>
        = &'a FileOptionalData<u64>
    where
        Self: 'a;

    async fn upsert_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        data: F,
    ) -> Result<Self::FileId<'a>, io::Error>
    where
        Self: 'a,
    {
        let data = data.into();
        assert_eq!(data.catalog_id, Some(self.file.catalog_id));
        if let Some(filename) = &data.filename {
            self.file.filename = filename.clone();
        }
        if let Some(catalog_id) = data.catalog_id {
            assert_eq!(catalog_id, 7);
            self.file.catalog_id = catalog_id;
        }
        Ok(self.file_id)
    }
    async fn update_file<'a, F: Into<Self::FileOptionalData<'a>>>(
        &'a mut self,
        id: Self::FileId<'a>,
        data: F,
    ) -> Result<(), io::Error>
    where
        Self: 'a,
    {
        if id != self.file_id {
            return Err(io::ErrorKind::NotFound.into());
        }
        let data = data.into();
        if let Some(node_run_id) = data.node_run_id {
            self.file.node_run_id = node_run_id;
        }
        if let Some(filename) = &data.filename {
            self.file.filename = filename.clone();
        }
        if let Some(catalog_id) = data.catalog_id {
            assert_eq!(catalog_id, 7);
            self.file.catalog_id = catalog_id;
        }
        Ok(())
    }

    async fn get_file<'a>(
        &'a mut self,
        id: Self::FileId<'a>,
    ) -> Result<Option<FileWithCatalog<Self::FileData<'a>, Option<Self::CatalogData<'a>>>>, io::Error>
    where
        Self: 'a,
    {
        Ok((id == self.file_id).then_some(FileWithCatalog {
            file: &*self.file,
            catalog: Some(&*self.catalog),
        }))
    }
    async fn get_file_by_node_run_filename<'a>(
        &'a mut self,
        node_run_id: uuid::Uuid,
        filename: &str,
    ) -> Result<Option<FileWithCatalog<Self::FileData<'a>, Option<Self::CatalogData<'a>>>>, io::Error>
    where
        Self: 'a,
    {
        Ok(
            (node_run_id == self.file.node_run_id && filename == self.file.filename).then_some(
                FileWithCatalog {
                    file: &*self.file,
                    catalog: Some(&*self.catalog),
                },
            ),
        )
    }
}

async fn read_catalog<'a, S: CatalogSystem + 'a>(
    store: &'a mut S,
    id: S::CatalogId<'a>,
) -> S::CatalogData<'a> {
    store.get_catalog(id).await.unwrap().unwrap()
}

#[tokio::test]
async fn identities_and_borrowed_data_are_independent() {
    let mut catalog = CatalogInput::new("/a", "s3://bucket/a", 10, "bin");
    let mut file = FileInput::new(uuid::Uuid::from_u128(1), "a", 7);
    let file_id = String::from("file-seven");
    let catalog_ptr = &catalog as *const CatalogInput;
    let file_ptr = &file as *const FileInput<u64>;
    let mut store = BorrowingStore {
        catalog: &mut catalog,
        file: &mut file,
        file_id: &file_id,
    };
    let mut data = CatalogInput::new("/a", "s3://bucket/a", 20, "csv");
    data.uploaded = true;
    let id = store
        .upsert_catalog(&CatalogOptionalData::from(&data as &dyn Catalog))
        .await
        .unwrap();
    assert_eq!(id, 7);
    assert!(store.get_catalog(id).await.unwrap().unwrap().uploaded());
    assert_eq!(
        store
            .upsert_catalog(&CatalogOptionalData::from(&CatalogInput::new(
                "/a",
                "s3://bucket/a",
                30,
                "csv"
            )))
            .await
            .unwrap(),
        id
    );
    let loaded = read_catalog(&mut store, id).await;
    assert!(std::ptr::eq(loaded, catalog_ptr));
    assert_eq!(loaded.url(), "s3://bucket/a");
    assert_eq!(loaded.size(), 30);
    assert!(!loaded.uploaded());
    let id = store
        .upsert_file(&FileOptionalData::from(&FileInput::new(
            uuid::Uuid::from_u128(1),
            "a.csv",
            7_u64,
        )
            as &dyn File<CatalogId = u64>))
        .await
        .unwrap()
        .to_owned();
    assert_eq!(id, file_id);
    assert_eq!(
        store
            .upsert_file(&FileOptionalData {
                node_run_id: Some(uuid::Uuid::from_u128(1)),
                filename: Some("new.csv".into()),
                catalog_id: Some(7)
            })
            .await
            .unwrap(),
        id
    );
    let loaded = {
        let name = String::from("/a");
        store
            .get_file_by_node_run_filename(uuid::Uuid::from_u128(1), "new.csv")
            .await
            .unwrap()
            .unwrap()
    };
    assert!(loaded.catalog.is_some() && std::ptr::eq(loaded.catalog.unwrap(), catalog_ptr));
    assert!(std::ptr::eq(loaded.file, file_ptr));
    assert_eq!(loaded.file.filename(), "new.csv");
    assert!(store.get_catalog(8).await.unwrap().is_none());
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn s3_preserves_identity_data_and_optional_data_gats() {
    use catalog::{
        ObjectStorage, PresignOperation,
        s3::{Config, Credentials, Error, S3},
    };
    use std::time::Duration;
    let mut catalog = CatalogInput::new("/a", "s3://bucket/a", 10, "bin");
    let mut file = FileInput::new(uuid::Uuid::from_u128(1), "a", 7);
    let mut store = S3::new(
        BorrowingStore {
            catalog: &mut catalog,
            file: &mut file,
            file_id: "file-seven",
        },
        Config::new(
            "http://localhost:9000",
            Credentials::new("access", "secret", None, None, "test"),
        ),
    )
    .unwrap();
    let id = store
        .upsert_catalog(&CatalogOptionalData::from(&CatalogInput::new(
            "/a",
            "s3://bucket/a",
            20,
            "csv",
        )))
        .await
        .unwrap();
    let data = CatalogOptionalData {
        key: Some("/a".into()),
        uploaded: Some(true),
        ..Default::default()
    };
    assert_eq!(store.upsert_catalog(&data).await.unwrap(), id);
    let loaded = store.get_catalog(id).await.unwrap().unwrap();
    assert!(loaded.uploaded());
    assert_eq!(loaded.size(), 20);
    assert_eq!(loaded.format(), "csv");
    let file_id = store
        .upsert_file(&FileOptionalData::from(&FileInput::new(
            uuid::Uuid::from_u128(1),
            "a",
            id,
        )))
        .await
        .unwrap()
        .to_owned();
    assert_eq!(
        store
            .upsert_file(&FileOptionalData {
                node_run_id: Some(uuid::Uuid::from_u128(1)),
                filename: Some("new".into()),
                catalog_id: Some(7)
            })
            .await
            .unwrap(),
        file_id
    );
    assert_eq!(
        store
            .get_file(&file_id)
            .await
            .unwrap()
            .unwrap()
            .file
            .catalog_id(),
        id
    );
    for operation in [PresignOperation::Upload, PresignOperation::Download] {
        let signed = store
            .presign(id, operation.clone(), Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(signed.operation, operation);
        assert!(signed.url.starts_with("http://localhost:9000/bucket/a?"));
        assert!(signed.url.contains("X-Amz-Signature="));
    }
    store
        .update_catalog(
            id,
            &CatalogOptionalData {
                uploaded: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .update_file(
            &file_id,
            &FileOptionalData {
                filename: Some("updated".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let loaded = store.get_file(&file_id).await.unwrap().unwrap();
    assert_eq!(loaded.file.filename(), "updated");
    assert_eq!(loaded.file.node_run_id(), uuid::Uuid::from_u128(1));
    assert!(!loaded.catalog.uploaded());
    assert_eq!(loaded.catalog.size(), 20);
    assert!(
        matches!(store.update_catalog(8, &CatalogOptionalData::default()).await, Err(Error::FileSystem(error)) if error.kind() == io::ErrorKind::NotFound)
    );
    assert!(
        matches!(store.update_file("missing", &FileOptionalData::default()).await, Err(Error::FileSystem(error)) if error.kind() == io::ErrorKind::NotFound)
    );
    assert!(
        matches!(store.presign(8, PresignOperation::Download, Duration::from_secs(60)).await, Err(Error::CatalogNotFound(id)) if id == "8")
    );
}

#[cfg(feature = "s3")]
#[test]
fn object_storage_contract_signs_preloaded_records_and_multipart_operations() {
    use catalog::{
        ObjectStorage, PresignOperation,
        s3::{Config, Credentials},
    };
    use std::time::Duration;
    let mut data = CatalogInput::new("/a", "s3://bucket/a", 10, "bin");
    let mut file = FileInput::new(uuid::Uuid::from_u128(1), "a", 7);
    let storage = Config::new(
        "http://localhost:9000",
        Credentials::new("access", "secret", None, None, "test"),
    )
    .build(BorrowingStore {
        catalog: &mut data,
        file: &mut file,
        file_id: "file-seven",
    })
    .unwrap();
    // The record is not persisted in this store: loaded-record signing must not
    // attempt a lookup (prepare may still be inside an uncommitted transaction).
    let record = CatalogInput::new("/uncommitted", "s3://bucket/uncommitted", 1024, "bin");
    let signed = storage
        .presign_catalog(&record, PresignOperation::Upload, Duration::from_secs(60))
        .unwrap();
    assert!(signed.url.contains("/bucket/uncommitted?"));
    assert!(storage.supports_multipart());
    let operation = PresignOperation::UploadPart {
        upload_id: "id+/part".into(),
        part_number: 2,
    };
    let part = storage
        .presign_catalog(&record, operation.clone(), Duration::from_secs(60))
        .unwrap();
    assert_eq!(part.operation, operation);
    assert!(part.url.contains("partNumber=2"));
}
