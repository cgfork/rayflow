use toasty::migration::{MigrationFile, MigrationReport, MigrationSet};
use url::Url;

pub const MIGRATIONS: MigrationSet = MigrationSet::new(&[
    MigrationFile::new(
        1,
        "initial_schema",
        include_str!("../migrations/0001_initial.sql"),
    ),
    MigrationFile::new(
        2,
        "legacy_job_defaults",
        include_str!("../migrations/0002_legacy_job_defaults.sql"),
    ),
    MigrationFile::new(
        3,
        "unique_run",
        include_str!("../migrations/0003_unique_run.sql"),
    ),
    MigrationFile::new(
        4,
        "command_result",
        include_str!("../migrations/0004_command_result.sql"),
    ),
    MigrationFile::new(
        5,
        "command_result_error_code",
        include_str!("../migrations/0005_command_result_error_code.sql"),
    ),
    MigrationFile::new(
        6,
        "node_run_ineffective",
        include_str!("../migrations/0006_node_run_ineffective.sql"),
    ),
    MigrationFile::new(
        7,
        "authentication",
        include_str!("../migrations/0007_authentication.sql"),
    ),
    MigrationFile::new(8, "catalog", include_str!("../migrations/0008_catalog.sql")),
    MigrationFile::new(
        9,
        "run_config_execution_fields",
        include_str!("../migrations/0009_run_config_execution_fields.sql"),
    ),
    MigrationFile::new(
        10,
        "catalog_uploaded",
        include_str!("../migrations/0010_catalog_uploaded.sql"),
    ),
    MigrationFile::new(
        11,
        "catalog_url",
        include_str!("../migrations/0011_catalog_url.sql"),
    ),
    MigrationFile::new(
        12,
        "catalog_remove_deleted_at",
        include_str!("../migrations/0012_catalog_remove_deleted_at.sql"),
    ),
    MigrationFile::new(
        13,
        "file_remove_deleted_at",
        include_str!("../migrations/0013_file_remove_deleted_at.sql"),
    ),
    MigrationFile::new(
        14,
        "auth_user",
        include_str!("../migrations/0014_auth_user.sql"),
    ),
    MigrationFile::new(
        15,
        "auth_session",
        include_str!("../migrations/0015_auth_session.sql"),
    ),
    MigrationFile::new(
        16,
        "auth_login_flow",
        include_str!("../migrations/0016_auth_login_flow.sql"),
    ),
    MigrationFile::new(
        17,
        "catalog_key",
        include_str!("../migrations/0017_catalog_key.sql"),
    ),
    MigrationFile::new(
        18,
        "file_fullpath",
        include_str!("../migrations/0018_file_fullpath.sql"),
    ),
    MigrationFile::new(
        19,
        "node_run_uuid",
        include_str!("../migrations/0019_node_run_uuid.sql"),
    ),
    MigrationFile::new(
        20,
        "file_node_run",
        include_str!("../migrations/0020_file_node_run.sql"),
    ),
    MigrationFile::new(
        21,
        "file_catalog_key",
        include_str!("../migrations/0021_file_catalog_key.sql"),
    ),
    MigrationFile::new(
        22,
        "uuid_references",
        include_str!("../migrations/0022_uuid_references.sql"),
    ),
    MigrationFile::new(
        23,
        "job_result",
        include_str!("../migrations/0023_job_result.sql"),
    ),
    MigrationFile::new(
        24,
        "job_artifact",
        include_str!("../migrations/0024_job_artifact.sql"),
    ),
    MigrationFile::new(
        25,
        "unique_run_config",
        include_str!("../migrations/0025_unique_run_config.sql"),
    ),
    MigrationFile::new(
        26,
        "named_node_config",
        include_str!("../migrations/0026_named_node_config.sql"),
    ),
    MigrationFile::new(27, "asset", include_str!("../migrations/0027_asset.sql")),
    MigrationFile::new(
        28,
        "asset_promotion",
        include_str!("../migrations/0028_asset_promotion.sql"),
    ),
]);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "database_url must be a mysql:// URL with a database name of 1–64 letters, digits, underscores, or hyphens"
    )]
    InvalidDatabaseUrl,
    #[error(transparent)]
    Database(#[from] toasty::Error),
}

pub async fn initialize(database_url: &str) -> Result<MigrationReport, Error> {
    let mut url = Url::parse(database_url).map_err(|_| Error::InvalidDatabaseUrl)?;
    let name = url.path().strip_prefix('/').unwrap_or_default().to_owned();
    if url.scheme() != "mysql"
        || url.host_str().is_none()
        || url.fragment().is_some()
        || name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(Error::InvalidDatabaseUrl);
    }

    url.set_path("/information_schema");
    let mut admin = toasty::Db::builder()
        .models(crate::models())
        .connect(url.as_str())
        .await?;
    // Toasty migrations require a database; database creation has no model operation.
    toasty::sql::statement(format!(
        "CREATE DATABASE IF NOT EXISTS `{name}` CHARACTER SET utf8mb4"
    ))
    .exec(&mut admin)
    .await?;
    let db = toasty::Db::builder()
        .models(crate::models())
        .connect(database_url)
        .await?;
    Ok(MIGRATIONS.apply(&db).await?)
}
