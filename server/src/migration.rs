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
