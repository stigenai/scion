use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};

#[tokio::test]
async fn sqlite_version_has_wal_fix() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let options = SqliteConnectOptions::new()
        .filename(dir.path().join("version.db"))
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let (version, source_id): (String, String) = sqlx::query_as("SELECT sqlite_version(), sqlite_source_id()")
        .fetch_one(&mut connection)
        .await?;
    println!("SQLite {version}, source ID {source_id}");

    let components = version
        .split('.')
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        components.as_slice() >= &[3, 54, 0],
        "SQLite {version} is older than 3.54.0"
    );
    Ok(())
}
