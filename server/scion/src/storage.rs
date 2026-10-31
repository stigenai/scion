//! 存储后端的选择与打开：元数据（内存 / SQLite / PostgreSQL）与对象（内存 / 本地目录 / S3 兼容）。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use scion_store::{BlobStore, MemStore, MetaStore};
use scion_store_blob::{ObjectBlobStore, S3Config, S3Provider};

/// 元数据后端。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaSpec {
    Mem,
    /// `sqlite://…` 或 `postgres://…`。
    Url(String),
}

/// 对象存储后端。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobSpec {
    Mem,
    Dir(PathBuf),
    /// `s3://bucket[/prefix]`，连接参数来自环境变量（见 [`s3_config`]）。
    S3 {
        bucket: String,
        prefix: String,
    },
}

pub fn parse_meta(s: &str) -> anyhow::Result<MetaSpec> {
    match s {
        "mem" => Ok(MetaSpec::Mem),
        _ if s.starts_with("sqlite:") || s.starts_with("postgres://") || s.starts_with("postgresql://") => {
            Ok(MetaSpec::Url(s.to_owned()))
        }
        _ => bail!("unsupported --meta {s:?}; expected mem, sqlite://PATH or postgres://…"),
    }
}

pub fn parse_blobs(s: &str) -> anyhow::Result<BlobSpec> {
    if s == "mem" {
        return Ok(BlobSpec::Mem);
    }
    if let Some(rest) = s.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            bail!("--blobs s3:// needs a bucket name, e.g. s3://scion-objects/prefix");
        }
        return Ok(BlobSpec::S3 {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
        });
    }
    let path = s.strip_prefix("file://").unwrap_or(s);
    if path.is_empty() {
        bail!("--blobs needs mem, a directory, or s3://bucket[/prefix]");
    }
    Ok(BlobSpec::Dir(PathBuf::from(path)))
}

/// Lite 档：同一目录下的 SQLite 数据库与对象目录。
pub fn lite(data_dir: &Path) -> (MetaSpec, BlobSpec) {
    let db = data_dir.join("meta.db");
    (
        MetaSpec::Url(format!("sqlite://{}", db.display())),
        BlobSpec::Dir(data_dir.join("objects")),
    )
}

/// 从环境变量读取 S3 连接参数。优先 `SCION_S3_*`，其次通用的 `AWS_*`。
/// `SCION_S3_PROVIDER`：auto（默认，按响应头识别）、minio、seaweedfs、aws、generic。
pub fn s3_config(bucket: &str, prefix: &str, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<S3Config> {
    let pick = |a: &str, b: &str| env(a).or_else(|| env(b)).unwrap_or_default();
    let provider: S3Provider = env("SCION_S3_PROVIDER")
        .unwrap_or_default()
        .parse()
        .context("SCION_S3_PROVIDER")?;
    Ok(S3Config {
        endpoint: env("SCION_S3_ENDPOINT").filter(|e| !e.is_empty()),
        bucket: bucket.to_owned(),
        region: env("SCION_S3_REGION")
            .or_else(|| env("AWS_REGION"))
            .unwrap_or_else(|| "us-east-1".into()),
        access_key_id: pick("SCION_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
        secret_access_key: pick("SCION_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"),
        session_token: env("SCION_S3_SESSION_TOKEN")
            .or_else(|| {
                // Explicit local credentials must not inherit an unrelated AWS session.
                if env("SCION_S3_ACCESS_KEY_ID").is_none() && env("SCION_S3_SECRET_ACCESS_KEY").is_none() {
                    env("AWS_SESSION_TOKEN")
                } else {
                    None
                }
            })
            .filter(|v| !v.is_empty()),
        provider,
        // 未设置时按服务决定（只有 AWS 默认 virtual-hosted）
        virtual_hosted_style: env("SCION_S3_VIRTUAL_HOSTED")
            .filter(|v| !v.is_empty())
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true")),
        prefix: prefix.to_owned(),
        max_retries: env("SCION_S3_MAX_RETRIES").and_then(|v| v.parse().ok()),
    })
}

/// 打开两个后端。两者都是内存时共用同一个 [`MemStore`]。对象存储会先做读写自检。
/// `SCION_BLOB_COMPRESSION=off` stores new objects uncompressed (reads handle both forms).
fn compression(env: &impl Fn(&str) -> Option<String>) -> bool {
    !matches!(env("SCION_BLOB_COMPRESSION").as_deref(), Some("off" | "0" | "false"))
}

/// Objects written together go into packs unless `SCION_BLOB_PACKS` is off.
fn packs(env: &impl Fn(&str) -> Option<String>) -> bool {
    !matches!(env("SCION_BLOB_PACKS").as_deref(), Some("off" | "0" | "false"))
}

fn with_packs(s: ObjectBlobStore, meta: &Arc<dyn MetaStore>, env: &impl Fn(&str) -> Option<String>) -> ObjectBlobStore {
    if packs(env) { s.with_packs(meta.clone()) } else { s }
}

/// The directory a `sqlite://` URL's database file lives in, so it can be created before
/// opening (SQLite creates the file but not missing parent directories).
pub fn sqlite_parent(url: &str) -> Option<PathBuf> {
    let path = url.strip_prefix("sqlite://")?;
    let path = path.split('?').next().unwrap_or_default();
    if path.is_empty() || path == ":memory:" {
        return None;
    }
    Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
}

pub async fn open(
    meta: &MetaSpec,
    blobs: &BlobSpec,
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<(Arc<dyn MetaStore>, Arc<dyn BlobStore>)> {
    let (meta, blobs, _) = open_objects(meta, blobs, env).await?;
    Ok((meta, blobs))
}

/// Like [`open`], also returning the object store itself when there is one (not `mem`), for
/// operations on its layout such as repacking.
pub async fn open_objects(
    meta: &MetaSpec,
    blobs: &BlobSpec,
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<(Arc<dyn MetaStore>, Arc<dyn BlobStore>, Option<Arc<ObjectBlobStore>>)> {
    let mem = Arc::new(MemStore::new());
    let meta_store: Arc<dyn MetaStore> = match meta {
        MetaSpec::Mem => mem.clone(),
        MetaSpec::Url(url) => {
            if let Some(dir) = sqlite_parent(url) {
                std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
            }
            scion_store_sql::connect(url).await.context("open metadata store")?
        }
    };
    let objects: Option<Arc<ObjectBlobStore>> = match blobs {
        BlobSpec::Mem => None,
        BlobSpec::Dir(dir) => {
            let s = ObjectBlobStore::local(dir)
                .context("open blob directory")?
                .with_compression(compression(&env));
            let s = with_packs(s, &meta_store, &env);
            s.self_test().await.context("blob store self-test")?;
            Some(Arc::new(s))
        }
        BlobSpec::S3 { bucket, prefix } => {
            let s = ObjectBlobStore::s3(&s3_config(bucket, prefix, &env)?)
                .context("configure s3")?
                .with_compression(compression(&env));
            let s = with_packs(s, &meta_store, &env);
            // 识别服务（MinIO / SeaweedFS / AWS …）、健康检查、bucket 是否存在、读写自检
            let backend = s.check().await.context("s3 check (endpoint, bucket and credentials)")?;
            tracing::info!(%backend, bucket, "object store ready");
            Some(Arc::new(s))
        }
    };
    let blob_store: Arc<dyn BlobStore> = match &objects {
        Some(o) => o.clone(),
        None => mem,
    };
    Ok((meta_store, blob_store, objects))
}

/// 用于日志：不暴露 URL 中的密码。
pub fn describe(meta: &MetaSpec, blobs: &BlobSpec) -> String {
    let m = match meta {
        MetaSpec::Mem => "mem".to_owned(),
        MetaSpec::Url(u) if u.starts_with("sqlite:") => u.clone(),
        MetaSpec::Url(u) => u
            .split_once('@')
            .map_or_else(|| "postgres".to_owned(), |(_, host)| format!("postgres://…@{host}")),
    };
    let b = match blobs {
        BlobSpec::Mem => "mem".to_owned(),
        BlobSpec::Dir(d) => d.display().to_string(),
        BlobSpec::S3 { bucket, prefix } => format!("s3://{bucket}/{prefix}"),
    };
    format!("meta={m} blobs={b}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn s3_temporary_credentials_keep_session_token() {
        let cfg = s3_config(
            "b",
            "",
            env_of(&[
                ("AWS_ACCESS_KEY_ID", "key"),
                ("AWS_SECRET_ACCESS_KEY", "secret"),
                ("AWS_SESSION_TOKEN", "session"),
            ]),
        )
        .expect("config");
        assert_eq!(cfg.session_token.as_deref(), Some("session"));
        let cfg = s3_config(
            "b",
            "",
            env_of(&[
                ("AWS_SESSION_TOKEN", "aws-session"),
                ("SCION_S3_SESSION_TOKEN", "override"),
            ]),
        )
        .expect("config");
        assert_eq!(cfg.session_token.as_deref(), Some("override"));
        let cfg = s3_config(
            "b",
            "",
            env_of(&[
                ("SCION_S3_ACCESS_KEY_ID", "local"),
                ("SCION_S3_SECRET_ACCESS_KEY", "local-secret"),
                ("AWS_SESSION_TOKEN", "unrelated-session"),
            ]),
        )
        .expect("config");
        assert!(
            cfg.session_token.is_none(),
            "local keys must not inherit the AWS session token"
        );
    }

    #[test]
    fn parse_meta_cases() {
        assert_eq!(parse_meta("mem").expect("ok"), MetaSpec::Mem);
        assert_eq!(
            parse_meta("sqlite:///x.db").expect("ok"),
            MetaSpec::Url("sqlite:///x.db".into())
        );
        assert!(matches!(parse_meta("postgres://h/db").expect("ok"), MetaSpec::Url(_)));
        assert!(matches!(parse_meta("postgresql://h/db").expect("ok"), MetaSpec::Url(_)));
        assert!(parse_meta("mysql://h").is_err());
    }

    #[test]
    fn parse_blobs_cases() {
        assert_eq!(parse_blobs("mem").expect("ok"), BlobSpec::Mem);
        assert_eq!(
            parse_blobs("/var/scion").expect("ok"),
            BlobSpec::Dir("/var/scion".into())
        );
        assert_eq!(
            parse_blobs("file:///var/scion").expect("ok"),
            BlobSpec::Dir("/var/scion".into())
        );
        assert_eq!(
            parse_blobs("s3://bucket/a/b").expect("ok"),
            BlobSpec::S3 {
                bucket: "bucket".into(),
                prefix: "a/b".into()
            }
        );
        assert_eq!(
            parse_blobs("s3://bucket").expect("ok"),
            BlobSpec::S3 {
                bucket: "bucket".into(),
                prefix: String::new()
            }
        );
        assert!(parse_blobs("s3://").is_err());
        assert!(parse_blobs("file://").is_err());
        assert!(parse_blobs("").is_err());
    }

    #[test]
    fn lite_layout() {
        let (m, b) = lite(Path::new("/data"));
        assert_eq!(m, MetaSpec::Url("sqlite:///data/meta.db".into()));
        assert_eq!(b, BlobSpec::Dir("/data/objects".into()));
    }

    #[test]
    fn s3_config_from_env_prefers_scion_vars() {
        let c = s3_config(
            "b",
            "p",
            env_of(&[
                ("SCION_S3_ENDPOINT", "http://s3:8333"),
                ("SCION_S3_ACCESS_KEY_ID", "scion-key"),
                ("AWS_ACCESS_KEY_ID", "aws-key"),
                ("AWS_SECRET_ACCESS_KEY", "aws-secret"),
                ("AWS_REGION", "cn-north-1"),
                ("SCION_S3_VIRTUAL_HOSTED", "true"),
                ("SCION_S3_PROVIDER", "MinIO"),
            ]),
        )
        .expect("config");
        assert_eq!(c.endpoint.as_deref(), Some("http://s3:8333"));
        assert_eq!(c.access_key_id, "scion-key");
        assert_eq!(c.secret_access_key, "aws-secret");
        assert_eq!(c.region, "cn-north-1");
        assert_eq!(c.virtual_hosted_style, Some(true));
        assert_eq!(c.provider, S3Provider::Minio);
        assert_eq!((c.bucket.as_str(), c.prefix.as_str()), ("b", "p"));
    }

    #[test]
    fn s3_config_defaults() {
        let c = s3_config(
            "b",
            "",
            env_of(&[("SCION_S3_ENDPOINT", ""), ("SCION_S3_VIRTUAL_HOSTED", "")]),
        )
        .expect("config");
        assert_eq!(c.endpoint, None);
        assert_eq!(c.region, "us-east-1");
        assert_eq!(c.virtual_hosted_style, None);
        assert_eq!(c.provider, S3Provider::Auto);
        assert!(c.access_key_id.is_empty());
        assert_eq!(c.max_retries, None);
        let c = s3_config(
            "b",
            "",
            env_of(&[("SCION_S3_MAX_RETRIES", "3"), ("SCION_S3_VIRTUAL_HOSTED", "0")]),
        )
        .expect("config");
        assert_eq!(c.max_retries, Some(3));
        assert_eq!(c.virtual_hosted_style, Some(false));
        let err = s3_config("b", "", env_of(&[("SCION_S3_PROVIDER", "ceph")])).expect_err("unknown provider");
        assert!(format!("{err:#}").contains("SCION_S3_PROVIDER"));
    }

    /// 真实 S3 兼容服务（CI 中为 SeaweedFS 与 MinIO）：服务端用 SCION_S3_* 打开并通过启动检查。
    #[tokio::test]
    async fn open_s3_runs_startup_check() {
        let Ok(endpoint) = std::env::var("SCION_TEST_S3_ENDPOINT") else {
            eprintln!("SCION_TEST_S3_ENDPOINT not set; skipping");
            return;
        };
        let var = |k: &str| std::env::var(k).unwrap_or_default();
        let (key, secret, provider) = (
            var("SCION_TEST_S3_ACCESS_KEY"),
            var("SCION_TEST_S3_SECRET_KEY"),
            var("SCION_TEST_S3_PROVIDER"),
        );
        let pairs = [
            ("SCION_S3_ENDPOINT", endpoint.as_str()),
            ("SCION_S3_ACCESS_KEY_ID", key.as_str()),
            ("SCION_S3_SECRET_ACCESS_KEY", secret.as_str()),
            ("SCION_S3_PROVIDER", provider.as_str()),
        ];
        let env = env_of(&pairs);
        let bucket = std::env::var("SCION_TEST_S3_BUCKET").unwrap_or_else(|_| "scion-test".into());
        let spec = parse_blobs(&format!("s3://{bucket}/server-open-test")).expect("spec");
        open(&MetaSpec::Mem, &spec, &env).await.expect("open s3");
        let missing = parse_blobs("s3://scion-no-such-bucket/x").expect("spec");
        let err = open(&MetaSpec::Mem, &missing, &env)
            .await
            .err()
            .expect("missing bucket");
        assert!(format!("{err:#}").contains("does not exist"), "{err:#}");
    }

    #[test]
    fn describe_hides_credentials() {
        let d = describe(
            &MetaSpec::Url("postgres://u:secret@db:5432/scion".into()),
            &BlobSpec::Mem,
        );
        assert!(!d.contains("secret"));
        assert!(d.contains("db:5432/scion"));
        assert_eq!(
            describe(&MetaSpec::Url("postgres://db/scion".into()), &BlobSpec::Mem),
            "meta=postgres blobs=mem"
        );
        let d = describe(
            &MetaSpec::Mem,
            &BlobSpec::S3 {
                bucket: "b".into(),
                prefix: "p".into(),
            },
        );
        assert_eq!(d, "meta=mem blobs=s3://b/p");
        let (m, b) = lite(Path::new("/d"));
        assert_eq!(describe(&m, &b), "meta=sqlite:///d/meta.db blobs=/d/objects");
    }

    #[tokio::test]
    async fn open_creates_missing_lite_directories() {
        let d = tempfile::tempdir().expect("tempdir");
        let data = d.path().join("nested/data");
        let (meta, blobs) = lite(&data);
        open(&meta, &blobs, |_| None).await.expect("open");
        assert!(data.join("meta.db").exists() && data.join("objects").is_dir());
    }

    #[test]
    fn packs_switch() {
        assert!(packs(&|_: &str| None));
        for off in ["off", "0", "false"] {
            assert!(!packs(&|_: &str| Some(off.into())), "{off}");
        }
    }

    #[test]
    fn compression_switch() {
        assert!(compression(&|_: &str| None));
        assert!(compression(&|_: &str| Some("on".into())));
        for off in ["off", "0", "false"] {
            assert!(!compression(&|_: &str| Some(off.into())), "{off}");
        }
    }

    #[test]
    fn sqlite_parent_cases() {
        assert_eq!(sqlite_parent("sqlite:///var/scion/meta.db"), Some("/var/scion".into()));
        assert_eq!(sqlite_parent("sqlite://data/meta.db?mode=rwc"), Some("data".into()));
        assert_eq!(sqlite_parent("sqlite://meta.db"), None);
        assert_eq!(sqlite_parent("sqlite://:memory:"), None);
        assert_eq!(sqlite_parent("sqlite://"), None);
        assert_eq!(sqlite_parent("postgres://x/y"), None);
    }

    #[tokio::test]
    async fn open_mem_shares_one_store() {
        let (meta, blobs) = open(&MetaSpec::Mem, &BlobSpec::Mem, env_of(&[])).await.expect("open");
        meta.create_repo("r").await.expect("repo");
        let id = scion_object::ObjectId::of_blob(b"x");
        blobs.put(id, bytes::Bytes::from_static(b"x")).await.expect("put");
        assert!(blobs.exists(&id).await.expect("exists"));
    }

    #[tokio::test]
    async fn open_lite_persists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (m, b) = lite(dir.path());
        {
            let (meta, _blobs) = open(&m, &b, env_of(&[])).await.expect("open");
            meta.create_repo("r").await.expect("repo");
        }
        let (meta, _blobs) = open(&m, &b, env_of(&[])).await.expect("reopen");
        assert_eq!(meta.list_repos().await.expect("list"), ["r"]);
        assert!(dir.path().join("meta.db").exists());
        assert!(dir.path().join("objects").is_dir());
    }

    #[tokio::test]
    async fn open_errors_are_reported() {
        let bad_meta = MetaSpec::Url("postgres://[bad".into());
        assert!(open(&bad_meta, &BlobSpec::Mem, env_of(&[])).await.is_err());
        let file = tempfile::NamedTempFile::new().expect("file");
        let bad_dir = BlobSpec::Dir(file.path().join("sub"));
        assert!(open(&MetaSpec::Mem, &bad_dir, env_of(&[])).await.is_err());
        let s3 = BlobSpec::S3 {
            bucket: "b".into(),
            prefix: String::new(),
        };
        let env = env_of(&[
            ("SCION_S3_ENDPOINT", "http://127.0.0.1:1"),
            ("SCION_S3_MAX_RETRIES", "0"),
        ]);
        let err = open(&MetaSpec::Mem, &s3, env).await.err().expect("unreachable s3");
        assert!(format!("{err:#}").contains("s3 check"), "{err:#}");
        let bad_s3 = BlobSpec::S3 {
            bucket: " ".into(),
            prefix: String::new(),
        };
        assert!(open(&MetaSpec::Mem, &bad_s3, env_of(&[])).await.is_err());
    }
}
