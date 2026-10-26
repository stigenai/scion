//! `scion`：服务端单二进制，按子命令区分角色。
//!
//! 目前有 `server`、`cache`、`admin` 和 `version`；`deriver`、`janitor` 按里程碑逐个加入。

mod authcmd;
mod autorepack;
mod fsck;
mod opscmd;
mod repack;
mod storage;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use scion_api::{App, ServerConfig};

#[derive(Debug, Parser)]
#[command(name = "scion", version, about = "Scion source control server")]
struct Cli {
    /// Log format.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text, env = "SCION_LOG_FORMAT")]
    log_format: LogFormat,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

// 只在启动时解析一次，变体大小不影响性能
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server (L3).
    Server(ServerArgs),
    /// Run a regional object cache (L2).
    Cache(CacheArgs),
    /// Print the version and protocol version.
    Version,
    /// Operations commands.
    #[command(subcommand)]
    Admin(AdminCmd),
}

#[derive(Debug, Subcommand)]
enum AdminCmd {
    /// Build derived indexes for older data (parents and skip lists, PathHistory, GitSha). Safe to repeat.
    /// A running server caches commit records: run this before starting the server, or restart it afterwards.
    Reindex(ReindexArgs),
    /// Generate the ES256 key that signs access tokens (PKCS#8 PEM, on standard output).
    JwtKeygen,
    /// Manage PATs and service tokens.
    Token {
        #[command(subcommand)]
        cmd: authcmd::TokenCmd,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Organizations and their members, directly in the metadata store (docs/design/authorization.md).
    Org {
        #[command(subcommand)]
        cmd: authcmd::OrgCmd,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Show or set the access control policy.
    Policy {
        #[command(subcommand)]
        cmd: authcmd::PolicyCmd,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Redact file contents: ids and history stay, reads return REDACTED (docs/guide/redaction.md).
    Redact {
        #[command(flatten)]
        args: opscmd::RedactArgs,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Undo a redaction (before it is purged).
    Unredact {
        redaction_id: String,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Schedule redacted objects for deletion (the server deletes them after the grace period, or now with --now).
    Purge {
        #[command(flatten)]
        args: opscmd::PurgeArgs,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// List redactions.
    Redactions {
        #[arg(long)]
        repo: Option<String>,
        #[arg(long = "id", value_name = "REDACTION_ID")]
        redaction_id: Option<String>,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Show leases (land leaders and others).
    Lease {
        #[command(subcommand)]
        cmd: opscmd::LeaseCmd,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Check repository data: that refs, commits, trees and files reference only existing data, and the object index against object storage. Read-only.
    Fsck {
        /// Only check this repository (repeatable); all when omitted.
        #[arg(long = "repo", value_name = "NAME")]
        repos: Vec<String>,
        /// Read every file and verify its hash (by default only checks it exists).
        #[arg(long)]
        verify: bool,
        #[command(flatten)]
        storage: StorageArgs,
    },
    /// Rewrite objects into large packs, storing older versions as deltas against newer ones, and remove what that supersedes. Like `git gc`; safe while the server runs.
    Repack {
        /// Walk only this repository's history (repeatable); all when omitted.
        #[arg(long = "repo", value_name = "NAME")]
        repos: Vec<String>,
        /// Keep packs younger than this when removing superseded ones.
        #[arg(long, value_name = "DURATION", default_value = "1h", value_parser = parse_duration)]
        prune_after: std::time::Duration,
        /// Only count what would be repacked.
        #[arg(long)]
        dry_run: bool,
        #[command(flatten)]
        storage: StorageArgs,
    },
}

#[derive(Debug, clap::Args)]
struct ReindexArgs {
    /// Only this repository; all when omitted.
    #[arg(long = "repo", value_name = "NAME")]
    repo: Option<String>,
    #[command(flatten)]
    storage: StorageArgs,
}

/// Storage options, shared by server and admin.
#[derive(Debug, clap::Args)]
struct StorageArgs {
    /// Lite tier: SQLite and a local directory, one process, no other services. Needs --data-dir.
    #[arg(long, global = true, requires = "data_dir", conflicts_with_all = ["meta", "blobs"])]
    lite: bool,
    /// The Lite tier's data directory (meta.db and objects/).
    #[arg(long, global = true, value_name = "DIR", env = "SCION_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Metadata store: mem, sqlite://PATH, postgres://…
    #[arg(long, global = true, value_name = "URL", default_value = "mem", env = "SCION_META")]
    meta: String,
    /// Object storage: mem, a directory, or s3://bucket[/prefix] (connection settings in SCION_S3_* variables).
    #[arg(long, global = true, value_name = "SPEC", default_value = "mem", env = "SCION_BLOBS")]
    blobs: String,
}

impl StorageArgs {
    fn specs(&self) -> anyhow::Result<(storage::MetaSpec, storage::BlobSpec)> {
        Ok(match (&self.data_dir, self.lite) {
            (Some(dir), true) => storage::lite(dir),
            _ => (storage::parse_meta(&self.meta)?, storage::parse_blobs(&self.blobs)?),
        })
    }
}

#[derive(Debug, clap::Args)]
struct ServerArgs {
    /// gRPC listen address.
    #[arg(long, default_value = "127.0.0.1:7080", env = "SCION_LISTEN")]
    listen: SocketAddr,
    /// Serve without authentication on an address other people can reach. Without it, the server
    /// only starts without `--auth-required` on a loopback address.
    #[arg(long, env = "SCION_ALLOW_ANONYMOUS")]
    allow_anonymous: bool,
    /// Repository to create at startup if missing (repeatable).
    #[arg(long = "repo", value_name = "NAME")]
    repos: Vec<String>,
    /// Do not serve git clients: without it, git's smart HTTP protocol (clone and fetch) is served on
    /// the same address, at each repository's URL.
    #[arg(long, env = "SCION_NO_GIT")]
    no_git: bool,
    /// Cluster mode: this node's address for other nodes (such as http://10.0.0.5:7080). Nodes that share one
    /// metadata store and object storage land each ref on the node holding its lease; others forward to it.
    #[arg(long, value_name = "URL", env = "SCION_ADVERTISE_URL", conflicts_with = "lite")]
    advertise_url: Option<String>,
    /// Cluster secret (the same on every node), sent with forwarded lands so the receiving node does not count
    /// them against rate limits again. Without it, a forwarded land counts on both nodes.
    #[arg(long, value_name = "SECRET", env = "SCION_CLUSTER_SECRET", hide_env_values = true)]
    cluster_secret: Option<String>,
    /// Limit on requests in flight; beyond it requests are refused at once (clients retry).
    #[arg(
        long,
        value_name = "N",
        default_value_t = 10_000,
        env = "SCION_MAX_CONCURRENT_REQUESTS"
    )]
    max_concurrent_requests: usize,
    /// Write rate limit per repository: RATE per second, optional BURST (default 2×RATE), such as 200 or 200/1000.
    #[arg(long, value_name = "RATE[/BURST]", value_parser = parse_write_rate, env = "SCION_REPO_WRITE_RATE")]
    repo_write_rate: Option<scion_api::WriteRate>,
    /// Request rate limit per caller (the authenticated subject, else the source IP), every RPC counting once: RATE[/BURST].
    #[arg(long, value_name = "RATE[/BURST]", value_parser = parse_write_rate, env = "SCION_TENANT_RATE")]
    tenant_rate: Option<scion_api::WriteRate>,
    /// Limit on requests in flight per caller.
    #[arg(long, value_name = "N", env = "SCION_TENANT_CONCURRENCY")]
    tenant_concurrency: Option<usize>,
    /// Limit on request time (the smaller of this and the client's deadline; DEADLINE_EXCEEDED when exceeded); 0s uses only the client's deadline.
    #[arg(long, value_name = "DURATION", default_value = "60s", value_parser = parse_duration,
        env = "SCION_MAX_REQUEST_TIMEOUT")]
    max_request_timeout: std::time::Duration,
    /// Read endpoint to recommend to clients (a `scion cache` address), optionally with a region: URL or URL@REGION.
    /// Repeatable; clients pick where to read objects from.
    #[arg(long = "read-endpoint", value_name = "URL[@REGION]", value_parser = parse_read_endpoint,
        env = "SCION_READ_ENDPOINTS", value_delimiter = ',')]
    read_endpoints: Vec<scion_api::ReadEndpoint>,
    /// How often WatchRefs polls the metadata store (the most a land on another node waits to reach subscribers), such as 500ms or 2s.
    #[arg(long, value_name = "DURATION", default_value = "1s", value_parser = parse_duration,
        env = "SCION_REF_POLL_INTERVAL")]
    ref_poll_interval: std::time::Duration,
    /// How long deleted repositories are kept (restorable), before their data is purged and the name freed (at least 2h).
    #[arg(long, value_name = "DURATION", default_value = "24h", value_parser = parse_duration,
        env = "SCION_REPO_DELETE_GRACE")]
    repo_delete_grace: std::time::Duration,
    /// Default grace period before redacted objects are purged (when AdminService.Purge gives none); undo is possible until then.
    #[arg(long, value_name = "DURATION", default_value = "24h", value_parser = parse_duration,
        env = "SCION_PURGE_GRACE")]
    purge_grace: std::time::Duration,
    #[command(flatten)]
    auth: authcmd::AuthArgs,
    #[command(flatten)]
    obs: ObsArgs,
    #[command(flatten)]
    events: EventArgs,
    #[command(flatten)]
    storage: StorageArgs,
    #[command(flatten)]
    repack: RepackArgs,
}

/// Repacking on its own (docs/guide/object-storage.md#repacking).
#[derive(Debug, Clone, clap::Args)]
struct RepackArgs {
    /// Repack when this much was written to the object store since the last repack (such as after a
    /// large import); 0 turns it off.
    #[arg(long, value_name = "SIZE", default_value = "256MiB", value_parser = parse_size,
        env = "SCION_REPACK_AFTER")]
    repack_after: u64,
    /// Repack when this long has passed since the last repack and anything was written; 0s turns it off.
    #[arg(long, value_name = "DURATION", default_value = "24h", value_parser = parse_duration,
        env = "SCION_REPACK_EVERY")]
    repack_every: std::time::Duration,
    /// How often to check whether a repack is due.
    #[arg(long, value_name = "DURATION", default_value = "15m", value_parser = parse_duration,
        env = "SCION_REPACK_CHECK_INTERVAL")]
    repack_check_interval: std::time::Duration,
}

impl RepackArgs {
    fn schedule(&self) -> autorepack::Schedule {
        autorepack::Schedule {
            check: self.repack_check_interval.max(std::time::Duration::from_secs(1)),
            after_bytes: self.repack_after,
            every: self.repack_every,
        }
    }
}

/// Delivery of ref changes (docs/guide/events.md).
#[derive(Debug, Clone, clap::Args)]
struct EventArgs {
    /// POST ref changes to this URL (JSON, at least once).
    #[arg(long, value_name = "URL", env = "SCION_EVENT_WEBHOOK")]
    event_webhook: Option<String>,
    /// Webhook signing secret: X-Scion-Signature-256 is the HMAC-SHA256 of the body.
    #[arg(
        long,
        value_name = "SECRET",
        env = "SCION_EVENT_WEBHOOK_SECRET",
        hide_env_values = true
    )]
    event_webhook_secret: Option<String>,
    /// Publish ref changes to NATS JetStream (needs a stream covering <prefix>.ref.>).
    #[arg(long, value_name = "URL", env = "SCION_EVENT_NATS")]
    event_nats: Option<String>,
    /// NATS subject prefix: subjects are <prefix>.ref.created|updated|deleted.
    #[arg(
        long,
        value_name = "PREFIX",
        default_value = "scion",
        env = "SCION_EVENT_NATS_SUBJECT"
    )]
    event_nats_subject: String,
    /// Pre-land check: before landing, creating or moving a ref, POST the incoming commits here; it decides.
    #[arg(long, value_name = "URL", env = "SCION_PRE_LAND_WEBHOOK")]
    pre_land_webhook: Option<String>,
    /// Signing secret for the pre-land check (X-Scion-Signature-256).
    #[arg(long, value_name = "SECRET", env = "SCION_PRE_LAND_SECRET", hide_env_values = true)]
    pre_land_secret: Option<String>,
    /// Timeout for the pre-land check.
    #[arg(long, value_name = "DURATION", default_value = "5s", value_parser = parse_duration,
        env = "SCION_PRE_LAND_TIMEOUT")]
    pre_land_timeout: std::time::Duration,
    /// Allow the change when the check service is unavailable (timeout, 5xx, connection failure); refused by default.
    #[arg(long, env = "SCION_PRE_LAND_FAIL_OPEN")]
    pre_land_fail_open: bool,
    /// How long events are kept in the metadata store (bounds both ListRefEvents and delivery).
    #[arg(long, value_name = "DURATION", default_value = "7d", value_parser = parse_duration,
        env = "SCION_EVENT_RETENTION")]
    event_retention: std::time::Duration,
}

impl EventArgs {
    fn config(&self) -> anyhow::Result<scion_api::events::EventsConfig> {
        let webhook = match &self.event_webhook {
            Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
                Some(scion_api::events::WebhookConfig {
                    url: url.clone(),
                    secret: self.event_webhook_secret.clone().filter(|s| !s.is_empty()),
                    timeout: std::time::Duration::from_secs(10),
                })
            }
            Some(url) => anyhow::bail!("--event-webhook must be an http:// or https:// URL, got {url:?}"),
            None => None,
        };
        let nats = self.event_nats.as_ref().map(|url| scion_api::events::NatsConfig {
            url: url.clone(),
            subject_prefix: self.event_nats_subject.clone(),
        });
        Ok(scion_api::events::EventsConfig {
            webhook,
            nats,
            retention: self.event_retention,
        })
    }

    fn pre_land(&self) -> anyhow::Result<Option<scion_api::prehook::PreLandConfig>> {
        match &self.pre_land_webhook {
            Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
                Ok(Some(scion_api::prehook::PreLandConfig {
                    url: url.clone(),
                    secret: self.pre_land_secret.clone().filter(|s| !s.is_empty()),
                    timeout: self.pre_land_timeout,
                    fail_open: self.pre_land_fail_open,
                }))
            }
            Some(url) => anyhow::bail!("--pre-land-webhook must be an http:// or https:// URL, got {url:?}"),
            None => Ok(None),
        }
    }
}

/// Metrics and tracing (shared by `scion server` and `scion cache`).
#[derive(Debug, Clone, Default, clap::Args)]
struct ObsArgs {
    /// HTTP address for Prometheus metrics (GET /metrics), such as 0.0.0.0:9464; none when omitted.
    #[arg(long, value_name = "ADDR", env = "SCION_METRICS_LISTEN")]
    metrics_listen: Option<SocketAddr>,
    /// OTLP gRPC address (such as http://otel-collector:4317) to export traces to an OpenTelemetry collector.
    /// The standard OTEL_EXPORTER_OTLP_ENDPOINT variable works too.
    #[arg(long, value_name = "URL", env = "SCION_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,
}

#[derive(Debug, clap::Args)]
struct CacheArgs {
    /// gRPC listen address.
    #[arg(long, default_value = "127.0.0.1:7090", env = "SCION_CACHE_LISTEN")]
    listen: SocketAddr,
    /// Upstream server address (repeatable: nodes of one cluster, interchangeable).
    #[arg(
        long = "upstream",
        value_name = "URL",
        required = true,
        env = "SCION_CACHE_UPSTREAMS",
        value_delimiter = ','
    )]
    upstreams: Vec<String>,
    /// This node's address for other cache nodes; with --peer, objects are sharded by rendezvous hashing.
    #[arg(long, value_name = "URL", env = "SCION_CACHE_ADVERTISE_URL")]
    advertise_url: Option<String>,
    /// Another cache node in the region (repeatable).
    #[arg(
        long = "peer",
        value_name = "URL",
        env = "SCION_CACHE_PEERS",
        value_delimiter = ',',
        requires = "advertise_url"
    )]
    peers: Vec<String>,
    /// Memory tier size, such as 512MiB or 8GiB.
    #[arg(long, value_name = "SIZE", default_value = "1GiB", value_parser = parse_size, env = "SCION_CACHE_MEMORY")]
    memory: u64,
    /// Disk tier directory (NVMe recommended); memory only when omitted.
    #[arg(long, value_name = "DIR", env = "SCION_CACHE_DISK_DIR")]
    disk_dir: Option<PathBuf>,
    /// Disk tier size.
    #[arg(long, value_name = "SIZE", default_value = "64GiB", value_parser = parse_size, env = "SCION_CACHE_DISK_SIZE")]
    disk_size: u64,
    /// Objects larger than this are not cached (at most 16MiB).
    #[arg(long, value_name = "SIZE", default_value = "8MiB", value_parser = parse_size)]
    max_object: u64,
    /// Warm-up: follow upstream ref changes and prefetch changed trees. REPO or REPO:REF[,REF...], repeatable.
    #[arg(
        long = "warm",
        value_name = "REPO[:REFS]",
        env = "SCION_CACHE_WARM",
        value_delimiter = ';'
    )]
    warm: Vec<scion_cache::WarmTarget>,
    /// Also prefetch files up to this size while warming; 0 prefetches only trees.
    #[arg(long, value_name = "SIZE", default_value = "0", value_parser = parse_size)]
    warm_blob_max: u64,
    /// Turn off hedging (resending slow requests to another upstream).
    #[arg(long)]
    no_hedge: bool,
    /// Token for warming (when the server requires authentication); best passed in the environment.
    #[arg(long, value_name = "TOKEN", env = "SCION_CACHE_WARM_TOKEN", hide_env_values = true)]
    warm_token: Option<String>,
    #[command(flatten)]
    obs: ObsArgs,
}

/// A byte count: a number, optionally with K/M/G/T (KiB/MiB/…, powers of 1024).
fn parse_size(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid size {s:?}; expected e.g. 512MiB"))?;
    let shift = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 10,
        "m" | "mb" | "mib" => 20,
        "g" | "gb" | "gib" => 30,
        "t" | "tb" | "tib" => 40,
        _ => return Err(format!("invalid size unit in {s:?}; use K, M, G or T")),
    };
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| format!("size {s:?} is too large"))
}

/// A duration: a number with ms, s or m.
fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration {s:?}; expected e.g. 500ms or 2s"))?;
    match unit {
        "ms" => Ok(std::time::Duration::from_millis(n)),
        "s" => Ok(std::time::Duration::from_secs(n)),
        "m" => Ok(std::time::Duration::from_secs(n * 60)),
        "h" => Ok(std::time::Duration::from_secs(n * 3600)),
        "d" => Ok(std::time::Duration::from_secs(n * 86_400)),
        _ => Err(format!("invalid duration unit in {s:?}; use ms, s, m, h or d")),
    }
}

fn parse_read_endpoint(s: &str) -> Result<scion_api::ReadEndpoint, String> {
    let (url, region) = match s.rsplit_once('@') {
        Some((u, r)) => (u.trim(), r.trim()),
        None => (s.trim(), ""),
    };
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(format!("read endpoint must be an http:// or https:// URL, got {url:?}"));
    }
    Ok(scion_api::ReadEndpoint {
        url: url.trim_end_matches('/').to_owned(),
        region: region.to_owned(),
    })
}

fn cache_config(args: &CacheArgs) -> anyhow::Result<scion_cache::CacheConfig> {
    let size = |v: u64, what: &str| usize::try_from(v).with_context(|| format!("{what} is too large"));
    Ok(scion_cache::CacheConfig {
        upstreams: args.upstreams.clone(),
        advertise_url: args.advertise_url.clone(),
        peers: args.peers.clone(),
        storage: scion_cache::StorageConfig {
            memory_bytes: size(args.memory, "--memory")?,
            disk: match &args.disk_dir {
                Some(dir) => Some(scion_cache::DiskConfig {
                    dir: dir.clone(),
                    capacity_bytes: size(args.disk_size, "--disk-size")?,
                }),
                None => None,
            },
            max_object_bytes: size(args.max_object, "--max-object")?,
        },
        pool: scion_cache::PoolConfig {
            hedge: !args.no_hedge,
            ..scion_cache::PoolConfig::default()
        },
        warm: args.warm.clone(),
        warm_options: scion_cache::WarmOptions {
            token: match &args.warm_token {
                Some(t) => Some(
                    format!("Bearer {}", t.trim())
                        .parse()
                        .map_err(|_| anyhow::anyhow!("--warm-token contains invalid characters"))?,
                ),
                None => None,
            },
            max_blob_bytes: args.warm_blob_max,
            ..scion_cache::WarmOptions::default()
        },
        ..scion_cache::CacheConfig::default()
    })
}

fn parse_write_rate(s: &str) -> Result<scion_api::WriteRate, String> {
    let (rate, burst) = match s.split_once('/') {
        Some((r, b)) => (r, Some(b)),
        None => (s, None),
    };
    let rate: u32 = rate
        .trim()
        .parse()
        .ok()
        .filter(|r| *r > 0)
        .ok_or_else(|| format!("invalid rate {rate:?}; expected a positive integer"))?;
    let burst = match burst {
        Some(b) => b
            .trim()
            .parse()
            .ok()
            .filter(|b| *b > 0)
            .ok_or_else(|| format!("invalid burst {b:?}; expected a positive integer"))?,
        None => rate.saturating_mul(2),
    };
    Ok(scion_api::WriteRate {
        per_second: rate,
        burst,
    })
}

/// Sets up logging and tracing (`service` is the OpenTelemetry service.name). The returned guard exports the remaining spans on exit.
fn init_tracing(format: LogFormat, service: &str, obs: &ObsArgs) -> anyhow::Result<scion_obs::trace::TraceGuard> {
    scion_obs::trace::init(&scion_obs::trace::TraceConfig {
        service_name: service.to_owned(),
        otlp_endpoint: obs.otlp_endpoint.clone(),
        json: format == LogFormat::Json,
        filter_env: "SCION_LOG".to_owned(),
    })
    .map_err(|e| anyhow::anyhow!("initializing tracing: {e}"))
}

/// Serves `/metrics` (with `--metrics-listen`) until the process exits.
async fn start_metrics(obs: &ObsArgs) -> anyhow::Result<Option<SocketAddr>> {
    let Some(addr) = obs.metrics_listen else {
        return Ok(None);
    };
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind metrics listener {addr}"))?;
    let local = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(e) = scion_obs::http::serve_metrics(listener, std::future::pending()).await {
            tracing::error!(error = %e, "metrics endpoint stopped");
        }
    });
    tracing::info!(addr = %local, "metrics listening");
    Ok(Some(local))
}

fn version_text() -> String {
    format!(
        "scion {} (protocol {})",
        env!("CARGO_PKG_VERSION"),
        scion_proto_version()
    )
}

fn scion_proto_version() -> &'static str {
    // scion-api 依赖 scion-proto；这里通过 API 层暴露的常量避免二进制直接依赖生成代码
    scion_api::PROTOCOL_VERSION
}

/// Opens storage and creates the `--repo` repositories if missing.
#[cfg(test)]
async fn build_app(args: &ServerArgs, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<(App, String)> {
    let (app, backends, _) = build_app_with_objects(args, env).await?;
    Ok((app, backends))
}

/// [`build_app`], also returning the object store itself (none for `mem`), for repacking.
async fn build_app_with_objects(
    args: &ServerArgs,
    env: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<(App, String, Option<Arc<scion_store_blob::ObjectBlobStore>>)> {
    let cluster = match &args.advertise_url {
        Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
            Some(scion_land::ClusterConfig::new(url.trim_end_matches('/')))
        }
        Some(url) => anyhow::bail!("--advertise-url must be an http:// or https:// URL, got {url:?}"),
        None => None,
    };
    if cluster.is_some() && args.cluster_secret.as_deref().is_none_or(str::is_empty) {
        tracing::warn!(
            "cluster mode without --cluster-secret: forwarded lands count against rate limits on both nodes"
        );
    }
    if let Some(s) = &args.cluster_secret {
        if tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(s.as_str()).is_err() {
            anyhow::bail!("--cluster-secret must be printable ASCII");
        }
    }
    let auth = args.auth.guard_config()?;
    check_exposure(args.listen, auth.required, args.allow_anonymous)?;
    let (meta_spec, blob_spec) = args.storage.specs()?;
    let (meta, blobs, objects) = storage::open_objects(&meta_spec, &blob_spec, env).await?;
    // 提前校验认证配置（密钥、外部签发方的密钥来源）和库里的访问控制策略，有误时拒绝启动
    let guard =
        scion_api::scion_auth::Guard::build(auth.clone(), meta.clone(), scion_api::scion_auth::remote::env_lookup())
            .map_err(|e| anyhow::anyhow!("invalid auth configuration: {e}"))?;
    if auth.required {
        guard
            .reload_policy()
            .await
            .map_err(|e| anyhow::anyhow!("loading the access policy: {e}"))?;
    }
    if auth.required && auth.signing_key_pem.is_none() {
        tracing::warn!(
            "authentication required but no --jwt-key: access tokens cannot be issued (PATs and external tokens still work)"
        );
    }
    for repo in &args.repos {
        match meta.create_repo(repo).await {
            Ok(()) | Err(scion_store::Error::RepoAlreadyExists(_)) => {}
            // 通过接口删除、等待清除的仓库：不自动恢复
            Err(scion_store::Error::RepoDeleted(_)) => {
                tracing::warn!(%repo, "--repo names a deleted repository; restore it with `scn repo restore`");
            }
            Err(e) => return Err(e).with_context(|| format!("create repository {repo:?}")),
        }
    }
    let app = App {
        config: Arc::new(ServerConfig {
            cluster,
            max_concurrent_requests: args.max_concurrent_requests,
            repo_write_rate: args.repo_write_rate,
            tenant_rate: args.tenant_rate,
            cluster_secret: args.cluster_secret.clone().filter(|s| !s.is_empty()),
            max_concurrent_per_tenant: args.tenant_concurrency,
            max_request_timeout: Some(args.max_request_timeout).filter(|d| !d.is_zero()),
            read_endpoints: args.read_endpoints.clone(),
            auth,
            ref_poll_interval: args.ref_poll_interval,
            purge_grace: args.purge_grace,
            repo_delete_grace: args.repo_delete_grace,
            events: args.events.config()?,
            pre_land: args.events.pre_land()?,
            git: !args.no_git,
            ..ServerConfig::default()
        }),
        meta,
        blobs,
    };
    Ok((app, storage::describe(&meta_spec, &blob_spec), objects))
}

/// Opens only the metadata store (for admin commands).
async fn open_meta(storage: &StorageArgs) -> anyhow::Result<Arc<dyn scion_store::MetaStore>> {
    let (meta_spec, blob_spec) = storage.specs()?;
    let (meta, _) = storage::open(&meta_spec, &blob_spec, |k| std::env::var(k).ok()).await?;
    Ok(meta)
}

/// `scion admin reindex`: one summary line per repository.
async fn reindex(args: &ReindexArgs, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<Vec<String>> {
    let (meta_spec, blob_spec) = args.storage.specs()?;
    let (meta, blobs) = storage::open(&meta_spec, &blob_spec, env).await?;
    let repos = match &args.repo {
        Some(r) => vec![r.clone()],
        None => meta.list_repos().await?,
    };
    let mut out = Vec::with_capacity(repos.len());
    for repo in repos {
        let stats = scion_query::reindex(meta.as_ref(), blobs.as_ref(), &repo)
            .await
            .with_context(|| format!("reindex {repo:?}"))?;
        out.push(scion_query::describe(&stats, &repo));
    }
    Ok(out)
}

/// Registers SIGINT / SIGTERM right away (before printing "listening", so an early signal does not kill the process),
/// and returns a future that completes on a signal.
fn shutdown_signal() -> impl std::future::Future<Output = ()> {
    #[cfg(unix)]
    let term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    #[cfg(unix)]
    let int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    async move {
        #[cfg(unix)]
        {
            let wait = |s: Option<tokio::signal::unix::Signal>| async move {
                match s {
                    Some(mut s) => {
                        s.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                () = wait(term) => {},
                () = wait(int) => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        tracing::info!("shutdown signal received, draining");
    }
}

/// Refuses to serve without authentication where others can reach it, unless asked to: anyone who
/// can connect could read and write every repository.
fn check_exposure(listen: SocketAddr, auth_required: bool, allow_anonymous: bool) -> anyhow::Result<()> {
    if auth_required || listen.ip().is_loopback() {
        return Ok(());
    }
    if allow_anonymous {
        tracing::warn!(%listen, "serving without authentication: anyone who can connect can read and write every repository");
        return Ok(());
    }
    anyhow::bail!(
        "refusing to listen on {listen} without authentication: anyone who can connect could read and write every \
         repository. Pass --auth-required (see docs/guide/auth.md), or --allow-anonymous to serve without it on purpose"
    )
}

async fn run_server(args: ServerArgs) -> anyhow::Result<()> {
    let (app, backends, objects) = build_app_with_objects(&args, |k| std::env::var(k).ok()).await?;
    let schedule = args.repack.schedule();
    match objects {
        Some(objects) if schedule.enabled() && objects.packs_enabled() => {
            autorepack::spawn(app.meta.clone(), objects, schedule);
        }
        _ => {}
    }
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    let addr = listener.local_addr()?;
    if backends.contains("meta=mem") || backends.contains("blobs=mem") {
        tracing::warn!("in-memory storage in use; data is lost on exit");
    }
    start_metrics(&args.obs).await?;
    let shutdown = shutdown_signal();
    // 集成测试按这一行解析实际监听地址，修改时同步 tests/
    tracing::info!(%addr, repos = ?args.repos, %backends, "scion server listening");
    scion_api::serve(listener, app, shutdown).await?;
    tracing::info!("scion server stopped");
    Ok(())
}

async fn run_cache(args: CacheArgs) -> anyhow::Result<()> {
    let config = cache_config(&args)?;
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    let addr = listener.local_addr()?;
    let (node, info, channels) = scion_cache::build(&config).await?;
    start_metrics(&args.obs).await?;
    let shutdown = shutdown_signal();
    // 集成测试按这一行解析实际监听地址
    tracing::info!(%addr, upstreams = ?config.upstreams, peers = ?config.peers,
        disk = ?config.storage.disk.as_ref().map(|d| d.dir.display().to_string()), "scion cache listening");
    scion_cache::serve_node(listener, node, info, channels, &config, shutdown).await?;
    tracing::info!("scion cache stopped");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("{}", version_text());
            Ok(())
        }
        Command::Server(args) => {
            let _trace = init_tracing(cli.log_format, "scion-server", &args.obs)?;
            run_server(args).await
        }
        Command::Cache(args) => {
            let _trace = init_tracing(cli.log_format, "scion-cache", &args.obs)?;
            run_cache(args).await
        }
        Command::Admin(AdminCmd::Reindex(args)) => {
            for line in reindex(&args, |k| std::env::var(k).ok()).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Admin(AdminCmd::JwtKeygen) => {
            print!("{}", scion_api::scion_auth::token::generate_signing_key());
            Ok(())
        }
        Command::Admin(AdminCmd::Token { cmd, storage }) => {
            let meta = open_meta(&storage).await?;
            for line in authcmd::token(&cmd, meta).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Admin(AdminCmd::Org { cmd, storage }) => {
            let meta = open_meta(&storage).await?;
            for line in authcmd::org(&cmd, meta).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Admin(AdminCmd::Policy { cmd, storage }) => {
            let meta = open_meta(&storage).await?;
            for line in authcmd::policy(&cmd, meta).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Admin(cmd) => {
            let (lines, ok) = admin_ops(cmd, |k| std::env::var(k).ok()).await?;
            for line in lines {
                println!("{line}");
            }
            if ok {
                Ok(())
            } else {
                anyhow::bail!("fsck found problems")
            }
        }
    }
}

/// Operations admin commands: returns the output and whether it succeeded (false when fsck finds problems).
async fn admin_ops(cmd: AdminCmd, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<(Vec<String>, bool)> {
    let open = |storage: &StorageArgs| {
        let specs = storage.specs();
        let env = &env;
        async move {
            let (meta_spec, blob_spec) = specs?;
            storage::open(&meta_spec, &blob_spec, env).await
        }
    };
    Ok(match cmd {
        AdminCmd::Redact { args, storage } => {
            let (meta, blobs) = open(&storage).await?;
            (opscmd::redact(&args, meta, blobs, &env).await?, true)
        }
        AdminCmd::Unredact { redaction_id, storage } => {
            let (meta, blobs) = open(&storage).await?;
            (opscmd::unredact(&redaction_id, meta, blobs).await?, true)
        }
        AdminCmd::Purge { args, storage } => {
            let (meta, blobs) = open(&storage).await?;
            (opscmd::purge(&args, meta, blobs).await?, true)
        }
        AdminCmd::Redactions {
            repo,
            redaction_id,
            storage,
        } => {
            let (meta, blobs) = open(&storage).await?;
            (
                opscmd::redactions(repo.as_deref(), redaction_id.as_deref(), meta, blobs).await?,
                true,
            )
        }
        AdminCmd::Lease { cmd, storage } => {
            let (meta, _) = open(&storage).await?;
            (opscmd::lease(&cmd, meta).await?, true)
        }
        AdminCmd::Fsck { repos, verify, storage } => {
            let (meta, blobs) = open(&storage).await?;
            opscmd::run_fsck(&fsck::FsckOptions { repos, verify }, meta, blobs).await?
        }
        AdminCmd::Repack {
            repos,
            prune_after,
            dry_run,
            storage,
        } => {
            let (meta_spec, blob_spec) = storage.specs()?;
            let (meta, _, objects) = storage::open_objects(&meta_spec, &blob_spec, &env).await?;
            let objects = objects.ok_or_else(|| anyhow::anyhow!("repack needs a directory or s3:// object store"))?;
            let opts = repack::RepackOptions {
                repos,
                prune_after,
                dry_run,
            };
            (repack::repack(&meta, &objects, &opts).await?.lines(dry_run), true)
        }
        other => anyhow::bail!("not an operations command: {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parse_sizes_and_durations() {
        assert_eq!(parse_size("1024"), Ok(1024));
        assert_eq!(parse_size("512MiB"), Ok(512 << 20));
        assert_eq!(parse_size("8gib"), Ok(8 << 30));
        assert_eq!(parse_size("2T"), Ok(2 << 40));
        assert_eq!(parse_size(" 4 k "), Ok(4096));
        assert_eq!(parse_size("7b"), Ok(7));
        assert!(parse_size("MiB").is_err());
        assert!(parse_size("5 parsecs").is_err());
        assert!(parse_size("99999999999T").is_err());
        assert_eq!(parse_duration("500ms"), Ok(std::time::Duration::from_millis(500)));
        assert_eq!(parse_duration("2s"), Ok(std::time::Duration::from_secs(2)));
        assert_eq!(parse_duration("3m"), Ok(std::time::Duration::from_secs(180)));
        assert_eq!(parse_duration("2h"), Ok(std::time::Duration::from_secs(7200)));
        assert_eq!(parse_duration("1d"), Ok(std::time::Duration::from_secs(86_400)));
        assert!(parse_duration("2w").is_err());
        assert!(parse_duration("s").is_err());
    }

    #[test]
    fn parse_read_endpoints() {
        assert_eq!(
            parse_read_endpoint("http://cache:7090/@cn-beijing"),
            Ok(scion_api::ReadEndpoint {
                url: "http://cache:7090".into(),
                region: "cn-beijing".into()
            })
        );
        assert_eq!(parse_read_endpoint("https://c").map(|e| e.region), Ok(String::new()));
        assert!(parse_read_endpoint("cache:7090").is_err());
        let cli = Cli::try_parse_from([
            "scion",
            "server",
            "--read-endpoint",
            "http://a:7090@r1",
            "--read-endpoint",
            "http://b:7090",
            "--ref-poll-interval",
            "250ms",
        ])
        .expect("parse");
        let Command::Server(a) = cli.command else {
            panic!("server");
        };
        assert_eq!(a.read_endpoints.len(), 2);
        assert_eq!(a.ref_poll_interval, std::time::Duration::from_millis(250));
    }

    #[test]
    fn parse_cache_args() {
        let cli = Cli::try_parse_from([
            "scion",
            "cache",
            "--upstream",
            "http://l3a:7080",
            "--upstream",
            "http://l3b:7080",
            "--advertise-url",
            "http://c1:7090",
            "--peer",
            "http://c2:7090",
            "--memory",
            "256MiB",
            "--disk-dir",
            "/var/cache/scion",
            "--disk-size",
            "10GiB",
            "--warm",
            "mono:main,release",
            "--warm-blob-max",
            "64KiB",
            "--no-hedge",
        ])
        .expect("parse");
        let Command::Cache(a) = cli.command else {
            panic!("cache");
        };
        let c = cache_config(&a).expect("config");
        assert_eq!(c.upstreams.len(), 2);
        assert_eq!(c.peers, ["http://c2:7090"]);
        assert_eq!(c.storage.memory_bytes, 256 << 20);
        assert_eq!(c.storage.disk.as_ref().map(|d| d.capacity_bytes), Some(10 << 30));
        assert_eq!(c.storage.max_object_bytes, 8 << 20);
        assert_eq!(c.warm[0].refs, ["main", "release"]);
        assert_eq!(c.warm_options.max_blob_bytes, 64 << 10);
        assert!(!c.pool.hedge);
        // 默认值
        let cli = Cli::try_parse_from(["scion", "cache", "--upstream", "http://l3:7080"]).expect("parse");
        let Command::Cache(a) = cli.command else {
            panic!("cache");
        };
        let c = cache_config(&a).expect("config");
        assert_eq!(
            (c.storage.memory_bytes, c.storage.disk.is_none(), c.pool.hedge),
            (1 << 30, true, true)
        );
        assert_eq!(a.listen, "127.0.0.1:7090".parse().expect("addr"));
        // 必须有上游；--peer 需要 --advertise-url
        assert!(Cli::try_parse_from(["scion", "cache"]).is_err());
        assert!(Cli::try_parse_from(["scion", "cache", "--upstream", "http://l3", "--peer", "http://c2"]).is_err());
    }

    #[test]
    fn parse_server_defaults() {
        let cli = Cli::try_parse_from(["scion", "server"]).expect("parse");
        match cli.command {
            Command::Server(a) => {
                assert_eq!(a.listen, "127.0.0.1:7080".parse().expect("addr"));
                assert!(a.repos.is_empty());
            }
            _ => panic!("expected server"),
        }
        assert_eq!(cli.log_format, LogFormat::Text);
    }

    #[test]
    fn parse_server_repos_and_json_logs() {
        let cli = Cli::try_parse_from([
            "scion",
            "--log-format",
            "json",
            "server",
            "--repo",
            "a",
            "--repo",
            "b/c",
        ])
        .expect("parse");
        assert_eq!(cli.log_format, LogFormat::Json);
        match cli.command {
            Command::Server(a) => {
                assert_eq!(a.repos, ["a", "b/c"]);
                assert!(!a.no_git);
            }
            _ => panic!("expected server"),
        }
    }

    #[tokio::test]
    async fn no_git_turns_the_gateway_off() {
        let a = args(&["--no-git", "--meta", "mem", "--blobs", "mem"]);
        let (app, _) = build_app(&a, |_| None).await.expect("app");
        assert!(!app.config.git);
        let a = args(&["--meta", "mem", "--blobs", "mem"]);
        let (app, _) = build_app(&a, |_| None).await.expect("app");
        assert!(app.config.git);
    }

    #[test]
    fn parse_rejects_bad_listen() {
        assert!(Cli::try_parse_from(["scion", "server", "--listen", "nope"]).is_err());
    }

    #[test]
    fn version_text_mentions_protocol() {
        assert!(version_text().contains(scion_api::PROTOCOL_VERSION));
        assert!(version_text().starts_with("scion "));
    }

    #[test]
    fn repack_schedule_flags() {
        let s = args(&[]).repack.schedule();
        assert_eq!(
            (s.after_bytes, s.every, s.check),
            (
                256 << 20,
                std::time::Duration::from_secs(86_400),
                std::time::Duration::from_secs(900)
            )
        );
        let off = args(&[
            "--repack-after",
            "0",
            "--repack-every",
            "0s",
            "--repack-check-interval",
            "0s",
        ]);
        let s = off.repack.schedule();
        assert!(!s.enabled());
        assert_eq!(s.check, std::time::Duration::from_secs(1), "at least a second");
    }

    fn args(extra: &[&str]) -> ServerArgs {
        let mut argv = vec!["scion", "server"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).expect("parse").command {
            Command::Server(a) => a,
            _ => panic!("expected server"),
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn exposed_servers_need_authentication_or_an_explicit_opt_out() {
        let any: SocketAddr = "0.0.0.0:7080".parse().expect("addr");
        let local: SocketAddr = "127.0.0.1:7080".parse().expect("addr");
        let local6: SocketAddr = "[::1]:7080".parse().expect("addr");
        let err = check_exposure(any, false, false).expect_err("open to the network");
        assert!(err.to_string().contains("--auth-required"), "{err}");
        check_exposure(any, true, false).expect("authenticated");
        check_exposure(any, false, true).expect("anonymous on purpose");
        check_exposure(local, false, false).expect("loopback");
        check_exposure(local6, false, false).expect("loopback v6");
        // build_app applies it, and the flag parses from the command line and the environment
        let a = args(&[
            "--listen",
            "0.0.0.0:0",
            "--allow-anonymous",
            "--meta",
            "mem",
            "--blobs",
            "mem",
        ]);
        assert!(a.allow_anonymous);
    }

    #[tokio::test]
    async fn build_app_refuses_an_exposed_anonymous_server() {
        let a = args(&["--listen", "0.0.0.0:0", "--meta", "mem", "--blobs", "mem"]);
        let err = build_app(&a, no_env).await.err().expect("refused");
        assert!(err.to_string().contains("without authentication"), "{err}");
    }

    #[tokio::test]
    async fn build_app_creates_repos_idempotently() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        let a = args(&["--lite", "--data-dir", d, "--repo", "a", "--repo", "b"]);
        let (app, backends) = build_app(&a, no_env).await.expect("app");
        assert_eq!(app.meta.list_repos().await.expect("list"), ["a", "b"]);
        assert!(backends.contains("meta.db"));
        drop(app);
        // 重启时仓库已存在，不应失败
        let (app, _) = build_app(&a, no_env).await.expect("restart");
        assert_eq!(app.meta.list_repos().await.expect("list"), ["a", "b"]);
    }

    #[tokio::test]
    async fn build_app_cluster_mode_from_advertise_url() {
        let (app, _) = build_app(&args(&["--advertise-url", "http://10.0.0.5:7080/"]), no_env)
            .await
            .expect("app");
        assert_eq!(
            app.config.cluster.as_ref().map(|c| c.node.as_str()),
            Some("http://10.0.0.5:7080")
        );
        let (app, _) = build_app(&args(&[]), no_env).await.expect("app");
        assert!(app.config.cluster.is_none());
        assert!(
            build_app(&args(&["--advertise-url", "10.0.0.5:7080"]), no_env)
                .await
                .is_err()
        );
        let lite = Cli::try_parse_from([
            "scion",
            "server",
            "--lite",
            "--data-dir",
            "/d",
            "--advertise-url",
            "http://x",
        ]);
        assert!(lite.is_err(), "lite mode is single-node");
    }

    #[tokio::test]
    async fn build_app_refuses_an_unloadable_policy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        let (app, _) = build_app(&args(&["--lite", "--data-dir", d]), no_env)
            .await
            .expect("app");
        app.meta
            .put_setting(scion_api::scion_auth::POLICY_KEY, b"broken [")
            .await
            .expect("store");
        drop(app);
        let err = build_app(&args(&["--lite", "--data-dir", d, "--auth-required"]), no_env)
            .await
            .err()
            .expect("refused");
        assert!(err.to_string().contains("access policy"), "{err}");
        // 不要求认证时不加载策略
        build_app(&args(&["--lite", "--data-dir", d]), no_env)
            .await
            .expect("auth not required");
    }

    #[tokio::test]
    async fn admin_operations_through_the_cli() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        let (app, _) = build_app(&args(&["--lite", "--data-dir", d, "--repo", "r"]), no_env)
            .await
            .expect("app");
        drop(app);
        let run = |extra: &[&str]| {
            let mut argv = vec!["scion", "admin"];
            argv.extend_from_slice(extra);
            argv.extend_from_slice(&["--lite", "--data-dir", d]);
            let cli = Cli::try_parse_from(argv).expect("parse");
            let Command::Admin(cmd) = cli.command else {
                panic!("admin");
            };
            admin_ops(cmd, no_env)
        };
        let (lines, ok) = run(&["fsck", "--verify"]).await.expect("fsck");
        assert!(ok, "{lines:?}");
        let (lines, ok) = run(&["repack", "--dry-run"]).await.expect("repack");
        assert!(
            ok && lines.len() == 1 && lines[0].starts_with("1 repositories"),
            "{lines:?}"
        );
        let (lines, ok) = run(&["repack", "--prune-after", "0s"]).await.expect("repack");
        assert!(ok && lines.len() == 3, "{lines:?}");
        let (lines, ok) = run(&["fsck", "--verify"]).await.expect("fsck after repack");
        assert!(ok, "{lines:?}");
        let (lines, ok) = run(&["fsck", "--repo", "missing"]).await.expect("fsck");
        assert!(!ok && lines.iter().any(|l| l.contains("does not exist")));
        assert_eq!(run(&["lease", "ls"]).await.expect("lease"), (vec![], true));
        assert_eq!(run(&["redactions"]).await.expect("ls"), (vec![], true));
        assert!(run(&["unredact", "rd_nope"]).await.is_err());
        assert!(run(&["purge", "rd_nope", "--now"]).await.is_err());
        let id = scion_object::ObjectId::of_blob(b"never uploaded").to_string();
        assert!(
            run(&["redact", "--repo", "r", "--reason", "x", &id]).await.is_err(),
            "not in the repository"
        );
        assert!(Cli::try_parse_from(["scion", "admin", "purge", "rd", "--now", "--grace", "1h"]).is_err());
        // 不是运维命令
        assert!(admin_ops(AdminCmd::JwtKeygen, no_env).await.is_err());
        // Repacking needs an object store, not mem
        let cli = Cli::try_parse_from(["scion", "admin", "repack"]).expect("parse");
        let Command::Admin(cmd) = cli.command else {
            panic!("admin")
        };
        assert!(admin_ops(cmd, no_env).await.is_err());
    }

    #[test]
    fn write_rate_flag() {
        let r = |v| parse_write_rate(v).map(|w| (w.per_second, w.burst));
        assert_eq!(r("200"), Ok((200, 400)));
        assert_eq!(r("200/1000"), Ok((200, 1000)));
        assert_eq!(r(" 5 / 7 "), Ok((5, 7)));
        for bad in ["", "0", "-1", "x", "5/0", "5/x"] {
            assert!(r(bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn build_app_applies_overload_settings() {
        let (app, _) = build_app(
            &args(&["--max-concurrent-requests", "64", "--repo-write-rate", "10/20"]),
            no_env,
        )
        .await
        .expect("app");
        assert_eq!(app.config.max_concurrent_requests, 64);
        assert_eq!(
            app.config.repo_write_rate,
            Some(scion_api::WriteRate {
                per_second: 10,
                burst: 20
            })
        );
        let (app, _) = build_app(&args(&[]), no_env).await.expect("defaults");
        assert_eq!(
            (app.config.max_concurrent_requests, app.config.repo_write_rate),
            (10_000, None)
        );
        assert_eq!(app.config.tenant_rate, None);
        assert_eq!(app.config.max_concurrent_per_tenant, None);
        assert_eq!(app.config.max_request_timeout, Some(std::time::Duration::from_secs(60)));
        let (app, _) = build_app(
            &args(&[
                "--tenant-rate",
                "50",
                "--tenant-concurrency",
                "8",
                "--max-request-timeout",
                "0s",
            ]),
            no_env,
        )
        .await
        .expect("tenant limits");
        assert_eq!(
            app.config.tenant_rate,
            Some(scion_api::WriteRate {
                per_second: 50,
                burst: 100
            })
        );
        assert_eq!(app.config.max_concurrent_per_tenant, Some(8));
        assert_eq!(app.config.max_request_timeout, None, "0s means client deadlines only");
        assert_eq!(app.config.cluster_secret, None);
        assert_eq!(app.config.repo_delete_grace, std::time::Duration::from_secs(24 * 3600));
        assert_eq!(
            app.config.events.retention,
            std::time::Duration::from_secs(7 * 24 * 3600)
        );
        assert!(app.config.events.webhook.is_none() && app.config.events.nats.is_none());
        let (app, _) = build_app(
            &args(&["--advertise-url", "http://10.0.0.5:7080", "--cluster-secret", "s3cret"]),
            no_env,
        )
        .await
        .expect("cluster");
        assert_eq!(app.config.cluster_secret.as_deref(), Some("s3cret"));
        assert!(
            build_app(&args(&["--cluster-secret", "bad\nsecret"]), no_env)
                .await
                .is_err()
        );
        build_app(&args(&["--advertise-url", "http://10.0.0.5:7080"]), no_env)
            .await
            .expect("cluster without a secret only warns");
    }

    #[tokio::test]
    async fn event_flags() {
        let (app, _) = build_app(
            &args(&[
                "--event-webhook",
                "https://hooks.example.com/scion",
                "--event-webhook-secret",
                "s3cret",
                "--event-nats",
                "nats://nats:4222",
                "--event-nats-subject",
                "csghub.scion",
                "--event-retention",
                "1d",
            ]),
            no_env,
        )
        .await
        .expect("events");
        let e = &app.config.events;
        let w = e.webhook.as_ref().expect("webhook");
        assert_eq!(
            (w.url.as_str(), w.secret.as_deref()),
            ("https://hooks.example.com/scion", Some("s3cret"))
        );
        let n = e.nats.as_ref().expect("nats");
        assert_eq!(
            (n.url.as_str(), n.subject_prefix.as_str()),
            ("nats://nats:4222", "csghub.scion")
        );
        assert_eq!(e.retention, std::time::Duration::from_secs(86_400));
        assert!(build_app(&args(&["--event-webhook", "ftp://x"]), no_env).await.is_err());
        assert!(app.config.pre_land.is_none());
        let (app, _) = build_app(
            &args(&[
                "--pre-land-webhook",
                "https://check.example.com/scion",
                "--pre-land-secret",
                "k",
                "--pre-land-timeout",
                "2s",
                "--pre-land-fail-open",
            ]),
            no_env,
        )
        .await
        .expect("pre-land");
        let p = app.config.pre_land.as_ref().expect("configured");
        assert_eq!(
            (p.secret.as_deref(), p.timeout, p.fail_open),
            (Some("k"), std::time::Duration::from_secs(2), true)
        );
        assert!(
            build_app(&args(&["--pre-land-webhook", "check"]), no_env)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn observability_flags() {
        let cli = Cli::try_parse_from([
            "scion",
            "server",
            "--metrics-listen",
            "127.0.0.1:0",
            "--otlp-endpoint",
            "http://collector:4317",
        ])
        .expect("parse");
        let Command::Server(a) = cli.command else {
            panic!("server");
        };
        assert_eq!(a.obs.otlp_endpoint.as_deref(), Some("http://collector:4317"));
        let bound = start_metrics(&a.obs).await.expect("metrics").expect("bound");
        assert_ne!(bound.port(), 0);
        assert_eq!(start_metrics(&ObsArgs::default()).await.expect("off"), None);
        let busy = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let taken = ObsArgs {
            metrics_listen: Some(busy.local_addr().expect("addr")),
            ..ObsArgs::default()
        };
        assert!(start_metrics(&taken).await.is_err(), "address in use");
        let cli = Cli::try_parse_from([
            "scion",
            "cache",
            "--upstream",
            "http://u",
            "--metrics-listen",
            "0.0.0.0:9464",
        ])
        .expect("parse");
        assert!(matches!(cli.command, Command::Cache(ref c) if c.obs.metrics_listen.is_some()));
    }

    #[tokio::test]
    async fn build_app_rejects_invalid_repo_and_specs() {
        assert!(build_app(&args(&["--repo", "../x"]), no_env).await.is_err());
        assert!(build_app(&args(&["--meta", "mysql://x"]), no_env).await.is_err());
        assert!(build_app(&args(&["--blobs", "s3://"]), no_env).await.is_err());
    }

    #[tokio::test]
    async fn build_app_defaults_to_memory() {
        let (_, backends) = build_app(&args(&[]), no_env).await.expect("app");
        assert_eq!(backends, "meta=mem blobs=mem");
    }

    #[tokio::test]
    async fn startup_skips_deleted_repositories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        let flags = ["--lite", "--data-dir", d, "--repo", "mono", "--repo-delete-grace", "3h"];
        let (app, _) = build_app(&args(&flags), no_env).await.expect("first start");
        assert_eq!(app.config.repo_delete_grace, std::time::Duration::from_secs(3 * 3600));
        app.meta.delete_repo("mono", 1).await.expect("delete");
        drop(app);
        let (app, _) = build_app(&args(&flags), no_env)
            .await
            .expect("restart with a deleted --repo");
        assert!(!app.meta.repo_exists("mono").await.expect("exists"), "not resurrected");
    }

    #[tokio::test]
    async fn admin_reindex_runs_on_lite_storage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().to_str().expect("utf8");
        // 先用 server 的方式建两个仓库
        drop(
            build_app(
                &args(&["--lite", "--data-dir", d, "--repo", "a", "--repo", "b"]),
                no_env,
            )
            .await
            .expect("app"),
        );
        let parse = |extra: &[&str]| {
            let mut argv = vec!["scion", "admin", "reindex", "--lite", "--data-dir", d];
            argv.extend_from_slice(extra);
            match Cli::try_parse_from(argv).expect("parse").command {
                Command::Admin(AdminCmd::Reindex(r)) => r,
                _ => panic!("expected reindex"),
            }
        };
        let all = reindex(&parse(&[]), no_env).await.expect("all repos");
        assert_eq!(
            all,
            [
                "a: 0 changesets, 0 lineage repaired, 0 path entries, 0 git shas",
                "b: 0 changesets, 0 lineage repaired, 0 path entries, 0 git shas"
            ]
        );
        assert_eq!(reindex(&parse(&["--repo", "a"]), no_env).await.expect("one").len(), 1);
        assert!(reindex(&parse(&["--repo", "nope"]), no_env).await.is_err());
    }

    #[test]
    fn parse_lite_requires_data_dir_and_excludes_meta() {
        assert!(Cli::try_parse_from(["scion", "server", "--lite"]).is_err());
        assert!(Cli::try_parse_from(["scion", "server", "--lite", "--data-dir", "/d", "--meta", "mem"]).is_err());
        let a = args(&["--meta", "sqlite:///x.db", "--blobs", "/b"]);
        assert_eq!(
            (a.storage.meta.as_str(), a.storage.blobs.as_str()),
            ("sqlite:///x.db", "/b")
        );
    }
}
