//! 基于对象存储的 [`BlobStore`]（服务端设计文档 §4.5、§4.6）：
//!
//! - [`ObjectBlobStore::local`]：本地文件系统，Lite 档使用；
//! - [`ObjectBlobStore::s3`]：任意兼容 S3 的服务（AWS S3、MinIO、SeaweedFS、Ceph RGW、阿里云 OSS 等）。
//!
//! 对象按内容寻址、全局只存一份，key 为 `{prefix}objects/{kind}/{hh}/{hex}`。
//! 只使用 S3 的核心子集（PUT / GET / HEAD），不依赖条件写：同一 key 的内容永远相同，重复写入天然幂等。
//! 读取时校验 BLAKE3，磁盘或存储服务的静默损坏会以错误形式暴露，而不是把坏数据发给客户端。
//!
//! Objects are stored zstd-compressed when that saves at least 10%; already-compressed content
//! (model weights, images) is stored as it is. Reads recognise a zstd frame by its magic number
//! and keep the decompressed bytes only if they match the object id, so objects written before
//! compression, and raw content that happens to start with the zstd magic, read back unchanged.
//!
//! With a pack index ([`ObjectBlobStore::with_packs`]), objects written together through
//! [`BlobStore::put_many`] (a commit's trees, an upload batch) are stored in one pack (see
//! [`pack`]) and found through the pack locations in the metadata store. Small objects then cost a
//! range of one file instead of a file each. Large objects, and objects written on their own,
//! stay loose, and reads find both.
//!
//! 不同 S3 兼容服务（MinIO、SeaweedFS 等）的差异由 [`provider`] 适配：启动检查
//! [`ObjectBlobStore::check`] 识别服务、做对应的健康检查，并在 bucket 缺失时给出该服务的创建方法。

mod delta;
mod pack;
mod provider;

pub use provider::{Detected, S3Provider, detect};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use moka::sync::Cache;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, RetryConfig};
use scion_object::ObjectId;
use scion_store::{BlobStore, Error, MetaStore, PackLocation, PackedObject, Result};

use delta::Base;

/// S3 兼容存储的连接参数。
#[derive(Clone, Default, PartialEq, Eq)]
pub struct S3Config {
    /// 服务地址，如 `http://minio:9000`；访问 AWS S3 时留空。
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// 服务种类；`Auto` 时在 [`ObjectBlobStore::check`] 中识别。
    pub provider: S3Provider,
    /// 是否使用 virtual-hosted 风格（`bucket.host`）。`None` 按服务决定：只有 AWS 默认使用，
    /// MinIO、SeaweedFS 等自建服务使用 path 风格。
    pub virtual_hosted_style: Option<bool>,
    /// key 前缀，多个部署共用一个 bucket 时用来隔离，如 `scion/`。
    pub prefix: String,
    /// 单个请求的最大重试次数；`None` 使用默认值（10 次，指数退避）。
    pub max_retries: Option<usize>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("provider", &self.provider)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("virtual_hosted_style", &self.virtual_hosted_style)
            .field("prefix", &self.prefix)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

pub struct ObjectBlobStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    /// Compress objects on write (reads always understand both forms).
    compress: bool,
    /// S3 后端的服务信息；本地目录为 None。
    s3: Option<S3Target>,
    /// Where packed objects live; None stores every object loose.
    packs: Option<Packs>,
}

/// The pack index, and a cache of locations already looked up (packed objects never move, except
/// when a redaction purge rewrites their pack, which a read notices and looks up again).
struct Packs {
    index: Arc<dyn MetaStore>,
    locations: Cache<ObjectId, PackLocation>,
    /// Packs whose trailer has been read into `locations`: objects written together (a commit's
    /// trees) are usually read together, and then need no index lookup.
    learned: Cache<[u8; 32], ()>,
    /// Small packs held whole, so that reading their other objects needs no request either.
    bytes: Cache<[u8; 32], Bytes>,
    /// Objects recently used as delta bases: reading history walks the same chains.
    bases: Cache<BaseKey, Bytes>,
    /// Blocks of large packs: a commit's objects, and the versions of a delta chain, are mostly
    /// close together, so one block read serves many objects.
    blocks: Cache<([u8; 32], u64), Bytes>,
}

/// A delta base in the cache: by id, or by where it is in a pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BaseKey {
    Id(ObjectId),
    At([u8; 32], u64),
}

/// An object for [`ObjectBlobStore::repack`], and the object to store it as a delta against
/// (typically the next version of the same file or directory).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepackItem {
    pub id: ObjectId,
    pub base: Option<ObjectId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepackStats {
    /// Objects written to new packs.
    pub packed: usize,
    /// Of those, stored as deltas.
    pub deltas: usize,
    /// Objects too large to pack, left where they are.
    pub large: usize,
    /// Objects that could not be read.
    pub missing: usize,
    pub packs: usize,
    /// Bytes of the new packs.
    pub bytes: u64,
    /// The new packs' names.
    pub written: Vec<[u8; 32]>,
}

/// What a repack leaves behind for the next one (`repack/last`): when it started, when the last
/// full one did, and the packs it wrote, which later repacks need not fold in again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepackMarker {
    pub started: std::time::SystemTime,
    /// When the last full repack (not incremental) started.
    pub full: Option<std::time::SystemTime>,
    pub packs: std::collections::HashSet<[u8; 32]>,
}

impl RepackMarker {
    /// Whether a file last modified at `t` came before the repack this marker records began (so
    /// that repack saw it). Times in object storage are often whole seconds: the second the
    /// repack started counts as after.
    fn covers_before(&self, t: std::time::SystemTime) -> bool {
        let secs = |t: std::time::SystemTime| t.duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
        secs(t) >= secs(self.started)
    }

    fn encode(&self) -> String {
        let secs = |t: std::time::SystemTime| t.duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let mut out = format!("started {}\n", secs(self.started));
        if let Some(f) = self.full {
            out.push_str(&format!("full {}\n", secs(f)));
        }
        let mut packs: Vec<String> = self.packs.iter().map(hex::encode).collect();
        packs.sort();
        for p in packs {
            out.push_str(&format!("pack {p}\n"));
        }
        out
    }

    /// The marker's text; one that cannot be read (as the plain timestamps of earlier servers,
    /// which only made full repacks) counts as a full repack when the marker was written.
    fn parse(body: &[u8], written: std::time::SystemTime) -> Self {
        let at = |v: &str| {
            v.parse()
                .ok()
                .map(|s| std::time::UNIX_EPOCH + std::time::Duration::from_secs(s))
        };
        let mut m = Self {
            started: written,
            full: None,
            packs: Default::default(),
        };
        let mut started = false;
        for line in String::from_utf8_lossy(body).lines() {
            match line.split_once(' ') {
                Some(("started", v)) => {
                    if let Some(t) = at(v) {
                        m.started = t;
                        started = true;
                    }
                }
                Some(("full", v)) => m.full = at(v),
                Some(("pack", v)) => {
                    if let Ok(n) = <[u8; 32]>::try_from(hex::decode(v).unwrap_or_default()) {
                        m.packs.insert(n);
                    }
                }
                _ => {}
            }
        }
        if !started {
            m.full = Some(written);
        }
        m
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PruneStats {
    pub packs_removed: usize,
    pub loose_removed: usize,
    /// Stored bytes removed.
    pub bytes: u64,
}

/// What reading an object from a pack found.
enum Packed {
    Found(Bytes),
    /// The pack is gone: rewritten since the location was looked up.
    Gone,
    Damaged,
}

#[derive(Debug, Clone)]
struct S3Target {
    endpoint: Option<String>,
    bucket: String,
    provider: S3Provider,
}

/// 启动检查识别出的后端。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobBackend {
    Local,
    S3(Detected),
}

impl std::fmt::Display for BlobBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => f.write_str("local"),
            Self::S3(d) => match &d.server {
                Some(s) => write!(f, "s3 ({}, Server: {s})", d.provider),
                None => write!(f, "s3 ({})", d.provider),
            },
        }
    }
}

/// zstd frames start with these bytes.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
/// Smaller objects are not worth compressing.
const MIN_COMPRESS: usize = 128;
const ZSTD_LEVEL: i32 = 3;
/// Larger objects are (de)compressed on a blocking thread, off the async runtime.
const BLOCKING_ABOVE: usize = 1 << 20;
/// Larger objects are stored loose: they gain little from sharing a file, and are read whole.
const PACK_MAX_OBJECT: usize = 1 << 20;
/// A pack is closed once it holds this many bytes of objects.
const PACK_TARGET: usize = 16 << 20;
/// Pack locations kept in memory.
const LOCATION_CACHE: u64 = 1 << 20;
/// The first read of a pack fetches this much of its end: the whole pack when it is small, else
/// at least its trailer for up to ~1,300 objects.
const PACK_TAIL_READ: u64 = 64 << 10;
/// Bytes of whole small packs kept in memory.
const PACK_BYTES_CACHE: u64 = 64 << 20;
/// Bytes of delta bases kept in memory.
const BASE_CACHE: u64 = 64 << 20;
/// Large packs are read in blocks of this size, keeping this many bytes of blocks in memory.
/// Objects spanning more than a few blocks are read directly.
const PACK_BLOCK: u64 = 1 << 20;
const BLOCK_CACHE: u64 = 128 << 20;
const MAX_BLOCKS_PER_READ: u64 = 4;
/// Longest delta chain repack writes, and the longest a read follows (damaged data could loop).
const MAX_DELTA_DEPTH: u8 = 50;
const MAX_READ_DEPTH: u8 = 64;
/// Repack packs objects up to this size (larger ones stay loose), into packs of this size.
const REPACK_MAX_OBJECT: usize = 8 << 20;
const REPACK_TARGET: usize = 64 << 20;
/// Objects repack reads at the same time.
const REPACK_PARALLEL: usize = 32;
/// Loose objects written at the same time.
const PARALLEL_PUTS: usize = 16;

/// The bytes to store for `data`: a zstd frame when it saves at least 10%, else `data` itself.
fn encode(data: &Bytes) -> Bytes {
    if data.len() < MIN_COMPRESS {
        return data.clone();
    }
    match zstd::bulk::compress(data, ZSTD_LEVEL) {
        Ok(z) if z.len() < data.len() - data.len() / 10 => Bytes::from(z),
        _ => data.clone(),
    }
}

/// The object bytes from what was stored: the decompressed frame if it matches `id`, else the
/// stored bytes if they do (uncompressed objects). None when neither matches.
fn decode(id: &ObjectId, stored: Bytes) -> Option<Bytes> {
    if stored.starts_with(&ZSTD_MAGIC) {
        if let Ok(plain) = zstd::stream::decode_all(stored.as_ref()) {
            if id.verify(&plain) {
                return Some(Bytes::from(plain));
            }
        }
    }
    id.verify(&stored).then_some(stored)
}

async fn off_runtime<T: Send + 'static>(size: usize, f: impl FnOnce() -> T + Send + 'static) -> T {
    if size <= BLOCKING_ABOVE {
        return f();
    }
    match tokio::runtime::Handle::try_current() {
        Ok(h) => match h.spawn_blocking(f).await {
            Ok(v) => v,
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        },
        Err(_) => f(),
    }
}

impl ObjectBlobStore {
    /// Turns compression on write on or off (default on). Reads handle both forms either way.
    pub fn with_compression(mut self, on: bool) -> Self {
        self.compress = on;
        self
    }

    /// Stores objects written together in packs, with their locations in `index`.
    pub fn with_packs(mut self, index: Arc<dyn MetaStore>) -> Self {
        self.packs = Some(Packs {
            index,
            locations: Cache::new(LOCATION_CACHE),
            learned: Cache::new(LOCATION_CACHE / 16),
            bytes: Cache::builder()
                .max_capacity(PACK_BYTES_CACHE)
                .weigher(|_, b: &Bytes| u32::try_from(b.len()).unwrap_or(u32::MAX))
                .build(),
            bases: Cache::builder()
                .max_capacity(BASE_CACHE)
                .weigher(|_, b: &Bytes| u32::try_from(b.len()).unwrap_or(u32::MAX))
                .support_invalidation_closures()
                .build(),
            blocks: Cache::builder()
                .max_capacity(BLOCK_CACHE)
                .weigher(|_, b: &Bytes| u32::try_from(b.len()).unwrap_or(u32::MAX))
                .build(),
        });
        self
    }

    /// 本地目录（不存在则创建）。
    pub fn local(dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|e| Error::Backend(format!("create blob dir: {e}")))?;
        let fs = LocalFileSystem::new_with_prefix(dir).map_err(|e| backend("open blob dir", e))?;
        Ok(Self {
            store: Arc::new(fs),
            prefix: String::new(),
            compress: true,
            s3: None,
            packs: None,
        })
    }

    /// In memory, with the same compression and packing as the other backends: for tests and
    /// benchmarks.
    pub fn memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: String::new(),
            compress: true,
            s3: None,
            packs: None,
        }
    }

    /// S3 兼容存储。只构造客户端，不发起网络请求；用 [`ObjectBlobStore::self_test`] 检查连通性。
    pub fn s3(cfg: &S3Config) -> Result<Self> {
        let mut retry = RetryConfig::default();
        if let Some(n) = cfg.max_retries {
            retry.max_retries = n;
        }
        Self::s3_with_retry(cfg, retry)
    }

    fn s3_with_retry(cfg: &S3Config, retry: RetryConfig) -> Result<Self> {
        if cfg.bucket.trim().is_empty() {
            return Err(Error::Backend("s3 bucket must not be empty".into()));
        }
        if cfg.endpoint.is_none() && cfg.provider.requires_endpoint() {
            return Err(Error::Backend(format!(
                "s3 provider {} needs an endpoint (SCION_S3_ENDPOINT)",
                cfg.provider
            )));
        }
        // 没有 endpoint 只能是 AWS
        let provider = match (cfg.provider, &cfg.endpoint) {
            (S3Provider::Auto, None) => S3Provider::Aws,
            (p, _) => p,
        };
        let virtual_hosted = cfg
            .virtual_hosted_style
            .unwrap_or_else(|| provider.default_virtual_hosted());
        let mut b = AmazonS3Builder::new()
            .with_retry(retry)
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_access_key_id(&cfg.access_key_id)
            .with_secret_access_key(&cfg.secret_access_key)
            .with_virtual_hosted_style_request(virtual_hosted);
        if let Some(endpoint) = &cfg.endpoint {
            b = b
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        let store = b.build().map_err(|e| backend("configure s3", e))?;
        Ok(Self {
            store: Arc::new(store),
            prefix: normalize_prefix(&cfg.prefix),
            compress: true,
            s3: Some(S3Target {
                endpoint: cfg.endpoint.clone(),
                bucket: cfg.bucket.clone(),
                provider,
            }),
            packs: None,
        })
    }

    /// 启动检查：S3 后端先识别服务并做该服务的健康检查（见 [`provider`]），再做读写自检；
    /// bucket 不存在时报错并给出该服务的创建方法。
    pub async fn check(&self) -> Result<BlobBackend> {
        let Some(t) = &self.s3 else {
            self.self_test().await?;
            return Ok(BlobBackend::Local);
        };
        let detected = match &t.endpoint {
            Some(endpoint) => detect(endpoint, t.provider).await?,
            None => Detected {
                provider: t.provider,
                server: None,
            },
        };
        if !self.bucket_exists().await? {
            return Err(Error::Backend(format!(
                "s3 bucket {:?} does not exist; {}",
                t.bucket,
                detected.provider.create_bucket_hint(&t.bucket)
            )));
        }
        self.self_test().await?;
        Ok(BlobBackend::S3(detected))
    }

    /// 列一次前缀判断 bucket 是否存在。存在但为空时返回空列表；不存在时 MinIO 与 SeaweedFS 都返回
    /// 404 `NoSuchBucket`，object_store 把它包成通用错误，只能从错误链里认出来。
    async fn bucket_exists(&self) -> Result<bool> {
        let prefix = Path::from(format!("{}objects", self.prefix));
        match self.store.list_with_delimiter(Some(&prefix)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) if is_no_such_bucket(&e) => Ok(false),
            Err(e) => Err(backend("list bucket", e)),
        }
    }

    /// 启动自检：写入、读回、校验一个探测对象（服务端设计文档 §4.6 `blobstore-check`）。
    pub async fn self_test(&self) -> Result<()> {
        let probe = Bytes::from_static(b"scion blobstore self-test v1");
        let id = ObjectId::of_blob(&probe);
        self.put(id, probe.clone()).await?;
        match self.get(&id).await? {
            Some(got) if got == probe => Ok(()),
            Some(_) => Err(Error::Backend("blobstore self-test read back different bytes".into())),
            None => Err(Error::Backend(
                "blobstore self-test object not readable after write".into(),
            )),
        }
    }

    /// 读出已有对象并校验 BLAKE3。对象在读取期间消失时视为不完好（随后会被重新写入）。
    async fn intact(&self, key: &Path, id: &ObjectId) -> Result<bool> {
        let res = match self.store.get(key).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(e) => return Err(backend("get object", e)),
        };
        let data = res.bytes().await.map_err(|e| backend("read object", e))?;
        let id = *id;
        Ok(off_runtime(data.len(), move || decode(&id, data).is_some()).await)
    }

    fn key(&self, id: &ObjectId) -> Path {
        Path::from(format!("{}{}", self.prefix, object_key(id)))
    }

    fn pack_path(&self, name: &[u8; 32]) -> Path {
        Path::from(format!("{}{}", self.prefix, pack::pack_key(name)))
    }

    /// Where `id` is packed, from the cache or the index (`fresh` skips the cache).
    async fn location(&self, id: &ObjectId, fresh: bool) -> Result<Option<PackLocation>> {
        let Some(p) = &self.packs else { return Ok(None) };
        if !fresh {
            if let Some(l) = p.locations.get(id) {
                return Ok(Some(l));
            }
        }
        let found = p.index.pack_locations(&[*id]).await?.remove(id);
        match found {
            Some(l) => {
                p.locations.insert(*id, l);
                if !p.learned.contains_key(&l.pack) {
                    // Only an optimisation: the object's own location is already known
                    if let Err(e) = self.learn(p, &l.pack).await {
                        tracing::warn!(error = %e, "could not read a pack trailer");
                    }
                }
            }
            None => p.locations.invalidate(id),
        }
        Ok(found)
    }

    /// Reads a pack's trailer into the location cache, keeping the whole pack when it is small.
    async fn learn(&self, p: &Packs, name: &[u8; 32]) -> Result<()> {
        let path = self.pack_path(name);
        let suffix = |n: u64| object_store::GetOptions {
            range: Some(object_store::GetRange::Suffix(n)),
            ..Default::default()
        };
        let res = match self.store.get_opts(&path, suffix(PACK_TAIL_READ)).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(backend("get pack trailer", e)),
        };
        let (total, whole) = (res.meta.size, res.range.start == 0);
        let mut tail = res.bytes().await.map_err(|e| backend("read pack trailer", e))?;
        let bad = || Error::Backend(format!("pack {} has a damaged trailer", pack::pack_key(name)));
        let need = pack::trailer_len(&tail).ok_or_else(bad)?;
        if need > tail.len() {
            let res = self
                .store
                .get_opts(&path, suffix(need as u64))
                .await
                .map_err(|e| backend("get pack trailer", e))?;
            tail = res.bytes().await.map_err(|e| backend("read pack trailer", e))?;
        }
        let entries = pack::parse_tail(&tail, total).ok_or_else(bad)?;
        self.remember_pack(p, name, &entries, whole.then_some(tail));
        Ok(())
    }

    fn remember_pack(&self, p: &Packs, name: &[u8; 32], entries: &[PackedObject], whole: Option<Bytes>) {
        for o in entries {
            let loc = PackLocation {
                pack: *name,
                offset: o.offset,
                len: o.len,
            };
            p.locations.insert(o.id, loc);
        }
        if let Some(b) = whole.filter(|b| b.len() as u64 <= PACK_TAIL_READ) {
            p.bytes.insert(*name, b);
        }
        p.learned.insert(*name, ());
    }

    /// The object from its pack (held in memory, or a range read).
    /// The stored bytes at `loc`. None when the pack is gone.
    async fn stored_at(&self, loc: &PackLocation) -> Result<Option<Bytes>> {
        let (start, end) = (loc.offset, loc.offset + loc.len);
        let held = self.packs.as_ref().and_then(|p| p.bytes.get(&loc.pack));
        match held {
            Some(b) if end <= b.len() as u64 => Ok(Some(b.slice(start as usize..end as usize))),
            _ => self.pack_range(&loc.pack, start, end).await,
        }
    }

    async fn read_packed(&self, id: &ObjectId, loc: &PackLocation, depth: u8) -> Result<Packed> {
        let Some(stored) = self.stored_at(loc).await? else {
            return Ok(Packed::Gone);
        };
        let oid = *id;
        if let Some(base) = delta::base_of(&stored) {
            let found = self.undelta(id, loc, base, stored.clone(), depth).await?;
            if matches!(found, Packed::Found(_)) {
                return Ok(found);
            }
            // Whole content that happens to start like a delta
            let whole = off_runtime(stored.len(), move || decode(&oid, stored)).await;
            return Ok(whole.map_or(found, Packed::Found));
        }
        Ok(match off_runtime(stored.len(), move || decode(&oid, stored)).await {
            Some(data) => Packed::Found(data),
            None => Packed::Damaged,
        })
    }

    /// The object from a delta at `loc`: reads its base (which may be a delta too) and applies it.
    async fn undelta(&self, id: &ObjectId, loc: &PackLocation, base: Base, stored: Bytes, depth: u8) -> Result<Packed> {
        let Some(base_bytes) = self.base_bytes(loc, base, depth).await? else {
            tracing::error!(%id, ?base, "delta base is missing");
            return Ok(Packed::Damaged);
        };
        let oid = *id;
        let size = stored.len() + base_bytes.len();
        let data = off_runtime(size, move || {
            delta::decode(&stored, &base_bytes)
                .filter(|d| oid.verify(d))
                .map(Bytes::from)
        })
        .await;
        Ok(data.map_or(Packed::Damaged, Packed::Found))
    }

    /// The bytes of the base of a delta at `loc`, through the base cache.
    async fn base_bytes(&self, loc: &PackLocation, base: Base, depth: u8) -> Result<Option<Bytes>> {
        let Some(p) = &self.packs else { return Ok(None) };
        if depth >= MAX_READ_DEPTH {
            tracing::error!(?base, "delta chain too long");
            return Ok(None);
        }
        let key = match base {
            Base::Id(b) => BaseKey::Id(b),
            Base::At { offset, .. } => BaseKey::At(loc.pack, offset),
        };
        if let Some(b) = p.bases.get(&key) {
            return Ok(Some(b));
        }
        let got = match base {
            Base::Id(b) => self.get_at(&b, depth + 1).await?,
            Base::At { offset, len } => self.read_at(&loc.pack, offset, len, depth + 1).await?,
        };
        if let Some(b) = &got {
            p.bases.insert(key, b.clone());
        }
        Ok(got)
    }

    /// The object stored at `offset..offset + len` of a pack, which a delta in that pack names as
    /// its base. Its id is not known here: the object built on it is checked instead.
    fn read_at<'a>(
        &'a self,
        pack: &'a [u8; 32],
        offset: u64,
        len: u64,
        depth: u8,
    ) -> futures::future::BoxFuture<'a, Result<Option<Bytes>>> {
        Box::pin(async move {
            let loc = PackLocation {
                pack: *pack,
                offset,
                len,
            };
            let Some(stored) = self.stored_at(&loc).await? else {
                return Ok(None);
            };
            if let Some(base) = delta::base_of(&stored) {
                let Some(base_bytes) = self.base_bytes(&loc, base, depth).await? else {
                    return Ok(None);
                };
                let size = stored.len() + base_bytes.len();
                let data = off_runtime(size, move || delta::decode(&stored, &base_bytes).map(Bytes::from)).await;
                return Ok(data);
            }
            // Repack names a base by where it is only when its stored form is unambiguous
            if !stored.starts_with(&ZSTD_MAGIC) {
                return Ok(Some(stored));
            }
            let data = off_runtime(stored.len(), move || zstd::stream::decode_all(stored.as_ref()).ok()).await;
            Ok(data.map(Bytes::from))
        })
    }

    /// [`BlobStore::get`] at a depth of delta resolution.
    fn get_at<'a>(&'a self, id: &'a ObjectId, depth: u8) -> futures::future::BoxFuture<'a, Result<Option<Bytes>>> {
        Box::pin(async move {
            let Some(p) = &self.packs else {
                return self.get_loose(id).await;
            };
            // A cached location may be stale (the pack rewritten since): look it up again on any miss
            if let Some(loc) = p.locations.get(id) {
                if let Packed::Found(data) = self.read_packed(id, &loc, depth).await? {
                    return Ok(Some(data));
                }
            }
            if let Some(loc) = self.location(id, true).await? {
                match self.read_packed(id, &loc, depth).await? {
                    Packed::Found(data) => return Ok(Some(data)),
                    Packed::Damaged => return self.forget_damaged(p, id).await,
                    Packed::Gone => {}
                }
            }
            self.get_loose(id).await
        })
    }

    /// Bytes `start..end` of a pack, through the block cache. None when the pack is gone.
    async fn pack_range(&self, name: &[u8; 32], start: u64, end: u64) -> Result<Option<Bytes>> {
        let path = self.pack_path(name);
        let direct = |e: object_store::Error| match e {
            object_store::Error::NotFound { .. } => Ok(None),
            e => Err(backend("get packed object", e)),
        };
        // An empty object (an empty file stored as it is): object stores reject empty ranges
        if end <= start {
            return Ok(Some(Bytes::new()));
        }
        let Some(p) = &self.packs else {
            return self.store.get_range(&path, start..end).await.map(Some).or_else(direct);
        };
        let (first, last) = (start / PACK_BLOCK, (end - 1) / PACK_BLOCK);
        if last - first >= MAX_BLOCKS_PER_READ {
            return self.store.get_range(&path, start..end).await.map(Some).or_else(direct);
        }
        let mut out = bytes::BytesMut::with_capacity((end - start) as usize);
        for b in first..=last {
            let block = match p.blocks.get(&(*name, b)) {
                Some(x) => x,
                None => {
                    // The last block may be short: a bounded range past the end returns the rest
                    let opts = object_store::GetOptions {
                        range: Some(object_store::GetRange::Bounded(b * PACK_BLOCK..(b + 1) * PACK_BLOCK)),
                        ..Default::default()
                    };
                    let x = match self.store.get_opts(&path, opts).await {
                        Ok(r) => r.bytes().await.map_err(|e| backend("read pack block", e))?,
                        Err(e) => return direct(e),
                    };
                    p.blocks.insert((*name, b), x.clone());
                    x
                }
            };
            let lo = start.max(b * PACK_BLOCK) - b * PACK_BLOCK;
            let hi = end.min((b + 1) * PACK_BLOCK) - b * PACK_BLOCK;
            if hi as usize > block.len() {
                // Shorter than the location says: a damaged pack, or not the pack it names
                return Ok(Some(Bytes::new()));
            }
            out.extend_from_slice(&block[lo as usize..hi as usize]);
        }
        Ok(Some(out.freeze()))
    }

    /// Forgets a damaged packed copy, so that uploading the object again stores a good one.
    async fn forget_damaged(&self, p: &Packs, id: &ObjectId) -> Result<Option<Bytes>> {
        tracing::error!(%id, "packed object failed integrity check");
        p.locations.invalidate(id);
        p.index.delete_pack_locations(&[*id]).await?;
        Err(Error::Backend(format!("object {id} failed integrity check")))
    }

    async fn get_loose(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        let res = match self.store.get(&self.key(id)).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(backend("get object", e)),
        };
        let stored = res.bytes().await.map_err(|e| backend("read object", e))?;
        let oid = *id;
        match off_runtime(stored.len(), move || decode(&oid, stored)).await {
            Some(data) => Ok(Some(data)),
            None => {
                tracing::error!(%id, "blob failed integrity check");
                Err(Error::Backend(format!("object {id} failed integrity check")))
            }
        }
    }

    /// The bytes to store for `data`, compressed if that is on and worth it.
    async fn stored_form(&self, data: &Bytes) -> Bytes {
        if !self.compress {
            return data.clone();
        }
        let d = data.clone();
        off_runtime(data.len(), move || encode(&d)).await
    }

    /// Writes loose objects, several at a time.
    async fn put_loose_many(&self, objects: &[&(ObjectId, Bytes)]) -> Result<()> {
        let puts: Vec<_> = objects.iter().map(|(id, d)| self.put(*id, d.clone())).collect();
        let mut puts = futures::stream::iter(puts).buffer_unordered(PARALLEL_PUTS);
        while let Some(r) = puts.next().await {
            r?;
        }
        Ok(())
    }

    /// Writes `objects` (verified, not yet packed) as packs of up to [`PACK_TARGET`] bytes.
    async fn put_packed(&self, packs: &Packs, objects: &[&(ObjectId, Bytes)]) -> Result<()> {
        let mut builder = pack::PackBuilder::new();
        for (i, (id, data)) in objects.iter().enumerate() {
            let stored = self.stored_form(data).await;
            builder.add(*id, &stored);
            if builder.data_len() >= PACK_TARGET || i + 1 == objects.len() {
                let full = std::mem::replace(&mut builder, pack::PackBuilder::new());
                self.write_pack(packs, full).await?;
            }
        }
        Ok(())
    }

    /// Stores a pack, then records where its objects are (bytes before index).
    /// Returns the pack's size and name.
    async fn write_pack(&self, packs: &Packs, builder: pack::PackBuilder) -> Result<(u64, [u8; 32])> {
        let (bytes, name, placed) = builder.finish();
        let size = bytes.len() as u64;
        self.store
            .put(&self.pack_path(&name), PutPayload::from_bytes(bytes.clone()))
            .await
            .map_err(|e| backend("put pack", e))?;
        packs.index.put_pack_locations(&name, &placed).await?;
        // Objects just written are likely read soon (rebase at land, clients fetching)
        self.remember_pack(packs, &name, &placed, Some(bytes));
        Ok((size, name))
    }

    /// Rewrites `items`, in order, into new packs, storing each as a delta against its base when
    /// the base comes earlier in `items`, the chain stays short and the delta is smaller. New
    /// locations replace the old ones; [`ObjectBlobStore::prune`] then removes what they superseded.
    ///
    /// Safe while the server runs: every object stays readable throughout. Bases should be the
    /// newer versions (so the versions read most are stored whole), and come before the objects
    /// stored against them.
    pub async fn repack(&self, items: &[RepackItem]) -> Result<RepackStats> {
        let packs = self
            .packs
            .as_ref()
            .ok_or_else(|| Error::Backend("repacking needs packs (SCION_BLOB_PACKS)".into()))?;
        let mut stats = RepackStats::default();
        // Chain length of each object packed so far (0: stored whole)
        let mut depth: std::collections::HashMap<ObjectId, u8> = std::collections::HashMap::new();
        // Objects stored as they are whose bytes start like a zstd frame or a delta: deltas name
        // them by id, since a base read by where it is cannot be told apart by its id
        let mut ambiguous = std::collections::HashSet::new();
        let raw: Cache<ObjectId, Bytes> = Cache::builder()
            .max_capacity(256 << 20)
            .weigher(|_, b: &Bytes| u32::try_from(b.len()).unwrap_or(u32::MAX))
            .build();
        let reads: Vec<_> = items
            .iter()
            .map(|it| async move { (it, self.get(&it.id).await) })
            .collect();
        let mut reads = futures::stream::iter(reads).buffered(REPACK_PARALLEL);
        let mut builder = pack::PackBuilder::new();
        while let Some((item, data)) = reads.next().await {
            let Some(data) = data? else {
                stats.missing += 1;
                continue;
            };
            if data.len() > REPACK_MAX_OBJECT || depth.contains_key(&item.id) {
                stats.large += usize::from(data.len() > REPACK_MAX_OBJECT);
                continue;
            }
            let mut stored = self.stored_form(&data).await;
            let mut d = 0;
            let base = item.base.and_then(|b| Some((b, *depth.get(&b)?)));
            if let Some((b, bd)) = base.filter(|(_, bd)| *bd < MAX_DELTA_DEPTH) {
                let base_bytes = match raw.get(&b) {
                    Some(x) => Some(x),
                    None => self.get(&b).await?,
                };
                if let Some(base_bytes) = base_bytes {
                    let (data2, whole) = (data.clone(), stored.len());
                    // By where it is when the base is in this pack and reads back unambiguously
                    let name = builder
                        .place(&b)
                        .filter(|_| !ambiguous.contains(&b))
                        .unwrap_or(Base::Id(b));
                    let encoded = off_runtime(data.len() + base_bytes.len(), move || {
                        delta::encode(name, &base_bytes, &data2, whole)
                    })
                    .await;
                    if let Some(e) = encoded {
                        stored = Bytes::from(e);
                        d = bd + 1;
                        stats.deltas += 1;
                    }
                }
            }
            if d == 0 && stored == data && (stored.starts_with(&ZSTD_MAGIC) || delta::base_of(&stored).is_some()) {
                ambiguous.insert(item.id);
            }
            depth.insert(item.id, d);
            raw.insert(item.id, data);
            builder.add(item.id, &stored);
            stats.packed += 1;
            if builder.data_len() >= REPACK_TARGET {
                let full = std::mem::replace(&mut builder, pack::PackBuilder::new());
                let (bytes, name) = self.write_pack(packs, full).await?;
                stats.bytes += bytes;
                stats.written.push(name);
                stats.packs += 1;
            }
        }
        if !builder.is_empty() {
            let (bytes, name) = self.write_pack(packs, builder).await?;
            stats.bytes += bytes;
            stats.written.push(name);
            stats.packs += 1;
        }
        Ok(stats)
    }

    /// Removes packs none of whose objects live there any more, and loose objects that are also
    /// packed. Packs younger than `min_age` are kept: a writer may not have recorded their
    /// locations yet.
    pub async fn prune(&self, min_age: std::time::Duration) -> Result<PruneStats> {
        let packs = self
            .packs
            .as_ref()
            .ok_or_else(|| Error::Backend("pruning needs packs (SCION_BLOB_PACKS)".into()))?;
        let mut stats = PruneStats::default();
        let cutoff = std::time::SystemTime::now() - min_age;
        let listed = self.list(&format!("{}packs/", self.prefix)).await?;
        let mut removed = Vec::new();
        for meta in listed {
            let Some(name) = pack_name(meta.location.as_ref()) else {
                continue;
            };
            if std::time::SystemTime::from(meta.last_modified) > cutoff {
                continue;
            }
            let entries = match self.read_trailer(&name).await? {
                Some(e) => e,
                None => {
                    tracing::warn!(pack = %meta.location, "pack with a damaged trailer left in place");
                    continue;
                }
            };
            let ids: Vec<ObjectId> = entries.iter().map(|e| e.id).collect();
            let mut live = false;
            for chunk in ids.chunks(500) {
                let current = packs.index.pack_locations(chunk).await?;
                if current.values().any(|l| l.pack == name) {
                    live = true;
                    break;
                }
            }
            if live {
                continue;
            }
            match self.store.delete(&meta.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(backend("delete pack", e)),
            }
            packs.bytes.invalidate(&name);
            packs.learned.invalidate(&name);
            removed.push(name);
            stats.packs_removed += 1;
            stats.bytes += meta.size;
        }
        packs.index.delete_packs(&removed).await?;
        let loose = self.list(&format!("{}objects/", self.prefix)).await?;
        for chunk in loose.chunks(500) {
            let ids: Vec<(ObjectId, &object_store::ObjectMeta)> = chunk
                .iter()
                .filter_map(|m| Some((loose_id(m.location.as_ref())?, m)))
                .collect();
            let only: Vec<ObjectId> = ids.iter().map(|(id, _)| *id).collect();
            let packed = packs.index.pack_locations(&only).await?;
            for (id, m) in ids.iter().filter(|(id, _)| packed.contains_key(id)) {
                match self.store.delete(&m.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(backend("delete object", e)),
                }
                tracing::debug!(%id, "removed loose copy of a packed object");
                stats.loose_removed += 1;
                stats.bytes += m.size;
            }
        }
        Ok(stats)
    }

    /// Whether objects are written into packs (which repacking needs).
    pub fn packs_enabled(&self) -> bool {
        self.packs.is_some()
    }

    /// What the last repack left ([`ObjectBlobStore::mark_repacked`]); None before the first.
    pub async fn last_repack(&self) -> Result<Option<RepackMarker>> {
        let got = match self.store.get(&self.repack_marker()).await {
            Ok(g) => g,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(backend("read repack marker", e)),
        };
        let written = std::time::SystemTime::from(got.meta.last_modified);
        let body = got.bytes().await.map_err(|e| backend("read repack marker", e))?;
        Ok(Some(RepackMarker::parse(&body, written)))
    }

    /// Records what a repack that started at `marker.started` left.
    pub async fn mark_repacked(&self, marker: &RepackMarker) -> Result<()> {
        self.store
            .put(&self.repack_marker(), PutPayload::from(marker.encode().into_bytes()))
            .await
            .map(drop)
            .map_err(|e| backend("write repack marker", e))
    }

    /// Packs and loose objects written since the last repack started (all of them for None),
    /// leaving out the packs it wrote: what the next repack would fold in.
    /// Each with whether it is a pack (else a loose object).
    async fn written_after(&self, marker: Option<&RepackMarker>) -> Result<Vec<(bool, object_store::ObjectMeta)>> {
        let mut out = Vec::new();
        for (dir, pack) in [("packs/", true), ("objects/", false)] {
            for m in self.list(&format!("{}{dir}", self.prefix)).await? {
                let theirs = pack
                    && pack_name(m.location.as_ref()).is_some_and(|n| marker.is_some_and(|k| k.packs.contains(&n)));
                if !theirs && marker.is_none_or(|k| k.covers_before(m.last_modified.into())) {
                    out.push((pack, m));
                }
            }
        }
        Ok(out)
    }

    /// Bytes of what was written since the last repack ([`ObjectBlobStore::new_objects`]'s files).
    pub async fn written_since(&self, marker: Option<&RepackMarker>) -> Result<u64> {
        Ok(self.written_after(marker).await?.iter().map(|(_, m)| m.size).sum())
    }

    /// The objects written since the last repack, for an incremental one: those of the packs and
    /// loose objects [`ObjectBlobStore::written_since`] counts.
    pub async fn new_objects(&self, marker: Option<&RepackMarker>) -> Result<Vec<ObjectId>> {
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (pack, m) in self.written_after(marker).await? {
            let key = m.location.as_ref();
            if let Some(name) = pack_name(key).filter(|_| pack) {
                for e in self.read_trailer(&name).await?.unwrap_or_default() {
                    if seen.insert(e.id) {
                        out.push(e.id);
                    }
                }
            } else if let Some(id) = loose_id(key).filter(|_| !pack) {
                if seen.insert(id) {
                    out.push(id);
                }
            }
        }
        Ok(out)
    }

    fn repack_marker(&self) -> Path {
        Path::from(format!("{}repack/last", self.prefix))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<object_store::ObjectMeta>> {
        let prefix = Path::from(prefix);
        let mut out = Vec::new();
        let mut s = self.store.list(Some(&prefix));
        while let Some(m) = s.next().await {
            match m {
                Ok(m) => out.push(m),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(backend("list objects", e)),
            }
        }
        Ok(out)
    }

    /// The entries of a pack from its trailer; None when the trailer is damaged.
    async fn read_trailer(&self, name: &[u8; 32]) -> Result<Option<Vec<PackedObject>>> {
        let path = self.pack_path(name);
        let suffix = |n: u64| object_store::GetOptions {
            range: Some(object_store::GetRange::Suffix(n)),
            ..Default::default()
        };
        let res = match self.store.get_opts(&path, suffix(PACK_TAIL_READ)).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(Some(Vec::new())),
            Err(e) => return Err(backend("get pack trailer", e)),
        };
        let total = res.meta.size;
        let mut tail = res.bytes().await.map_err(|e| backend("read pack trailer", e))?;
        let Some(need) = pack::trailer_len(&tail) else {
            return Ok(None);
        };
        if need > tail.len() {
            let res = self
                .store
                .get_opts(&path, suffix(need as u64))
                .await
                .map_err(|e| backend("get pack trailer", e))?;
            tail = res.bytes().await.map_err(|e| backend("read pack trailer", e))?;
        }
        Ok(pack::parse_tail(&tail, total))
    }

    /// Removes a packed object by rewriting its pack without it. Objects whose location already
    /// points elsewhere are left out of the new pack.
    async fn delete_packed(&self, packs: &Packs, id: &ObjectId, loc: &PackLocation) -> Result<()> {
        // Objects stored as deltas against this one are rewritten whole while it is still readable
        let dependents = packs.index.delta_dependents(id).await?;
        if !dependents.is_empty() {
            let mut builder = pack::PackBuilder::new();
            for d in &dependents {
                let data = self
                    .get(d)
                    .await?
                    .ok_or_else(|| Error::Backend(format!("delta {d} on {id} is missing")))?;
                builder.add(*d, &self.stored_form(&data).await);
            }
            self.write_pack(packs, builder).await?;
            packs.bases.invalidate(&BaseKey::Id(*id));
        }
        let old = self.pack_path(&loc.pack);
        let bytes = match self.store.get(&old).await {
            Ok(r) => r.bytes().await.map_err(|e| backend("read pack", e))?,
            Err(object_store::Error::NotFound { .. }) => {
                packs.locations.invalidate(id);
                return packs.index.delete_pack_locations(&[*id]).await;
            }
            Err(e) => return Err(backend("get pack", e)),
        };
        let entries = pack::parse(&bytes).ok_or_else(|| Error::Backend(format!("pack holding {id} is damaged")))?;
        let others: Vec<ObjectId> = entries.iter().map(|e| e.id).filter(|o| o != id).collect();
        let current = packs.index.pack_locations(&others).await?;
        let at: HashMap<(u64, u64), ObjectId> = entries.iter().map(|e| ((e.offset, e.len), e.id)).collect();
        let mut builder = pack::PackBuilder::new();
        for e in entries
            .iter()
            .filter(|e| current.get(&e.id).is_some_and(|l| l.pack == loc.pack))
        {
            let stored = &bytes[e.offset as usize..(e.offset + e.len) as usize];
            // Deltas naming their base by where it is follow it to its new place, or name it by id
            if let Some(Base::At { offset, len }) = delta::base_of(stored) {
                let base = at
                    .get(&(offset, len))
                    .ok_or_else(|| Error::Backend(format!("pack holding {id} is damaged")))?;
                let name = builder.place(base).unwrap_or(Base::Id(*base));
                let moved = delta::rebase(stored, name).ok_or_else(|| Error::Backend("bad delta".into()))?;
                builder.add(e.id, &moved);
                continue;
            }
            builder.add(e.id, stored);
        }
        if !builder.is_empty() {
            self.write_pack(packs, builder).await?;
        }
        packs.locations.invalidate(id);
        packs.index.delete_pack_locations(&[*id]).await?;
        packs.bytes.invalidate(&loc.pack);
        packs.learned.invalidate(&loc.pack);
        let gone = loc.pack;
        // Bases cached by where they were in the old pack, the purged object among them
        if let Err(e) = packs
            .bases
            .invalidate_entries_if(move |k, _| matches!(k, BaseKey::At(p, _) if *p == gone))
        {
            tracing::warn!(error = %e, "could not drop the purged pack's cached bases");
        }
        for b in 0..=(bytes.len() as u64 / PACK_BLOCK) {
            packs.blocks.invalidate(&(loc.pack, b));
        }
        match self.store.delete(&old).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(backend("delete pack", e)),
        }
    }
}

/// The pack name in a pack's key (its last segment).
fn pack_name(key: &str) -> Option<[u8; 32]> {
    let hex = key.rsplit('/').next()?;
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// The object id in a loose object's key (`…objects/{kind}/{hh}/{hex}`).
fn loose_id(key: &str) -> Option<ObjectId> {
    let mut parts = key.rsplit('/');
    let digest = parts.next()?;
    let _hh = parts.next()?;
    let kind_algo = parts.next()?;
    format!("{kind_algo}{digest}").parse().ok()
}

/// 对象在存储中的相对 key：`objects/{kind}/{hh}/{hex}`。
/// 以哈希开头的两位做目录，让对象均匀分散到 S3 分区和本地目录。
fn object_key(id: &ObjectId) -> String {
    let hex = id.to_string();
    // 文本形式为 `<kind><algo><hex>`，如 b1af13…
    let (kind_algo, digest) = hex.split_at(2);
    format!("objects/{kind_algo}/{}/{digest}", &digest[..2])
}

/// 错误链中是否有 S3 错误码 `NoSuchBucket`。
fn is_no_such_bucket(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if err.to_string().contains("NoSuchBucket") {
            return true;
        }
        cur = err.source();
    }
    false
}

/// 规范化前缀：去掉首尾的 `/`，非空时补一个结尾的 `/`。
fn normalize_prefix(p: &str) -> String {
    let p = p.trim_matches('/');
    if p.is_empty() { String::new() } else { format!("{p}/") }
}

fn backend(context: &'static str, e: object_store::Error) -> Error {
    tracing::warn!(error = %e, context, "blobstore error");
    Error::Backend(format!("{context}: {}", short(&e)))
}

/// 错误摘要：只保留类别，不含 URL、密钥等细节。
fn short(e: &object_store::Error) -> &'static str {
    match e {
        object_store::Error::NotFound { .. } => "not found",
        object_store::Error::PermissionDenied { .. } => "permission denied",
        object_store::Error::Unauthenticated { .. } => "unauthenticated",
        object_store::Error::Precondition { .. } => "precondition failed",
        object_store::Error::NotSupported { .. } => "not supported",
        object_store::Error::InvalidPath { .. } => "invalid path",
        _ => "storage request failed",
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
        if !id.verify(&data) {
            return Err(Error::Backend(format!(
                "refusing to store bytes that do not match {id}"
            )));
        }
        let key = self.key(&id);
        let stored = if self.compress {
            let d = data.clone();
            off_runtime(data.len(), move || encode(&d)).await
        } else {
            data.clone()
        };
        // 已存在且完好就不再上传。已存在但大小不符或内容校验失败（截断、位翻转）时覆盖写入，
        // 否则损坏的对象永远无法被重新上传修复，跨仓库去重还会让新仓库引用到坏数据。
        // An existing copy may be compressed or not (written before compression, or by a store
        // with compression off); either is fine if intact.
        let sizes = [stored.len() as u64, data.len() as u64];
        match self.store.head(&key).await {
            Ok(meta) if sizes.contains(&meta.size) && self.intact(&key, &id).await? => return Ok(()),
            Ok(_) => tracing::warn!(%id, "existing blob is damaged; overwriting with verified bytes"),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(backend("head object", e)),
        }
        self.store
            .put(&key, PutPayload::from_bytes(stored))
            .await
            .map_err(|e| backend("put object", e))?;
        Ok(())
    }

    async fn put_many(&self, objects: &[(ObjectId, Bytes)]) -> Result<()> {
        if let Some((id, _)) = objects.iter().find(|(id, d)| !id.verify(d)) {
            return Err(Error::Backend(format!(
                "refusing to store bytes that do not match {id}"
            )));
        }
        let (small, large): (Vec<_>, Vec<_>) = objects.iter().partition(|(_, d)| d.len() <= PACK_MAX_OBJECT);
        self.put_loose_many(&large).await?;
        let Some(packs) = &self.packs else {
            return self.put_loose_many(&small).await;
        };
        let mut seen = std::collections::HashSet::new();
        let small: Vec<_> = small.into_iter().filter(|(id, _)| seen.insert(*id)).collect();
        let ids: Vec<ObjectId> = small.iter().map(|(id, _)| *id).collect();
        let known = packs.index.pack_locations(&ids).await?;
        let new: Vec<_> = small.into_iter().filter(|(id, _)| !known.contains_key(id)).collect();
        // A pack of one saves nothing and costs an index row
        match new.len() {
            0 => Ok(()),
            1 => self.put_loose_many(&new).await,
            _ => self.put_packed(packs, &new).await,
        }
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        self.get_at(id, 0).await
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool> {
        if self.location(id, false).await?.is_some() {
            return Ok(true);
        }
        match self.store.head(&self.key(id)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(backend("head object", e)),
        }
    }

    async fn delete(&self, id: &ObjectId) -> Result<()> {
        if let Some(packs) = &self.packs {
            if let Some(loc) = self.location(id, true).await? {
                self.delete_packed(packs, id, &loc).await?;
            }
        }
        match self.store.delete(&self.key(id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(backend("delete object", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::testsuite;

    #[test]
    fn repack_marker_round_trips_and_reads_old_ones() {
        let at = |s| std::time::UNIX_EPOCH + std::time::Duration::from_secs(s);
        let m = RepackMarker {
            started: at(100),
            full: Some(at(50)),
            packs: [[1u8; 32], [2u8; 32]].into_iter().collect(),
        };
        assert_eq!(RepackMarker::parse(m.encode().as_bytes(), at(999)), m);
        let incremental_first = RepackMarker {
            full: None,
            ..m.clone()
        };
        assert_eq!(
            RepackMarker::parse(incremental_first.encode().as_bytes(), at(999)),
            incremental_first
        );
        // Earlier servers wrote only a timestamp, after full repacks
        let old = RepackMarker::parse(b"1700000000", at(200));
        assert_eq!((old.started, old.full, old.packs.len()), (at(200), Some(at(200)), 0));
        assert!(m.covers_before(at(100)) && m.covers_before(at(101)) && !m.covers_before(at(99)));
    }

    #[test]
    fn object_key_layout() {
        let id = ObjectId::of_blob(b"");
        assert_eq!(
            object_key(&id),
            "objects/b1/af/af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        let t = ObjectId::hash(scion_object::ObjectKind::Tree, b"");
        assert!(object_key(&t).starts_with("objects/t1/af/"));
    }

    #[test]
    fn normalize_prefix_cases() {
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("/"), "");
        assert_eq!(normalize_prefix("scion"), "scion/");
        assert_eq!(normalize_prefix("/a/b/"), "a/b/");
    }

    #[test]
    fn s3_config_debug_redacts_secret() {
        let cfg = S3Config {
            secret_access_key: "hunter2".into(),
            ..Default::default()
        };
        assert!(!format!("{cfg:?}").contains("hunter2"));
    }

    #[test]
    fn no_such_bucket_found_anywhere_in_chain() {
        #[derive(Debug)]
        struct Wrap(&'static str, Option<Box<Wrap>>);
        impl std::fmt::Display for Wrap {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Wrap {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|w| w as _)
            }
        }
        let deep = Wrap("list failed", Some(Box::new(Wrap("<Code>NoSuchBucket</Code>", None))));
        assert!(is_no_such_bucket(&deep));
        assert!(!is_no_such_bucket(&Wrap(
            "list failed",
            Some(Box::new(Wrap("AccessDenied", None)))
        )));
    }

    #[test]
    fn short_error_categories() {
        let nf = object_store::Error::NotFound {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&nf), "not found");
        let pd = object_store::Error::PermissionDenied {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&pd), "permission denied");
        let ua = object_store::Error::Unauthenticated {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&ua), "unauthenticated");
        let pc = object_store::Error::Precondition {
            path: "p".into(),
            source: "x".into(),
        };
        assert_eq!(short(&pc), "precondition failed");
        let ns = object_store::Error::NotSupported { source: "x".into() };
        assert_eq!(short(&ns), "not supported");
        let g = object_store::Error::Generic {
            store: "s",
            source: "secret-url".into(),
        };
        assert_eq!(short(&g), "storage request failed");
        assert_eq!(backend("ctx", g), Error::Backend("ctx: storage request failed".into()));
    }

    #[tokio::test]
    async fn local_conformance_and_self_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path().join("blobs")).expect("open");
        testsuite::blob_store(&s).await;
        s.self_test().await.expect("self test");
    }

    fn packed(dir: &std::path::Path) -> (ObjectBlobStore, Arc<scion_store::MemStore>) {
        let index = Arc::new(scion_store::MemStore::new());
        let s = ObjectBlobStore::local(dir).expect("open").with_packs(index.clone());
        (s, index)
    }

    /// Files under `dir/sub`, recursively.
    fn files(dir: &std::path::Path, sub: &str) -> Vec<std::path::PathBuf> {
        fn walk(p: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for e in std::fs::read_dir(p).into_iter().flatten().flatten() {
                if e.path().is_dir() {
                    walk(&e.path(), out);
                } else {
                    out.push(e.path());
                }
            }
        }
        let mut out = Vec::new();
        walk(&dir.join(sub), &mut out);
        out
    }

    fn objects(n: usize, tag: &str) -> Vec<(ObjectId, Bytes)> {
        (0..n)
            .map(|i| {
                let d = Bytes::from([format!("{tag} {i}\n").as_bytes(), &text(i * 3)].concat());
                (ObjectId::of_blob(&d), d)
            })
            .collect()
    }

    #[tokio::test]
    async fn packed_conformance_and_self_test() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, _) = packed(dir.path());
        testsuite::blob_store(&s).await;
        s.self_test().await.expect("self test");
        let off = ObjectBlobStore::local(dir.path().join("plain"))
            .expect("open")
            .with_compression(false)
            .with_packs(Arc::new(scion_store::MemStore::new()));
        testsuite::blob_store(&off).await;
    }

    #[tokio::test]
    async fn objects_written_together_share_a_pack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let mut objs = objects(50, "small");
        let big = noise(PACK_MAX_OBJECT + 1);
        objs.push((ObjectId::of_blob(&big), big.clone()));
        s.put_many(&objs).await.expect("put many");
        assert_eq!(files(dir.path(), "packs").len(), 1, "one pack for the small objects");
        assert_eq!(files(dir.path(), "objects").len(), 1, "the large object stays loose");
        let ids: Vec<ObjectId> = objs.iter().map(|(id, _)| *id).collect();
        assert_eq!(index.pack_locations(&ids).await.expect("locations").len(), 50);
        // A fresh store (another server) finds them through the index
        let other = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        for (id, d) in &objs {
            assert_eq!(other.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
        // Writing them again adds nothing; one new object on its own stays loose
        let mut again = objs[..10].to_vec();
        again.extend(objects(1, "lonely"));
        s.put_many(&again).await.expect("again");
        assert_eq!(files(dir.path(), "packs").len(), 1);
        assert_eq!(files(dir.path(), "objects").len(), 2);
        // Loose objects written before packs are still read
        let plain = ObjectBlobStore::local(dir.path()).expect("open");
        let old = text(500);
        let old_id = ObjectId::of_blob(&old);
        plain.put(old_id, old.clone()).await.expect("loose");
        assert_eq!(s.get(&old_id).await, Ok(Some(old)));
        assert_eq!(s.exists(&old_id).await, Ok(true));
    }

    #[tokio::test]
    async fn large_batches_split_into_several_packs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, _) = packed(dir.path());
        let objs: Vec<(ObjectId, Bytes)> = (0..20u8)
            .map(|i| {
                let mut d = noise(PACK_MAX_OBJECT).to_vec();
                d[0] = i;
                let d = Bytes::from(d);
                (ObjectId::of_blob(&d), d)
            })
            .collect();
        s.put_many(&objs).await.expect("put many");
        assert_eq!(files(dir.path(), "packs").len(), 2, "20 MiB in packs of 16 MiB");
        for (id, d) in &objs {
            assert_eq!(s.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
    }

    #[tokio::test]
    async fn damaged_packed_object_fails_and_is_repaired_by_rewriting() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let objs = objects(5, "fragile");
        s.put_many(&objs).await.expect("put many");
        let pack = files(dir.path(), "packs").remove(0);
        let (id, data) = &objs[2];
        let loc = index.pack_locations(&[*id]).await.expect("loc")[id];
        let mut bytes = std::fs::read(&pack).expect("read pack");
        bytes[loc.offset as usize] ^= 0xff;
        std::fs::write(&pack, &bytes).expect("damage");
        // `s` holds the pack it wrote in memory; another server reads the damaged copy
        let s = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        assert!(matches!(s.get(id).await, Err(Error::Backend(m)) if m.contains("integrity")));
        assert!(
            index.pack_locations(&[*id]).await.expect("loc").is_empty(),
            "damaged copy forgotten"
        );
        assert_eq!(s.get(id).await, Ok(None));
        s.put_many(&objs[1..3]).await.expect("upload again");
        assert_eq!(s.get(id).await.as_ref(), Ok(&Some(data.clone())));
    }

    #[tokio::test]
    async fn deleting_a_packed_object_rewrites_its_pack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        // `reader` has looked up every location before the pack is rewritten. The pack is too
        // large to be held in memory, so reads go to storage.
        let reader = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        let objs: Vec<(ObjectId, Bytes)> = (0..6)
            .map(|i| {
                let d = Bytes::from([format!("secret {i}\n").as_bytes(), &noise(20_000)].concat());
                (ObjectId::of_blob(&d), d)
            })
            .collect();
        s.put_many(&objs).await.expect("put many");
        for (id, _) in &objs {
            assert!(reader.exists(id).await.expect("exists"));
        }
        let old_pack = files(dir.path(), "packs");
        s.delete(&objs[0].0).await.expect("delete");
        let new_pack = files(dir.path(), "packs");
        assert_eq!(new_pack.len(), 1);
        assert_ne!(old_pack, new_pack, "the pack is rewritten");
        let rest = std::fs::read(&new_pack[0]).expect("read");
        assert!(!rest.windows(objs[0].1.len()).any(|w| w == objs[0].1), "bytes are gone");
        assert_eq!(reader.get(&objs[0].0).await, Ok(None));
        for (id, d) in &objs[1..] {
            assert_eq!(
                reader.get(id).await.as_ref(),
                Ok(&Some(d.clone())),
                "stale locations looked up again"
            );
        }
        // Deleting every other object leaves no pack behind
        for (id, _) in &objs[1..] {
            s.delete(id).await.expect("delete");
        }
        assert!(files(dir.path(), "packs").is_empty());
        // Objects already moved to another pack are not copied again
        let objs = objects(4, "moved");
        s.put_many(&objs).await.expect("put");
        let p = files(dir.path(), "packs");
        index
            .put_pack_locations(
                &[9; 32],
                &[scion_store::PackedObject {
                    id: objs[1].0,
                    offset: 0,
                    len: 1,
                    base: None,
                }],
            )
            .await
            .expect("move");
        s.delete(&objs[0].0).await.expect("delete");
        let rewritten = std::fs::read(
            files(dir.path(), "packs")
                .into_iter()
                .find(|f| !p.contains(f))
                .expect("new"),
        )
        .expect("read");
        assert_eq!(pack::parse(&rewritten).expect("pack").len(), 2);
        // A pack that is already gone, or damaged
        let objs = objects(3, "gone");
        s.put_many(&objs).await.expect("put");
        let loc = index.pack_locations(&[objs[0].0]).await.expect("loc")[&objs[0].0];
        std::fs::write(dir.path().join(pack::pack_key(&loc.pack)), b"junk").expect("damage");
        assert!(matches!(s.delete(&objs[0].0).await, Err(Error::Backend(m)) if m.contains("damaged")));
        std::fs::remove_file(dir.path().join(pack::pack_key(&loc.pack))).expect("remove");
        s.delete(&objs[0].0).await.expect("delete from a missing pack");
        assert!(index.pack_locations(&[objs[0].0]).await.expect("loc").is_empty());
    }

    #[tokio::test]
    async fn the_first_read_of_a_pack_learns_its_other_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let small = objects(5, "small pack");
        let many = objects(1_500, "large trailer");
        let large: Vec<(ObjectId, Bytes)> = (0..5)
            .map(|i| {
                let d = Bytes::from([format!("large pack {i}\n").as_bytes(), &noise(30_000)].concat());
                (ObjectId::of_blob(&d), d)
            })
            .collect();
        for objs in [&small, &many, &large] {
            s.put_many(objs).await.expect("put many");
        }
        let packs = files(dir.path(), "packs");
        let reader = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        for objs in [&small, &many, &large] {
            assert_eq!(reader.get(&objs[0].0).await.as_ref(), Ok(&Some(objs[0].1.clone())));
        }
        // The other objects no longer need the index
        let ids: Vec<ObjectId> = [&small, &many, &large]
            .iter()
            .flat_map(|o| o.iter().map(|(id, _)| *id))
            .collect();
        index.delete_pack_locations(&ids).await.expect("forget");
        for objs in [&small, &many, &large] {
            assert_eq!(reader.get(&objs[3].0).await.as_ref(), Ok(&Some(objs[3].1.clone())));
        }
        // ... and the small pack is held whole
        for p in &packs {
            std::fs::remove_file(p).expect("remove");
        }
        assert_eq!(reader.get(&small[4].0).await.as_ref(), Ok(&Some(small[4].1.clone())));
        assert_eq!(
            reader.get(&large[4].0).await,
            Ok(Some(large[4].1.clone())),
            "large packs: the blocks already read are kept"
        );
        // A damaged trailer: the object is still read through its own location
        let objs = objects(3, "bad trailer");
        s.put_many(&objs).await.expect("put");
        let loc = index.pack_locations(&[objs[0].0]).await.expect("loc")[&objs[0].0];
        let path = dir.path().join(pack::pack_key(&loc.pack));
        let mut bytes = std::fs::read(&path).expect("read");
        let n = bytes.len();
        bytes[n - 1] = b'X';
        std::fs::write(&path, &bytes).expect("damage");
        let fresh = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        assert_eq!(fresh.get(&objs[1].0).await.as_ref(), Ok(&Some(objs[1].1.clone())));
    }

    /// 21 versions of a file, newest first, each a small change of the next.
    fn versions() -> Vec<(ObjectId, Bytes)> {
        (0..21)
            .rev()
            .map(|v| {
                let d: String = (0..400)
                    .map(|i| format!("line {i}: {}\n", if i % 21 == v { v + 1 } else { 0 }))
                    .collect();
                let d = Bytes::from(d);
                (ObjectId::of_blob(&d), d)
            })
            .collect()
    }

    /// Newest first, each stored against the next newer one.
    fn chain(vs: &[(ObjectId, Bytes)]) -> Vec<RepackItem> {
        vs.iter()
            .enumerate()
            .map(|(i, (id, _))| RepackItem {
                id: *id,
                base: i.checked_sub(1).map(|j| vs[j].0),
            })
            .collect()
    }

    /// The stored form of every object in the packs under `dir`.
    fn stored_objects(dir: &std::path::Path) -> Vec<(ObjectId, Vec<u8>)> {
        let mut out = Vec::new();
        for f in files(dir, "packs") {
            let bytes = std::fs::read(&f).expect("read pack");
            for e in pack::parse(&bytes).expect("pack") {
                out.push((e.id, bytes[e.offset as usize..(e.offset + e.len) as usize].to_vec()));
            }
        }
        out
    }

    /// Repack names bases in the same pack by where they are, which saves 31 bytes a delta, and
    /// by id when their stored bytes could be mistaken for a zstd frame or a delta.
    #[tokio::test]
    async fn repack_names_bases_by_where_they_are() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let vs = versions();
        // Incompressible content that starts like a zstd frame or a delta, in two versions each
        let mut tricky = Vec::new();
        let mut by_id = b"SCDL".to_vec();
        by_id.extend_from_slice(&vs[0].0.to_bytes());
        for prefix in [&ZSTD_MAGIC[..], b"SCDP\x05\x05", &by_id] {
            let mut newer = prefix.to_vec();
            newer.extend_from_slice(&noise(3_000));
            let mut older = newer.clone();
            older[2_000] ^= 1;
            for d in [newer, older] {
                let d = Bytes::from(d);
                tricky.push((ObjectId::of_blob(&d), d));
            }
        }
        let all: Vec<(ObjectId, Bytes)> = vs.iter().chain(&tricky).cloned().collect();
        s.put_many(&all).await.expect("pack");
        let mut items = chain(&vs);
        items.extend(tricky.chunks(2).flat_map(chain));
        let stats = s.repack(&items).await.expect("repack");
        assert_eq!(stats.deltas, vs.len() - 1 + 3, "{stats:?}");
        s.prune(std::time::Duration::ZERO).await.expect("prune");
        let stored: HashMap<ObjectId, Vec<u8>> = stored_objects(dir.path()).into_iter().collect();
        for (i, (id, _)) in vs.iter().enumerate().skip(1) {
            let Some(Base::At { offset, len }) = delta::base_of(&stored[id]) else {
                panic!("version {i} is not a delta by place");
            };
            let base = &stored[&vs[i - 1].0];
            assert_eq!(len, base.len() as u64);
            assert!(offset > 0);
        }
        for pair in tricky.chunks(2) {
            assert_eq!(delta::base_of(&stored[&pair[1].0]), Some(Base::Id(pair[0].0)));
        }
        let fresh = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        // Oldest first, so every chain is read from the bottom with nothing cached
        for (id, d) in all.iter().rev() {
            assert_eq!(fresh.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
        // Whole content that starts like a delta is read back as it is
        let mut odd = b"SCDP\x01\x01".to_vec();
        odd.extend_from_slice(&noise(50));
        let odd = Bytes::from(odd);
        let odd_id = ObjectId::of_blob(&odd);
        s.put_many(&[(odd_id, odd.clone()), vs[0].clone()]).await.expect("put");
        assert_eq!(fresh.get(&odd_id).await, Ok(Some(odd)));
    }

    /// The in-memory backend compresses and packs like the others.
    #[tokio::test]
    async fn memory_store_packs_and_compresses() {
        let index = Arc::new(scion_store::MemStore::new());
        let s = ObjectBlobStore::memory().with_packs(index.clone());
        let objects: Vec<(ObjectId, Bytes)> = (0..20)
            .map(|i| {
                let d = text(200 + i);
                (ObjectId::of_blob(&d), d)
            })
            .collect();
        s.put_many(&objects).await.expect("pack");
        let ids: Vec<ObjectId> = objects.iter().map(|o| o.0).collect();
        let locs = index.pack_locations(&ids).await.expect("locations");
        assert_eq!(locs.len(), ids.len(), "packed");
        assert!(locs.values().all(|l| l.len * 4 < 200 * 25), "compressed");
        for (id, d) in &objects {
            assert_eq!(s.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
    }

    /// An empty file in a pack too large to hold whole is read without a request for an empty
    /// range, which object stores reject.
    #[tokio::test]
    async fn empty_objects_in_large_packs_read_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let empty = Bytes::new();
        let mut objects = vec![(ObjectId::of_blob(&empty), empty.clone())];
        objects.extend((0..40).map(|i| {
            let d = noise(4_000 + i);
            (ObjectId::of_blob(&d), d)
        }));
        s.put_many(&objects).await.expect("pack");
        let fresh = ObjectBlobStore::local(dir.path()).expect("open").with_packs(index);
        for (id, d) in &objects {
            assert_eq!(fresh.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
    }

    #[tokio::test]
    async fn repack_cuts_chains_at_the_maximum_depth() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let n = usize::from(MAX_DELTA_DEPTH) + 2;
        let vs: Vec<(ObjectId, Bytes)> = (0..n)
            .map(|v| {
                let d: String = (0..n).map(|i| format!("line {i}: {}\n", u8::from(i == v))).collect();
                let d = Bytes::from(d);
                (ObjectId::of_blob(&d), d)
            })
            .collect();
        s.put_many(&vs).await.expect("pack");
        let stats = s.repack(&chain(&vs)).await.expect("repack");
        // The newest whole, MAX_DELTA_DEPTH deltas, then whole again
        assert_eq!((stats.packed, stats.deltas), (n, n - 2));
        let fresh = ObjectBlobStore::local(dir.path()).expect("open").with_packs(index);
        for (id, d) in vs.iter().rev() {
            assert_eq!(fresh.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
    }

    #[tokio::test]
    async fn repack_stores_versions_as_deltas_and_prune_removes_what_it_superseded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let vs = versions();
        // Written before: loose objects, and a pack
        for (id, d) in &vs[..10] {
            s.put(*id, d.clone()).await.expect("loose");
        }
        s.put_many(&vs[10..]).await.expect("pack");
        let before: u64 = files(dir.path(), "")
            .iter()
            .map(|f| f.metadata().expect("meta").len())
            .sum();
        let stats = s.repack(&chain(&vs)).await.expect("repack");
        // v20 whole, the others deltas
        assert_eq!((stats.packed, stats.deltas, stats.packs, stats.missing), (21, 20, 1, 0));
        let fresh = || {
            ObjectBlobStore::local(dir.path())
                .expect("open")
                .with_packs(index.clone())
        };
        for (id, d) in &vs {
            assert_eq!(fresh().get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
        // Too young to prune the old pack
        let young = s
            .prune(std::time::Duration::from_secs(3600))
            .await
            .expect("prune young");
        assert_eq!((young.packs_removed, young.loose_removed), (0, 10));
        let pruned = s.prune(std::time::Duration::ZERO).await.expect("prune");
        assert_eq!((pruned.packs_removed, pruned.loose_removed), (1, 0));
        assert!(pruned.bytes > 0);
        let after: u64 = files(dir.path(), "")
            .iter()
            .map(|f| f.metadata().expect("meta").len())
            .sum();
        assert!(after * 2 < before, "{after} vs {before}");
        let reader = fresh();
        for (id, d) in vs.iter().rev() {
            assert_eq!(
                reader.get(id).await.as_ref(),
                Ok(&Some(d.clone())),
                "oldest first, through the chain"
            );
        }
        // Repacking the same objects again writes the same pack (packs are named by content)
        s.repack(&chain(&vs)).await.expect("again");
        assert_eq!(
            s.prune(std::time::Duration::ZERO).await.expect("prune").packs_removed,
            0
        );
        // In another order it is a new pack, and the old one goes
        let mut reordered = chain(&vs);
        reordered.swap(0, 1);
        s.repack(&reordered).await.expect("reordered");
        assert_eq!(
            s.prune(std::time::Duration::ZERO).await.expect("prune").packs_removed,
            1
        );
        assert_eq!(files(dir.path(), "packs").len(), 1);
        for (id, d) in &vs {
            assert_eq!(fresh().get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
    }

    #[tokio::test]
    async fn repack_skips_missing_large_and_repeated_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, _) = packed(dir.path());
        let big = noise(REPACK_MAX_OBJECT + 1);
        let big_id = ObjectId::of_blob(&big);
        s.put(big_id, big.clone()).await.expect("big");
        let small = text(100);
        let small_id = ObjectId::of_blob(&small);
        s.put(small_id, small.clone()).await.expect("small");
        let items = [
            RepackItem {
                id: ObjectId::of_blob(b"never stored"),
                base: None,
            },
            RepackItem { id: big_id, base: None },
            // Its base is too large to be packed, so it is stored whole
            RepackItem {
                id: small_id,
                base: Some(big_id),
            },
            RepackItem {
                id: small_id,
                base: None,
            },
        ];
        let stats = s.repack(&items).await.expect("repack");
        assert_eq!((stats.missing, stats.large, stats.packed, stats.deltas), (1, 1, 1, 0));
        assert_eq!(s.get(&big_id).await, Ok(Some(big)));
        let plain = ObjectBlobStore::local(dir.path()).expect("open");
        assert!(matches!(plain.repack(&items).await, Err(Error::Backend(_))));
        assert!(matches!(
            plain.prune(std::time::Duration::ZERO).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn purging_a_delta_base_rewrites_its_dependents_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        let vs = versions();
        s.put_many(&vs).await.expect("pack");
        s.repack(&chain(&vs)).await.expect("repack");
        s.prune(std::time::Duration::ZERO).await.expect("prune");
        assert_eq!(index.delta_dependents(&vs[0].0).await.expect("deps"), vec![vs[1].0]);
        s.delete(&vs[0].0).await.expect("purge the newest version");
        assert!(index.delta_dependents(&vs[0].0).await.expect("deps").is_empty());
        let reader = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        assert_eq!(reader.get(&vs[0].0).await, Ok(None));
        for (id, d) in &vs[1..] {
            assert_eq!(reader.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
        // A delta whose base is nowhere fails its read, and is forgotten so an upload repairs it
        let (ghost, obj) = (text(300), text(301));
        let (ghost_id, obj_id) = (ObjectId::of_blob(&ghost), ObjectId::of_blob(&obj));
        let mut b = pack::PackBuilder::new();
        b.add(
            obj_id,
            &delta::encode(Base::Id(ghost_id), &ghost, &obj, obj.len()).expect("delta"),
        );
        b.add(vs[1].0, &vs[1].1);
        s.write_pack(s.packs.as_ref().expect("packs"), b).await.expect("write");
        let reader = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        assert!(matches!(reader.get(&obj_id).await, Err(Error::Backend(m)) if m.contains("integrity")));
        assert!(index.pack_locations(&[obj_id]).await.expect("loc").is_empty());
        s.put_many(&[(obj_id, obj.clone()), vs[2].clone()])
            .await
            .expect("upload again");
        assert_eq!(reader.get(&obj_id).await, Ok(Some(obj)));
    }

    #[tokio::test]
    async fn large_packs_are_read_in_cached_blocks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (s, index) = packed(dir.path());
        // 3 MiB of incompressible objects of 300 KB: several cross a block boundary
        let objs: Vec<(ObjectId, Bytes)> = (0..10u8)
            .map(|i| {
                let mut d = noise(300_000).to_vec();
                d[0] = i;
                let d = Bytes::from(d);
                (ObjectId::of_blob(&d), d)
            })
            .collect();
        s.put_many(&objs).await.expect("put");
        s.repack(
            &objs
                .iter()
                .map(|(id, _)| RepackItem { id: *id, base: None })
                .collect::<Vec<_>>(),
        )
        .await
        .expect("repack");
        s.prune(std::time::Duration::ZERO).await.expect("prune");
        let reader = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        for (id, d) in &objs {
            assert_eq!(reader.get(id).await.as_ref(), Ok(&Some(d.clone())));
        }
        // Everything is in cached blocks now: reads no longer need the pack
        for f in files(dir.path(), "packs") {
            std::fs::remove_file(f).expect("remove");
        }
        for (id, d) in &objs {
            assert_eq!(reader.get(id).await.as_ref(), Ok(&Some(d.clone())), "from blocks");
        }
        // An object larger than a few blocks is read directly, not through the cache
        let big = noise(5 << 20);
        let big_id = ObjectId::of_blob(&big);
        s.repack(&[RepackItem { id: big_id, base: None }])
            .await
            .expect("missing is fine");
        s.put(big_id, big.clone()).await.expect("loose");
        let stats = s
            .repack(&[RepackItem { id: big_id, base: None }])
            .await
            .expect("repack big");
        assert_eq!(stats.packed, 1);
        let reader = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        assert_eq!(reader.get(&big_id).await, Ok(Some(big)));
        // A location pointing past the end of its pack is an error, not other bytes
        let loc = index.pack_locations(&[big_id]).await.expect("loc")[&big_id];
        let past = scion_store::PackedObject {
            id: objs[0].0,
            offset: loc.offset + loc.len + 2 * PACK_BLOCK,
            len: 10,
            base: None,
        };
        index
            .put_pack_locations(&loc.pack, &[past])
            .await
            .expect("bad location");
        let reader = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(index.clone());
        assert!(reader.get(&objs[0].0).await.is_err());
    }

    #[test]
    fn keys_back_to_names() {
        let id = ObjectId::of_blob(b"x");
        assert_eq!(loose_id(&format!("pre/{}", object_key(&id))), Some(id));
        assert_eq!(loose_id("objects/zz/aa/nothex"), None);
        assert_eq!(loose_id("short"), None);
        let name = [0xab; 32];
        assert_eq!(pack_name(&format!("pre/{}", pack::pack_key(&name))), Some(name));
        assert_eq!(pack_name("packs/ab/abc"), None);
        assert_eq!(pack_name(&format!("packs/zz/{}", "z".repeat(64))), None);
    }

    #[tokio::test]
    async fn without_packs_put_many_writes_loose_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let objs = objects(20, "loose");
        s.put_many(&objs).await.expect("put many");
        assert_eq!(files(dir.path(), "objects").len(), 20);
        assert!(files(dir.path(), "packs").is_empty());
    }

    fn text(n: usize) -> Bytes {
        Bytes::from(
            (0..n)
                .map(|i| format!("line {} of a source file\n", i % 97))
                .collect::<String>(),
        )
    }

    fn noise(n: usize) -> Bytes {
        // xorshift: incompressible
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        Bytes::from(
            (0..n)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect::<Vec<u8>>(),
        )
    }

    fn stored_len(dir: &std::path::Path, id: &ObjectId) -> u64 {
        std::fs::metadata(dir.join(object_key(id))).expect("stored").len()
    }

    #[test]
    fn encode_decode_rules() {
        let small = Bytes::from_static(b"tiny");
        assert_eq!(encode(&small), small, "below MIN_COMPRESS");
        let t = text(1000);
        let z = encode(&t);
        assert!(
            z.starts_with(&ZSTD_MAGIC) && z.len() * 4 < t.len(),
            "{} -> {}",
            t.len(),
            z.len()
        );
        assert_eq!(decode(&ObjectId::of_blob(&t), z), Some(t.clone()));
        let n = noise(4096);
        assert_eq!(encode(&n), n, "incompressible content is stored as it is");
        // Raw content that starts with the zstd magic is not mistaken for a frame
        let mut tricky = ZSTD_MAGIC.to_vec();
        tricky.extend_from_slice(&noise(300));
        let tricky = Bytes::from(tricky);
        assert_eq!(decode(&ObjectId::of_blob(&tricky), tricky.clone()), Some(tricky));
        // A frame of something else
        assert_eq!(decode(&ObjectId::of_blob(b"other"), encode(&t)), None);
        assert_eq!(decode(&ObjectId::of_blob(b"x"), Bytes::from_static(b"y")), None);
    }

    #[tokio::test]
    async fn compressed_storage_round_trips_and_reads_old_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let (t, n) = (text(5000), noise(2 << 20));
        let (tid, nid) = (ObjectId::of_blob(&t), ObjectId::of_blob(&n));
        s.put(tid, t.clone()).await.expect("put text");
        s.put(nid, n.clone()).await.expect("put noise");
        assert!(stored_len(dir.path(), &tid) * 4 < t.len() as u64, "text is compressed");
        assert_eq!(stored_len(dir.path(), &nid), n.len() as u64, "noise is not");
        assert_eq!(s.get(&tid).await, Ok(Some(t.clone())));
        assert_eq!(s.get(&nid).await, Ok(Some(n)));
        // Large compressible objects go through the blocking pool
        let big = text(200_000);
        let bid = ObjectId::of_blob(&big);
        s.put(bid, big.clone()).await.expect("put big");
        assert!(stored_len(dir.path(), &bid) * 4 < big.len() as u64);
        assert_eq!(s.get(&bid).await, Ok(Some(big)));

        // Objects written without compression (older servers) read back and are not rewritten
        let old = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_compression(false);
        let legacy = text(3000);
        let lid = ObjectId::of_blob(&legacy);
        old.put(lid, legacy.clone()).await.expect("legacy put");
        assert_eq!(stored_len(dir.path(), &lid), legacy.len() as u64);
        assert_eq!(s.get(&lid).await, Ok(Some(legacy.clone())));
        s.put(lid, legacy.clone()).await.expect("put again");
        assert_eq!(stored_len(dir.path(), &lid), legacy.len() as u64, "intact copy kept");
        assert_eq!(
            old.get(&tid).await,
            Ok(Some(t)),
            "compressed objects read without compression on"
        );
    }

    #[tokio::test]
    async fn damaged_compressed_objects_fail_reads_and_are_repaired() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let t = text(5000);
        let id = ObjectId::of_blob(&t);
        s.put(id, t.clone()).await.expect("put");
        let file = dir.path().join(object_key(&id));
        let mut z = std::fs::read(&file).expect("read");
        let mid = z.len() / 2;
        z[mid] ^= 0xff;
        std::fs::write(&file, &z).expect("flip");
        assert!(matches!(s.get(&id).await, Err(Error::Backend(m)) if m.contains("integrity")));
        s.put(id, t.clone()).await.expect("repair");
        assert_eq!(s.get(&id).await, Ok(Some(t)));
    }

    #[tokio::test]
    async fn off_runtime_without_a_runtime_and_with_a_panic() {
        assert_eq!(
            std::thread::spawn(|| futures_lite_block_on(off_runtime(BLOCKING_ABOVE + 1, || 7)))
                .join()
                .ok(),
            Some(7)
        );
        let r = tokio::spawn(off_runtime(BLOCKING_ABOVE + 1, || -> u8 { panic!("boom") })).await;
        assert!(r.is_err_and(|e| e.is_panic()));
    }

    /// Polls a future to completion on the current thread (no tokio runtime).
    fn futures_lite_block_on<F: std::future::Future>(f: F) -> F::Output {
        use std::task::{Context, Poll, Waker};
        let mut f = std::pin::pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    #[tokio::test]
    async fn local_corruption_detected_on_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let id = ObjectId::of_blob(b"good");
        s.put(id, Bytes::from_static(b"good")).await.expect("put");
        std::fs::write(dir.path().join(object_key(&id)), b"evil").expect("tamper");
        assert!(matches!(s.get(&id).await, Err(Error::Backend(m)) if m.contains("integrity")));
    }

    /// 回归测试（PR #1 review）：已损坏的对象必须能被重新上传修复。
    #[tokio::test]
    async fn put_repairs_truncated_and_bit_flipped_objects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let data = Bytes::from_static(b"precious model weights");
        let id = ObjectId::of_blob(&data);
        let file = dir.path().join(object_key(&id));
        s.put(id, data.clone()).await.expect("put");

        // 截断：大小不一致
        std::fs::write(&file, b"precious").expect("truncate");
        assert!(matches!(s.get(&id).await, Err(Error::Backend(_))));
        s.put(id, data.clone()).await.expect("re-upload repairs");
        assert_eq!(s.get(&id).await, Ok(Some(data.clone())));

        // 位翻转：大小相同但内容不同
        let mut flipped = data.to_vec();
        flipped[0] ^= 0x01;
        std::fs::write(&file, &flipped).expect("flip");
        assert!(matches!(s.get(&id).await, Err(Error::Backend(_))));
        s.put(id, data.clone()).await.expect("re-upload repairs");
        assert_eq!(s.get(&id).await, Ok(Some(data)));
    }

    #[tokio::test]
    async fn put_rejects_bytes_not_matching_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let id = ObjectId::of_blob(b"good");
        assert!(matches!(
            s.put(id, Bytes::from_static(b"evil")).await,
            Err(Error::Backend(_))
        ));
        assert_eq!(s.exists(&id).await, Ok(false));
    }

    #[tokio::test]
    async fn intact_missing_object_is_not_intact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        let id = ObjectId::of_blob(b"x");
        assert_eq!(s.intact(&s.key(&id), &id).await, Ok(false));
    }

    #[tokio::test]
    async fn local_self_test_detects_tampered_probe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("open");
        s.self_test().await.expect("first run");
        let probe = ObjectId::of_blob(b"scion blobstore self-test v1");
        std::fs::remove_file(dir.path().join(object_key(&probe))).expect("remove");
        // 删除后再自检：重新写入并读回，仍应成功
        s.self_test().await.expect("rewrites probe");
    }

    #[test]
    fn local_unusable_dir_rejected() {
        let file = tempfile::NamedTempFile::new().expect("file");
        // 路径是一个普通文件，不能作为目录
        assert!(matches!(
            ObjectBlobStore::local(file.path().join("x")),
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn s3_unreachable_endpoint_reports_backend_error() {
        let cfg = S3Config {
            endpoint: Some("http://127.0.0.1:1".into()),
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
            prefix: "p".into(),
            ..Default::default()
        };
        let no_retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let s = ObjectBlobStore::s3_with_retry(&cfg, no_retry).expect("build is offline");
        let id = ObjectId::of_blob(b"x");
        assert!(matches!(s.exists(&id).await, Err(Error::Backend(_))));
        assert!(matches!(s.get(&id).await, Err(Error::Backend(_))));
        assert!(matches!(
            s.put(id, Bytes::from_static(b"x")).await,
            Err(Error::Backend(_))
        ));
        assert!(matches!(s.self_test().await, Err(Error::Backend(_))));
    }

    #[test]
    fn s3_max_retries_override_accepted() {
        let cfg = S3Config {
            bucket: "b".into(),
            max_retries: Some(0),
            ..Default::default()
        };
        assert!(ObjectBlobStore::s3(&cfg).is_ok());
    }

    #[test]
    fn s3_missing_bucket_rejected() {
        let cfg = S3Config {
            endpoint: Some("http://127.0.0.1:1".into()),
            ..Default::default()
        };
        assert!(matches!(ObjectBlobStore::s3(&cfg), Err(Error::Backend(_))));
    }

    #[test]
    fn provider_defaults_applied_at_build() {
        // 自建服务必须有 endpoint
        for provider in [S3Provider::Minio, S3Provider::SeaweedFs] {
            let cfg = S3Config {
                bucket: "b".into(),
                provider,
                ..Default::default()
            };
            assert!(matches!(ObjectBlobStore::s3(&cfg), Err(Error::Backend(m)) if m.contains("needs an endpoint")));
        }
        // 没有 endpoint 的 Auto 即 AWS
        let aws = ObjectBlobStore::s3(&S3Config {
            bucket: "b".into(),
            ..Default::default()
        })
        .expect("aws");
        assert_eq!(aws.s3.expect("s3").provider, S3Provider::Aws);
        let minio = ObjectBlobStore::s3(&S3Config {
            endpoint: Some("http://minio:9000".into()),
            bucket: "b".into(),
            provider: S3Provider::Minio,
            ..Default::default()
        })
        .expect("minio");
        assert_eq!(minio.s3.expect("s3").provider, S3Provider::Minio);
    }

    #[tokio::test]
    async fn local_check_reports_local() {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = ObjectBlobStore::local(dir.path()).expect("local");
        assert_eq!(s.check().await, Ok(BlobBackend::Local));
        assert_eq!(BlobBackend::Local.to_string(), "local");
        let d = Detected {
            provider: S3Provider::Minio,
            server: Some("MinIO".into()),
        };
        assert_eq!(BlobBackend::S3(d.clone()).to_string(), "s3 (minio, Server: MinIO)");
        let bare = Detected { server: None, ..d };
        assert_eq!(BlobBackend::S3(bare).to_string(), "s3 (minio)");
    }

    #[tokio::test]
    async fn s3_check_reports_unreachable_endpoint() {
        let s = ObjectBlobStore::s3(&S3Config {
            endpoint: Some("http://127.0.0.1:1".into()),
            bucket: "b".into(),
            ..Default::default()
        })
        .expect("build");
        // 直连时在识别阶段报 unreachable；经过代理时代理返回 502，随后列 bucket 失败
        assert!(matches!(s.check().await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn s3_check_identifies_provider() {
        let Some(cfg) = s3_from_env() else {
            eprintln!("SCION_TEST_S3_ENDPOINT not set; skipping");
            return;
        };
        let s = ObjectBlobStore::s3(&cfg).expect("build");
        let BlobBackend::S3(d) = s.check().await.expect("check") else {
            panic!("expected s3")
        };
        assert_ne!(d.provider, S3Provider::Auto);
        if cfg.provider != S3Provider::Auto {
            assert_eq!(d.provider, cfg.provider);
        }
        // 自动识别得到同样的结果
        let auto = ObjectBlobStore::s3(&S3Config {
            provider: S3Provider::Auto,
            ..cfg.clone()
        })
        .expect("build");
        assert_eq!(auto.check().await, Ok(BlobBackend::S3(d.clone())));
        // 配置成另一种已知服务：报错
        if matches!(d.provider, S3Provider::Minio | S3Provider::SeaweedFs) {
            let other = if d.provider == S3Provider::Minio {
                S3Provider::SeaweedFs
            } else {
                S3Provider::Minio
            };
            let wrong = ObjectBlobStore::s3(&S3Config {
                provider: other,
                ..cfg.clone()
            })
            .expect("build");
            assert!(matches!(wrong.check().await, Err(Error::Backend(m)) if m.contains("configured as")));
        }
        // bucket 不存在：给出该服务的创建方法
        let missing = ObjectBlobStore::s3(&S3Config {
            bucket: format!("scion-missing-{}", std::process::id()),
            ..cfg
        })
        .expect("build");
        let err = missing.check().await.expect_err("missing bucket");
        let hint = d.provider.create_bucket_hint("x");
        let tool = hint
            .split('`')
            .nth(1)
            .unwrap_or("")
            .split_whitespace()
            .next()
            .unwrap_or("");
        assert!(
            matches!(&err, Error::Backend(m) if m.contains("does not exist") && m.contains(tool)),
            "{err:?}"
        );
    }

    /// 真实 S3 兼容服务：需要 `SCION_TEST_S3_ENDPOINT` 等环境变量（CI 中由 SeaweedFS 和 MinIO 提供），未设置时跳过。
    fn s3_from_env() -> Option<S3Config> {
        let endpoint = std::env::var("SCION_TEST_S3_ENDPOINT").ok()?;
        Some(S3Config {
            endpoint: Some(endpoint),
            bucket: std::env::var("SCION_TEST_S3_BUCKET").unwrap_or_else(|_| "scion-test".into()),
            region: "us-east-1".into(),
            access_key_id: std::env::var("SCION_TEST_S3_ACCESS_KEY").unwrap_or_default(),
            secret_access_key: std::env::var("SCION_TEST_S3_SECRET_KEY").unwrap_or_default(),
            // 期望的服务种类（CI 中分别为 seaweedfs、minio）；未设置时自动识别
            provider: std::env::var("SCION_TEST_S3_PROVIDER")
                .ok()
                .map(|p| p.parse().expect("SCION_TEST_S3_PROVIDER"))
                .unwrap_or_default(),
            virtual_hosted_style: None,
            max_retries: None,
            // 每次运行用独立前缀，互不干扰
            prefix: format!(
                "t{}-{}",
                std::process::id(),
                scion_object::ObjectId::of_blob(&rand_bytes()).digest()[0]
            ),
        })
    }

    fn rand_bytes() -> [u8; 16] {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        t.to_le_bytes()
    }

    #[tokio::test]
    async fn s3_conformance_and_self_test() {
        let Some(cfg) = s3_from_env() else {
            eprintln!("SCION_TEST_S3_ENDPOINT not set; skipping");
            return;
        };
        let s = ObjectBlobStore::s3(&cfg).expect("build");
        testsuite::blob_store(&s).await;
        s.self_test().await.expect("self test");
        // Packs use range reads
        let packed = ObjectBlobStore::s3(&S3Config {
            prefix: format!("{}-packed", cfg.prefix),
            ..cfg
        })
        .expect("build")
        .with_packs(Arc::new(scion_store::MemStore::new()));
        testsuite::blob_store(&packed).await;
    }

    #[tokio::test]
    async fn s3_wrong_credentials_rejected() {
        let Some(mut cfg) = s3_from_env() else {
            eprintln!("SCION_TEST_S3_ENDPOINT not set; skipping");
            return;
        };
        cfg.secret_access_key = "wrong".into();
        let no_retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let s = ObjectBlobStore::s3_with_retry(&cfg, no_retry).expect("build");
        assert!(matches!(s.self_test().await, Err(Error::Backend(_))));
        assert!(matches!(s.check().await, Err(Error::Backend(_))));
    }
}
