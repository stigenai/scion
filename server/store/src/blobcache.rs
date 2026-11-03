//! L3 读对象的进程级缓存（服务端设计文档 §7.3）。
//!
//! - 小对象（默认不超过 1 MiB，主要是目录树和小文件）放进按字节计权的 moka 缓存；对象不可变，
//!   缓存无需失效；
//! - 同一对象的并发读只向后端发一次（single-flight），热点对象不会同时打到 S3 很多次；
//! - 同时在途的后端读有上限，S3 变慢时不会无限堆积连接。
//!
//! 「不存在」的结果不缓存（对象可能稍后写入）。

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use moka::sync::Cache;
use scion_flight::Group;
use scion_object::ObjectId;
use tokio::sync::Semaphore;

use crate::{BlobStore, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobCacheConfig {
    /// 缓存的总字节数；0 表示不缓存（仍然合并并发读、限制并发）。
    pub capacity_bytes: u64,
    /// 超过这个大小的对象不缓存。
    pub max_object_bytes: usize,
    /// 同时在途的后端读上限。
    pub max_concurrent_reads: usize,
}

impl Default for BlobCacheConfig {
    fn default() -> Self {
        Self {
            capacity_bytes: 256 << 20,
            max_object_bytes: 1 << 20,
            max_concurrent_reads: 256,
        }
    }
}

pub struct CachedBlobs {
    inner: Arc<dyn BlobStore>,
    cache: Option<Cache<ObjectId, Bytes>>,
    flight: Group<ObjectId, Result<Option<Bytes>>>,
    permits: Semaphore,
    max_object_bytes: usize,
}

impl CachedBlobs {
    pub fn new(inner: Arc<dyn BlobStore>, config: BlobCacheConfig) -> Self {
        let cache = (config.capacity_bytes > 0).then(|| {
            Cache::builder()
                .max_capacity(config.capacity_bytes)
                .weigher(|_k: &ObjectId, v: &Bytes| u32::try_from(v.len()).unwrap_or(u32::MAX))
                .build()
        });
        Self {
            inner,
            cache,
            flight: Group::new(),
            permits: Semaphore::new(config.max_concurrent_reads.max(1)),
            max_object_bytes: config.max_object_bytes,
        }
    }

    fn remember(&self, id: ObjectId, data: &Bytes) {
        if let Some(c) = &self.cache
            && data.len() <= self.max_object_bytes
        {
            c.insert(id, data.clone());
        }
    }

    fn cached(&self, id: &ObjectId) -> Option<Bytes> {
        self.cache.as_ref().and_then(|c| c.get(id))
    }

    async fn load(&self, id: ObjectId) -> Result<Option<Bytes>> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| Error::Backend("blob reads closed".into()))?;
        let data = self.inner.get(&id).await?;
        if let Some(d) = &data {
            self.remember(id, d);
        }
        Ok(data)
    }
}

#[async_trait]
impl BlobStore for CachedBlobs {
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
        self.inner.put(id, data.clone()).await?;
        // 刚写入的对象很可能马上被读到（合入时的 rebase、客户端拉取）
        self.remember(id, &data);
        Ok(())
    }

    async fn put_many(&self, objects: &[(ObjectId, Bytes)]) -> Result<()> {
        self.inner.put_many(objects).await?;
        for (id, data) in objects {
            self.remember(*id, data);
        }
        Ok(())
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        if let Some(d) = self.cached(id) {
            scion_obs::metrics().object_cache("l3", true);
            return Ok(Some(d));
        }
        scion_obs::metrics().object_cache("l3", false);
        self.flight.run(*id, || self.load(*id)).await
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool> {
        if self.cached(id).is_some() {
            return Ok(true);
        }
        self.inner.exists(id).await
    }

    async fn delete(&self, id: &ObjectId) -> Result<()> {
        if let Some(c) = &self.cache {
            c.invalidate(id);
        }
        self.inner.delete(id).await?;
        // 删除期间进行中的读可能又把旧字节放回缓存
        if let Some(c) = &self.cache {
            c.invalidate(id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemStore;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// 统计后端读次数、可注入延迟和错误的 BlobStore。
    struct Counting {
        inner: MemStore,
        gets: AtomicUsize,
        exists: AtomicUsize,
        delay: Duration,
        fail: bool,
    }

    #[async_trait]
    impl BlobStore for Counting {
        async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
            if self.fail {
                return Err(Error::Backend("down".into()));
            }
            self.inner.put(id, data).await
        }
        async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            if self.fail {
                return Err(Error::Backend("down".into()));
            }
            self.inner.get(id).await
        }
        async fn exists(&self, id: &ObjectId) -> Result<bool> {
            self.exists.fetch_add(1, Ordering::SeqCst);
            self.inner.exists(id).await
        }
    }

    fn counting(delay: Duration, fail: bool) -> Arc<Counting> {
        Arc::new(Counting {
            inner: MemStore::new(),
            gets: AtomicUsize::new(0),
            exists: AtomicUsize::new(0),
            delay,
            fail,
        })
    }

    #[tokio::test]
    async fn delete_defaults_to_unsupported() {
        let back = counting(Duration::ZERO, false);
        assert!(matches!(
            back.delete(&ObjectId::of_blob(b"x")).await,
            Err(Error::Backend(m)) if m.contains("not supported")
        ));
        let cached = CachedBlobs::new(back, BlobCacheConfig::default());
        assert!(
            cached.delete(&ObjectId::of_blob(b"x")).await.is_err(),
            "errors pass through"
        );
    }

    #[tokio::test]
    async fn conformance_with_and_without_cache() {
        crate::testsuite::blob_store(&CachedBlobs::new(Arc::new(MemStore::new()), BlobCacheConfig::default())).await;
        let off = BlobCacheConfig {
            capacity_bytes: 0,
            ..BlobCacheConfig::default()
        };
        crate::testsuite::blob_store(&CachedBlobs::new(Arc::new(MemStore::new()), off)).await;
    }

    #[tokio::test]
    async fn caches_small_objects_and_coalesces_reads() {
        let back = counting(Duration::from_millis(20), false);
        let small = Bytes::from_static(b"small");
        let big = Bytes::from(vec![1u8; 64]);
        let (sid, bid) = (ObjectId::of_blob(&small), ObjectId::of_blob(&big));
        back.inner.put(sid, small.clone()).await.expect("put");
        back.inner.put(bid, big.clone()).await.expect("put");
        let c = Arc::new(CachedBlobs::new(
            back.clone(),
            BlobCacheConfig {
                capacity_bytes: 1 << 20,
                max_object_bytes: 16,
                max_concurrent_reads: 4,
            },
        ));
        // 并发读同一个对象：只读一次后端
        let tasks: Vec<_> = (0..10)
            .map(|_| {
                let c = c.clone();
                tokio::spawn(async move { c.get(&bid).await })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.expect("join"), Ok(Some(big.clone())));
        }
        assert_eq!(back.gets.load(Ordering::SeqCst), 1);
        // 大对象不缓存：再读一次走后端
        c.get(&bid).await.expect("big");
        assert_eq!(back.gets.load(Ordering::SeqCst), 2);
        // 小对象缓存后不再读后端
        c.get(&sid).await.expect("small");
        c.get(&sid).await.expect("small again");
        assert_eq!(back.gets.load(Ordering::SeqCst), 3);
        assert!(c.exists(&sid).await.expect("exists"));
        assert_eq!(back.exists.load(Ordering::SeqCst), 0, "answered from the cache");
        assert!(c.exists(&bid).await.expect("exists"));
        assert_eq!(back.exists.load(Ordering::SeqCst), 1);
        // 不存在的不缓存
        let missing = ObjectId::of_blob(b"missing");
        assert_eq!(c.get(&missing).await, Ok(None));
        back.inner
            .put(missing, Bytes::from_static(b"missing"))
            .await
            .expect("late put");
        assert!(c.get(&missing).await.expect("get").is_some());
    }

    #[tokio::test]
    async fn put_populates_and_errors_pass_through() {
        let back = counting(Duration::ZERO, false);
        let c = CachedBlobs::new(back.clone(), BlobCacheConfig::default());
        let data = Bytes::from_static(b"written");
        let id = ObjectId::of_blob(&data);
        c.put(id, data.clone()).await.expect("put");
        assert_eq!(c.get(&id).await, Ok(Some(data)));
        assert_eq!(back.gets.load(Ordering::SeqCst), 0, "served from the cache after put");

        let down = counting(Duration::ZERO, true);
        let c = CachedBlobs::new(down, BlobCacheConfig::default());
        assert!(c.put(id, Bytes::from_static(b"written")).await.is_err());
        assert_eq!(c.get(&id).await, Err(Error::Backend("down".into())));

        // 容量为 0：不缓存，但仍然可用
        let back = counting(Duration::ZERO, false);
        let c = CachedBlobs::new(
            back.clone(),
            BlobCacheConfig {
                capacity_bytes: 0,
                ..BlobCacheConfig::default()
            },
        );
        c.put(id, Bytes::from_static(b"written")).await.expect("put");
        c.get(&id).await.expect("get");
        c.get(&id).await.expect("get");
        assert_eq!(back.gets.load(Ordering::SeqCst), 2);
    }
}
