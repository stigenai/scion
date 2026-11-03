//! 合入批处理中的写后缓冲：批内 rebase 产生的对象与记录先放在内存里，后面的请求直接在上面继续
//! rebase，整批确定后再一次性写入存储（[`ChangesetWriter::persist`]）。
//!
//! 同时作为读缓存：批内读过的对象（客户端提交、目录树、文件内容）只从存储读一次。对象按内容寻址、
//! 不可变，缓存不需要失效。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::{ObjectId, Tree};
use scion_store::{BlobStore, ChangesetRecord, MetaStore};
use scion_tree::{TreeError, TreeSource};

use crate::{LandError, Result};

/// 批内的对象与记录。
pub(crate) struct Pending {
    blobs: Arc<dyn BlobStore>,
    /// 批内新产生、尚未写入的对象。
    created: Mutex<HashMap<ObjectId, Bytes>>,
    /// 从存储读到的对象。
    fetched: Mutex<HashMap<ObjectId, Bytes>>,
    /// 批内新产生、尚未写入的提交记录。
    records: HashMap<ObjectId, ChangesetRecord>,
}

impl Pending {
    pub(crate) fn new(blobs: Arc<dyn BlobStore>) -> Self {
        Self {
            blobs,
            created: Mutex::new(HashMap::new()),
            fetched: Mutex::new(HashMap::new()),
            records: HashMap::new(),
        }
    }

    /// 是否是批内新产生的提交。
    pub(crate) fn is_pending(&self, id: &ObjectId) -> bool {
        self.records.contains_key(id)
    }

    /// 读提交记录：批内的优先，其次存储。
    pub(crate) async fn record(&self, meta: &dyn MetaStore, repo: &str, id: ObjectId) -> Result<ChangesetRecord> {
        if let Some(r) = self.records.get(&id) {
            return Ok(r.clone());
        }
        meta.get_changeset(repo, &id)
            .await?
            .ok_or_else(|| LandError::ObjectsNotUploaded(vec![id]))
    }

    pub(crate) fn add_record(&mut self, rec: ChangesetRecord) {
        self.records.insert(rec.id, rec);
    }

    pub(crate) fn add_object(&self, id: ObjectId, data: Bytes) {
        if let Ok(mut c) = self.created.lock() {
            c.insert(id, data);
        }
    }

    /// 并发预取一组对象（例如整批请求的提交对象），之后的读取命中缓存。
    pub(crate) async fn prefetch(&self, ids: &[ObjectId]) {
        let mut set = tokio::task::JoinSet::new();
        for id in ids {
            if self.cached(id).is_some() {
                continue;
            }
            let (blobs, id) = (self.blobs.clone(), *id);
            set.spawn(async move { (id, blobs.get(&id).await) });
        }
        while let Some(joined) = set.join_next().await {
            // 预取失败不影响正确性：之后按需读取时会报告错误
            if let Ok((id, Ok(Some(data)))) = joined
                && let Ok(mut f) = self.fetched.lock()
            {
                f.insert(id, data);
            }
        }
    }

    fn cached(&self, id: &ObjectId) -> Option<Bytes> {
        if let Some(d) = self.created.lock().ok().and_then(|c| c.get(id).cloned()) {
            return Some(d);
        }
        self.fetched.lock().ok().and_then(|f| f.get(id).cloned())
    }
}

#[async_trait]
impl BlobStore for Pending {
    /// 写入只进缓冲区，由 [`ChangesetWriter::persist`] 统一落盘。
    async fn put(&self, id: ObjectId, data: Bytes) -> scion_store::Result<()> {
        self.add_object(id, data);
        Ok(())
    }

    async fn get(&self, id: &ObjectId) -> scion_store::Result<Option<Bytes>> {
        if let Some(d) = self.cached(id) {
            return Ok(Some(d));
        }
        let data = self.blobs.get(id).await?;
        if let (Some(d), Ok(mut f)) = (&data, self.fetched.lock()) {
            f.insert(*id, d.clone());
        }
        Ok(data)
    }

    async fn exists(&self, id: &ObjectId) -> scion_store::Result<bool> {
        if self.cached(id).is_some() {
            return Ok(true);
        }
        self.blobs.exists(id).await
    }
}

#[async_trait]
impl TreeSource for Pending {
    async fn load_tree(&self, id: &ObjectId) -> scion_tree::Result<Tree> {
        let bytes = self
            .get(id)
            .await
            .map_err(|e| TreeError::Storage(e.to_string()))?
            .ok_or_else(|| TreeError::BadTree(*id, "not found".into()))?;
        Tree::decode(&bytes).map_err(|e| TreeError::BadTree(*id, e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_object::{ObjectKind, Tree as T};
    use scion_store::MemStore;

    #[tokio::test]
    async fn overlay_reads_created_then_store_and_caches() {
        let store = Arc::new(MemStore::new());
        let stored = Bytes::from_static(b"stored");
        let stored_id = ObjectId::of_blob(&stored);
        store.put(stored_id, stored.clone()).await.expect("put");
        let p = Pending::new(store.clone());
        let created = Bytes::from_static(b"created");
        let created_id = ObjectId::of_blob(&created);
        BlobStore::put(&p, created_id, created.clone()).await.expect("buffer");
        assert_eq!(store.get(&created_id).await, Ok(None), "not written through");
        assert_eq!(p.get(&created_id).await, Ok(Some(created)));
        assert_eq!(p.get(&stored_id).await, Ok(Some(stored.clone())));
        assert!(p.exists(&created_id).await.expect("exists"));
        assert!(p.exists(&stored_id).await.expect("exists"));
        let missing = ObjectId::of_blob(b"missing");
        assert_eq!(p.get(&missing).await, Ok(None));
        assert!(!p.exists(&missing).await.expect("exists"));

        // 预取：之后即使存储里没有也能读到缓存
        let other = Bytes::from_static(b"other");
        let other_id = ObjectId::of_blob(&other);
        store.put(other_id, other.clone()).await.expect("put");
        p.prefetch(&[other_id, stored_id, missing]).await;
        assert_eq!(p.cached(&other_id), Some(other));

        // 目录树
        let tree = T::default();
        BlobStore::put(&p, tree.id(), Bytes::from(tree.encode()))
            .await
            .expect("tree");
        assert_eq!(p.load_tree(&tree.id()).await.expect("load"), tree);
        assert!(matches!(p.load_tree(&missing).await, Err(TreeError::BadTree(..))));
        BlobStore::put(&p, ObjectId::hash(ObjectKind::Tree, b"x"), Bytes::from_static(b"junk"))
            .await
            .expect("junk");
        assert!(matches!(
            p.load_tree(&ObjectId::hash(ObjectKind::Tree, b"x")).await,
            Err(TreeError::BadTree(..))
        ));
    }

    #[tokio::test]
    async fn records_prefer_pending() {
        let store = Arc::new(MemStore::new());
        store.create_repo("r").await.expect("repo");
        let id = ObjectId::hash(ObjectKind::Changeset, b"c");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
            fp_depth: 1,
            fp_jump: None,
        };
        let mut p = Pending::new(store.clone());
        assert_eq!(
            p.record(store.as_ref(), "r", id).await,
            Err(LandError::ObjectsNotUploaded(vec![id]))
        );
        assert!(!p.is_pending(&id));
        p.add_record(rec.clone());
        assert!(p.is_pending(&id));
        assert_eq!(p.record(store.as_ref(), "r", id).await, Ok(rec));
    }
}
