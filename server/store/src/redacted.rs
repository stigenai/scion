//! 按仓库检查 redaction 的 [`BlobStore`] 包装（服务端设计文档 §9）。

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::{ObjectId, ObjectKind};

use crate::{BlobStore, Error, MetaStore, Redaction, Result};

/// 只有 blob（文件内容）可以 redact：目录树和提交对象是历史结构，redact 它们会让检出和查询失效。
pub fn redactable(id: &ObjectId) -> bool {
    id.kind() == ObjectKind::Blob
}

/// `records` 中第一条有效的 redaction 对应的错误。
pub fn redacted_error(records: &[Redaction]) -> Option<Error> {
    records.iter().find(|r| r.active).map(|r| Error::Redacted {
        id: r.id,
        redaction_id: r.redaction_id.clone(),
    })
}

/// 读取在 `repo` 中被 redact 的 blob 时返回 [`Error::Redacted`]，其余操作原样转发。
///
/// 查询（ReadFile、Blame）和合入时的三方合并经由它读取，redact 过的内容不会出现在应答里，
/// 也不会被合并进新的提交。批量读对象（GetObjects）自己批量检查，不经过这里。
pub struct RedactedBlobs {
    inner: Arc<dyn BlobStore>,
    meta: Arc<dyn MetaStore>,
    repo: String,
}

impl RedactedBlobs {
    pub fn new(inner: Arc<dyn BlobStore>, meta: Arc<dyn MetaStore>, repo: impl Into<String>) -> Self {
        Self {
            inner,
            meta,
            repo: repo.into(),
        }
    }
}

#[async_trait]
impl BlobStore for RedactedBlobs {
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
        self.inner.put(id, data).await
    }

    async fn put_many(&self, objects: &[(ObjectId, Bytes)]) -> Result<()> {
        self.inner.put_many(objects).await
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        if redactable(id)
            && let Some(e) = redacted_error(&self.meta.get_redactions(&self.repo, &[*id]).await?)
        {
            return Err(e);
        }
        self.inner.get(id).await
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool> {
        self.inner.exists(id).await
    }

    async fn delete(&self, id: &ObjectId) -> Result<()> {
        self.inner.delete(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IndexedObject, MemStore};

    fn rec(id: ObjectId, active: bool) -> Redaction {
        Redaction {
            repo: "r".into(),
            id,
            redaction_id: "rd1".into(),
            reason: String::new(),
            created_by: String::new(),
            created_at_ms: 0,
            active,
            purge_after_ms: None,
            purged_at_ms: None,
            seq: 0,
        }
    }

    #[tokio::test]
    async fn redacted_blobs_are_refused_per_repo() {
        let store = Arc::new(MemStore::new());
        store.create_repo("r").await.expect("repo");
        store.create_repo("other").await.expect("repo");
        let secret = Bytes::from_static(b"secret");
        let id = ObjectId::of_blob(&secret);
        store.put(id, secret.clone()).await.expect("put");
        for repo in ["r", "other"] {
            store
                .index_objects(repo, &[IndexedObject { id, size: 6 }])
                .await
                .expect("index");
        }
        let r = RedactedBlobs::new(store.clone(), store.clone(), "r");
        let other = RedactedBlobs::new(store.clone(), store.clone(), "other");
        assert_eq!(r.get(&id).await, Ok(Some(secret.clone())));
        store.put_redactions(&[rec(id, true)]).await.expect("redact");
        assert_eq!(
            r.get(&id).await,
            Err(Error::Redacted {
                id,
                redaction_id: "rd1".into()
            })
        );
        assert_eq!(other.get(&id).await, Ok(Some(secret.clone())), "other repo unaffected");
        assert_eq!(r.exists(&id).await, Ok(true));
        // 撤销后恢复
        store.put_redactions(&[rec(id, false)]).await.expect("unredact");
        assert_eq!(r.get(&id).await, Ok(Some(secret)));
        // 写入与删除原样转发；目录树不查 redaction
        let tree = ObjectId::hash(ObjectKind::Tree, b"t");
        assert!(!redactable(&tree) && redactable(&id));
        assert_eq!(r.get(&tree).await, Ok(None));
        let new = Bytes::from_static(b"new");
        r.put(ObjectId::of_blob(&new), new.clone()).await.expect("put");
        r.delete(&id).await.expect("delete");
        assert_eq!(store.get(&id).await, Ok(None));
        assert_eq!(redacted_error(&[rec(id, false)]), None);
    }
}
