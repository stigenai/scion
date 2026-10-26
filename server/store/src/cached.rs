//! 提交记录的进程级缓存（服务端设计文档 §7）。
//!
//! 提交记录写入后不再改变（`put_changeset` 保留第一次写入），因此可以无失效地缓存。
//! 只缓存存在的记录，「不存在」的结果不缓存。其余方法原样转发。
//!
//! 缓存按仓库名索引。仓库删除后名字要等清除之后才能重新使用（宽限期不短于 [`RECORD_TTL`]），
//! 所以重新创建的同名仓库不会读到旧仓库的记录。集群中其它节点的缓存也靠这个时限失效。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use moka::sync::Cache;
use scion_object::ObjectId;

use crate::{
    ChangesetRecord, IdempotencyRecord, IndexedObject, LandedChange, Lease, MetaStore, OrgMember, OrgRecord,
    PackLocation, PackedObject, PathChange, PathHistoryEntry, Redaction, RedactionFilter, RefEvent, RefExpect,
    RefLogEntry, RefState, RepoFilter, RepoInfo, RepoPatch, Result, TagAnnotation, TokenRecord,
};

/// 提交记录在缓存中的最长保留时间。
pub const RECORD_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

/// 在任意 [`MetaStore`] 外加一层提交记录缓存。
pub struct CachedMeta {
    inner: Arc<dyn MetaStore>,
    records: Cache<(Arc<str>, ObjectId), ChangesetRecord>,
}

impl CachedMeta {
    /// `capacity` 为最多缓存的提交记录条数。
    pub fn new(inner: Arc<dyn MetaStore>, capacity: u64) -> Self {
        Self {
            inner,
            records: Cache::builder().max_capacity(capacity).time_to_live(RECORD_TTL).build(),
        }
    }

    fn key(repo: &str, id: ObjectId) -> (Arc<str>, ObjectId) {
        (Arc::from(repo), id)
    }

    fn remember(&self, repo: &str, rec: &ChangesetRecord) {
        self.records.insert(Self::key(repo, rec.id), rec.clone());
    }

    /// 预热：按代数从高到低分窗口读入仓库的全部提交记录（受容量上限约束）。返回读入的条数。
    /// 同一代数的记录超过一个窗口时，超出部分留待按需读取。
    pub async fn warm(&self, repo: &str, window: usize) -> Result<usize> {
        let window = window.max(1);
        let mut max_generation = u64::MAX >> 1;
        let mut loaded = 0;
        loop {
            let recs = self.inner.changesets_below(repo, max_generation, window).await?;
            let Some(lowest) = recs.iter().map(|r| r.generation).min() else {
                break;
            };
            for r in &recs {
                self.remember(repo, r);
            }
            loaded += recs.len();
            if lowest <= 1 || recs.len() < window {
                break;
            }
            max_generation = lowest - 1;
        }
        Ok(loaded)
    }
}

#[async_trait]
impl MetaStore for CachedMeta {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        self.inner.create_repo(repo).await
    }

    async fn repo_exists(&self, repo: &str) -> Result<bool> {
        self.inner.repo_exists(repo).await
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        self.inner.list_repos().await
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        self.inner.get_ref(repo, name).await
    }

    async fn land_commit_as(
        &self,
        repo: &str,
        name: &str,
        expect: RefExpect,
        target: ObjectId,
        landed: &[LandedChange],
        requests: &[IdempotencyRecord],
        actor: &str,
    ) -> Result<RefState> {
        self.inner
            .land_commit_as(repo, name, expect, target, landed, requests, actor)
            .await
    }

    async fn landed_changes(&self, repo: &str, name: &str, change_ids: &[[u8; 16]]) -> Result<Vec<LandedChange>> {
        self.inner.landed_changes(repo, name, change_ids).await
    }

    async fn get_idempotency(&self, repo: &str, request_id: &str) -> Result<Option<IdempotencyRecord>> {
        self.inner.get_idempotency(repo, request_id).await
    }

    async fn expire_idempotency(&self, before_ms: i64) -> Result<u64> {
        self.inner.expire_idempotency(before_ms).await
    }

    async fn acquire_lease(&self, scope: &str, holder: &str, ttl_ms: i64, now_ms: i64) -> Result<Lease> {
        self.inner.acquire_lease(scope, holder, ttl_ms, now_ms).await
    }

    async fn release_lease(&self, scope: &str, holder: &str) -> Result<()> {
        self.inner.release_lease(scope, holder).await
    }

    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
        self.inner.index_objects(repo, objects).await
    }

    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        self.inner.missing_objects(repo, ids).await
    }

    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>> {
        self.inner.object_sizes(repo, ids).await
    }

    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
        // 不在写入时缓存：写入可能因为已存在而被忽略，以存储中的第一次写入为准
        self.inner.put_changeset(repo, rec).await
    }

    async fn put_changesets(&self, repo: &str, recs: &[ChangesetRecord]) -> Result<()> {
        self.inner.put_changesets(repo, recs).await
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
        if let Some(rec) = self.cached_changeset(repo, id) {
            return Ok(Some(rec));
        }
        let rec = self.inner.get_changeset(repo, id).await?;
        if let Some(r) = &rec {
            self.remember(repo, r);
        }
        Ok(rec)
    }

    fn cached_changeset(&self, repo: &str, id: &ObjectId) -> Option<ChangesetRecord> {
        self.records.get(&Self::key(repo, *id))
    }

    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
        self.inner.list_refs(repo).await
    }

    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>> {
        let recs = self.inner.changesets_below(repo, max_generation, limit).await?;
        for r in &recs {
            self.remember(repo, r);
        }
        Ok(recs)
    }

    async fn repair_lineage(
        &self,
        repo: &str,
        id: &ObjectId,
        parents: &[ObjectId],
        fp_depth: u64,
        fp_jump: Option<ObjectId>,
    ) -> Result<()> {
        self.inner.repair_lineage(repo, id, parents, fp_depth, fp_jump).await?;
        // 记录唯一会变化的情形：清掉旧记录，下次从存储读取
        self.records.invalidate(&Self::key(repo, *id));
        Ok(())
    }

    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
        self.inner.index_paths(repo, cs, generation, changes).await
    }

    async fn index_paths_many(&self, repo: &str, entries: &[(ObjectId, u64, Vec<PathChange>)]) -> Result<()> {
        self.inner.index_paths_many(repo, entries).await
    }

    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>> {
        self.inner.path_history(repo, path, max_generation, limit).await
    }

    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_derived(repo, kind, version, key).await
    }

    async fn get_derived_many(
        &self,
        repo: &str,
        kind: &str,
        version: u32,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.inner.get_derived_many(repo, kind, version, keys).await
    }

    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_derived(repo, kind, version, key, value).await
    }

    async fn put_token(&self, token: &TokenRecord) -> Result<()> {
        self.inner.put_token(token).await
    }

    async fn get_token(&self, hash: &[u8; 32]) -> Result<Option<TokenRecord>> {
        self.inner.get_token(hash).await
    }

    async fn list_tokens(&self, subject: Option<&str>) -> Result<Vec<TokenRecord>> {
        self.inner.list_tokens(subject).await
    }

    async fn delete_token(&self, id: &str) -> Result<bool> {
        self.inner.delete_token(id).await
    }

    // Memberships change and are cached by the authorization layer, not here
    async fn create_org(&self, org: &OrgRecord) -> Result<()> {
        self.inner.create_org(org).await
    }

    async fn get_org(&self, name: &str) -> Result<Option<OrgRecord>> {
        self.inner.get_org(name).await
    }

    async fn list_orgs(&self) -> Result<Vec<OrgRecord>> {
        self.inner.list_orgs().await
    }

    async fn set_org_visibility(&self, name: &str, visibility: &str) -> Result<bool> {
        self.inner.set_org_visibility(name, visibility).await
    }

    async fn delete_org(&self, name: &str) -> Result<bool> {
        self.inner.delete_org(name).await
    }

    async fn put_org_member(&self, member: &OrgMember) -> Result<()> {
        self.inner.put_org_member(member).await
    }

    async fn delete_org_member(&self, org: &str, subject: &str) -> Result<bool> {
        self.inner.delete_org_member(org, subject).await
    }

    async fn list_org_members(&self, org: &str) -> Result<Vec<OrgMember>> {
        self.inner.list_org_members(org).await
    }

    async fn list_memberships(&self, subject: &str) -> Result<Vec<OrgMember>> {
        self.inner.list_memberships(subject).await
    }

    async fn get_setting(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get_setting(key).await
    }

    async fn put_setting(&self, key: &str, value: &[u8]) -> Result<()> {
        self.inner.put_setting(key, value).await
    }

    // redaction 记录可变，不缓存
    async fn put_redactions(&self, entries: &[Redaction]) -> Result<()> {
        self.inner.put_redactions(entries).await
    }

    async fn get_redactions(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<Redaction>> {
        self.inner.get_redactions(repo, ids).await
    }

    async fn list_redactions(&self, filter: &RedactionFilter) -> Result<Vec<Redaction>> {
        self.inner.list_redactions(filter).await
    }

    async fn object_repos(&self, id: &ObjectId) -> Result<Vec<String>> {
        self.inner.object_repos(id).await
    }

    async fn put_pack_locations(&self, pack: &[u8; 32], objects: &[PackedObject]) -> Result<()> {
        self.inner.put_pack_locations(pack, objects).await
    }

    async fn pack_locations(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, PackLocation>> {
        self.inner.pack_locations(ids).await
    }

    async fn delete_pack_locations(&self, ids: &[ObjectId]) -> Result<()> {
        self.inner.delete_pack_locations(ids).await
    }

    async fn delta_dependents(&self, base: &ObjectId) -> Result<Vec<ObjectId>> {
        self.inner.delta_dependents(base).await
    }

    async fn delete_packs(&self, packs: &[[u8; 32]]) -> Result<()> {
        self.inner.delete_packs(packs).await
    }

    async fn compact_after_repack(&self) -> Result<()> {
        self.inner.compact_after_repack().await
    }

    async fn list_leases(&self) -> Result<Vec<(String, Lease)>> {
        self.inner.list_leases().await
    }

    async fn create_repo_with(&self, info: &RepoInfo) -> Result<()> {
        self.inner.create_repo_with(info).await
    }

    async fn get_repo(&self, repo: &str) -> Result<Option<RepoInfo>> {
        self.inner.get_repo(repo).await
    }

    async fn list_repo_infos(&self, filter: &RepoFilter) -> Result<Vec<RepoInfo>> {
        self.inner.list_repo_infos(filter).await
    }

    async fn update_repo(&self, repo: &str, patch: &RepoPatch) -> Result<RepoInfo> {
        self.inner.update_repo(repo, patch).await
    }

    async fn delete_repo(&self, repo: &str, now_ms: i64) -> Result<()> {
        self.inner.delete_repo(repo, now_ms).await
    }

    async fn restore_repo(&self, repo: &str) -> Result<()> {
        self.inner.restore_repo(repo).await
    }

    async fn purge_repo(&self, repo: &str, batch: usize) -> Result<bool> {
        self.inner.purge_repo(repo, batch).await
    }

    async fn fork_repo(&self, source: &str, target: &str, now_ms: i64) -> Result<()> {
        self.inner.fork_repo(source, target, now_ms).await
    }

    async fn list_ref_events(&self, after_seq: u64, limit: usize, repo: Option<&str>) -> Result<Vec<RefEvent>> {
        self.inner.list_ref_events(after_seq, limit, repo).await
    }

    async fn ref_event_bounds(&self) -> Result<(Option<u64>, u64)> {
        self.inner.ref_event_bounds().await
    }

    async fn prune_ref_events(&self, before_ms: i64) -> Result<u64> {
        self.inner.prune_ref_events(before_ms).await
    }

    async fn delete_ref(&self, repo: &str, name: &str, expect: Option<u64>) -> Result<()> {
        self.inner.delete_ref(repo, name, expect).await
    }

    async fn ref_log(&self, repo: &str, name: &str, before: Option<u64>, limit: usize) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(repo, name, before, limit).await
    }

    async fn put_tag_annotation(&self, repo: &str, name: &str, annotation: &TagAnnotation) -> Result<()> {
        self.inner.put_tag_annotation(repo, name, annotation).await
    }

    async fn tag_annotations(&self, repo: &str) -> Result<HashMap<String, TagAnnotation>> {
        self.inner.tag_annotations(repo).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemStore, testsuite};
    use scion_object::ObjectKind;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn cached() -> CachedMeta {
        CachedMeta::new(Arc::new(MemStore::new()), 1000)
    }

    // 缓存层必须保持全部语义：完整跑一遍一致性测试
    /// 没有实现的可选方法返回「不支持」（后端错误），而不是静默成功。
    #[tokio::test]
    async fn optional_methods_default_to_unsupported() {
        let s = Counting {
            inner: MemStore::new(),
            reads: AtomicUsize::new(0),
        };
        let id = ObjectId::of_blob(b"x");
        let unsupported = |e: crate::Error| matches!(e, crate::Error::Backend(m) if m.contains("not supported"));
        assert!(unsupported(s.put_redactions(&[]).await.expect_err("put")));
        assert!(unsupported(s.get_redactions("r", &[id]).await.expect_err("get")));
        assert!(unsupported(
            s.list_redactions(&RedactionFilter::default()).await.expect_err("list")
        ));
        assert!(unsupported(s.object_repos(&id).await.expect_err("repos")));
        assert!(unsupported(s.list_leases().await.expect_err("leases")));
        assert!(unsupported(s.get_setting("k").await.expect_err("setting")));
        let info = RepoInfo::new("r", 1);
        assert!(unsupported(s.create_repo_with(&info).await.expect_err("create")));
        assert!(unsupported(s.get_repo("r").await.expect_err("get repo")));
        assert!(unsupported(
            s.list_repo_infos(&RepoFilter::default()).await.expect_err("list repos")
        ));
        assert!(unsupported(
            s.update_repo("r", &RepoPatch::default()).await.expect_err("update")
        ));
        assert!(unsupported(s.delete_repo("r", 1).await.expect_err("delete")));
        assert!(unsupported(s.restore_repo("r").await.expect_err("restore")));
        assert!(unsupported(s.purge_repo("r", 1).await.expect_err("purge")));
        assert!(unsupported(s.fork_repo("r", "f", 1).await.expect_err("fork")));
        assert!(unsupported(
            s.delete_ref("r", "main", None).await.expect_err("delete ref")
        ));
    }

    #[tokio::test]
    async fn conformance_meta() {
        testsuite::meta_store(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_object_index() {
        testsuite::object_index(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_changesets_and_refs() {
        testsuite::changesets_and_refs(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_derived() {
        testsuite::derived(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_landing() {
        testsuite::landing(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_leases() {
        testsuite::leases(&cached()).await;
        testsuite::tokens_and_settings(&cached()).await;
        testsuite::orgs_and_members(&cached()).await;
        testsuite::redactions(&cached()).await;
        testsuite::repo_admin(&cached()).await;
        testsuite::ref_events(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_batch_writes() {
        testsuite::batch_writes(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_history_index() {
        testsuite::history_index(&cached()).await;
    }

    #[tokio::test]
    async fn conformance_fp_skip_chain() {
        testsuite::fp_skip_chain(&cached()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        testsuite::concurrent_cas(Arc::new(cached())).await;
    }

    /// 统计底层读取次数的 MetaStore。
    struct Counting {
        inner: MemStore,
        reads: AtomicUsize,
    }

    #[async_trait]
    impl MetaStore for Counting {
        async fn create_repo(&self, repo: &str) -> Result<()> {
            self.inner.create_repo(repo).await
        }
        async fn repo_exists(&self, repo: &str) -> Result<bool> {
            self.inner.repo_exists(repo).await
        }
        async fn list_repos(&self) -> Result<Vec<String>> {
            self.inner.list_repos().await
        }
        async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
            self.inner.get_ref(repo, name).await
        }
        async fn land_commit_as(
            &self,
            repo: &str,
            name: &str,
            expect: RefExpect,
            target: ObjectId,
            landed: &[LandedChange],
            requests: &[IdempotencyRecord],
            actor: &str,
        ) -> Result<RefState> {
            self.inner
                .land_commit_as(repo, name, expect, target, landed, requests, actor)
                .await
        }

        async fn landed_changes(&self, repo: &str, name: &str, change_ids: &[[u8; 16]]) -> Result<Vec<LandedChange>> {
            self.inner.landed_changes(repo, name, change_ids).await
        }

        async fn get_idempotency(&self, repo: &str, request_id: &str) -> Result<Option<IdempotencyRecord>> {
            self.inner.get_idempotency(repo, request_id).await
        }

        async fn expire_idempotency(&self, before_ms: i64) -> Result<u64> {
            self.inner.expire_idempotency(before_ms).await
        }

        async fn acquire_lease(&self, scope: &str, holder: &str, ttl_ms: i64, now_ms: i64) -> Result<Lease> {
            self.inner.acquire_lease(scope, holder, ttl_ms, now_ms).await
        }

        async fn release_lease(&self, scope: &str, holder: &str) -> Result<()> {
            self.inner.release_lease(scope, holder).await
        }
        async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
            self.inner.index_objects(repo, objects).await
        }
        async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
            self.inner.missing_objects(repo, ids).await
        }
        async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>> {
            self.inner.object_sizes(repo, ids).await
        }
        async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
            self.inner.put_changeset(repo, rec).await
        }
        async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_changeset(repo, id).await
        }
        async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
            self.inner.list_refs(repo).await
        }
        async fn changesets_below(
            &self,
            repo: &str,
            max_generation: u64,
            limit: usize,
        ) -> Result<Vec<ChangesetRecord>> {
            self.inner.changesets_below(repo, max_generation, limit).await
        }
        async fn repair_lineage(
            &self,
            repo: &str,
            id: &ObjectId,
            p: &[ObjectId],
            d: u64,
            j: Option<ObjectId>,
        ) -> Result<()> {
            self.inner.repair_lineage(repo, id, p, d, j).await
        }
        async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
            self.inner.index_paths(repo, cs, generation, changes).await
        }
        async fn path_history(
            &self,
            repo: &str,
            path: &str,
            max_generation: u64,
            limit: usize,
        ) -> Result<Vec<PathHistoryEntry>> {
            self.inner.path_history(repo, path, max_generation, limit).await
        }
        async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.get_derived(repo, kind, version, key).await
        }
        async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
            self.inner.put_derived(repo, kind, version, key, value).await
        }
    }

    #[tokio::test]
    async fn caches_existing_records_only() {
        let counting = Arc::new(Counting {
            inner: MemStore::new(),
            reads: AtomicUsize::new(0),
        });
        counting.create_repo("r").await.expect("repo");
        let c = CachedMeta::new(counting.clone(), 100);
        let id = ObjectId::hash(ObjectKind::Changeset, b"c");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
            fp_depth: 1,
            fp_jump: None,
        };

        // 不存在：每次都访问底层，不缓存否定结果
        assert_eq!(c.get_changeset("r", &id).await, Ok(None));
        assert_eq!(c.get_changeset("r", &id).await, Ok(None));
        assert_eq!(counting.reads.load(Ordering::Relaxed), 2);
        assert_eq!(c.cached_changeset("r", &id), None);

        c.put_changeset("r", &rec).await.expect("put");
        assert_eq!(c.cached_changeset("r", &id), None, "writes are not cached");
        assert_eq!(c.get_changeset("r", &id).await, Ok(Some(rec.clone())));
        assert_eq!(c.get_changeset("r", &id).await, Ok(Some(rec.clone())));
        assert_eq!(
            counting.reads.load(Ordering::Relaxed),
            3,
            "second read served from cache"
        );
        assert_eq!(c.cached_changeset("r", &id), Some(rec.clone()));
        // 仓库隔离
        assert_eq!(c.cached_changeset("other", &id), None);

        // 批量预取的记录进入缓存
        let id2 = ObjectId::hash(ObjectKind::Changeset, b"c2");
        let rec2 = ChangesetRecord {
            id: id2,
            generation: 2,
            ..rec
        };
        c.put_changeset("r", &rec2).await.expect("put");
        assert_eq!(c.changesets_below("r", 10, 10).await.expect("below").len(), 2);
        assert_eq!(c.cached_changeset("r", &id2), Some(rec2));
    }

    #[tokio::test]
    async fn warm_loads_every_record_across_windows() {
        let c = cached();
        c.create_repo("r").await.expect("repo");
        let mut ids = Vec::new();
        for n in 1..=10u8 {
            let id = ObjectId::hash(ObjectKind::Changeset, &[n]);
            let rec = ChangesetRecord {
                id,
                root_tree: id,
                parents: vec![],
                generation: u64::from(n),
                fp_depth: 1,
                fp_jump: None,
            };
            c.put_changeset("r", &rec).await.expect("put");
            ids.push(id);
        }
        assert_eq!(c.warm("r", 3).await, Ok(10));
        assert!(ids.iter().all(|id| c.cached_changeset("r", id).is_some()));
        assert_eq!(c.warm("r", 0).await, Ok(10), "window clamped to 1");
        c.create_repo("empty").await.expect("repo");
        assert_eq!(c.warm("empty", 3).await, Ok(0));
        assert!(c.warm("nope", 3).await.is_err());
    }

    #[tokio::test]
    async fn repair_lineage_invalidates_cached_record() {
        let c = cached();
        c.create_repo("r").await.expect("repo");
        let id = ObjectId::hash(ObjectKind::Changeset, b"legacy");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
            fp_depth: 0,
            fp_jump: None,
        };
        c.put_changeset("r", &rec).await.expect("put");
        assert_eq!(
            c.get_changeset("r", &id).await.expect("ok").map(|r| r.fp_depth),
            Some(0)
        );
        c.repair_lineage("r", &id, &[], 1, None).await.expect("set");
        assert_eq!(c.cached_changeset("r", &id), None, "stale entry dropped");
        assert_eq!(
            c.get_changeset("r", &id).await.expect("ok").map(|r| r.fp_depth),
            Some(1)
        );
    }

    #[test]
    fn default_cached_changeset_is_none() {
        let m = MemStore::new();
        assert_eq!(MetaStore::cached_changeset(&m, "r", &ObjectId::of_blob(b"x")), None);
    }
}
