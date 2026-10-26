//! Scion 存储抽象。
//!
//! - [`MetaStore`]：仓库与 ref 等元数据。ref 是系统中**唯一的可变状态**，只能通过 CAS 修改。
//! - [`BlobStore`]：按内容寻址的对象字节，全局只存一份，写入天然幂等；
//!   仓库能看到哪些对象由 MetaStore 的对象索引决定。
//!
//! 本 crate 提供内存实现 [`MemStore`]；SQLite / PostgreSQL / YugabyteDB 与 S3 实现位于其它 crate，
//! 都必须通过 [`testsuite`]（feature `testsuite`）中的一致性测试。

mod blobcache;
mod cached;
mod error;
mod mem;
mod redacted;
mod repo;
#[cfg(any(test, feature = "testsuite"))]
pub mod testsuite;

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::ObjectId;

pub use blobcache::{BlobCacheConfig, CachedBlobs};
pub use cached::{CachedMeta, RECORD_TTL};
pub use error::{Error, Result};
pub use mem::MemStore;
pub use redacted::{RedactedBlobs, redactable, redacted_error};

pub use repo::{MAX_REPO_NAME_LEN, validate_repo_name};

/// 仓库的默认分支。
pub const DEFAULT_BRANCH: &str = "main";

/// 仓库的属性（`repos` 表）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoInfo {
    pub name: String,
    pub default_branch: String,
    pub description: String,
    pub created_at_ms: i64,
    /// 软删除的时间；`None` 为正常的仓库。
    pub deleted_at_ms: Option<i64>,
    /// fork 的来源仓库。
    pub forked_from: Option<String>,
}

impl RepoInfo {
    /// 新仓库：默认分支为 [`DEFAULT_BRANCH`]。
    pub fn new(name: &str, created_at_ms: i64) -> Self {
        Self {
            name: name.to_owned(),
            default_branch: DEFAULT_BRANCH.to_owned(),
            description: String::new(),
            created_at_ms,
            deleted_at_ms: None,
            forked_from: None,
        }
    }
}

/// 修改仓库属性：`None` 的字段不变。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoPatch {
    pub default_branch: Option<String>,
    pub description: Option<String>,
}

/// 列出仓库的条件。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoFilter {
    /// 名字前缀（例如 `opencsg/`）。
    pub prefix: String,
    /// 只返回名字大于它的仓库（分页游标）。
    pub after: Option<String>,
    pub limit: usize,
    /// true 时只列出已删除（等待清除）的仓库。
    pub deleted: bool,
}

/// `prefix` 的上界：仓库名只含 ASCII 字符（[`validate_repo_name`]），追加 0x7F 后大于所有以它开头的名字。
pub fn prefix_upper_bound(prefix: &str) -> String {
    format!("{prefix}\u{7f}")
}

/// 一次 ref 变化（`ref_events` 表）。与 ref 的修改在同一个事务里写入，`seq` 全局递增且与提交顺序一致，
/// 消费方按 `seq` 增量读取，不会漏掉变化。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefEvent {
    pub seq: u64,
    pub repo: String,
    pub name: String,
    /// 变化前的提交；`None` 表示 ref 是新建的。
    pub old_target: Option<ObjectId>,
    /// 变化后的提交；`None` 表示 ref 被删除。
    pub new_target: Option<ObjectId>,
    /// 变化后的版本（删除时为被删除的版本）。
    pub version: u64,
    pub created_at_ms: i64,
}

/// ref 的当前状态。`version` 每次更新加 1，作为 CAS 条件和 fencing token。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefState {
    pub target: ObjectId,
    pub version: u64,
}

/// What an annotated tag says about the commit it names. It counts only while `target` is
/// still the tag ref's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagAnnotation {
    pub target: ObjectId,
    pub tagger: scion_object::Signature,
    pub message: String,
    /// The git tag object it came from (a push or an import), kept byte for byte.
    pub git_object: Option<Vec<u8>>,
}

/// One move of a ref, from its audit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefLogEntry {
    /// The version after the move (1: created).
    pub version: u64,
    pub target: ObjectId,
    pub updated_at_ms: i64,
}

/// 合入过 ref 的 change（`landed` 表）：同一 ref 上每个 change id 只合入一次。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LandedChange {
    pub change_id: [u8; 16],
    /// 客户端提交的 changeset。
    pub changeset: ObjectId,
    /// 进入 ref 历史的 changeset（经过 rebase 时与 `changeset` 不同）。
    pub landed: ObjectId,
    /// 合入后 ref 的版本。
    pub ref_version: u64,
}

/// 租约（`leases` 表）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub holder: String,
    /// 每换一次持有者加 1。
    pub token: u64,
    pub expires_at_ms: i64,
}

/// 写请求的结果（`idempotency` 表），按 (仓库, request id) 保存一段时间。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyRecord {
    pub request_id: String,
    /// 请求参数的指纹：同一 request id 参数不同时拒绝。
    pub fingerprint: Vec<u8>,
    /// 编码后的结果，原样返回给重试的请求。
    pub response: Vec<u8>,
    pub created_at_ms: i64,
}

/// ref CAS 的前置条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefExpect {
    /// ref 必须不存在（创建）。
    Absent,
    /// ref 必须存在且版本等于该值。
    Version(u64),
}

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// 创建仓库；已存在时返回 [`Error::RepoAlreadyExists`]。
    async fn create_repo(&self, repo: &str) -> Result<()>;

    async fn repo_exists(&self, repo: &str) -> Result<bool>;

    /// 按名字字节序返回全部仓库。
    async fn list_repos(&self) -> Result<Vec<String>>;

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>>;

    /// 条件更新 ref：前置条件满足时把 target 设为 `target`，版本加 1（新建时版本为 1）。
    /// 条件不满足时返回 [`Error::RefVersionMismatch`]，其中带上当前版本。
    async fn cas_ref(&self, repo: &str, name: &str, expect: RefExpect, target: ObjectId) -> Result<RefState> {
        self.land_commit(repo, name, expect, target, &[], &[]).await
    }

    /// 合入的原子提交（服务端设计文档 §6.2）：在同一个事务里条件更新 ref、登记合入的 change
    /// （`landed`）和请求结果（`requests`，用于幂等）。ref 条件不满足时什么都不写。
    ///
    /// 同一 ref 上已登记过的 change id、已存在的 request id 保留第一次的记录。
    async fn land_commit(
        &self,
        repo: &str,
        name: &str,
        expect: RefExpect,
        target: ObjectId,
        landed: &[LandedChange],
        requests: &[IdempotencyRecord],
    ) -> Result<RefState>;

    /// 查询 change 是否已经合入过 ref，返回已登记的记录（顺序不定）。
    async fn landed_changes(&self, repo: &str, name: &str, change_ids: &[[u8; 16]]) -> Result<Vec<LandedChange>>;

    /// 读取请求结果（幂等表）。
    async fn get_idempotency(&self, repo: &str, request_id: &str) -> Result<Option<IdempotencyRecord>>;

    /// 删除 `created_at_ms` 早于 `before_ms` 的请求结果，返回删除的条数。
    async fn expire_idempotency(&self, before_ms: i64) -> Result<u64>;

    /// 申请或续约租约（服务端设计文档 §6.2、§10）：租约不存在、已过期（`expires_at_ms <= now_ms`）或
    /// 已由 `holder` 持有时，把它交给 `holder`，有效期到 `now_ms + ttl_ms`；换了持有者时 `token` 加 1。
    /// 返回申请之后的租约（持有者可能是别人）。
    ///
    /// 时间由调用方给出，节点间的时钟偏差会让两个节点短暂地都认为自己持有租约；租约只用来减少
    /// 竞争，正确性由 ref 的条件更新保证。
    async fn acquire_lease(&self, scope: &str, holder: &str, ttl_ms: i64, now_ms: i64) -> Result<Lease>;

    /// 释放 `holder` 持有的租约（让它立即过期）；别人持有时不做任何事。
    async fn release_lease(&self, scope: &str, holder: &str) -> Result<()>;

    /// 把对象登记到仓库的对象索引（幂等）。调用前对象字节必须已写入 [`BlobStore`]：
    /// 索引里有的对象一定可读，这是写入顺序不变式（服务端设计文档 §4.5）。
    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()>;

    /// 返回 `ids` 中**该仓库**索引里没有的对象，保持输入中的相对顺序，重复的 id 只返回一次。
    /// 对象字节全局只存一份，但仓库之间互相看不到对方有哪些对象。
    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>>;

    /// 返回 `ids` 中**该仓库**已登记对象的大小；未登记的不出现在结果里。
    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>>;

    /// 记录一个已创建的 changeset 及其根目录树（服务端设计文档 §4.3 的 `cs_tree`）。
    /// 幂等：同一 id 重复写入时保留第一次的记录（相同 changeset 建出的树必然相同）。
    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()>;

    /// 批量记录 changeset（合入批处理一次写入整批 rebase 结果）。语义与逐个 [`MetaStore::put_changeset`]
    /// 相同；远端数据库应覆盖默认实现，用一个事务完成。
    async fn put_changesets(&self, repo: &str, recs: &[ChangesetRecord]) -> Result<()> {
        for r in recs {
            self.put_changeset(repo, r).await?;
        }
        Ok(())
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>>;

    /// 只查内存缓存、不访问存储的提交记录读取；没有缓存层的实现返回 None。
    /// 查询层在批量预取之前先调用它，命中缓存就不必发出窗口查询。
    fn cached_changeset(&self, _repo: &str, _id: &ObjectId) -> Option<ChangesetRecord> {
        None
    }

    /// 按名字字节序列出仓库的全部 ref。
    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>>;

    /// 代数不大于 `max_generation` 的提交记录，按代数从高到低，最多 `limit` 条（同代数时顺序不定）。
    /// 用于沿历史批量预取，避免逐条查询。
    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>>;

    /// 修复旧数据的谱系字段：父提交列表与跳表（`fp_depth` / `fp_jump`），直接覆盖。
    /// 只供重建索引使用，写入的值都由不可变的提交对象推导而来，所以覆盖是安全的；
    /// 根目录树与代数不变。提交不存在时什么也不做。
    async fn repair_lineage(
        &self,
        repo: &str,
        id: &ObjectId,
        parents: &[ObjectId],
        fp_depth: u64,
        fp_jump: Option<ObjectId>,
    ) -> Result<()>;

    /// 登记提交改动的路径（PathHistory 索引，相对第一个父提交）。幂等。
    /// 应在 [`MetaStore::put_changeset`] 之前写入：记录存在即表示索引完整。
    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()>;

    /// 批量登记多个提交的改动路径，每项为 (提交, 代数, 改动)。语义与逐个 [`MetaStore::index_paths`] 相同。
    async fn index_paths_many(&self, repo: &str, entries: &[(ObjectId, u64, Vec<PathChange>)]) -> Result<()> {
        for (cs, generation, changes) in entries {
            self.index_paths(repo, *cs, *generation, changes).await?;
        }
        Ok(())
    }

    /// 改动了 `path` 或其下任意路径、且代数不大于 `max_generation` 的提交，按代数从高到低，最多 `limit` 条。
    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>>;

    /// 读取派生数据（服务端设计文档 §8）。`kind` 区分种类，`version` 为派生算法版本：
    /// 算法变化时提升版本，旧值自然失效，提交 id 不受影响。
    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 批量读取派生数据，结果与 `keys` 一一对应。远端数据库应覆盖默认实现，用一次往返完成。
    async fn get_derived_many(
        &self,
        repo: &str,
        kind: &str,
        version: u32,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(self.get_derived(repo, kind, version, key).await?);
        }
        Ok(out)
    }

    /// 写入派生数据。派生值由输入唯一决定，重复写入保留第一次的值。
    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()>;

    /// 登记不透明令牌（`tokens` 表，只存哈希）。`id` 已存在时返回 [`Error::Backend`]。
    async fn put_token(&self, _token: &TokenRecord) -> Result<()> {
        Err(unsupported("tokens"))
    }

    /// 按令牌哈希查找。
    async fn get_token(&self, _hash: &[u8; 32]) -> Result<Option<TokenRecord>> {
        Err(unsupported("tokens"))
    }

    /// 列出令牌（`subject` 为 `None` 时列出全部），按创建时间排序。
    async fn list_tokens(&self, _subject: Option<&str>) -> Result<Vec<TokenRecord>> {
        Err(unsupported("tokens"))
    }

    /// 按公开标识删除令牌，返回是否存在。
    async fn delete_token(&self, _id: &str) -> Result<bool> {
        Err(unsupported("tokens"))
    }

    /// Creates an organization; [`Error::OrgAlreadyExists`] if the name is taken.
    async fn create_org(&self, _org: &OrgRecord) -> Result<()> {
        Err(unsupported("organizations"))
    }

    async fn get_org(&self, _name: &str) -> Result<Option<OrgRecord>> {
        Err(unsupported("organizations"))
    }

    /// Every organization, by name.
    async fn list_orgs(&self) -> Result<Vec<OrgRecord>> {
        Err(unsupported("organizations"))
    }

    /// Changes an organization's visibility; false if there is no such organization.
    async fn set_org_visibility(&self, _name: &str, _visibility: &str) -> Result<bool> {
        Err(unsupported("organizations"))
    }

    /// Deletes an organization and its memberships; false if there was none.
    async fn delete_org(&self, _name: &str) -> Result<bool> {
        Err(unsupported("organizations"))
    }

    /// Adds a member, or changes the role of an existing one; [`Error::OrgNotFound`] if the
    /// organization does not exist.
    async fn put_org_member(&self, _member: &OrgMember) -> Result<()> {
        Err(unsupported("organizations"))
    }

    /// Removes a member; false if they were not one.
    async fn delete_org_member(&self, _org: &str, _subject: &str) -> Result<bool> {
        Err(unsupported("organizations"))
    }

    /// An organization's members, by subject.
    async fn list_org_members(&self, _org: &str) -> Result<Vec<OrgMember>> {
        Err(unsupported("organizations"))
    }

    /// The organizations `subject` belongs to, by organization.
    async fn list_memberships(&self, _subject: &str) -> Result<Vec<OrgMember>> {
        Err(unsupported("organizations"))
    }

    /// 读取全局设置（例如访问控制策略）。
    async fn get_setting(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Err(unsupported("settings"))
    }

    /// 写入（覆盖）全局设置。
    async fn put_setting(&self, _key: &str, _value: &[u8]) -> Result<()> {
        Err(unsupported("settings"))
    }

    /// 写入 redaction 记录（`redactions` 表，按 (仓库, 对象) 覆盖）。每条记录由存储分配一个新的
    /// `seq`（全局递增，忽略传入的值），缓存层按 `seq` 增量同步。仓库不存在时返回 [`Error::RepoNotFound`]。
    async fn put_redactions(&self, _entries: &[Redaction]) -> Result<()> {
        Err(unsupported("redactions"))
    }

    /// `ids` 中在该仓库有 redaction 记录的（包括已撤销的），顺序不定。
    async fn get_redactions(&self, _repo: &str, _ids: &[ObjectId]) -> Result<Vec<Redaction>> {
        Err(unsupported("redactions"))
    }

    /// 按条件列出 redaction 记录，按 `seq` 升序。
    async fn list_redactions(&self, _filter: &RedactionFilter) -> Result<Vec<Redaction>> {
        Err(unsupported("redactions"))
    }

    /// Records where the objects of a pack live (see [`PackLocation`]). Idempotent; a later record
    /// for the same object replaces the earlier one (both copies hold the same bytes).
    async fn put_pack_locations(&self, _pack: &[u8; 32], _objects: &[PackedObject]) -> Result<()> {
        Err(unsupported("pack locations"))
    }

    /// Where the packed objects among `ids` live; objects not in any pack are left out.
    async fn pack_locations(&self, _ids: &[ObjectId]) -> Result<HashMap<ObjectId, PackLocation>> {
        Err(unsupported("pack locations"))
    }

    /// Forgets packs that were removed from object storage (no object lives in them any more).
    async fn delete_packs(&self, _packs: &[[u8; 32]]) -> Result<()> {
        Ok(())
    }

    /// Reclaims the space a repack left behind: the pack locations it replaced (most of them),
    /// and the slack in indexes over random ids. Must not hold up reads or writes for long.
    async fn compact_after_repack(&self) -> Result<()> {
        Ok(())
    }

    /// The packed objects stored as deltas against `base`.
    async fn delta_dependents(&self, _base: &ObjectId) -> Result<Vec<ObjectId>> {
        Err(unsupported("pack locations"))
    }

    /// Forgets where `ids` are packed (their pack is being rewritten or a copy is damaged).
    async fn delete_pack_locations(&self, _ids: &[ObjectId]) -> Result<()> {
        Err(unsupported("pack locations"))
    }

    /// 对象索引中有 `id` 的仓库（按名字排序）。用于判断对象字节能否物理删除。
    async fn object_repos(&self, _id: &ObjectId) -> Result<Vec<String>> {
        Err(unsupported("object lookups"))
    }

    /// 列出全部租约（含已过期的），按 scope 排序。
    async fn list_leases(&self) -> Result<Vec<(String, Lease)>> {
        Err(unsupported("lease listings"))
    }

    /// 按给定属性创建仓库（`deleted_at_ms` 忽略）。名字已被使用时返回 [`Error::RepoAlreadyExists`]，
    /// 被已删除、尚未清除的仓库占用时返回 [`Error::RepoDeleted`]。
    async fn create_repo_with(&self, _info: &RepoInfo) -> Result<()> {
        Err(unsupported("repository attributes"))
    }

    /// 仓库属性，包括已删除、尚未清除的仓库。
    async fn get_repo(&self, _repo: &str) -> Result<Option<RepoInfo>> {
        Err(unsupported("repository attributes"))
    }

    /// 按名字字节序列出仓库。
    async fn list_repo_infos(&self, _filter: &RepoFilter) -> Result<Vec<RepoInfo>> {
        Err(unsupported("repository attributes"))
    }

    /// 修改仓库属性，返回修改后的属性。
    async fn update_repo(&self, _repo: &str, _patch: &RepoPatch) -> Result<RepoInfo> {
        Err(unsupported("repository attributes"))
    }

    /// 软删除：之后按名字找不到这个仓库，数据保留到 [`MetaStore::purge_repo`]。
    async fn delete_repo(&self, _repo: &str, _now_ms: i64) -> Result<()> {
        Err(unsupported("repository deletion"))
    }

    /// 恢复软删除的仓库。没有这个名字的已删除仓库时返回 [`Error::RepoNotFound`]。
    async fn restore_repo(&self, _repo: &str) -> Result<()> {
        Err(unsupported("repository deletion"))
    }

    /// 物理清除已删除仓库的数据，每次最多删除约 `batch` 行；返回 true 表示已全部清除（名字可以重新使用）。
    /// 对象字节在仓库之间共享，不在这里删除。
    async fn purge_repo(&self, _repo: &str, _batch: usize) -> Result<bool> {
        Err(unsupported("repository deletion"))
    }

    /// fork：在一个事务里创建 `target`，复制 `source` 的属性、对象索引、提交记录、派生数据、路径历史、
    /// 全部 ref 和有效的 redaction（重新分配 seq）。合入记录和请求结果不复制。
    async fn fork_repo(&self, _source: &str, _target: &str, _now_ms: i64) -> Result<()> {
        Err(unsupported("repository forks"))
    }

    /// 按 `seq` 升序读取 `after_seq` 之后的 ref 变化，最多 `limit` 条；`repo` 不为空时只返回该仓库的。
    async fn list_ref_events(&self, _after_seq: u64, _limit: usize, _repo: Option<&str>) -> Result<Vec<RefEvent>> {
        Err(unsupported("ref events"))
    }

    /// (仍保留的最小 `seq`, 已分配的最大 `seq`)。全部清理后最小值为 `None`，最大值仍是计数器的值，
    /// 消费方据此判断自己落后的部分是否已被清理。
    async fn ref_event_bounds(&self) -> Result<(Option<u64>, u64)> {
        Err(unsupported("ref events"))
    }

    /// 删除早于 `before_ms` 的 ref 变化，返回删除的条数。
    async fn prune_ref_events(&self, _before_ms: i64) -> Result<u64> {
        Err(unsupported("ref events"))
    }

    /// 删除 ref。`expect` 为 `Some(v)` 时要求当前版本等于 v。ref 不存在或版本不符时返回
    /// [`Error::RefVersionMismatch`]（`current` 为当前版本）。
    async fn delete_ref(&self, _repo: &str, _name: &str, _expect: Option<u64>) -> Result<()> {
        Err(unsupported("ref deletion"))
    }

    /// Saves (or replaces) the annotation of tag ref `name`.
    async fn put_tag_annotation(&self, _repo: &str, _name: &str, _annotation: &TagAnnotation) -> Result<()> {
        Err(unsupported("tag annotations"))
    }

    /// The annotations of the repository's tags, by ref name (rows whose target the ref has since
    /// left included: callers compare).
    async fn tag_annotations(&self, _repo: &str) -> Result<HashMap<String, TagAnnotation>> {
        Err(unsupported("tag annotations"))
    }

    /// The ref's moves, newest first: versions below `before` (all when None), at most `limit`.
    /// Deleting a ref deletes them; a ref that never existed has none.
    async fn ref_log(&self, _repo: &str, _name: &str, _before: Option<u64>, _limit: usize) -> Result<Vec<RefLogEntry>> {
        Err(unsupported("ref log"))
    }
}

/// 一个仓库中一个对象的 redaction（服务端设计文档 §9）。对象 id 与历史不变，读取返回 `REDACTED`；
/// 安排清除（purge）后，`janitor` 在宽限期过后物理删除对象字节。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redaction {
    pub repo: String,
    pub id: ObjectId,
    /// 同一次操作的记录共用一个标识。
    pub redaction_id: String,
    pub reason: String,
    pub created_by: String,
    pub created_at_ms: i64,
    /// false 表示已撤销（unredact）：记录保留，缓存层据此恢复读取。
    pub active: bool,
    /// 安排的物理删除时间。
    pub purge_after_ms: Option<i64>,
    /// 对象字节已删除的时间。
    pub purged_at_ms: Option<i64>,
    /// 最后一次修改的序号（全局递增），由存储分配。
    pub seq: u64,
}

/// [`MetaStore::list_redactions`] 的条件；字段都为空时列出全部。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedactionFilter {
    pub repo: Option<String>,
    pub redaction_id: Option<String>,
    /// 只列出 `seq` 大于该值的记录。
    pub after_seq: u64,
    /// 只列出到期待清除的记录：有效、`purge_after_ms <= 该值`、尚未清除。
    pub purge_due_at: Option<i64>,
    /// 最多返回的条数；0 表示不限。
    pub limit: usize,
}

impl RedactionFilter {
    /// 记录是否满足条件（不考虑 `limit`），供内存实现与测试使用。
    pub fn matches(&self, r: &Redaction) -> bool {
        self.repo.as_ref().is_none_or(|x| *x == r.repo)
            && self.redaction_id.as_ref().is_none_or(|x| *x == r.redaction_id)
            && r.seq > self.after_seq
            && self
                .purge_due_at
                .is_none_or(|t| r.active && r.purged_at_ms.is_none() && r.purge_after_ms.is_some_and(|p| p <= t))
    }
}

fn unsupported(what: &str) -> Error {
    Error::Backend(format!("{what} are not supported by this metadata store"))
}

/// 不透明令牌（PAT / service token / 刷新令牌）的记录。令牌本身不保存，只保存 SHA-256。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenRecord {
    /// 公开标识：列出、吊销时使用。
    pub id: String,
    pub hash: [u8; 32],
    /// `pat` 或 `refresh`。
    pub kind: String,
    pub subject: String,
    /// 组名，不能包含换行。
    pub groups: Vec<String>,
    pub description: String,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
}

/// An organization (docs/design/authorization.md): it owns the repositories named `NAME/…`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgRecord {
    pub name: String,
    /// `private` (only members read) or `internal` (every signed-in user reads).
    pub visibility: String,
    pub created_at_ms: i64,
    pub created_by: String,
}

/// A member of an organization: `owner`, `maintainer`, `developer` or `reporter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgMember {
    pub org: String,
    pub subject: String,
    pub role: String,
    pub added_at_ms: i64,
    pub added_by: String,
}

/// 已创建 changeset 的元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetRecord {
    pub id: ObjectId,
    /// 应用 delta 之后的根目录树。
    pub root_tree: ObjectId,
    /// 全部父提交，第一个为 first parent；根提交为空。
    pub parents: Vec<ObjectId>,
    /// 代数：根提交为 1，其余为所有 parent 中最大代数加 1。用于 log 排序和祖先判断剪枝。
    pub generation: u64,
    /// 在第一父提交链上的深度：根提交为 1；0 表示未知（跳表建立之前写入的数据）。
    pub fp_depth: u64,
    /// 第一父提交链上的跳跃指针（skew-binary 跳表）；None 表示指向自己（根提交）或未知。
    pub fp_jump: Option<ObjectId>,
}

impl ChangesetRecord {
    pub fn first_parent(&self) -> Option<ObjectId> {
        self.parents.first().copied()
    }

    /// 跳跃指针指向的提交（根提交指向自己）。
    pub fn jump(&self) -> ObjectId {
        self.fp_jump.unwrap_or(self.id)
    }
}

/// 由第一个父提交计算新提交的 `(fp_depth, fp_jump)`。
///
/// Myers 的 skew-binary 跳表：只存一个跳跃指针，即可在 O(log n) 步内找到第一父提交链上任意深度的祖先。
/// 规则：父提交 p 的跳跃 j = jump(p)、jj = jump(j)；若 depth(p) - depth(j) == depth(j) - depth(jj)
/// 则跳到 jj，否则跳到 p。父提交深度未知时结果也未知（0, None）。
pub async fn fp_skip(
    meta: &dyn MetaStore,
    repo: &str,
    first_parent: Option<ObjectId>,
) -> Result<(u64, Option<ObjectId>)> {
    let Some(p) = first_parent else { return Ok((1, None)) };
    let missing = |id: ObjectId| Error::Backend(format!("changeset {id} referenced but not recorded"));
    let p = meta.get_changeset(repo, &p).await?.ok_or_else(|| missing(p))?;
    if p.fp_depth == 0 {
        return Ok((0, None));
    }
    let j = meta
        .get_changeset(repo, &p.jump())
        .await?
        .ok_or_else(|| missing(p.jump()))?;
    let jj = meta
        .get_changeset(repo, &j.jump())
        .await?
        .ok_or_else(|| missing(j.jump()))?;
    Ok(fp_skip_step(&p, &j, &jj))
}

/// 已知第一父提交 `p`、它的跳跃目标 `j`、`j` 的跳跃目标 `jj` 时，新提交的 (深度, 跳跃指针)。
/// `p` 的深度必须已知（非 0）。供需要从其它来源（例如尚未写入的批处理结果）读记录的调用方使用。
pub fn fp_skip_step(p: &ChangesetRecord, j: &ChangesetRecord, jj: &ChangesetRecord) -> (u64, Option<ObjectId>) {
    let jump = if p.fp_depth - j.fp_depth == j.fp_depth - jj.fp_depth {
        jj.id
    } else {
        p.id
    };
    (p.fp_depth + 1, Some(jump))
}

/// 提交对一个路径的改动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathChange {
    pub path: String,
    pub deleted: bool,
}

/// PathHistory 索引中的一条记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathHistoryEntry {
    pub cs: ObjectId,
    pub generation: u64,
    pub path: String,
    pub deleted: bool,
}

/// `candidate` 是否等于 `path` 或在 `path` 之下（按路径段）。
pub fn path_within(candidate: &str, path: &str) -> bool {
    candidate == path
        || (candidate.len() > path.len() && candidate.starts_with(path) && candidate.as_bytes()[path.len()] == b'/')
}

/// 对象索引中的一条记录。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedObject {
    pub id: ObjectId,
    /// 对象字节数（未压缩）。
    pub size: u64,
}

/// An object inside a pack: its stored bytes are `len` bytes at `offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedObject {
    pub id: ObjectId,
    pub offset: u64,
    pub len: u64,
    /// The object it is stored as a delta against, which must stay readable.
    pub base: Option<ObjectId>,
}

/// Where a packed object lives: the pack's content hash, and the byte range of its stored bytes.
/// Packs are global, like the objects in them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PackLocation {
    pub pack: [u8; 32],
    pub offset: u64,
    pub len: u64,
}

/// 按内容寻址的对象字节存储，**全局共享**：相同内容在所有仓库中只存一份。
/// 仓库能访问哪些对象由 [`MetaStore`] 的对象索引决定。
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// 写入对象。同一 id 重复写入是幂等的。调用方负责保证 `data` 与 `id` 相符。
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()>;

    /// Writes several objects, as one unit where the store supports it (packs). The default
    /// writes them one by one.
    async fn put_many(&self, objects: &[(ObjectId, Bytes)]) -> Result<()> {
        for (id, data) in objects {
            self.put(*id, data.clone()).await?;
        }
        Ok(())
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>>;

    async fn exists(&self, id: &ObjectId) -> Result<bool>;

    /// 物理删除对象（redaction 清除）。对象不存在时不是错误。
    async fn delete(&self, _id: &ObjectId) -> Result<()> {
        Err(Error::Backend(
            "deleting objects is not supported by this blob store".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_within_respects_segments() {
        assert!(path_within("src", "src"));
        assert!(path_within("src/a.rs", "src"));
        assert!(path_within("src/b/c", "src/b"));
        assert!(!path_within("src.txt", "src"));
        assert!(!path_within("srcx/a", "src"));
        assert!(!path_within("sr", "src"));
    }

    #[test]
    fn first_parent_of_record() {
        let id = ObjectId::of_blob(b"x");
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: 1,
            fp_depth: 0,
            fp_jump: None,
        };
        assert_eq!(rec.first_parent(), None);
    }
}
