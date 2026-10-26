//! 历史查询：log、祖先判断、merge-base（服务端设计文档 §8 Query）。
//!
//! 只依赖 [`MetaStore`] 里的提交记录（父提交与代数）；log 需要提交说明时再从 [`BlobStore`] 读取提交对象。
//! 遍历按代数从高到低进行：祖先的代数一定比后代小，因此可以用代数剪枝，并保证分页时不会重复。

mod archive;
mod blame;
mod gitsha;
mod grep;
mod lastcommit;
mod reindex;
mod treestats;

pub use archive::{ArchiveFormat, ArchiveOptions, Emit, redacted_placeholder, write_archive};
pub use blame::{BLAME_KIND, BLAME_VERSION, BlameResult, blame};
pub use gitsha::{
    GIT_BLOB_ID_KIND, GIT_BLOB_KIND, GIT_COMMIT_KIND, GIT_OBJECT_VERSION, GIT_SHA1_EXTENSION, GIT_TREE_ID_KIND,
    GIT_TREE_KIND, GITSHA_KIND, GITSHA_VERSION, SCION_ID_HEADER, cached_git_shas, derive_git_sha, git_commit_body,
    git_commit_body_plain, git_hash, git_sha_of, git_tree_body, index_git_sha, lookup_git_sha, parse_git_sha,
    tree_git_sha,
};
pub use grep::{GrepMatch, GrepOptions, GrepResult, GrepStart, MAX_LINE, grep};
pub use lastcommit::{LAST_COMMIT_KIND, LAST_COMMIT_VERSION, last_commits};
pub use reindex::{ReindexStats, all_records, describe, reindex};
pub use treestats::{TREE_STATS_KIND, TREE_STATS_VERSION, TreeStats, tree_stats};

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use scion_object::{Changeset, ObjectId};
use scion_store::{BlobStore, ChangesetRecord, MetaStore};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QueryError {
    /// 提交记录不存在（id 不属于该仓库）。
    #[error("changeset {0} not found")]
    NotFound(ObjectId),
    /// 提交对象缺失或无法解码。
    #[error("changeset {0} is missing or corrupt: {1}")]
    Corrupt(ObjectId, String),
    #[error(transparent)]
    Store(#[from] scion_store::Error),
    #[error("path {0:?} not found")]
    PathNotFound(String),
    #[error("{0:?} is a directory")]
    NotAFile(String),
    /// 目录树缺失或损坏，或读取目录树时存储出错。
    #[error(transparent)]
    Tree(#[from] scion_tree::TreeError),
    /// 对象不存在（例如文件内容已被清除）。
    #[error("object {0} is missing")]
    MissingObject(ObjectId),
    /// 生成归档失败（例如条目不能用该格式表示）。
    #[error("archive: {0}")]
    Archive(String),
}

pub type Result<T> = std::result::Result<T, QueryError>;

/// 一次查询内的提交记录缓存。
pub(crate) struct Graph<'a> {
    meta: &'a dyn MetaStore,
    repo: &'a str,
    records: HashMap<ObjectId, ChangesetRecord>,
}

impl<'a> Graph<'a> {
    pub(crate) fn new(meta: &'a dyn MetaStore, repo: &'a str) -> Self {
        Self {
            meta,
            repo,
            records: HashMap::new(),
        }
    }

    pub(crate) async fn get(&mut self, id: ObjectId) -> Result<&ChangesetRecord> {
        if !self.records.contains_key(&id) {
            let rec = self
                .meta
                .get_changeset(self.repo, &id)
                .await?
                .ok_or(QueryError::NotFound(id))?;
            self.records.insert(id, rec);
        }
        Ok(&self.records[&id])
    }

    /// 已知 `id` 的代数不大于 `below` 时使用：缓存未命中则批量预取 `below` 以下的一个窗口，
    /// 沿历史往回走时把逐条查询变成每 [`PREFETCH_WINDOW`] 条一次。
    pub(crate) async fn get_below(&mut self, id: ObjectId, below: u64) -> Result<&ChangesetRecord> {
        if !self.records.contains_key(&id) {
            // 进程级缓存命中就不必发出窗口查询
            if let Some(rec) = self.meta.cached_changeset(self.repo, &id) {
                self.records.insert(id, rec);
                return Ok(&self.records[&id]);
            }
            for rec in self.meta.changesets_below(self.repo, below, PREFETCH_WINDOW).await? {
                self.records.entry(rec.id).or_insert(rec);
            }
        }
        self.get(id).await
    }
}

/// 第一父提交链上深度为 `depth` 的祖先（从 `x` 往回，O(log n) 步）。
/// 遇到深度未知的记录（跳表建立之前写入的数据）或 `depth` 不在链上时返回 None。
pub(crate) async fn fp_ancestor_at(g: &mut Graph<'_>, x: ObjectId, depth: u64) -> Result<Option<ObjectId>> {
    let mut cur = g.get(x).await?.clone();
    if cur.fp_depth == 0 || depth == 0 || depth > cur.fp_depth {
        return Ok(None);
    }
    while cur.fp_depth > depth {
        let jump = g.get(cur.jump()).await?.clone();
        if jump.fp_depth == 0 {
            return Ok(None);
        }
        cur = if jump.fp_depth >= depth {
            jump
        } else {
            match cur.first_parent() {
                Some(p) => g.get(p).await?.clone(),
                None => return Ok(None),
            }
        };
    }
    Ok(Some(cur.id))
}

/// `x` 是否在 `c` 的第一父提交链上（含相等）。None 表示无法判断（深度未知）。
pub(crate) async fn on_first_parent_chain(g: &mut Graph<'_>, c: ObjectId, x: ObjectId) -> Result<Option<bool>> {
    let depth = g.get(x).await?.fp_depth;
    if depth == 0 || g.get(c).await?.fp_depth == 0 {
        return Ok(None);
    }
    Ok(Some(fp_ancestor_at(g, c, depth).await? == Some(x)))
}

/// 批量预取的窗口大小。
pub const PREFETCH_WINDOW: usize = 1024;

/// 按路径过滤的 log 一次从索引读取的候选提交数。
pub const PATH_CANDIDATES: usize = 4096;

/// `ancestor` 是否是 `descendant` 的祖先（含相等）。
pub async fn is_ancestor(meta: &dyn MetaStore, repo: &str, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
    let mut g = Graph::new(meta, repo);
    is_ancestor_in(&mut g, ancestor, descendant).await
}

async fn is_ancestor_in(g: &mut Graph<'_>, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
    // 快速路径：在第一父提交链上（最常见的情况，O(log n)）
    if on_first_parent_chain(g, descendant, ancestor).await? == Some(true) {
        return Ok(true);
    }
    let target_gen = g.get(ancestor).await?.generation;
    let mut queue = vec![(descendant, u64::MAX)];
    let mut seen = HashSet::new();
    while let Some((c, below)) = queue.pop() {
        if c == ancestor {
            return Ok(true);
        }
        if !seen.insert(c) {
            continue;
        }
        let rec = g.get_below(c, below).await?.clone();
        // 代数不大于目标的提交不可能以目标为祖先（相等时只能是它自己，上面已判断）
        if rec.generation <= target_gen {
            continue;
        }
        let below = rec.generation.saturating_sub(1);
        queue.extend(rec.parents.iter().map(|p| (*p, below)));
    }
    Ok(false)
}

/// 一次 ref 变化引入的提交：从 `new` 可达、从 `old` 不可达的提交，按代数从新到旧，最多 `limit` 个。
/// 返回 (提交, 是否被截断)。`old` 为空（新建的 ref）时返回 `new` 的历史。
pub async fn new_commits(
    meta: &dyn MetaStore,
    repo: &str,
    old: Option<ObjectId>,
    new: ObjectId,
    limit: usize,
) -> Result<(Vec<ChangesetRecord>, bool)> {
    let mut g = Graph::new(meta, repo);
    let old_gen = match old {
        Some(o) => Some((o, g.get(o).await?.generation)),
        None => None,
    };
    let mut queue: BinaryHeap<(u64, ObjectId)> = BinaryHeap::new();
    let mut queued = HashSet::from([new]);
    queue.push((g.get(new).await?.generation, new));
    let mut out = Vec::new();
    while let Some((generation, id)) = queue.pop() {
        if let Some((o, og)) = old_gen {
            // 代数不大于 old 的提交才可能是 old 的祖先
            if id == o || (generation <= og && is_ancestor_in(&mut g, id, o).await?) {
                continue;
            }
        }
        if out.len() == limit {
            return Ok((out, true));
        }
        let rec = g.get(id).await?.clone();
        for p in &rec.parents {
            if queued.insert(*p) {
                queue.push((g.get(*p).await?.generation, *p));
            }
        }
        out.push(rec);
    }
    Ok((out, false))
}

const PARENT1: u8 = 1;
const PARENT2: u8 = 2;
const STALE: u8 = 4;
const RESULT: u8 = 8;

/// `a` 与 `b` 的最佳公共祖先（互不为祖先），按代数从高到低排序。没有公共祖先时为空。
pub async fn merge_bases(meta: &dyn MetaStore, repo: &str, a: ObjectId, b: ObjectId) -> Result<Vec<ObjectId>> {
    let mut g = Graph::new(meta, repo);
    if a == b {
        g.get(a).await?;
        return Ok(vec![a]);
    }
    let mut flags: HashMap<ObjectId, u8> = HashMap::new();
    let mut queue: BinaryHeap<(u64, ObjectId)> = BinaryHeap::new();
    for (id, f) in [(a, PARENT1), (b, PARENT2)] {
        let generation_of = g.get(id).await?.generation;
        *flags.entry(id).or_default() |= f;
        queue.push((generation_of, id));
    }
    let mut candidates = Vec::new();
    // 队列里还有未被标记为 STALE 的提交时继续
    while queue.iter().any(|(_, id)| flags[id] & STALE == 0) {
        let Some((_, c)) = queue.pop() else { break };
        let mut f = flags[&c] & (PARENT1 | PARENT2 | STALE);
        if f & (PARENT1 | PARENT2) == (PARENT1 | PARENT2) {
            if flags[&c] & RESULT == 0 {
                *flags.entry(c).or_default() |= RESULT;
                candidates.push(c);
            }
            f |= STALE;
        }
        let rec = g.get(c).await?.clone();
        let below = rec.generation.saturating_sub(1);
        for p in rec.parents {
            let pf = flags.entry(p).or_default();
            if *pf & f == f {
                continue;
            }
            *pf |= f;
            let generation_of = g.get_below(p, below).await?.generation;
            queue.push((generation_of, p));
        }
    }
    // 去掉是其它候选祖先的候选
    let mut bases = Vec::new();
    for (i, c) in candidates.iter().enumerate() {
        let mut redundant = false;
        for (j, other) in candidates.iter().enumerate() {
            if i != j && is_ancestor_in(&mut g, *c, *other).await? {
                redundant = true;
                break;
            }
        }
        if !redundant {
            bases.push(*c);
        }
    }
    let mut keyed = Vec::with_capacity(bases.len());
    for id in bases {
        keyed.push((Reverse(g.get(id).await?.generation), id));
    }
    keyed.sort();
    Ok(keyed.into_iter().map(|(_, id)| id).collect())
}

/// log 中的一条：提交记录与提交对象。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub record: ChangesetRecord,
    pub changeset: Changeset,
}

/// 一页 log。`next` 为下一页的起点（尚未展开的前沿提交）；为 None 表示没有更多。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub entries: Vec<LogEntry>,
    pub next: Option<Vec<ObjectId>>,
}

/// 提交是否改动了 `path`（相对第一个父提交）：改动的路径等于它、在它下面，或是它的上级。
fn touches(cs: &Changeset, path: &str) -> bool {
    let under = |q: &str, dir: &str| q.len() > dir.len() && q.starts_with(dir) && q.as_bytes()[dir.len()] == b'/';
    cs.delta
        .iter()
        .any(|c| c.path == path || under(&c.path, path) || under(path, &c.path))
}

/// 读取并解码提交对象。
pub async fn load_changeset(blobs: &dyn BlobStore, id: ObjectId) -> Result<Changeset> {
    let bytes = blobs
        .get(&id)
        .await?
        .ok_or_else(|| QueryError::Corrupt(id, "object not found".into()))?;
    Changeset::decode(&bytes).map_err(|e| QueryError::Corrupt(id, e.to_string()))
}

/// 从 `start`（一个或多个提交）开始按代数从新到旧列出历史，覆盖所有父提交。
///
/// - `path` 非空时只返回改动了该路径的提交（相对第一个父提交，与 delta 的语义一致）；
/// - 每页最多 `limit` 条，最多检查 `max_scan` 个提交；扫描到上限时提前返回，`next` 仍可继续。
pub async fn log(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    start: &[ObjectId],
    path: Option<&str>,
    limit: usize,
    max_scan: usize,
) -> Result<LogPage> {
    log_with(meta, blobs, repo, start, path, limit, max_scan, PATH_CANDIDATES).await
}

#[allow(clippy::too_many_arguments)]
async fn log_with(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    start: &[ObjectId],
    path: Option<&str>,
    limit: usize,
    max_scan: usize,
    candidate_limit: usize,
) -> Result<LogPage> {
    let mut g = Graph::new(meta, repo);
    let mut queue: BinaryHeap<(u64, ObjectId)> = BinaryHeap::new();
    let mut queued = HashSet::new();
    for id in start {
        if queued.insert(*id) {
            queue.push((g.get(*id).await?.generation, *id));
        }
    }
    let path = path.filter(|p| !p.is_empty());
    // 按路径过滤时先查 PathHistory 索引，只解码命中的提交；索引中查不到任何记录（例如索引之前写入的
    // 旧数据）时退回逐个解码检查
    let top = queue.peek().map_or(0, |(generation, _)| *generation);
    let candidates = match path {
        Some(p) => {
            let hist = meta.path_history(repo, p, top, candidate_limit).await?;
            let truncated = hist.len() == candidate_limit;
            let floor = hist.last().map_or(0, |e| e.generation);
            let set: HashSet<ObjectId> = hist.into_iter().map(|e| e.cs).collect();
            (!set.is_empty()).then_some((set, truncated, floor))
        }
        None => None,
    };
    let mut entries = Vec::new();
    let mut scanned = 0;
    while entries.len() < limit.max(1) && scanned < max_scan.max(1) {
        // 候选被截断时，低于最后一个候选代数的部分留到下一页（下一页会重新读取索引）
        if let (Some((_, true, floor)), Some((generation, _))) = (&candidates, queue.peek()) {
            if generation < floor {
                break;
            }
        }
        let Some((_, id)) = queue.pop() else { break };
        scanned += 1;
        let record = g.get(id).await?.clone();
        let below = record.generation.saturating_sub(1);
        for p in &record.parents {
            if queued.insert(*p) {
                queue.push((g.get_below(*p, below).await?.generation, *p));
            }
        }
        let wanted = match (&candidates, path) {
            (None, None) => true,
            (Some((set, _, _)), _) => set.contains(&id),
            (None, Some(_)) => false,
        };
        if wanted {
            entries.push(LogEntry {
                record,
                changeset: load_changeset(blobs, id).await?,
            });
        } else if candidates.is_none() {
            if let Some(p) = path {
                let changeset = load_changeset(blobs, id).await?;
                if touches(&changeset, p) {
                    entries.push(LogEntry { record, changeset });
                }
            }
        }
    }
    let next = if queue.is_empty() {
        None
    } else {
        let mut frontier: Vec<_> = queue.into_sorted_vec().into_iter().rev().map(|(_, id)| id).collect();
        frontier.dedup();
        Some(frontier)
    };
    Ok(LogPage { entries, next })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::{EntryMode, FileChange, Signature};
    use scion_store::MemStore;
    use std::collections::BTreeMap;

    pub(crate) const REPO: &str = "r";

    /// 构造提交图的小工具：直接写入提交对象和记录（不建树，root_tree 用占位）。
    pub(crate) struct Builder {
        pub(crate) store: MemStore,
        n: u32,
        /// 是否写 PathHistory 索引（false 模拟索引建立之前的旧数据）。
        indexed: bool,
    }

    impl Builder {
        pub(crate) async fn new() -> Self {
            Self::with_index(true).await
        }

        pub(crate) async fn with_index(indexed: bool) -> Self {
            let store = MemStore::new();
            store.create_repo(REPO).await.expect("repo");
            Self { store, n: 0, indexed }
        }

        pub(crate) async fn commit(&mut self, parents: &[ObjectId], paths: &[&str]) -> ObjectId {
            self.n += 1;
            let sig = Signature {
                name: "a".into(),
                email: "a@x".into(),
                time_ms: 0,
                tz_offset_minutes: 0,
            };
            let delta = paths
                .iter()
                .map(|p| FileChange::put(*p, EntryMode::File, ObjectId::of_blob(b"x"), 1))
                .collect();
            let cs = Changeset::new(
                parents.to_vec(),
                sig.clone(),
                sig,
                format!("c{}", self.n),
                {
                    let mut id = [0u8; 16];
                    id[..4].copy_from_slice(&self.n.to_le_bytes());
                    id
                },
                delta,
                BTreeMap::new(),
            )
            .expect("valid");
            let bytes = Bytes::from(cs.encode());
            let id = cs.id();
            self.store.put(id, bytes).await.expect("put");
            let mut generation = 0;
            for p in parents {
                generation = generation.max(
                    self.store
                        .get_changeset(REPO, p)
                        .await
                        .expect("ok")
                        .expect("parent")
                        .generation,
                );
            }
            let (fp_depth, fp_jump) = scion_store::fp_skip(&self.store, REPO, parents.first().copied())
                .await
                .expect("skip");
            let rec = ChangesetRecord {
                id,
                root_tree: ObjectId::hash(scion_object::ObjectKind::Tree, b""),
                parents: parents.to_vec(),
                generation: generation + 1,
                fp_depth,
                fp_jump,
            };
            if self.indexed {
                let changes: Vec<_> = paths
                    .iter()
                    .map(|p| scion_store::PathChange {
                        path: (*p).to_owned(),
                        deleted: false,
                    })
                    .collect();
                self.store
                    .index_paths(REPO, id, generation + 1, &changes)
                    .await
                    .expect("index");
            }
            self.store.put_changeset(REPO, &rec).await.expect("record");
            id
        }
    }

    fn ids(page: &LogPage) -> Vec<ObjectId> {
        page.entries.iter().map(|e| e.record.id).collect()
    }

    /// a ─ b ─ c ─────── m ─ n
    ///      \           /
    ///       d ─ e ────'
    async fn diamond() -> (Builder, [ObjectId; 7]) {
        diamond_with(true).await
    }

    async fn diamond_with(indexed: bool) -> (Builder, [ObjectId; 7]) {
        let mut b = Builder::with_index(indexed).await;
        let a = b.commit(&[], &["root.txt"]).await;
        let bb = b.commit(&[a], &["src/b.rs"]).await;
        let c = b.commit(&[bb], &["src/c.rs"]).await;
        let d = b.commit(&[bb], &["docs/d.md"]).await;
        let e = b.commit(&[d], &["docs/e.md"]).await;
        let m = b.commit(&[c, e], &[]).await;
        let n = b.commit(&[m], &["src/n.rs"]).await;
        (b, [a, bb, c, d, e, m, n])
    }

    #[tokio::test]
    async fn new_commits_between_two_tips() {
        let (b, [a, bb, c, d, e, m, n]) = diamond().await;
        let s = &b.store;
        let ids = |v: (Vec<ChangesetRecord>, bool)| (v.0.into_iter().map(|r| r.id).collect::<Vec<_>>(), v.1);
        assert_eq!(
            ids(new_commits(s, REPO, Some(c), n, 10).await.expect("ok")),
            (vec![n, m, e, d], false)
        );
        assert_eq!(
            ids(new_commits(s, REPO, Some(m), n, 10).await.expect("ok")),
            (vec![n], false)
        );
        assert_eq!(
            ids(new_commits(s, REPO, Some(n), n, 10).await.expect("ok")),
            (vec![], false)
        );
        // 回退（新提交是旧提交的祖先）：没有新提交
        assert_eq!(
            ids(new_commits(s, REPO, Some(n), bb, 10).await.expect("ok")),
            (vec![], false)
        );
        // 分叉：只返回新分支上独有的提交
        let (got, _) = ids(new_commits(s, REPO, Some(c), e, 10).await.expect("ok"));
        assert_eq!(got, [e, d]);
        // 新建的 ref：全部历史；超过上限时截断
        let (all, truncated) = ids(new_commits(s, REPO, None, n, 100).await.expect("ok"));
        assert_eq!((all.len(), truncated), (7, false));
        assert_eq!(all.last(), Some(&a));
        assert_eq!(
            ids(new_commits(s, REPO, None, n, 2).await.expect("ok")),
            (vec![n, m], true)
        );
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert!(new_commits(s, REPO, Some(ghost), n, 10).await.is_err());
    }

    #[tokio::test]
    async fn is_ancestor_cases() {
        let (b, [a, bb, c, d, e, m, n]) = diamond().await;
        let s = &b.store;
        assert!(is_ancestor(s, REPO, a, n).await.expect("ok"));
        assert!(is_ancestor(s, REPO, e, m).await.expect("ok"), "via second parent");
        assert!(is_ancestor(s, REPO, n, n).await.expect("ok"), "reflexive");
        assert!(!is_ancestor(s, REPO, c, e).await.expect("ok"), "siblings");
        assert!(!is_ancestor(s, REPO, n, a).await.expect("ok"), "reversed");
        assert!(is_ancestor(s, REPO, bb, d).await.expect("ok"));
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(is_ancestor(s, REPO, ghost, n).await, Err(QueryError::NotFound(ghost)));
    }

    #[tokio::test]
    async fn first_parent_chain_queries() {
        let (b, [a, bb, c, d, e, m, n]) = diamond().await;
        let mut g = Graph::new(&b.store, REPO);
        assert_eq!(fp_ancestor_at(&mut g, n, 1).await.expect("ok"), Some(a));
        assert_eq!(fp_ancestor_at(&mut g, n, 4).await.expect("ok"), Some(m));
        assert_eq!(fp_ancestor_at(&mut g, n, 5).await.expect("ok"), Some(n));
        assert_eq!(fp_ancestor_at(&mut g, n, 6).await.expect("ok"), None, "deeper than n");
        assert_eq!(fp_ancestor_at(&mut g, n, 0).await.expect("ok"), None);
        assert_eq!(on_first_parent_chain(&mut g, n, c).await.expect("ok"), Some(true));
        assert_eq!(on_first_parent_chain(&mut g, n, bb).await.expect("ok"), Some(true));
        assert_eq!(
            on_first_parent_chain(&mut g, n, e).await.expect("ok"),
            Some(false),
            "second parent side"
        );
        assert_eq!(on_first_parent_chain(&mut g, e, d).await.expect("ok"), Some(true));
        // 深度未知的旧数据：无法判断
        let legacy = ObjectId::hash(scion_object::ObjectKind::Changeset, b"legacy");
        let rec = ChangesetRecord {
            id: legacy,
            root_tree: legacy,
            parents: vec![],
            generation: 1,
            fp_depth: 0,
            fp_jump: None,
        };
        b.store.put_changeset(REPO, &rec).await.expect("put");
        assert_eq!(on_first_parent_chain(&mut g, n, legacy).await.expect("ok"), None);
        assert_eq!(fp_ancestor_at(&mut g, legacy, 1).await.expect("ok"), None);
    }

    /// 长链上的跳表：任意深度的祖先都能找到，结果与逐个往回走一致。
    #[tokio::test]
    async fn fp_ancestor_at_matches_linear_walk_on_long_chain() {
        let mut b = Builder::new().await;
        let mut chain = vec![b.commit(&[], &["f"]).await];
        for _ in 0..200 {
            let tip = *chain.last().expect("tip");
            chain.push(b.commit(&[tip], &["f"]).await);
        }
        let tip = *chain.last().expect("tip");
        let mut g = Graph::new(&b.store, REPO);
        for (i, id) in chain.iter().enumerate() {
            assert_eq!(
                fp_ancestor_at(&mut g, tip, i as u64 + 1).await.expect("ok"),
                Some(*id),
                "depth {}",
                i + 1
            );
        }
        assert!(is_ancestor(&b.store, REPO, chain[0], tip).await.expect("ok"));
    }

    #[tokio::test]
    async fn merge_bases_cases() {
        let (b, [a, bb, c, _d, e, m, n]) = diamond().await;
        let s = &b.store;
        assert_eq!(merge_bases(s, REPO, c, e).await.expect("ok"), vec![bb]);
        assert_eq!(
            merge_bases(s, REPO, n, e).await.expect("ok"),
            vec![e],
            "one is ancestor of the other"
        );
        assert_eq!(merge_bases(s, REPO, m, m).await.expect("ok"), vec![m]);
        assert_eq!(merge_bases(s, REPO, a, n).await.expect("ok"), vec![a]);
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(merge_bases(s, REPO, ghost, n).await, Err(QueryError::NotFound(ghost)));
        assert_eq!(
            merge_bases(s, REPO, ghost, ghost).await,
            Err(QueryError::NotFound(ghost))
        );
    }

    #[tokio::test]
    async fn merge_bases_criss_cross_has_two_bases() {
        // x ─ p1 ─ q1          两个分支互相合并后再分叉：最佳公共祖先有两个
        //  \    ╳
        //   p2 ─ q2
        let mut b = Builder::new().await;
        let x = b.commit(&[], &["x"]).await;
        let p1 = b.commit(&[x], &["p1"]).await;
        let p2 = b.commit(&[x], &["p2"]).await;
        let q1 = b.commit(&[p1, p2], &["q1"]).await;
        let q2 = b.commit(&[p2, p1], &["q2"]).await;
        let mut bases = merge_bases(&b.store, REPO, q1, q2).await.expect("ok");
        bases.sort();
        let mut want = vec![p1, p2];
        want.sort();
        assert_eq!(bases, want);
        // 没有公共祖先
        let lonely = b.commit(&[], &["other"]).await;
        assert!(merge_bases(&b.store, REPO, lonely, q1).await.expect("ok").is_empty());
    }

    #[tokio::test]
    async fn log_orders_by_generation_and_covers_merges() {
        let (b, [a, bb, c, d, e, m, n]) = diamond().await;
        let page = log(&b.store, &b.store, REPO, &[n], None, 100, 1000).await.expect("log");
        let got = ids(&page);
        assert_eq!(got.len(), 7);
        assert_eq!(&got[..2], &[n, m]);
        assert_eq!(got[6], a);
        assert!(got.contains(&c) && got.contains(&d) && got.contains(&e) && got.contains(&bb));
        assert_eq!(page.next, None);
        assert_eq!(page.entries[0].changeset.message, "c7");
    }

    #[tokio::test]
    async fn log_paginates_without_duplicates() {
        let (b, all) = diamond().await;
        let mut start = vec![all[6]];
        let mut seen = Vec::new();
        loop {
            let page = log(&b.store, &b.store, REPO, &start, None, 2, 1000).await.expect("log");
            assert!(page.entries.len() <= 2);
            seen.extend(ids(&page));
            match page.next {
                Some(n) => start = n,
                None => break,
            }
        }
        let unique: HashSet<_> = seen.iter().collect();
        assert_eq!(seen.len(), 7);
        assert_eq!(unique.len(), 7, "no duplicates across pages");
    }

    #[tokio::test]
    async fn log_filters_by_path() {
        // 有索引与无索引（逐个解码）结果一致
        for indexed in [true, false] {
            let (b, [a, bb, c, _d, _e, _m, n]) = diamond_with(indexed).await;
            let page = log(&b.store, &b.store, REPO, &[n], Some("src"), 100, 1000)
                .await
                .expect("log");
            assert_eq!(ids(&page), vec![n, c, bb], "indexed={indexed}");
            let page = log(&b.store, &b.store, REPO, &[n], Some("root.txt"), 100, 1000)
                .await
                .expect("log");
            assert_eq!(ids(&page), vec![a], "indexed={indexed}");
            let page = log(&b.store, &b.store, REPO, &[n], Some("docs"), 1, 1000)
                .await
                .expect("log");
            assert_eq!(page.entries.len(), 1, "page size respected, indexed={indexed}");
            assert!(page.next.is_some());
        }
        let (b, [_a, _bb, c, _d, _e, _m, n]) = diamond().await;
        let page = log(&b.store, &b.store, REPO, &[n], Some("src/c.rs/deeper"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(ids(&page), vec![c], "a file that is a parent of the path counts");
        let page = log(&b.store, &b.store, REPO, &[n], Some(""), 1, 1000)
            .await
            .expect("empty path = no filter");
        assert_eq!(ids(&page), vec![n]);
    }

    /// 候选被截断时，下一页会重新读取索引，不遗漏也不重复。
    #[tokio::test]
    async fn log_index_candidates_truncated_across_pages() {
        let (b, [_a, bb, c, _d, _e, _m, n]) = diamond().await;
        let mut start = vec![n];
        let mut seen = Vec::new();
        for _ in 0..20 {
            let page = log_with(&b.store, &b.store, REPO, &start, Some("src"), 100, 1000, 1)
                .await
                .expect("log");
            seen.extend(ids(&page));
            match page.next {
                Some(next) => start = next,
                None => break,
            }
        }
        assert_eq!(seen, vec![n, c, bb]);
    }

    #[tokio::test]
    async fn log_scan_budget_returns_partial_page() {
        let (b, [_a, _bb, _c, _d, _e, _m, n]) = diamond().await;
        let page = log(&b.store, &b.store, REPO, &[n], Some("root.txt"), 100, 3)
            .await
            .expect("log");
        assert!(page.entries.is_empty(), "root.txt changed only in the oldest commit");
        let next = page.next.expect("can continue");
        let page = log(&b.store, &b.store, REPO, &next, Some("root.txt"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(page.entries.len(), 1);
    }

    #[tokio::test]
    async fn log_errors() {
        let mut b = Builder::new().await;
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert_eq!(
            log(&b.store, &b.store, REPO, &[ghost], None, 10, 10).await,
            Err(QueryError::NotFound(ghost))
        );
        // 记录存在但提交对象缺失
        let rec = ChangesetRecord {
            id: ghost,
            root_tree: ghost,
            parents: vec![],
            generation: 1,
            fp_depth: 0,
            fp_jump: None,
        };
        b.store.put_changeset(REPO, &rec).await.expect("record");
        assert!(matches!(
            log(&b.store, &b.store, REPO, &[ghost], None, 10, 10).await,
            Err(QueryError::Corrupt(..))
        ));
        // 提交对象无法解码
        let junk = Bytes::from_static(b"not a changeset");
        let bad = ObjectId::hash(scion_object::ObjectKind::Changeset, &junk);
        b.store.put(bad, junk).await.expect("put");
        b.store
            .put_changeset(REPO, &ChangesetRecord { id: bad, ..rec })
            .await
            .expect("record");
        assert!(matches!(
            log(&b.store, &b.store, REPO, &[bad], None, 10, 10).await,
            Err(QueryError::Corrupt(..))
        ));
        // 仓库不存在
        let _ = b.commit(&[], &["a"]).await;
        assert!(matches!(
            log(&b.store, &b.store, "nope", &[bad], None, 10, 10).await,
            Err(QueryError::Store(_))
        ));
    }

    #[test]
    fn touches_rules() {
        let sig = Signature {
            name: "a".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        let cs = Changeset::new(
            vec![],
            sig.clone(),
            sig,
            "m",
            [0; 16],
            vec![FileChange::delete("src/lib.rs")],
            BTreeMap::new(),
        )
        .expect("valid");
        assert!(touches(&cs, "src/lib.rs"));
        assert!(touches(&cs, "src"));
        assert!(!touches(&cs, "sr"), "prefix must end at a path boundary");
        assert!(!touches(&cs, "src/lib"));
        assert!(touches(&cs, "src/lib.rs/x"));
    }

    #[test]
    fn error_display() {
        assert!(QueryError::PathNotFound("p".into()).to_string().contains("not found"));
        assert!(QueryError::NotAFile("d".into()).to_string().contains("directory"));
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        assert!(QueryError::NotFound(id).to_string().contains("not found"));
        assert!(QueryError::Corrupt(id, "bad".into()).to_string().contains("corrupt"));
    }
}
