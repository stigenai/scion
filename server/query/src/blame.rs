//! Blame：文件每一行最后是在哪个提交引入的。
//!
//! 沿第一父提交链往回，找出文件每个版本的引入提交；从最老的版本往新算，相邻版本做行级 diff，
//! 未变的行沿用上一版的归属，新增的行归到引入提交。每个版本的结果按 (引入提交, 路径) 存为派生数据，
//! 下次往回走到已有结果就停止，只计算增量。
//!
//! M2 只沿第一父提交链追溯：经由合并进来的改动归到合并提交上。

use std::collections::HashMap;

use async_trait::async_trait;
use scion_object::{EntryMode, ObjectId, Tree};
use scion_store::{BlobStore, MetaStore};
use scion_tree::{TreeError, TreeSource, lookup};

use crate::{Graph, QueryError, Result, on_first_parent_chain};

/// 派生数据种类与算法版本。
pub const BLAME_KIND: &str = "blame";
/// 2: lines are carried with an exact (LCS) diff where affordable, matching git more closely.
pub const BLAME_VERSION: u32 = 2;

/// 某个版本文件的逐行归属。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameResult {
    /// 文件内容。
    pub blob: ObjectId,
    /// 引入这个版本的提交。
    pub introduced_by: ObjectId,
    /// 第 i 行（从 0 开始）最后被改动的提交。
    pub lines: Vec<ObjectId>,
}

/// 从 `&dyn BlobStore` 读取目录树。
pub(crate) struct Trees<'a>(pub(crate) &'a dyn BlobStore);

#[async_trait]
impl TreeSource for Trees<'_> {
    async fn load_tree(&self, id: &ObjectId) -> scion_tree::Result<Tree> {
        let bytes = self
            .0
            .get(id)
            .await
            .map_err(|e| TreeError::Storage(e.to_string()))?
            .ok_or_else(|| TreeError::BadTree(*id, "not found".into()))?;
        Tree::decode(&bytes).map_err(|e| TreeError::BadTree(*id, e.to_string()))
    }
}

/// 文件在某个提交中的 blob；不存在或是目录时为 None。
async fn file_at(g: &mut Graph<'_>, trees: &Trees<'_>, cs: ObjectId, path: &str) -> Result<Option<ObjectId>> {
    let root = g.get(cs).await?.root_tree;
    Ok(lookup(trees, root, path)
        .await?
        .filter(|e| e.mode != EntryMode::Tree)
        .map(|e| e.id))
}

fn cache_key(introduced_by: &ObjectId, path: &str) -> Vec<u8> {
    let mut k = introduced_by.to_bytes().to_vec();
    k.extend_from_slice(path.as_bytes());
    k
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(b: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*pos)?;
        *pos += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

/// 编码：提交个数、各提交 id、区间个数、各区间 (行数, 提交下标)。
fn encode(lines: &[ObjectId]) -> Vec<u8> {
    let mut commits: Vec<ObjectId> = Vec::new();
    let mut index: HashMap<ObjectId, u64> = HashMap::new();
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for id in lines {
        let i = *index.entry(*id).or_insert_with(|| {
            commits.push(*id);
            commits.len() as u64 - 1
        });
        match ranges.last_mut() {
            Some((n, last)) if *last == i => *n += 1,
            _ => ranges.push((1, i)),
        }
    }
    let mut out = Vec::new();
    put_varint(&mut out, commits.len() as u64);
    for c in &commits {
        out.extend_from_slice(&c.to_bytes());
    }
    put_varint(&mut out, ranges.len() as u64);
    for (n, i) in ranges {
        put_varint(&mut out, n);
        put_varint(&mut out, i);
    }
    out
}

fn decode(b: &[u8]) -> Option<Vec<ObjectId>> {
    let mut pos = 0;
    let n = usize::try_from(get_varint(b, &mut pos)?).ok()?;
    let mut commits = Vec::with_capacity(n.min(b.len()));
    for _ in 0..n {
        let raw = b.get(pos..pos + scion_object::ID_LEN)?;
        commits.push(ObjectId::from_bytes(raw).ok()?);
        pos += scion_object::ID_LEN;
    }
    let ranges = get_varint(b, &mut pos)?;
    let mut lines = Vec::new();
    for _ in 0..ranges {
        let count = get_varint(b, &mut pos)?;
        let idx = usize::try_from(get_varint(b, &mut pos)?).ok()?;
        let id = *commits.get(idx)?;
        lines.extend(std::iter::repeat_n(id, usize::try_from(count).ok()?));
    }
    (pos == b.len()).then_some(lines)
}

async fn content(blobs: &dyn BlobStore, id: &ObjectId) -> Result<Vec<u8>> {
    blobs
        .get(id)
        .await?
        .map(|b| b.to_vec())
        .ok_or_else(|| QueryError::Corrupt(*id, "file content missing".into()))
}

/// The line-diff algorithm to use: exact LCS (minimal, like `git diff --minimal`) when the part
/// between the common prefix and suffix is small enough for its table (about 32 MB at most),
/// else Myers. similar's Myers is not always minimal on large files.
fn diff_algorithm(old: &[u8], new: &[u8]) -> similar::Algorithm {
    const MAX_LCS_CELLS: usize = 4_000_000;
    let (a, b): (Vec<&[u8]>, Vec<&[u8]>) = (
        old.split_inclusive(|c| *c == b'\n').collect(),
        new.split_inclusive(|c| *c == b'\n').collect(),
    );
    let pre = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
    let suf = a[pre..]
        .iter()
        .rev()
        .zip(b[pre..].iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    if (a.len() - pre - suf).saturating_mul(b.len() - pre - suf) <= MAX_LCS_CELLS {
        similar::Algorithm::Lcs
    } else {
        similar::Algorithm::Myers
    }
}

/// 由上一版的逐行归属和两个版本的内容，算出新版本的逐行归属。
fn carry(old: &[u8], old_lines: &[ObjectId], new: &[u8], introduced_by: ObjectId) -> Vec<ObjectId> {
    let diff = similar::TextDiff::configure()
        .algorithm(diff_algorithm(old, new))
        .diff_lines(old, new);
    let mut out = Vec::new();
    for op in diff.ops() {
        match op.tag() {
            similar::DiffTag::Equal => out.extend_from_slice(&old_lines[op.old_range()]),
            similar::DiffTag::Delete => {}
            similar::DiffTag::Insert | similar::DiffTag::Replace => {
                out.extend(std::iter::repeat_n(introduced_by, op.new_range().len()))
            }
        }
    }
    out
}

fn line_count(data: &[u8]) -> usize {
    similar::TextDiff::from_lines(&[][..], data)
        .ops()
        .iter()
        .map(|op| op.new_range().len())
        .sum()
}

/// 各版本 (引入提交, blob)，从新到旧；以及最老那个版本的缓存结果（如果命中缓存）。
type Versions = (Vec<(ObjectId, ObjectId)>, Option<Vec<ObjectId>>);

async fn cached(meta: &dyn MetaStore, repo: &str, intro: &ObjectId, path: &str) -> Result<Option<Vec<ObjectId>>> {
    // 缓存损坏（无法解码）时忽略并重新计算
    let raw = meta
        .get_derived(repo, BLAME_KIND, BLAME_VERSION, &cache_key(intro, path))
        .await?;
    Ok(raw.as_deref().and_then(decode))
}

/// 第一个父提交（批量预取）。
async fn first_parent(g: &mut Graph<'_>, id: ObjectId) -> Result<Option<ObjectId>> {
    let rec = g.get(id).await?.clone();
    match rec.first_parent() {
        Some(p) => Ok(Some(g.get_below(p, rec.generation.saturating_sub(1)).await?.id)),
        None => Ok(None),
    }
}

/// 用 PathHistory 索引收集版本：候选提交按代数从新到旧，用第一父提交链跳表（O(log n)）判断是否在链上，
/// 只在真正的版本引入处读目录树，不逐个提交往回走。
/// 索引缺失、深度未知（旧数据）或与实际不一致时返回 None，由调用方退回扫描。
async fn versions_by_index(
    meta: &dyn MetaStore,
    g: &mut Graph<'_>,
    trees: &Trees<'_>,
    repo: &str,
    cs: ObjectId,
    blob: ObjectId,
    path: &str,
) -> Result<Option<Versions>> {
    let top = g.get(cs).await?.generation;
    // 按代数从高到低（path_history 已排序）
    let candidates: Vec<(ObjectId, bool)> = meta
        .path_history(repo, path, top, usize::MAX >> 1)
        .await?
        .into_iter()
        .filter(|e| e.path == path)
        .map(|e| (e.cs, e.deleted))
        .collect();
    if candidates.is_empty() {
        return Ok(None);
    }
    // 在 `from` 的第一父提交链上、从下标 `start` 起的第一个候选
    async fn next_on_chain(
        g: &mut Graph<'_>,
        candidates: &[(ObjectId, bool)],
        start: usize,
        from: ObjectId,
    ) -> Result<Option<Option<usize>>> {
        for (i, (c, _)) in candidates.iter().enumerate().skip(start) {
            match on_first_parent_chain(g, from, *c).await {
                Ok(Some(true)) => return Ok(Some(Some(i))),
                Ok(Some(false)) => {}
                Ok(None) => return Ok(None),
                // 写索引后、写记录前崩溃留下的孤立索引行：不属于任何历史，跳过
                Err(QueryError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(Some(None))
    }

    let mut versions = Vec::new();
    // versions[..probed] 已查过缓存
    let mut probed = 0;
    let mut blob = blob;
    let mut idx = match next_on_chain(g, &candidates, 0, cs).await? {
        Some(Some(i)) => i,
        // 深度未知，或文件存在却没有任何改动记录：索引不可用
        _ => return Ok(None),
    };
    loop {
        let (intro, deleted) = candidates[idx];
        if deleted {
            return Ok(None);
        }
        versions.push((intro, blob));
        // 最新版本单独查一次（重复查询的常见情况），之后每 PROBE_BATCH 个版本批量查一次：
        // 远端数据库上逐版本查询的往返会主导冷启动耗时
        if versions.len() - probed >= if probed == 0 { 1 } else { PROBE_BATCH } {
            if let Some(lines) = probe(meta, repo, path, &mut versions, &mut probed).await? {
                return Ok(Some((versions, Some(lines))));
            }
        }
        let Some(parent) = first_parent(g, intro).await? else {
            break;
        };
        let older = match next_on_chain(g, &candidates, idx + 1, parent).await? {
            Some(Some(i)) => i,
            // 链上更早没有改动：文件在 intro 之前不存在
            Some(None) => break,
            None => return Ok(None),
        };
        let (y, y_deleted) = candidates[older];
        if y_deleted {
            break;
        }
        match file_at(g, trees, y, path).await? {
            Some(b) => blob = b,
            None => return Ok(None),
        }
        idx = older;
    }
    let base = probe(meta, repo, path, &mut versions, &mut probed).await?;
    Ok(Some((versions, base)))
}

/// 每批查询缓存的版本数。
const PROBE_BATCH: usize = 64;

/// 批量查 `versions[probed..]`（新到旧）的缓存；命中时把更老的版本截掉并返回命中的结果。
async fn probe(
    meta: &dyn MetaStore,
    repo: &str,
    path: &str,
    versions: &mut Vec<(ObjectId, ObjectId)>,
    probed: &mut usize,
) -> Result<Option<Vec<ObjectId>>> {
    let keys: Vec<Vec<u8>> = versions[*probed..].iter().map(|(cs, _)| cache_key(cs, path)).collect();
    if keys.is_empty() {
        return Ok(None);
    }
    let found = meta.get_derived_many(repo, BLAME_KIND, BLAME_VERSION, &keys).await?;
    // 缓存损坏（无法解码）时当作未命中
    if let Some((i, lines)) = found
        .iter()
        .enumerate()
        .find_map(|(i, raw)| raw.as_deref().and_then(decode).map(|l| (i, l)))
    {
        versions.truncate(*probed + i + 1);
        return Ok(Some(lines));
    }
    *probed = versions.len();
    Ok(None)
}

/// 不依赖索引：沿第一父提交链逐个比较目录树中的文件。
async fn versions_by_scan(
    meta: &dyn MetaStore,
    g: &mut Graph<'_>,
    trees: &Trees<'_>,
    repo: &str,
    cs: ObjectId,
    blob: ObjectId,
    path: &str,
) -> Result<Versions> {
    let mut versions: Vec<(ObjectId, ObjectId)> = Vec::new();
    let (mut cur, mut blob) = (cs, blob);
    loop {
        // 找到当前 blob 的引入提交：沿第一父提交链走到 blob 不同为止
        while let Some(parent) = first_parent(g, cur).await? {
            if file_at(g, trees, parent, path).await? != Some(blob) {
                break;
            }
            cur = parent;
        }
        if let Some(lines) = cached(meta, repo, &cur, path).await? {
            versions.push((cur, blob));
            return Ok((versions, Some(lines)));
        }
        versions.push((cur, blob));
        let Some(parent) = first_parent(g, cur).await? else {
            return Ok((versions, None));
        };
        match file_at(g, trees, parent, path).await? {
            Some(older) => {
                cur = parent;
                blob = older;
            }
            None => return Ok((versions, None)),
        }
    }
}

/// `path` 在提交 `cs` 中的逐行归属。
pub async fn blame(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    cs: ObjectId,
    path: &str,
) -> Result<BlameResult> {
    let mut g = Graph::new(meta, repo);
    let trees = Trees(blobs);
    let root = g.get(cs).await?.root_tree;
    let entry = lookup(&trees, root, path)
        .await?
        .ok_or_else(|| QueryError::PathNotFound(path.to_owned()))?;
    if entry.mode == EntryMode::Tree {
        return Err(QueryError::NotAFile(path.to_owned()));
    }

    let (mut versions, base) = match versions_by_index(meta, &mut g, &trees, repo, cs, entry.id, path).await? {
        Some(v) => v,
        None => versions_by_scan(meta, &mut g, &trees, repo, cs, entry.id, path).await?,
    };

    // 从最老的版本往新计算。只缓存最终版本和每 CHECKPOINT_EVERY 个版本一个检查点：
    // 每次缓存都是一次持久化写入，逐版本缓存会让冷启动的耗时被写入主导。
    versions.reverse();
    let total = versions.len();
    let mut iter = versions.into_iter().enumerate();
    let (_, (first_cs, first_blob)) = iter.next().ok_or_else(|| QueryError::PathNotFound(path.to_owned()))?;
    let mut prev = content(blobs, &first_blob).await?;
    let from_cache = base.is_some();
    let mut lines = base.unwrap_or_else(|| vec![first_cs; line_count(&prev)]);
    if !from_cache && total == 1 {
        save(meta, repo, &first_cs, path, &lines).await?;
    }
    let mut last = (first_cs, first_blob);
    for (i, (introduced_by, blob)) in iter {
        let data = content(blobs, &blob).await?;
        lines = carry(&prev, &lines, &data, introduced_by);
        if i + 1 == total || i % CHECKPOINT_EVERY == 0 {
            save(meta, repo, &introduced_by, path, &lines).await?;
        }
        prev = data;
        last = (introduced_by, blob);
    }
    Ok(BlameResult {
        blob: last.1,
        introduced_by: last.0,
        lines,
    })
}

/// 中间版本每隔多少个存一个缓存检查点。
const CHECKPOINT_EVERY: usize = 32;

async fn save(meta: &dyn MetaStore, repo: &str, intro: &ObjectId, path: &str, lines: &[ObjectId]) -> Result<()> {
    Ok(meta
        .put_derived(repo, BLAME_KIND, BLAME_VERSION, &cache_key(intro, path), &encode(lines))
        .await?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::{Changeset, FileChange, Signature};
    use scion_store::{ChangesetRecord, MemStore};
    use std::collections::BTreeMap;

    const REPO: &str = "r";

    pub(crate) struct Repo {
        pub(crate) store: MemStore,
        n: u8,
        /// 是否写 PathHistory 索引（false 模拟索引建立之前的旧数据）。
        indexed: bool,
    }

    impl Repo {
        pub(crate) async fn new() -> Self {
            Self::with_index(true).await
        }

        pub(crate) async fn with_index(indexed: bool) -> Self {
            let store = MemStore::new();
            store.create_repo(REPO).await.expect("repo");
            Self { store, n: 0, indexed }
        }

        /// 提交：用 scion-tree 真正建树，写入内容、目录树、提交对象与记录。
        pub(crate) async fn commit(&mut self, parents: &[ObjectId], files: &[(&str, Option<&str>)]) -> ObjectId {
            self.n += 1;
            let sig = Signature {
                name: "a".into(),
                email: "a@x".into(),
                time_ms: 0,
                tz_offset_minutes: 0,
            };
            let mut delta = Vec::new();
            for (path, content) in files {
                match content {
                    Some(c) => {
                        let id = ObjectId::of_blob(c.as_bytes());
                        self.store
                            .put(id, Bytes::copy_from_slice(c.as_bytes()))
                            .await
                            .expect("blob");
                        delta.push(FileChange::put(*path, EntryMode::File, id, c.len() as u64));
                    }
                    None => delta.push(FileChange::delete(*path)),
                }
            }
            let cs = Changeset::new(
                parents.to_vec(),
                sig.clone(),
                sig,
                format!("c{}", self.n),
                [self.n; 16],
                delta,
                BTreeMap::new(),
            )
            .expect("valid");
            let base = match parents.first() {
                Some(p) => Some(
                    self.store
                        .get_changeset(REPO, p)
                        .await
                        .expect("ok")
                        .expect("parent")
                        .root_tree,
                ),
                None => None,
            };
            let built = scion_tree::apply_delta(&Trees(&self.store), base, &cs.delta)
                .await
                .expect("tree");
            for (id, b) in &built.new_trees {
                self.store.put(*id, b.clone()).await.expect("tree");
            }
            let id = cs.id();
            self.store.put(id, Bytes::from(cs.encode())).await.expect("cs");
            let mut generation = 0;
            for p in parents {
                generation = generation.max(
                    self.store
                        .get_changeset(REPO, p)
                        .await
                        .expect("ok")
                        .expect("p")
                        .generation,
                );
            }
            let (fp_depth, fp_jump) = scion_store::fp_skip(&self.store, REPO, parents.first().copied())
                .await
                .expect("skip");
            let rec = ChangesetRecord {
                id,
                root_tree: built.root,
                parents: parents.to_vec(),
                generation: generation + 1,
                fp_depth,
                fp_jump,
            };
            if self.indexed {
                let changes: Vec<_> = cs
                    .delta
                    .iter()
                    .map(|c| scion_store::PathChange {
                        path: c.path.clone(),
                        deleted: matches!(c.op, scion_object::FileOp::Delete),
                    })
                    .collect();
                self.store
                    .index_paths(REPO, id, rec.generation, &changes)
                    .await
                    .expect("index");
            }
            self.store.put_changeset(REPO, &rec).await.expect("record");
            id
        }

        async fn blame(&self, cs: ObjectId, path: &str) -> Result<BlameResult> {
            blame(&self.store, &self.store, REPO, cs, path).await
        }
    }

    #[tokio::test]
    async fn attributes_each_line_to_the_commit_that_last_changed_it() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f.txt", Some("one\ntwo\nthree\n"))]).await;
        let b = r.commit(&[a], &[("other", Some("x"))]).await; // 不改 f.txt
        let c = r.commit(&[b], &[("f.txt", Some("one\nTWO\nthree\nfour\n"))]).await;
        let d = r
            .commit(&[c], &[("f.txt", Some("zero\none\nTWO\nthree\nfour\n"))])
            .await;
        let res = r.blame(d, "f.txt").await.expect("blame");
        assert_eq!(res.lines, vec![d, a, c, a, c]);
        assert_eq!(res.introduced_by, d);
        // 在未改动该文件的提交上 blame：结果相同的版本
        let at_b = r.blame(b, "f.txt").await.expect("blame");
        assert_eq!(at_b.lines, vec![a, a, a]);
        assert_eq!(at_b.introduced_by, a);
    }

    #[tokio::test]
    async fn uses_cache_and_survives_corrupt_cache() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("1\n2\n"))]).await;
        let b = r.commit(&[a], &[("f", Some("1\n2\n3\n"))]).await;
        let first = r.blame(b, "f").await.expect("blame");
        // 结果已缓存
        let cached = r
            .store
            .get_derived(REPO, BLAME_KIND, BLAME_VERSION, &cache_key(&b, "f"))
            .await
            .expect("ok");
        assert_eq!(cached.as_deref().and_then(decode), Some(first.lines.clone()));
        // 再加一个提交：只需从 b 的缓存往后算
        let c = r.commit(&[b], &[("f", Some("0\n1\n2\n3\n"))]).await;
        assert_eq!(r.blame(c, "f").await.expect("blame").lines, vec![c, a, a, b]);
        // 损坏的缓存被忽略，结果仍正确
        let mut r2 = Repo::new().await;
        let a2 = r2.commit(&[], &[("f", Some("x\n"))]).await;
        r2.store
            .put_derived(REPO, BLAME_KIND, BLAME_VERSION, &cache_key(&a2, "f"), b"garbage")
            .await
            .expect("put");
        assert_eq!(r2.blame(a2, "f").await.expect("blame").lines, vec![a2]);
    }

    #[tokio::test]
    async fn file_deleted_and_recreated_starts_fresh() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("old\n"))]).await;
        let b = r.commit(&[a], &[("f", None), ("g", Some("g"))]).await;
        let c = r.commit(&[b], &[("f", Some("old\nnew\n"))]).await;
        assert_eq!(r.blame(c, "f").await.expect("blame").lines, vec![c, c]);
    }

    #[tokio::test]
    async fn merge_attributes_to_merge_commit_along_first_parent() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("base\n"))]).await;
        let side = r.commit(&[a], &[("f", Some("base\nside\n"))]).await;
        let main = r.commit(&[a], &[("g", Some("g"))]).await;
        let m = r.commit(&[main, side], &[("f", Some("base\nside\n"))]).await;
        assert_eq!(r.blame(m, "f").await.expect("blame").lines, vec![a, m]);
    }

    #[tokio::test]
    async fn errors_and_edge_cases() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("dir/f", Some("")), ("noeol", Some("a\nb"))]).await;
        assert!(matches!(r.blame(a, "nope").await, Err(QueryError::PathNotFound(_))));
        assert!(matches!(r.blame(a, "dir").await, Err(QueryError::NotAFile(_))));
        assert_eq!(
            r.blame(a, "dir/f").await.expect("empty file").lines,
            Vec::<ObjectId>::new()
        );
        assert_eq!(r.blame(a, "noeol").await.expect("no trailing newline").lines.len(), 2);
        let ghost = ObjectId::hash(scion_object::ObjectKind::Changeset, b"ghost");
        assert!(matches!(r.blame(ghost, "f").await, Err(QueryError::NotFound(_))));
        // 内容缺失
        let empty = MemStore::new();
        let id = ObjectId::of_blob(b"missing");
        assert!(matches!(content(&empty, &id).await, Err(QueryError::Corrupt(..))));
    }

    /// 同一段历史：有索引与无索引（扫描）得到完全相同的 blame。
    #[tokio::test]
    async fn index_and_scan_agree_on_every_file_and_commit() {
        let steps: Vec<Vec<(&str, Option<&str>)>> = vec![
            vec![("a", Some("1\n2\n3\n")), ("dir/b", Some("x\n"))],
            vec![("a", Some("1\nX\n3\n"))],
            vec![("c", Some("c\n"))],
            vec![("dir/b", Some("x\ny\n")), ("a", Some("0\n1\nX\n3\n"))],
            vec![("c", None)],
            vec![("c", Some("again\n")), ("a", Some("0\n1\nX\n"))],
        ];
        let (mut fast, mut slow) = (Repo::with_index(true).await, Repo::with_index(false).await);
        let (mut tf, mut ts) = (Vec::new(), Vec::new());
        for step in &steps {
            let pf: Vec<_> = tf.last().copied().into_iter().collect();
            let ps: Vec<_> = ts.last().copied().into_iter().collect();
            tf.push(fast.commit(&pf, step).await);
            ts.push(slow.commit(&ps, step).await);
        }
        assert_eq!(tf, ts, "same history, same ids");
        for (i, cs) in tf.iter().enumerate() {
            for path in ["a", "dir/b", "c"] {
                let (x, y) = (fast.blame(*cs, path).await, slow.blame(*cs, path).await);
                match (x, y) {
                    (Ok(x), Ok(y)) => assert_eq!(x, y, "{path} at step {i}"),
                    (Err(QueryError::PathNotFound(_)), Err(QueryError::PathNotFound(_))) => {}
                    other => panic!("{path} at step {i}: {other:?}"),
                }
            }
        }
    }

    /// 索引与实际不一致（声称在文件仍存在时被删除）时，退回扫描，结果依然正确。
    #[tokio::test]
    async fn inconsistent_index_falls_back_to_scan() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("1\n"))]).await;
        let b = r.commit(&[a], &[("f", Some("1\n2\n"))]).await;
        let rec = r.store.get_changeset(REPO, &b).await.expect("ok").expect("b");
        // 伪造一条 b 删除了 f 的索引（与实际相反）
        let lie = [scion_store::PathChange {
            path: "f".into(),
            deleted: true,
        }];
        let fake = ObjectId::hash(scion_object::ObjectKind::Changeset, b"fake");
        r.store
            .index_paths(REPO, fake, rec.generation, &lie)
            .await
            .expect("index");
        let c = r.commit(&[b], &[("g", Some("g"))]).await;
        r.store
            .index_paths(REPO, c, rec.generation + 1, &lie)
            .await
            .expect("lie about c");
        assert_eq!(r.blame(c, "f").await.expect("blame").lines, vec![a, b]);
    }

    /// 只缓存最终版本与检查点；之后 blame 中间版本仍然正确。
    #[tokio::test]
    async fn caches_final_version_and_checkpoints_only() {
        let mut r = Repo::new().await;
        let mut ids = Vec::new();
        let mut content = String::new();
        for i in 0..40 {
            content.push_str(&format!("line {i}\n"));
            let parents: Vec<_> = ids.last().copied().into_iter().collect();
            ids.push(r.commit(&parents, &[("f", Some(content.as_str()))]).await);
        }
        let tip = *ids.last().expect("tip");
        assert_eq!(r.blame(tip, "f").await.expect("blame").lines, ids);
        async fn has(r: &Repo, id: ObjectId) -> bool {
            r.store
                .get_derived(REPO, BLAME_KIND, BLAME_VERSION, &cache_key(&id, "f"))
                .await
                .expect("ok")
                .is_some()
        }
        assert!(has(&r, tip).await, "final version cached");
        assert!(has(&r, ids[32]).await, "checkpoint");
        assert!(!has(&r, ids[5]).await, "intermediate not cached");
        // 中间版本：从检查点或最早版本重新计算，结果正确
        assert_eq!(r.blame(ids[20], "f").await.expect("blame").lines, ids[..21].to_vec());
        assert_eq!(r.blame(ids[35], "f").await.expect("blame").lines, ids[..36].to_vec());
    }

    /// 缓存批量探测：命中落在第二批或更后时，截断版本列表后结果仍正确。
    #[tokio::test]
    async fn checkpoint_found_in_later_probe_batch() {
        let mut r = Repo::new().await;
        let mut ids = Vec::new();
        let mut content = String::new();
        for i in 0..200 {
            content.push_str(&format!("line {i}\n"));
            let parents: Vec<_> = ids.last().copied().into_iter().collect();
            ids.push(r.commit(&parents, &[("f", Some(content.as_str()))]).await);
        }
        assert_eq!(r.blame(ids[199], "f").await.expect("blame").lines, ids);
        // ids[180] 往下：第一批只有自己，第二批 179..116 中命中检查点 160 与 128，取最新的 160
        for at in [180, 129, 64, 0] {
            assert_eq!(
                r.blame(ids[at], "f").await.expect("blame").lines,
                ids[..=at].to_vec(),
                "at {at}"
            );
        }
    }

    /// 先写索引后写记录：中途崩溃留下的孤立索引行被忽略。
    #[tokio::test]
    async fn orphan_index_rows_are_ignored() {
        let mut r = Repo::new().await;
        let a = r.commit(&[], &[("f", Some("1\n"))]).await;
        let b = r.commit(&[a], &[("f", Some("1\n2\n"))]).await;
        let gen_b = r
            .store
            .get_changeset(REPO, &b)
            .await
            .expect("ok")
            .expect("b")
            .generation;
        let orphan = ObjectId::hash(scion_object::ObjectKind::Changeset, b"never recorded");
        let row = [scion_store::PathChange {
            path: "f".into(),
            deleted: false,
        }];
        r.store
            .index_paths(REPO, orphan, gen_b, &row)
            .await
            .expect("orphan row");
        assert_eq!(r.blame(b, "f").await.expect("blame").lines, vec![a, b]);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let (x, y) = (ObjectId::of_blob(b"x"), ObjectId::of_blob(b"y"));
        for lines in [vec![], vec![x], vec![x, x, y, y, y, x], vec![y; 300]] {
            assert_eq!(decode(&encode(&lines)), Some(lines.clone()));
        }
        assert_eq!(decode(b""), None);
        assert_eq!(decode(&[1, 0]), None, "truncated id");
        let mut extra = encode(&[x]);
        extra.push(0);
        assert_eq!(decode(&extra), None, "trailing bytes");
        let mut bad_idx = Vec::new();
        put_varint(&mut bad_idx, 0);
        put_varint(&mut bad_idx, 1);
        put_varint(&mut bad_idx, 1);
        put_varint(&mut bad_idx, 5);
        assert_eq!(decode(&bad_idx), None, "index out of range");
        assert_eq!(get_varint(&[0x80; 11], &mut 0), None, "overlong varint");
    }

    #[test]
    fn diff_algorithm_choice() {
        let lines = |n: usize, tag: &str| (0..n).map(|i| format!("{tag} {i}\n")).collect::<String>().into_bytes();
        assert_eq!(diff_algorithm(b"a\nb\n", b"a\nc\n"), similar::Algorithm::Lcs);
        assert_eq!(diff_algorithm(b"", b""), similar::Algorithm::Lcs);
        // Big files that differ everywhere: the table would be too large
        assert_eq!(
            diff_algorithm(&lines(3000, "old"), &lines(3000, "new")),
            similar::Algorithm::Myers
        );
        // Big files with a small changed middle: only the middle counts
        let (mut a, mut b) = (lines(20_000, "same"), lines(20_000, "same"));
        a.extend(lines(100, "old"));
        b.extend(lines(120, "new"));
        a.extend(lines(20_000, "tail"));
        b.extend(lines(20_000, "tail"));
        assert_eq!(diff_algorithm(&a, &b), similar::Algorithm::Lcs);
    }

    #[test]
    fn carry_and_line_count() {
        let (o, n) = (ObjectId::of_blob(b"o"), ObjectId::of_blob(b"n"));
        assert_eq!(carry(b"a\nb\n", &[o, o], b"a\nB\nc\n", n), vec![o, n, n]);
        assert_eq!(carry(b"a\nb\n", &[o, o], b"b\n", n), vec![o]);
        assert_eq!(line_count(b""), 0);
        assert_eq!(line_count(b"a\nb"), 2);
    }
}
