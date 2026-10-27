//! LastCommitPerEntry 派生数据：目录中每个条目最后一次被改动的提交（文件列表页的「最后提交」列）。
//!
//! 沿第一父提交链追溯：条目在某个提交的 delta（相对第一父提交）中出现，就算被这个提交改动。
//! 子目录被改动指它下面的任意路径被改动。As `git log -1 -- PATH` does, a merge that took the entry
//! as one of the merged branches had it did not change it: the change is that branch's last one.
//!
//! 计算顺序：
//! 1. 缓存（提交, 目录）命中直接返回；
//! 2. 第一父提交链上 64 个提交以内有缓存的祖先时增量计算：依次应用其后各提交的改动，
//!    改动到的条目归到最近改动它的提交，其余沿用祖先的结果；
//! 3. 其余条目用 PathHistory 索引找第一父提交链上最近的候选（并发查询）；
//! 4. 索引不可用（旧数据深度未知或缺行）时沿第一父提交链逐个扫描 delta，最多 [`MAX_SCAN`] 个提交。

use std::collections::{HashMap, HashSet};

use futures::StreamExt;
use scion_object::ObjectId;
use scion_store::{MetaStore, path_within};

use crate::{Graph, QueryError, Result, load_changeset, on_first_parent_chain};
use scion_store::BlobStore;

pub const LAST_COMMIT_KIND: &str = "last-commit";
/// 2: a merge that took an entry from a merged branch gives it to that branch's last change.
pub const LAST_COMMIT_VERSION: u32 = 2;
/// Merges followed into their branches for one entry, at most.
const MAX_SIDE_HOPS: usize = 16;
/// 扫描回退时最多检查的提交数。
pub const MAX_SCAN: usize = 2000;
/// 往回找有缓存的祖先时最多检查的提交数。
pub const ANCESTOR_REACH: usize = 64;
/// 并发的索引查询数。
const INDEX_PARALLEL: usize = 16;
/// 每次从索引读取的候选数。
const CANDIDATE_BATCH: usize = 64;

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

fn cache_key(cs: &ObjectId, dir: &str) -> Vec<u8> {
    let mut k = cs.to_bytes().to_vec();
    k.extend_from_slice(dir.as_bytes());
    k
}

/// 编码：依次为 名字长度（u32 LE）、名字、提交 id。
fn encode(map: &[(String, ObjectId)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, id) in map {
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&id.to_bytes());
    }
    out
}

fn decode(mut b: &[u8]) -> Option<HashMap<String, ObjectId>> {
    let mut out = HashMap::new();
    while !b.is_empty() {
        let len = u32::from_le_bytes(b.get(..4)?.try_into().ok()?) as usize;
        b = &b[4..];
        let name = std::str::from_utf8(b.get(..len)?).ok()?.to_owned();
        b = &b[len..];
        let id = ObjectId::from_bytes(b.get(..scion_object::ID_LEN)?).ok()?;
        b = &b[scion_object::ID_LEN..];
        out.insert(name, id);
    }
    Some(out)
}

async fn cached(
    meta: &dyn MetaStore,
    repo: &str,
    cs: &ObjectId,
    dir: &str,
) -> Result<Option<HashMap<String, ObjectId>>> {
    Ok(meta
        .get_derived(repo, LAST_COMMIT_KIND, LAST_COMMIT_VERSION, &cache_key(cs, dir))
        .await?
        .and_then(|v| decode(&v)))
}

/// `dir` 下被 `delta` 改动的直接条目名。
fn touched<'a>(paths: impl Iterator<Item = &'a str>, dir: &str) -> HashSet<String> {
    paths
        .filter(|p| dir.is_empty() || (path_within(p, dir) && *p != dir))
        .map(|p| {
            let rest = if dir.is_empty() { p } else { &p[dir.len() + 1..] };
            rest.split('/').next().unwrap_or(rest).to_owned()
        })
        .collect()
}

/// 用索引找 `path` 在 `cs` 的第一父提交链上最近的改动。`Ok(None)` 表示索引无法回答。
async fn by_index(
    meta: &dyn MetaStore,
    g: &mut Graph<'_>,
    repo: &str,
    cs: ObjectId,
    path: &str,
) -> Result<Option<ObjectId>> {
    let mut top = g.get(cs).await?.generation;
    let mut checked = HashSet::new();
    loop {
        let hist = meta.path_history(repo, path, top, CANDIDATE_BATCH).await?;
        // 结果按 (代数降序, 路径) 排列，上限限制的是路径行而不是提交：满页时最后一行所在的那一代
        // 可能还没读完（同代的旁支提交可能占满整页），只处理更高的代，下一页从这一代重新读
        let full = hist.len() == CANDIDATE_BATCH;
        let floor = match hist.last() {
            Some(last) if full => last.generation,
            _ => 0,
        };
        for e in hist.iter().take_while(|e| e.generation > floor) {
            if !checked.insert(e.cs) {
                continue;
            }
            match on_first_parent_chain(g, cs, e.cs).await {
                Ok(Some(true)) => return Ok(Some(e.cs)),
                Ok(Some(false)) => {}
                Ok(None) => return Ok(None),
                // 写索引后、写记录前崩溃留下的孤立索引行：不属于任何历史，跳过
                Err(QueryError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        match hist.first() {
            // 还有更低的代没读，且这一页推进了（不全是同一代）
            Some(first) if full && first.generation > floor => top = floor,
            // 不满页且没找到：条目存在却没有改动记录（建立索引之前的数据）；
            // 或者同一代的行超过一页：都交给第一父提交链扫描
            _ => return Ok(None),
        }
    }
}

/// 第一父提交链上最近（[`ANCESTOR_REACH`] 个提交以内）有缓存的祖先：以它的结果为起点，
/// 依次应用其后各提交的改动。合入频繁而浏览较少时，最新提交的父提交往往没有缓存。
#[allow(clippy::too_many_arguments)]
async fn from_cached_ancestor(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    g: &mut Graph<'_>,
    repo: &str,
    cs: ObjectId,
    dir: &str,
    pending: &mut HashSet<String>,
    found: &mut HashMap<String, ObjectId>,
) -> Result<()> {
    // chain[0] = cs，之后依次为第一父提交
    let mut chain = vec![cs];
    while chain.len() <= ANCESTOR_REACH {
        match g.get(chain[chain.len() - 1]).await?.first_parent() {
            Some(p) => chain.push(p),
            None => break,
        }
    }
    let keys: Vec<Vec<u8>> = chain[1..].iter().map(|id| cache_key(id, dir)).collect();
    let hits = meta
        .get_derived_many(repo, LAST_COMMIT_KIND, LAST_COMMIT_VERSION, &keys)
        .await?;
    let Some((i, base)) = hits
        .iter()
        .enumerate()
        .find_map(|(i, v)| v.as_deref().and_then(decode).map(|m| (i + 1, m)))
    else {
        return Ok(());
    };
    // 从新到旧：最先改动到某个条目的提交就是它的最后提交
    for id in &chain[..i] {
        let c = load_changeset(blobs, *id).await?;
        for name in touched(c.delta.iter().map(|d| d.path.as_str()), dir) {
            if pending.remove(&name) {
                found.insert(name, *id);
            }
        }
    }
    pending.retain(|name| match base.get(name) {
        Some(id) => {
            found.insert(name.clone(), *id);
            false
        }
        None => true,
    });
    Ok(())
}

/// 沿第一父提交链扫描，给 `pending` 中的条目找到最近的改动。
async fn by_scan(
    blobs: &dyn BlobStore,
    g: &mut Graph<'_>,
    cs: ObjectId,
    dir: &str,
    pending: &mut HashSet<String>,
    out: &mut HashMap<String, ObjectId>,
) -> Result<()> {
    let mut cur = Some(cs);
    let mut scanned = 0;
    while let Some(id) = cur {
        if pending.is_empty() || scanned >= MAX_SCAN {
            break;
        }
        scanned += 1;
        let c = load_changeset(blobs, id).await?;
        for name in touched(c.delta.iter().map(|d| d.path.as_str()), dir) {
            if pending.remove(&name) {
                out.insert(name, id);
            }
        }
        cur = g.get(id).await?.first_parent();
    }
    Ok(())
}

/// `dir`（在提交 `cs` 中存在的目录，空串为根目录）里 `names` 各自最后被改动的提交，与 `names` 一一对应。
/// 扫描上限内找不到的条目为 None。`cache` 为 true 时 `names` 必须是目录的全部条目，结果写入缓存。
pub async fn last_commits(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    cs: ObjectId,
    dir: &str,
    names: &[String],
    cache: bool,
) -> Result<Vec<Option<ObjectId>>> {
    let mut found: HashMap<String, ObjectId> = HashMap::new();
    if cache {
        if let Some(hit) = cached(meta, repo, &cs, dir).await? {
            found = hit;
        }
    }
    let mut g = Graph::new(meta, repo);
    let mut pending: HashSet<String> = names.iter().filter(|n| !found.contains_key(*n)).cloned().collect();
    let complete_hit = pending.is_empty() && !found.is_empty();
    if cache && !pending.is_empty() {
        from_cached_ancestor(meta, blobs, &mut g, repo, cs, dir, &mut pending, &mut found).await?;
    }
    // 逐条目查索引：互不依赖，并发执行（每个任务用自己的提交记录缓存）
    let lookups = futures::stream::iter(std::mem::take(&mut pending).into_iter().map(|name| async move {
        let mut g = Graph::new(meta, repo);
        let hit = by_index(meta, &mut g, repo, cs, &join(dir, &name)).await;
        (name, hit)
    }))
    .buffer_unordered(INDEX_PARALLEL)
    .collect::<Vec<_>>()
    .await;
    let mut unknown = HashSet::new();
    for (name, hit) in lookups {
        match hit? {
            Some(id) => {
                found.insert(name, id);
            }
            None => {
                unknown.insert(name);
            }
        }
    }
    if !unknown.is_empty() {
        by_scan(blobs, &mut g, cs, dir, &mut unknown, &mut found).await?;
    }
    through_merges(meta, blobs, repo, dir, names, &mut found).await?;
    let out: Vec<Option<ObjectId>> = names.iter().map(|n| found.get(n).copied()).collect();
    if cache && unknown.is_empty() && !complete_hit {
        let all: Vec<(String, ObjectId)> = names
            .iter()
            .zip(&out)
            .filter_map(|(n, id)| id.map(|id| (n.clone(), id)))
            .collect();
        meta.put_derived(
            repo,
            LAST_COMMIT_KIND,
            LAST_COMMIT_VERSION,
            &cache_key(&cs, dir),
            &encode(&all),
        )
        .await?;
    }
    Ok(out)
}

/// Entries given to a merge that took them as a merged branch had them go to that branch's last
/// change instead (again, when that is such a merge too).
async fn through_merges(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    dir: &str,
    names: &[String],
    found: &mut HashMap<String, ObjectId>,
) -> Result<()> {
    let wanted: Vec<(String, ObjectId)> = names.iter().filter_map(|n| Some((n.clone(), *found.get(n)?))).collect();
    let moved = futures::stream::iter(wanted.into_iter().map(|(name, mut id)| async move {
        let mut g = Graph::new(meta, repo);
        let path = join(dir, &name);
        for _ in 0..MAX_SIDE_HOPS {
            let rec = g.get(id).await?.clone();
            if rec.parents.len() < 2 {
                break;
            }
            let Some(side) = crate::side_with_same(&mut g, blobs, &rec, &path).await? else {
                break;
            };
            id = match by_index(meta, &mut g, repo, side, &path).await? {
                Some(c) => c,
                None => {
                    let mut one = HashSet::from([name.clone()]);
                    let mut at = HashMap::new();
                    by_scan(blobs, &mut g, side, dir, &mut one, &mut at).await?;
                    match at.remove(&name) {
                        Some(c) => c,
                        None => break,
                    }
                }
            };
        }
        Ok::<_, QueryError>((name, id))
    }))
    .buffer_unordered(INDEX_PARALLEL)
    .collect::<Vec<_>>()
    .await;
    for m in moved {
        let (name, id) = m?;
        found.insert(name, id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{Builder, REPO};

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_owned()).collect()
    }

    async fn run(b: &Builder, cs: ObjectId, dir: &str, n: &[&str], cache: bool) -> Vec<Option<ObjectId>> {
        last_commits(&b.store, &b.store, REPO, cs, dir, &names(n), cache)
            .await
            .expect("last commits")
    }

    #[test]
    fn touched_and_codec() {
        let t = touched(["a/x", "a/y/z", "ab", "b", "a"].into_iter(), "a");
        assert_eq!(t, ["x", "y"].iter().map(|s| (*s).to_owned()).collect());
        let root = touched(["a/x", "b"].into_iter(), "");
        assert_eq!(root, ["a", "b"].iter().map(|s| (*s).to_owned()).collect());
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        let enc = encode(&[("n".into(), id), ("中".into(), id)]);
        let dec = decode(&enc).expect("decode");
        assert_eq!((dec.len(), dec["中"]), (2, id));
        assert_eq!(decode(&enc[..enc.len() - 1]), None);
        assert_eq!(decode(&[9, 0, 0, 0, b'a']), None);
        assert_eq!(decode(&[]), Some(HashMap::new()));
    }

    async fn lc(store: &scion_store::MemStore, cs: ObjectId, dir: &str, n: &[&str]) -> Vec<Option<ObjectId>> {
        last_commits(store, store, REPO, cs, dir, &names(n), false)
            .await
            .expect("last commits")
    }

    /// a ─ b ─ c ─ m
    ///      \     /
    ///       d ──'
    #[tokio::test]
    async fn first_parent_semantics_index_and_scan_agree() {
        use crate::blame::tests::Repo;
        for indexed in [true, false] {
            let mut r = Repo::with_index(indexed).await;
            let a = r
                .commit(
                    &[],
                    &[
                        ("README", Some("r")),
                        ("src/lib.rs", Some("l")),
                        ("src/util/x.rs", Some("x")),
                    ],
                )
                .await;
            let bb = r.commit(&[a], &[("src/lib.rs", Some("l2"))]).await;
            let c = r.commit(&[bb], &[("README", Some("r2"))]).await;
            let d = r
                .commit(&[bb], &[("src/util/x.rs", Some("x2")), ("docs/new.md", Some("n"))])
                .await;
            // 合并提交的 delta 相对第一父提交：带进来 d 的改动
            let m = r
                .commit(&[c, d], &[("src/util/x.rs", Some("x2")), ("docs/new.md", Some("n"))])
                .await;
            // As git log -1: the merge took docs and src as d had them
            assert_eq!(
                lc(&r.store, m, "", &["README", "docs", "src"]).await,
                vec![Some(c), Some(d), Some(d)],
                "indexed={indexed}"
            );
            assert_eq!(
                lc(&r.store, m, "src", &["lib.rs", "util"]).await,
                vec![Some(bb), Some(d)]
            );
            assert_eq!(
                lc(&r.store, c, "src", &["lib.rs", "util"]).await,
                vec![Some(bb), Some(a)]
            );
            // d 不在 c 的第一父提交链上
            assert_eq!(lc(&r.store, d, "", &["README", "docs"]).await, vec![Some(a), Some(d)]);
            // 不存在的条目
            assert_eq!(lc(&r.store, c, "", &["nope"]).await, vec![None]);
            // A merge that resolved the file differently from both sides changed it
            let e = r.commit(&[m], &[("README", Some("r3"))]).await;
            let f = r.commit(&[m], &[("README", Some("r4"))]).await;
            let m2 = r.commit(&[e, f], &[("README", Some("r5"))]).await;
            assert_eq!(lc(&r.store, m2, "", &["README", "docs"]).await, vec![Some(m2), Some(d)]);
        }
    }

    #[tokio::test]
    async fn caches_and_extends_incrementally() {
        let mut b = Builder::new().await;
        let a = b.commit(&[], &["x", "y"]).await;
        let bb = b.commit(&[a], &["x"]).await;
        assert_eq!(run(&b, bb, "", &["x", "y"], true).await, vec![Some(bb), Some(a)]);
        let key = cache_key(&bb, "");
        assert!(
            b.store
                .get_derived(REPO, LAST_COMMIT_KIND, LAST_COMMIT_VERSION, &key)
                .await
                .expect("get")
                .is_some()
        );
        // 缓存命中
        assert_eq!(run(&b, bb, "", &["x", "y"], true).await, vec![Some(bb), Some(a)]);
        // 子提交从父提交的缓存增量计算：新条目 z、改动的 y
        let c = b.commit(&[bb], &["y", "z"]).await;
        assert_eq!(
            run(&b, c, "", &["x", "y", "z"], true).await,
            vec![Some(bb), Some(c), Some(c)]
        );
        let got = cached(&b.store, REPO, &c, "").await.expect("cached").expect("hit");
        assert_eq!(got.len(), 3);
        // 不缓存的调用不写入
        let d = b.commit(&[c], &["x"]).await;
        assert_eq!(run(&b, d, "", &["x"], false).await, vec![Some(d)]);
        assert_eq!(cached(&b.store, REPO, &d, "").await.expect("cached"), None);
        // 缓存里缺的条目（损坏或旧版本）照常计算
        b.store
            .put_derived(
                REPO,
                LAST_COMMIT_KIND,
                LAST_COMMIT_VERSION,
                &cache_key(&d, ""),
                &encode(&[]),
            )
            .await
            .expect("put");
        assert_eq!(run(&b, d, "", &["x", "z"], true).await, vec![Some(d), Some(c)]);
    }

    #[tokio::test]
    async fn extends_from_a_cached_ancestor() {
        let mut b = Builder::new().await;
        let a = b.commit(&[], &["x", "y", "z"]).await;
        assert_eq!(run(&b, a, "", &["x", "y", "z"], true).await, vec![Some(a); 3]);
        // 几次合入之后才有人浏览：从 a 的缓存起，依次应用 b、c、d 的改动
        let bb = b.commit(&[a], &["x", "w"]).await;
        let c = b.commit(&[bb], &["x"]).await;
        let d = b.commit(&[c], &["y"]).await;
        assert_eq!(
            run(&b, d, "", &["w", "x", "y", "z"], true).await,
            vec![Some(bb), Some(c), Some(d), Some(a)]
        );
        // 超出可达范围的祖先缓存不使用（结果仍然正确，来自索引）
        let mut tip = d;
        for _ in 0..ANCESTOR_REACH {
            tip = b.commit(&[tip], &["hot"]).await;
        }
        assert_eq!(run(&b, tip, "", &["hot", "z"], true).await, vec![Some(tip), Some(a)]);
    }

    #[tokio::test]
    async fn scan_limit_and_errors() {
        let mut b = Builder::with_index(false).await;
        let a = b.commit(&[], &["old"]).await;
        let mut tip = a;
        for _ in 0..MAX_SCAN {
            tip = b.commit(&[tip], &["hot"]).await;
        }
        // 超出扫描上限：找不到，也不写缓存
        assert_eq!(run(&b, tip, "", &["hot", "old"], true).await, vec![Some(tip), None]);
        assert_eq!(cached(&b.store, REPO, &tip, "").await.expect("cached"), None);
        let stray = ObjectId::hash(scion_object::ObjectKind::Changeset, b"stray");
        let err = last_commits(&b.store, &b.store, REPO, stray, "", &names(&["x"]), false).await;
        assert_eq!(err, Err(QueryError::NotFound(stray)));
    }

    /// 旁支提交与第一父提交同代，并且在目录下改动了更多排序靠前的路径：索引的一页可能全是旁支。
    #[tokio::test]
    async fn same_generation_side_rows_do_not_hide_the_first_parent() {
        let mut b = Builder::new().await;
        let a = b.commit(&[], &["d/old", "x"]).await;
        let main1 = b.commit(&[a], &["d/zzz"]).await; // 代数 2
        // 旁支 s（代数 2）改动 70 个排在 d/zzz 之前的路径
        let many: Vec<String> = (0..70).map(|i| format!("d/a{i:02}")).collect();
        let refs: Vec<&str> = many.iter().map(String::as_str).collect();
        b.commit(&[a], &refs).await;
        let tip = b.commit(&[main1], &["x"]).await;
        // 同一代超过一页：交给扫描，结果仍是 main1
        assert_eq!(run(&b, tip, "", &["d"], false).await, vec![Some(main1)]);
        assert_eq!(run(&b, tip, "", &["d"], true).await, vec![Some(main1)]);

        // 同代的行不满一页，但被更高一代的旁支行挤到下一页：从这一代重新读
        let mut b = Builder::new().await;
        let a = b.commit(&[], &["d/old", "x"]).await;
        let main1 = b.commit(&[a], &["d/zzz"]).await;
        let sixty: Vec<String> = (0..60).map(|i| format!("d/a{i:02}")).collect();
        let s = b
            .commit(&[a], &sixty.iter().map(String::as_str).collect::<Vec<_>>())
            .await;
        let ten: Vec<String> = (0..10).map(|i| format!("d/b{i:02}")).collect();
        b.commit(&[s], &ten.iter().map(String::as_str).collect::<Vec<_>>())
            .await; // 代数 3
        let tip = b.commit(&[main1], &["x"]).await; // 代数 3
        let mut g = Graph::new(&b.store, REPO);
        assert_eq!(by_index(&b.store, &mut g, REPO, tip, "d").await, Ok(Some(main1)));
    }

    #[tokio::test]
    async fn orphan_index_rows_are_skipped() {
        let mut b = Builder::new().await;
        let a = b.commit(&[], &["f"]).await;
        let orphan = ObjectId::hash(scion_object::ObjectKind::Changeset, b"orphan");
        let change = scion_store::PathChange {
            path: "f".into(),
            deleted: false,
        };
        b.store.index_paths(REPO, orphan, 5, &[change]).await.expect("index");
        let tip = b.commit(&[a], &["g"]).await;
        assert_eq!(run(&b, tip, "", &["f"], false).await, vec![Some(a)]);
    }
}
