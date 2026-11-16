//! GitSha 派生数据：Scion 提交与 git 提交 sha 的双向映射（服务端设计文档 §8「GitSha」）。
//!
//! - 从 git 导入的提交在扩展字段 `git.sha1` 中记录原始 git sha（20 字节），创建提交时即登记反向映射。
//! - 其余提交按 git 的对象格式逐层计算：文件 → `blob`，目录 → `tree`，提交 → `commit`。commit 对象
//!   在 committer 之后多一行 `scion-id <提交 id>` 头（git 允许额外的头，jj 的 `change-id` 同理），
//!   保证不同的 Scion 提交得到不同的 sha（git 时间只精确到秒，也不含 change id）。
//!
//! 结果都写进派生数据：提交 → sha（`git-commit`）、sha → 提交（`git-sha1`）、目录树 → sha、
//! 文件 → sha。未变化的子目录 id 相同，命中缓存，所以一次新提交只需计算改动路径上的对象。

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::pin::Pin;

use scion_object::{Changeset, EntryMode, ObjectId, Tree};
use scion_store::{BlobStore, ChangesetRecord, MetaStore};
use sha1::{Digest, Sha1};

use crate::{QueryError, Result, load_changeset};

/// 记录 git 提交 sha 的扩展字段（SPEC.md §5）。
pub const GIT_SHA1_EXTENSION: &str = "git.sha1";
pub const GITSHA_KIND: &str = "git-sha1";
pub const GITSHA_VERSION: u32 = 1;
/// 提交 id → git sha。
pub const GIT_COMMIT_KIND: &str = "git-commit";
/// 目录树 id → git tree sha。
pub const GIT_TREE_KIND: &str = "git-tree";
/// 文件 id → git blob sha。
pub const GIT_BLOB_KIND: &str = "git-blob";
/// git tree SHA → directory id, written with `git-tree` (the git gateway finds pushed trees' bases with it).
pub const GIT_TREE_ID_KIND: &str = "git-tree-id";
/// git blob SHA → file id, written with `git-blob`.
pub const GIT_BLOB_ID_KIND: &str = "git-blob-id";
pub const GIT_OBJECT_VERSION: u32 = 1;
/// 计算得到的 commit 对象里记录 Scion 提交 id 的头。
pub const SCION_ID_HEADER: &str = scion_object::git::SCION_ID_HEADER;

/// 40 位小写十六进制的 git sha。
pub fn parse_git_sha(s: &str) -> Option<[u8; 20]> {
    if s.len() != 40 || !s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return None;
    }
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out).ok()?;
    Some(out)
}

/// 提交中记录的 git sha（扩展字段存在且恰好 20 字节时）。
pub fn git_sha_of(cs: &Changeset) -> Option<[u8; 20]> {
    cs.extensions
        .get(GIT_SHA1_EXTENSION)
        .and_then(|v| v.as_slice().try_into().ok())
}

/// 若提交带有 git sha，登记反向映射（第一次写入为准）。
pub async fn index_git_sha(meta: &dyn MetaStore, repo: &str, id: ObjectId, cs: &Changeset) -> Result<()> {
    if let Some(sha) = git_sha_of(cs) {
        meta.put_derived(repo, GITSHA_KIND, GITSHA_VERSION, &sha, &id.to_bytes())
            .await?;
    }
    Ok(())
}

/// git sha → Scion 提交 id。
pub async fn lookup_git_sha(meta: &dyn MetaStore, repo: &str, sha: &[u8; 20]) -> Result<Option<ObjectId>> {
    Ok(meta
        .get_derived(repo, GITSHA_KIND, GITSHA_VERSION, sha)
        .await?
        .and_then(|v| ObjectId::from_bytes(&v).ok()))
}

/// git 对象的 sha：`sha1("<kind> <len>\0" + body)`。
pub fn git_hash(kind: &str, body: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(format!("{kind} {}\0", body.len()).as_bytes());
    h.update(body);
    h.finalize().into()
}

fn git_mode(mode: EntryMode) -> &'static str {
    match mode {
        EntryMode::File => "100644",
        EntryMode::Executable => "100755",
        EntryMode::Symlink => "120000",
        EntryMode::Tree => "40000",
    }
}

/// git tree 对象的内容。条目按 git 的规则排序：子目录名按末尾带 `/` 比较。
pub fn git_tree_body(entries: &[(String, EntryMode, [u8; 20])]) -> Vec<u8> {
    let key = |name: &str, mode: EntryMode| {
        let mut k = name.as_bytes().to_vec();
        if mode == EntryMode::Tree {
            k.push(b'/');
        }
        k
    };
    let mut sorted: Vec<_> = entries.iter().collect();
    sorted.sort_by_cached_key(|(name, mode, _)| key(name, *mode));
    let mut out = Vec::new();
    for (name, mode, sha) in sorted {
        out.extend_from_slice(git_mode(*mode).as_bytes());
        out.push(b' ');
        out.extend_from_slice(name.as_bytes());
        out.push(0);
        out.extend_from_slice(sha);
    }
    out
}

/// 计算得到的 git commit 对象的内容。
pub fn git_commit_body(id: ObjectId, cs: &Changeset, tree: &[u8; 20], parents: &[[u8; 20]]) -> Vec<u8> {
    commit_body(Some(id), cs, tree, parents)
}

/// The commit object rebuilt from the commit's fields without the `scion-id` header: what an
/// imported commit's object was, when git wrote no other headers.
pub fn git_commit_body_plain(cs: &Changeset, tree: &[u8; 20], parents: &[[u8; 20]]) -> Vec<u8> {
    commit_body(None, cs, tree, parents)
}

fn commit_body(id: Option<ObjectId>, cs: &Changeset, tree: &[u8; 20], parents: &[[u8; 20]]) -> Vec<u8> {
    scion_object::git::commit_body(id, tree, parents, &cs.author, &cs.committer, &cs.message)
}

async fn cached(meta: &dyn MetaStore, repo: &str, kind: &str, id: &ObjectId) -> Result<Option<[u8; 20]>> {
    Ok(meta
        .get_derived(repo, kind, GIT_OBJECT_VERSION, &id.to_bytes())
        .await?
        .and_then(|v| v.as_slice().try_into().ok()))
}

/// 已经算好的提交 sha（不触发计算），结果与 `ids` 一一对应。
pub async fn cached_git_shas(meta: &dyn MetaStore, repo: &str, ids: &[ObjectId]) -> Result<Vec<Option<[u8; 20]>>> {
    let keys: Vec<Vec<u8>> = ids.iter().map(|id| id.to_bytes().to_vec()).collect();
    Ok(meta
        .get_derived_many(repo, GIT_COMMIT_KIND, GIT_OBJECT_VERSION, &keys)
        .await?
        .into_iter()
        .map(|v| v.and_then(|v| v.as_slice().try_into().ok()))
        .collect())
}

/// 提交的 git sha：有缓存直接返回，否则先算出尚未计算的祖先（父提交在前），再算它本身。
/// 文件内容已被清除（purge）时无法计算，返回 [`QueryError::MissingObject`]。
pub async fn derive_git_sha(meta: &dyn MetaStore, blobs: &dyn BlobStore, repo: &str, id: ObjectId) -> Result<[u8; 20]> {
    if let Some(sha) = cached(meta, repo, GIT_COMMIT_KIND, &id).await? {
        return Ok(sha);
    }
    let mut done: HashMap<ObjectId, [u8; 20]> = HashMap::new();
    let mut loaded: HashMap<ObjectId, (ChangesetRecord, Changeset)> = HashMap::new();
    let mut stack = vec![id];
    while let Some(&top) = stack.last() {
        if done.contains_key(&top) {
            stack.pop();
            continue;
        }
        let (rec, cs) = match loaded.entry(top) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let rec = meta.get_changeset(repo, &top).await?.ok_or(QueryError::NotFound(top))?;
                let cs = load_changeset(blobs, top).await?;
                e.insert((rec, cs))
            }
        };
        let sha = match git_sha_of(cs) {
            Some(sha) => sha,
            None => {
                let mut missing = Vec::new();
                for p in &rec.parents {
                    if done.contains_key(p) {
                        continue;
                    }
                    match cached(meta, repo, GIT_COMMIT_KIND, p).await? {
                        Some(sha) => {
                            done.insert(*p, sha);
                        }
                        None => missing.push(*p),
                    }
                }
                if !missing.is_empty() {
                    stack.extend(missing);
                    continue;
                }
                let tree = tree_sha(meta, blobs, repo, rec.root_tree).await?;
                let parents: Vec<[u8; 20]> = rec.parents.iter().map(|p| done[p]).collect();
                git_hash("commit", &git_commit_body(top, cs, &tree, &parents))
            }
        };
        meta.put_derived(repo, GITSHA_KIND, GITSHA_VERSION, &sha, &top.to_bytes())
            .await?;
        meta.put_derived(repo, GIT_COMMIT_KIND, GIT_OBJECT_VERSION, &top.to_bytes(), &sha)
            .await?;
        done.insert(top, sha);
        loaded.remove(&top);
        stack.pop();
    }
    Ok(done[&id])
}

type ShaFuture<'a> = Pin<Box<dyn Future<Output = Result<[u8; 20]>> + Send + 'a>>;

/// A directory's git tree SHA, computing and caching it (and its subdirectories' and files')
/// when it is not cached yet. Commits imported from git get their SHA without their trees', so
/// their trees may need this.
pub async fn tree_git_sha(meta: &dyn MetaStore, blobs: &dyn BlobStore, repo: &str, id: ObjectId) -> Result<[u8; 20]> {
    tree_sha(meta, blobs, repo, id).await
}

/// 目录树的 git sha（递归，深度等于目录层数）。
fn tree_sha<'a>(meta: &'a dyn MetaStore, blobs: &'a dyn BlobStore, repo: &'a str, id: ObjectId) -> ShaFuture<'a> {
    Box::pin(async move {
        if let Some(sha) = cached(meta, repo, GIT_TREE_KIND, &id).await? {
            return Ok(sha);
        }
        let bytes = blobs.get(&id).await?.ok_or(QueryError::MissingObject(id))?;
        let tree = Tree::decode(&bytes).map_err(|e| QueryError::Corrupt(id, e.to_string()))?;
        let files: Vec<&scion_object::TreeEntry> =
            tree.entries().iter().filter(|e| e.mode != EntryMode::Tree).collect();
        let keys: Vec<Vec<u8>> = files.iter().map(|e| e.id.to_bytes().to_vec()).collect();
        let known = meta
            .get_derived_many(repo, GIT_BLOB_KIND, GIT_OBJECT_VERSION, &keys)
            .await?;
        let mut shas: HashMap<ObjectId, [u8; 20]> = HashMap::new();
        for (e, v) in files.iter().zip(known) {
            let sha = match v.and_then(|v| <[u8; 20]>::try_from(v.as_slice()).ok()) {
                Some(sha) => sha,
                None => {
                    let content = blobs.get(&e.id).await?.ok_or(QueryError::MissingObject(e.id))?;
                    let sha = git_hash("blob", &content);
                    meta.put_derived(repo, GIT_BLOB_KIND, GIT_OBJECT_VERSION, &e.id.to_bytes(), &sha)
                        .await?;
                    meta.put_derived(repo, GIT_BLOB_ID_KIND, GIT_OBJECT_VERSION, &sha, &e.id.to_bytes())
                        .await?;
                    sha
                }
            };
            shas.insert(e.id, sha);
        }
        // One batch read for the subdirectories derived already; only the rest recurse
        let dirs: Vec<ObjectId> = tree
            .entries()
            .iter()
            .filter(|e| e.mode == EntryMode::Tree)
            .map(|e| e.id)
            .collect();
        let dir_keys: Vec<Vec<u8>> = dirs.iter().map(|id| id.to_bytes().to_vec()).collect();
        let dir_known = meta
            .get_derived_many(repo, GIT_TREE_KIND, GIT_OBJECT_VERSION, &dir_keys)
            .await?;
        let dir_shas: HashMap<ObjectId, [u8; 20]> = dirs
            .iter()
            .zip(dir_known)
            .filter_map(|(id, v)| Some((*id, <[u8; 20]>::try_from(v?.as_slice()).ok()?)))
            .collect();
        let mut entries = Vec::with_capacity(tree.entries().len());
        for e in tree.entries() {
            let sha = if e.mode == EntryMode::Tree {
                match dir_shas.get(&e.id) {
                    Some(sha) => *sha,
                    None => tree_sha(meta, blobs, repo, e.id).await?,
                }
            } else {
                shas[&e.id]
            };
            entries.push((e.name.clone(), e.mode, sha));
        }
        let sha = git_hash("tree", &git_tree_body(&entries));
        meta.put_derived(repo, GIT_TREE_KIND, GIT_OBJECT_VERSION, &id.to_bytes(), &sha)
            .await?;
        meta.put_derived(repo, GIT_TREE_ID_KIND, GIT_OBJECT_VERSION, &sha, &id.to_bytes())
            .await?;
        Ok(sha)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::{Signature, TreeEntry};
    use scion_store::MemStore;
    use std::collections::BTreeMap;

    fn hexsha(s: &str) -> [u8; 20] {
        parse_git_sha(s).expect("sha")
    }

    fn sig(name: &str, time_ms: i64, tz: i32) -> Signature {
        Signature {
            name: name.into(),
            email: format!("{}@x", name.to_lowercase().replace(' ', "")),
            time_ms,
            tz_offset_minutes: tz,
        }
    }

    async fn put_blob(s: &MemStore, content: &[u8]) -> TreeEntry {
        let id = ObjectId::of_blob(content);
        s.put(id, Bytes::copy_from_slice(content)).await.expect("put");
        TreeEntry {
            name: String::new(),
            mode: EntryMode::File,
            id,
            size: content.len() as u64,
        }
    }

    async fn put_tree(s: &MemStore, entries: Vec<TreeEntry>) -> ObjectId {
        let tree = Tree::new(entries).expect("tree");
        let bytes = tree.encode();
        let id = ObjectId::hash(scion_object::ObjectKind::Tree, &bytes);
        s.put(id, Bytes::from(bytes)).await.expect("put");
        id
    }

    fn named(mut e: TreeEntry, name: &str, mode: EntryMode) -> TreeEntry {
        e.name = name.into();
        e.mode = mode;
        e
    }

    /// 与 `git mktree` 得到的结果相同的目录：a.txt、a/f、run（可执行）、link（符号链接）。
    async fn sample_tree(s: &MemStore) -> ObjectId {
        let hello = put_blob(s, b"hello\n").await;
        let sub = put_tree(s, vec![named(hello.clone(), "f", EntryMode::File)]).await;
        let run = put_blob(s, b"#!/bin/sh\n").await;
        let link = put_blob(s, b"target").await;
        put_tree(
            s,
            vec![
                named(hello, "a.txt", EntryMode::File),
                TreeEntry {
                    name: "a".into(),
                    mode: EntryMode::Tree,
                    id: sub,
                    size: 0,
                },
                named(run, "run", EntryMode::Executable),
                named(link, "link", EntryMode::Symlink),
            ],
        )
        .await
    }

    async fn put_commit(s: &MemStore, cs: &Changeset, root: ObjectId, generation: u64) -> ObjectId {
        let id = cs.id();
        s.put(id, Bytes::from(cs.encode())).await.expect("put");
        let rec = ChangesetRecord {
            id,
            root_tree: root,
            parents: cs.parents.clone(),
            generation,
            fp_depth: 0,
            fp_jump: None,
        };
        s.put_changesets("r", &[rec]).await.expect("record");
        id
    }

    fn native(parents: Vec<ObjectId>, msg: &str) -> Changeset {
        Changeset::new(
            parents,
            sig("A U", 1_700_000_000_999, 480),
            sig("C", 1_700_000_001_000, -90),
            msg,
            [1; 16],
            vec![],
            BTreeMap::new(),
        )
        .expect("valid")
    }

    #[test]
    fn git_object_vectors() {
        assert_eq!(
            git_hash("blob", b"hello\n"),
            hexsha("ce013625030ba8dba906f756967f9e9ca394464a")
        );
        assert_eq!(
            git_hash("tree", &git_tree_body(&[])),
            hexsha("4b825dc642cb6eb9a060e54bf8d69288fbee4904")
        );
        // 子目录 "a" 按 "a/" 排在 "a.txt" 之后
        let body = git_tree_body(&[
            (
                "a".into(),
                EntryMode::Tree,
                hexsha("10731d0b170b98481a00bdca161e874e0ab93377"),
            ),
            (
                "run".into(),
                EntryMode::Executable,
                hexsha("1a2485251c33a70432394c93fb89330ef214bfc9"),
            ),
            (
                "link".into(),
                EntryMode::Symlink,
                hexsha("1de565933b05f74c75ff9a6520af5f9f8a5a2f1d"),
            ),
            (
                "a.txt".into(),
                EntryMode::File,
                hexsha("ce013625030ba8dba906f756967f9e9ca394464a"),
            ),
        ]);
        assert_eq!(
            git_hash("tree", &body),
            hexsha("0af600176988e30f33680a463fea38056e15db90")
        );
    }

    #[test]
    fn commit_body_format() {
        let mut cs = native(vec![], "msg\n");
        cs.author.name = "A <U>\n".into();
        let id = cs.id();
        let body = git_commit_body(id, &cs, &[0xaa; 20], &[[0x11; 20], [0x22; 20]]);
        let expect = format!(
            "tree {}\nparent {}\nparent {}\nauthor A U <au@x> 1700000000 +0800\n\
             committer C <c@x> 1700000001 -0130\nscion-id {id}\n\nmsg\n",
            "aa".repeat(20),
            "11".repeat(20),
            "22".repeat(20)
        );
        assert_eq!(String::from_utf8(body).expect("utf8"), expect);
    }

    #[tokio::test]
    async fn derive_native_chain_and_cache() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        let root = sample_tree(&s).await;
        let c1 = native(vec![], "one\n");
        let id1 = put_commit(&s, &c1, root, 1).await;
        let c2 = native(vec![id1], "two\n");
        let id2 = put_commit(&s, &c2, root, 2).await;

        // 先算子提交：父提交随之算出
        let sha2 = derive_git_sha(&s, &s, "r", id2).await.expect("derive");
        let tree = hexsha("0af600176988e30f33680a463fea38056e15db90");
        let sha1 = git_hash("commit", &git_commit_body(id1, &c1, &tree, &[]));
        assert_eq!(sha2, git_hash("commit", &git_commit_body(id2, &c2, &tree, &[sha1])));
        assert_eq!(
            cached_git_shas(&s, "r", &[id1, id2]).await,
            Ok(vec![Some(sha1), Some(sha2)])
        );
        assert_eq!(lookup_git_sha(&s, "r", &sha1).await, Ok(Some(id1)));
        assert_eq!(lookup_git_sha(&s, "r", &sha2).await, Ok(Some(id2)));
        assert_eq!(cached(&s, "r", GIT_TREE_KIND, &root).await, Ok(Some(tree)));
        // and the other way round
        let back = s.get_derived("r", GIT_TREE_ID_KIND, GIT_OBJECT_VERSION, &tree).await;
        assert_eq!(back, Ok(Some(root.to_bytes().to_vec())));
        let hello_sha = hexsha("ce013625030ba8dba906f756967f9e9ca394464a");
        let back = s
            .get_derived("r", GIT_BLOB_ID_KIND, GIT_OBJECT_VERSION, &hello_sha)
            .await;
        assert_eq!(back, Ok(Some(ObjectId::of_blob(b"hello\n").to_bytes().to_vec())));

        // 缓存命中时不再读取对象：删掉文件内容后仍能得到结果
        let hello = ObjectId::of_blob(b"hello\n");
        s.delete(&hello).await.expect("delete");
        assert_eq!(derive_git_sha(&s, &s, "r", id2).await, Ok(sha2));

        // 新提交复用未变化的子目录；内容缺失的新目录报错
        let lost = put_tree(&s, vec![named(put_blob(&s, b"x").await, "x", EntryMode::File)]).await;
        let gone = ObjectId::of_blob(b"x");
        s.delete(&gone).await.expect("delete");
        let c3 = native(vec![id2], "three\n");
        let id3 = put_commit(&s, &c3, lost, 3).await;
        assert_eq!(
            derive_git_sha(&s, &s, "r", id3).await,
            Err(QueryError::MissingObject(gone))
        );
        assert_eq!(cached_git_shas(&s, "r", &[id3]).await, Ok(vec![None]));
    }

    #[tokio::test]
    async fn derive_imported_and_errors() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        let root = sample_tree(&s).await;
        // 导入的提交直接使用记录的 sha，不需要父提交和目录树
        let imported = cs_with(Some(vec![7; 20]));
        let missing_tree = ObjectId::hash(scion_object::ObjectKind::Tree, b"nope");
        let idi = put_commit(&s, &imported, missing_tree, 1).await;
        assert_eq!(derive_git_sha(&s, &s, "r", idi).await, Ok([7; 20]));
        // 原生提交以导入提交为父
        let child = native(vec![idi], "c\n");
        let idc = put_commit(&s, &child, root, 2).await;
        let tree = hexsha("0af600176988e30f33680a463fea38056e15db90");
        assert_eq!(
            derive_git_sha(&s, &s, "r", idc).await,
            Ok(git_hash("commit", &git_commit_body(idc, &child, &tree, &[[7; 20]])))
        );
        // 不属于该仓库的提交
        let stray = native(vec![], "stray\n").id();
        assert_eq!(
            derive_git_sha(&s, &s, "r", stray).await,
            Err(QueryError::NotFound(stray))
        );
        // 目录树对象缺失或损坏
        let bad = native(vec![], "bad\n");
        let idb = put_commit(&s, &bad, missing_tree, 1).await;
        assert_eq!(
            derive_git_sha(&s, &s, "r", idb).await,
            Err(QueryError::MissingObject(missing_tree))
        );
        let junk = ObjectId::hash(scion_object::ObjectKind::Tree, b"junk");
        s.put(junk, Bytes::from_static(b"junk")).await.expect("put");
        let bad2 = native(vec![], "bad2\n");
        let idb2 = put_commit(&s, &bad2, junk, 1).await;
        assert!(matches!(
            derive_git_sha(&s, &s, "r", idb2).await,
            Err(QueryError::Corrupt(id, _)) if id == junk
        ));
    }

    fn cs_with(ext: Option<Vec<u8>>) -> Changeset {
        let sig = Signature {
            name: "a".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        let mut extensions = BTreeMap::new();
        if let Some(v) = ext {
            extensions.insert(GIT_SHA1_EXTENSION.to_string(), v);
        }
        Changeset::new(vec![], sig.clone(), sig, "m", [0; 16], vec![], extensions).expect("valid")
    }

    #[test]
    fn parse_git_sha_cases() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(parse_git_sha(sha).map(|b| b[0]), Some(0x01));
        assert_eq!(parse_git_sha(&sha.to_uppercase()), None, "uppercase rejected");
        assert_eq!(parse_git_sha(&sha[..39]), None);
        assert_eq!(parse_git_sha("main"), None);
        assert_eq!(parse_git_sha("g123456789abcdef0123456789abcdef01234567"), None);
    }

    #[test]
    fn git_sha_of_requires_20_bytes() {
        assert_eq!(git_sha_of(&cs_with(Some(vec![7; 20]))), Some([7; 20]));
        assert_eq!(git_sha_of(&cs_with(Some(vec![7; 19]))), None);
        assert_eq!(git_sha_of(&cs_with(None)), None);
    }

    #[tokio::test]
    async fn index_and_lookup() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        let cs = cs_with(Some(vec![9; 20]));
        let id = cs.id();
        index_git_sha(&s, "r", id, &cs).await.expect("index");
        assert_eq!(lookup_git_sha(&s, "r", &[9; 20]).await, Ok(Some(id)));
        assert_eq!(lookup_git_sha(&s, "r", &[8; 20]).await, Ok(None));
        // 没有 git sha 的提交：什么都不写
        let plain = cs_with(None);
        index_git_sha(&s, "r", plain.id(), &plain).await.expect("noop");
        // 损坏的映射值被忽略
        s.put_derived("r", GITSHA_KIND, GITSHA_VERSION, &[1; 20], b"bad")
            .await
            .expect("put");
        assert_eq!(lookup_git_sha(&s, "r", &[1; 20]).await, Ok(None));
    }
}
