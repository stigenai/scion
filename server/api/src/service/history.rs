//! QueryService 中的历史查询：Log、IsAncestor、MergeBase、Diff。

use bytes::Bytes;
use scion_object::{ObjectId, ObjectKind, Signature};
use scion_proto::v1::{
    self, BlameRequest, BlameResponse, DerivedKind, DiffRequest, DiffResponse, GetDerivedRequest, GetDerivedResponse,
    GetGitShasRequest, GetGitShasResponse, IsAncestorRequest, IsAncestorResponse, ListLastCommitsRequest,
    ListLastCommitsResponse, LogRequest, LogResponse, MergeBaseRequest, MergeBaseResponse, ResolveRevisionRequest,
    ResolveRevisionResponse, StatRequest, StatResponse,
};
use scion_query::{LogEntry, QueryError};
use scion_store::{BlobStore, MetaStore};
use scion_tree::BlobTreeSource;
use tonic::Status;

use crate::service::query::{check_path, proto_entry};
use crate::service::support::resolve_revision;
use crate::status;

pub(crate) const DEFAULT_PAGE_SIZE: u32 = 50;
pub(crate) const MAX_PAGE_SIZE: u32 = 1000;
/// 每页最多检查的提交数（按路径过滤时的延迟上限）。
pub(crate) const MAX_SCAN: usize = 10_000;

/// 分页令牌：前沿提交 id 依次拼接后的十六进制。
pub(crate) fn encode_token(ids: &[ObjectId]) -> String {
    hex::encode(ids.iter().flat_map(|id| id.to_bytes()).collect::<Vec<u8>>())
}

pub(crate) fn decode_token(token: &str) -> Result<Vec<ObjectId>, Status> {
    let bad = || Status::invalid_argument("invalid page_token");
    let raw = hex::decode(token).map_err(|_| bad())?;
    if raw.is_empty() || raw.len() % scion_object::ID_LEN != 0 {
        return Err(bad());
    }
    raw.chunks(scion_object::ID_LEN)
        .map(|c| match ObjectId::from_bytes(c) {
            Ok(id) if id.kind() == ObjectKind::Changeset => Ok(id),
            _ => Err(bad()),
        })
        .collect()
}

pub(crate) fn from_query(e: QueryError) -> Status {
    match e {
        QueryError::NotFound(id) => Status::not_found(format!("changeset {id} not found")),
        QueryError::Corrupt(id, msg) => {
            tracing::error!(%id, error = %msg, "changeset object missing or corrupt");
            Status::internal(format!("changeset {id} is missing or corrupt"))
        }
        QueryError::Store(e) => status::from_store(e),
        QueryError::PathNotFound(p) => status::path_not_found(&p),
        QueryError::NotAFile(p) => Status::failed_precondition(format!("{p:?} is a directory")),
        QueryError::Tree(e) => status::from_tree(e),
        QueryError::MissingObject(id) => Status::failed_precondition(format!("object {id} is missing (purged?)")),
        QueryError::Archive(msg) => Status::failed_precondition(format!("cannot build the archive: {msg}")),
    }
}

fn proto_sig(s: &Signature) -> v1::Signature {
    v1::Signature {
        name: s.name.clone(),
        email: s.email.clone(),
        time_ms: s.time_ms,
        tz_offset_minutes: s.tz_offset_minutes,
    }
}

fn id_bytes(id: &ObjectId) -> Bytes {
    Bytes::copy_from_slice(&id.to_bytes())
}

fn log_entry(e: LogEntry) -> v1::LogEntry {
    v1::LogEntry {
        changeset_id: id_bytes(&e.record.id),
        parents: e.record.parents.iter().map(id_bytes).collect(),
        author: Some(proto_sig(&e.changeset.author)),
        committer: Some(proto_sig(&e.changeset.committer)),
        message: e.changeset.message,
        generation: e.record.generation,
        root_tree: id_bytes(&e.record.root_tree),
        extensions: e
            .changeset
            .extensions
            .into_iter()
            .map(|(k, v)| (k, Bytes::from(v)))
            .collect(),
    }
}

pub(crate) async fn log(meta: &dyn MetaStore, blobs: &dyn BlobStore, req: LogRequest) -> Result<LogResponse, Status> {
    check_path(&req.path)?;
    let start = if req.page_token.is_empty() {
        vec![resolve_revision(meta, &req.repo, &req.revision).await?.id]
    } else {
        decode_token(&req.page_token)?
    };
    let limit = match req.page_size {
        0 => DEFAULT_PAGE_SIZE,
        n => n.min(MAX_PAGE_SIZE),
    } as usize;
    let page = if req.follow {
        if req.path.is_empty() {
            return Err(Status::invalid_argument("follow needs a path"));
        }
        scion_query::log_follow(meta, blobs, &req.repo, &start, &req.path, limit, MAX_SCAN).await
    } else {
        scion_query::log(meta, blobs, &req.repo, &start, Some(req.path.as_str()), limit, MAX_SCAN).await
    }
    .map_err(from_query)?;
    Ok(LogResponse {
        entries: page.entries.into_iter().map(log_entry).collect(),
        next_page_token: page.next.map(|f| encode_token(&f)).unwrap_or_default(),
        renamed_from: page.renamed_from.unwrap_or_default(),
    })
}

pub(crate) async fn is_ancestor(meta: &dyn MetaStore, req: IsAncestorRequest) -> Result<IsAncestorResponse, Status> {
    let a = resolve_revision(meta, &req.repo, &req.ancestor).await?.id;
    let d = resolve_revision(meta, &req.repo, &req.descendant).await?.id;
    let yes = scion_query::is_ancestor(meta, &req.repo, a, d)
        .await
        .map_err(from_query)?;
    Ok(IsAncestorResponse { is_ancestor: yes })
}

pub(crate) async fn merge_base(meta: &dyn MetaStore, req: MergeBaseRequest) -> Result<MergeBaseResponse, Status> {
    let a = resolve_revision(meta, &req.repo, &req.a).await?.id;
    let b = resolve_revision(meta, &req.repo, &req.b).await?.id;
    let bases = scion_query::merge_bases(meta, &req.repo, a, b)
        .await
        .map_err(from_query)?;
    Ok(MergeBaseResponse {
        bases: bases.iter().map(id_bytes).collect(),
    })
}

pub(crate) async fn diff(
    meta: &dyn MetaStore,
    blobs: std::sync::Arc<dyn BlobStore>,
    req: DiffRequest,
) -> Result<DiffResponse, Status> {
    check_path(&req.path)?;
    let from = if req.from.is_empty() {
        None
    } else {
        Some(resolve_revision(meta, &req.repo, &req.from).await?.root_tree)
    };
    let to = resolve_revision(meta, &req.repo, &req.to).await?.root_tree;
    let files = scion_tree::diff(&BlobTreeSource(blobs), from, Some(to), &req.path)
        .await
        .map_err(status::from_tree)?;
    Ok(DiffResponse {
        files: files
            .into_iter()
            .map(|d| v1::FileDiff {
                path: d.path,
                old: d.old.as_ref().map(proto_entry),
                new: d.new.as_ref().map(proto_entry),
            })
            .collect(),
    })
}

/// Diff 流式返回时每批的文件数。
pub(crate) const DIFF_BATCH: usize = 500;

/// 把差异切成若干批；没有差异时也返回一批（空），让客户端收到明确的结束。
pub(crate) fn diff_batches(resp: DiffResponse, batch: usize) -> Vec<DiffResponse> {
    if resp.files.is_empty() {
        return vec![resp];
    }
    resp.files
        .chunks(batch.max(1))
        .map(|c| DiffResponse { files: c.to_vec() })
        .collect()
}

pub(crate) async fn resolve(
    meta: &dyn MetaStore,
    req: ResolveRevisionRequest,
) -> Result<ResolveRevisionResponse, Status> {
    let rec = resolve_revision(meta, &req.repo, &req.revision).await?;
    Ok(ResolveRevisionResponse {
        changeset_id: id_bytes(&rec.id),
        root_tree: id_bytes(&rec.root_tree),
        generation: rec.generation,
        parents: rec.parents.iter().map(id_bytes).collect(),
    })
}

pub(crate) async fn stat(
    meta: &dyn MetaStore,
    blobs: std::sync::Arc<dyn BlobStore>,
    req: StatRequest,
) -> Result<StatResponse, Status> {
    check_path(&req.path)?;
    let rec = resolve_revision(meta, &req.repo, &req.revision).await?;
    let entry = if req.path.is_empty() {
        scion_object::TreeEntry {
            name: String::new(),
            mode: scion_object::EntryMode::Tree,
            id: rec.root_tree,
            size: 0,
        }
    } else {
        scion_tree::lookup(&BlobTreeSource(blobs), rec.root_tree, &req.path)
            .await
            .map_err(status::from_tree)?
            .ok_or_else(|| status::path_not_found(&req.path))?
    };
    Ok(StatResponse {
        changeset_id: id_bytes(&rec.id),
        entry: Some(proto_entry(&entry)),
    })
}

/// ListLastCommits 每页的默认（也是最大）条目数。
pub(crate) const MAX_LAST_COMMITS: u32 = 1000;

pub(crate) fn join_path(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

/// `names` 为目录中对调用方可见的条目（按名字排序）；`all` 表示它就是目录的全部条目，此时结果写入缓存。
/// `hidden` 中的条目只列名字，不给出最后提交。
pub(crate) async fn list_last_commits(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    req: &ListLastCommitsRequest,
    cs: ObjectId,
    names: Vec<String>,
    all: bool,
    hidden: &std::collections::HashSet<String>,
) -> Result<ListLastCommitsResponse, Status> {
    let total = names.len();
    let limit = match req.limit {
        0 => MAX_LAST_COMMITS,
        n => n.min(MAX_LAST_COMMITS),
    } as usize;
    let start = (req.offset as usize).min(total);
    let end = start.saturating_add(limit).min(total);
    let page = &names[start..end];
    let cache = all && page.len() == total;
    let found = scion_query::last_commits(meta, blobs, &req.repo, cs, &req.path, page, cache)
        .await
        .map_err(from_query)?;
    let mut entries = Vec::with_capacity(page.len());
    let mut ids = Vec::new();
    for (name, id) in page.iter().zip(found) {
        let id = id.filter(|_| !hidden.contains(name));
        if let Some(id) = id {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        entries.push(v1::LastCommit {
            name: name.clone(),
            changeset_id: id.as_ref().map(id_bytes).unwrap_or_default(),
        });
    }
    let mut commits = Vec::with_capacity(ids.len());
    for id in ids {
        let record = meta
            .get_changeset(&req.repo, &id)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| from_query(QueryError::NotFound(id)))?;
        let changeset = scion_query::load_changeset(blobs, id).await.map_err(from_query)?;
        commits.push(LogEntry { record, changeset });
    }
    commits.sort_by_key(|c| std::cmp::Reverse(c.record.generation));
    Ok(ListLastCommitsResponse {
        changeset_id: id_bytes(&cs),
        entries,
        commits: commits.into_iter().map(log_entry).collect(),
        total: u32::try_from(total).unwrap_or(u32::MAX),
    })
}

/// 一次 GetGitShas 最多映射的提交数。
pub(crate) const MAX_GIT_SHAS: usize = 1000;

pub(crate) async fn get_git_shas(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    req: GetGitShasRequest,
) -> Result<GetGitShasResponse, Status> {
    if req.changeset_ids.len() > MAX_GIT_SHAS {
        return Err(Status::invalid_argument(format!(
            "at most {MAX_GIT_SHAS} changeset ids per request"
        )));
    }
    let ids = req
        .changeset_ids
        .iter()
        .map(|b| match ObjectId::from_bytes(b) {
            Ok(id) if id.kind() == ObjectKind::Changeset => Ok(id),
            _ => Err(Status::invalid_argument("invalid changeset id")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let cached = scion_query::cached_git_shas(meta, &req.repo, &ids)
        .await
        .map_err(from_query)?;
    let mut out = Vec::with_capacity(ids.len());
    for (id, sha) in ids.into_iter().zip(cached) {
        let sha = match sha {
            Some(sha) => sha,
            None => scion_query::derive_git_sha(meta, blobs, &req.repo, id)
                .await
                .map_err(from_query)?,
        };
        out.push(Bytes::copy_from_slice(&sha));
    }
    Ok(GetGitShasResponse { git_sha1: out })
}

pub(crate) async fn get_derived(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    req: GetDerivedRequest,
) -> Result<GetDerivedResponse, Status> {
    let rec = resolve_revision(meta, &req.repo, &req.revision).await?;
    let value = match DerivedKind::try_from(req.kind) {
        Ok(DerivedKind::GitSha1) => {
            let sha = scion_query::derive_git_sha(meta, blobs, &req.repo, rec.id)
                .await
                .map_err(from_query)?;
            Some(v1::get_derived_response::Value::GitSha1(Bytes::copy_from_slice(&sha)))
        }
        Ok(DerivedKind::Generation) => Some(v1::get_derived_response::Value::Generation(v1::Generation {
            generation: rec.generation,
            first_parent_depth: rec.fp_depth,
        })),
        _ => {
            return Err(Status::invalid_argument(format!(
                "unsupported derived kind {}",
                req.kind
            )));
        }
    };
    Ok(GetDerivedResponse {
        changeset_id: id_bytes(&rec.id),
        value,
    })
}

pub(crate) async fn blame(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    req: BlameRequest,
) -> Result<BlameResponse, Status> {
    check_path(&req.path)?;
    if req.path.is_empty() {
        return Err(Status::invalid_argument("path is required"));
    }
    let cs = resolve_revision(meta, &req.repo, &req.revision).await?.id;
    let res = scion_query::blame(meta, blobs, &req.repo, cs, &req.path)
        .await
        .map_err(from_query)?;

    // 提交按首次出现的顺序编号；连续同一提交的行合并为一个区间
    let mut commits: Vec<ObjectId> = Vec::new();
    let mut ranges: Vec<v1::BlameRange> = Vec::new();
    for (i, id) in res.lines.iter().enumerate() {
        let idx = match commits.iter().position(|c| c == id) {
            Some(p) => p,
            None => {
                commits.push(*id);
                commits.len() - 1
            }
        } as u32;
        match ranges.last_mut() {
            Some(r) if r.commit_index == idx => r.line_count += 1,
            _ => ranges.push(v1::BlameRange {
                start_line: i as u32 + 1,
                line_count: 1,
                commit_index: idx,
            }),
        }
    }
    let mut out = Vec::with_capacity(commits.len());
    for id in &commits {
        let cs = scion_query::load_changeset(blobs, *id).await.map_err(from_query)?;
        out.push(v1::BlameCommit {
            changeset_id: id_bytes(id),
            author: Some(proto_sig(&cs.author)),
            summary: cs.message.lines().next().unwrap_or_default().to_owned(),
        });
    }
    Ok(BlameResponse {
        blob_id: id_bytes(&res.blob),
        commits: out,
        ranges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::land::LandSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, put, spec, svc, upload};
    use scion_store::ChangesetRecord;
    use std::sync::Arc;
    use tonic::Code;

    fn cs(n: u8) -> ObjectId {
        ObjectId::hash(ObjectKind::Changeset, &[n])
    }

    #[test]
    fn token_roundtrip_and_validation() {
        let ids = vec![cs(1), cs(2)];
        assert_eq!(decode_token(&encode_token(&ids)).ok(), Some(ids));
        let code = |s: &str| decode_token(s).map(|_| ()).map_err(|e| e.code());
        assert_eq!(code("zz"), Err(Code::InvalidArgument), "not hex");
        assert_eq!(code(""), Err(Code::InvalidArgument), "empty");
        assert_eq!(code("0102"), Err(Code::InvalidArgument), "wrong length");
        let blob = encode_token(&[ObjectId::of_blob(b"x")]);
        assert_eq!(code(&blob), Err(Code::InvalidArgument), "not a changeset id");
    }

    #[test]
    fn from_query_maps_every_variant() {
        assert_eq!(from_query(QueryError::PathNotFound("p".into())).code(), Code::NotFound);
        assert_eq!(
            from_query(QueryError::NotAFile("d".into())).code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            from_query(QueryError::Tree(scion_tree::TreeError::Storage("x".into()))).code(),
            Code::Unavailable
        );
        assert_eq!(from_query(QueryError::NotFound(cs(1))).code(), Code::NotFound);
        assert_eq!(
            from_query(QueryError::Corrupt(cs(1), "x".into())).code(),
            Code::Internal
        );
        assert_eq!(
            from_query(QueryError::Store(scion_store::Error::Backend("x".into()))).code(),
            Code::Unavailable
        );
    }

    /// main: a ─ b ─ c，另有从 a 分出的 d。
    async fn setup() -> (Arc<scion_store::MemStore>, [ChangesetRecord; 4]) {
        let (store, w) = svc().await;
        for c in [&b"1"[..], b"2", b"3", b"4"] {
            upload(&w, c).await;
        }
        let mk = |p: &[ObjectId], d| spec_to_changeset(spec(p, d), 10).expect("spec");
        let a = w.create(REPO, &mk(&[], vec![put("README", b"1")])).await.expect("a");
        let b = w
            .create(REPO, &mk(&[a.id], vec![put("src/x.rs", b"2")]))
            .await
            .expect("b");
        let c = w
            .create(REPO, &mk(&[b.id], vec![put("README", b"3")]))
            .await
            .expect("c");
        let d = w.create(REPO, &mk(&[a.id], vec![put("other", b"4")])).await.expect("d");
        let land = LandSvc::new(scion_land::Coordinator::new(w.writer().clone(), Default::default()));
        for x in [&a, &b, &c] {
            land.land(REPO, "main", x.id, false).await.expect("land");
        }
        (store, [a, b, c, d])
    }

    fn log_req(revision: &str, path: &str, page_size: u32, token: &str) -> LogRequest {
        LogRequest {
            repo: REPO.into(),
            revision: revision.into(),
            path: path.into(),
            page_size,
            page_token: token.into(),
            follow: false,
        }
    }

    #[tokio::test]
    async fn log_pages_and_filters() {
        let (store, [a, b, c, _d]) = setup().await;
        let r = log(store.as_ref(), store.as_ref(), log_req("main", "", 2, ""))
            .await
            .expect("page 1");
        let ids: Vec<_> = r
            .entries
            .iter()
            .map(|e| ObjectId::from_bytes(&e.changeset_id).expect("id"))
            .collect();
        assert_eq!(ids, vec![c.id, b.id]);
        assert_eq!(r.entries[0].author.as_ref().expect("author").name, "Ada");
        assert_eq!(r.entries[0].generation, 3);
        assert_eq!(ObjectId::from_bytes(&r.entries[0].root_tree).ok(), Some(c.root_tree));
        let r2 = log(store.as_ref(), store.as_ref(), log_req("", "", 2, &r.next_page_token))
            .await
            .expect("page 2");
        assert_eq!(r2.entries.len(), 1);
        assert_eq!(ObjectId::from_bytes(&r2.entries[0].changeset_id).ok(), Some(a.id));
        assert!(r2.next_page_token.is_empty());
        let r = log(store.as_ref(), store.as_ref(), log_req("main", "README", 0, ""))
            .await
            .expect("filtered");
        assert_eq!(r.entries.len(), 2, "default page size; only commits touching README");
        let r = log(store.as_ref(), store.as_ref(), log_req("main", "", 5000, ""))
            .await
            .expect("clamped");
        assert_eq!(r.entries.len(), 3);
    }

    #[tokio::test]
    async fn log_errors() {
        let (store, _) = setup().await;
        let code = |r: Result<LogResponse, Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("nope", "", 1, "")).await),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("main", "/bad", 1, "")).await),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("", "", 1, "zz")).await),
            Err(Code::InvalidArgument)
        );
        let mut follow = log_req("main", "", 1, "");
        follow.follow = true;
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), follow).await),
            Err(Code::InvalidArgument)
        );
        let unknown = encode_token(&[cs(99)]);
        assert_eq!(
            code(log(store.as_ref(), store.as_ref(), log_req("", "", 1, &unknown)).await),
            Err(Code::NotFound)
        );
    }

    #[tokio::test]
    async fn ancestry_queries() {
        let (store, [a, _b, c, d]) = setup().await;
        let anc = |x: &ObjectId, y: &str| IsAncestorRequest {
            repo: REPO.into(),
            ancestor: x.to_string(),
            descendant: y.into(),
        };
        assert!(
            is_ancestor(store.as_ref(), anc(&a.id, "main"))
                .await
                .expect("ok")
                .is_ancestor
        );
        assert!(
            !is_ancestor(store.as_ref(), anc(&d.id, "main"))
                .await
                .expect("ok")
                .is_ancestor
        );
        let mb = merge_base(
            store.as_ref(),
            MergeBaseRequest {
                repo: REPO.into(),
                a: "main".into(),
                b: d.id.to_string(),
            },
        )
        .await
        .expect("ok");
        assert_eq!(mb.bases.len(), 1);
        assert_eq!(ObjectId::from_bytes(&mb.bases[0]).ok(), Some(a.id));
        let r = is_ancestor(
            store.as_ref(),
            IsAncestorRequest {
                repo: REPO.into(),
                ancestor: "nope".into(),
                descendant: c.id.to_string(),
            },
        )
        .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = merge_base(
            store.as_ref(),
            MergeBaseRequest {
                repo: REPO.into(),
                a: "main".into(),
                b: "nope".into(),
            },
        )
        .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }

    #[tokio::test]
    async fn diff_between_revisions() {
        let (store, [a, _b, _c, _d]) = setup().await;
        let blobs: Arc<dyn BlobStore> = store.clone();
        let req = |from: &str, path: &str| DiffRequest {
            repo: REPO.into(),
            from: from.into(),
            to: "main".into(),
            path: path.into(),
        };
        let r = diff(store.as_ref(), blobs.clone(), req(&a.id.to_string(), ""))
            .await
            .expect("diff");
        let paths: Vec<_> = r.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["README", "src/x.rs"]);
        assert!(r.files[0].old.is_some() && r.files[0].new.is_some(), "modified");
        assert!(r.files[1].old.is_none(), "added");
        let r = diff(store.as_ref(), blobs.clone(), req("", ""))
            .await
            .expect("from empty");
        assert_eq!(r.files.len(), 2);
        let r = diff(store.as_ref(), blobs.clone(), req(&a.id.to_string(), "src"))
            .await
            .expect("limited");
        assert_eq!(r.files.len(), 1);
        let code = |r: Result<DiffResponse, Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(diff(store.as_ref(), blobs.clone(), req("nope", "")).await),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(diff(store.as_ref(), blobs, req("", "a//b")).await),
            Err(Code::InvalidArgument)
        );
    }

    #[test]
    fn diff_batches_split_and_keep_empty() {
        let f = |p: &str| v1::FileDiff {
            path: p.into(),
            old: None,
            new: None,
        };
        let batches = diff_batches(
            DiffResponse {
                files: vec![f("a"), f("b"), f("c")],
            },
            2,
        );
        assert_eq!(batches.iter().map(|b| b.files.len()).collect::<Vec<_>>(), vec![2, 1]);
        assert_eq!(diff_batches(DiffResponse { files: vec![] }, 2).len(), 1);
        assert_eq!(
            diff_batches(DiffResponse { files: vec![f("a")] }, 0).len(),
            1,
            "batch clamped to 1"
        );
    }

    #[tokio::test]
    async fn resolve_stat_and_derived() {
        let (store, [a, b, _c, _d]) = setup().await;
        let blobs: Arc<dyn BlobStore> = store.clone();
        let r = resolve(
            store.as_ref(),
            ResolveRevisionRequest {
                repo: REPO.into(),
                revision: "main".into(),
            },
        )
        .await
        .expect("resolve");
        assert_eq!(r.generation, 3);
        assert_eq!(r.parents.len(), 1);
        let r = resolve(
            store.as_ref(),
            ResolveRevisionRequest {
                repo: REPO.into(),
                revision: a.id.to_string(),
            },
        )
        .await
        .expect("by id");
        assert!(r.parents.is_empty());

        let st = |path: &str| StatRequest {
            repo: REPO.into(),
            revision: "main".into(),
            path: path.into(),
        };
        let root = stat(store.as_ref(), blobs.clone(), st(""))
            .await
            .expect("root")
            .entry
            .expect("entry");
        assert_eq!((root.name.as_str(), root.mode()), ("", v1::EntryMode::Tree));
        let file = stat(store.as_ref(), blobs.clone(), st("src/x.rs"))
            .await
            .expect("file")
            .entry
            .expect("entry");
        assert_eq!((file.mode(), file.size), (v1::EntryMode::File, 1));
        let code = |r: Result<StatResponse, Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(stat(store.as_ref(), blobs.clone(), st("nope")).await),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(stat(store.as_ref(), blobs.clone(), st("/bad")).await),
            Err(Code::InvalidArgument)
        );

        let gd = |kind: DerivedKind, rev: &str| GetDerivedRequest {
            repo: REPO.into(),
            revision: rev.into(),
            kind: kind.into(),
        };
        let g = get_derived(
            store.as_ref(),
            store.as_ref(),
            gd(DerivedKind::Generation, &b.id.to_string()),
        )
        .await
        .expect("gen");
        assert_eq!(
            g.value,
            Some(v1::get_derived_response::Value::Generation(v1::Generation {
                generation: 2,
                first_parent_depth: 2
            }))
        );
        let g = get_derived(store.as_ref(), store.as_ref(), gd(DerivedKind::GitSha1, "main"))
            .await
            .expect("sha");
        let Some(v1::get_derived_response::Value::GitSha1(sha)) = g.value else {
            panic!("native commits get a computed sha: {:?}", g.value);
        };
        let by_sha = resolve(
            store.as_ref(),
            ResolveRevisionRequest {
                repo: REPO.into(),
                revision: hex::encode(&sha),
            },
        )
        .await
        .expect("resolve by sha");
        assert_eq!(by_sha.changeset_id, g.changeset_id, "computed sha resolves back");
        let r = get_derived(store.as_ref(), store.as_ref(), gd(DerivedKind::Unspecified, "main")).await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::InvalidArgument));
        let r = resolve(
            store.as_ref(),
            ResolveRevisionRequest {
                repo: REPO.into(),
                revision: "nope".into(),
            },
        )
        .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }

    #[tokio::test]
    async fn get_derived_git_sha_of_imported_commit() {
        let (store, w) = svc().await;
        let mut sp = spec(&[], vec![]);
        sp.extensions
            .insert(scion_query::GIT_SHA1_EXTENSION.into(), Bytes::from_static(&[0xab; 20]));
        let cs = w
            .create(REPO, &spec_to_changeset(sp, 10).expect("spec"))
            .await
            .expect("create");
        let req = GetDerivedRequest {
            repo: REPO.into(),
            revision: hex::encode([0xab; 20]),
            kind: DerivedKind::GitSha1.into(),
        };
        let g = get_derived(store.as_ref(), store.as_ref(), req).await.expect("derived");
        assert_eq!(
            ObjectId::from_bytes(&g.changeset_id).ok(),
            Some(cs.id),
            "resolved by git sha"
        );
        assert_eq!(
            g.value,
            Some(v1::get_derived_response::Value::GitSha1(Bytes::from_static(
                &[0xab; 20]
            )))
        );
    }

    #[tokio::test]
    async fn get_git_shas_batch() {
        let (store, w) = svc().await;
        upload(&w, b"x").await;
        let one = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[], vec![put("f", b"x")]), 10).expect("spec"),
            )
            .await
            .expect("one");
        let two = w
            .create(REPO, &spec_to_changeset(spec(&[one.id], vec![]), 11).expect("spec"))
            .await
            .expect("two");
        let req = |ids: Vec<Bytes>| GetGitShasRequest {
            repo: REPO.into(),
            changeset_ids: ids,
        };
        // 第一次：两个都要计算；第二次：命中缓存，结果相同
        let ids = vec![id_bytes(&two.id), id_bytes(&one.id), id_bytes(&two.id)];
        let first = get_git_shas(store.as_ref(), store.as_ref(), req(ids.clone()))
            .await
            .expect("shas");
        assert_eq!(first.git_sha1.len(), 3);
        assert_eq!(first.git_sha1[0], first.git_sha1[2]);
        assert_ne!(first.git_sha1[0], first.git_sha1[1]);
        assert!(first.git_sha1.iter().all(|s| s.len() == 20));
        let again = get_git_shas(store.as_ref(), store.as_ref(), req(ids))
            .await
            .expect("cached");
        assert_eq!(again, first);
        let empty = get_git_shas(store.as_ref(), store.as_ref(), req(vec![])).await;
        assert_eq!(empty.expect("empty"), GetGitShasResponse::default());

        let code = |r: Result<GetGitShasResponse, Status>| r.map(|_| ()).map_err(|s| s.code());
        let too_many = vec![id_bytes(&one.id); MAX_GIT_SHAS + 1];
        assert_eq!(
            code(get_git_shas(store.as_ref(), store.as_ref(), req(too_many)).await),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(get_git_shas(store.as_ref(), store.as_ref(), req(vec![Bytes::from_static(b"x")])).await),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(get_git_shas(store.as_ref(), store.as_ref(), req(vec![id_bytes(&one.root_tree)])).await),
            Err(Code::InvalidArgument),
            "tree id is not a changeset id"
        );
        let stray = spec_to_changeset(spec(&[], vec![]), 99).expect("spec").id();
        assert_eq!(
            code(get_git_shas(store.as_ref(), store.as_ref(), req(vec![id_bytes(&stray)])).await),
            Err(Code::NotFound)
        );
        // 文件内容已清除、还没算过的提交
        upload(&w, b"gone").await;
        let three = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[two.id], vec![put("g", b"gone")]), 12).expect("spec"),
            )
            .await
            .expect("three");
        store.delete(&ObjectId::of_blob(b"gone")).await.expect("delete");
        assert_eq!(
            code(get_git_shas(store.as_ref(), store.as_ref(), req(vec![id_bytes(&three.id)])).await),
            Err(Code::FailedPrecondition)
        );
    }

    #[tokio::test]
    async fn blame_ranges_and_commits() {
        let (store, w) = svc().await;
        for c in [&b"a\nb\n"[..], b"a\nB\nc\n"] {
            upload(&w, c).await;
        }
        let mk = |p: &[ObjectId], d| spec_to_changeset(spec(p, d), 10).expect("spec");
        let one = w.create(REPO, &mk(&[], vec![put("f", b"a\nb\n")])).await.expect("one");
        let two = w
            .create(REPO, &mk(&[one.id], vec![put("f", b"a\nB\nc\n")]))
            .await
            .expect("two");
        let req = |path: &str| BlameRequest {
            repo: REPO.into(),
            revision: two.id.to_string(),
            path: path.into(),
        };
        let r = blame(store.as_ref(), store.as_ref(), req("f")).await.expect("blame");
        assert_eq!(r.commits.len(), 2);
        assert_eq!(ObjectId::from_bytes(&r.commits[0].changeset_id).ok(), Some(one.id));
        assert_eq!(r.commits[0].summary, "m");
        let ranges: Vec<_> = r
            .ranges
            .iter()
            .map(|x| (x.start_line, x.line_count, x.commit_index))
            .collect();
        assert_eq!(ranges, vec![(1, 1, 0), (2, 2, 1)]);
        let code = |r: Result<BlameResponse, Status>| r.map(|_| ()).map_err(|s| s.code());
        assert_eq!(
            code(blame(store.as_ref(), store.as_ref(), req("")).await),
            Err(Code::InvalidArgument)
        );
        assert_eq!(
            code(blame(store.as_ref(), store.as_ref(), req("nope")).await),
            Err(Code::NotFound)
        );
        assert_eq!(
            code(blame(store.as_ref(), store.as_ref(), req("/x")).await),
            Err(Code::InvalidArgument)
        );
    }
}
