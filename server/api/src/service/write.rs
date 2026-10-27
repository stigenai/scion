//! WriteService：上传文件内容、创建提交。

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use scion_land::ChangesetWriter;
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, ObjectKind, Signature};
use scion_proto::v1::write_service_server::WriteService;
use scion_proto::v1::{
    self, ChangesetSpec, CreateChangesetRequest, CreateChangesetResponse, UploadBlobsRequest, UploadBlobsResponse,
    file_change,
};
use scion_store::{BlobStore, ChangesetRecord, MetaStore};
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{authorize, authorize_any, principal, to_status};
use crate::config::ServerConfig;
use crate::limit::RateLimiter;
use crate::service::support::{check_repo, parse_id};
use crate::status;
use scion_auth::{Action, Guard, Request as AuthRequest};

pub struct WriteSvc {
    config: Arc<ServerConfig>,
    meta: Arc<dyn MetaStore>,
    writer: Arc<ChangesetWriter>,
    limiter: Option<Arc<RateLimiter>>,
    guard: Arc<Guard>,
}

impl WriteSvc {
    pub fn new(config: Arc<ServerConfig>, meta: Arc<dyn MetaStore>, blobs: Arc<dyn BlobStore>) -> Self {
        let writer = Arc::new(ChangesetWriter::new(meta.clone(), blobs.clone()));
        Self {
            config,
            meta,
            writer,
            limiter: None,
            guard: Guard::disabled(),
        }
    }

    pub fn with_guard(mut self, guard: Arc<Guard>) -> Self {
        self.guard = guard;
        self
    }

    /// 创建提交需要对每个改动路径有写权限（有整个仓库写权限时不再逐个检查）。
    async fn authorize_paths(&self, p: &scion_auth::Principal, repo: &str, cs: &Changeset) -> Result<(), Status> {
        let whole = AuthRequest::repo(repo, Action::Write);
        if self.guard.check(p, &whole).await.map_err(to_status)? {
            return Ok(());
        }
        authorize_any(&self.guard, p, repo, Action::Write).await?;
        for c in &cs.delta {
            authorize(
                &self.guard,
                p,
                AuthRequest::repo(repo, Action::Write).with_path(&c.path),
            )
            .await?;
        }
        Ok(())
    }

    /// Makes the edits in `spec` (replacements in files of its first parent) puts of the new
    /// contents, which it stores in the repository. Reading those files needs read access.
    async fn resolve_edits(
        &self,
        p: &scion_auth::Principal,
        repo: &str,
        spec: &mut ChangesetSpec,
    ) -> Result<(), Status> {
        if !spec
            .delta
            .iter()
            .any(|c| matches!(c.op, Some(file_change::Op::Edit(_))))
        {
            return Ok(());
        }
        let parent = spec
            .parents
            .first()
            .ok_or_else(|| Status::invalid_argument("an edit needs a parent commit to edit files of"))?;
        let parent = parse_id(parent)?;
        let root = self
            .meta
            .get_changeset(repo, &parent)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| Status::not_found(format!("changeset {parent} not found")))?
            .root_tree;
        let trees = scion_tree::BlobTreeSource(self.writer.blobs().clone());
        let mut new = Vec::new();
        for c in &mut spec.delta {
            let Some(file_change::Op::Edit(e)) = &c.op else {
                continue;
            };
            authorize(&self.guard, p, AuthRequest::repo(repo, Action::Read).with_path(&c.path)).await?;
            let entry = scion_tree::lookup(&trees, root, &c.path)
                .await
                .map_err(status::from_tree)?
                .filter(|e| matches!(e.mode, EntryMode::File | EntryMode::Executable))
                .ok_or_else(|| Status::failed_precondition(format!("{}: not a file in the parent commit", c.path)))?;
            if entry.size > MAX_EDITED {
                return Err(Status::failed_precondition(format!(
                    "{}: larger than {MAX_EDITED} bytes; upload the new content instead",
                    c.path
                )));
            }
            let data = self
                .writer
                .blobs()
                .get(&entry.id)
                .await
                .map_err(status::from_store)?
                .ok_or_else(|| Status::failed_precondition(format!("{}: its content is missing", c.path)))?;
            let edited = Bytes::from(edit_text(&data, &e.replacements, &c.path)?);
            let id = ObjectId::of_blob(&edited);
            c.op = Some(file_change::Op::Put(v1::PutFile {
                mode: crate::service::query::proto_mode(entry.mode).into(),
                id: Bytes::copy_from_slice(&id.to_bytes()),
                size: edited.len() as u64,
            }));
            new.push((id, edited));
        }
        self.writer.store(repo, &new).await.map_err(status::from_land)
    }

    /// 按仓库限制写入速率（上传与创建提交各算一次）。
    pub fn with_rate_limit(mut self, limiter: Arc<RateLimiter>) -> Self {
        self.limiter = Some(limiter);
        self
    }

    pub(crate) fn writer(&self) -> &Arc<ChangesetWriter> {
        &self.writer
    }

    /// 写入对象字节并登记到仓库索引（先字节、后索引）。
    async fn store(&self, repo: &str, objects: &[(ObjectId, Bytes)]) -> Result<(), Status> {
        self.writer.store(repo, objects).await.map_err(status::from_land)
    }

    /// Stores a batch of uploaded objects and adds their ids to `stored`.
    async fn flush_uploads(
        &self,
        repo: &str,
        batch: &mut Vec<(ObjectId, Bytes)>,
        stored: &mut Vec<Bytes>,
    ) -> Result<(), Status> {
        if batch.is_empty() {
            return Ok(());
        }
        self.store(repo, batch).await?;
        stored.extend(batch.drain(..).map(|(id, _)| Bytes::copy_from_slice(&id.to_bytes())));
        Ok(())
    }

    /// Stores objects for `p`, who needs to be able to write somewhere in the repository.
    pub(crate) async fn store_as(
        &self,
        p: &scion_auth::Principal,
        repo: &str,
        objects: &[(ObjectId, Bytes)],
    ) -> Result<(), Status> {
        authorize_any(&self.guard, p, repo, Action::Write).await?;
        check_repo(self.meta.as_ref(), repo).await?;
        RateLimiter::admit(self.limiter.as_deref(), repo)?;
        self.store(repo, objects).await
    }

    /// Creates a commit the git gateway built for `p`: the paths are checked as for
    /// CreateChangeset, but the gateway may set `git.sha1` and `git.commit`, which it checked.
    pub(crate) async fn create_as(
        &self,
        p: &scion_auth::Principal,
        repo: &str,
        cs: &Changeset,
    ) -> Result<ChangesetRecord, Status> {
        self.authorize_paths(p, repo, cs).await?;
        RateLimiter::admit(self.limiter.as_deref(), repo)?;
        self.create(repo, cs).await
    }

    /// 校验并创建提交：父提交必须已存在，引用的文件必须已上传，delta 必须能应用到 first parent 上。
    pub(crate) async fn create(&self, repo: &str, cs: &Changeset) -> Result<ChangesetRecord, Status> {
        check_repo(self.meta.as_ref(), repo).await?;
        self.writer.create(repo, cs).await.map_err(status::from_land)
    }
}

/// An upload stream is written to storage after this many bytes or objects (or at its end).
const UPLOAD_BATCH_BYTES: usize = 16 << 20;
const UPLOAD_BATCH_OBJECTS: usize = 4096;

/// 拼接客户端流中的对象分块，并在对象完整时校验 BLAKE3。
pub(crate) struct UploadAssembler {
    max_object_bytes: u64,
    repo: Option<String>,
    current: Option<(ObjectId, BytesMut)>,
}

impl UploadAssembler {
    pub(crate) fn new(max_object_bytes: u64) -> Self {
        Self {
            max_object_bytes,
            repo: None,
            current: None,
        }
    }

    pub(crate) fn repo(&self) -> Option<&str> {
        self.repo.as_deref()
    }

    /// 处理一条消息；对象完整时返回 `(id, 字节)`。
    pub(crate) fn push(&mut self, msg: UploadBlobsRequest) -> Result<Option<(ObjectId, Bytes)>, Status> {
        match (&self.repo, msg.repo.is_empty()) {
            (None, true) => return Err(Status::invalid_argument("the first UploadBlobs message must set repo")),
            (None, false) => self.repo = Some(msg.repo.clone()),
            (Some(r), false) if *r != msg.repo => {
                return Err(Status::invalid_argument(
                    "all UploadBlobs messages must target the same repo",
                ));
            }
            _ => {}
        }
        let id = parse_id(&msg.id)?;
        if id.kind() != ObjectKind::Blob {
            return Err(Status::invalid_argument(format!(
                "only blobs can be uploaded, got {id}"
            )));
        }
        let buf = match &mut self.current {
            None if msg.offset == 0 => &mut self.current.insert((id, BytesMut::new())).1,
            None => {
                return Err(Status::invalid_argument(format!(
                    "first chunk of {id} must start at offset 0"
                )));
            }
            Some((cur, buf)) if *cur == id && msg.offset == buf.len() as u64 => buf,
            Some((cur, buf)) => {
                return Err(Status::invalid_argument(format!(
                    "expected chunk of {cur} at offset {}, got {id} at {}",
                    buf.len(),
                    msg.offset
                )));
            }
        };
        let new_len = buf.len() as u64 + msg.data.len() as u64;
        if new_len > self.max_object_bytes {
            return Err(status::limit_exceeded(
                "max_upload_object_bytes",
                usize::try_from(new_len).unwrap_or(usize::MAX),
                usize::try_from(self.max_object_bytes).unwrap_or(usize::MAX),
            ));
        }
        buf.extend_from_slice(&msg.data);
        if !msg.last {
            return Ok(None);
        }
        let (id, buf) = self
            .current
            .take()
            .unwrap_or_else(|| unreachable!("current was set above"));
        let bytes = buf.freeze();
        if !id.verify(&bytes) {
            return Err(Status::invalid_argument(format!(
                "uploaded content does not match {id}"
            )));
        }
        Ok(Some((id, bytes)))
    }

    /// 流结束：不允许停在对象中途。
    pub(crate) fn finish(self) -> Result<(), Status> {
        match self.current {
            None => Ok(()),
            Some((id, _)) => Err(Status::invalid_argument(format!(
                "stream ended before the last chunk of {id}"
            ))),
        }
    }
}

/// Largest file an edit can change on the server.
const MAX_EDITED: u64 = 64 << 20;

/// `data` with `replacements` applied in order; each must match exactly once unless `all`.
fn edit_text(data: &[u8], replacements: &[v1::TextReplacement], path: &str) -> Result<Vec<u8>, Status> {
    let mut out = data.to_vec();
    for (i, r) in replacements.iter().enumerate() {
        let n = i + 1;
        if r.old.is_empty() {
            return Err(Status::invalid_argument(format!(
                "{path}: replacement {n} has nothing to replace"
            )));
        }
        let at: Vec<usize> = memchr::memmem::find_iter(&out, &r.old[..]).collect();
        // Overlapping matches count once, as the replacement goes left to right
        let mut starts = Vec::new();
        for a in at {
            if starts.last().is_none_or(|l: &usize| a >= l + r.old.len()) {
                starts.push(a);
            }
        }
        match (starts.len(), r.all) {
            (0, _) => {
                return Err(Status::failed_precondition(format!(
                    "{path}: replacement {n}: the text to replace is not in the file"
                )));
            }
            (1, _) | (_, true) => {}
            (k, false) => {
                return Err(Status::failed_precondition(format!(
                    "{path}: replacement {n}: the text to replace appears {k} times; include more around it, or replace all"
                )));
            }
        }
        let mut next = Vec::with_capacity(out.len());
        let mut from = 0;
        for s in starts {
            next.extend_from_slice(&out[from..s]);
            next.extend_from_slice(&r.new);
            from = s + r.old.len();
        }
        next.extend_from_slice(&out[from..]);
        out = next;
    }
    Ok(out)
}

fn entry_mode(m: i32) -> Result<EntryMode, Status> {
    match v1::EntryMode::try_from(m) {
        Ok(v1::EntryMode::File) => Ok(EntryMode::File),
        Ok(v1::EntryMode::Executable) => Ok(EntryMode::Executable),
        Ok(v1::EntryMode::Symlink) => Ok(EntryMode::Symlink),
        _ => Err(Status::invalid_argument(format!(
            "invalid file mode {m}; expected FILE, EXECUTABLE or SYMLINK"
        ))),
    }
}

fn signature(s: Option<v1::Signature>, which: &str) -> Result<Signature, Status> {
    let s = s.ok_or_else(|| Status::invalid_argument(format!("{which} is required")))?;
    Ok(Signature {
        name: s.name,
        email: s.email,
        time_ms: s.time_ms,
        tz_offset_minutes: s.tz_offset_minutes,
    })
}

/// 把请求中的结构化提交转换并校验为 [`Changeset`]（delta 自动按路径排序）。
pub(crate) fn spec_to_changeset(spec: ChangesetSpec, max_changes: u32) -> Result<Changeset, Status> {
    if spec.delta.len() > max_changes as usize {
        return Err(status::limit_exceeded(
            "max_changes_per_changeset",
            spec.delta.len(),
            max_changes as usize,
        ));
    }
    let parents = spec
        .parents
        .iter()
        .map(|p| parse_id(p))
        .collect::<Result<Vec<_>, _>>()?;
    let change_id: [u8; 16] =
        spec.change_id.as_ref().try_into().map_err(|_| {
            Status::invalid_argument(format!("change_id must be 16 bytes, got {}", spec.change_id.len()))
        })?;
    let mut delta = Vec::with_capacity(spec.delta.len());
    for c in spec.delta {
        delta.push(match c.op {
            Some(file_change::Op::Put(p)) => FileChange::put(c.path, entry_mode(p.mode)?, parse_id(&p.id)?, p.size),
            Some(file_change::Op::Delete(_)) => FileChange::delete(c.path),
            Some(file_change::Op::Edit(_)) => {
                return Err(status::invalid_changeset(&c.path, "an edit was not applied"));
            }
            None => return Err(status::invalid_changeset(&c.path, "file change has no operation")),
        });
    }
    let extensions: BTreeMap<String, Vec<u8>> = spec.extensions.into_iter().map(|(k, v)| (k, v.to_vec())).collect();
    Changeset::new(
        parents,
        signature(spec.author, "author")?,
        signature(spec.committer, "committer")?,
        spec.message,
        change_id,
        delta,
        extensions,
    )
    .map_err(|e| status::invalid_changeset("", e.to_string()))
}

#[tonic::async_trait]
impl WriteService for WriteSvc {
    async fn upload_blobs(
        &self,
        request: Request<Streaming<UploadBlobsRequest>>,
    ) -> Result<Response<UploadBlobsResponse>, Status> {
        let p = principal(&request);
        let mut stream = request.into_inner();
        let mut asm = UploadAssembler::new(self.config.limits.max_upload_object_bytes);
        let mut checked = false;
        let mut stored = Vec::new();
        // Objects are written in batches, so that a store with packs keeps them together
        let mut batch: Vec<(ObjectId, Bytes)> = Vec::new();
        let mut batch_bytes = 0;
        let mut repo = String::new();
        while let Some(msg) = stream.message().await? {
            let done = asm.push(msg)?;
            if !checked {
                repo = asm.repo().unwrap_or_default().to_owned();
                authorize_any(&self.guard, &p, &repo, Action::Write).await?;
                check_repo(self.meta.as_ref(), &repo).await?;
                RateLimiter::admit(self.limiter.as_deref(), &repo)?;
                checked = true;
            }
            if let Some((id, bytes)) = done {
                batch_bytes += bytes.len();
                batch.push((id, bytes));
                if batch_bytes >= UPLOAD_BATCH_BYTES || batch.len() >= UPLOAD_BATCH_OBJECTS {
                    self.flush_uploads(&repo, &mut batch, &mut stored).await?;
                    batch_bytes = 0;
                }
            }
        }
        asm.finish()?;
        self.flush_uploads(&repo, &mut batch, &mut stored).await?;
        Ok(Response::new(UploadBlobsResponse { stored_ids: stored }))
    }

    async fn create_changeset(
        &self,
        request: Request<CreateChangesetRequest>,
    ) -> Result<Response<CreateChangesetResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        let mut spec = req
            .changeset
            .ok_or_else(|| Status::invalid_argument("changeset is required"))?;
        self.resolve_edits(&p, &req.repo, &mut spec).await?;
        let cs = spec_to_changeset(spec, self.config.limits.max_changes_per_changeset)?;
        self.authorize_paths(&p, &req.repo, &cs).await?;
        // git sha 会被当作这个提交的身份（按 sha 解析版本、事件中的 git_sha），只有导入者可以声明：
        // 否则任何写者都能让任意 sha 指向自己的提交，并抢在真正的导入之前占住它
        // The raw git object decides what git clients see for the commit: the same rule
        let reserved = [
            scion_query::GIT_SHA1_EXTENSION,
            scion_git::objects::GIT_COMMIT_EXTENSION,
        ]
        .into_iter()
        .find(|e| cs.extensions.contains_key(*e));
        if let Some(ext) = reserved {
            authorize(&self.guard, &p, AuthRequest::repo(&req.repo, Action::Admin))
                .await
                .map_err(|s| {
                    Status::permission_denied(format!(
                        "the {ext} extension is reserved for git imports and needs admin on the repo: {}",
                        s.message()
                    ))
                })?;
        }
        RateLimiter::admit(self.limiter.as_deref(), &req.repo)?;
        let rec = self.create(&req.repo, &cs).await?;
        tracing::info!(repo = %req.repo, changeset = %rec.id, request_id = %req.request_id, "changeset created");
        Ok(Response::new(CreateChangesetResponse {
            changeset_id: Bytes::copy_from_slice(&rec.id.to_bytes()),
            root_tree: Bytes::copy_from_slice(&rec.root_tree.to_bytes()),
            generation: rec.generation,
        }))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn edits_replace_text_once_or_everywhere() {
        let r = |old: &str, new: &str, all: bool| v1::TextReplacement {
            old: Bytes::copy_from_slice(old.as_bytes()),
            new: Bytes::copy_from_slice(new.as_bytes()),
            all,
        };
        let edit = |data: &str, reps: &[v1::TextReplacement]| {
            edit_text(data.as_bytes(), reps, "f").map(|v| String::from_utf8(v).expect("utf8"))
        };
        assert_eq!(edit("a b c", &[r("b", "B", false)]).expect("once"), "a B c");
        assert_eq!(edit("x x x", &[r("x", "y", true)]).expect("all"), "y y y");
        assert_eq!(edit("aaa", &[r("aa", "b", true)]).expect("overlapping"), "ba");
        assert_eq!(
            edit("one two", &[r("one", "1", false), r("1 two", "12", false)]).expect("in order"),
            "12"
        );
        let code = |res: Result<String, Status>| res.expect_err("refused").code();
        assert_eq!(code(edit("abc", &[r("z", "y", false)])), Code::FailedPrecondition);
        assert_eq!(code(edit("abc", &[r("z", "y", true)])), Code::FailedPrecondition);
        let twice = edit("x x", &[r("x", "y", false)]).expect_err("ambiguous");
        assert!(twice.message().contains("appears 2 times"), "{twice:?}");
        assert_eq!(code(edit("abc", &[r("", "y", false)])), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn git_sha_extension_needs_admin() {
        use scion_auth::{GuardConfig, Principal};
        let store = Arc::new(MemStore::new());
        store.create_repo(REPO).await.expect("repo");
        let guard = Guard::build(
            GuardConfig {
                required: true,
                ..GuardConfig::default()
            },
            store.clone(),
            Arc::new(|_: &str| None),
        )
        .expect("guard");
        guard
            .set_policy(
                r#"
                [[rule]]
                subjects = ["user:alice"]
                repos = ["r"]
                actions = ["write"]

                [[rule]]
                subjects = ["user:importer"]
                repos = ["r"]
                actions = ["write", "admin"]
                "#,
            )
            .await
            .expect("policy");
        let w = WriteSvc::new(Arc::new(ServerConfig::default()), store.clone(), store).with_guard(guard);
        let call = |who: &str, git_sha: bool, n: u8| {
            let mut sp = spec(&[], vec![]);
            sp.change_id = Bytes::copy_from_slice(&[n; 16]);
            if git_sha {
                sp.extensions
                    .insert(scion_query::GIT_SHA1_EXTENSION.into(), Bytes::from_static(&[0xab; 20]));
            }
            let mut r = Request::new(CreateChangesetRequest {
                repo: REPO.into(),
                request_id: String::new(),
                changeset: Some(sp),
            });
            r.extensions_mut().insert(Principal::user(who, vec![]));
            r
        };
        // 普通写者：可以创建提交，但不能声明 git sha
        assert!(w.create_changeset(call("alice", false, 1)).await.is_ok());
        let err = w.create_changeset(call("alice", true, 2)).await.expect_err("reserved");
        assert_eq!(err.code(), Code::PermissionDenied);
        assert!(err.message().contains("git.sha1"), "{err:?}");
        // 导入者（仓库 admin）可以
        assert!(w.create_changeset(call("importer", true, 3)).await.is_ok());
        // The raw git object is reserved the same way
        let raw = |who: &str, n: u8| {
            let mut r = call(who, false, n);
            let sp = r.get_mut().changeset.as_mut().expect("spec");
            sp.extensions.insert(
                scion_git::objects::GIT_COMMIT_EXTENSION.into(),
                Bytes::from_static(b"tree x"),
            );
            r
        };
        let err = w.create_changeset(raw("alice", 4)).await.expect_err("reserved");
        assert!(err.message().contains("git.commit"), "{err:?}");
        assert!(w.create_changeset(raw("importer", 5)).await.is_ok());
    }
    use scion_store::MemStore;
    use tonic::Code;

    pub(crate) const REPO: &str = "r";

    pub(crate) async fn svc() -> (Arc<MemStore>, WriteSvc) {
        let store = Arc::new(MemStore::new());
        store.create_repo(REPO).await.expect("repo");
        let w = WriteSvc::new(Arc::new(ServerConfig::default()), store.clone(), store.clone());
        (store, w)
    }

    pub(crate) fn sig() -> v1::Signature {
        v1::Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 1,
            tz_offset_minutes: 480,
        }
    }

    pub(crate) fn put(path: &str, content: &[u8]) -> v1::FileChange {
        v1::FileChange {
            path: path.into(),
            op: Some(file_change::Op::Put(v1::PutFile {
                mode: v1::EntryMode::File.into(),
                id: Bytes::copy_from_slice(&ObjectId::of_blob(content).to_bytes()),
                size: content.len() as u64,
            })),
        }
    }

    fn del(path: &str) -> v1::FileChange {
        v1::FileChange {
            path: path.into(),
            op: Some(file_change::Op::Delete(v1::DeleteFile {})),
        }
    }

    pub(crate) fn spec(parents: &[ObjectId], delta: Vec<v1::FileChange>) -> ChangesetSpec {
        ChangesetSpec {
            parents: parents.iter().map(|p| Bytes::copy_from_slice(&p.to_bytes())).collect(),
            author: Some(sig()),
            committer: Some(sig()),
            message: "m".into(),
            change_id: Bytes::from_static(&[7; 16]),
            delta,
            extensions: Default::default(),
        }
    }

    /// 上传文件内容（直接写存储，模拟 UploadBlobs 已完成）。
    pub(crate) async fn upload(w: &WriteSvc, content: &[u8]) {
        let id = ObjectId::of_blob(content);
        w.store(REPO, &[(id, Bytes::copy_from_slice(content))])
            .await
            .expect("store");
    }

    fn msg(repo: &str, data: &[u8], id_of: &[u8], offset: u64, last: bool) -> UploadBlobsRequest {
        UploadBlobsRequest {
            repo: repo.into(),
            id: Bytes::copy_from_slice(&ObjectId::of_blob(id_of).to_bytes()),
            offset,
            data: Bytes::copy_from_slice(data),
            last,
        }
    }

    #[test]
    fn assembler_single_and_multi_chunk() {
        let mut a = UploadAssembler::new(100);
        let (id, b) = a.push(msg("r", b"hi", b"hi", 0, true)).expect("ok").expect("done");
        assert_eq!((id, b.as_ref()), (ObjectId::of_blob(b"hi"), &b"hi"[..]));
        assert_eq!(a.repo(), Some("r"));
        assert!(a.push(msg("", b"hel", b"hello", 0, false)).expect("ok").is_none());
        let (_, b) = a.push(msg("r", b"lo", b"hello", 3, true)).expect("ok").expect("done");
        assert_eq!(b.as_ref(), b"hello");
        a.finish().expect("clean end");
    }

    #[test]
    fn assembler_rejects_protocol_violations() {
        let bad = |a: &mut UploadAssembler, m| a.push(m).map(|_| ()).map_err(|s| s.code());
        let mut a = UploadAssembler::new(100);
        assert_eq!(
            bad(&mut a, msg("", b"x", b"x", 0, true)),
            Err(Code::InvalidArgument),
            "repo missing"
        );
        let mut a = UploadAssembler::new(100);
        a.push(msg("r", b"x", b"x", 0, true)).expect("ok");
        assert_eq!(
            bad(&mut a, msg("other", b"y", b"y", 0, true)),
            Err(Code::InvalidArgument),
            "repo changed"
        );
        let mut a = UploadAssembler::new(100);
        assert_eq!(
            bad(&mut a, msg("r", b"x", b"x", 5, true)),
            Err(Code::InvalidArgument),
            "nonzero start"
        );
        let mut a = UploadAssembler::new(100);
        a.push(msg("r", b"he", b"hello", 0, false)).expect("ok");
        assert_eq!(
            bad(&mut a, msg("r", b"llo", b"hello", 3, true)),
            Err(Code::InvalidArgument),
            "gap"
        );
        let mut a = UploadAssembler::new(100);
        a.push(msg("r", b"he", b"hello", 0, false)).expect("ok");
        assert_eq!(
            bad(&mut a, msg("r", b"x", b"x", 2, true)),
            Err(Code::InvalidArgument),
            "interleaved"
        );
        let mut a = UploadAssembler::new(100);
        assert_eq!(
            bad(&mut a, msg("r", b"evil", b"good", 0, true)),
            Err(Code::InvalidArgument),
            "hash mismatch"
        );
        let mut a = UploadAssembler::new(3);
        assert_eq!(
            bad(&mut a, msg("r", b"toolong", b"toolong", 0, true)),
            Err(Code::InvalidArgument),
            "too big"
        );
        let mut a = UploadAssembler::new(100);
        let tree = UploadBlobsRequest {
            repo: "r".into(),
            id: Bytes::copy_from_slice(&ObjectId::hash(ObjectKind::Tree, b"").to_bytes()),
            offset: 0,
            data: Bytes::new(),
            last: true,
        };
        assert_eq!(bad(&mut a, tree), Err(Code::InvalidArgument), "non-blob");
        let mut a = UploadAssembler::new(100);
        a.push(msg("r", b"he", b"hello", 0, false)).expect("ok");
        assert_eq!(
            a.finish().map_err(|s| s.code()),
            Err(Code::InvalidArgument),
            "truncated stream"
        );
    }

    #[test]
    fn spec_to_changeset_converts_and_sorts() {
        let p = ObjectId::hash(ObjectKind::Changeset, b"p");
        let mut s = spec(&[p], vec![put("z", b"z"), del("a")]);
        s.extensions.insert("k".into(), Bytes::from_static(b"v"));
        let cs = spec_to_changeset(s, 10).expect("valid");
        assert_eq!(cs.parents, vec![p]);
        assert_eq!(cs.delta.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(), ["a", "z"]);
        assert_eq!(cs.extensions.get("k").map(Vec::as_slice), Some(&b"v"[..]));
        assert_eq!(cs.author.tz_offset_minutes, 480);
    }

    #[test]
    fn spec_to_changeset_rejects_bad_input() {
        let code = |s| spec_to_changeset(s, 2).map(|_| ()).map_err(|e: Status| e.code());
        assert_eq!(
            code(spec(&[], vec![put("a", b""), put("b", b""), put("c", b"")])),
            Err(Code::InvalidArgument)
        );
        let mut s = spec(&[], vec![]);
        s.change_id = Bytes::from_static(b"short");
        assert_eq!(code(s), Err(Code::InvalidArgument));
        let mut s = spec(&[], vec![]);
        s.author = None;
        assert_eq!(code(s), Err(Code::InvalidArgument));
        let mut s = spec(&[], vec![]);
        s.committer = None;
        assert_eq!(code(s), Err(Code::InvalidArgument));
        let mut s = spec(&[], vec![]);
        s.parents = vec![Bytes::from_static(b"bad")];
        assert_eq!(code(s), Err(Code::InvalidArgument));
        let s = spec(
            &[],
            vec![v1::FileChange {
                path: "a".into(),
                op: None,
            }],
        );
        assert_eq!(code(s), Err(Code::InvalidArgument));
        let mut tree_put = put("a", b"");
        if let Some(file_change::Op::Put(p)) = &mut tree_put.op {
            p.mode = v1::EntryMode::Tree.into();
        }
        assert_eq!(code(spec(&[], vec![tree_put])), Err(Code::InvalidArgument));
        assert_eq!(code(spec(&[], vec![put("/abs", b"")])), Err(Code::InvalidArgument));
        assert_eq!(
            code(spec(&[], vec![put("a", b""), del("a")])),
            Err(Code::InvalidArgument),
            "duplicate path"
        );
    }

    #[test]
    fn entry_mode_all() {
        assert_eq!(
            entry_mode(v1::EntryMode::Executable.into()).ok(),
            Some(EntryMode::Executable)
        );
        assert_eq!(entry_mode(v1::EntryMode::Symlink.into()).ok(), Some(EntryMode::Symlink));
        assert!(entry_mode(99).is_err());
        assert!(entry_mode(0).is_err());
    }

    #[tokio::test]
    async fn create_root_then_child() {
        let (store, w) = svc().await;
        upload(&w, b"hello").await;
        let root = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[], vec![put("a/b.txt", b"hello")]), 10).expect("spec"),
            )
            .await
            .expect("root");
        assert_eq!(root.generation, 1);
        assert_eq!(root.first_parent(), None);
        // 提交对象和新目录树都已登记，可被 GetObjects 读到
        assert_eq!(
            store.missing_objects(REPO, &[root.id, root.root_tree]).await,
            Ok(vec![])
        );
        let child = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[root.id], vec![del("a/b.txt")]), 10).expect("spec"),
            )
            .await
            .expect("child");
        assert_eq!(child.generation, 2);
        assert_eq!(child.first_parent(), Some(root.id));
        assert_eq!((root.fp_depth, child.fp_depth), (1, 2));
        assert_eq!(child.fp_jump, Some(root.id));
        assert_eq!(child.root_tree, scion_object::Tree::default().id());
        assert_eq!(store.get_changeset(REPO, &child.id).await, Ok(Some(child.clone())));
        // 路径索引已写入
        let hist = store.path_history(REPO, "a", 10, 10).await.expect("history");
        assert_eq!(
            hist.iter().map(|e| (e.cs, e.deleted)).collect::<Vec<_>>(),
            vec![(child.id, true), (root.id, false)]
        );
        // 幂等：同样的内容再次创建得到同样的结果
        let again = w
            .create(
                REPO,
                &spec_to_changeset(spec(&[root.id], vec![del("a/b.txt")]), 10).expect("spec"),
            )
            .await
            .expect("again");
        assert_eq!(again, child);
    }

    #[tokio::test]
    async fn create_errors() {
        let (_store, w) = svc().await;
        let code = |r: Result<ChangesetRecord, Status>| r.map_err(|s| s.code()).err();
        let cs = |p: &[ObjectId], d| spec_to_changeset(spec(p, d), 10).expect("spec");
        // 文件未上传
        assert_eq!(
            code(w.create(REPO, &cs(&[], vec![put("a", b"never uploaded")])).await),
            Some(Code::FailedPrecondition)
        );
        // 父提交不存在
        let ghost = ObjectId::hash(ObjectKind::Changeset, b"ghost");
        assert_eq!(
            code(w.create(REPO, &cs(&[ghost], vec![])).await),
            Some(Code::FailedPrecondition)
        );
        // 声明的大小与实际不符
        upload(&w, b"four").await;
        let mut lie = put("a", b"four");
        if let Some(file_change::Op::Put(p)) = &mut lie.op {
            p.size = 999;
        }
        assert_eq!(
            code(
                w.create(REPO, &spec_to_changeset(spec(&[], vec![lie]), 10).expect("spec"))
                    .await
            ),
            Some(Code::InvalidArgument)
        );
        // delta 无法应用
        assert_eq!(
            code(w.create(REPO, &cs(&[], vec![del("missing")])).await),
            Some(Code::InvalidArgument)
        );
        // 仓库不存在
        assert_eq!(code(w.create("nope", &cs(&[], vec![])).await), Some(Code::NotFound));
    }

    #[tokio::test]
    async fn create_merge_generation_is_max_plus_one() {
        let (_store, w) = svc().await;
        let cs = |p: &[ObjectId], d| spec_to_changeset(spec(p, d), 10).expect("spec");
        let a = w.create(REPO, &cs(&[], vec![])).await.expect("a");
        let b = w.create(REPO, &cs(&[a.id], vec![])).await.expect("b");
        let mut other = spec(&[], vec![]);
        other.message = "other root".into();
        let c = w
            .create(REPO, &spec_to_changeset(other, 10).expect("spec"))
            .await
            .expect("c");
        let m = w.create(REPO, &cs(&[c.id, b.id], vec![])).await.expect("merge");
        assert_eq!(m.generation, 3);
        assert_eq!(m.first_parent(), Some(c.id));
        assert_eq!(m.parents, vec![c.id, b.id]);
    }
}
