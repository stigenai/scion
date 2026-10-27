//! 写入、合入与查询。

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use scion_object::{EntryMode, ObjectId};
use scion_proto::v1::{
    self, ChangesetSpec, CreateChangesetRequest, GetLandStatusRequest, LandRequest, LandResponse, LandState,
    ListDirRequest, ListRefLogRequest, ListRefsRequest, ReadFileRequest, ResolveRefRequest, UploadBlobsRequest,
    file_change, land_service_client::LandServiceClient, query_service_client::QueryServiceClient,
    ref_service_client::RefServiceClient, write_service_client::WriteServiceClient,
};

use crate::client::Client;
use crate::error::{Error, Result};

/// One move of a ref (`scn reflog`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefLogEntry {
    /// The ref's version after the move (1: created).
    pub version: u64,
    pub changeset: ObjectId,
    pub updated_at_ms: i64,
    pub how: v1::RefMove,
    /// Who moved it, comma-separated when several requests landed together; empty when the
    /// server did not record it.
    pub actor: String,
}

/// ref 当前状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefTip {
    pub changeset: ObjectId,
    pub root_tree: ObjectId,
    pub version: u64,
}

/// 创建提交的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedChangeset {
    pub id: ObjectId,
    pub root_tree: ObjectId,
    pub generation: u64,
}

/// 合入结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Landed {
    pub tip: ObjectId,
    pub version: u64,
    /// 本次合入的提交（从旧到新）；为空表示提交早已在 ref 的历史中。
    pub landed: Vec<LandedChangeset>,
    /// 早已合入过这个 ref 的 change（同一 ref 上每个 change 只合入一次），没有重复合入。
    pub previously_landed: Vec<LandedChangeset>,
}

impl Landed {
    /// 是否经过服务端 rebase（进入历史的提交与提交的不同）。
    pub fn rebased(&self) -> bool {
        self.landed.iter().any(|l| l.changeset != l.landed)
    }
}

/// 合入的一个提交：`changeset` 是提交的，`landed` 是进入 ref 历史的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LandedChangeset {
    pub changeset: ObjectId,
    pub landed: ObjectId,
    pub change_id: [u8; 16],
}

/// 目录条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub mode: EntryMode,
    pub id: ObjectId,
    pub size: u64,
}

/// 提交作者 / 提交者。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub name: String,
    pub email: String,
}

/// 带时间的签名（导入外部历史时用来保留原始时间与时区）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub name: String,
    pub email: String,
    /// Unix 毫秒。
    pub time_ms: i64,
    /// 相对 UTC 的分钟数。
    pub tz_offset_minutes: i32,
}

impl Signature {
    pub(crate) fn to_proto(&self) -> v1::Signature {
        v1::Signature {
            name: self.name.clone(),
            email: self.email.clone(),
            time_ms: self.time_ms,
            tz_offset_minutes: self.tz_offset_minutes,
        }
    }
}

fn bytes_of(id: &ObjectId) -> Bytes {
    Bytes::copy_from_slice(&id.to_bytes())
}

pub(crate) fn id_from(raw: &[u8], what: &str) -> Result<ObjectId> {
    ObjectId::from_bytes(raw).map_err(|e| Error::Protocol(format!("bad {what} in response: {e}")))
}

fn proto_mode(m: EntryMode) -> v1::EntryMode {
    match m {
        EntryMode::File => v1::EntryMode::File,
        EntryMode::Executable => v1::EntryMode::Executable,
        EntryMode::Symlink => v1::EntryMode::Symlink,
        EntryMode::Tree => v1::EntryMode::Tree,
    }
}

pub(crate) fn mode_from(m: i32) -> Result<EntryMode> {
    match v1::EntryMode::try_from(m) {
        Ok(v1::EntryMode::File) => Ok(EntryMode::File),
        Ok(v1::EntryMode::Executable) => Ok(EntryMode::Executable),
        Ok(v1::EntryMode::Symlink) => Ok(EntryMode::Symlink),
        Ok(v1::EntryMode::Tree) => Ok(EntryMode::Tree),
        _ => Err(Error::Protocol(format!("unknown entry mode {m}"))),
    }
}

/// 一次合入在可重试错误下的最多尝试次数（同一个 request id）。
pub const LAND_ATTEMPTS: u32 = 3;

fn landed_from(resp: &LandResponse) -> Result<Landed> {
    let list = |v: &[v1::LandedChangeset]| -> Result<Vec<LandedChangeset>> {
        v.iter()
            .map(|l| {
                Ok(LandedChangeset {
                    changeset: id_from(&l.changeset_id, "changeset id")?,
                    landed: id_from(&l.landed_id, "landed id")?,
                    change_id: l.change_id.as_ref().try_into().map_err(|_| {
                        Error::Protocol(format!("change id must be 16 bytes, got {}", l.change_id.len()))
                    })?,
                })
            })
            .collect()
    };
    Ok(Landed {
        tip: id_from(&resp.tip, "tip")?,
        version: resp.ref_version,
        landed: list(&resp.landed)?,
        previously_landed: list(&resp.previously_landed)?,
    })
}

fn request_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// 把对象切成上传消息：第一条带 repo，按 `chunk` 分块，最后一块 last=true。
fn upload_messages(repo: &str, id: &ObjectId, data: &Bytes, chunk: usize) -> Vec<UploadBlobsRequest> {
    let chunk = chunk.max(1);
    let mut out = Vec::with_capacity(data.len().div_ceil(chunk).max(1));
    let mut off = 0;
    loop {
        let end = (off + chunk).min(data.len());
        out.push(UploadBlobsRequest {
            repo: repo.to_owned(),
            id: bytes_of(id),
            offset: off as u64,
            data: data.slice(off..end),
            last: end == data.len(),
        });
        if end == data.len() {
            return out;
        }
        off = end;
    }
}

impl Client {
    /// 上传文件内容，只传服务端还没有的。返回各内容的 Blob id（与输入顺序一致）。
    pub async fn upload_blobs(&self, contents: &[Bytes]) -> Result<Vec<ObjectId>> {
        let repo = self.repo()?;
        let ids: Vec<ObjectId> = contents.iter().map(|c| ObjectId::of_blob(c)).collect();
        let missing = self.missing_objects(&ids).await?;
        if missing.is_empty() {
            return Ok(ids);
        }
        let max = self.server_info().limits.max_upload_object_bytes;
        let chunk = usize::try_from(self.server_info().limits.max_chunk_bytes).unwrap_or(usize::MAX);
        let mut msgs = Vec::new();
        let mut queued = std::collections::HashSet::new();
        for (id, data) in ids.iter().zip(contents) {
            if !missing.contains(id) || !queued.insert(*id) {
                continue;
            }
            if max > 0 && data.len() as u64 > max {
                return Err(Error::Protocol(format!(
                    "{id} is {} bytes, above the server limit of {max} bytes",
                    data.len()
                )));
            }
            msgs.extend(upload_messages(&repo, id, data, chunk));
        }
        let mut svc = WriteServiceClient::new(self.channel.clone());
        svc.upload_blobs(self.request(tokio_stream::iter(msgs))?).await?;
        Ok(ids)
    }

    /// 以 `parent`（`None` 表示根提交）为父提交开始构造一个提交。
    pub fn changeset(&self, parent: Option<ObjectId>) -> ChangesetBuilder<'_> {
        ChangesetBuilder {
            client: self,
            parents: parent.into_iter().collect(),
            author: None,
            committer: None,
            change_id: None,
            message: String::new(),
            puts: Vec::new(),
            deletes: Vec::new(),
            edits: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }

    /// 合入：把 `reference` 移到 `changeset`。落后时由服务端 rebase，冲突时返回 MERGE_CONFLICT
    /// （详情见 [`crate::Error::merge_conflict`]）。
    pub async fn land(&self, reference: &str, changeset: &ObjectId) -> Result<Landed> {
        self.land_with_id(reference, changeset, &request_id(), false).await
    }

    /// 只允许快进的合入：落后时返回 NOT_FAST_FORWARD。
    pub async fn land_fast_forward(&self, reference: &str, changeset: &ObjectId) -> Result<Landed> {
        self.land_with_id(reference, changeset, &request_id(), true).await
    }

    /// 用调用方指定的 request id 合入（例如进程重启后用记下的 id 重试，或用 [`Client::land_status`] 查询）。
    ///
    /// 同一个 request id 最多尝试 [`LAND_ATTEMPTS`] 次：网络中断、超时、写入竞争等可重试错误时原样重试，
    /// 服务端按 request id 去重，不会重复合入。
    pub async fn land_with_id(
        &self,
        reference: &str,
        changeset: &ObjectId,
        request_id: &str,
        fast_forward_only: bool,
    ) -> Result<Landed> {
        self.land_call(reference, changeset, request_id, fast_forward_only, false)
            .await
    }

    /// What [`Client::land`] would do now, without doing it: the commits it would add (rebased
    /// ones with the ids they would get), or the error it would fail with, such as a conflict.
    pub async fn land_check(&self, reference: &str, changeset: &ObjectId) -> Result<Landed> {
        self.land_call(reference, changeset, "", false, true).await
    }

    async fn land_call(
        &self,
        reference: &str,
        changeset: &ObjectId,
        request_id: &str,
        fast_forward_only: bool,
        dry_run: bool,
    ) -> Result<Landed> {
        let req = LandRequest {
            repo: self.repo()?,
            request_id: request_id.to_owned(),
            r#ref: reference.to_owned(),
            changeset_id: bytes_of(changeset),
            fast_forward_only,
            dry_run,
        };
        let mut attempt = 0;
        let resp = loop {
            attempt += 1;
            match LandServiceClient::new(self.channel.clone())
                .land(self.request(req.clone())?)
                .await
            {
                Ok(r) => break r.into_inner(),
                Err(s) => {
                    let e = Error::from(s);
                    if attempt >= LAND_ATTEMPTS || !e.is_retryable() {
                        return Err(e);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
                }
            }
        };
        landed_from(&resp)
    }

    /// 按 request id 查询合入结果：成功合入过返回结果；未收到、处理中、失败或已过期时为 `None`。
    pub async fn land_status(&self, request_id: &str) -> Result<Option<Landed>> {
        let req = GetLandStatusRequest {
            repo: self.repo()?,
            request_id: request_id.to_owned(),
        };
        let resp = LandServiceClient::new(self.channel.clone())
            .get_land_status(self.request(req)?)
            .await?
            .into_inner();
        match (LandState::try_from(resp.state), resp.result) {
            (Ok(LandState::Landed), Some(r)) => Ok(Some(landed_from(&r)?)),
            (Ok(LandState::Landed), None) => Err(Error::Protocol("LANDED status without a result".into())),
            _ => Ok(None),
        }
    }

    /// 解析 ref；不存在时返回 `Ok(None)`。
    pub async fn resolve_ref(&self, reference: &str) -> Result<Option<RefTip>> {
        let req = ResolveRefRequest {
            repo: self.repo()?,
            r#ref: reference.to_owned(),
        };
        match RefServiceClient::new(self.channel.clone())
            .resolve_ref(self.request(req)?)
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                Ok(Some(RefTip {
                    changeset: id_from(&r.changeset_id, "changeset id")?,
                    root_tree: id_from(&r.root_tree, "root tree")?,
                    version: r.version,
                }))
            }
            Err(s) => {
                let e = Error::from(s);
                if e.reason() == Some(v1::ErrorReason::RefNotFound) {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// 列出全部 ref（名字、提交、版本），按名字字节序。
    pub async fn list_refs(&self) -> Result<Vec<(String, ObjectId, u64)>> {
        let req = ListRefsRequest { repo: self.repo()? };
        let resp = RefServiceClient::new(self.channel.clone())
            .list_refs(self.request(req)?)
            .await?
            .into_inner();
        resp.refs
            .into_iter()
            .map(|r| Ok((r.name, id_from(&r.changeset_id, "changeset id")?, r.version)))
            .collect()
    }

    /// The moves of ref `name`, newest first: versions below `before` (from the newest when None),
    /// at most `limit` (0: the server's default). Also returns where the next page starts.
    pub async fn ref_log(
        &self,
        name: &str,
        before: Option<u64>,
        limit: u32,
    ) -> Result<(Vec<RefLogEntry>, Option<u64>)> {
        let req = ListRefLogRequest {
            repo: self.repo()?,
            r#ref: name.to_owned(),
            before_version: before.unwrap_or(0),
            limit,
        };
        let resp = RefServiceClient::new(self.channel.clone())
            .list_ref_log(self.request(req)?)
            .await?
            .into_inner();
        let entries = resp
            .entries
            .iter()
            .map(|e| {
                Ok(RefLogEntry {
                    version: e.version,
                    changeset: id_from(&e.changeset_id, "changeset id")?,
                    updated_at_ms: e.updated_at_ms,
                    how: e.r#move(),
                    actor: e.actor.clone(),
                })
            })
            .collect::<Result<_>>()?;
        Ok((
            entries,
            (resp.next_before_version > 0).then_some(resp.next_before_version),
        ))
    }

    /// 读取 `revision`（ref 名或提交 id）下的文件，并校验内容哈希。
    pub async fn read_file(&self, revision: &str, path: &str) -> Result<Bytes> {
        let req = ReadFileRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: path.to_owned(),
            offset: 0,
        };
        let mut stream = QueryServiceClient::new(self.channel.clone())
            .read_file(self.request(req)?)
            .await?
            .into_inner();
        let mut buf = BytesMut::new();
        let mut blob: Option<ObjectId> = None;
        while let Some(part) = stream.message().await? {
            let id = id_from(&part.blob_id, "blob id")?;
            if blob.is_some_and(|b| b != id) || part.offset != buf.len() as u64 {
                return Err(Error::Protocol(format!("inconsistent ReadFile stream for {path:?}")));
            }
            blob = Some(id);
            buf.extend_from_slice(&part.data);
            if part.last {
                let data = buf.freeze();
                if !id.verify(&data) {
                    return Err(Error::Corrupt(id));
                }
                return Ok(data);
            }
        }
        Err(Error::Protocol(format!(
            "ReadFile stream for {path:?} ended before the last chunk"
        )))
    }

    /// `revision`（ref 名或提交 id）的根目录树。
    pub async fn root_tree(&self, revision: &str) -> Result<ObjectId> {
        let req = ListDirRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: String::new(),
        };
        let resp = QueryServiceClient::new(self.channel.clone())
            .list_dir(self.request(req)?)
            .await?
            .into_inner();
        id_from(&resp.tree_id, "tree id")
    }

    /// 列出 `revision` 下的目录（`""` 为根目录）。
    pub async fn list_dir(&self, revision: &str, path: &str) -> Result<Vec<DirEntry>> {
        let req = ListDirRequest {
            repo: self.repo()?,
            revision: revision.to_owned(),
            path: path.to_owned(),
        };
        let resp = QueryServiceClient::new(self.channel.clone())
            .list_dir(self.request(req)?)
            .await?
            .into_inner();
        resp.entries
            .into_iter()
            .map(|e| {
                Ok(DirEntry {
                    mode: mode_from(e.mode)?,
                    id: id_from(&e.id, "entry id")?,
                    name: e.name,
                    size: e.size,
                })
            })
            .collect()
    }
}

/// 构造提交：收集改动，`create()` 时先上传缺失的文件内容，再创建提交。
pub struct ChangesetBuilder<'a> {
    client: &'a Client,
    parents: Vec<ObjectId>,
    author: Option<Signature>,
    committer: Option<Signature>,
    change_id: Option<[u8; 16]>,
    message: String,
    puts: Vec<(String, EntryMode, Bytes)>,
    deletes: Vec<String>,
    /// Replacements per path, applied by the server to the first parent's file.
    edits: BTreeMap<String, Vec<v1::TextReplacement>>,
    extensions: BTreeMap<String, Bytes>,
}

impl ChangesetBuilder<'_> {
    /// Replaces `old`, which must appear exactly once, with `new` in the file at `path` as the
    /// first parent has it. The server applies it, so the file is neither downloaded nor
    /// uploaded; several edits of one path apply in order.
    pub fn edit(self, path: impl Into<String>, old: impl Into<Bytes>, new: impl Into<Bytes>) -> Self {
        self.edit_with(path, old, new, false)
    }

    /// [`ChangesetBuilder::edit`] of every occurrence of `old` (at least one).
    pub fn edit_all(self, path: impl Into<String>, old: impl Into<Bytes>, new: impl Into<Bytes>) -> Self {
        self.edit_with(path, old, new, true)
    }

    fn edit_with(mut self, path: impl Into<String>, old: impl Into<Bytes>, new: impl Into<Bytes>, all: bool) -> Self {
        self.edits.entry(path.into()).or_default().push(v1::TextReplacement {
            old: old.into(),
            new: new.into(),
            all,
        });
        self
    }

    /// 写入普通文件。
    pub fn put(self, path: impl Into<String>, content: impl Into<Bytes>) -> Self {
        self.put_with_mode(path, EntryMode::File, content)
    }

    pub fn put_with_mode(mut self, path: impl Into<String>, mode: EntryMode, content: impl Into<Bytes>) -> Self {
        self.puts.push((path.into(), mode, content.into()));
        self
    }

    pub fn delete(mut self, path: impl Into<String>) -> Self {
        self.deletes.push(path.into());
        self
    }

    /// 追加一个父提交（合并提交）。
    pub fn parent(mut self, parent: ObjectId) -> Self {
        self.parents.push(parent);
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    /// 作者（时间取当前时间，时区为 UTC）。提交者未单独设置时与作者相同。
    pub fn author(mut self, name: impl Into<String>, email: impl Into<String>) -> Self {
        self.author = Some(Signature {
            name: name.into(),
            email: email.into(),
            time_ms: now_ms(),
            tz_offset_minutes: 0,
        });
        self
    }

    /// 带原始时间与时区的作者。
    pub fn author_signature(mut self, sig: Signature) -> Self {
        self.author = Some(sig);
        self
    }

    pub fn committer_signature(mut self, sig: Signature) -> Self {
        self.committer = Some(sig);
        self
    }

    /// 指定 change_id（默认随机生成）。导入外部历史时用确定的值，使重复导入得到相同的提交 id。
    pub fn change_id(mut self, id: [u8; 16]) -> Self {
        self.change_id = Some(id);
        self
    }

    pub fn extension(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.extensions.insert(key.into(), value.into());
        self
    }

    fn spec(&self, ids: &[ObjectId]) -> ChangesetSpec {
        let author = self.author.clone().unwrap_or_else(|| Signature {
            name: "scion".into(),
            email: "scion@localhost".into(),
            time_ms: now_ms(),
            tz_offset_minutes: 0,
        });
        let committer = self.committer.clone().unwrap_or_else(|| author.clone());
        let mut delta: Vec<v1::FileChange> = self
            .puts
            .iter()
            .zip(ids)
            .map(|((path, mode, content), id)| v1::FileChange {
                path: path.clone(),
                op: Some(file_change::Op::Put(v1::PutFile {
                    mode: proto_mode(*mode).into(),
                    id: bytes_of(id),
                    size: content.len() as u64,
                })),
            })
            .collect();
        delta.extend(self.deletes.iter().map(|p| v1::FileChange {
            path: p.clone(),
            op: Some(file_change::Op::Delete(v1::DeleteFile {})),
        }));
        delta.extend(self.edits.iter().map(|(p, r)| v1::FileChange {
            path: p.clone(),
            op: Some(file_change::Op::Edit(v1::EditFile {
                replacements: r.clone(),
            })),
        }));
        ChangesetSpec {
            parents: self.parents.iter().map(bytes_of).collect(),
            author: Some(author.to_proto()),
            committer: Some(committer.to_proto()),
            message: self.message.clone(),
            change_id: match self.change_id {
                Some(id) => Bytes::copy_from_slice(&id),
                None => Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
            },
            delta,
            extensions: self.extensions.clone().into_iter().collect(),
        }
    }

    pub async fn create(self) -> Result<CreatedChangeset> {
        let contents: Vec<Bytes> = self.puts.iter().map(|(_, _, c)| c.clone()).collect();
        let ids = self.client.upload_blobs(&contents).await?;
        let req = CreateChangesetRequest {
            repo: self.client.repo()?,
            request_id: request_id(),
            changeset: Some(self.spec(&ids)),
        };
        let resp = WriteServiceClient::new(self.client.channel.clone())
            .create_changeset(self.client.request(req)?)
            .await?
            .into_inner();
        Ok(CreatedChangeset {
            id: id_from(&resp.changeset_id, "changeset id")?,
            root_tree: id_from(&resp.root_tree, "root tree")?,
            generation: resp.generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_messages_split_and_mark_last() {
        let data = Bytes::from_static(b"abcdefg");
        let id = ObjectId::of_blob(&data);
        let m = upload_messages("r", &id, &data, 3);
        assert_eq!(m.iter().map(|x| x.offset).collect::<Vec<_>>(), [0, 3, 6]);
        assert_eq!(m.iter().map(|x| x.last).collect::<Vec<_>>(), [false, false, true]);
        assert!(m.iter().all(|x| x.repo == "r"));
        let empty = upload_messages("r", &ObjectId::of_blob(b""), &Bytes::new(), 0);
        assert_eq!(empty.len(), 1);
        assert!(empty[0].last && empty[0].data.is_empty());
    }

    #[test]
    fn mode_roundtrip() {
        for m in [
            EntryMode::File,
            EntryMode::Executable,
            EntryMode::Symlink,
            EntryMode::Tree,
        ] {
            assert_eq!(mode_from(proto_mode(m).into()).ok(), Some(m));
        }
        assert!(matches!(mode_from(0), Err(Error::Protocol(_))));
        assert!(matches!(mode_from(42), Err(Error::Protocol(_))));
    }

    #[test]
    fn id_from_bad_bytes() {
        assert!(matches!(id_from(b"x", "thing"), Err(Error::Protocol(m)) if m.contains("thing")));
    }

    #[test]
    fn request_ids_are_unique_uuids() {
        let (a, b) = (request_id(), request_id());
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
        assert!(now_ms() > 1_577_836_800_000);
    }
}
