//! RefService：解析、列出与订阅 ref，以及直接创建、修改、删除 ref（分支管理）。
//!
//! 平时前进 ref 走 LandService（服务端 rebase、批处理、幂等）；这里的写操作是对单个 ref 的 CAS，
//! 不 rebase，用于建分支、删分支和强制回退。删除后重新创建的 ref 版本从 1 开始。

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use scion_object::ObjectId;
use scion_proto::v1;
use scion_proto::v1::ref_service_server::RefService;
use scion_proto::v1::{
    CreateRefRequest, CreateRefResponse, DeleteRefRequest, DeleteRefResponse, ListRefLogRequest, ListRefLogResponse,
    ListRefsRequest, ListRefsResponse, RefEvent, RefInfo, RefLogEntry, RefMove, ResolveRefRequest, ResolveRefResponse,
    UpdateRefRequest, UpdateRefResponse, WatchRefsRequest, WatchRefsResponse,
};
use scion_store::{BlobStore, MetaStore, RefExpect, RefState, TagAnnotation};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::auth::{authorize, authorize_any, principal};
use crate::service::support::{check_repo, parse_id, resolve_revision};
use crate::status;
use crate::watch::{RefHub, RefUpdate};
use scion_auth::{Action, Guard, Principal, Request as AuthRequest};

/// Entries a ref log page holds by default and at most.
const DEFAULT_REF_LOG: usize = 20;
const MAX_REF_LOG: usize = 1000;

pub type WatchRefsStream = Pin<Box<dyn Stream<Item = Result<WatchRefsResponse, Status>> + Send>>;

pub struct RefSvc {
    meta: Arc<dyn MetaStore>,
    hub: Arc<RefHub>,
    guard: Arc<Guard>,
    pre_land: Option<Arc<crate::prehook::PreLand>>,
    /// For a tag's git object: its target's git SHA is computed from the objects.
    blobs: Option<Arc<dyn BlobStore>>,
}

impl RefSvc {
    pub fn new(meta: Arc<dyn MetaStore>, hub: Arc<RefHub>) -> Self {
        Self {
            meta,
            hub,
            guard: Guard::disabled(),
            pre_land: None,
            blobs: None,
        }
    }

    pub fn with_blobs(mut self, blobs: Arc<dyn BlobStore>) -> Self {
        self.blobs = Some(blobs);
        self
    }

    /// Checks an annotation for tag `reference` on `target` and fills in what is left out: the
    /// caller as the tagger, now as the time. A git object must be a tag of that name on the
    /// target's commit.
    async fn annotation(
        &self,
        p: &Principal,
        repo: &str,
        reference: &str,
        target: ObjectId,
        a: v1::TagAnnotation,
    ) -> Result<TagAnnotation, Status> {
        let Some(name) = reference.strip_prefix(scion_land::TAG_PREFIX) else {
            return Err(Status::invalid_argument("only tags (tags/NAME) can be annotated"));
        };
        let tagger = match a.tagger {
            Some(t) if !t.name.is_empty() => {
                if t.tz_offset_minutes.abs() > 1440 {
                    return Err(Status::invalid_argument("tagger time zone out of range"));
                }
                scion_object::Signature {
                    name: t.name,
                    email: t.email,
                    time_ms: t.time_ms,
                    tz_offset_minutes: t.tz_offset_minutes,
                }
            }
            _ => scion_object::Signature {
                name: p.subject.clone(),
                email: String::new(),
                time_ms: crate::service::repos::now_ms(),
                tz_offset_minutes: 0,
            },
        };
        let git_object = if a.git_object.is_empty() {
            None
        } else {
            let tag = scion_object::git::parse_tag(&a.git_object)
                .ok_or_else(|| Status::invalid_argument("git_object is not a git tag object"))?;
            let blobs = self
                .blobs
                .as_ref()
                .ok_or_else(|| Status::unimplemented("this server does not take git tag objects"))?;
            let sha = scion_query::derive_git_sha(self.meta.as_ref(), blobs.as_ref(), repo, target)
                .await
                .map_err(crate::service::history::from_query)?;
            if tag.kind != "commit" || tag.name != name || tag.object != sha {
                return Err(Status::invalid_argument(format!(
                    "git_object must be the tag {name:?} of the target commit"
                )));
            }
            Some(a.git_object.to_vec())
        };
        Ok(TagAnnotation {
            target,
            tagger,
            message: a.message,
            git_object,
        })
    }

    /// 创建或移动 ref 之前调用外部检查（与合入相同）。
    pub fn with_pre_land(mut self, hook: Option<Arc<crate::prehook::PreLand>>) -> Self {
        self.pre_land = hook;
        self
    }

    pub fn with_guard(mut self, guard: Arc<Guard>) -> Self {
        self.guard = guard;
        self
    }

    /// 写 ref 前的检查：land 权限、ref 名、目标是仓库中的提交。
    async fn writable(&self, p: &Principal, repo: &str, reference: &str, target: &[u8]) -> Result<ObjectId, Status> {
        authorize(
            &self.guard,
            p,
            AuthRequest::repo(repo, Action::Land).with_ref(reference),
        )
        .await?;
        scion_land::validate_ref_name(reference).map_err(status::from_land)?;
        let id = parse_id(target)?;
        if self
            .meta
            .get_changeset(repo, &id)
            .await
            .map_err(status::from_store)?
            .is_none()
        {
            return Err(status::object_missing(&[id]));
        }
        if let Some(hook) = &self.pre_land {
            hook.check(p, repo, reference, id).await?;
        }
        Ok(id)
    }

    /// CAS 成功后通知订阅者并记日志。
    async fn written(&self, p: &Principal, repo: &str, reference: &str, state: RefState, what: &str) -> RefInfo {
        tracing::info!(%repo, r#ref = %reference, target = %state.target, version = state.version,
            by = %p.subject, "ref {what}");
        self.hub.publish(repo, reference, state).await;
        ref_info(reference.to_owned(), state)
    }
}

fn ref_info(name: String, s: RefState) -> RefInfo {
    RefInfo {
        name,
        changeset_id: Bytes::copy_from_slice(&s.target.to_bytes()),
        version: s.version,
        annotation: None,
    }
}

pub(crate) fn proto_annotation(a: &TagAnnotation) -> v1::TagAnnotation {
    v1::TagAnnotation {
        message: a.message.clone(),
        tagger: Some(v1::Signature {
            name: a.tagger.name.clone(),
            email: a.tagger.email.clone(),
            time_ms: a.tagger.time_ms,
            tz_offset_minutes: a.tagger.tz_offset_minutes,
        }),
        git_object: a.git_object.clone().map(Bytes::from).unwrap_or_default(),
    }
}

fn event(u: &RefUpdate) -> RefEvent {
    RefEvent {
        name: u.name.clone(),
        changeset_id: Bytes::copy_from_slice(&u.target.to_bytes()),
        root_tree: u
            .root_tree
            .map(|t| Bytes::copy_from_slice(&t.to_bytes()))
            .unwrap_or_default(),
        version: u.version,
    }
}

/// 一个订阅者的过滤状态：只关心的 ref，以及每个 ref 已发送的版本。
struct Filter {
    refs: HashSet<String>,
    sent: HashMap<String, u64>,
}

impl Filter {
    /// 是否需要发送；需要时记下版本。
    fn take(&mut self, u: &RefUpdate) -> bool {
        if !self.refs.is_empty() && !self.refs.contains(&u.name) {
            return false;
        }
        let sent = self.sent.entry(u.name.clone()).or_insert(0);
        if u.version <= *sent {
            return false;
        }
        *sent = u.version;
        true
    }

    fn take_all(&mut self, updates: &[RefUpdate]) -> Vec<RefEvent> {
        updates.iter().filter(|u| self.take(u)).map(event).collect()
    }
}

/// 订阅者的发送循环：通道里的变化逐批发送（同时到达的合并成一条响应），落后太多时重新取快照。
async fn pump(
    hub: Arc<RefHub>,
    repo: String,
    mut filter: Filter,
    mut rx: broadcast::Receiver<RefUpdate>,
    out: mpsc::Sender<Result<WatchRefsResponse, Status>>,
) {
    loop {
        let mut events = Vec::new();
        let first = tokio::select! {
            r = rx.recv() => r,
            () = out.closed() => return,
            () = hub.wait_closed() => return,
        };
        let mut next = Some(first);
        while let Some(r) = next.take() {
            match r {
                Ok(u) => {
                    if filter.take(&u) {
                        events.push(event(&u));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => match hub.snapshot(&repo).await {
                    Ok(snap) => events.extend(filter.take_all(&snap)),
                    Err(e) => {
                        let _ = out.send(Err(status::from_store(e))).await;
                        return;
                    }
                },
                Err(broadcast::error::RecvError::Closed) => return,
            }
            next = match rx.try_recv() {
                Ok(u) => Some(Ok(u)),
                Err(broadcast::error::TryRecvError::Lagged(n)) => Some(Err(broadcast::error::RecvError::Lagged(n))),
                Err(_) => None,
            };
        }
        if !events.is_empty()
            && out
                .send(Ok(WatchRefsResponse {
                    events,
                    snapshot: false,
                }))
                .await
                .is_err()
        {
            return;
        }
    }
}

#[tonic::async_trait]
impl RefService for RefSvc {
    type WatchRefsStream = WatchRefsStream;

    async fn resolve_ref(&self, request: Request<ResolveRefRequest>) -> Result<Response<ResolveRefResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        authorize_any(&self.guard, &p, &req.repo, Action::Read).await?;
        let state = self
            .meta
            .get_ref(&req.repo, &req.r#ref)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| status::ref_not_found(&req.r#ref))?;
        let rec = resolve_revision(self.meta.as_ref(), &req.repo, &state.target.to_string()).await?;
        Ok(Response::new(ResolveRefResponse {
            changeset_id: Bytes::copy_from_slice(&rec.id.to_bytes()),
            root_tree: Bytes::copy_from_slice(&rec.root_tree.to_bytes()),
            version: state.version,
        }))
    }

    async fn list_ref_log(&self, request: Request<ListRefLogRequest>) -> Result<Response<ListRefLogResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        authorize_any(&self.guard, &p, &req.repo, Action::Read).await?;
        check_repo(self.meta.as_ref(), &req.repo).await?;
        let limit = match req.limit {
            0 => DEFAULT_REF_LOG,
            n => (n as usize).min(MAX_REF_LOG),
        };
        let before = (req.before_version > 0).then_some(req.before_version);
        // One more than the page: the move into its oldest entry starts there
        let rows = self
            .meta
            .ref_log(&req.repo, &req.r#ref, before, limit + 1)
            .await
            .map_err(status::from_store)?;
        let mut entries = Vec::with_capacity(rows.len().min(limit));
        for (i, e) in rows.iter().take(limit).enumerate() {
            let prev = rows.get(i + 1).filter(|p| p.version + 1 == e.version);
            let ref_move = match prev {
                _ if e.version == 1 => RefMove::Created,
                Some(p) => {
                    let forward = scion_query::is_ancestor(self.meta.as_ref(), &req.repo, p.target, e.target)
                        .await
                        .map_err(crate::service::history::from_query)?;
                    if forward { RefMove::FastForward } else { RefMove::Forced }
                }
                // The entry before is gone (a fork starts its log afresh)
                None => RefMove::Unspecified,
            };
            entries.push(RefLogEntry {
                version: e.version,
                changeset_id: Bytes::copy_from_slice(&e.target.to_bytes()),
                updated_at_ms: e.updated_at_ms,
                r#move: ref_move.into(),
            });
        }
        let next_before_version = if rows.len() > limit {
            entries.last().map_or(0, |e| e.version)
        } else {
            0
        };
        Ok(Response::new(ListRefLogResponse {
            entries,
            next_before_version,
        }))
    }

    async fn list_refs(&self, request: Request<ListRefsRequest>) -> Result<Response<ListRefsResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        authorize_any(&self.guard, &p, &req.repo, Action::Read).await?;
        check_repo(self.meta.as_ref(), &req.repo).await?;
        let refs = self.meta.list_refs(&req.repo).await.map_err(status::from_store)?;
        let notes = if refs.iter().any(|(n, _)| scion_land::is_tag(n)) {
            self.meta.tag_annotations(&req.repo).await.map_err(status::from_store)?
        } else {
            Default::default()
        };
        Ok(Response::new(ListRefsResponse {
            refs: refs
                .into_iter()
                .map(|(name, s)| {
                    // Only while the tag still names the commit it was annotated on
                    let annotation = notes.get(&name).filter(|a| a.target == s.target).map(proto_annotation);
                    RefInfo {
                        annotation,
                        ..ref_info(name, s)
                    }
                })
                .collect(),
        }))
    }

    async fn create_ref(&self, request: Request<CreateRefRequest>) -> Result<Response<CreateRefResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        let target = self.writable(&p, &req.repo, &req.r#ref, &req.changeset_id).await?;
        let annotation = match req.annotation {
            Some(a) => Some(self.annotation(&p, &req.repo, &req.r#ref, target, a).await?),
            None => None,
        };
        let state = self
            .meta
            .cas_ref(&req.repo, &req.r#ref, RefExpect::Absent, target)
            .await
            .map_err(status::from_store)?;
        // After the ref, so a refused create leaves an existing tag's annotation alone; if it
        // cannot be saved, the tag goes again rather than stay without it
        if let Some(a) = &annotation {
            if let Err(e) = self.meta.put_tag_annotation(&req.repo, &req.r#ref, a).await {
                let _ = self.meta.delete_ref(&req.repo, &req.r#ref, Some(state.version)).await;
                return Err(status::from_store(e));
            }
        }
        let mut info = self.written(&p, &req.repo, &req.r#ref, state, "created").await;
        info.annotation = annotation.as_ref().map(proto_annotation);
        Ok(Response::new(CreateRefResponse { r#ref: Some(info) }))
    }

    async fn update_ref(&self, request: Request<UpdateRefRequest>) -> Result<Response<UpdateRefResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        if scion_land::is_tag(&req.r#ref) {
            return Err(status::from_land(scion_land::LandError::TagImmutable(
                req.r#ref.clone(),
            )));
        }
        let target = self.writable(&p, &req.repo, &req.r#ref, &req.changeset_id).await?;
        let expect = match req.expected_version {
            0 => {
                self.meta
                    .get_ref(&req.repo, &req.r#ref)
                    .await
                    .map_err(status::from_store)?
                    .ok_or_else(|| status::ref_not_found(&req.r#ref))?
                    .version
            }
            v => v,
        };
        let state = self
            .meta
            .cas_ref(&req.repo, &req.r#ref, RefExpect::Version(expect), target)
            .await
            .map_err(status::from_store)?;
        let info = self.written(&p, &req.repo, &req.r#ref, state, "updated").await;
        Ok(Response::new(UpdateRefResponse { r#ref: Some(info) }))
    }

    async fn delete_ref(&self, request: Request<DeleteRefRequest>) -> Result<Response<DeleteRefResponse>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        authorize(
            &self.guard,
            &p,
            AuthRequest::repo(&req.repo, Action::Land).with_ref(&req.r#ref),
        )
        .await?;
        let repo = self
            .meta
            .get_repo(&req.repo)
            .await
            .map_err(status::from_store)?
            .filter(|r| r.deleted_at_ms.is_none())
            .ok_or_else(|| status::repo_not_found(&req.repo))?;
        if repo.default_branch == req.r#ref {
            return Err(Status::failed_precondition(format!(
                "{:?} is the default branch of {}; change the default branch first",
                req.r#ref, req.repo
            )));
        }
        let expect = Some(req.expected_version).filter(|v| *v != 0);
        match self.meta.delete_ref(&req.repo, &req.r#ref, expect).await {
            Ok(()) => {}
            Err(scion_store::Error::RefVersionMismatch { current: None, .. }) => {
                return Err(status::ref_not_found(&req.r#ref));
            }
            Err(e) => return Err(status::from_store(e)),
        }
        tracing::info!(repo = %req.repo, r#ref = %req.r#ref, by = %p.subject, "ref deleted");
        Ok(Response::new(DeleteRefResponse {}))
    }

    async fn watch_refs(&self, request: Request<WatchRefsRequest>) -> Result<Response<Self::WatchRefsStream>, Status> {
        let p = principal(&request);
        let req = request.into_inner();
        authorize_any(&self.guard, &p, &req.repo, Action::Read).await?;
        check_repo(self.meta.as_ref(), &req.repo).await?;
        // 先订阅再取快照：两者之间的变化会出现在通道里，按版本去重
        let rx = self.hub.subscribe(&req.repo);
        let snap = self.hub.snapshot(&req.repo).await.map_err(status::from_store)?;
        let mut filter = Filter {
            refs: req.refs.into_iter().collect(),
            sent: req.known_versions,
        };
        let events = filter.take_all(&snap);
        let (tx, out) = mpsc::channel(16);
        let _ = tx.send(Ok(WatchRefsResponse { events, snapshot: true })).await;
        tokio::spawn(pump(self.hub.clone(), req.repo, filter, rx, tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(out))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::land::LandSvc;
    use crate::service::write::spec_to_changeset;
    use crate::service::write::tests::{REPO, spec, svc};
    use tonic::Code;

    #[tokio::test]
    async fn resolve_and_list() {
        let (store, w) = svc().await;
        let refs = RefSvc::new(
            store.clone(),
            RefHub::new(store.clone(), std::time::Duration::from_secs(3600)),
        );
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        LandSvc::new(scion_land::Coordinator::new(w.writer().clone(), Default::default()))
            .land(REPO, "main", a.id, false)
            .await
            .expect("land");

        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
            }))
            .await
            .expect("resolve")
            .into_inner();
        assert_eq!(ObjectId::from_bytes(&r.changeset_id), Ok(a.id));
        assert_eq!(ObjectId::from_bytes(&r.root_tree), Ok(a.root_tree));
        assert_eq!(r.version, 1);

        let l = refs
            .list_refs(Request::new(ListRefsRequest { repo: REPO.into() }))
            .await
            .expect("list")
            .into_inner();
        assert_eq!(l.refs.len(), 1);
        assert_eq!(l.refs[0].name, "main");
    }

    #[tokio::test]
    async fn ref_log_says_how_each_move_happened() {
        let (store, w) = svc().await;
        let refs = RefSvc::new(
            store.clone(),
            RefHub::new(store.clone(), std::time::Duration::from_secs(3600)),
        );
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        let b = w
            .create(REPO, &spec_to_changeset(spec(&[a.id], vec![]), 11).expect("s"))
            .await
            .expect("b");
        let id = |x: ObjectId| Bytes::copy_from_slice(&x.to_bytes());
        refs.create_ref(Request::new(CreateRefRequest {
            repo: REPO.into(),
            r#ref: "main".into(),
            changeset_id: id(a.id),
            annotation: None,
        }))
        .await
        .expect("create");
        for target in [b.id, a.id] {
            refs.update_ref(Request::new(UpdateRefRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
                changeset_id: id(target),
                expected_version: 0,
            }))
            .await
            .expect("update");
        }
        let log = |before_version, limit| {
            refs.list_ref_log(Request::new(ListRefLogRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
                before_version,
                limit,
            }))
        };
        let all = log(0, 0).await.expect("log").into_inner();
        let brief: Vec<(u64, Option<ObjectId>, RefMove)> = all
            .entries
            .iter()
            .map(|e| (e.version, ObjectId::from_bytes(&e.changeset_id).ok(), e.r#move()))
            .collect();
        assert_eq!(
            brief,
            [
                (3, Some(a.id), RefMove::Forced),
                (2, Some(b.id), RefMove::FastForward),
                (1, Some(a.id), RefMove::Created),
            ]
        );
        assert_eq!(all.next_before_version, 0);
        // Pages: the move into a page's oldest entry still comes from the entry before it
        let first = log(0, 1).await.expect("page").into_inner();
        assert_eq!((first.entries.len(), first.next_before_version), (1, 3));
        let second = log(3, 1).await.expect("page").into_inner();
        assert_eq!(second.entries[0].r#move(), RefMove::FastForward);
        assert_eq!(second.next_before_version, 2);

        let err = refs
            .list_ref_log(Request::new(ListRefLogRequest {
                repo: "nope".into(),
                ..Default::default()
            }))
            .await
            .expect_err("no repo");
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn annotated_tags_keep_what_they_say() {
        let (store, w) = svc().await;
        let hub = RefHub::new(store.clone(), std::time::Duration::from_secs(3600));
        let refs = RefSvc::new(store.clone(), hub.clone()).with_blobs(store.clone());
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        let create = |name: &str, annotation| CreateRefRequest {
            repo: REPO.into(),
            r#ref: name.into(),
            changeset_id: Bytes::copy_from_slice(&a.id.to_bytes()),
            annotation,
        };
        let note = |message: &str, git_object: Vec<u8>| v1::TagAnnotation {
            message: message.into(),
            tagger: None,
            git_object: git_object.into(),
        };
        // The caller tags, now
        let made = refs
            .create_ref(Request::new(create("tags/v1", Some(note("Release 1", vec![])))))
            .await
            .expect("tag")
            .into_inner()
            .r#ref
            .expect("ref");
        let tagger = made.annotation.as_ref().and_then(|a| a.tagger.clone()).expect("tagger");
        assert!(tagger.time_ms > 0, "{tagger:?}");
        // Creating it again is refused and leaves the annotation alone
        let err = refs
            .create_ref(Request::new(create("tags/v1", Some(note("other", vec![])))))
            .await
            .expect_err("exists");
        assert_eq!(err.code(), Code::FailedPrecondition);
        let listed = refs
            .list_refs(Request::new(ListRefsRequest { repo: REPO.into() }))
            .await
            .expect("list")
            .into_inner()
            .refs;
        assert_eq!(
            listed[0].annotation.as_ref().map(|a| a.message.as_str()),
            Some("Release 1")
        );

        // A git tag object must be that tag of that commit
        let sha = scion_query::derive_git_sha(store.as_ref(), store.as_ref(), REPO, a.id)
            .await
            .expect("sha");
        let sig = scion_object::Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 1_700_000_000_000,
            tz_offset_minutes: 60,
        };
        let raw = scion_object::git::tag_body(&sha, "v2", &sig, "signed\n");
        let v2 = refs
            .create_ref(Request::new(create("tags/v2", Some(note("signed\n", raw.clone())))))
            .await
            .expect("git tag")
            .into_inner()
            .r#ref
            .expect("ref");
        assert_eq!(v2.annotation.expect("note").git_object.as_ref(), raw.as_slice());
        let wrong = scion_object::git::tag_body(&[0; 20], "v3", &sig, "x");
        for (name, annotation, code) in [
            ("tags/v3", note("x", wrong), Code::InvalidArgument),
            ("tags/v3", note("x", raw), Code::InvalidArgument),
            ("tags/v3", note("x", b"not a tag".to_vec()), Code::InvalidArgument),
            ("main", note("x", vec![]), Code::InvalidArgument),
        ] {
            let err = refs
                .create_ref(Request::new(create(name, Some(annotation))))
                .await
                .expect_err("refused");
            assert_eq!(err.code(), code, "{name}: {err:?}");
        }
        let bad_tz = v1::TagAnnotation {
            tagger: Some(v1::Signature {
                name: "x".into(),
                tz_offset_minutes: 2000,
                ..Default::default()
            }),
            ..note("x", vec![])
        };
        let err = refs
            .create_ref(Request::new(create("tags/v4", Some(bad_tz))))
            .await
            .expect_err("tz");
        assert_eq!(err.code(), Code::InvalidArgument);
        // Without the objects, a git tag cannot be checked
        let raw = scion_object::git::tag_body(&sha, "v5", &sig, "x\n");
        let err = RefSvc::new(store.clone(), hub)
            .create_ref(Request::new(create("tags/v5", Some(note("x", raw)))))
            .await
            .expect_err("no blobs");
        assert_eq!(err.code(), Code::Unimplemented);
    }

    #[tokio::test]
    async fn create_update_and_delete_refs() {
        let (store, w) = svc().await;
        let refs = RefSvc::new(
            store.clone(),
            RefHub::new(store.clone(), std::time::Duration::from_secs(3600)),
        );
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        let b = w
            .create(REPO, &spec_to_changeset(spec(&[a.id], vec![]), 11).expect("s"))
            .await
            .expect("b");
        let id = |x: ObjectId| Bytes::copy_from_slice(&x.to_bytes());
        let create = |name: &str, target: ObjectId| CreateRefRequest {
            repo: REPO.into(),
            r#ref: name.into(),
            changeset_id: id(target),
            annotation: None,
        };
        let made = refs
            .create_ref(Request::new(create("feature/x", b.id)))
            .await
            .expect("create")
            .into_inner()
            .r#ref
            .expect("ref");
        assert_eq!((made.name.as_str(), made.version), ("feature/x", 1));
        let err = refs
            .create_ref(Request::new(create("feature/x", a.id)))
            .await
            .expect_err("exists");
        assert_eq!(err.code(), Code::FailedPrecondition);
        let err = refs
            .create_ref(Request::new(create("../x", a.id)))
            .await
            .expect_err("bad name");
        assert_eq!(err.code(), Code::InvalidArgument);
        let missing = ObjectId::hash(scion_object::ObjectKind::Changeset, b"nope");
        let err = refs
            .create_ref(Request::new(create("y", missing)))
            .await
            .expect_err("missing");
        assert_eq!(err.code(), Code::NotFound, "OBJECT_MISSING");

        // Tags can be created and deleted but never moved
        refs.create_ref(Request::new(create("tags/v1", a.id)))
            .await
            .expect("tag");
        let err = refs
            .update_ref(Request::new(UpdateRefRequest {
                repo: REPO.into(),
                r#ref: "tags/v1".into(),
                changeset_id: id(b.id),
                expected_version: 0,
            }))
            .await
            .expect_err("tags never move");
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("tag"), "{err:?}");

        // 强制回退（不是快进）；带版本时检查版本
        let update = |target: ObjectId, expected_version: u64| UpdateRefRequest {
            repo: REPO.into(),
            r#ref: "feature/x".into(),
            changeset_id: id(target),
            expected_version,
        };
        let moved = refs
            .update_ref(Request::new(update(a.id, 0)))
            .await
            .expect("update")
            .into_inner()
            .r#ref
            .expect("ref");
        assert_eq!(
            (ObjectId::from_bytes(&moved.changeset_id), moved.version),
            (Ok(a.id), 2)
        );
        let err = refs.update_ref(Request::new(update(b.id, 1))).await.expect_err("stale");
        assert_eq!(err.code(), Code::FailedPrecondition);
        refs.update_ref(Request::new(update(b.id, 2))).await.expect("versioned");
        let err = refs
            .update_ref(Request::new(UpdateRefRequest {
                r#ref: "nope".into(),
                ..update(a.id, 0)
            }))
            .await
            .expect_err("no such ref");
        assert_eq!(err.code(), Code::NotFound);

        // 删除：默认分支受保护；版本不符、ref 不存在
        refs.create_ref(Request::new(create("main", b.id))).await.expect("main");
        let delete = |name: &str, expected_version: u64| DeleteRefRequest {
            repo: REPO.into(),
            r#ref: name.into(),
            expected_version,
        };
        let err = refs
            .delete_ref(Request::new(delete("main", 0)))
            .await
            .expect_err("default branch");
        assert_eq!(err.code(), Code::FailedPrecondition);
        let err = refs
            .delete_ref(Request::new(delete("feature/x", 1)))
            .await
            .expect_err("stale");
        assert_eq!(err.code(), Code::FailedPrecondition);
        refs.delete_ref(Request::new(delete("feature/x", 3)))
            .await
            .expect("delete");
        let err = refs
            .delete_ref(Request::new(delete("feature/x", 0)))
            .await
            .expect_err("gone");
        assert_eq!(err.code(), Code::NotFound);
        let err = refs
            .delete_ref(Request::new(DeleteRefRequest {
                repo: "nope".into(),
                ..delete("x", 0)
            }))
            .await
            .expect_err("no repo");
        assert_eq!(err.code(), Code::NotFound);
        let left = refs
            .list_refs(Request::new(ListRefsRequest { repo: REPO.into() }))
            .await
            .expect("list")
            .into_inner();
        assert_eq!(
            left.refs.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            ["main", "tags/v1"]
        );
    }

    #[tokio::test]
    async fn errors() {
        let (store, _w) = svc().await;
        let refs = RefSvc::new(store.clone(), RefHub::new(store, std::time::Duration::from_secs(3600)));
        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: REPO.into(),
                r#ref: "main".into(),
            }))
            .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = refs
            .resolve_ref(Request::new(ResolveRefRequest {
                repo: "x".into(),
                r#ref: "main".into(),
            }))
            .await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
        let r = refs.list_refs(Request::new(ListRefsRequest { repo: "x".into() })).await;
        assert_eq!(r.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }

    fn hub(store: Arc<scion_store::MemStore>) -> Arc<RefHub> {
        RefHub::new(store, std::time::Duration::from_secs(3600))
    }

    async fn next(stream: &mut WatchRefsStream) -> WatchRefsResponse {
        use tokio_stream::StreamExt;
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("timely")
            .expect("item")
            .expect("ok")
    }

    #[tokio::test]
    async fn watch_sends_snapshot_then_lands() {
        let (store, w) = svc().await;
        let h = hub(store.clone());
        let refs = RefSvc::new(store.clone(), h.clone());
        let lands = LandSvc::new(scion_land::Coordinator::new(w.writer().clone(), Default::default())).with_ref_hub(h);
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        lands.land(REPO, "main", a.id, false).await.expect("main");
        lands.land(REPO, "dev", a.id, false).await.expect("dev");
        lands.land(REPO, "old", a.id, false).await.expect("old");

        // 只订阅 main 与 old；old 的版本客户端已知
        let mut stream = refs
            .watch_refs(Request::new(WatchRefsRequest {
                repo: REPO.into(),
                refs: vec!["main".into(), "old".into()],
                known_versions: HashMap::from([("old".to_string(), 1)]),
            }))
            .await
            .expect("watch")
            .into_inner();
        let snap = next(&mut stream).await;
        assert!(snap.snapshot);
        assert_eq!(snap.events.len(), 1);
        assert_eq!(snap.events[0].name, "main");
        assert_eq!(ObjectId::from_bytes(&snap.events[0].root_tree), Ok(a.root_tree));

        let b = w
            .create(REPO, &spec_to_changeset(spec(&[a.id], vec![]), 11).expect("s"))
            .await
            .expect("b");
        lands.land(REPO, "dev", b.id, false).await.expect("dev, not watched");
        lands.land(REPO, "main", b.id, false).await.expect("main");
        let ev = next(&mut stream).await;
        assert!(!ev.snapshot);
        assert_eq!(ev.events.len(), 1);
        assert_eq!((ev.events[0].name.as_str(), ev.events[0].version), ("main", 2));
        assert_eq!(ObjectId::from_bytes(&ev.events[0].changeset_id), Ok(b.id));

        let missing = refs
            .watch_refs(Request::new(WatchRefsRequest {
                repo: "x".into(),
                ..Default::default()
            }))
            .await;
        assert_eq!(missing.map(|_| ()).map_err(|s| s.code()), Err(Code::NotFound));
    }

    fn update(name: &str, version: u64) -> RefUpdate {
        RefUpdate {
            name: name.into(),
            target: ObjectId::of_blob(name.as_bytes()),
            root_tree: None,
            version,
        }
    }

    #[test]
    fn filter_keeps_newer_versions_of_watched_refs() {
        let mut all = Filter {
            refs: HashSet::new(),
            sent: HashMap::from([("a".to_string(), 2)]),
        };
        assert!(!all.take(&update("a", 2)));
        assert!(all.take(&update("a", 3)));
        assert!(all.take(&update("b", 1)));
        assert!(!all.take(&update("b", 1)));
        let mut some = Filter {
            refs: HashSet::from(["a".to_string()]),
            sent: HashMap::new(),
        };
        assert!(!some.take(&update("b", 1)));
        assert_eq!(some.take_all(&[update("a", 1), update("b", 1)]).len(), 1);
        assert!(event(&update("a", 1)).root_tree.is_empty());
    }

    /// 订阅者落后太多（通道 Lagged）：重新取快照补上；同时到达的变化合并成一条响应。
    #[tokio::test]
    async fn pump_recovers_from_lag_and_batches() {
        let (store, w) = svc().await;
        let h = hub(store.clone());
        let a = w
            .create(REPO, &spec_to_changeset(spec(&[], vec![]), 10).expect("s"))
            .await
            .expect("a");
        store
            .cas_ref(REPO, "main", scion_store::RefExpect::Absent, a.id)
            .await
            .expect("main");
        let (btx, brx) = broadcast::channel(2);
        for v in 1..=4 {
            btx.send(update("x", v)).expect("send");
        }
        let (tx, mut out) = mpsc::channel(4);
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        let task = tokio::spawn(pump(h, REPO.into(), filter, brx, tx));
        let resp = out.recv().await.expect("resp").expect("ok");
        // 快照里的 main，加上通道里剩下的 x 的最新版本
        let got: Vec<_> = resp.events.iter().map(|e| (e.name.as_str(), e.version)).collect();
        assert!(got.contains(&("main", 1)), "{got:?}");
        assert!(got.contains(&("x", 4)), "{got:?}");
        // 通道关闭：发送循环结束
        drop(btx);
        task.await.expect("pump ends");

        // 服务端停机：发送循环结束，流随之结束
        let (btx, brx) = broadcast::channel(2);
        let (tx, mut out) = mpsc::channel(1);
        let closing = hub(store.clone());
        closing.close();
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        pump(closing, REPO.into(), filter, brx, tx).await;
        assert!(out.recv().await.is_none());
        drop(btx);

        // 客户端断开：发送循环结束
        let (btx, brx) = broadcast::channel(2);
        let (tx, out) = mpsc::channel(1);
        drop(out);
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        pump(hub(store.clone()), REPO.into(), filter, brx, tx).await;
        drop(btx);

        // 重新取快照失败：把错误发给客户端
        let (btx, brx) = broadcast::channel(1);
        btx.send(update("x", 1)).expect("send");
        btx.send(update("x", 2)).expect("send");
        let (tx, mut out) = mpsc::channel(1);
        let filter = Filter {
            refs: HashSet::new(),
            sent: HashMap::new(),
        };
        pump(hub(store), "missing".into(), filter, brx, tx).await;
        assert_eq!(
            out.recv().await.expect("err").map(|_| ()).map_err(|s| s.code()),
            Err(Code::NotFound)
        );
    }
}
