//! The git gateway: git's smart HTTP protocol on Scion repositories ([design](../../../docs/design/git-gateway.md)).
//!
//! [`Gateway::serve`] answers `GET /<repo>/info/refs?service=…` and `POST /<repo>/git-upload-pack`
//! (protocol v0 and v2) and, with a [`receive::PushBackend`], `POST /<repo>/git-receive-pack`.

pub mod delta;
pub mod objects;
pub mod pack;
pub mod pkt;
pub mod receive;
pub mod unpack;
pub mod upload;

use std::convert::Infallible;
use std::sync::Arc;

use base64::Engine;
use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Method, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, Limited, StreamBody, combinators::BoxBody};
use scion_auth::{Action, AuthError, Guard, Principal, Request as AuthRequest};
use scion_object::ObjectId;
use scion_store::{BlobStore, MetaStore};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::upload::{Advertised, FetchRequest, Out};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Query(#[from] scion_query::QueryError),
    #[error(transparent)]
    Store(#[from] scion_store::Error),
    /// The repository holds something git cannot be given (design §7).
    #[error("{0}")]
    Unservable(String),
    /// A feature of the protocol this gateway does not offer yet.
    #[error("{0}")]
    Unsupported(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("{0}")]
    Internal(String),
}

impl From<pkt::PktError> for Error {
    fn from(e: pkt::PktError) -> Self {
        Error::Protocol(e.0)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// The response body: whole for advertisements and errors, streamed for packs.
pub type Body = BoxBody<Bytes, Infallible>;

/// The largest fetch request accepted: wants and haves, after gzip.
const MAX_REQUEST: usize = 16 << 20;
/// The largest push accepted: the commands and the pack, spooled to a temporary file.
const MAX_PUSH: usize = 16 << 30;

/// The endpoints of the smart HTTP protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    InfoRefs,
    UploadPack,
    ReceivePack,
}

/// The repository and endpoint of a git request, or `None` for any other request (gRPC among
/// them, whose paths are `/package.Service/Method`). A `.git` suffix on the repository is dropped.
pub fn route(method: &Method, path: &str, headers: &HeaderMap) -> Option<(String, Endpoint)> {
    let grpc = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/grpc"));
    if grpc {
        return None;
    }
    let (repo, endpoint) = if *method == Method::GET {
        (path.strip_suffix("/info/refs")?, Endpoint::InfoRefs)
    } else if *method == Method::POST {
        if let Some(r) = path.strip_suffix("/git-upload-pack") {
            (r, Endpoint::UploadPack)
        } else {
            (path.strip_suffix("/git-receive-pack")?, Endpoint::ReceivePack)
        }
    } else {
        return None;
    };
    let repo = repo.strip_prefix('/')?;
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    (!repo.is_empty()).then(|| (repo.to_string(), endpoint))
}

/// Serves git's smart HTTP protocol from Scion's storage.
pub struct Gateway {
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
    guard: Arc<Guard>,
    agent: String,
    /// Where pushes go, and the largest file a push may hold; without it pushes are refused.
    push: Option<(Arc<dyn receive::PushBackend>, u64)>,
}

impl Gateway {
    pub fn new(meta: Arc<dyn MetaStore>, blobs: Arc<dyn BlobStore>, guard: Arc<Guard>, version: &str) -> Self {
        Self {
            meta,
            blobs,
            guard,
            agent: format!("scion/{version}"),
            push: None,
        }
    }

    /// Accepts pushes, written through `backend`, with files up to `max_file` bytes.
    pub fn with_push(mut self, backend: Arc<dyn receive::PushBackend>, max_file: u64) -> Self {
        self.push = Some((backend, max_file));
        self
    }

    /// Answers one git request (one that [`route`] recognized).
    pub async fn serve<B>(&self, req: http::Request<B>, repo: String, endpoint: Endpoint) -> Response<Body>
    where
        B: http_body::Body<Data = Bytes> + Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let (parts, body) = req.into_parts();
        let service = match endpoint {
            Endpoint::InfoRefs => parts
                .uri
                .query()
                .unwrap_or_default()
                .split('&')
                .find_map(|kv| kv.strip_prefix("service=")),
            Endpoint::UploadPack => Some("git-upload-pack"),
            Endpoint::ReceivePack => Some("git-receive-pack"),
        };
        let push = match service {
            Some("git-upload-pack") => false,
            Some("git-receive-pack") if self.push.is_some() => true,
            Some("git-receive-pack") => {
                return text(
                    StatusCode::FORBIDDEN,
                    "this server does not accept git pushes; push with scn",
                );
            }
            _ => return text(StatusCode::FORBIDDEN, "only git's smart HTTP protocol is supported"),
        };
        let action = if push { Action::Write } else { Action::Read };
        let principal = match self.check(&parts.headers, &repo, action).await {
            Ok(p) => p,
            Err(r) => return *r,
        };
        let default = match self.meta.get_repo(&repo).await {
            Ok(Some(info)) if info.deleted_at_ms.is_none() => info.default_branch,
            Ok(_) => return text(StatusCode::NOT_FOUND, &format!("repository {repo} not found")),
            Err(e) => return failed(&e.into()),
        };
        let v2 = parts
            .headers
            .get("git-protocol")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(':').any(|p| p == "version=2"));
        tracing::debug!(%repo, subject = %principal.subject, ?endpoint, v2, "git request");
        if endpoint == Endpoint::InfoRefs {
            return if push {
                self.receive_refs(&repo, &default).await
            } else {
                self.info_refs(&repo, &default, v2).await
            };
        }
        if push {
            let file = match spool_body(&parts.headers, body, MAX_PUSH).await {
                Ok(f) => f,
                Err(r) => return *r,
            };
            return self.receive_pack(&principal, &repo, &default, file).await;
        }
        let body = match read_body(&parts.headers, body, MAX_REQUEST).await {
            Ok(b) => b,
            Err(r) => return *r,
        };
        self.upload_pack(repo, default, v2, body).await
    }

    /// Authenticates the request and checks it may read (or write) the repository. Missing or bad
    /// credentials get a 401 that makes git ask for them.
    async fn check(
        &self,
        headers: &HeaderMap,
        repo: &str,
        action: Action,
    ) -> std::result::Result<Principal, Box<Response<Body>>> {
        let token = credentials(headers);
        let p = match self.guard.authenticate(token.as_deref()).await {
            Ok(p) => p,
            Err(e) => return Err(Box::new(auth_error(e, true))),
        };
        let checked = if action == Action::Read {
            self.guard.authorize(&p, &AuthRequest::repo(repo, action)).await
        } else {
            // Writing may be allowed on some paths only: each commit is checked when it is made
            self.guard.authorize_any(&p, repo, action).await
        };
        match checked {
            Ok(()) => Ok(p),
            Err(e) => Err(Box::new(auth_error(e, token.is_none()))),
        }
    }

    async fn advertised(&self, repo: &str, default: &str) -> Result<Advertised> {
        upload::advertised(self.meta.as_ref(), self.blobs.as_ref(), repo, default).await
    }

    async fn info_refs(&self, repo: &str, default: &str, v2: bool) -> Response<Body> {
        let mut out = BytesMut::new();
        if v2 {
            // As git http-backend: no service line before a v2 advertisement
            out = upload::advertise_v2(&self.agent);
        } else {
            let adv = match self.advertised(repo, default).await {
                Ok(a) => a,
                Err(e) => return failed(&e),
            };
            pkt::put_line(&mut out, "# service=git-upload-pack");
            out.extend_from_slice(pkt::FLUSH);
            out.extend_from_slice(&upload::advertise_v0(&adv, &self.agent));
        }
        git_response("application/x-git-upload-pack-advertisement", full(out.freeze()))
    }

    async fn receive_refs(&self, repo: &str, default: &str) -> Response<Body> {
        let adv = match self.advertised(repo, default).await {
            Ok(a) => a,
            Err(e) => return failed(&e),
        };
        let mut out = BytesMut::new();
        pkt::put_line(&mut out, "# service=git-receive-pack");
        out.extend_from_slice(pkt::FLUSH);
        out.extend_from_slice(&receive::advertise(&adv, &self.agent));
        git_response("application/x-git-receive-pack-advertisement", full(out.freeze()))
    }

    /// A push: unpacks, turns the new commits into Scion commits, updates each ref and reports.
    async fn receive_pack(&self, p: &Principal, repo: &str, default: &str, body: std::fs::File) -> Response<Body> {
        const RESULT: &str = "application/x-git-receive-pack-result";
        let Some((backend, max_file)) = &self.push else {
            return text(StatusCode::FORBIDDEN, "this server does not accept git pushes");
        };
        // Reading the pack is blocking work: files and inflating
        let max_file = *max_file;
        let parsed = tokio::task::spawn_blocking(move || {
            use std::io::BufRead;
            let mut r = std::io::BufReader::with_capacity(1 << 16, body);
            let req = receive::parse_request(&mut r)?;
            let empty = r.fill_buf().map(|b| b.is_empty()).unwrap_or(true);
            let unpacked = if empty && req.commands.iter().all(|c| c.new == receive::ZERO) {
                Ok(unpack::Unpacked::default())
            } else {
                unpack::unpack(r, max_file)
            };
            Ok::<_, Error>((req, unpacked))
        })
        .await;
        let (req, unpacked) = match parsed {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return git_response(RESULT, full(err_pkt(&e))),
            Err(e) => return failed(&Error::Internal(format!("reading the push: {e}"))),
        };
        let (unpack_status, results) = match &unpacked {
            Ok(pack) => {
                let mut conv = receive::Converter::new(self.meta.as_ref(), self.blobs.as_ref(), repo, pack);
                let mut results = Vec::with_capacity(req.commands.len());
                for c in &req.commands {
                    let r = self.apply(p, repo, backend.as_ref(), &mut conv, c).await;
                    if let Err(e) = &r {
                        tracing::info!(%repo, git_ref = %c.git_ref, error = %e, "git push refused");
                    }
                    results.push((c.git_ref.clone(), r));
                }
                (Ok(()), results)
            }
            Err(e) => (
                Err(e.to_string()),
                req.commands
                    .iter()
                    .map(|c| (c.git_ref.clone(), Err("unpacker error".to_string())))
                    .collect(),
            ),
        };
        tracing::info!(%repo, subject = %p.subject, default, commands = results.len(), "git push");
        let report = receive::report(unpack_status, &results);
        let body = if req.side_band {
            let mut out = BytesMut::new();
            for chunk in report.chunks(pkt::MAX_BAND_DATA) {
                out.extend_from_slice(&pkt::band(1, chunk));
            }
            out.extend_from_slice(pkt::FLUSH);
            out.freeze()
        } else {
            report.freeze()
        };
        git_response(RESULT, full(body))
    }

    /// One ref command: checks the old value, converts the new commits, updates the ref.
    async fn apply(
        &self,
        p: &Principal,
        repo: &str,
        backend: &dyn receive::PushBackend,
        conv: &mut receive::Converter<'_>,
        c: &receive::Command,
    ) -> std::result::Result<(), String> {
        let name = receive::scion_ref_name(&c.git_ref)?;
        let current = self.meta.get_ref(repo, &name).await.map_err(|e| e.to_string())?;
        let current_sha = match &current {
            Some(state) => {
                let mut objects = objects::Objects::new(self.meta.as_ref(), self.blobs.as_ref(), repo);
                // An annotated tag is known to git by its tag object
                let refs = [(name.clone(), *state)];
                match upload::annotated_tags(&mut objects, &refs)
                    .await
                    .map_err(|e| e.to_string())?
                    .remove(&name)
                {
                    Some(t) => t.sha,
                    None => objects.commit_sha(state.target).await.map_err(|e| e.to_string())?,
                }
            }
            None => receive::ZERO,
        };
        if c.old != current_sha {
            return Err("fetch first".into());
        }
        if c.new == receive::ZERO {
            let state = current.ok_or("no such ref")?;
            return backend
                .update(
                    p,
                    repo,
                    receive::RefUpdate::Delete {
                        name,
                        version: state.version,
                    },
                )
                .await;
        }
        let (tip, annotation) = match conv.pushed_tag(&c.new).map_err(|e| e.to_string())? {
            Some(_) if !name.starts_with(upload::TAG_PREFIX) => {
                return Err("an annotated tag can only be pushed to refs/tags/".into());
            }
            Some((commit, raw)) => (commit, Some(raw)),
            None => (c.new, None),
        };
        let new = conv.convert(p, backend, tip).await.map_err(|e| e.to_string())?;
        let update = match current {
            None => receive::RefUpdate::Create { name, new, annotation },
            Some(state) => {
                let ff = scion_query::is_ancestor(self.meta.as_ref(), repo, state.target, new)
                    .await
                    .map_err(|e| e.to_string())?;
                if ff {
                    receive::RefUpdate::Land { name, new }
                } else {
                    receive::RefUpdate::Move {
                        name,
                        new,
                        version: state.version,
                    }
                }
            }
        };
        backend.update(p, repo, update).await
    }

    async fn upload_pack(&self, repo: String, default: String, v2: bool, body: Bytes) -> Response<Body> {
        const RESULT: &str = "application/x-git-upload-pack-result";
        if v2 {
            let req = match upload::parse_v2(&body) {
                Ok(r) => r,
                Err(e) => return git_response(RESULT, full(err_pkt(&e))),
            };
            match req.command.as_str() {
                "ls-refs" => match self.advertised(&repo, &default).await {
                    Ok(adv) => git_response(RESULT, full(upload::ls_refs(&adv, &req.args).freeze())),
                    Err(e) => git_response(RESULT, full(err_pkt(&e))),
                },
                "fetch" => match upload::parse_fetch_v2(&req.args) {
                    Ok(f) => git_response(RESULT, self.fetch(repo, f, true)),
                    Err(e) => git_response(RESULT, full(err_pkt(&e))),
                },
                other => git_response(
                    RESULT,
                    full(err_pkt(&Error::Unsupported(format!("unknown command {other:?}")))),
                ),
            }
        } else {
            match upload::parse_fetch_v0(&body) {
                Ok(f) => git_response(RESULT, self.fetch(repo, f, false)),
                Err(e) => git_response(RESULT, full(err_pkt(&e))),
            }
        }
    }

    /// Negotiates and, once ready, streams the pack.
    fn fetch(&self, repo: String, req: FetchRequest, v2: bool) -> Body {
        let (tx, rx) = mpsc::channel::<Bytes>(16);
        let meta = self.meta.clone();
        let blobs = self.blobs.clone();
        tokio::spawn(async move {
            fetch(meta.as_ref(), blobs.as_ref(), &repo, &req, v2, &tx).await;
        });
        let stream = ReceiverStream::new(rx).map(|b| Ok::<_, Infallible>(http_body::Frame::data(b)));
        BoxBody::new(StreamBody::new(stream))
    }
}

/// Answers a fetch request into `tx`. An error before the pack starts goes to the client as an
/// `ERR` packet, in place of the acknowledgments, so that git prints just that; once the pack has
/// started, on side band 3 (without side bands the pack just ends). The plan and the commits are
/// built before answering, as that is where most errors come from.
async fn fetch(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    req: &FetchRequest,
    v2: bool,
    tx: &mpsc::Sender<Bytes>,
) {
    if req.wants.is_empty() {
        return;
    }
    let started = match negotiate(meta, blobs, repo, req, v2).await {
        Ok((head, None)) => {
            let _ = tx.send(head.freeze()).await;
            return;
        }
        Ok((head, Some(sel))) => start_pack(meta, blobs, repo, &sel).await.map(|s| (head, s)),
        Err(e) => Err(e),
    };
    let (head, started) = match started {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(%repo, error = %e, "git fetch failed");
            let _ = tx.send(err_pkt(&e)).await;
            return;
        }
    };
    if tx.send(head.freeze()).await.is_err() {
        return;
    }
    let out = Out::new(tx.clone(), req.side_band, !req.no_progress);
    let opts = upload::PackOptions {
        thin: req.thin_pack,
        ofs: req.ofs_delta,
    };
    match upload::write_pack(started, &out, opts).await {
        Ok(()) => out.flush().await,
        Err(e) => {
            tracing::warn!(%repo, error = %e, "git fetch failed");
            out.error(&e.to_string()).await;
        }
    }
}

/// What the pack is to hold: the wanted and common commits, and the shallow boundary.
struct Selection {
    wants: upload::Wants,
    haves: Vec<ObjectId>,
    cut: std::collections::HashSet<ObjectId>,
    filter: upload::Filter,
}

/// The selection once ready to send the pack (`None` when the client should send more haves).
type Found = Option<Selection>;

/// The acknowledgments, and what the pack is to hold. Wants this repository does not have are
/// refused before anything else is sent.
async fn negotiate(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    req: &FetchRequest,
    v2: bool,
) -> Result<(BytesMut, Found)> {
    let mut wants = upload::resolve_wants(meta, blobs, repo, &req.wants).await?;
    let client_shallow = upload::common(meta, repo, &req.shallow).await?;
    let shallow = upload::shallow(meta, blobs, repo, &wants.commits, &client_shallow, req.deepen).await?;
    wants.commits.extend(shallow.more_wants.iter().copied());
    let mut shallow_lines = Vec::new();
    for s in &shallow.shallow {
        shallow_lines.push(format!("shallow {}", hex::encode(s)));
    }
    for s in &shallow.unshallow {
        shallow_lines.push(format!("unshallow {}", hex::encode(s)));
    }
    let common = upload::common(meta, repo, &req.haves).await?;
    let ready = req.done || upload::ready(req, common.len());
    let ids = common.iter().map(|(_, id)| *id).collect();
    let mut head = BytesMut::new();
    if v2 {
        if !req.done {
            pkt::put_line(&mut head, "acknowledgments");
            if common.is_empty() {
                pkt::put_line(&mut head, "NAK");
            }
            for (sha, _) in &common {
                pkt::put_line(&mut head, &format!("ACK {}", hex::encode(sha)));
            }
            if !ready {
                head.extend_from_slice(pkt::FLUSH);
                return Ok((head, None));
            }
            pkt::put_line(&mut head, "ready");
            head.extend_from_slice(pkt::DELIM);
        }
        if req.deepen.is_some() {
            pkt::put_line(&mut head, "shallow-info");
            for l in &shallow_lines {
                pkt::put_line(&mut head, l);
            }
            head.extend_from_slice(pkt::DELIM);
        }
        pkt::put_line(&mut head, "packfile");
        return Ok((
            head,
            Some(Selection {
                wants,
                haves: ids,
                cut: shallow.cut,
                filter: req.filter,
            }),
        ));
    }
    // v0: the shallow update comes first, in every response of a stateless exchange
    if req.deepen.is_some() {
        for l in &shallow_lines {
            pkt::put_line(&mut head, l);
        }
        head.extend_from_slice(pkt::FLUSH);
        if req.wants_only {
            return Ok((head, None));
        }
    }
    let last = common.last().map(|(s, _)| hex::encode(s));
    if req.multi_ack {
        for (sha, _) in &common {
            pkt::put_line(&mut head, &format!("ACK {} common", hex::encode(sha)));
        }
        if let Some(l) = last.as_ref().filter(|_| ready) {
            pkt::put_line(&mut head, &format!("ACK {l} ready"));
        }
    }
    if !(req.done || (req.no_done && ready && req.multi_ack)) {
        pkt::put_line(&mut head, "NAK");
        return Ok((head, None));
    }
    match &last {
        Some(l) => pkt::put_line(&mut head, &format!("ACK {l}")),
        None => pkt::put_line(&mut head, "NAK"),
    }
    Ok((
        head,
        Some(Selection {
            wants,
            haves: ids,
            cut: shallow.cut,
            filter: req.filter,
        }),
    ))
}

/// Plans the pack of a selection and builds its first commits.
async fn start_pack<'a>(
    meta: &'a dyn MetaStore,
    blobs: &'a dyn BlobStore,
    repo: &'a str,
    sel: &Selection,
) -> Result<upload::Started<'a>> {
    let plan = upload::plan(meta, blobs, repo, &sel.wants, &sel.haves, &sel.cut, sel.filter).await?;
    upload::check_redactions(meta, repo, &plan.files).await?;
    upload::start(meta, blobs, repo, plan, upload::COMMIT_BUFFER).await
}

/// The token in `Authorization`: the password of Basic credentials, or a Bearer token.
fn credentials(headers: &HeaderMap) -> Option<String> {
    let v = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, rest) = v.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        return Some(rest.trim().to_string());
    }
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let raw = base64::engine::general_purpose::STANDARD.decode(rest.trim()).ok()?;
    let pair = String::from_utf8(raw).ok()?;
    let (_, password) = pair.split_once(':')?;
    (!password.is_empty()).then(|| password.to_string())
}

/// 401 asks git for credentials; 403 when the credentials it sent cannot read the repository.
fn auth_error(e: AuthError, anonymous: bool) -> Response<Body> {
    match e {
        AuthError::Missing | AuthError::Expired | AuthError::Invalid(_) => unauthorized(&e.to_string()),
        AuthError::Denied { .. } if anonymous => unauthorized("authentication required"),
        AuthError::Denied { .. } => text(StatusCode::FORBIDDEN, &e.to_string()),
        AuthError::Unavailable(_) => text(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        AuthError::BadRequest(_) => text(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

fn unauthorized(msg: &str) -> Response<Body> {
    let mut r = text(StatusCode::UNAUTHORIZED, msg);
    r.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        http::HeaderValue::from_static("Basic realm=\"Scion\""),
    );
    r
}

/// The request body, gunzipped when git compressed it.
async fn read_body<B>(headers: &HeaderMap, body: B, limit: usize) -> std::result::Result<Bytes, Box<Response<Body>>>
where
    B: http_body::Body<Data = Bytes> + Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let raw = match Limited::new(body, limit).collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => {
            return Err(Box::new(text(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("request body: {e}"),
            )));
        }
    };
    let gzip = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));
    if !gzip {
        return Ok(raw);
    }
    use std::io::Read;
    let mut out = Vec::new();
    let mut dec = flate2::read::GzDecoder::new(&raw[..]).take(limit as u64 + 1);
    if let Err(e) = dec.read_to_end(&mut out) {
        return Err(Box::new(text(
            StatusCode::BAD_REQUEST,
            &format!("gzip request body: {e}"),
        )));
    }
    if out.len() > limit {
        return Err(Box::new(text(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")));
    }
    Ok(Bytes::from(out))
}

/// Writes a push's body to a temporary file, gunzipped when git compressed it, and returns
/// it rewound. More than `limit` bytes is refused.
async fn spool_body<B>(
    headers: &HeaderMap,
    body: B,
    limit: usize,
) -> std::result::Result<std::fs::File, Box<Response<Body>>>
where
    B: http_body::Body<Data = Bytes> + Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    use std::io::{Seek, Write};
    let io = |e: std::io::Error| Box::new(failed(&Error::Internal(format!("spooling the push: {e}"))));
    let file = tempfile::tempfile().map_err(io)?;
    let gzip = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));
    let mut out: Box<dyn Write + Send> = if gzip {
        Box::new(flate2::write::GzDecoder::new(file.try_clone().map_err(io)?))
    } else {
        Box::new(file.try_clone().map_err(io)?)
    };
    let mut body = std::pin::pin!(body);
    let mut total = 0usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| {
            let e: Box<dyn std::error::Error + Send + Sync> = e.into();
            Box::new(text(StatusCode::BAD_REQUEST, &format!("request body: {e}")))
        })?;
        let Ok(data) = frame.into_data() else { continue };
        total += data.len();
        if total > limit {
            return Err(Box::new(text(StatusCode::PAYLOAD_TOO_LARGE, "the push is too large")));
        }
        if let Err(e) = out.write_all(&data) {
            return Err(Box::new(text(StatusCode::BAD_REQUEST, &format!("request body: {e}"))));
        }
    }
    if let Err(e) = out.flush() {
        return Err(Box::new(text(StatusCode::BAD_REQUEST, &format!("request body: {e}"))));
    }
    drop(out);
    let mut file = file;
    file.rewind().map_err(io)?;
    Ok(file)
}

fn full(b: Bytes) -> Body {
    BoxBody::new(Full::new(b))
}

fn err_pkt(e: &Error) -> Bytes {
    let mut out = BytesMut::new();
    pkt::put_line(&mut out, &format!("ERR {e}"));
    out.freeze()
}

fn git_response(content_type: &'static str, body: Body) -> Response<Body> {
    let mut r = Response::new(body);
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, http::HeaderValue::from_static(content_type));
    h.insert(header::CACHE_CONTROL, http::HeaderValue::from_static("no-cache"));
    r
}

/// A plain-text error, which git prints as `remote: …`.
fn text(status: StatusCode, msg: &str) -> Response<Body> {
    let mut r = Response::new(full(Bytes::from(format!("{msg}\n"))));
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r
}

fn failed(e: &Error) -> Response<Body> {
    tracing::warn!(error = %e, "git request failed");
    text(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, v.parse().expect("value"));
        }
        h
    }

    #[test]
    fn routes_git_requests_only() {
        let none = HeaderMap::new();
        assert_eq!(
            route(&Method::GET, "/agentlab/app/info/refs", &none),
            Some(("agentlab/app".into(), Endpoint::InfoRefs))
        );
        assert_eq!(
            route(&Method::POST, "/agentlab/app.git/git-upload-pack", &none),
            Some(("agentlab/app".into(), Endpoint::UploadPack))
        );
        assert_eq!(
            route(&Method::POST, "/mono/git-receive-pack", &none),
            Some(("mono".into(), Endpoint::ReceivePack))
        );
        let grpc = headers(&[("content-type", "application/grpc")]);
        assert_eq!(route(&Method::POST, "/x/git-upload-pack", &grpc), None);
        assert_eq!(route(&Method::POST, "/scion.v1.QueryService/Log", &none), None);
        assert_eq!(route(&Method::GET, "/info/refs", &none), None);
        assert_eq!(route(&Method::PUT, "/x/info/refs", &none), None);
        assert_eq!(route(&Method::GET, "/x/git-upload-pack", &none), None);
    }

    #[test]
    fn reads_credentials() {
        // "git:scn_pat_x" and ":scn_pat_x"
        let basic = headers(&[("authorization", "Basic Z2l0OnNjbl9wYXRfeA==")]);
        assert_eq!(credentials(&basic).as_deref(), Some("scn_pat_x"));
        let no_user = headers(&[("authorization", "basic OnNjbl9wYXRfeA==")]);
        assert_eq!(credentials(&no_user).as_deref(), Some("scn_pat_x"));
        let bearer = headers(&[("authorization", "Bearer abc")]);
        assert_eq!(credentials(&bearer).as_deref(), Some("abc"));
        // "git:" has no password; garbage and other schemes are no credentials
        for bad in ["Basic Z2l0Og==", "Basic !!!", "Digest x", "Basic"] {
            assert_eq!(credentials(&headers(&[("authorization", bad)])), None, "{bad}");
        }
    }

    #[test]
    fn auth_errors_ask_for_credentials() {
        let denied = || AuthError::Denied {
            repo: "r".into(),
            action: "read",
            reference: None,
            path: None,
        };
        let r = auth_error(denied(), true);
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        assert!(r.headers().contains_key(header::WWW_AUTHENTICATE));
        assert_eq!(auth_error(denied(), false).status(), StatusCode::FORBIDDEN);
        assert_eq!(auth_error(AuthError::Expired, false).status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            auth_error(AuthError::Unavailable("x".into()), false).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            auth_error(AuthError::BadRequest("x".into()), false).status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn reads_gzipped_bodies() {
        use std::io::Write;
        let mut z = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        z.write_all(b"0009done\n").expect("gzip");
        let gz = z.finish().expect("gzip");
        let h = headers(&[("content-encoding", "gzip")]);
        let body = read_body(&h, Full::new(Bytes::from(gz)), MAX_REQUEST)
            .await
            .expect("body");
        assert_eq!(&body[..], b"0009done\n");
        let plain = read_body(&HeaderMap::new(), Full::new(Bytes::from_static(b"x")), MAX_REQUEST).await;
        assert_eq!(plain.expect("body"), Bytes::from_static(b"x"));
        let bad = read_body(&h, Full::new(Bytes::from_static(b"not gzip")), MAX_REQUEST).await;
        assert_eq!(bad.expect_err("bad gzip").status(), StatusCode::BAD_REQUEST);
    }

    async fn gateway() -> Gateway {
        let s = Arc::new(scion_store::MemStore::new());
        s.create_repo("r").await.expect("repo");
        Gateway::new(s.clone(), s, Guard::disabled(), "0.0.0")
    }

    async fn call(g: &Gateway, method: Method, uri: &str, v2: bool, body: &'static [u8]) -> (StatusCode, Bytes) {
        let mut b = http::Request::builder().method(method).uri(uri);
        if v2 {
            b = b.header("git-protocol", "version=2");
        }
        let req = b.body(Full::new(Bytes::from_static(body))).expect("request");
        let (repo, endpoint) = route(req.method(), req.uri().path(), req.headers()).expect("git request");
        let resp = g.serve(req, repo, endpoint).await;
        let status = resp.status();
        (status, resp.into_body().collect().await.expect("body").to_bytes())
    }

    #[tokio::test]
    async fn answers_requests_it_cannot_serve() {
        let g = gateway().await;
        let (st, body) = call(&g, Method::GET, "/r/info/refs", false, b"").await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        assert!(String::from_utf8_lossy(&body).contains("smart HTTP"));
        let (st, _) = call(&g, Method::GET, "/r/info/refs?service=git-receive-pack", false, b"").await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        let (st, _) = call(
            &g,
            Method::GET,
            "/missing/info/refs?service=git-upload-pack",
            false,
            b"",
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        // An empty repository advertises no refs, with v0 and v2
        let (st, body) = call(&g, Method::GET, "/r/info/refs?service=git-upload-pack", false, b"").await;
        assert_eq!(st, StatusCode::OK);
        assert!(String::from_utf8_lossy(&body).contains("capabilities^{}"));
        let (_, body) = call(&g, Method::GET, "/r/info/refs?service=git-upload-pack", true, b"").await;
        assert!(body.starts_with(b"000eversion 2\n"), "{body:?}");
    }

    #[tokio::test]
    async fn reports_protocol_errors_as_err_packets() {
        let g = gateway().await;
        let cases: [(bool, &'static [u8], &str); 5] = [
            (true, b"0011command=nope\n0000", "unknown command"),
            (true, b"zzzz", "bad length"),
            (true, b"0011command=fetch00010014deepen-relative\n0000", "--deepen"),
            (false, b"0014deepen-relative\n0000", "--deepen"),
            (true, b"0014command=ls-refs\n0000", ""),
        ];
        for (v2, body, want) in cases {
            let (st, got) = call(&g, Method::POST, "/r/git-upload-pack", v2, body).await;
            assert_eq!(st, StatusCode::OK);
            let got = String::from_utf8_lossy(&got).into_owned();
            if want.is_empty() {
                assert_eq!(got, "0000");
            } else {
                assert!(got.contains("ERR ") && got.contains(want), "{want}: {got}");
            }
        }
        // No wants: nothing to send
        let (_, got) = call(&g, Method::POST, "/r/git-upload-pack", false, b"0000").await;
        assert!(got.is_empty());
        // A want this repository does not have
        let (_, got) = call(
            &g,
            Method::POST,
            "/r/git-upload-pack",
            false,
            b"0032want 0101010101010101010101010101010101010101\n00000009done\n",
        )
        .await;
        assert!(String::from_utf8_lossy(&got).contains("not our ref"), "{got:?}");
    }

    #[tokio::test]
    async fn reports_unservable_commits_before_the_pack() {
        use scion_object::{Changeset, Signature};
        let (g, stub) = pushing().await;
        let sig = Signature {
            name: "A".into(),
            email: "a@x".into(),
            time_ms: 1_700_000_000_000,
            tz_offset_minutes: 0,
        };
        // An imported commit whose git SHA its fields do not rebuild, as a signed one without
        // its raw object
        let ext = [("git.sha1".to_string(), vec![0xee; 20])].into_iter().collect();
        let cs = Changeset::new(vec![], sig.clone(), sig, "signed\n", [4; 16], vec![], ext).expect("cs");
        let rec = stub.writer.create("r", &cs).await.expect("create");
        g.meta
            .cas_ref("r", "main", scion_store::RefExpect::Absent, rec.id)
            .await
            .expect("ref");
        let want = "want eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee side-band-64k";
        let mut v0 = BytesMut::new();
        pkt::put_line(&mut v0, want);
        v0.extend_from_slice(pkt::FLUSH);
        pkt::put_line(&mut v0, "done");
        let mut v2 = BytesMut::new();
        pkt::put_line(&mut v2, "command=fetch");
        v2.extend_from_slice(pkt::DELIM);
        pkt::put_line(&mut v2, "want eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        pkt::put_line(&mut v2, "done");
        v2.extend_from_slice(pkt::FLUSH);
        for (v2, body) in [(false, v0), (true, v2)] {
            let body: &'static [u8] = Box::leak(body.to_vec().into_boxed_slice());
            let (_, got) = call(&g, Method::POST, "/r/git-upload-pack", v2, body).await;
            // Just the error, with no acknowledgments before it
            let got = String::from_utf8_lossy(&got).into_owned();
            assert!(got[4..].starts_with("ERR git commit eeee"), "{got}");
            assert!(
                got.contains("without its raw git object") && got.contains("scn 0.1.11"),
                "{got}"
            );
        }
        // A commit past the budget fails once the pack has started: on side band 3
        let sel = Selection {
            wants: upload::resolve_wants(g.meta.as_ref(), g.blobs.as_ref(), "r", &[[0xee; 20]])
                .await
                .expect("wants"),
            haves: vec![],
            cut: Default::default(),
            filter: upload::Filter::None,
        };
        let plan = upload::plan(
            g.meta.as_ref(),
            g.blobs.as_ref(),
            "r",
            &sel.wants,
            &[],
            &sel.cut,
            sel.filter,
        )
        .await
        .expect("plan");
        let started = upload::start(g.meta.as_ref(), g.blobs.as_ref(), "r", plan, 0)
            .await
            .expect("start");
        let (tx, mut rx) = mpsc::channel(16);
        let out = Out::new(tx, true, false);
        let e = upload::write_pack(started, &out, upload::PackOptions::default())
            .await
            .expect_err("unservable");
        assert!(e.to_string().contains("without its raw git object"));
        drop(out);
        let mut sent = Vec::new();
        while let Some(b) = rx.recv().await {
            sent.extend_from_slice(&b);
        }
        // Only the pack header went out
        assert_eq!(&sent[4..9], b"\x01PACK", "{sent:?}");
    }

    /// Writes through the store directly and records ref updates.
    struct Stub {
        writer: scion_land::ChangesetWriter,
        updates: std::sync::Mutex<Vec<receive::RefUpdate>>,
    }

    #[async_trait::async_trait]
    impl receive::PushBackend for Stub {
        async fn store(
            &self,
            _: &Principal,
            repo: &str,
            objects: Vec<(ObjectId, Bytes)>,
        ) -> std::result::Result<(), String> {
            self.writer.store(repo, &objects).await.map_err(|e| e.to_string())
        }
        async fn create(
            &self,
            _: &Principal,
            repo: &str,
            cs: &scion_object::Changeset,
        ) -> std::result::Result<scion_store::ChangesetRecord, String> {
            self.writer.create(repo, cs).await.map_err(|e| e.to_string())
        }
        async fn update(&self, _: &Principal, _: &str, u: receive::RefUpdate) -> std::result::Result<(), String> {
            self.updates.lock().expect("lock").push(u);
            Ok(())
        }
    }

    async fn pushing() -> (Gateway, Arc<Stub>) {
        let s = Arc::new(scion_store::MemStore::new());
        s.create_repo("r").await.expect("repo");
        let stub = Arc::new(Stub {
            writer: scion_land::ChangesetWriter::new(s.clone(), s.clone()),
            updates: Default::default(),
        });
        let g = Gateway::new(s.clone(), s, Guard::disabled(), "0.0.0").with_push(stub.clone(), 1 << 20);
        (g, stub)
    }

    async fn push(g: &Gateway, body: Vec<u8>) -> String {
        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/r/git-receive-pack")
            .body(Full::new(Bytes::from(body)))
            .expect("request");
        let resp = g.serve(req, "r".into(), Endpoint::ReceivePack).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let b = resp.into_body().collect().await.expect("body").to_bytes();
        String::from_utf8_lossy(&b).into_owned()
    }

    fn commands(lines: &[String], pack: &[u8]) -> Vec<u8> {
        let mut b = BytesMut::new();
        for (i, l) in lines.iter().enumerate() {
            let l = if i == 0 {
                format!("{l}\0report-status")
            } else {
                l.clone()
            };
            pkt::put_line(&mut b, &l);
        }
        b.extend_from_slice(pkt::FLUSH);
        b.extend_from_slice(pack);
        b.to_vec()
    }

    #[tokio::test]
    async fn reports_each_ref_of_a_push() {
        let (g, stub) = pushing().await;
        let (st, adv) = call(&g, Method::GET, "/r/info/refs?service=git-receive-pack", false, b"").await;
        assert_eq!(st, StatusCode::OK);
        assert!(String::from_utf8_lossy(&adv).contains("report-status"));
        let zero = "0".repeat(40);
        // A pack that is not one: every ref fails
        let out = push(
            &g,
            commands(&[format!("{zero} {} refs/heads/main", "ab".repeat(20))], b"PACKjunk"),
        )
        .await;
        assert!(
            out.contains("unpack protocol error: bad pack") && out.contains("ng refs/heads/main unpacker error"),
            "{out}"
        );
        // Deletes need no pack; stale old values, missing refs and other namespaces are refused
        let out = push(
            &g,
            commands(
                &[
                    format!("{zero} {zero} refs/heads/gone"),
                    format!("{} {zero} refs/heads/stale", "01".repeat(20)),
                    format!("{zero} {zero} refs/notes/x"),
                ],
                b"",
            ),
        )
        .await;
        assert!(out.contains("unpack ok"), "{out}");
        assert!(out.contains("ng refs/heads/gone no such ref"), "{out}");
        assert!(out.contains("ng refs/heads/stale fetch first"), "{out}");
        assert!(out.contains("ng refs/notes/x only refs/heads/*"), "{out}");
        // A real commit: a file, a tree and the commit, then the same commit on a second branch
        let blob = b"hello\n";
        let blob_sha = unpack::object_sha(unpack::ObjType::Blob, blob);
        let mut tree = b"100644 a.txt\0".to_vec();
        tree.extend_from_slice(&blob_sha);
        let tree_sha = unpack::object_sha(unpack::ObjType::Tree, &tree);
        let commit = format!(
            "tree {}\nauthor A <a@x> 1700000000 +0000\ncommitter A <a@x> 1700000000 +0000\n\nfirst\n",
            hex::encode(tree_sha)
        );
        let commit_sha = hex::encode(unpack::object_sha(unpack::ObjType::Commit, commit.as_bytes()));
        let (mut w, head) = pack::PackWriter::new(3);
        let mut p = head.to_vec();
        p.extend_from_slice(&w.object(pack::Kind::Blob, blob));
        p.extend_from_slice(&w.object(pack::Kind::Tree, &tree));
        p.extend_from_slice(&w.object(pack::Kind::Commit, commit.as_bytes()));
        p.extend_from_slice(&w.finish());
        let out = push(
            &g,
            commands(
                &[
                    format!("{zero} {commit_sha} refs/heads/main"),
                    format!("{zero} {commit_sha} refs/tags/v1"),
                ],
                &p,
            ),
        )
        .await;
        assert!(
            out.contains("ok refs/heads/main") && out.contains("ok refs/tags/v1"),
            "{out}"
        );
        let updates = stub.updates.lock().expect("lock").clone();
        assert!(
            matches!(&updates[..], [
            receive::RefUpdate::Create { name: a, .. },
            receive::RefUpdate::Create { name: b, .. },
        ] if a == "main" && b == "tags/v1"),
            "{updates:?}"
        );
        // A malformed request
        let (_, out) = call(&g, Method::POST, "/r/git-receive-pack", false, b"zzzz").await;
        assert!(String::from_utf8_lossy(&out).contains("ERR "));
    }

    /// A tree derived before the reverse mappings existed: a push that renames its directory
    /// sends neither the directory nor its file, and the gateway indexes the parent to find them.
    #[tokio::test]
    async fn finds_unsent_objects_in_trees_derived_without_reverse_mappings() {
        use scion_object::{Changeset, EntryMode, FileChange, Signature, Tree};
        use scion_query::{
            GIT_BLOB_KIND, GIT_COMMIT_KIND, GIT_OBJECT_VERSION, GIT_TREE_KIND, GITSHA_KIND, GITSHA_VERSION,
        };
        let (g, stub) = pushing().await;
        let meta = g.meta.clone();
        let content = b"a\n";
        let file = ObjectId::of_blob(content);
        stub.writer
            .store("r", &[(file, Bytes::from_static(content))])
            .await
            .expect("store");
        let sig = Signature {
            name: "A".into(),
            email: "a@x".into(),
            time_ms: 1_700_000_000_000,
            tz_offset_minutes: 0,
        };
        let cs = Changeset::new(
            vec![],
            sig.clone(),
            sig,
            "base\n",
            [3; 16],
            vec![FileChange::put("docs/a.txt", EntryMode::File, file, 2)],
            Default::default(),
        )
        .expect("cs");
        let rec = stub.writer.create("r", &cs).await.expect("create");
        meta.cas_ref("r", "main", scion_store::RefExpect::Absent, rec.id)
            .await
            .expect("ref");
        // The old derivation: forward entries only
        let root = Tree::decode(&g.blobs.get(&rec.root_tree).await.expect("get").expect("root")).expect("tree");
        let docs = root.entries()[0].id;
        let blob_sha = scion_query::git_hash("blob", content);
        let docs_sha = scion_query::git_hash(
            "tree",
            &scion_query::git_tree_body(&[("a.txt".into(), EntryMode::File, blob_sha)]),
        );
        let root_sha = scion_query::git_hash(
            "tree",
            &scion_query::git_tree_body(&[("docs".into(), EntryMode::Tree, docs_sha)]),
        );
        let base_sha = scion_query::git_hash("commit", &scion_query::git_commit_body(rec.id, &cs, &root_sha, &[]));
        for (kind, key, value) in [
            (GIT_BLOB_KIND, file.to_bytes().to_vec(), blob_sha.to_vec()),
            (GIT_TREE_KIND, docs.to_bytes().to_vec(), docs_sha.to_vec()),
            (GIT_TREE_KIND, rec.root_tree.to_bytes().to_vec(), root_sha.to_vec()),
            (GIT_COMMIT_KIND, rec.id.to_bytes().to_vec(), base_sha.to_vec()),
        ] {
            meta.put_derived("r", kind, GIT_OBJECT_VERSION, &key, &value)
                .await
                .expect("put");
        }
        meta.put_derived("r", GITSHA_KIND, GITSHA_VERSION, &base_sha, &rec.id.to_bytes())
            .await
            .expect("put");
        // The push: a new root naming the same directory "manual"
        let tree = {
            let mut t = b"40000 manual\0".to_vec();
            t.extend_from_slice(&docs_sha);
            t
        };
        let tree_sha = unpack::object_sha(unpack::ObjType::Tree, &tree);
        let commit = format!(
            "tree {}\nparent {}\nauthor A <a@x> 1700000001 +0000\ncommitter A <a@x> 1700000001 +0000\n\nrename\n",
            hex::encode(tree_sha),
            hex::encode(base_sha)
        );
        let new_sha = hex::encode(unpack::object_sha(unpack::ObjType::Commit, commit.as_bytes()));
        let (mut w, head) = pack::PackWriter::new(2);
        let mut p = head.to_vec();
        p.extend_from_slice(&w.object(pack::Kind::Tree, &tree));
        p.extend_from_slice(&w.object(pack::Kind::Commit, commit.as_bytes()));
        p.extend_from_slice(&w.finish());
        let out = push(
            &g,
            commands(&[format!("{} {new_sha} refs/heads/main", hex::encode(base_sha))], &p),
        )
        .await;
        assert!(out.contains("ok refs/heads/main"), "{out}");
        let updates = stub.updates.lock().expect("lock").clone();
        let [receive::RefUpdate::Land { new, .. }] = &updates[..] else {
            panic!("{updates:?}")
        };
        let made = meta.get_changeset("r", new).await.expect("get").expect("record");
        let root = Tree::decode(&g.blobs.get(&made.root_tree).await.expect("get").expect("root")).expect("tree");
        assert_eq!(root.entries()[0].name, "manual");
        assert_eq!(root.entries()[0].id, docs);
    }

    /// A partial clone asks for a file by SHA in a repository derived before the reverse
    /// mappings existed: the refs' trees are indexed once, and the file is found.
    #[tokio::test]
    async fn finds_wanted_files_in_trees_derived_without_reverse_mappings() {
        use scion_object::{Changeset, EntryMode, FileChange, Signature};
        use scion_query::{GIT_BLOB_KIND, GIT_OBJECT_VERSION, GIT_TREE_KIND};
        let (g, stub) = pushing().await;
        let content = b"wanted\n";
        let file = ObjectId::of_blob(content);
        stub.writer
            .store("r", &[(file, Bytes::from_static(content))])
            .await
            .expect("store");
        let sig = Signature {
            name: "A".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
        };
        let cs = Changeset::new(
            vec![],
            sig.clone(),
            sig,
            "m\n",
            [4; 16],
            vec![FileChange::put("f.txt", EntryMode::File, file, content.len() as u64)],
            Default::default(),
        )
        .expect("cs");
        let rec = stub.writer.create("r", &cs).await.expect("create");
        g.meta
            .cas_ref("r", "main", scion_store::RefExpect::Absent, rec.id)
            .await
            .expect("ref");
        let blob_sha = scion_query::git_hash("blob", content);
        let root_sha = scion_query::git_hash(
            "tree",
            &scion_query::git_tree_body(&[("f.txt".into(), EntryMode::File, blob_sha)]),
        );
        for (kind, key, value) in [
            (GIT_BLOB_KIND, file.to_bytes().to_vec(), blob_sha.to_vec()),
            (GIT_TREE_KIND, rec.root_tree.to_bytes().to_vec(), root_sha.to_vec()),
        ] {
            g.meta
                .put_derived("r", kind, GIT_OBJECT_VERSION, &key, &value)
                .await
                .expect("put");
        }
        let w = upload::resolve_wants(g.meta.as_ref(), g.blobs.as_ref(), "r", &[blob_sha, root_sha])
            .await
            .expect("wants");
        assert_eq!((w.files, w.trees), (vec![file], vec![rec.root_tree]));
        // Indexed once: an unknown SHA now fails without walking again
        let e = upload::resolve_wants(g.meta.as_ref(), g.blobs.as_ref(), "r", &[[9; 20]]).await;
        assert!(matches!(e, Err(Error::Protocol(m)) if m.contains("not our ref")));
    }
}
