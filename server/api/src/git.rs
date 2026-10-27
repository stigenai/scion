//! Sends git's smart HTTP requests to the git gateway before the gRPC layers see them, so one
//! listener and one repository URL serve both `scn` and git.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use scion_git::Gateway;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct GitLayer {
    gateway: Option<Arc<Gateway>>,
}

impl GitLayer {
    /// `None` leaves every request to the inner service.
    pub fn new(gateway: Option<Arc<Gateway>>) -> Self {
        Self { gateway }
    }
}

impl<S> Layer<S> for GitLayer {
    type Service = GitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GitService {
            inner,
            gateway: self.gateway.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GitService<S> {
    inner: S,
    gateway: Option<Arc<Gateway>>,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for GitService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: http_body::Body<Data = Bytes> + Send + 'static,
    ReqBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    ResBody: http_body::Body<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let git = self
            .gateway
            .as_ref()
            .and_then(|g| scion_git::route(req.method(), req.uri().path(), req.headers()).map(|r| (g.clone(), r)));
        if let Some((gateway, (repo, endpoint))) = git {
            return Box::pin(async move { Ok(gateway.serve(req, repo, endpoint).await.map(tonic::body::Body::new)) });
        }
        let fut = self.inner.call(req);
        Box::pin(async move { Ok(fut.await?.map(tonic::body::Body::new)) })
    }
}

/// Pushes through the same services as `scn`: CreateChangeset's path checks, Land (fast-forward
/// only) and the ref RPCs, so authorization, pre-land checks, events and cluster forwarding apply.
pub struct PushBackend {
    pub write: Arc<crate::service::WriteSvc>,
    pub land: Arc<crate::service::LandSvc>,
    pub refs: Arc<crate::service::RefSvc>,
}

fn as_principal<T>(p: &scion_auth::Principal, msg: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.extensions_mut().insert(p.clone());
    r
}

/// What git prints after `ng REF`.
fn reason(s: tonic::Status) -> String {
    use tonic_types::StatusExt;
    if let Some(info) = s.get_error_details().error_info() {
        if info.reason == "NOT_FAST_FORWARD" {
            return "non-fast-forward".into();
        }
    }
    s.message().to_string()
}

#[tonic::async_trait]
impl scion_git::receive::PushBackend for PushBackend {
    async fn store(
        &self,
        p: &scion_auth::Principal,
        repo: &str,
        objects: Vec<(scion_object::ObjectId, Bytes)>,
    ) -> Result<(), String> {
        self.write.store_as(p, repo, &objects).await.map_err(reason)
    }

    async fn create(
        &self,
        p: &scion_auth::Principal,
        repo: &str,
        cs: &scion_object::Changeset,
    ) -> Result<scion_store::ChangesetRecord, String> {
        self.write.create_as(p, repo, cs).await.map_err(reason)
    }

    async fn update(
        &self,
        p: &scion_auth::Principal,
        repo: &str,
        u: scion_git::receive::RefUpdate,
    ) -> Result<(), String> {
        use scion_git::receive::RefUpdate;
        use scion_proto::v1::land_service_server::LandService;
        use scion_proto::v1::ref_service_server::RefService;
        use scion_proto::v1::{CreateRefRequest, DeleteRefRequest, LandRequest, UpdateRefRequest};
        let id = |c: scion_object::ObjectId| Bytes::copy_from_slice(&c.to_bytes());
        let repo = repo.to_string();
        let result = match u {
            RefUpdate::Land { name, new } => LandService::land(
                self.land.as_ref(),
                as_principal(
                    p,
                    LandRequest {
                        repo,
                        request_id: String::new(),
                        r#ref: name,
                        changeset_id: id(new),
                        fast_forward_only: true,
                        dry_run: false,
                    },
                ),
            )
            .await
            .map(drop),
            RefUpdate::Create { name, new, annotation } => {
                // The message and tagger come from the tag object, which is kept as it is
                let annotation = annotation.map(|raw| {
                    let tag = scion_object::git::parse_tag(&raw);
                    scion_proto::v1::TagAnnotation {
                        message: tag.as_ref().map(|t| t.message.clone()).unwrap_or_default(),
                        tagger: tag.and_then(|t| t.tagger).map(|s| scion_proto::v1::Signature {
                            name: s.name,
                            email: s.email,
                            time_ms: s.time_ms,
                            tz_offset_minutes: s.tz_offset_minutes,
                        }),
                        git_object: raw.into(),
                    }
                });
                self.refs
                    .create_ref(as_principal(
                        p,
                        CreateRefRequest {
                            repo,
                            r#ref: name,
                            changeset_id: id(new),
                            annotation,
                        },
                    ))
                    .await
                    .map(drop)
            }
            RefUpdate::Move { name, new, version } => self
                .refs
                .update_ref(as_principal(
                    p,
                    UpdateRefRequest {
                        repo,
                        r#ref: name,
                        changeset_id: id(new),
                        expected_version: version,
                    },
                ))
                .await
                .map(drop),
            RefUpdate::Delete { name, version } => self
                .refs
                .delete_ref(as_principal(
                    p,
                    DeleteRefRequest {
                        repo,
                        r#ref: name,
                        expected_version: version,
                    },
                ))
                .await
                .map(drop),
        };
        result.map_err(reason)
    }
}
