//! OrgService: organizations and their members (docs/design/authorization.md, proto/scion/v1/org.proto).
//!
//! Management checks read the members from the store, not the authorization cache, so a removed
//! owner loses control at once.

use std::sync::Arc;

use scion_auth::orgs::{Role, VISIBILITIES, validate_org_name};
use scion_auth::{Guard, Principal};
use scion_proto::v1::org_service_server::OrgService;
use scion_proto::v1::{
    CreateOrgRequest, CreateOrgResponse, DeleteOrgRequest, DeleteOrgResponse, GetOrgRequest, GetOrgResponse,
    ListMembersRequest, ListMembersResponse, ListOrgsRequest, ListOrgsResponse, Member, Org, PutMemberRequest,
    PutMemberResponse, RemoveMemberRequest, RemoveMemberResponse, SetOrgVisibilityRequest, SetOrgVisibilityResponse,
};
use scion_store::{MetaStore, OrgMember, OrgRecord, RepoFilter};
use tonic::{Request, Response, Status};

use crate::auth::{principal, to_status};
use crate::service::repos::now_ms;
use crate::status;

pub struct OrgSvc {
    meta: Arc<dyn MetaStore>,
    guard: Arc<Guard>,
}

fn org_proto(o: OrgRecord) -> Org {
    Org {
        name: o.name,
        visibility: o.visibility,
        created_at_ms: o.created_at_ms,
        created_by: o.created_by,
    }
}

fn member_proto(m: OrgMember) -> Member {
    Member {
        subject: m.subject,
        role: m.role,
        added_at_ms: m.added_at_ms,
        added_by: m.added_by,
    }
}

fn visibility(v: &str) -> Result<String, Status> {
    match v {
        "" => Ok("private".into()),
        v if VISIBILITIES.contains(&v) => Ok(v.into()),
        v => Err(Status::invalid_argument(format!(
            "unknown visibility {v:?}; use private or internal"
        ))),
    }
}

impl OrgSvc {
    pub fn new(meta: Arc<dyn MetaStore>, guard: Arc<Guard>) -> Self {
        Self { meta, guard }
    }

    /// The caller, who must be signed in, on a server whose chain includes `orgs`.
    fn caller<T>(&self, request: &Request<T>) -> Result<Principal, Status> {
        if !self.guard.orgs_enabled() {
            return Err(Status::failed_precondition(
                "organizations are not enabled on this server: add \"orgs\" to [authorization] chain in auth.toml",
            ));
        }
        let p = principal(request);
        if !p.authenticated {
            return Err(Status::unauthenticated("authentication required"));
        }
        Ok(p)
    }

    async fn org(&self, name: &str) -> Result<OrgRecord, Status> {
        self.meta
            .get_org(name)
            .await
            .map_err(status::from_store)?
            .ok_or_else(|| Status::not_found(format!("organization {name:?} not found")))
    }

    async fn role(&self, org: &str, subject: &str) -> Result<Option<Role>, Status> {
        Ok(self
            .meta
            .list_org_members(org)
            .await
            .map_err(status::from_store)?
            .into_iter()
            .find(|m| m.subject == subject)
            .and_then(|m| Role::parse(&m.role)))
    }

    async fn is_admin(&self, p: &Principal) -> Result<bool, Status> {
        self.guard.is_admin(p).await.map_err(to_status)
    }

    /// Owners of `org`, and system administrators.
    async fn require_owner(&self, p: &Principal, org: &str) -> Result<(), Status> {
        if self.role(org, &p.subject).await? == Some(Role::Owner) || self.is_admin(p).await? {
            return Ok(());
        }
        Err(Status::permission_denied(format!(
            "only owners of {org:?} can change it"
        )))
    }

    /// Whether repositories (live, or deleted and not purged) exist under `org/`.
    async fn has_repos(&self, org: &str) -> Result<bool, Status> {
        for deleted in [false, true] {
            let filter = RepoFilter {
                prefix: format!("{org}/"),
                after: None,
                limit: 1,
                deleted,
            };
            if !self
                .meta
                .list_repo_infos(&filter)
                .await
                .map_err(status::from_store)?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Refuses a change that would leave `org` without an owner.
    async fn keep_an_owner(&self, org: &str, subject: &str) -> Result<(), Status> {
        let owners: Vec<OrgMember> = self
            .meta
            .list_org_members(org)
            .await
            .map_err(status::from_store)?
            .into_iter()
            .filter(|m| m.role == Role::Owner.as_str())
            .collect();
        if owners.len() == 1 && owners[0].subject == subject {
            return Err(Status::failed_precondition(format!(
                "{subject:?} is the last owner of {org:?}; make someone else owner first"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl OrgService for OrgSvc {
    async fn create_org(&self, request: Request<CreateOrgRequest>) -> Result<Response<CreateOrgResponse>, Status> {
        let p = self.caller(&request)?;
        if self.guard.org_creation_admin_only() && !self.is_admin(&p).await? {
            return Err(Status::permission_denied(
                "a system administrator must create organizations",
            ));
        }
        let req = request.into_inner();
        validate_org_name(&req.name).map_err(Status::invalid_argument)?;
        let visibility = visibility(&req.visibility)?;
        // The new owner would gain the repositories already under the name
        if self.has_repos(&req.name).await? && !self.is_admin(&p).await? {
            return Err(Status::permission_denied(format!(
                "repositories already exist under {:?}; a system administrator must create this organization",
                req.name
            )));
        }
        let now = now_ms();
        let org = OrgRecord {
            name: req.name.clone(),
            visibility,
            created_at_ms: now,
            created_by: p.subject.clone(),
        };
        self.meta.create_org(&org).await.map_err(status::from_store)?;
        self.meta
            .put_org_member(&OrgMember {
                org: req.name.clone(),
                subject: p.subject.clone(),
                role: Role::Owner.as_str().into(),
                added_at_ms: now,
                added_by: p.subject.clone(),
            })
            .await
            .map_err(status::from_store)?;
        self.guard.forget_org_changes(Some(&req.name), Some(&p.subject)).await;
        Ok(Response::new(CreateOrgResponse {
            org: Some(org_proto(org)),
        }))
    }

    async fn get_org(&self, request: Request<GetOrgRequest>) -> Result<Response<GetOrgResponse>, Status> {
        self.caller(&request)?;
        let org = self.org(&request.into_inner().name).await?;
        Ok(Response::new(GetOrgResponse {
            org: Some(org_proto(org)),
        }))
    }

    async fn list_orgs(&self, request: Request<ListOrgsRequest>) -> Result<Response<ListOrgsResponse>, Status> {
        let p = self.caller(&request)?;
        let mut orgs = self.meta.list_orgs().await.map_err(status::from_store)?;
        if request.into_inner().mine {
            let mine: Vec<String> = self
                .meta
                .list_memberships(&p.subject)
                .await
                .map_err(status::from_store)?
                .into_iter()
                .map(|m| m.org)
                .collect();
            orgs.retain(|o| mine.contains(&o.name));
        }
        Ok(Response::new(ListOrgsResponse {
            orgs: orgs.into_iter().map(org_proto).collect(),
        }))
    }

    async fn set_org_visibility(
        &self,
        request: Request<SetOrgVisibilityRequest>,
    ) -> Result<Response<SetOrgVisibilityResponse>, Status> {
        let p = self.caller(&request)?;
        let req = request.into_inner();
        let v = visibility(&req.visibility)?;
        self.org(&req.name).await?;
        self.require_owner(&p, &req.name).await?;
        self.meta
            .set_org_visibility(&req.name, &v)
            .await
            .map_err(status::from_store)?;
        self.guard.forget_org_changes(Some(&req.name), None).await;
        Ok(Response::new(SetOrgVisibilityResponse {
            org: Some(org_proto(self.org(&req.name).await?)),
        }))
    }

    async fn delete_org(&self, request: Request<DeleteOrgRequest>) -> Result<Response<DeleteOrgResponse>, Status> {
        let p = self.caller(&request)?;
        let name = request.into_inner().name;
        self.org(&name).await?;
        self.require_owner(&p, &name).await?;
        if self.has_repos(&name).await? {
            return Err(Status::failed_precondition(format!(
                "{name:?} still has repositories; delete them (and wait until they are purged) first"
            )));
        }
        self.meta.delete_org(&name).await.map_err(status::from_store)?;
        self.guard.forget_org_changes(Some(&name), None).await;
        Ok(Response::new(DeleteOrgResponse {}))
    }

    async fn put_member(&self, request: Request<PutMemberRequest>) -> Result<Response<PutMemberResponse>, Status> {
        let p = self.caller(&request)?;
        let req = request.into_inner();
        let role = Role::parse(&req.role).ok_or_else(|| {
            Status::invalid_argument(format!(
                "unknown role {:?}; use owner, maintainer, developer or reporter",
                req.role
            ))
        })?;
        if req.subject.trim().is_empty() || req.subject.contains('\n') {
            return Err(Status::invalid_argument("subject must be a non-empty single line"));
        }
        self.org(&req.org).await?;
        self.require_owner(&p, &req.org).await?;
        if role != Role::Owner {
            self.keep_an_owner(&req.org, &req.subject).await?;
        }
        let member = OrgMember {
            org: req.org.clone(),
            subject: req.subject.clone(),
            role: role.as_str().into(),
            added_at_ms: now_ms(),
            added_by: p.subject.clone(),
        };
        self.meta.put_org_member(&member).await.map_err(status::from_store)?;
        self.guard.forget_org_changes(Some(&req.org), Some(&req.subject)).await;
        let stored = self
            .meta
            .list_org_members(&req.org)
            .await
            .map_err(status::from_store)?
            .into_iter()
            .find(|m| m.subject == req.subject)
            .unwrap_or(member);
        Ok(Response::new(PutMemberResponse {
            member: Some(member_proto(stored)),
        }))
    }

    async fn remove_member(
        &self,
        request: Request<RemoveMemberRequest>,
    ) -> Result<Response<RemoveMemberResponse>, Status> {
        let p = self.caller(&request)?;
        let req = request.into_inner();
        self.org(&req.org).await?;
        // Members may leave on their own
        if p.subject != req.subject {
            self.require_owner(&p, &req.org).await?;
        }
        self.keep_an_owner(&req.org, &req.subject).await?;
        if !self
            .meta
            .delete_org_member(&req.org, &req.subject)
            .await
            .map_err(status::from_store)?
        {
            return Err(Status::not_found(format!(
                "{:?} is not a member of {:?}",
                req.subject, req.org
            )));
        }
        self.guard.forget_org_changes(Some(&req.org), Some(&req.subject)).await;
        Ok(Response::new(RemoveMemberResponse {}))
    }

    async fn list_members(
        &self,
        request: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        let p = self.caller(&request)?;
        let org = request.into_inner().org;
        self.org(&org).await?;
        if self.role(&org, &p.subject).await?.is_none() && !self.is_admin(&p).await? {
            return Err(Status::permission_denied(format!(
                "only members of {org:?} can list them"
            )));
        }
        let members = self.meta.list_org_members(&org).await.map_err(status::from_store)?;
        Ok(Response::new(ListMembersResponse {
            members: members.into_iter().map(member_proto).collect(),
        }))
    }
}
