//! OrgService on a server whose authorization chain includes orgs and personal namespaces.

use std::sync::Arc;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_client::{Auth, Client, NewRepo};
use scion_proto::v1::auth_service_client::AuthServiceClient;
use scion_proto::v1::org_service_client::OrgServiceClient;
use scion_proto::v1::{
    Action, CheckAccessRequest, CreateOrgRequest, DeleteOrgRequest, GetOrgRequest, ListMembersRequest, ListOrgsRequest,
    PutMemberRequest, RemoveMemberRequest, SetOrgVisibilityRequest,
};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::Code;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const POLICY: &str = r#"
[[rule]]
subjects = ["user:root"]
repos = ["*"]
actions = ["admin"]
"#;

struct Server {
    url: String,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start(chain: &str) -> Server {
    start_with_config(&format!("[authorization]\nchain = {chain}")).await
}

async fn start_with_config(config: &str) -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("shared/old").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse(config).expect("auth config"),
        signing_key_pem: None,
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    guard.set_policy(POLICY).await.expect("policy");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            warm_record_cache: false,
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Server { url, guard, _stop: tx }
}

async fn pat(s: &Server, subject: &str) -> String {
    s.guard
        .create_token("pat", subject, vec![], "test", None)
        .await
        .expect("token")
        .0
}

fn as_user<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    r
}

async fn orgs(s: &Server) -> OrgServiceClient<Channel> {
    OrgServiceClient::connect(s.url.clone()).await.expect("connect")
}

fn code<T>(r: Result<T, tonic::Status>) -> Code {
    r.err().map_or(Code::Ok, |e| e.code())
}

#[tokio::test(flavor = "multi_thread")]
async fn organizations_are_self_managed() {
    let s = start(r#"["policy", "orgs", "personal"]"#).await;
    let (alice, bob, eve, root) = (
        pat(&s, "alice").await,
        pat(&s, "bob").await,
        pat(&s, "eve").await,
        pat(&s, "root").await,
    );
    let mut o = orgs(&s).await;
    let create = |name: &str, visibility: &str| CreateOrgRequest {
        name: name.into(),
        visibility: visibility.into(),
    };
    let put = |org: &str, subject: &str, role: &str| PutMemberRequest {
        org: org.into(),
        subject: subject.into(),
        role: role.into(),
    };
    let remove = |org: &str, subject: &str| RemoveMemberRequest {
        org: org.into(),
        subject: subject.into(),
    };
    let members = |org: &str| ListMembersRequest { org: org.into() };

    // Anyone signed in creates an organization and becomes its owner
    assert_eq!(
        code(o.create_org(tonic::Request::new(create("acme", ""))).await),
        Code::Unauthenticated
    );
    let org = o
        .create_org(as_user(create("acme", ""), &alice))
        .await
        .expect("create")
        .into_inner()
        .org
        .expect("org");
    assert_eq!((org.visibility.as_str(), org.created_by.as_str()), ("private", "alice"));
    assert_eq!(
        code(o.create_org(as_user(create("acme", ""), &bob)).await),
        Code::AlreadyExists
    );
    assert_eq!(
        code(o.create_org(as_user(create("user", ""), &bob)).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code(o.create_org(as_user(create("x y", ""), &bob)).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code(o.create_org(as_user(create("z", "public"), &bob)).await),
        Code::InvalidArgument
    );
    // Repositories already exist under shared/: only a system administrator
    assert_eq!(
        code(o.create_org(as_user(create("shared", ""), &bob)).await),
        Code::PermissionDenied
    );
    o.create_org(as_user(create("shared", "internal"), &root))
        .await
        .expect("root may");
    let got = o
        .get_org(as_user(GetOrgRequest { name: "acme".into() }, &eve))
        .await
        .expect("get");
    assert_eq!(got.into_inner().org.map(|o| o.name).as_deref(), Some("acme"));
    assert_eq!(
        code(o.get_org(as_user(GetOrgRequest { name: "nope".into() }, &eve)).await),
        Code::NotFound
    );
    let all = o
        .list_orgs(as_user(ListOrgsRequest { mine: false }, &eve))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(all.orgs.len(), 2);
    let mine = o
        .list_orgs(as_user(ListOrgsRequest { mine: true }, &alice))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(mine.orgs.iter().map(|o| o.name.as_str()).collect::<Vec<_>>(), ["acme"]);

    // Owners manage members; others cannot
    assert_eq!(
        code(o.list_members(as_user(members("acme"), &bob)).await),
        Code::PermissionDenied
    );
    assert_eq!(
        code(o.put_member(as_user(put("acme", "bob", "developer"), &bob)).await),
        Code::PermissionDenied
    );
    o.put_member(as_user(put("acme", "bob", "developer"), &alice))
        .await
        .expect("add bob");
    let listed = o
        .list_members(as_user(members("acme"), &bob))
        .await
        .expect("bob lists")
        .into_inner();
    assert_eq!(
        listed
            .members
            .iter()
            .map(|m| (m.subject.as_str(), m.role.as_str()))
            .collect::<Vec<_>>(),
        [("alice", "owner"), ("bob", "developer")]
    );
    o.list_members(as_user(members("acme"), &root))
        .await
        .expect("root lists");
    assert_eq!(
        code(o.put_member(as_user(put("acme", "bob", "admin"), &alice)).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code(o.put_member(as_user(put("acme", " ", "owner"), &alice)).await),
        Code::InvalidArgument
    );
    assert_eq!(
        code(o.put_member(as_user(put("nope", "bob", "owner"), &alice)).await),
        Code::NotFound
    );
    // The last owner stays
    assert_eq!(
        code(o.put_member(as_user(put("acme", "alice", "reporter"), &alice)).await),
        Code::FailedPrecondition
    );
    assert_eq!(
        code(o.remove_member(as_user(remove("acme", "alice"), &alice)).await),
        Code::FailedPrecondition
    );

    // Roles authorize repository work right away
    let check = |token: String, repo: &str, action: Action| {
        let (url, repo) = (s.url.clone(), repo.to_owned());
        async move {
            let mut a = AuthServiceClient::connect(url).await.expect("connect");
            let req = CheckAccessRequest {
                repo,
                action: action as i32,
                r#ref: String::new(),
                path: String::new(),
            };
            a.check_access(as_user(req, &token))
                .await
                .expect("check")
                .into_inner()
                .allowed
        }
    };
    assert!(check(bob.clone(), "acme/app", Action::Write).await);
    assert!(!check(bob.clone(), "acme/app", Action::Land).await);
    assert!(!check(eve.clone(), "acme/app", Action::Read).await, "private");
    assert!(check(eve.clone(), "shared/old", Action::Read).await, "internal");
    assert!(
        check(eve.clone(), "user/eve/notes", Action::Admin).await,
        "personal namespace"
    );
    o.put_member(as_user(put("acme", "bob", "maintainer"), &alice))
        .await
        .expect("promote");
    assert!(
        check(bob.clone(), "acme/app", Action::Land).await,
        "no waiting for the cache"
    );
    let client = Client::builder()
        .endpoint(&s.url)
        .auth(Auth::Bearer(alice.clone()))
        .build()
        .await
        .expect("connect");
    client
        .create_repo("acme/app", NewRepo::default())
        .await
        .expect("an owner creates repositories");
    assert!(client.create_repo("other/app", NewRepo::default()).await.is_err());
    o.set_org_visibility(as_user(
        SetOrgVisibilityRequest {
            name: "acme".into(),
            visibility: "internal".into(),
        },
        &alice,
    ))
    .await
    .expect("visibility");
    assert!(check(eve.clone(), "acme/app", Action::Read).await);
    assert_eq!(
        code(
            o.set_org_visibility(as_user(
                SetOrgVisibilityRequest {
                    name: "acme".into(),
                    visibility: "private".into(),
                },
                &bob,
            ))
            .await
        ),
        Code::PermissionDenied
    );

    // Members leave, owners remove them
    o.put_member(as_user(put("acme", "eve", "reporter"), &alice))
        .await
        .expect("add eve");
    o.remove_member(as_user(remove("acme", "eve"), &eve))
        .await
        .expect("eve leaves");
    assert_eq!(
        code(o.remove_member(as_user(remove("acme", "eve"), &alice)).await),
        Code::NotFound
    );
    o.remove_member(as_user(remove("acme", "bob"), &alice))
        .await
        .expect("remove bob");
    assert!(!check(bob.clone(), "acme/app", Action::Write).await);

    // An organization with repositories cannot be deleted
    let delete = |name: &str| DeleteOrgRequest { name: name.into() };
    assert_eq!(
        code(o.delete_org(as_user(delete("acme"), &bob)).await),
        Code::PermissionDenied
    );
    assert_eq!(
        code(o.delete_org(as_user(delete("acme"), &alice)).await),
        Code::FailedPrecondition
    );
    o.create_org(as_user(create("tmp", ""), &bob)).await.expect("create");
    o.delete_org(as_user(delete("tmp"), &bob)).await.expect("delete");
    assert_eq!(code(o.delete_org(as_user(delete("tmp"), &bob)).await), Code::NotFound);
}

#[tokio::test(flavor = "multi_thread")]
async fn without_orgs_in_the_chain_the_service_says_so() {
    let s = start(r#"["policy"]"#).await;
    let alice = pat(&s, "alice").await;
    let err = orgs(&s)
        .await
        .list_orgs(as_user(ListOrgsRequest { mine: false }, &alice))
        .await
        .expect_err("not enabled");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("[authorization] chain"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_only_creation_preserves_member_management() {
    let s = start_with_config(
        r#"
[authorization]
chain = ["policy", "orgs"]
org_creation_admin_only = true
"#,
    )
    .await;
    let root = pat(&s, "root").await;
    let alice = pat(&s, "alice").await;
    let mut o = orgs(&s).await;
    let create = || CreateOrgRequest {
        name: "acme".into(),
        visibility: "private".into(),
    };
    assert_eq!(code(o.create_org(create()).await), Code::Unauthenticated);
    assert_eq!(
        code(o.create_org(as_user(create(), &alice)).await),
        Code::PermissionDenied
    );
    let org = o
        .create_org(as_user(create(), &root))
        .await
        .expect("system admin creates")
        .into_inner()
        .org
        .expect("org");
    assert_eq!(org.created_by, "root");
    o.put_member(as_user(
        PutMemberRequest {
            org: "acme".into(),
            subject: "alice".into(),
            role: "owner".into(),
        },
        &root,
    ))
    .await
    .expect("assign owner");
    o.put_member(as_user(
        PutMemberRequest {
            org: "acme".into(),
            subject: "bob".into(),
            role: "developer".into(),
        },
        &alice,
    ))
    .await
    .expect("org owner still manages members");
    assert_eq!(
        code(
            o.create_org(as_user(
                CreateOrgRequest {
                    name: "other".into(),
                    visibility: "".into()
                },
                &alice
            ))
            .await
        ),
        Code::PermissionDenied,
        "org owner is not a system admin"
    );
    let all = o
        .list_orgs(as_user(ListOrgsRequest { mine: false }, &root))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(all.orgs.len(), 1, "denied calls must not create organizations");
}
