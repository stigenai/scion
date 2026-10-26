//! 开启认证的服务端：PAT、starhub 风格的 HS256 JWT、换取访问令牌、按路径的 ACL、管理接口。

use std::sync::Arc;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_client::{Auth, Client, Code, ObjectId};
use scion_proto::v1::admin_service_client::AdminServiceClient;
use scion_proto::v1::auth_service_client::AuthServiceClient;
use scion_proto::v1::{
    self, CheckAccessRequest, CreateTokenRequest, ExchangeTokenRequest, GetPolicyRequest, ListTokensRequest,
    RevokeTokenRequest, SetPolicyRequest, WhoAmIRequest,
};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic_types::StatusExt;

const POLICY: &str = r#"
[[rule]]
subjects = ["group:eng"]
repos = ["mono"]
actions = ["land"]

[[rule]]
subjects = ["user:contractor"]
repos = ["mono"]
actions = ["write"]
paths = ["src/ui"]

[[rule]]
subjects = ["anonymous", "*"]
repos = ["public"]
actions = ["read"]

[[rule]]
subjects = ["user:root"]
repos = ["*"]
actions = ["admin"]
"#;

const AUTH: &str = r#"
[token]
issuer = "https://scion.test"

[[issuer]]
issuer = "OpenCSG"
hs256_secret = "signing-key"
subject_claim = "current_user"
"#;

struct Server {
    url: String,
    store: Arc<MemStore>,
    guard: Arc<Guard>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start() -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    store.create_repo("public").await.expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse(AUTH).expect("auth config"),
        signing_key_pem: Some(scion_api::scion_auth::token::generate_signing_key()),
    };
    // 同一个元数据库上的另一个 Guard：用来创建令牌、写入策略（相当于 scion admin 命令）
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
        blobs: store.clone(),
    };
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Server {
        url,
        store,
        guard,
        _stop: tx,
    }
}

async fn pat(s: &Server, subject: &str, groups: &[&str]) -> String {
    s.guard
        .create_token(
            "pat",
            subject,
            groups.iter().map(|g| (*g).to_string()).collect(),
            "test",
            None,
        )
        .await
        .expect("token")
        .0
}

async fn client(s: &Server, repo: &str, token: Option<&str>) -> Client {
    let auth = token.map_or(Auth::None, |t| Auth::Bearer(t.into()));
    Client::builder()
        .endpoint(&s.url)
        .repo(repo)
        .auth(auth)
        .build()
        .await
        .expect("connect")
}

fn starhub_jwt(user: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        + 3600;
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({"uuid": "u-1", "current_user": user, "iss": "OpenCSG", "exp": exp}),
        &EncodingKey::from_secret(b"signing-key"),
    )
    .expect("sign")
}

fn with_token<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).expect("header"),
    );
    r
}

async fn channel(s: &Server) -> Channel {
    tonic::transport::Endpoint::from_shared(s.url.clone())
        .expect("url")
        .connect()
        .await
        .expect("connect")
}

#[tokio::test(flavor = "multi_thread")]
async fn tokens_and_acl_are_enforced() {
    let s = start().await;
    let eng = pat(&s, "bob", &["eng"]).await;
    let c = client(&s, "mono", Some(&eng)).await;
    assert!(c.has_feature(scion_proto::features::AUTH_REQUIRED));
    assert!(!c.has_feature(scion_proto::features::AUTH_DEVICE));

    // eng 可以写入、合入
    let a = c
        .changeset(None)
        .put("src/ui/button.rs", "button")
        .put("src/core.rs", "core")
        .create()
        .await
        .expect("create");
    c.land("main", &a.id).await.expect("land");

    // 匿名：mono 拒绝，public 允许读
    let anon = client(&s, "mono", None).await;
    let e = anon.resolve_ref("main").await.expect_err("anonymous");
    assert_eq!(code(&e), Some(Code::Unauthenticated));
    let pub_anon = client(&s, "public", None).await;
    assert_eq!(pub_anon.list_refs().await.expect("public read"), []);
    // 无效令牌
    let bad = client(&s, "mono", Some("scn_pat_nonsense")).await;
    assert_eq!(
        code(&bad.list_refs().await.expect_err("bad token")),
        Some(Code::Unauthenticated)
    );

    // starhub 签发的 JWT：用户 bob 不在策略的 eng 组里（JWT 不带组），只能读 public
    let jwt = starhub_jwt("bob");
    let sh = client(&s, "mono", Some(&jwt)).await;
    let e = sh.list_refs().await.expect_err("not granted");
    assert_eq!(code(&e), Some(Code::PermissionDenied));
    assert_eq!(e.reason(), Some(v1::ErrorReason::PermissionDenied));
    assert_eq!(
        client(&s, "public", Some(&jwt)).await.list_refs().await.expect("ok"),
        []
    );

    // 只能写 src/ui 的外包
    let ct = pat(&s, "contractor", &[]).await;
    let cc = client(&s, "mono", Some(&ct)).await;
    let tip = c.resolve_ref("main").await.expect("ok").expect("main");
    let ok = cc
        .changeset(Some(tip.changeset))
        .put("src/ui/button.rs", "button v2")
        .create()
        .await;
    assert!(ok.is_ok(), "{ok:?}");
    let outside = cc
        .changeset(Some(tip.changeset))
        .put("src/core.rs", "hacked")
        .create()
        .await
        .expect_err("outside src/ui");
    assert_eq!(code(&outside), Some(Code::PermissionDenied));
    assert_eq!(
        code(&cc.land("main", &tip.changeset).await.expect_err("no land")),
        Some(Code::PermissionDenied)
    );
    // 读：可读路径之外的文件拒绝；按 id 读对象需要整个仓库的读权限；列目录只显示可读路径
    assert_eq!(
        cc.read_file("main", "src/ui/button.rs").await.expect("read").as_ref(),
        b"button"
    );
    assert_eq!(
        code(&cc.read_file("main", "src/core.rs").await.expect_err("denied")),
        Some(Code::PermissionDenied)
    );
    assert_eq!(
        code(&cc.get_object(&tip.root_tree).await.expect_err("objects")),
        Some(Code::PermissionDenied)
    );
    let root: Vec<String> = cc
        .list_dir("main", "")
        .await
        .expect("ls")
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(root, ["src"]);
    let src: Vec<String> = cc
        .list_dir("main", "src")
        .await
        .expect("ls")
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(src, ["ui"], "core.rs hidden");
    // 打包：整个仓库需要全部读权限，可读的子目录可以
    let mut sink = Vec::new();
    let err = cc
        .archive("main", "", scion_client::ArchiveFormat::Tar, "", &mut sink)
        .await
        .expect_err("whole repo");
    assert_eq!(code(&err), Some(Code::PermissionDenied));
    let (_, n) = cc
        .archive("main", "src/ui", scion_client::ArchiveFormat::Tar, "", &mut sink)
        .await
        .expect("readable subtree");
    assert!(n > 0 && sink.windows(16).any(|w| w == b"src/ui/button.rs"));
    // 最后提交：只列可见条目；看不到全部内容的目录（src）不给出提交
    let last = cc.last_commits("main", "", 0, 0).await.expect("last commits");
    assert_eq!((last.total, last.entries.len()), (1, 1));
    assert_eq!(last.entries[0], ("src".to_string(), None));
    assert!(last.commits.is_empty());
    let last = cc.last_commits("main", "src", 0, 0).await.expect("last commits");
    assert_eq!(last.entries.len(), 1);
    assert_eq!(last.entries[0].0, "ui");
    assert!(last.entries[0].1.is_some());
    assert_eq!(last.commits.len(), 1);
    // 统计：整个仓库需要全部读权限，可读的子目录可以
    assert_eq!(
        code(&cc.tree_stats("main", "").await.expect_err("whole repo")),
        Some(Code::PermissionDenied)
    );
    assert_eq!(cc.tree_stats("main", "src/ui").await.expect("stats").files, 1);
    assert_eq!(
        code(&cc.tree_stats("main", "src").await.expect_err("src has hidden files")),
        Some(Code::PermissionDenied)
    );
    let changed = cc.diff(None, "main", None).await.expect("diff");
    assert!(changed.iter().all(|f| f.path.starts_with("src/ui")), "{changed:?}");
    assert_eq!(cc.log("main", None, 10, None).await.expect("log").entries.len(), 1);
    // log --follow: a rename from a path the caller cannot read ends the history there
    let mv = c
        .changeset(Some(tip.changeset))
        .delete("src/core.rs")
        .put("src/ui/core.rs", "core")
        .create()
        .await
        .expect("move");
    c.land("main", &mv.id).await.expect("land");
    let page = c.log_follow("main", "src/ui/core.rs", 10, None).await.expect("follow");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.renamed_from.as_deref(), Some("src/core.rs"));
    let older = c
        .log_follow("main", "src/core.rs", 10, page.next_page_token.as_deref())
        .await
        .expect("older");
    assert_eq!((older.entries.len(), older.renamed_from), (1, None));
    let page = cc.log_follow("main", "src/ui/core.rs", 10, None).await.expect("follow");
    assert_eq!(page.entries.len(), 1);
    assert_eq!((page.renamed_from, page.next_page_token), (None, None));
    // 上传对象（写入前的准备）只需在仓库里有写权限
    assert!(cc.missing_objects(&[ObjectId::of_blob(b"x")]).await.is_ok());
    drop(s.store);
}

fn code(e: &scion_client::Error) -> Option<Code> {
    match e {
        scion_client::Error::Rpc { code, .. } => Some(*code),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_and_admin_services() {
    let s = start().await;
    let ch = channel(&s).await;
    let mut auth = AuthServiceClient::new(ch.clone());

    // 用 starhub 的 JWT 换取本服务签发的访问令牌
    let ex = auth
        .exchange_token(ExchangeTokenRequest {
            subject_token: starhub_jwt("alice"),
        })
        .await
        .expect("exchange")
        .into_inner();
    assert_eq!(ex.subject, "alice");
    assert!(ex.expires_at_ms > 0);
    let me = auth
        .who_am_i(with_token(WhoAmIRequest {}, &ex.access_token))
        .await
        .expect("whoami")
        .into_inner();
    assert_eq!((me.subject.as_str(), me.auth_required), ("alice", true));
    let anon = auth.who_am_i(WhoAmIRequest {}).await.expect("anonymous").into_inner();
    assert_eq!(anon.subject, "anonymous");
    let err = auth
        .exchange_token(ExchangeTokenRequest {
            subject_token: "scn_pat_unknown".into(),
        })
        .await
        .expect_err("bad credential");
    assert_eq!(err.code(), Code::Unauthenticated);
    let check = |action: v1::Action, repo: &str, path: &str| CheckAccessRequest {
        repo: repo.into(),
        action: action.into(),
        r#ref: String::new(),
        path: path.into(),
    };
    let ct = pat(&s, "contractor", &[]).await;
    let allowed = |r: tonic::Response<v1::CheckAccessResponse>| r.into_inner().allowed;
    assert!(allowed(
        auth.check_access(with_token(check(v1::Action::Write, "mono", "src/ui/x"), &ct))
            .await
            .expect("check")
    ));
    assert!(!allowed(
        auth.check_access(with_token(check(v1::Action::Write, "mono", "src/x"), &ct))
            .await
            .expect("check")
    ));
    let missing_action = auth
        .check_access(with_token(check(v1::Action::Unspecified, "mono", ""), &ct))
        .await
        .expect_err("action required");
    assert_eq!(missing_action.code(), Code::InvalidArgument);
    let no_repo = auth
        .check_access(with_token(check(v1::Action::Read, "", ""), &ct))
        .await
        .expect_err("repo required");
    assert_eq!(no_repo.code(), Code::InvalidArgument);
    // 没有配置 OIDC：device login 不可用
    let dl = auth
        .start_device_login(v1::StartDeviceLoginRequest {})
        .await
        .expect_err("not configured");
    assert_eq!(dl.code(), Code::Unavailable);
    let poll = auth
        .poll_device_login(v1::PollDeviceLoginRequest {
            device_code: String::new(),
        })
        .await
        .expect_err("code required");
    assert_eq!(poll.code(), Code::InvalidArgument);

    // 管理接口：只有系统管理员
    let mut admin = AdminServiceClient::new(ch);
    let denied = admin
        .get_policy(with_token(GetPolicyRequest {}, &ct))
        .await
        .expect_err("not admin");
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_eq!(
        denied.get_error_details().error_info().map(|i| i.reason.clone()),
        Some("PERMISSION_DENIED".into())
    );
    assert_eq!(
        admin
            .get_policy(GetPolicyRequest {})
            .await
            .expect_err("anonymous")
            .code(),
        Code::Unauthenticated
    );
    let root = pat(&s, "root", &[]).await;
    let policy = admin
        .get_policy(with_token(GetPolicyRequest {}, &root))
        .await
        .expect("get")
        .into_inner()
        .policy;
    assert_eq!(policy, POLICY);
    let created = admin
        .create_token(with_token(
            CreateTokenRequest {
                subject: "ci".into(),
                groups: vec!["eng".into()],
                description: "CI".into(),
                ttl_seconds: 3600,
            },
            &root,
        ))
        .await
        .expect("create")
        .into_inner();
    let info = created.info.expect("info");
    assert!(created.token.starts_with("scn_pat_"));
    assert!(info.expires_at_ms > info.created_at_ms);
    let ci = client(&s, "mono", Some(&created.token)).await;
    ci.list_refs().await.expect("new token works");
    let listed = admin
        .list_tokens(with_token(ListTokensRequest { subject: "ci".into() }, &root))
        .await
        .expect("list")
        .into_inner()
        .tokens;
    assert_eq!(listed.len(), 1);
    let all = admin
        .list_tokens(with_token(ListTokensRequest::default(), &root))
        .await
        .expect("list")
        .into_inner()
        .tokens;
    assert!(all.len() >= 3);
    let revoked = admin
        .revoke_token(with_token(RevokeTokenRequest { id: info.id }, &root))
        .await
        .expect("revoke")
        .into_inner();
    assert!(revoked.revoked);
    assert!(ci.list_refs().await.is_err(), "revoked token rejected");
    let rules = admin
        .set_policy(with_token(
            SetPolicyRequest {
                policy: "[[rule]]\nsubjects=[\"*\"]\nrepos=[\"*\"]\nactions=[\"admin\"]\n".into(),
            },
            &root,
        ))
        .await
        .expect("set")
        .into_inner()
        .rules;
    assert_eq!(rules, 1);
    let bad = admin
        .set_policy(with_token(
            SetPolicyRequest {
                policy: "nope [".into(),
            },
            &root,
        ))
        .await
        .expect_err("invalid");
    assert_eq!(bad.code(), Code::InvalidArgument);
    // 新策略立即生效：contractor 现在是管理员
    admin
        .get_policy(with_token(GetPolicyRequest {}, &ct))
        .await
        .expect("now admin");
}

/// 库里的策略加载失败：自签/外部 JWT 不查库也能验证，但服务端拒绝一切（Unavailable），
/// 不退回到「没配策略」的默认放行；策略修好后自动恢复。
#[tokio::test(flavor = "multi_thread")]
async fn unloadable_policy_denies_until_fixed() {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    store
        .put_setting(scion_api::scion_auth::POLICY_KEY, b"broken [")
        .await
        .expect("policy");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth: GuardConfig {
                required: true,
                auth: AuthConfig::parse(AUTH).expect("auth config"),
                signing_key_pem: None,
            },
            policy_poll_interval: std::time::Duration::from_secs(3600),
            warm_record_cache: false,
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store.clone(),
    };
    let (_stop, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    let c = Client::builder()
        .endpoint(&url)
        .repo("mono")
        .auth(Auth::Bearer(starhub_jwt("alice")))
        .build()
        .await
        .expect("connect");
    let err = c.list_refs().await.expect_err("policy not loaded");
    assert_eq!(code(&err), Some(Code::Unavailable), "{err:?}");

    // 修好策略：不等轮询周期（1 小时），未加载状态下每秒重试
    store
        .put_setting(
            scion_api::scion_auth::POLICY_KEY,
            b"[[rule]]\nsubjects = [\"user:alice\"]\nrepos = [\"mono\"]\nactions = [\"read\"]\n",
        )
        .await
        .expect("fix");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match c.list_refs().await {
            Ok(_) => break,
            Err(e) if tokio::time::Instant::now() < deadline => {
                assert_eq!(code(&e), Some(Code::Unavailable), "{e:?}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => panic!("policy never loaded: {e:?}"),
        }
    }
}
