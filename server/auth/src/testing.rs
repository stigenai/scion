//! 测试用的身份提供方：OIDC 发现文档、JWKS、device flow、RFC 7662 内省、OPA 风格的鉴权回调。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::extract::{Form, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};

use crate::token::{Issuer, generate_signing_key};

#[derive(Default)]
struct Shared {
    base: String,
    pem: String,
    kid: String,
    jwks: Value,
    approved: HashSet<String>,
    calls: HashMap<String, usize>,
    /// Introspection answers that replace the built-in ones, by token.
    introspection: HashMap<String, Value>,
    /// An authorization answer that replaces the built-in ones.
    authz: Option<Value>,
}

/// 测试用的身份提供方。
#[derive(Clone)]
pub struct FakeIdp {
    shared: Arc<Mutex<Shared>>,
}

type St = Arc<Mutex<Shared>>;

fn count(st: &St, uri: &Uri) {
    if let Ok(mut s) = st.lock() {
        *s.calls.entry(uri.path().to_owned()).or_default() += 1;
    }
}

fn base(st: &St) -> String {
    st.lock().map(|s| s.base.clone()).unwrap_or_default()
}

async fn discovery(State(st): State<St>, uri: Uri) -> Json<Value> {
    count(&st, &uri);
    let b = base(&st);
    Json(json!({
        "issuer": b,
        "device_authorization_endpoint": format!("{b}/device"),
        "token_endpoint": format!("{b}/token"),
        "jwks_uri": format!("{b}/jwks"),
    }))
}

async fn no_device(State(st): State<St>) -> Json<Value> {
    let b = base(&st);
    Json(json!({"issuer": b, "token_endpoint": format!("{b}/token"), "jwks_uri": format!("{b}/jwks")}))
}

async fn jwks(State(st): State<St>, uri: Uri) -> Json<Value> {
    count(&st, &uri);
    Json(st.lock().map(|s| s.jwks.clone()).unwrap_or_default())
}

async fn device(State(st): State<St>, uri: Uri) -> Json<Value> {
    count(&st, &uri);
    Json(json!({
        "device_code": "dc-1",
        "user_code": "ABCD-EFGH",
        "verification_uri": format!("{}/activate", base(&st)),
        "interval": 1,
        "expires_in": 600,
    }))
}

fn error(code: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": code}))).into_response()
}

async fn token(State(st): State<St>, uri: Uri, Form(form): Form<HashMap<String, String>>) -> Response {
    count(&st, &uri);
    let dc = form.get("device_code").cloned().unwrap_or_default();
    match dc.as_str() {
        "slow" => return error("slow_down"),
        "denied" => return error("access_denied"),
        "expired" => return error("expired_token"),
        "weird" => return error("server_error"),
        "no-id-token" => return Json(json!({"access_token": "x"})).into_response(),
        _ => {}
    }
    let (approved, b, pem, kid) = match st.lock() {
        Ok(s) => (s.approved.contains(&dc), s.base.clone(), s.pem.clone(), s.kid.clone()),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if !approved {
        return error("authorization_pending");
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let claims = json!({
        "iss": b, "aud": "scn", "sub": "u-123", "preferred_username": "alice",
        "groups": ["eng"], "iat": now, "exp": now + 300,
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid);
    let id_token = jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_ec_pem(pem.as_bytes()).expect("test key"),
    )
    .expect("sign");
    Json(json!({"id_token": id_token, "access_token": "at"})).into_response()
}

async fn gh_device(State(st): State<St>, uri: Uri) -> Json<Value> {
    count(&st, &uri);
    Json(json!({
        "device_code": "gh-dc",
        "user_code": "WXYZ-1234",
        "verification_uri": format!("{}/gh/login/device", base(&st)),
        "interval": 1,
        "expires_in": 900,
    }))
}

/// GitHub answers 200 with an `error` field while the device flow is pending.
async fn gh_token(State(st): State<St>, uri: Uri, Form(form): Form<HashMap<String, String>>) -> Json<Value> {
    count(&st, &uri);
    let dc = form.get("device_code").cloned().unwrap_or_default();
    Json(match dc.as_str() {
        "slow" => json!({"error": "slow_down"}),
        "denied" => json!({"error": "access_denied"}),
        "expired" => json!({"error": "expired_token"}),
        "weird" => json!({"error": "incorrect_client_credentials"}),
        "no-token" => json!({}),
        "bad-token" => json!({"access_token": "bad"}),
        "no-login" => json!({"access_token": "nologin"}),
        dc if st.lock().is_ok_and(|s| s.approved.contains(dc)) => json!({"access_token": "gho_octocat"}),
        _ => json!({"error": "authorization_pending"}),
    })
}

async fn gh_user(State(st): State<St>, uri: Uri, headers: HeaderMap) -> Response {
    count(&st, &uri);
    if headers.get("user-agent").is_none() {
        return StatusCode::FORBIDDEN.into_response();
    }
    match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        Some("Bearer gho_octocat") => Json(json!({"login": "octocat", "id": 1})).into_response(),
        Some("Bearer nologin") => Json(json!({"id": 2})).into_response(),
        _ => StatusCode::UNAUTHORIZED.into_response(),
    }
}

/// GitHub organization lists: `opencsg` has octocat (spelled Octocat) and alice, and a team `core`
/// with octocat; `big` has 150 members, u0 to u149, served in pages.
async fn gh_list(State(st): State<St>, uri: Uri, headers: HeaderMap) -> Response {
    let q: HashMap<&str, &str> = uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .collect();
    count(&st, &uri);
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer ght") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let users = |names: &[&str]| names.iter().map(|n| json!({"login": n})).collect::<Vec<_>>();
    let all: Vec<Value> = match uri.path() {
        "/gh/orgs/opencsg/members" => users(&["Octocat", "alice"]),
        "/gh/orgs/opencsg/teams" => vec![json!({"slug": "core"})],
        "/gh/orgs/opencsg/teams/core/members" => users(&["octocat"]),
        "/gh/orgs/big/members" => (0..150).map(|i| json!({"login": format!("u{i}")})).collect(),
        "/gh/orgs/big/teams" => Vec::new(),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let num = |k: &str, d: usize| q.get(k).and_then(|v| v.parse().ok()).unwrap_or(d);
    let (per, page) = (num("per_page", 30), num("page", 1));
    Json(all.into_iter().skip(per * (page - 1)).take(per).collect::<Vec<_>>()).into_response()
}

async fn introspect(
    State(st): State<St>,
    uri: Uri,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    count(&st, &uri);
    // scion:s3cret
    let expected = "Basic c2Npb246czNjcmV0";
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(expected) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let set = form
        .get("token")
        .and_then(|t| st.lock().ok().and_then(|s| s.introspection.get(t).cloned()));
    let body = match form.get("token").map(String::as_str) {
        _ if set.is_some() => set.unwrap_or_default(),
        Some("good-token") => json!({"active": true, "username": "alice", "groups": ["eng"]}),
        Some("sub-only") => json!({"active": true, "sub": "bob"}),
        _ => json!({"active": false}),
    };
    Json(body).into_response()
}

async fn authz(State(st): State<St>, uri: Uri, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    count(&st, &uri);
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer opa") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Some(v) = st.lock().ok().and_then(|s| s.authz.clone()) {
        return Json(v).into_response();
    }
    let i = &body["input"];
    let out = match (i["subject"].as_str(), i["repo"].as_str(), i["action"].as_str()) {
        (Some("anonymous"), Some("public"), Some("read")) => json!({"result": {"allow": true}}),
        (Some("anonymous"), _, _) => json!({}),
        (_, Some("models/a"), Some("read")) => json!({"result": true}),
        _ => json!({"result": {"allow": false}}),
    };
    Json(out).into_response()
}

impl FakeIdp {
    pub async fn start() -> Self {
        let pem = generate_signing_key();
        let issuer = Issuer::from_pem(&pem, "fake", 60).expect("key");
        let jwk = issuer.jwk().clone();
        let shared = Arc::new(Mutex::new(Shared {
            pem,
            kid: jwk.kid.clone(),
            jwks: json!({"keys": [jwk]}),
            ..Shared::default()
        }));
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/no-device/.well-known/openid-configuration", get(no_device))
            .route("/jwks", get(jwks))
            .route("/device", post(device))
            .route("/token", post(token))
            .route("/introspect", post(introspect))
            .route("/authz", post(authz))
            .route("/gh/login/device/code", post(gh_device))
            .route("/gh/login/oauth/access_token", post(gh_token))
            .route("/gh/user", get(gh_user))
            .route("/gh/orgs/{org}/members", get(gh_list))
            .route("/gh/orgs/{org}/teams", get(gh_list))
            .route("/gh/orgs/{org}/teams/{team}/members", get(gh_list))
            .route("/status-500", post(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
            .route("/not-json", post(|| async { "plain text" }))
            .route(
                "/not-json/.well-known/openid-configuration",
                get(|| async { "plain text" }),
            )
            .with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        if let Ok(mut s) = shared.lock() {
            s.base = base;
        }
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { shared }
    }

    pub fn base(&self) -> String {
        base(&self.shared)
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base())
    }

    pub fn calls(&self, path: &str) -> usize {
        self.shared
            .lock()
            .map(|s| s.calls.get(path).copied().unwrap_or(0))
            .unwrap_or(0)
    }

    /// From now on `/introspect` answers `body` for `token`.
    pub fn set_introspection(&self, token: &str, body: Value) {
        if let Ok(mut s) = self.shared.lock() {
            s.introspection.insert(token.to_owned(), body);
        }
    }

    /// From now on `/authz` answers `body` for every request.
    pub fn set_authz(&self, body: Value) {
        if let Ok(mut s) = self.shared.lock() {
            s.authz = Some(body);
        }
    }

    pub fn approve(&self, device_code: &str) {
        if let Ok(mut s) = self.shared.lock() {
            s.approved.insert(device_code.to_owned());
        }
    }

    /// 签一个本 IdP 的令牌（JWKS 里的密钥）。
    pub fn sign(&self, claims: &Value) -> String {
        let (pem, kid) = self
            .shared
            .lock()
            .map(|s| (s.pem.clone(), s.kid.clone()))
            .unwrap_or_default();
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(kid);
        jsonwebtoken::encode(&header, claims, &EncodingKey::from_ec_pem(pem.as_bytes()).expect("key")).expect("sign")
    }
}
