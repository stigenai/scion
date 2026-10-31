//! Exercise the actual provider selected by Scion without changing process-global env.
use super::*;
use object_store::ClientOptions;
use object_store::client::{HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService};
use std::sync::Mutex;

#[derive(Debug, Clone, Default)]
struct Sts(Arc<Mutex<Vec<String>>>);

impl HttpConnector for Sts {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl HttpService for Sts {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        assert_eq!(request.uri().host(), Some("sts.us-east-1.amazonaws.com"));
        assert_eq!(request.method(), "POST");
        let query = request.uri().query().expect("STS parameters").to_owned();
        assert!(query.contains("Action=AssumeRoleWithWebIdentity"));
        assert!(query.contains("RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Ffixture"));
        let mut calls = self.0.lock().expect("calls");
        calls.push(query);
        let n = calls.len();
        // Already-expired first credentials force a refresh without sleeps or clock mocks.
        let expires = if n == 1 {
            "2000-01-01T00:00:00Z"
        } else {
            "2100-01-01T00:00:00Z"
        };
        Ok(HttpResponse::new(format!("<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>key{n}</AccessKeyId><SecretAccessKey>secret{n}</SecretAccessKey><SessionToken>session{n}</SessionToken><Expiration>{expires}</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>").into()))
    }
}

#[tokio::test]
async fn aws_web_identity_refreshes_and_rejects_empty_static_keys() {
    let mode = std::env::var("SCION_CREDENTIAL_TEST_CHILD").ok();
    if mode.is_none() {
        let token = tempfile::NamedTempFile::new().expect("token file");
        for mode in ["web", "empty"] {
            let mut command = std::process::Command::new(std::env::current_exe().expect("test binary"));
            for (key, _) in std::env::vars().filter(|(k, _)| k.starts_with("AWS_")) {
                command.env_remove(key);
            }
            command
                .args([
                    "--exact",
                    "credential_tests::aws_web_identity_refreshes_and_rejects_empty_static_keys",
                    "--nocapture",
                ])
                .env("SCION_CREDENTIAL_TEST_CHILD", mode)
                .env("AWS_WEB_IDENTITY_TOKEN_FILE", token.path())
                .env("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/fixture");
            if mode == "empty" {
                command.env("AWS_ACCESS_KEY_ID", "").env("AWS_SECRET_ACCESS_KEY", "");
            }
            assert!(
                command.status().expect("child test").success(),
                "credential case {mode}"
            );
        }
        return;
    }
    let cfg = S3Config::default();
    if mode.as_deref() == Some("empty") {
        assert!(matches!(s3_credentials(&cfg), Err(Error::Backend(m)) if m.contains("empty AWS static credentials")));
        return;
    }
    let path = std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").expect("token path");
    std::fs::write(&path, "first").expect("token");
    let sts = Sts::default();
    let store = s3_credentials(&cfg)
        .expect("credentials")
        .with_bucket_name("fixture")
        .with_region("us-east-1")
        .with_http_connector(sts.clone())
        .build()
        .expect("store");
    let first = store.credentials().get_credential().await.expect("first credentials");
    assert_eq!(first.key_id, "key1");
    assert_eq!(first.token.as_deref(), Some("session1"));
    std::fs::write(&path, "rotated").expect("rotate projected token");
    let second = store
        .credentials()
        .get_credential()
        .await
        .expect("refreshed credentials");
    assert_eq!(second.key_id, "key2");
    assert_eq!(second.secret_key, "secret2");
    assert_eq!(second.token.as_deref(), Some("session2"));
    assert_eq!(
        store.credentials().get_credential().await.expect("cached").key_id,
        "key2"
    );
    let calls = sts.0.lock().expect("calls");
    assert_eq!(calls.len(), 2);
    assert!(calls[0].contains("WebIdentityToken=first"));
    assert!(calls[1].contains("WebIdentityToken=rotated"));
}

#[tokio::test]
async fn explicit_credentials_and_session_tokens_are_preserved() {
    for token in [None, Some("temporary-session".to_owned())] {
        let cfg = S3Config {
            access_key_id: "local-key".into(),
            secret_access_key: "local-secret".into(),
            session_token: token.clone(),
            ..Default::default()
        };
        let store = s3_credentials(&cfg)
            .expect("credentials")
            .with_bucket_name("fixture")
            .with_region("us-east-1")
            .with_http_connector(Sts::default())
            .build()
            .expect("store");
        let credentials = store.credentials().get_credential().await.expect("static credentials");
        assert_eq!(credentials.key_id, "local-key");
        assert_eq!(credentials.secret_key, "local-secret");
        assert_eq!(credentials.token, token);
        assert!(!format!("{cfg:?}").contains("local-secret"));
        assert!(!format!("{cfg:?}").contains("temporary-session"));
    }
    for cfg in [
        S3Config {
            access_key_id: "only-key".into(),
            ..Default::default()
        },
        S3Config {
            secret_access_key: "only-secret".into(),
            ..Default::default()
        },
        S3Config {
            session_token: Some("only-token".into()),
            ..Default::default()
        },
    ] {
        assert!(s3_credentials(&cfg).is_err(), "partial credentials must not fall back");
    }
}
