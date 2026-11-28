use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;

use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
use scion_api::{App, ServerConfig, serve};
use scion_store::{MemStore, MetaStore};
use serde_json::Value;
use tokio::net::TcpListener;

async fn server(subject: &str, oidc_issuer: Option<String>) -> (String, String, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let config = oidc_issuer
        .map(|issuer| {
            format!("[oidc]\nissuer = \"{issuer}\"\nclient_id = \"scn\"\nsubject_claim = \"preferred_username\"\n")
        })
        .unwrap_or_default();
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse(&config).expect("auth"),
        signing_key_pem: Some(scion_api::scion_auth::token::generate_signing_key()),
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    let (pat, _) = guard
        .create_token("pat", subject, vec!["admins".into()], "", None)
        .await
        .expect("pat");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store.clone(),
    };
    tokio::spawn(serve(listener, app, std::future::pending()));
    (url, pat, store)
}

fn scn(root: &Path, cwd: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scn"));
    command
        .env_clear()
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root)
        .env("SCION_CREDENTIALS_FILE", root.join("credentials.json"))
        .env("SCION_NO_DAEMON", "1")
        .current_dir(cwd);
    command
}

fn login(root: &Path, endpoint: &str, token: &str) -> Output {
    let mut child = scn(root, root)
        .args(["login", endpoint, "--with-token", "--json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    writeln!(child.stdin.take().expect("stdin"), "{token}").expect("write token");
    child.wait_with_output().expect("login")
}

fn json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("json")
}

fn whoami(command: &mut Command, args: &[&str], endpoint: &str, subject: &str) {
    let out = json(command.args(["whoami", "--json"]).args(args).output().expect("whoami"));
    assert_eq!(out["endpoint"], endpoint);
    assert_eq!(out["subject"], subject);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_successful_login_sets_the_default() {
    let (a, pat_a, _) = server("alice", None).await;
    let (b, pat_b, _) = server("bob", None).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();

    assert_eq!(json(login(root, &a, &pat_a))["subject"], "alice");
    whoami(&mut scn(root, root), &[], &a, "alice");
    assert_eq!(json(login(root, &b, &pat_b))["subject"], "bob");
    whoami(&mut scn(root, root), &[], &b, "bob");
    assert_eq!(login(root, &a, "scn_pat_nope").status.code(), Some(5));
    whoami(&mut scn(root, root), &[], &b, "bob");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endpoint_overrides_and_credential_scope() {
    let (a, pat_a, store_a) = server("alice", None).await;
    let (b, pat_b, store_b) = server("bob", None).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    json(login(root, &a, &pat_a));
    assert_eq!(login(root, &b, &pat_a).status.code(), Some(5));

    // Give B a copy of A's token record so the clone can detect use of that token.
    let token = store_a.list_tokens(Some("alice")).await.expect("list tokens");
    assert_eq!(token.len(), 1);
    store_b.put_token(&token[0]).await.expect("copy token record");
    let repo_b = format!("{b}/mono");
    let denied = scn(root, root)
        .args(["clone", &repo_b, "denied", "--json"])
        .output()
        .expect("clone without a credential for B");
    assert_eq!(denied.status.code(), Some(5));
    json(
        scn(root, root)
            .env("SCION_TOKEN", &pat_a)
            .args(["clone", &repo_b, "allowed", "--json"])
            .output()
            .expect("clone with a credential for B"),
    );

    json(login(root, &b, &pat_b));
    json(login(root, &a, &pat_a));
    for (url, wc) in [(&a, "a"), (&b, "b")] {
        json(
            scn(root, root)
                .args(["clone", &format!("{url}/mono"), wc, "--json"])
                .output()
                .expect("clone"),
        );
    }
    whoami(&mut scn(root, &root.join("b")), &[], &b, "bob");
    whoami(scn(root, &root.join("a")).env("SCION_ENDPOINT", &b), &[], &b, "bob");
    whoami(
        scn(root, root).env("SCION_ENDPOINT", &b),
        &["--endpoint", &a],
        &a,
        "alice",
    );
    whoami(&mut scn(root, root), &["--endpoint", &a, &b], &b, "bob");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_login_sets_the_default() {
    let idp = scion_api::scion_auth::testing::FakeIdp::start().await;
    let (url, _, _) = server("unused", Some(idp.base())).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let mut child = scn(root, root)
        .args(["login", &url, "--json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("login");
    use std::io::BufRead;
    let mut prompt = String::new();
    let mut stderr = std::io::BufReader::new(child.stderr.take().expect("stderr"));
    stderr.read_line(&mut prompt).expect("read device prompt");
    assert!(prompt.contains("ABCD-EFGH"), "{prompt}");
    idp.approve("dc-1");
    assert_eq!(json(child.wait_with_output().expect("login"))["subject"], "alice");
    whoami(&mut scn(root, root), &[], &url, "alice");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clone_and_import_use_the_saved_server() {
    let (a, pat_a, store_a) = server("alice", None).await;
    let (b, pat_b, store_b) = server("bob", None).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let wc_b = root.join("b");
    json(login(root, &b, &pat_b));
    json(
        scn(root, root)
            .args(["clone", &format!("{b}/mono"), "b", "--json"])
            .output()
            .expect("clone B"),
    );
    json(login(root, &a, &pat_a));
    whoami(&mut scn(root, &wc_b), &[], &b, "bob");
    json(
        scn(root, &wc_b)
            .args(["clone", "mono", "../mono", "--json"])
            .output()
            .expect("clone"),
    );
    whoami(&mut scn(root, &root.join("mono")), &[], &a, "alice");

    let git = |args: &[&str]| {
        assert!(
            Command::new("git")
                .args(["-c", "user.name=Test", "-c", "user.email=test@example.invalid"])
                .args(args)
                .current_dir(root)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", root.join("empty-config"))
                .status()
                .expect("git")
                .success()
        );
    };
    git(&["init", "-q", "-b", "main", "source"]);
    git(&["-C", "source", "commit", "-q", "--allow-empty", "-m", "First commit"]);
    let imported = json(
        scn(root, &wc_b)
            .env("PATH", std::env::var_os("PATH").expect("PATH"))
            .args(["import", "git", "../source", "--repo", "mono", "--json"])
            .output()
            .expect("import"),
    );
    assert_eq!(imported["commits"], 1);
    let tip_a = store_a
        .get_ref("mono", "main")
        .await
        .expect("ref on A")
        .expect("import on A");
    assert_eq!(imported["tip"], tip_a.target.to_string());
    assert!(store_b.get_ref("mono", "main").await.expect("ref on B").is_none());
}
