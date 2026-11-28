//! `scn` must not read or write credentials in the current directory when `HOME` or
//! `XDG_CONFIG_HOME` is missing, empty or relative.

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use scion_api::{App, ServerConfig, serve};
use scion_store::MemStore;
use tokio::net::TcpListener;

/// R1..R5: the environments that have no absolute config directory.
const ROWS: [(&str, &[(&str, &str)]); 5] = [
    ("R1", &[]),
    ("R2", &[("HOME", "")]),
    ("R3", &[("HOME", "h")]),
    ("R4", &[("XDG_CONFIG_HOME", "x")]),
    ("R5", &[("XDG_CONFIG_HOME", "")]),
];

/// A server that requires authentication, with this auth.toml.
async fn auth_server_with(toml: &str) -> (String, Arc<scion_api::scion_auth::Guard>) {
    use scion_api::scion_auth::{AuthConfig, Guard, GuardConfig};
    let store = Arc::new(MemStore::new());
    scion_store::MetaStore::create_repo(store.as_ref(), "mono")
        .await
        .expect("repo");
    let auth = GuardConfig {
        required: true,
        auth: AuthConfig::parse(toml).expect("auth"),
        signing_key_pem: Some(scion_api::scion_auth::token::generate_signing_key()),
    };
    let guard = Guard::build(auth.clone(), store.clone(), Arc::new(|_: &str| None)).expect("guard");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig {
            auth,
            ..ServerConfig::default()
        }),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, std::future::pending()));
    (url, guard)
}

fn spawn_scn(cwd: &Path, vars: &[(&str, &str)], args: &[&str], stdin: Option<&str>) -> std::process::Child {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("SCION_NO_DAEMON", "1")
        .envs(vars.iter().copied())
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(input.as_bytes())
            .expect("write");
    }
    drop(child.stdin.take());
    child
}

fn scn(cwd: &Path, vars: &[(&str, &str)], args: &[&str], stdin: Option<&str>) -> Output {
    spawn_scn(cwd, vars, args, stdin).wait_with_output().expect("wait")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn entries(dir: &Path) -> usize {
    std::fs::read_dir(dir).expect("read_dir").count()
}

/// Run `f` on a thread, so the test server keeps running on the async runtime.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.expect("join")
}

#[tokio::test(flavor = "multi_thread")]
async fn login_without_an_absolute_config_dir_writes_nothing() {
    let (url, guard) = auth_server_with("").await;
    let (pat, _) = guard
        .create_token("pat", "ci", vec!["eng".into()], "", None)
        .await
        .expect("pat");
    for (name, vars) in &ROWS[..4] {
        let dir = tempfile::tempdir().expect("tempdir");
        let (url, pat) = (url.clone(), pat.clone());
        let d = dir.path().to_path_buf();
        let outs = blocking(move || {
            vec![
                scn(&d, vars, &["login", &url, "--with-token"], Some(&format!("{pat}\n"))),
                scn(&d, vars, &["login", &url, "--token", &pat], None),
                scn(&d, vars, &["logout", &url], None),
            ]
        })
        .await;
        for out in outs {
            assert_eq!(out.status.code(), Some(2), "{name}: {}", stderr(&out));
            assert!(
                stderr(&out).contains("SCION_CREDENTIALS_FILE"),
                "{name}: {}",
                stderr(&out)
            );
        }
        assert_eq!(entries(dir.path()), 0, "{name}: the directory must stay empty");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn login_checks_the_config_dir_before_it_connects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let d = dir.path().to_path_buf();
    let out = blocking(move || scn(&d, &[], &["login", "http://127.0.0.1:1", "--token", "scn_pat_zq"], None)).await;
    assert_eq!(out.status.code(), Some(2), "{}", stderr(&out));
    assert!(stderr(&out).contains("SCION_CREDENTIALS_FILE"), "{}", stderr(&out));
    assert_eq!(entries(dir.path()), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn login_with_an_explicit_relative_file_is_allowed() {
    let (url, guard) = auth_server_with("").await;
    let (pat, _) = guard
        .create_token("pat", "ci", vec!["eng".into()], "", None)
        .await
        .expect("pat");
    let dir = tempfile::tempdir().expect("tempdir");
    let d = dir.path().to_path_buf();
    let out = blocking(move || {
        scn(
            &d,
            &[("SCION_CREDENTIALS_FILE", "c.json")],
            &["login", &url, "--with-token"],
            Some(&format!("{pat}\n")),
        )
    })
    .await;
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(dir.path().join("c.json").is_file());
    assert_eq!(entries(dir.path()), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn whoami_ignores_relative_credential_files() {
    let (url, guard) = auth_server_with("").await;
    let (pat, _) = guard
        .create_token("pat", "ci", vec!["eng".into()], "", None)
        .await
        .expect("pat");
    let body = format!("{{\"{url}\": \"{pat}\"}}");
    let planted = [
        ".config/scion/credentials.json",
        "h/.config/scion/credentials.json",
        "x/scion/credentials.json",
        "scion/credentials.json",
    ];
    let subject = |d: std::path::PathBuf, vars: Vec<(&'static str, &'static str)>, url: String| {
        blocking(move || {
            let out = scn(&d, &vars, &["whoami", &url, "--json"], None);
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
            v["subject"].as_str().expect("subject").to_owned()
        })
    };
    for (name, vars) in &ROWS {
        let dir = tempfile::tempdir().expect("tempdir");
        for p in planted {
            let f = dir.path().join(p);
            std::fs::create_dir_all(f.parent().expect("parent")).expect("mkdir");
            std::fs::write(f, &body).expect("plant");
        }
        let s = subject(dir.path().to_path_buf(), vars.to_vec(), url.clone()).await;
        assert_eq!(s, "anonymous", "{name}");
    }
    // Positive control: the planted token is valid when it is named explicitly.
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("c.json"), &body).expect("plant");
    let s = subject(
        dir.path().to_path_buf(),
        vec![("SCION_CREDENTIALS_FILE", "c.json")],
        url.clone(),
    )
    .await;
    assert_eq!(s, "ci");
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_login_without_an_absolute_config_dir_fails_first() {
    let idp = scion_api::scion_auth::testing::FakeIdp::start().await;
    let toml = format!(
        "[oidc]\nissuer = \"{}\"\nclient_id = \"scn\"\nsubject_claim = \"preferred_username\"\n",
        idp.base()
    );
    let (url, _) = auth_server_with(&toml).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let d = dir.path().to_path_buf();
    let (out, timed_out) = blocking(move || {
        let mut child = spawn_scn(&d, &[], &["login", &url], None);
        let start = Instant::now();
        let mut timed_out = false;
        while child.try_wait().expect("try_wait").is_none() {
            if start.elapsed() > Duration::from_secs(10) {
                child.kill().expect("kill");
                timed_out = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        (child.wait_with_output().expect("wait"), timed_out)
    })
    .await;
    assert!(!timed_out, "scn kept polling: {}", stderr(&out));
    assert_eq!(out.status.code(), Some(2), "{}", stderr(&out));
    assert!(stderr(&out).contains("SCION_CREDENTIALS_FILE"), "{}", stderr(&out));
    assert!(!stderr(&out).contains("ABCD-EFGH"), "{}", stderr(&out));
    assert_eq!(entries(dir.path()), 0);
}
