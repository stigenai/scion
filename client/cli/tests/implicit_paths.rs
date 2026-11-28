//! Implicit settings and daemon paths come from absolute roots only (ent-3cb).
//!
//! Every child runs with a cleared environment, a private cwd and a bounded wait.

use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use scion_api::{App, ServerConfig, serve};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

/// Run scn with only `env` set. Kill and reap it after 15 s.
fn scn(cwd: &Path, env: &[(&str, &str)], args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(cwd)
        .args(args)
        .env_clear()
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn scn");
    let start = Instant::now();
    loop {
        if child.try_wait().expect("try_wait").is_some() || start.elapsed() > Duration::from_secs(15) {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    child.wait_with_output().expect("reap scn")
}

fn names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = walk(dir, dir);
    v.sort();
    v
}

fn walk(root: &Path, dir: &Path) -> Vec<String> {
    let mut v = vec![];
    for e in std::fs::read_dir(dir).expect("read_dir") {
        let p = e.expect("entry").path();
        v.push(p.strip_prefix(root).expect("prefix").display().to_string());
        if p.is_dir() {
            v.extend(walk(root, &p));
        }
    }
    v
}

fn abs_or_none(v: &Option<String>) -> Option<&str> {
    v.as_deref().filter(|s| Path::new(s).is_absolute())
}

/// AC1: the settings path, for each class of each root.
#[test]
fn config_path_matrix() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().canonicalize().expect("canonical");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(cwd.join(".config/scion")).expect("plant dir");
    std::fs::write(cwd.join(".config/scion/config.json"), r#"{"user.name":"Planted"}"#).expect("plant");
    let good_xdg = root.join("xdg").display().to_string();
    let good_home = root.join("home").display().to_string();
    let classes = |good: &str| [None, Some(String::new()), Some(".".into()), Some("rel".into()), Some(good.into())];
    for xdg in classes(&good_xdg) {
        for home in classes(&good_home) {
            let mut env = vec![];
            if let Some(x) = &xdg {
                env.push(("XDG_CONFIG_HOME", x.as_str()));
            }
            if let Some(h) = &home {
                env.push(("HOME", h.as_str()));
            }
            let want: Option<PathBuf> = abs_or_none(&xdg)
                .map(|x| Path::new(x).join("scion/config.json"))
                .or_else(|| abs_or_none(&home).map(|h| Path::new(h).join(".config/scion/config.json")));
            let before = names(&cwd);
            let out = scn(&cwd, &env, &["config", "list", "--json"]);
            let label = format!("xdg={xdg:?} home={home:?}");
            match want {
                Some(w) => {
                    assert_eq!(out.status.code(), Some(0), "{label}: {out:?}");
                    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
                    assert_eq!(v["path"], w.display().to_string(), "{label}");
                    assert!(v["values"].as_object().expect("values").is_empty(), "{label}");
                }
                None => {
                    for args in [
                        &["config", "list"][..],
                        &["config", "get", "user.name"],
                        &["config", "set", "user.name", "X"],
                        &["config", "unset", "user.name"],
                    ] {
                        let out = scn(&cwd, &env, args);
                        assert_eq!(out.status.code(), Some(2), "{label} {args:?}: {out:?}");
                    }
                }
            }
            assert_eq!(names(&cwd), before, "{label}: cwd unchanged");
            assert_eq!(
                std::fs::read(cwd.join(".config/scion/config.json")).expect("sentinel"),
                br#"{"user.name":"Planted"}"#
            );
        }
    }
    // An explicit file is used as given, even when relative; empty selects the defaults.
    let out = scn(&cwd, &[("SCION_CONFIG_FILE", "own.json")], &["config", "set", "user.name", "Ada"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    assert!(cwd.join("own.json").exists());
    let out = scn(&cwd, &[("SCION_CONFIG_FILE", "")], &["config", "list"]);
    assert_eq!(out.status.code(), Some(2), "{out:?}");
}

async fn serve_mem() -> (String, Arc<MemStore>) {
    let store = Arc::new(MemStore::new());
    store.create_repo("app").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store.clone(),
    };
    tokio::spawn(serve(listener, app, std::future::pending()));
    (url, store)
}

/// AC2: with no safe settings path, commit ignores the cwd settings and uses git's identity.
#[tokio::test(flavor = "multi_thread")]
async fn author_without_settings() {
    let (url, _store) = serve_mem().await;
    tokio::task::spawn_blocking(move || {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonical");
        let git = root.join("gitconfig");
        std::fs::write(&git, "[user]\n\tname = Git User\n\temail = git@example.com\n").expect("gitconfig");
        let git = git.display().to_string();
        let env = [
            ("PATH", std::env::var("PATH").expect("PATH")),
            ("SCION_NO_DAEMON", "1".into()),
            ("GIT_CONFIG_GLOBAL", git),
            ("GIT_CONFIG_NOSYSTEM", "1".into()),
        ];
        let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let out = scn(&root, &env, &["clone", &format!("{url}/app"), "wc", "--json"]);
        assert!(out.status.success(), "{out:?}");
        let wc = root.join("wc");
        std::fs::create_dir_all(wc.join(".config/scion")).expect("plant");
        std::fs::write(
            wc.join(".config/scion/config.json"),
            r#"{"user.name":"Planted","user.email":"planted@example.com"}"#,
        )
        .expect("plant");
        std::fs::write(wc.join("a.txt"), "1").expect("write");
        let out = scn(&wc, &env, &["commit", "-m", "one", "--json"]);
        assert!(out.status.success(), "{out:?}");
        let out = scn(&wc, &env, &["log", "-n", "1", "--json"]);
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
        assert_eq!(v["entries"][0]["author"], "Git User", "{v}");
        assert_eq!(v["entries"][0]["email"], "git@example.com", "{v}");
    })
    .await
    .expect("join");
}

/// Counts connections to a socket. Nothing is read from them.
struct Spy {
    count: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Spy {
    fn at(path: &Path) -> Spy {
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        let l = UnixListener::bind(path).expect("bind socket");
        l.set_nonblocking(true).expect("nonblocking");
        let count = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (c, s) = (count.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                if l.accept().is_ok() {
                    c.fetch_add(1, Ordering::Relaxed);
                } else {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        });
        Spy {
            count,
            stop,
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().expect("thread").join().expect("join");
        self.count.load(Ordering::Relaxed)
    }
}

/// AC3 at the command line: the socket path that `daemon status` reports.
#[test]
fn daemon_path_matrix() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().canonicalize().expect("canonical");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(&cwd).expect("cwd");
    let g = |s: &str| root.join(s).display().to_string();
    let (home, xdg_cache, xdg_run, sock, cache) = (g("home"), g("cache"), g("run"), g("o/d.sock"), g("o/c"));
    let cases: Vec<(Vec<(&str, &str)>, Option<String>)> = vec![
        (vec![("HOME", &home)], Some(format!("{home}/.cache/scion/daemon.sock"))),
        (vec![("HOME", &home), ("XDG_CACHE_HOME", &xdg_cache)], Some(format!("{xdg_cache}/scion/daemon.sock"))),
        (vec![("HOME", &home), ("XDG_RUNTIME_DIR", &xdg_run)], Some(format!("{xdg_run}/scion/daemon.sock"))),
        (
            vec![("HOME", "rel"), ("XDG_CACHE_HOME", &xdg_cache), ("XDG_RUNTIME_DIR", "")],
            Some(format!("{xdg_cache}/scion/daemon.sock")),
        ),
        (vec![("HOME", "rel"), ("XDG_RUNTIME_DIR", &xdg_run)], None),
        (vec![("HOME", "rel"), ("XDG_CACHE_HOME", "."), ("XDG_RUNTIME_DIR", "rel")], None),
        (vec![("HOME", ""), ("XDG_CACHE_HOME", ""), ("XDG_RUNTIME_DIR", "")], None),
        (vec![], None),
        (vec![("HOME", "."), ("SCION_DAEMON_SOCKET", &sock)], None),
        (vec![("HOME", &home), ("SCION_DAEMON_SOCKET", &sock)], Some(sock.clone())),
        (vec![("HOME", &home), ("SCION_DAEMON_CACHE_DIR", &cache)], Some(format!("{home}/.cache/scion/daemon.sock"))),
        (vec![("SCION_DAEMON_SOCKET", &sock), ("SCION_DAEMON_CACHE_DIR", &cache)], Some(sock.clone())),
        (vec![("HOME", &home), ("SCION_DAEMON_SOCKET", "")], Some(format!("{home}/.cache/scion/daemon.sock"))),
    ];
    for (env, want) in cases {
        let before = names(&cwd);
        let out = scn(&cwd, &env, &["daemon", "status", "--json"]);
        match want {
            Some(w) => {
                let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
                assert_eq!(v["socket"], w, "{env:?}: {out:?}");
            }
            None => assert_eq!(out.status.code(), Some(2), "{env:?}: {out:?}"),
        }
        assert_eq!(names(&cwd), before, "{env:?}");
    }
}

/// AC4: with no complete path pair, daemon commands refuse and touch nothing.
#[test]
fn daemon_without_paths() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().canonicalize().expect("canonical");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(&cwd).expect("cwd");
    let over = root.join("over");
    let sock = over.join("d.sock");
    let cache = over.join("cache");
    let (sock_s, cache_s) = (sock.display().to_string(), cache.display().to_string());
    // (env, whether the socket override is valid)
    let cases: Vec<Vec<(&str, &str)>> = vec![
        vec![],
        vec![("HOME", "rel")],
        vec![("HOME", "rel"), ("SCION_DAEMON_SOCKET", &sock_s)], // invalid cache, valid socket
        vec![("HOME", "rel"), ("SCION_DAEMON_CACHE_DIR", &cache_s)], // invalid socket, valid cache
    ];
    for env in cases {
        let spy = env.iter().any(|(k, _)| *k == "SCION_DAEMON_SOCKET").then(|| Spy::at(&sock));
        let pid = over.join("daemon.pid");
        if spy.is_some() {
            std::fs::write(&pid, "1\n").expect("pid");
        }
        let (before_cwd, before_over) = (names(&cwd), names(&over.parent().map_or(root.clone(), Path::to_path_buf)));
        for args in [&["daemon", "run"][..], &["daemon", "status"], &["daemon", "stop"]] {
            let out = scn(&cwd, &env, args);
            assert_eq!(out.status.code(), Some(2), "{env:?} {args:?}: {out:?}");
            assert!(!out.stderr.is_empty(), "an actionable message");
        }
        assert_eq!(names(&cwd), before_cwd, "{env:?}");
        assert_eq!(names(&root), before_over, "{env:?}: no mutation outside cwd");
        if spy.is_some() {
            assert_eq!(std::fs::read_to_string(&pid).expect("pid"), "1\n");
        }
        if let Some(s) = spy {
            assert_eq!(s.finish(), 0, "{env:?}: no socket connection");
            std::fs::remove_file(&sock).expect("rm socket");
            std::fs::remove_file(&pid).expect("rm pid");
        }
    }
}

/// AC4: automatic client use skips the daemon and reaches the direct endpoint.
#[tokio::test(flavor = "multi_thread")]
async fn client_skips_daemon_without_paths() {
    let (url, _store) = serve_mem().await;
    tokio::task::spawn_blocking(move || {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonical");
        let cwd = root.join("cwd");
        std::fs::create_dir_all(&cwd).expect("cwd");
        let ok_sock = root.join("ok/d.sock");
        let ok_sock_s = ok_sock.display().to_string();
        // default socket under the cwd (HOME unset on the base: "."), and a valid socket with no cache
        for (env, planted) in [
            (vec![], cwd.join(".cache/scion/daemon.sock")),
            (vec![("HOME", "rel")], cwd.join("rel/.cache/scion/daemon.sock")),
            (vec![("HOME", "rel"), ("SCION_DAEMON_SOCKET", ok_sock_s.as_str())], ok_sock.clone()),
        ] {
            let spy = Spy::at(&planted);
            let out = scn(&cwd, &env, &["doctor", "--json", "--endpoint", &url]);
            assert_eq!(spy.finish(), 0, "{env:?}: no socket connection");
            assert_eq!(out.status.code(), Some(0), "{env:?}: {out:?}");
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
            assert_eq!(v["objects"], url, "{env:?}: direct");
            std::fs::remove_file(&planted).expect("rm socket");
        }
    })
    .await
    .expect("join");
}
