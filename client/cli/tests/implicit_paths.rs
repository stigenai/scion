//! Implicit settings and daemon paths come from absolute roots only (ent-3cb).
//!
//! Every child runs with a cleared environment, a private cwd and a bounded wait.

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
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

/// The default daemon socket under `home` on this platform.
fn native_sock(home: &Path) -> PathBuf {
    if cfg!(target_os = "macos") {
        home.join("Library/Caches/scion/daemon.sock")
    } else {
        home.join(".cache/scion/daemon.sock")
    }
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
    let classes = |good: &str| {
        [
            None,
            Some(String::new()),
            Some(".".into()),
            Some("rel".into()),
            Some(good.into()),
        ]
    };
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
    let out = scn(
        &cwd,
        &[("SCION_CONFIG_FILE", "own.json")],
        &["config", "set", "user.name", "Ada"],
    );
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    assert!(cwd.join("own.json").exists());
    let out = scn(&cwd, &[("SCION_CONFIG_FILE", "")], &["config", "list"]);
    assert_eq!(out.status.code(), Some(2), "{out:?}");
}

/// AC1: with no settings path, get, list, set and unset exit 2 and leave a fresh cwd empty
/// (no settings directory, no temporary file).
#[test]
fn no_settings_path_creates_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cwd = tmp.path().canonicalize().expect("canonical").join("cwd");
    std::fs::create_dir_all(&cwd).expect("cwd");
    for env in [vec![], vec![("HOME", "rel"), ("XDG_CONFIG_HOME", ".")]] {
        for args in [
            &["config", "get", "user.name"][..],
            &["config", "list"],
            &["config", "set", "user.name", "X"],
            &["config", "unset", "user.name"],
        ] {
            let out = scn(&cwd, &env, args);
            assert_eq!(out.status.code(), Some(2), "{env:?} {args:?}: {out:?}");
            assert_eq!(names(&cwd), Vec::<String>::new(), "{env:?} {args:?}: cwd stays empty");
        }
    }
}

async fn serve_mem() -> String {
    let store = Arc::new(MemStore::new());
    store.create_repo("app").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, std::future::pending()));
    url
}

/// A clone of the empty repo in `root/wc`, and the env for scn: git's identity is "Git User",
/// no daemon, and `extra`. The cwd settings hold the identity "Planted".
fn clone_with_planted_settings(url: &str, root: &Path, extra: &[(&str, &str)]) -> (PathBuf, Vec<(String, String)>) {
    let git = root.join("gitconfig");
    std::fs::write(&git, "[user]\n\tname = Git User\n\temail = git@example.com\n").expect("gitconfig");
    let mut env = vec![
        ("PATH".to_string(), std::env::var("PATH").expect("PATH")),
        ("SCION_NO_DAEMON".into(), "1".into()),
        ("GIT_CONFIG_GLOBAL".into(), git.display().to_string()),
        ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
    ];
    env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let e: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let out = scn(root, &e, &["clone", &format!("{url}/app"), "wc", "--json"]);
    assert!(out.status.success(), "{out:?}");
    let wc = root.join("wc");
    std::fs::create_dir_all(wc.join(".config/scion")).expect("plant");
    std::fs::write(
        wc.join(".config/scion/config.json"),
        r#"{"user.name":"Planted","user.email":"planted@example.com"}"#,
    )
    .expect("plant");
    (wc, env)
}

fn pairs(env: &[(String, String)]) -> Vec<(&str, &str)> {
    env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

/// AC2: with no safe settings path, both author lookups ignore the cwd settings and use git's
/// identity. `commit` uses `resolve_author`; an annotated tag uses `own_author`.
#[tokio::test(flavor = "multi_thread")]
async fn author_without_settings() {
    let url = serve_mem().await;
    tokio::task::spawn_blocking(move || {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonical");
        let (wc, env) = clone_with_planted_settings(&url, &root, &[]);
        let env = pairs(&env);
        std::fs::write(wc.join("a.txt"), "1").expect("write");
        let out = scn(&wc, &env, &["commit", "-m", "one", "--json"]);
        assert!(out.status.success(), "{out:?}");
        let out = scn(&wc, &env, &["log", "-n", "1", "--json"]);
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
        assert_eq!(v["entries"][0]["author"], "Git User", "{v}");
        assert_eq!(v["entries"][0]["email"], "git@example.com", "{v}");
        let out = scn(&wc, &env, &["tag", "-a", "v1", "-m", "release", "--json"]);
        assert!(out.status.success(), "{out:?}");
        let out = scn(&wc, &env, &["show", "v1", "--json"]);
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
        assert_eq!(v["tag"]["tagger"], "Git User <git@example.com>", "{v}");
    })
    .await
    .expect("join");
}

/// AC1 and AC2: a malformed settings file at a valid path keeps each lookup's behaviour.
/// `commit` (resolve_author) fails and names the file; a tag (own_author) falls back to no identity.
#[tokio::test(flavor = "multi_thread")]
async fn malformed_settings_keep_each_lookup() {
    let url = serve_mem().await;
    tokio::task::spawn_blocking(move || {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonical");
        let bad = root.join("bad.json");
        std::fs::write(&bad, "{").expect("bad");
        let bad_s = bad.display().to_string();
        // the clone itself already reads the settings, so the bad file comes after it
        let (wc, mut env) = clone_with_planted_settings(&url, &root, &[]);
        env.push(("SCION_CONFIG_FILE".into(), bad_s));
        let env = pairs(&env);
        std::fs::write(wc.join("a.txt"), "1").expect("write");
        let out = scn(&wc, &env, &["commit", "-m", "one", "--json"]);
        assert!(!out.status.success(), "{out:?}");
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("not valid JSON"),
            "{out:?}"
        );
        assert_eq!(std::fs::read(&bad).expect("bad"), b"{");
        // own_author drops the error: the tag is made, and its tagger is not the git identity
        let out = scn(
            &wc,
            &env,
            &["commit", "-m", "one", "--author", "A <a@example.com>", "--json"],
        );
        assert!(out.status.success(), "{out:?}");
        let out = scn(&wc, &env, &["tag", "-a", "v1", "-m", "release", "--json"]);
        assert!(out.status.success(), "{out:?}");
        let out = scn(&wc, &env, &["show", "v1", "--json"]);
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
        assert_ne!(v["tag"]["tagger"], "Git User <git@example.com>", "{v}");
        assert_ne!(v["tag"]["tagger"], "Planted <planted@example.com>", "{v}");
    })
    .await
    .expect("join");
}

/// Listens on a socket and counts connections. Nothing is read from them.
#[cfg(unix)]
struct Spy(UnixListener);

#[cfg(unix)]
impl Spy {
    fn at(path: &Path) -> Spy {
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        let l = UnixListener::bind(path).expect("bind socket");
        l.set_nonblocking(true).expect("nonblocking");
        Spy(l)
    }

    /// Accept every connection that is queued, then count. Call it after scn has exited:
    /// a connection that scn made is queued by then.
    fn finish(self) -> usize {
        let mut n = 0;
        while self.0.accept().is_ok() {
            n += 1;
        }
        n
    }
}

/// The counter sees one connection that sends no data, and none when nobody connects.
#[cfg(unix)]
#[test]
fn spy_counts_connections() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sock = tmp.path().join("s.sock");
    assert_eq!(Spy::at(&sock).finish(), 0);
    std::fs::remove_file(&sock).expect("rm");
    let spy = Spy::at(&sock);
    let _c = UnixStream::connect(&sock).expect("connect");
    assert_eq!(spy.finish(), 1);
}

/// AC3 at the command line: the socket path that `daemon status` reports, per platform.
/// The cache path is checked by the unit tests of `daemon::config`.
#[test]
fn daemon_path_matrix() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().canonicalize().expect("canonical");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(&cwd).expect("cwd");
    let g = |s: &str| root.join(s).display().to_string();
    let (home, xdg_cache, xdg_run, sock, cache) = (g("home"), g("cache"), g("run"), g("o/d.sock"), g("o/c"));
    let mac_home = native_sock(&root.join("home")).display().to_string();
    let linux_home = format!("{home}/.cache/scion/daemon.sock");
    type Case<'a> = (Vec<(&'a str, &'a str)>, Option<String>, Option<String>);
    // (env, expected on Linux, expected on macOS: it ignores XDG)
    let cases: Vec<Case> = vec![
        (vec![("HOME", &home)], Some(linux_home.clone()), Some(mac_home.clone())),
        (
            vec![("HOME", &home), ("XDG_CACHE_HOME", &xdg_cache)],
            Some(format!("{xdg_cache}/scion/daemon.sock")),
            Some(mac_home.clone()),
        ),
        (
            vec![("HOME", &home), ("XDG_RUNTIME_DIR", &xdg_run)],
            Some(format!("{xdg_run}/scion/daemon.sock")),
            Some(mac_home.clone()),
        ),
        (
            vec![("HOME", "rel"), ("XDG_CACHE_HOME", &xdg_cache), ("XDG_RUNTIME_DIR", "")],
            Some(format!("{xdg_cache}/scion/daemon.sock")),
            None,
        ),
        (vec![("HOME", "rel"), ("XDG_RUNTIME_DIR", &xdg_run)], None, None),
        (
            vec![("HOME", "rel"), ("XDG_CACHE_HOME", "."), ("XDG_RUNTIME_DIR", "rel")],
            None,
            None,
        ),
        (
            vec![("HOME", ""), ("XDG_CACHE_HOME", ""), ("XDG_RUNTIME_DIR", "")],
            None,
            None,
        ),
        (vec![], None, None),
        (vec![("HOME", "."), ("SCION_DAEMON_SOCKET", &sock)], None, None),
        (
            vec![("HOME", &home), ("SCION_DAEMON_SOCKET", &sock)],
            Some(sock.clone()),
            Some(sock.clone()),
        ),
        (
            vec![("HOME", &home), ("SCION_DAEMON_CACHE_DIR", &cache)],
            Some(linux_home.clone()),
            Some(mac_home.clone()),
        ),
        (
            vec![("SCION_DAEMON_SOCKET", &sock), ("SCION_DAEMON_CACHE_DIR", &cache)],
            Some(sock.clone()),
            Some(sock.clone()),
        ),
        (
            vec![("HOME", &home), ("SCION_DAEMON_SOCKET", "")],
            Some(linux_home.clone()),
            Some(mac_home.clone()),
        ),
        // an empty cache override selects the default, which a relative HOME cannot give
        (
            vec![
                ("HOME", "rel"),
                ("SCION_DAEMON_SOCKET", &sock),
                ("SCION_DAEMON_CACHE_DIR", ""),
            ],
            None,
            None,
        ),
        // a relative cache override is used as given, so the socket override alone is enough
        (
            vec![
                ("HOME", "rel"),
                ("SCION_DAEMON_SOCKET", &sock),
                ("SCION_DAEMON_CACHE_DIR", "rel"),
            ],
            Some(sock.clone()),
            Some(sock.clone()),
        ),
    ];
    for (env, linux, mac) in cases {
        let want = if cfg!(target_os = "macos") { mac } else { linux };
        let before = names(&root);
        let out = scn(&cwd, &env, &["daemon", "status", "--json"]);
        match want {
            Some(w) => {
                let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
                assert_eq!(v["socket"], w, "{env:?}: {out:?}");
            }
            None => assert_eq!(out.status.code(), Some(2), "{env:?}: {out:?}"),
        }
        assert_eq!(names(&root), before, "{env:?}: nothing created");
    }
}

/// What stands where a daemon PID file would be.
#[cfg(unix)]
enum Pid {
    /// A FIFO that blocks any reader, so a read shows as a timeout.
    Fifo,
    /// The PID of a test-owned `sleep`, so a signal shows as its death.
    Child,
}

/// AC4: with no complete path pair, daemon commands refuse and touch nothing: no connection to a
/// socket, no read of the PID file, no signal, no spawn.
#[cfg(unix)]
#[test]
fn daemon_without_paths() {
    // (HOME, socket override, cache override)
    let cases = [
        (None, false, false),
        (Some("rel"), false, false),
        (Some("rel"), true, false),
        (Some("rel"), false, true),
        (Some("."), true, false),
    ];
    for (home, with_sock, with_cache) in cases {
        for kind in [Pid::Fifo, Pid::Child] {
            let tmp = tempfile::tempdir().expect("tempdir");
            let root = tmp.path().canonicalize().expect("canonical");
            let cwd = root.join("cwd");
            std::fs::create_dir_all(&cwd).expect("cwd");
            let over = root.join("over");
            let (sock, cache) = (over.join("d.sock"), over.join("cache"));
            let (sock_s, cache_s) = (sock.display().to_string(), cache.display().to_string());
            let mut env: Vec<(&str, &str)> = vec![];
            if let Some(h) = home {
                env.push(("HOME", h));
            }
            if with_sock {
                env.push(("SCION_DAEMON_SOCKET", &sock_s));
            }
            if with_cache {
                env.push(("SCION_DAEMON_CACHE_DIR", &cache_s));
            }
            // where the old code put the socket: under the cwd
            let planted = native_sock(&cwd.join(home.unwrap_or(".")));
            let mut spies = vec![Spy::at(&planted)];
            if with_sock {
                spies.push(Spy::at(&sock));
            }
            let pid_path = scion_daemon::pid_file(if with_sock { &sock } else { &planted });
            let (mut child, _fifo) = match kind {
                Pid::Fifo => {
                    let st = Command::new("mkfifo").arg(&pid_path).status().expect("mkfifo");
                    assert!(st.success());
                    let f = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&pid_path)
                        .expect("hold fifo");
                    (None, Some(f))
                }
                Pid::Child => {
                    let c = Command::new("sleep").arg("60").spawn().expect("sleep");
                    std::fs::write(&pid_path, format!("{}\n", c.id())).expect("pid");
                    (Some(c), None)
                }
            };
            let before = names(&root);
            for args in [&["daemon", "run"][..], &["daemon", "status"], &["daemon", "stop"]] {
                let out = scn(&cwd, &env, args);
                assert_eq!(out.status.code(), Some(2), "{env:?} {args:?}: {out:?}");
                let err = String::from_utf8_lossy(&out.stderr);
                assert!(
                    err.contains("no daemon paths") && err.contains("SCION_DAEMON_SOCKET"),
                    "{err}"
                );
            }
            assert_eq!(names(&root), before, "{env:?}: nothing created or removed");
            for s in spies {
                assert_eq!(s.finish(), 0, "{env:?}: no socket connection");
            }
            if let Some(c) = child.as_mut() {
                assert!(
                    c.try_wait().expect("try_wait").is_none(),
                    "{env:?}: child was signalled"
                );
                assert_eq!(
                    std::fs::read_to_string(&pid_path).expect("pid"),
                    format!("{}\n", c.id())
                );
                c.kill().expect("kill");
                c.wait().expect("reap");
            }
        }
    }
}

/// AC4: automatic client use skips the daemon and reaches the direct endpoint. No connection
/// reaches a socket, no daemon is spawned, and nothing is created.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn client_skips_daemon_without_paths() {
    let url = serve_mem().await;
    tokio::task::spawn_blocking(move || {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().canonicalize().expect("canonical");
        let cwd = root.join("cwd");
        std::fs::create_dir_all(&cwd).expect("cwd");
        let home = root.join("home");
        let (home_s, ok_sock) = (home.display().to_string(), root.join("ok/d.sock"));
        let (ok_sock_s, ok_cache_s) = (
            ok_sock.display().to_string(),
            root.join("ok/cache").display().to_string(),
        );
        // (env, a socket to watch, the other sockets to watch)
        let cases: Vec<(Vec<(&str, &str)>, PathBuf)> = vec![
            (vec![], native_sock(&cwd.join("."))),
            (vec![("HOME", "rel")], native_sock(&cwd.join("rel"))),
            // a valid socket with no cache
            (
                vec![("HOME", "rel"), ("SCION_DAEMON_SOCKET", &ok_sock_s)],
                ok_sock.clone(),
            ),
            // a valid cache with no socket
            (
                vec![("HOME", "rel"), ("SCION_DAEMON_CACHE_DIR", &ok_cache_s)],
                native_sock(&cwd.join("rel")),
            ),
            // valid paths, the daemon turned off
            (vec![("HOME", &home_s), ("SCION_NO_DAEMON", "1")], native_sock(&home)),
        ];
        for (env, planted) in cases {
            let spy = Spy::at(&planted);
            let before = names(&root);
            let out = scn(&cwd, &env, &["doctor", "--json", "--endpoint", &url]);
            assert_eq!(spy.finish(), 0, "{env:?}: no socket connection");
            assert_eq!(out.status.code(), Some(0), "{env:?}: {out:?}");
            let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
            assert_eq!(v["objects"], url, "{env:?}: direct");
            assert_eq!(names(&root), before, "{env:?}: nothing created");
            std::fs::remove_file(&planted).expect("rm socket");
        }
    })
    .await
    .expect("join");
}
