//! 运行编译好的 `scn` 二进制，检查输出格式与退出码。

use std::process::Command;
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_store::MemStore;
use tokio::net::TcpListener;

fn scn(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .env_remove("SCION_ENDPOINT")
        .env_remove("SCION_TOKEN")
        // daemon 有专门的测试；其余测试直连，不在机器上留下后台进程
        .env("SCION_NO_DAEMON", "1")
        .output()
        .expect("run scn")
}

#[test]
fn version_json() {
    let out = scn(&["version", "--json"]);
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["schema"], "scion.cli.version/v1");
}

#[test]
fn usage_error_exits_2() {
    assert_eq!(scn(&["no-such-command"]).status.code(), Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_against_live_server() {
    let store = Arc::new(MemStore::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));

    let u = url.clone();
    let out = tokio::task::spawn_blocking(move || scn(&["doctor", "--json", "--endpoint", &u]))
        .await
        .expect("join");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["schema"], "scion.cli.doctor/v1");
    assert_eq!(v["ok"], true);
    assert_eq!(v["protocol_version"], scion_proto::PROTOCOL_VERSION);

    let u = url.clone();
    let out = tokio::task::spawn_blocking(move || scn(&["doctor", "--endpoint", &u]))
        .await
        .expect("join");
    assert!(String::from_utf8_lossy(&out.stdout).contains("status:    ok"));
    let _ = tx.send(());
}

#[tokio::test]
async fn doctor_unreachable_exits_4_with_hint() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind")
        .local_addr()
        .expect("addr");
    let url = format!("http://{addr}");
    let out = scn(&["doctor", "--json", "--endpoint", &url]);
    assert_eq!(out.status.code(), Some(4));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["schema"], "scion.cli.error/v1");
    assert_eq!(v["retryable"], true);
    assert!(v["hint"].as_str().expect("hint").contains("--endpoint"));

    let out = scn(&["doctor", "--endpoint", &url]);
    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stderr).contains("hint:"));
}

#[test]
fn doctor_invalid_endpoint_exits_1() {
    let out = scn(&["doctor", "--endpoint", "ftp://x"]);
    assert_eq!(out.status.code(), Some(1));
}

/// The working copy's current commit, from `scn log`.
fn head_now(wc: &std::path::Path) -> String {
    json_of(&scn_in(wc, &["log", "HEAD", "-n", "1", "--json"]))["entries"][0]["id"]
        .as_str()
        .expect("id")
        .to_owned()
}

/// 在指定目录运行 scn。
fn scn_in(dir: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(dir)
        .args(args)
        .env_remove("SCION_ENDPOINT")
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_REPO")
        .env_remove("SCION_AUTHOR")
        .env("SCION_NO_DAEMON", "1")
        .output()
        .expect("run scn")
}

fn json_of(out: &std::process::Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    serde_json::from_slice(&out.stdout).expect("json")
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_prints_commits_as_they_land() {
    use scion_store::MetaStore;
    use std::io::BufRead;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let author = ["--author", "Ada <ada@example.com>"];
        let wc = root.join("app");
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "--json"]));
        let land = |file: &str| {
            std::fs::write(wc.join(file), file).expect("write");
            json_of(&scn_in(&wc, &[&["commit", "-m", file, "--json"][..], &author].concat()));
            json_of(&scn_in(&wc, &["land", "--json"]));
            head_now(&wc)
        };
        let first = land("a");
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_scn"))
            .current_dir(&wc)
            .args(["watch", "main", "--count", "2"])
            .env("SCION_NO_DAEMON", "1")
            .env_remove("SCION_TOKEN")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("watch");
        let mut err = std::io::BufReader::new(child.stderr.take().expect("stderr"));
        let mut line = String::new();
        err.read_line(&mut line).expect("read");
        assert_eq!(line, format!("Watching main at {}; Ctrl-C stops\n", &first[..12]));
        let lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let out = std::io::BufReader::new(child.stdout.take().expect("stdout"));
        let seen = lines.clone();
        let reader = std::thread::spawn(move || {
            for l in out.lines() {
                seen.lock().expect("lock").push(l.expect("line"));
            }
        });
        let wait_for = |n: usize| {
            for _ in 0..100 {
                if lines.lock().expect("lock").len() >= n {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            panic!("watch printed {:?}", lines.lock().expect("lock"));
        };
        let b = land("b");
        wait_for(1);
        let c = land("c");
        wait_for(2);
        assert!(child.wait().expect("wait").success(), "stops after --count moves");
        reader.join().expect("reader");
        assert_eq!(
            *lines.lock().expect("lock"),
            [
                format!("main  {}  Ada  b", &b[..12]),
                format!("main  {}  Ada  c", &c[..12])
            ]
        );
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn land_reports_the_server_rebase() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let author = ["--author", "Ada <ada@example.com>"];
        let first = root.join("first");
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "first", "--json"]));
        std::fs::write(first.join("a.txt"), "a").expect("write");
        json_of(&scn_in(
            &first,
            &[&["commit", "-m", "a", "--json"][..], &author].concat(),
        ));
        json_of(&scn_in(&first, &["land", "--json"]));
        let second = root.join("second");
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "second", "--json"]));

        // first lands b; second, still on the old base, lands c and is rebased over b
        std::fs::write(first.join("b.txt"), "b").expect("write");
        json_of(&scn_in(
            &first,
            &[&["commit", "-m", "b", "--json"][..], &author].concat(),
        ));
        json_of(&scn_in(&first, &["land", "--json"]));
        let b = head_now(&first);
        // As in git, log shows the working copy's history; the branch's name shows the server's
        let base = head_now(&second);
        let top = |args: &[&str]| json_of(&scn_in(&second, args))["entries"][0]["id"].clone();
        assert_eq!(top(&["log", "-n", "1", "--json"]), base.as_str());
        assert_eq!(top(&["log", "main", "-n", "1", "--json"]), b.as_str());
        // Another repository named with --repo from inside the working copy: its main, not our HEAD
        let other = format!("{url}/other");
        json_of(&scn_in(&root, &["repo", "create", &other, "--json"]));
        let oc = root.join("oc");
        json_of(&scn_in(&root, &["clone", &other, "oc", "--json"]));
        std::fs::write(oc.join("o.txt"), "o").expect("write");
        json_of(&scn_in(&oc, &[&["commit", "-m", "o", "--json"][..], &author].concat()));
        json_of(&scn_in(&oc, &["land", "--json"]));
        let out = scn_in(&second, &["cat", "o.txt", "--repo", &other]);
        assert_eq!(out.stdout, b"o", "{out:?}");
        // status compares with the branch on the server
        let v = json_of(&scn_in(&second, &["status", "--json"]));
        assert_eq!(v["behind"], serde_json::json!({"count": 1, "exact": true}));
        let sb = |args: &[&str]| String::from_utf8_lossy(&scn_in(&second, args).stdout).into_owned();
        assert_eq!(sb(&["status", "-sb"]), "## main [behind 1]\n");
        assert_eq!(sb(&["status", "-sb", "--no-ahead-behind"]), "## main\n");
        assert!(sb(&["status"]).contains("Your branch is behind 'main' on the server by 1 commit."));
        assert!(json_of(&scn_in(&first, &["status", "--json"]))["behind"]["count"] == 0);
        // A changed file's lines are counted against its old version on the server
        std::fs::write(first.join("a.txt"), "a\nx\ny\n").expect("write");
        let out = scn_in(&first, &[&["commit", "-m", "x"][..], &author].concat());
        assert!(
            String::from_utf8_lossy(&out.stdout).ends_with(" 1 file changed, 3 insertions(+), 1 deletion(-)\n"),
            "{out:?}"
        );
        json_of(&scn_in(&first, &["uncommit", "--json"]));
        std::fs::write(first.join("a.txt"), "a").expect("write");
        std::fs::write(second.join("c.txt"), "c").expect("write");
        let committed = json_of(&scn_in(
            &second,
            &[&["commit", "-m", "c", "--json"][..], &author].concat(),
        ))["changeset"]
            .as_str()
            .expect("id")
            .to_owned();
        assert_eq!(sb(&["status", "-sb"]), "## main [ahead 1, behind 1]\n");
        let out = scn_in(&second, &["land"]);
        assert!(out.status.success(), "{out:?}");
        let landed = head_now(&second);
        assert_ne!(landed, committed);
        // The landed commit names the one it was made as
        let v = json_of(&scn_in(&second, &["log", "-n", "1", "--json"]));
        assert_eq!(v["entries"][0]["rebased_from"], committed.as_str());
        let show = String::from_utf8_lossy(&scn_in(&second, &["show", "--name-only"]).stdout).into_owned();
        assert!(
            show.contains(&format!("\nRebased-from: {}\n", &committed[..12])),
            "{show}"
        );
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        // The range starts at b, the server's tip before the land, not second's old base
        assert!(
            text.contains(&format!(
                "   {}..{}  main -> main (rebased onto the latest main)",
                &b[..12],
                &landed[..12]
            )),
            "{text}"
        );
        assert!(
            text.contains(&format!("\nYour commit {} is now {}", &committed[..12], &landed[..12])),
            "{text}"
        );
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn revert_undoes_a_landed_commit() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let wc = root.join("wc");
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "wc", "--json"]));
        let run = |args: &[&str]| scn_in(&wc, &[args, &["--author", "Ada <ada@example.com>"][..]].concat());
        let text = |out: &std::process::Output| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            )
        };
        let land = |file: &str, content: &str, message: &str| {
            std::fs::write(wc.join(file), content).expect("write");
            json_of(&run(&["commit", "-m", message, "--json"]));
            json_of(&scn_in(&wc, &["land", "--json"]));
            head_now(&wc)
        };
        let read = || std::fs::read_to_string(wc.join("f.txt")).expect("read");
        land("f.txt", "1\n2\n3\n4\n5\n", "base");
        let first = land("f.txt", "one\n2\n3\n4\n5\n", "first line");
        let last = land("f.txt", "one\n2\n3\n4\nfive\n", "last line");

        // The first line goes back; the later change to the last line stays
        let v = json_of(&run(&["revert", &first, "--json"]));
        assert_eq!(
            (v["commit"].as_str(), v["written"].as_u64()),
            (Some(first.as_str()), Some(1))
        );
        assert_eq!(read(), "1\n2\n3\n4\nfive\n");
        let log = text(&scn_in(&wc, &["log", "-n", "1"]));
        assert!(log.contains("Revert \"first line\""), "{log}");
        assert!(log.contains(&format!("This reverts commit {first}.")), "{log}");
        json_of(&scn_in(&wc, &["land", "--json"]));
        // Already undone: nothing to commit
        let out = run(&["revert", &first]);
        assert!(out.status.success(), "{out:?}");
        assert!(
            text(&out).starts_with("nothing to commit: main already has the result of reverting"),
            "{}",
            text(&out)
        );
        // A dirty tree is refused; a pending commit gets the revert on top
        std::fs::write(wc.join("g.txt"), "g").expect("write");
        assert_eq!(run(&["revert", &last]).status.code(), Some(1));
        json_of(&run(&["commit", "-m", "g", "--json"]));
        json_of(&run(&["revert", &last, "--json"]));
        assert_eq!(text(&scn_in(&wc, &["status", "-sb"])), "## main [ahead 2]\n");
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["ahead"], 2);
        // uncommit drops only the revert
        json_of(&scn_in(&wc, &["uncommit", "--json"]));
        json_of(&scn_in(&wc, &["restore", ".", "--json"]));
        assert_eq!(text(&scn_in(&wc, &["status", "-sb"])), "## main [ahead 1]\n");
        json_of(&scn_in(&wc, &["land", "--json"]));

        // The last line changed again since: reverting `last` conflicts
        land("f.txt", "1\n2\n3\n4\nFIVE\n", "shout");
        let out = run(&["revert", &last]);
        assert_eq!(out.status.code(), Some(3), "{out:?}");
        assert!(
            text(&out).contains(&format!("could not revert {};", &last[..12])),
            "{}",
            text(&out)
        );
        let status = text(&scn_in(&wc, &["status"]));
        assert!(
            status.contains(&format!("You are currently reverting commit {}.", &last[..12])),
            "{status}"
        );
        assert!(status.contains("scn revert --abort"), "{status}");
        json_of(&scn_in(&wc, &["revert", "--abort", "--json"]));
        assert_eq!(read(), "1\n2\n3\n4\nFIVE\n");
        let out = scn_in(&wc, &["revert", "--abort"]);
        assert_eq!(out.status.code(), Some(2));
        assert!(text(&out).contains("no revert is in progress"));
        // Again, then take the revert's side and commit with the saved message
        assert_eq!(run(&["revert", &last]).status.code(), Some(3));
        json_of(&scn_in(&wc, &["resolve", "--theirs", "f.txt", "--json"]));
        json_of(&run(&["commit", "--json"]));
        // --theirs takes the whole file as it was before `last`, as in git
        assert_eq!(read(), "one\n2\n3\n4\n5\n");
        let log = text(&scn_in(&wc, &["log", "-n", "1"]));
        assert!(log.contains("Revert \"last line\""), "{log}");
        json_of(&scn_in(&wc, &["land", "--json"]));

        // A merge needs -m; -m needs a merge
        json_of(&scn_in(&wc, &["switch", "-c", "feature", "--json"]));
        land("h.txt", "h", "add h");
        json_of(&scn_in(&wc, &["switch", "main", "--json"]));
        land("f.txt", "1\n2\n3\n4\n5\n6\n", "six");
        let six = "1\n2\n3\n4\n5\n6\n";
        json_of(&run(&["merge", "feature", "--json"]));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let merge = head_now(&wc);
        let out = run(&["revert", &merge]);
        assert_eq!(out.status.code(), Some(2));
        assert!(text(&out).contains("is a merge commit; pass -m 1"), "{}", text(&out));
        assert!(text(&run(&["revert", "-m", "3", &merge])).contains("has 2 parents; -m takes 1 to 2"));
        assert!(text(&run(&["revert", "-m", "1", &first])).contains("is not a merge commit"));
        json_of(&run(&["revert", "-m", "1", &merge, "--json"]));
        assert!(!wc.join("h.txt").exists());
        assert_eq!(read(), six);
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn cherry_pick_applies_a_commit_from_another_branch() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let wc = root.join("wc");
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "wc", "--json"]));
        let run = |args: &[&str]| scn_in(&wc, args);
        let text = |out: &std::process::Output| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            )
        };
        let land = |file: &str, content: &str, message: &str, who: &str| {
            std::fs::write(wc.join(file), content).expect("write");
            json_of(&run(&["commit", "-m", message, "--author", who, "--json"]));
            json_of(&run(&["land", "--json"]));
            head_now(&wc)
        };
        let me = "Ada <ada@example.com>";
        let read = || std::fs::read_to_string(wc.join("f.txt")).expect("read");
        land("f.txt", "1\n2\n3\n", "base", me);
        json_of(&run(&["branch", "release", "--json"]));
        let fix = land(
            "f.txt",
            "1\n2\nthree\n",
            "fix three\n\nBecause.",
            "Bob <bob@example.com>",
        );
        land("f.txt", "one\n2\nthree\n", "main only", me);

        // On the release branch: the fix arrives with its message and author, not main's other change
        json_of(&run(&["switch", "release", "--json"]));
        // SCION_AUTHOR, an agent's own identity, does not replace the original author
        let out = Command::new(env!("CARGO_BIN_EXE_scn"))
            .current_dir(&wc)
            .args(["cherry-pick", &fix, "--json"])
            .env("SCION_AUTHOR", me)
            .env("SCION_NO_DAEMON", "1")
            .output()
            .expect("run scn");
        let v = json_of(&out);
        assert_eq!(v["commit"].as_str(), Some(fix.as_str()));
        assert_eq!(read(), "1\n2\nthree\n");
        let top = json_of(&run(&["log", "-n", "1", "--json"]))["entries"][0].clone();
        assert_eq!(top["message"].as_str(), Some("fix three\n\nBecause."));
        assert_eq!(
            (top["author"].as_str(), top["email"].as_str()),
            (Some("Bob"), Some("bob@example.com"))
        );
        assert_eq!(top["committer"].as_str(), Some("Ada"), "who applied it commits");
        json_of(&run(&["uncommit", "--json"]));
        json_of(&run(&["restore", ".", "--json"]));
        // -x names the original; --author overrides
        json_of(&run(&["cherry-pick", "-x", &fix, "--author", me, "--json"]));
        let top = json_of(&run(&["log", "-n", "1", "--json"]))["entries"][0].clone();
        assert_eq!(
            top["message"].as_str().map(str::to_owned),
            Some(format!("fix three\n\nBecause.\n\n(cherry picked from commit {fix})"))
        );
        assert_eq!(top["author"].as_str(), Some("Ada"));
        json_of(&run(&["land", "--json"]));
        let out = run(&["cherry-pick", &fix]);
        assert!(
            text(&out).starts_with("nothing to commit: release already has the result of cherry-picking"),
            "{}",
            text(&out)
        );

        // The release branch changed the line too: a conflict, then --abort
        land("f.txt", "1\n2\nTHREE\n", "release three", me);
        json_of(&run(&["switch", "main", "--json"]));
        let other = land("f.txt", "one\n2\n3!\n", "main three", me);
        json_of(&run(&["switch", "release", "--json"]));
        let out = run(&["cherry-pick", &other]);
        assert_eq!(out.status.code(), Some(3), "{}", text(&out));
        assert!(
            text(&out).contains(&format!("could not apply {};", &other[..12])),
            "{}",
            text(&out)
        );
        let status = text(&run(&["status"]));
        assert!(status.contains("You are currently cherry-picking commit"), "{status}");
        assert!(text(&run(&["revert", "--abort"])).contains("no revert is in progress"));
        json_of(&run(&["cherry-pick", "--abort", "--json"]));
        assert_eq!(read(), "1\n2\nTHREE\n");
        assert!(text(&run(&["cherry-pick", "--abort"])).contains("no cherry-pick is in progress"));
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_rebases_a_stack_of_commits_one_by_one() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let (a, b) = (root.join("a"), root.join("b"));
        let author = ["--author", "Ada <ada@example.com>"];
        let commit = |wc: &std::path::Path, file: &str, content: &str, message: &str| {
            std::fs::write(wc.join(file), content).expect("write");
            json_of(&scn_in(
                wc,
                &[&["commit", "-m", message, "--json"][..], &author].concat(),
            ));
        };
        let text = |out: &std::process::Output| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            )
        };
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "a", "--json"]));
        commit(&a, "x.txt", "x\n", "base x");
        commit(&a, "y.txt", "y\n", "base y");
        json_of(&scn_in(&a, &["land", "--json"]));
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "b", "--json"]));
        // a stacks two commits; b changes the same lines of both files
        commit(&a, "x.txt", "ax\n", "a x");
        commit(&a, "y.txt", "ay\n", "a y");
        assert_eq!(
            text(&scn_in(&a, &["status", "-sb", "--no-ahead-behind"])),
            "## main [ahead 2]\n"
        );
        commit(&b, "x.txt", "bx\n", "b x");
        commit(&b, "y.txt", "by\n", "b y");
        json_of(&scn_in(&b, &["land", "--json"]));

        let out = scn_in(&a, &["sync"]);
        assert_eq!(out.status.code(), Some(3), "{}", text(&out));
        assert!(text(&out).contains("`scn sync --abort` goes back"), "{}", text(&out));
        let out = scn_in(&a, &["status"]);
        assert!(
            text(&out).contains("(use \"scn sync --abort\" to go back to before the sync)"),
            "{}",
            text(&out)
        );
        // Giving up puts both pending commits back as they were
        let v = json_of(&scn_in(&a, &["sync", "--abort", "--json"]));
        assert_eq!(v["schema"], "scion.cli.sync-abort/v1");
        assert_eq!(std::fs::read_to_string(a.join("x.txt")).expect("x"), "ax\n");
        assert_eq!(
            text(&scn_in(&a, &["status", "-sb", "--no-ahead-behind"])),
            "## main [ahead 2]\n"
        );
        let out = scn_in(&a, &["sync"]);
        assert_eq!(out.status.code(), Some(3), "{}", text(&out));
        json_of(&scn_in(&a, &["resolve", "--ours", "x.txt", "--json"]));
        // Finishing the first goes on with the second, which conflicts too
        let out = scn_in(&a, &["commit"]);
        assert_eq!(out.status.code(), Some(3), "{}", text(&out));
        assert!(text(&out).contains("y.txt"), "{}", text(&out));
        json_of(&scn_in(&a, &["resolve", "--ours", "y.txt", "--json"]));
        json_of(&scn_in(&a, &["commit", "--json"]));
        let log = json_of(&scn_in(&a, &["log", "-n", "3", "--json"]));
        let subjects: Vec<&str> = log["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .map(|e| e["message"].as_str().expect("message"))
            .collect();
        assert_eq!(subjects, ["a y", "a x", "b y"]);
        let v = json_of(&scn_in(&a, &["land", "--json"]));
        assert_eq!(v["rebased"], false);
        assert_eq!(std::fs::read_to_string(a.join("y.txt")).expect("y"), "ay\n");
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_takes_paths_relative_to_the_current_directory() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let wc = root.join("app");
        let author = ["--author", "Ada <ada@example.com>"];
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "--json"]));
        std::fs::create_dir_all(wc.join("src")).expect("mkdir");
        std::fs::write(wc.join("src/lib.rs"), "lib").expect("write");
        std::fs::write(wc.join("src/main.rs"), "main").expect("write");
        std::fs::write(wc.join("README.md"), "readme").expect("write");

        // From src/, `lib.rs` is src/lib.rs and `../README.md` the root's
        let v = json_of(&scn_in(
            &wc.join("src"),
            &[
                &["commit", "-m", "lib and readme", "lib.rs", "../README.md", "--json"][..],
                &author,
            ]
            .concat(),
        ));
        assert_eq!(v["insertions"], 2, "{v}");
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["added"], serde_json::json!(["src/main.rs"]));

        let out = scn_in(&wc, &[&["commit", "-m", "m", "nope.txt"][..], &author].concat());
        assert_eq!(out.status.code(), Some(1), "{out:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("no local changes in nope.txt") && err.contains("after -m"),
            "{err}"
        );
        for flag in ["-a", "--amend"] {
            let out = scn_in(&wc, &["commit", flag, "-m", "m", "src"]);
            assert_eq!(out.status.code(), Some(2), "{flag}: {out:?}");
        }
        json_of(&scn_in(
            &wc,
            &[&["commit", "-m", "the rest", "--json"][..], &author].concat(),
        ));
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["added"], serde_json::json!([]));
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_needs_force_and_takes_relative_paths() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let wc = root.join("app");
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "--json"]));
        std::fs::create_dir_all(wc.join("src/gen")).expect("mkdir");
        std::fs::write(wc.join("src/gen/x.rs"), "x").expect("write");
        std::fs::write(wc.join("top.txt"), "t").expect("write");

        let out = scn_in(&wc, &["clean"]);
        assert_eq!(out.status.code(), Some(2), "{out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("pass -f"));
        let out = scn_in(&wc, &["clean", "-n"]);
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            "Would remove src/\nWould remove top.txt\n"
        );
        assert!(wc.join("top.txt").exists());
        // From src/, `gen` is src/gen
        let v = json_of(&scn_in(&wc.join("src"), &["clean", "-fd", "gen", "--json"]));
        assert_eq!(v["removed"], serde_json::json!(["src/gen/"]));
        let out = scn_in(&wc, &["clean", "-f"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "Removing top.txt\n");
        assert!(!wc.join("top.txt").exists() && !wc.join("src").exists());
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn stash_saves_lists_shows_and_pops() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let wc = root.join("app");
        let author = ["--author", "Ada <ada@example.com>"];
        let text = |args: &[&str]| {
            let out = scn_in(&wc, args);
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            )
        };
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "--json"]));
        std::fs::write(wc.join("a.txt"), "a\n").expect("write");
        json_of(&scn_in(
            &wc,
            &[&["commit", "-m", "base", "--json"][..], &author].concat(),
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        assert_eq!(text(&["stash"]), "No local changes to save\n");

        std::fs::write(wc.join("a.txt"), "stashed\n").expect("write");
        let saved = text(&["stash"]);
        assert!(
            saved.starts_with("Saved working directory and index state WIP on main: c1"),
            "{saved}"
        );
        assert!(saved.ends_with(" base\n"), "{saved}");
        assert_eq!(std::fs::read_to_string(wc.join("a.txt")).expect("a"), "a\n");
        assert!(text(&["stash", "list"]).starts_with("stash@{0}: WIP on main: c1"));
        assert!(
            text(&["stash", "show"]).contains("a.txt | 2"),
            "{}",
            text(&["stash", "show"])
        );
        assert!(text(&["stash", "show", "-p", "stash@{0}"]).contains("+stashed"));
        let v = json_of(&scn_in(&wc, &["stash", "pop", "--json"]));
        assert_eq!(
            (v["schema"].as_str(), v["dropped"].as_bool()),
            (Some("scion.cli.stash/v1"), Some(true))
        );
        assert_eq!(std::fs::read_to_string(wc.join("a.txt")).expect("a"), "stashed\n");
        let out = scn_in(&wc, &["stash", "pop"]);
        assert_eq!(out.status.code(), Some(1), "{out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("scn stash list"));
        assert_eq!(scn_in(&wc, &["stash", "apply", "first"]).status.code(), Some(2));

        // The branch changed the same line since: the entry stays, the file gets markers
        assert!(text(&["stash", "push", "-m", "mine"]).ends_with("state mine\n"));
        let v = json_of(&scn_in(&wc, &["stash", "apply", "--json"]));
        assert_eq!(v["dropped"], false);
        json_of(&scn_in(&wc, &["restore", ".", "--json"]));
        std::fs::write(wc.join("a.txt"), "landed\n").expect("write");
        json_of(&scn_in(
            &wc,
            &[&["commit", "-m", "landed", "--json"][..], &author].concat(),
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let out = scn_in(&wc, &["stash", "pop", "0"]);
        assert_eq!(out.status.code(), Some(3), "{out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("could not apply stash@{0} cleanly"));
        assert!(
            std::fs::read_to_string(wc.join("a.txt"))
                .expect("a")
                .contains("<<<<<<<")
        );
        assert!(
            text(&["stash", "apply"]).contains("or `scn stash`"),
            "needs a clean tree"
        );
        json_of(&scn_in(&wc, &["restore", ".", "--json"]));
        assert!(text(&["stash", "drop"]).starts_with("Dropped stash@{0} (c1"));
        assert_eq!(text(&["stash", "list"]), "");
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn reflog_lists_moves_newest_first() {
    use scion_store::MetaStore;
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

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let wc = root.join("app");
        let author = ["--author", "Ada <ada@example.com>"];
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "--json"]));
        for (i, subject) in ["first", "second"].iter().enumerate() {
            std::fs::write(wc.join("a.txt"), format!("{i}")).expect("write");
            json_of(&scn_in(
                &wc,
                &[&["commit", "-m", subject, "--json"][..], &author].concat(),
            ));
            json_of(&scn_in(&wc, &["land", "--json"]));
        }
        json_of(&scn_in(&wc, &["tag", "v1", "--json"]));

        let out = scn_in(&wc, &["reflog"]);
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "{text}");
        assert!(
            lines[0].contains(" main@{0}: ") && lines[0].ends_with(" fast-forward: second"),
            "{text}"
        );
        assert!(
            lines[1].contains(" main@{1}: ") && lines[1].ends_with(" created: first"),
            "{text}"
        );
        let v = json_of(&scn_in(
            &root,
            &["reflog", "main", "-n", "1", "--repo", &format!("{url}/app"), "--json"],
        ));
        assert_eq!(v["schema"], "scion.cli.reflog/v1");
        assert_eq!(v["entries"].as_array().map(Vec::len), Some(1));
        assert_eq!(v["entries"][0]["move"], "fast-forward");
        let v = json_of(&scn_in(&wc, &["reflog", "v1", "--json"]));
        assert_eq!(
            (v["ref"].as_str(), v["entries"][0]["move"].as_str()),
            (Some("v1"), Some("created"))
        );
        let out = scn_in(&wc, &["reflog", "nope"]);
        assert_eq!(out.status.code(), Some(2), "{out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("no branch or tag \"nope\""));
    })
    .await
    .expect("test");
}

#[tokio::test(flavor = "multi_thread")]
async fn clone_commit_land_sync_workflow() {
    use scion_store::MetaStore;
    let store = Arc::new(MemStore::new());
    store.create_repo("team/app").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        // clone 到默认目录（仓库名最后一段）
        let v = json_of(&scn_in(&root, &["clone", "team/app", "--endpoint", &u, "--json"]));
        assert_eq!(v["schema"], "scion.cli.clone/v1");
        let wc = root.join("app");
        assert!(wc.join(".scion/state.json").exists());

        // 新增文件 → status → commit → land（endpoint 取自工作副本）
        std::fs::create_dir_all(wc.join("src")).expect("mkdir");
        std::fs::write(wc.join("src/lib.rs"), "pub fn f() {}\n").expect("write");
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["added"], serde_json::json!(["src/lib.rs"]));
        let out = scn_in(&wc, &["status", "-sb"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "## main\nA  src/lib.rs\n");
        let out = scn_in(&wc, &["status", "--porcelain"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "A  src/lib.rs\n");
        let out = scn_in(&wc.join("src"), &["status", "--porcelain"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "A  src/lib.rs\n", "root-relative");
        let text = scn_in(&wc.join("src"), &["status", "-s"]);
        assert_eq!(
            String::from_utf8_lossy(&text.stdout).trim(),
            "A  lib.rs",
            "works from a subdirectory, with paths relative to it"
        );
        let text = String::from_utf8_lossy(&scn_in(&wc, &["status"]).stdout).into_owned();
        assert!(
            text.starts_with("On branch main\n") && text.contains("\tnew file:   src/lib.rs"),
            "{text}"
        );
        let v = json_of(&scn_in(
            &wc,
            &["commit", "-m", "add lib", "--author", "Ada <ada@example.com>", "--json"],
        ));
        assert!(v["changeset"].as_str().expect("id").starts_with("c1"));
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert!(v["pending"].is_string());
        let v = json_of(&scn_in(&wc, &["land", "--json"]));
        assert_eq!(v["version"], 1);

        // cat / ls：工作副本内不需要 --repo
        let out = scn_in(&wc, &["cat", "main:src/lib.rs"]);
        assert_eq!(out.stdout, b"pub fn f() {}\n");
        let v = json_of(&scn_in(&wc, &["ls", "--json"]));
        assert_eq!(v["entries"][0]["name"], "src");
        assert_eq!(v["entries"][0]["kind"], "dir");
        // 工作副本外需要 --repo
        let out = scn_in(&root, &["cat", "src/lib.rs", "--endpoint", &u]);
        assert_eq!(out.status.code(), Some(2));
        let out = scn_in(&root, &["cat", "src/lib.rs", "--endpoint", &u, "--repo", "team/app"]);
        assert_eq!(out.stdout, b"pub fn f() {}\n");

        // archive：写文件（用系统的 tar / unzip 校验），或写到标准输出
        let tgz = root.join("app.tar.gz");
        let v = json_of(&scn_in(
            &wc,
            &[
                "archive",
                "--prefix",
                "app-main/",
                "-o",
                tgz.to_str().expect("path"),
                "--json",
            ],
        ));
        assert_eq!(v["schema"], "scion.cli.archive/v1");
        assert!(v["bytes"].as_u64().expect("bytes") > 0);
        let listed = std::process::Command::new("tar")
            .arg("-tzf")
            .arg(&tgz)
            .output()
            .expect("tar");
        assert_eq!(
            String::from_utf8_lossy(&listed.stdout),
            "app-main/src/\napp-main/src/lib.rs\n"
        );
        let zip = root.join("src.zip");
        let out = scn_in(
            &wc,
            &[
                "archive",
                "main:src",
                "--format",
                "zip",
                "-o",
                zip.to_str().expect("path"),
            ],
        );
        assert!(out.status.success() && out.stdout.is_empty(), "{out:?}");
        let listed = std::process::Command::new("unzip")
            .arg("-Z1")
            .arg(&zip)
            .output()
            .expect("unzip");
        assert_eq!(String::from_utf8_lossy(&listed.stdout), "src/lib.rs\n");
        let out = scn_in(&wc, &["archive", "--format", "tar"]);
        assert_eq!(out.status.code(), Some(0));
        assert_eq!(out.stdout.len() % 512, 0, "a tar stream on stdout");
        let out = scn_in(&wc, &["archive", "--format", "rar"]);
        assert_eq!(out.status.code(), Some(2));

        // Default author: `scn config` first, then git's settings; a hint when neither is set
        let cfg = root.join("scion-config.json");
        let iso = |args: &[&str], with_git: bool| {
            let mut c = Command::new(env!("CARGO_BIN_EXE_scn"));
            c.current_dir(&wc)
                .args(args)
                .env_remove("SCION_AUTHOR")
                .env("SCION_NO_DAEMON", "1")
                .env("SCION_CONFIG_FILE", &cfg)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env(
                    "GIT_CONFIG_GLOBAL",
                    root.join(if with_git { "gitconfig" } else { "none" }),
                );
            c.output().expect("run scn")
        };
        std::fs::write(
            root.join("gitconfig"),
            "[user]\n\tname = Git User\n\temail = git@example.com\n",
        )
        .expect("gitconfig");
        std::fs::write(wc.join("a1.txt"), "1").expect("write");
        let out = iso(&["commit", "-m", "anon"], false);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("scn config set user.name"),
            "{out:?}"
        );
        json_of(&iso(&["land", "--json"], false));
        std::fs::write(wc.join("a2.txt"), "2").expect("write");
        json_of(&iso(&["commit", "-m", "from git", "--json"], true));
        json_of(&iso(&["land", "--json"], true));
        json_of(&iso(&["config", "set", "user.name", "Ada", "--json"], true));
        assert_eq!(
            String::from_utf8_lossy(&iso(&["config", "get", "user.name"], true).stdout).trim(),
            "Ada"
        );
        std::fs::write(wc.join("a3.txt"), "3").expect("write");
        json_of(&iso(&["commit", "-m", "mixed", "--json"], true));
        json_of(&iso(&["land", "--json"], true));
        let log = json_of(&scn_in(&wc, &["log", "-n", "3", "--json"]));
        let who = |i: usize| {
            let e = &log["entries"][i];
            format!(
                "{} <{}>",
                e["author"].as_str().unwrap_or(""),
                e["email"].as_str().unwrap_or("")
            )
        };
        assert_eq!(who(0), "Ada <git@example.com>", "name from scn config, email from git");
        assert_eq!(who(1), "Git User <git@example.com>");
        assert_eq!(who(2), "scion <scion@localhost>");
        assert_eq!(
            iso(&["config", "set", "core.editor", "vi"], true).status.code(),
            Some(2)
        );
        // git's spellings: config KEY [VALUE], --list, --unset, --global
        let text = |args: &[&str]| String::from_utf8_lossy(&iso(args, true).stdout).into_owned();
        assert!(
            iso(&["config", "--global", "user.email", "ada@example.com"], true)
                .status
                .success()
        );
        assert_eq!(text(&["config", "user.email"]), "ada@example.com\n");
        assert!(
            text(&["config", "--list"]).contains("user.email=ada@example.com"),
            "list"
        );
        assert!(text(&["config"]).contains("user.name=Ada"), "bare config lists");
        assert!(iso(&["config", "--unset", "user.email"], true).status.success());
        assert!(!text(&["config", "-l"]).contains("user.email"));

        // restore (paths relative to the current directory), uncommit, commit --amend
        std::fs::write(wc.join("src/lib.rs"), "broken").expect("edit");
        std::fs::write(wc.join("src/extra.rs"), "x").expect("add");
        let v = json_of(&scn_in(&wc.join("src"), &["restore", "lib.rs", "--json"]));
        assert_eq!(v["restored"], serde_json::json!(["src/lib.rs"]));
        assert_eq!(
            std::fs::read_to_string(wc.join("src/lib.rs")).expect("lib"),
            "pub fn f() {}\n"
        );
        let out = scn_in(&wc, &["restore", "."]);
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("kept 1 added file(s)"),
            "{out:?}"
        );
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "extra", "--author", "Ada <ada@example.com>", "--json"],
        ));
        let v = json_of(&scn_in(&wc, &["uncommit", "--json"]));
        assert_eq!(v["schema"], "scion.cli.uncommit/v1");
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["added"], serde_json::json!(["src/extra.rs"]));
        json_of(&scn_in(
            &wc,
            &[
                "commit",
                "-m",
                "extra v1",
                "--author",
                "Ada <ada@example.com>",
                "--json",
            ],
        ));
        std::fs::write(wc.join("src/extra2.rs"), "y").expect("add");
        let out = scn_in(&wc, &["commit", "--amend", "-m", "extra files", "-m", "body"]);
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(text.starts_with("[main c1"), "{text}");
        assert!(
            text.ends_with(
                "] extra files\n 2 files changed, 2 insertions(+)\n create mode 100644 src/extra.rs\n \
                 create mode 100644 src/extra2.rs\n"
            ),
            "{text}"
        );
        json_of(&scn_in(&wc, &["land", "--json"]));
        let log = json_of(&scn_in(&wc, &["log", "-n", "1", "--json"]));
        assert_eq!(log["entries"][0]["message"], "extra files\n\nbody");
        assert_eq!(log["entries"][0]["author"], "Ada", "amend keeps the author");
        let out = scn_in(&wc, &["uncommit"]);
        assert_eq!(out.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("not yet landed"),
            "{out:?}"
        );

        // switch -c / merge, including a conflict (exit code 3)
        let v = json_of(&scn_in(&wc, &["switch", "-c", "topic", "--json"]));
        assert_eq!((v["ref"].as_str(), v["created"].as_bool()), (Some("topic"), Some(true)));
        std::fs::write(wc.join("topic.txt"), "t").expect("write");
        std::fs::write(wc.join("src/lib.rs"), "pub fn f() -> u8 { 2 }\n").expect("edit");
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "topic", "--author", "Ada <ada@example.com>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let out = scn_in(&wc, &["switch", "topic"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "Already on 'topic'\n");
        let v = json_of(&scn_in(&wc, &["switch", "main", "--json"]));
        assert_eq!(v["removed"], 1);
        let v = json_of(&scn_in(
            &wc,
            &["merge", "topic", "--author", "Ada <ada@example.com>", "--json"],
        ));
        assert!(v["changeset"].is_string());
        json_of(&scn_in(&wc, &["land", "--json"]));
        let out = scn_in(&wc, &["merge", "topic"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "Already up to date.\n");
        json_of(&scn_in(&wc, &["switch", "topic", "--json"]));
        std::fs::write(wc.join("src/lib.rs"), "pub fn f() -> u8 { 3 }\n").expect("edit");
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "t3", "--author", "Ada <ada@example.com>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        json_of(&scn_in(&wc, &["switch", "main", "--json"]));
        std::fs::write(wc.join("src/lib.rs"), "pub fn f() -> u8 { 4 }\n").expect("edit");
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "m4", "--author", "Ada <ada@example.com>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        // An explicit author: without one configured (as on CI) a hint comes first
        let out = scn_in(&wc, &["merge", "topic", "--author", "Ada <ada@example.com>"]);
        assert_eq!(out.status.code(), Some(3), "{out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).starts_with(
                "CONFLICT (content): Merge conflict in src/lib.rs\n\
                 scn: automatic merge failed; fix conflicts and then commit the result\n"
            ),
            "{out:?}"
        );
        let out = scn_in(&wc, &["status", "--short"]);
        assert!(
            String::from_utf8_lossy(&out.stdout).starts_with("UU src/lib.rs"),
            "{out:?}"
        );
        // Abort, merge again, take theirs, commit without -m (the merge's message), land
        json_of(&scn_in(&wc, &["merge", "--abort", "--json"]));
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["conflicted"], serde_json::json!([]));
        let out = scn_in(&wc, &["merge", "topic", "--json"]);
        assert_eq!(out.status.code(), Some(3));
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
        assert_eq!(
            v["conflicts"],
            serde_json::json!([{"path": "src/lib.rs", "kind": "content"}])
        );
        let out = scn_in(&wc, &["resolve", "src/lib.rs"]);
        assert_eq!(out.status.code(), Some(1), "markers remain: {out:?}");
        assert_eq!(
            scn_in(&wc, &["commit", "--author", "Ada <ada@example.com>"])
                .status
                .code(),
            Some(3),
            "unresolved"
        );
        let v = json_of(&scn_in(&wc.join("src"), &["resolve", "lib.rs", "--theirs", "--json"]));
        assert_eq!(v["remaining"], serde_json::json!([]));
        assert_eq!(
            std::fs::read_to_string(wc.join("src/lib.rs")).expect("lib"),
            "pub fn f() -> u8 { 3 }\n"
        );
        json_of(&scn_in(&wc, &["commit", "--json"]));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let log = json_of(&scn_in(&wc, &["log", "-n", "1", "--json"]));
        assert_eq!(log["entries"][0]["message"], "Merge topic into main");
        assert_eq!(log["entries"][0]["parents"].as_array().map(Vec::len), Some(2));
        assert_eq!(scn_in(&wc, &["commit"]).status.code(), Some(2), "-m required again");

        // git-style revisions and arguments
        let head = json_of(&scn_in(&wc, &["log", "-n", "1", "--json"]))["entries"][0]["id"]
            .as_str()
            .expect("id")
            .to_owned();
        let v = json_of(&scn_in(&wc, &["log", "HEAD^2", "-n", "1", "--oneline", "--json"]));
        assert_eq!(v["entries"][0]["message"], "t3", "second parent of the merge");
        let v = json_of(&scn_in(&wc, &["log", "@~1", "-n", "1", "--json"]));
        assert_eq!(v["entries"][0]["message"], "m4");
        let v = json_of(&scn_in(&wc, &["log", "-n", "5", "--json", "--", "topic.txt"]));
        assert!(v["entries"].as_array().is_some_and(|e| !e.is_empty()));
        let v = json_of(&scn_in(&wc, &["diff", "HEAD~1..HEAD", "--name-only", "--json"]));
        assert_eq!(v["files"][0]["path"], "src/lib.rs");
        assert_eq!(v["to"], head);
        let v = json_of(&scn_in(&wc, &["diff", "HEAD~1...HEAD^2", "--name-only", "--json"]));
        assert!(
            v["files"]
                .as_array()
                .is_some_and(|f| f.iter().any(|f| f["path"] == "src/lib.rs"))
        );
        let v = json_of(&scn_in(&wc, &["diff", "HEAD^", "--json", "--", "src"]));
        assert!(v["files"].as_array().is_some_and(|f| {
            f.iter()
                .all(|f| f["path"].as_str().is_some_and(|p| p.starts_with("src/")))
        }));
        let out = scn_in(&wc, &["show"]);
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(text.starts_with(&format!("commit {head}\nMerge: ")), "{text}");
        assert!(text.contains("    Merge topic into main"), "{text}");
        let v = json_of(&scn_in(&wc, &["show", "HEAD^2", "--name-only", "--json"]));
        assert_eq!(v["commit"]["message"], "t3");
        assert_eq!(v["diff"]["files"][0]["path"], "src/lib.rs");
        let out = scn_in(&wc, &["cat", "HEAD~1:src/lib.rs"]);
        assert_eq!(out.stdout, b"pub fn f() -> u8 { 4 }\n");
        json_of(&scn_in(&wc, &["branch", "old", "HEAD~1", "--json"]));
        let out = scn_in(&wc, &["branch"]);
        assert!(String::from_utf8_lossy(&out.stdout).contains("old"), "{out:?}");
        let out = scn_in(&wc, &["branch", "-d", "old"]);
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            text.starts_with("Deleted branch old (was c1") && text.ends_with(").\n"),
            "{text}"
        );
        assert!(!String::from_utf8_lossy(&scn_in(&wc, &["branch"]).stdout).contains("old"));
        std::fs::write(wc.join("am.txt"), "am").expect("write");
        json_of(&scn_in(
            &wc,
            &["commit", "-am", "am", "--author", "Ada <ada@example.com>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        assert_eq!(
            scn_in(&root, &["log", "HEAD", "--repo", "team/app", "--endpoint", &u])
                .status
                .code(),
            Some(2),
            "HEAD needs a working copy"
        );
        assert_eq!(scn_in(&wc, &["log", "--path", "a", "--", "b"]).status.code(), Some(2));

        // Transitional git spellings: they work and point at the scn command
        let hinted = |out: &std::process::Output, scn: &str| {
            let err = String::from_utf8_lossy(&out.stderr);
            assert!(err.contains(&format!("use `scn {scn}` instead")), "{err}");
            assert!(out.status.success(), "{out:?}");
        };
        let author = ["--author", "Ada <ada@example.com>"];
        hinted(&scn_in(&wc, &["checkout", "-b", "legacy"]), "switch -c NAME");
        std::fs::write(wc.join("old.txt"), "o").expect("write");
        hinted(&scn_in(&wc, &["add", "-A"]), "commit");
        json_of(&scn_in(
            &wc,
            &[&["commit", "-m", "old", "--json"][..], &author].concat(),
        ));
        assert_eq!(scn_in(&wc, &["push", "--force"]).status.code(), Some(2));
        assert_eq!(scn_in(&wc, &["push", "upstream"]).status.code(), Some(2));
        assert_eq!(
            scn_in(&wc, &["push", "origin", "main"]).status.code(),
            Some(2),
            "on legacy"
        );
        hinted(&scn_in(&wc, &["push", "-u", "origin", "legacy"]), "land");
        hinted(
            &scn_in(&wc, &["mv", "old.txt", "src"]),
            "commit (after moving the files yourself)",
        );
        assert!(wc.join("src/old.txt").exists() && !wc.join("old.txt").exists());
        hinted(
            &scn_in(&wc, &["rm", "src/old.txt"]),
            "commit (after deleting the files yourself)",
        );
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["deleted"], serde_json::json!(["old.txt"]), "a move then a delete");
        hinted(&scn_in(&wc, &["reset", "--hard"]), "restore .");
        assert!(wc.join("old.txt").exists());
        std::fs::write(wc.join("old.txt"), "changed").expect("edit");
        hinted(&scn_in(&wc, &["checkout", "--", "old.txt"]), "restore PATH");
        assert_eq!(std::fs::read_to_string(wc.join("old.txt")).expect("old"), "o");
        std::fs::write(wc.join("old.txt"), "changed again").expect("edit");
        json_of(&scn_in(&wc, &[&["commit", "-m", "c", "--json"][..], &author].concat()));
        let out = scn_in(&wc, &["pull", "--ff-only"]);
        assert_eq!(out.status.code(), Some(2), "a pending commit: {out:?}");
        hinted(&scn_in(&wc, &["reset", "--soft", "HEAD~1"]), "uncommit");
        let v = json_of(&scn_in(&wc, &["status", "--json"]));
        assert_eq!(v["modified"], serde_json::json!(["old.txt"]));
        hinted(&scn_in(&wc, &["checkout", "old.txt"]), "restore PATH");
        hinted(&scn_in(&wc, &["pull", "--rebase", "origin", "legacy"]), "sync");
        hinted(&scn_in(&wc, &["checkout", "main"]), "switch BRANCH");
        assert_eq!(scn_in(&wc, &["reset", "--hard", "HEAD~3"]).status.code(), Some(2));
        let out = scn_in(&wc, &["rebase", "--abort"]);
        assert_eq!(out.status.code(), Some(1), "no sync to abort: {out:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("use `scn sync --abort` instead"));
        assert_eq!(scn_in(&wc, &["rebase"]).status.code(), Some(2));
        for cont in [&["rebase", "--continue"][..], &["merge", "--continue"]] {
            let out = scn_in(&wc, cont);
            assert_eq!(out.status.code(), Some(2), "nothing to continue: {out:?}");
            assert!(String::from_utf8_lossy(&out.stderr).contains("use `scn commit` instead"));
        }
        assert_eq!(scn_in(&wc, &["checkout"]).status.code(), Some(2));
        assert_eq!(scn_in(&wc, &["rm", "missing.txt"]).status.code(), Some(2));
        assert_eq!(scn_in(&wc, &["mv", "missing.txt", "x"]).status.code(), Some(2));
        json_of(&scn_in(&wc, &["branch", "-d", "legacy", "--json"]));

        // A bare revision for ls / archive, as in git; :DIR forces a path
        let v = json_of(&scn_in(&wc, &["ls", "main", "--json"]));
        assert_eq!((v["revision"].as_str(), v["path"].as_str()), (Some("main"), Some("")));
        let v = json_of(&scn_in(&wc, &["ls", "src", "--json"]));
        assert_eq!(v["path"], "src", "not a revision, so a path");
        let v = json_of(&scn_in(&wc, &["ls", "HEAD~1", "--json"]));
        assert_eq!(v["path"], "");
        let out = scn_in(&wc, &["ls", ":main"]);
        assert_eq!(out.status.code(), Some(1), "forced path that does not exist: {out:?}");
        let tar = root.join("bare.tar");
        json_of(&scn_in(
            &wc,
            &[
                "archive",
                "main",
                "--format",
                "tar",
                "-o",
                tar.to_str().expect("path"),
                "--json",
            ],
        ));
        assert!(std::fs::metadata(&tar).expect("tar").len() > 0);

        // Tags
        let v = json_of(&scn_in(&wc, &["tag", "v1.0", "--json"]));
        assert_eq!(
            (v["name"].as_str(), v["action"].as_str()),
            (Some("v1.0"), Some("created"))
        );
        json_of(&scn_in(&wc, &["tag", "v0.9", "HEAD~1", "--json"]));
        assert!(scn_in(&wc, &["tag", "-l"]).status.success());
        let v = json_of(&scn_in(&wc, &["tag", "--json"]));
        let names: Vec<&str> = v["tags"]
            .as_array()
            .expect("tags")
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        assert_eq!(names, ["v0.9", "v1.0"]);
        assert!(
            !String::from_utf8_lossy(&scn_in(&wc, &["branch"]).stdout).contains("tags/"),
            "tags are not branches"
        );
        let v = json_of(&scn_in(&wc, &["log", "v1.0", "-n", "1", "--json"]));
        assert_eq!(v["entries"][0]["id"].as_str(), Some(head_now(&wc).as_str()));
        assert_eq!(scn_in(&wc, &["tag", "v1.0"]).status.code(), Some(1), "already exists");
        json_of(&scn_in(&wc, &["tag", "-d", "v0.9", "--json"]));
        let v = json_of(&scn_in(&wc, &["tag", "--json"]));
        assert_eq!(v["tags"].as_array().map(Vec::len), Some(1));

        // 第二个工作副本落后后 sync
        let v = json_of(&scn_in(
            &root,
            &["clone", "team/app", "second", "--endpoint", &u, "--json"],
        ));
        assert!(v["base"].is_string());
        std::fs::write(wc.join("src/lib.rs"), "pub fn f() -> u8 { 1 }\n").expect("edit");
        json_of(&scn_in(&wc, &["commit", "-m", "edit", "--json"]));
        let out = scn_in(&wc, &["land"]);
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(text.starts_with(&format!("To {u}/team/app\n   c1")), "{text}");
        assert!(text.ends_with("  main -> main\n"), "{text}");
        let out = scn_in(&root.join("second"), &["sync"]);
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            text.starts_with("Updating c1") && text.ends_with("\nFast-forward\n 1 file changed\n"),
            "{text}"
        );
        let out = scn_in(&root.join("second"), &["sync"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "Already up to date.\n");
        assert_eq!(
            std::fs::read_to_string(root.join("second/src/lib.rs")).expect("read"),
            "pub fn f() -> u8 { 1 }\n"
        );

        // 错误：没有改动时 land → 退出码 1 并给出提示
        let out = scn_in(&wc, &["land"]);
        assert_eq!(out.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&out.stderr).contains("hint:"));
        // 不在工作副本中
        let out = scn_in(&root, &["status", "--json"]);
        assert_eq!(out.status.code(), Some(1));
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn import_git_repository() {
    use scion_store::MetaStore;
    let store = Arc::new(MemStore::new());
    store.create_repo("imported").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        let git = |args: &[&str]| {
            let out = Command::new("git")
                .current_dir(root.join("src-repo"))
                .args(args)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_AUTHOR_NAME", "A")
                .env("GIT_AUTHOR_EMAIL", "a@x")
                .env("GIT_COMMITTER_NAME", "A")
                .env("GIT_COMMITTER_EMAIL", "a@x")
                .output()
                .expect("git");
            assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        };
        std::fs::create_dir(root.join("src-repo")).expect("mkdir");
        git(&["init", "-q", "-b", "main"]);
        std::fs::write(root.join("src-repo/hello.txt"), "from git\n").expect("write");
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "first"]);

        let v = json_of(&scn_in(
            &root,
            &[
                "import",
                "git",
                "src-repo",
                "--repo",
                "imported",
                "--endpoint",
                &u,
                "--json",
            ],
        ));
        assert_eq!(v["schema"], "scion.cli.import/v1");
        assert_eq!(v["commits"], 1);
        let out = scn_in(&root, &["cat", "hello.txt", "--repo", "imported", "--endpoint", &u]);
        assert_eq!(out.stdout, b"from git\n");
        let text = scn_in(
            &root,
            &["import", "git", "src-repo", "--repo", "imported", "--endpoint", &u],
        );
        assert!(String::from_utf8_lossy(&text.stdout).contains("imported 1 commits"));
        // 不是 git 仓库
        let out = scn_in(&root, &["import", "git", ".", "--repo", "imported", "--endpoint", &u]);
        assert_eq!(out.status.code(), Some(1));
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn log_and_diff_commands() {
    use scion_store::MetaStore;
    let store = Arc::new(MemStore::new());
    store.create_repo("hist").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        json_of(&scn_in(&root, &["clone", "hist", "--endpoint", &u, "--json"]));
        let wc = root.join("hist");
        std::fs::write(wc.join("a.txt"), "one\ntwo\n").expect("write");
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "first", "--author", "Ada <a@x>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        std::fs::write(wc.join("a.txt"), "one\nthree\n").expect("edit");
        std::fs::write(wc.join("b.bin"), [0u8, 1, 2]).expect("binary");

        // 本地未提交的改动
        let out = scn_in(&wc, &["diff"]);
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        assert!(text.contains("-two") && text.contains("+three"), "{text}");
        assert!(text.contains("Binary files /dev/null and b/b.bin differ"), "{text}");
        let out = scn_in(&wc, &["diff", "--stat"]);
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            " a.txt | 2 +-\n b.bin | Bin 0 -> 3 bytes\n 2 files changed, 1 insertion(+), 1 deletion(-)\n"
        );
        let out = scn_in(&wc, &["diff", "--name-status"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "M\ta.txt\nA\tb.bin\n");
        let out = scn_in(&wc, &["show", "--name-status"]);
        assert!(out.status.success(), "{out:?}");
        let out = scn_in(&wc, &["show", "--stat"]);
        assert!(String::from_utf8_lossy(&out.stdout).contains(" a.txt | "), "{out:?}");
        let v = json_of(&scn_in(&wc, &["diff", "--name-only", "--json"]));
        assert_eq!(v["files"].as_array().expect("files").len(), 2);
        let v = json_of(&scn_in(&wc, &["diff", "--path", "a.txt", "--json"]));
        assert_eq!(v["files"][0]["status"], "M");

        json_of(&scn_in(&wc, &["commit", "-m", "second", "--json"]));
        json_of(&scn_in(&wc, &["land", "--json"]));

        // log：从新到旧，按路径过滤
        let v = json_of(&scn_in(&wc, &["log", "--json"]));
        let msgs: Vec<_> = v["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .map(|e| e["message"].as_str().expect("msg").to_owned())
            .collect();
        assert_eq!(msgs, ["second", "first"]);
        let first_id = v["entries"][1]["id"].as_str().expect("id").to_owned();
        let v = json_of(&scn_in(&wc, &["log", "--path", "b.bin", "--json"]));
        assert_eq!(v["entries"].as_array().expect("entries").len(), 1);
        let text = String::from_utf8_lossy(&scn_in(&wc, &["log", "-n", "1"]).stdout).to_string();
        assert!(text.contains("second") && !text.contains("first"), "{text}");
        assert!(text.starts_with("commit c1") && text.contains("\nDate:   "), "{text}");
        let text = String::from_utf8_lossy(&scn_in(&wc, &["log", "--oneline", "-1"]).stdout).to_string();
        assert_eq!(text.lines().count(), 1, "git's -N: {text}");
        let pretty = String::from_utf8_lossy(&scn_in(&wc, &["log", "--pretty=oneline", "-1"]).stdout).to_string();
        assert_eq!(pretty, text);
        let out = scn_in(&wc, &["commit", "a message"]);
        assert_eq!(out.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("goes after -m"),
            "{out:?}"
        );
        let text = String::from_utf8_lossy(&scn_in(&wc, &["log", "--oneline"]).stdout).to_string();
        let lines: Vec<&str> = text.lines().collect();
        assert!(
            lines.len() == 2 && lines[0].ends_with(" second") && lines[0].len() == 19,
            "{text}"
        );

        // 某个提交引入的改动；两个版本之间的差异
        let v = json_of(&scn_in(&wc, &["diff", "main", "--name-only", "--json"]));
        let names: Vec<_> = v["files"]
            .as_array()
            .expect("files")
            .iter()
            .map(|f| f["path"].as_str().expect("p").to_owned())
            .collect();
        assert_eq!(names, ["a.txt", "b.bin"]);
        let text = String::from_utf8_lossy(&scn_in(&wc, &["diff", &first_id, "main"]).stdout).to_string();
        assert!(text.contains("+three"), "{text}");
        // blame：a.txt 第 1 行来自 first，第 2 行来自 second
        let v = json_of(&scn_in(&wc, &["blame", "a.txt", "--json"]));
        let commits: Vec<_> = v["lines"]
            .as_array()
            .expect("lines")
            .iter()
            .map(|l| l["commit"].as_str().expect("c").to_owned())
            .collect();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0], first_id);
        assert_ne!(commits[1], first_id);
        let text = String::from_utf8_lossy(&scn_in(&wc, &["blame", "main:a.txt"]).stdout).to_string();
        assert!(text.contains(" 2) three"), "{text}");
        let text = String::from_utf8_lossy(&scn_in(&wc, &["blame", "-L", "2", "main:a.txt"]).stdout).to_string();
        assert!(text.lines().count() == 1 && text.contains(" 2) three"), "{text}");
        assert_eq!(
            scn_in(&wc, &["blame", "-L", "3,4", "main:a.txt"]).status.code(),
            Some(2),
            "past the end"
        );
        // restore --source: an older version as a local change
        let out = scn_in(&wc, &["restore", "--source", "HEAD~1", "a.txt"]);
        assert!(out.status.success(), "{out:?}");
        assert_eq!(std::fs::read_to_string(wc.join("a.txt")).expect("a"), "one\ntwo\n");
        assert_eq!(
            String::from_utf8_lossy(&scn_in(&wc, &["status", "-s"]).stdout),
            "M  a.txt\n"
        );
        json_of(&scn_in(&wc, &["restore", "a.txt", "--json"]));
        let out = scn_in(&wc, &["checkout", "HEAD~1", "--", "a.txt"]);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("restore --source REV PATH"),
            "{out:?}"
        );
        assert_eq!(std::fs::read_to_string(wc.join("a.txt")).expect("a"), "one\ntwo\n");
        json_of(&scn_in(&wc, &["restore", ".", "--json"]));
        let out = scn_in(&wc, &["diff", "a", "b", "c"]);
        assert_eq!(out.status.code(), Some(2));
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

/// 默认经 daemon：第一次连接时自动拉起，之后 status / stop 管理它。
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn daemon_is_started_on_demand_and_can_be_stopped() {
    let store = Arc::new(MemStore::new());
    scion_store::MetaStore::create_repo(store.as_ref(), "mono")
        .await
        .expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("d.sock");
    let run = |args: &[&str]| {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let socket = socket.clone();
        let cache = dir.path().join("cache");
        let url = url.clone();
        std::thread::spawn(move || {
            Command::new(env!("CARGO_BIN_EXE_scn"))
                .args(&args)
                .env("SCION_ENDPOINT", &url)
                .env("SCION_DAEMON_SOCKET", &socket)
                .env("SCION_DAEMON_CACHE_DIR", &cache)
                .env_remove("SCION_NO_DAEMON")
                .env_remove("SCION_TOKEN")
                .output()
                .expect("run scn")
        })
        .join()
        .expect("thread")
    };
    let out = run(&["daemon", "status", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["running"], false);

    // doctor 经 daemon 连接：daemon 被自动拉起
    let out = run(&["doctor", "--json"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert!(v["objects"].as_str().expect("objects").starts_with("daemon"), "{v}");
    assert_eq!(v["role"], "server", "capabilities come from the server");

    let out = run(&["daemon", "status", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["running"], true);
    assert!(v["pid"].as_u64().is_some());

    // 写入与读取都经 daemon
    let out = run(&["ls", "--repo", "mono", "main:"]);
    assert!(!out.status.success(), "ref main does not exist yet");
    let out = run(&["daemon", "stop", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["stopped"], true, "{v}");
    assert!(!socket.exists());
    let out = run(&["daemon", "stop"]);
    assert!(String::from_utf8_lossy(&out.stdout).contains("not running"));
}

/// 要求认证的服务端（可选 OIDC 登录）：返回地址与用来创建令牌的 Guard。
async fn auth_server(oidc_issuer: Option<String>) -> (String, Arc<scion_api::scion_auth::Guard>) {
    let toml = oidc_issuer
        .map(|i| format!("[oidc]\nissuer = \"{i}\"\nclient_id = \"scn\"\nsubject_claim = \"preferred_username\"\n"))
        .unwrap_or_default();
    auth_server_with(&toml).await
}

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

fn scn_auth(url: &str, creds: &std::path::Path, args: &[&str], stdin: Option<&str>) -> std::process::Output {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(args)
        .env("SCION_ENDPOINT", url)
        .env("SCION_CREDENTIALS_FILE", creds)
        .env("SCION_NO_DAEMON", "1")
        .env_remove("SCION_TOKEN")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
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
    child.wait_with_output().expect("wait")
}

#[tokio::test(flavor = "multi_thread")]
async fn login_with_token_whoami_and_logout() {
    let (url, guard) = auth_server(None).await;
    let (pat, _) = guard
        .create_token("pat", "ci", vec!["eng".into()], "", None)
        .await
        .expect("pat");
    let dir = tempfile::tempdir().expect("tempdir");
    let creds = dir.path().join("credentials.json");
    let run = |args: &[&str], stdin: Option<&str>| {
        let (url, creds) = (url.clone(), creds.clone());
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let stdin = stdin.map(str::to_owned);
        std::thread::spawn(move || {
            let a: Vec<&str> = args.iter().map(String::as_str).collect();
            scn_auth(&url, &creds, &a, stdin.as_deref())
        })
        .join()
        .expect("thread")
    };
    let out = run(&["whoami", "--json"], None);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(
        (v["subject"].as_str(), v["auth_required"].as_bool()),
        (Some("anonymous"), Some(true))
    );
    // 无效令牌：认证失败，退出码 5
    let out = run(&["login", "--token", "scn_pat_nope"], None);
    assert_eq!(out.status.code(), Some(5), "{}", String::from_utf8_lossy(&out.stderr));
    // 从标准输入读取令牌
    let out = run(&["login", "--with-token", "--json"], Some(&format!("{pat}\n")));
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["subject"], "ci");
    let out = run(&["whoami"], None);
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("ci on"),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    let out = run(&["logout"], None);
    assert!(String::from_utf8_lossy(&out.stdout).contains("logged out"));
    let out = run(&["logout"], None);
    assert!(String::from_utf8_lossy(&out.stdout).contains("no saved credentials"));
    // The token as an argument, before or after the server
    let out = run(&["login", &pat, &url, "--json"], None);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let out = run(&["login", &url, &pat, "--json"], None);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(run(&["login", &pat, "--token", &pat], None).status.code(), Some(2));
    assert_eq!(run(&["login", &pat, "--with-token"], Some("x")).status.code(), Some(2));
    assert_eq!(run(&["login", "a", "b"], None).status.code(), Some(2));
    let out = run(&["login", "--token", " "], None);
    assert_eq!(out.status.code(), Some(2));
    // 服务端没有配置浏览器登录
    let out = run(&["login"], None);
    assert!(String::from_utf8_lossy(&out.stderr).contains("--token"));
}

#[tokio::test(flavor = "multi_thread")]
async fn browser_login_saves_a_refresh_token() {
    let idp = scion_api::scion_auth::testing::FakeIdp::start().await;
    let (url, _) = auth_server(Some(idp.base())).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let creds = dir.path().join("credentials.json");
    let (u, c) = (url.clone(), creds.clone());
    let login = std::thread::spawn(move || scn_auth(&u, &c, &["login", "--json"], None));
    // 用户在浏览器里完成登录
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    idp.approve("dc-1");
    let out = login.join().expect("thread");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stderr).contains("ABCD-EFGH"));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["subject"], "alice");
    let saved = std::fs::read_to_string(&creds).expect("saved");
    assert!(saved.contains("scn_refresh_"));
    let out = std::thread::spawn(move || scn_auth(&url, &creds, &["whoami", "--json"], None))
        .join()
        .expect("thread");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(v["subject"], "alice");
}

#[tokio::test(flavor = "multi_thread")]
async fn sparse_checkout_commands() {
    use scion_store::MetaStore;
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    let seed = scion_client::Client::builder()
        .endpoint(&url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let cs = seed
        .changeset(None)
        .put("top.txt", "t")
        .put("a/x.txt", "x")
        .put("b/y.txt", "y")
        .create()
        .await
        .expect("seed");
    seed.land("main", &cs.id).await.expect("land");

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        let v = json_of(&scn_in(
            &root,
            &["clone", "mono", "--sparse", "a/", "--endpoint", &u, "--json"],
        ));
        assert_eq!(v["sparse"], serde_json::json!(["a"]));
        let wc = root.join("mono");
        assert!(wc.join("top.txt").exists() && wc.join("a/x.txt").exists());
        assert!(!wc.join("b").exists());
        let out = scn_in(&root, &["clone", "mono", "w2", "--sparse", "a,b", "--endpoint", &u]);
        assert!(out.status.success() && out.stdout.is_empty(), "{out:?}");
        assert_eq!(String::from_utf8_lossy(&out.stderr), "Cloning into 'w2'...\n");
        let out = scn_in(&root, &["clone", "mono", "w3", "-q", "--endpoint", &u]);
        assert!(out.status.success() && out.stderr.is_empty(), "{out:?}");

        let v = json_of(&scn_in(&wc, &["sparse", "add", "b", "--json"]));
        assert_eq!(v["schema"], "scion.cli.sparse/v1");
        assert_eq!(v["dirs"], serde_json::json!(["a", "b"]));
        assert_eq!(v["written"], 1);
        assert!(wc.join("b/y.txt").exists());
        let out = scn_in(&wc, &["sparse", "ls"]);
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "sparse checkout:\n  a\n  b"
        );
        let out = scn_in(&wc, &["sparse", "rm", "a", "b"]);
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "sparse checkout: root files only\n(0 written, 2 removed)"
        );
        // 不在范围中的目录：用法错误
        let out = scn_in(&wc, &["sparse", "rm", "zzz"]);
        assert_eq!(out.status.code(), Some(2));
        // 范围外的文件
        std::fs::create_dir_all(wc.join("b")).expect("mkdir");
        std::fs::write(wc.join("b/new.txt"), "n").expect("write");
        let out = scn_in(&wc, &["status"]);
        assert_eq!(out.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&out.stderr).contains("outside the sparse checkout"));
        std::fs::remove_dir_all(wc.join("b")).expect("rm");
        let v = json_of(&scn_in(&wc, &["sparse", "set", "b", "--json"]));
        assert_eq!(v["dirs"], serde_json::json!(["b"]));
        let out = scn_in(&wc, &["sparse", "disable"]);
        assert!(String::from_utf8_lossy(&out.stdout).starts_with("full checkout"));
        assert!(wc.join("a/x.txt").exists() && wc.join("b/y.txt").exists());
        let v = json_of(&scn_in(&wc, &["sparse", "ls", "--json"]));
        assert!(v["dirs"].is_null());
        let out = scn_in(&wc, &["sparse", "add", "a"]);
        assert_eq!(out.status.code(), Some(2), "full checkout: use set");
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn repo_and_branch_commands() {
    let store = Arc::new(MemStore::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        let e = ["--endpoint", u.as_str()];
        let run = |args: &[&str]| {
            let mut v: Vec<&str> = args.to_vec();
            v.extend_from_slice(&e);
            scn_in(&root, &v)
        };
        let v = json_of(&run(&[
            "repo",
            "create",
            "org/app",
            "--description",
            "the app",
            "--json",
        ]));
        assert_eq!(
            (v["schema"].as_str(), v["default_branch"].as_str()),
            (Some("scion.cli.repo/v1"), Some("main"))
        );
        let out = run(&["repo", "create", "org/app"]);
        assert_eq!(out.status.code(), Some(1), "already exists");
        json_of(&run(&["repo", "create", "org/lib", "--json"]));
        let v = json_of(&run(&["repo", "ls", "org/", "--limit", "1", "--json"]));
        assert_eq!(v["repos"].as_array().map(Vec::len), Some(1));
        assert_eq!(v["truncated"], true);
        let out = run(&["repo", "ls"]);
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "org/app  (default branch main)\norg/lib  (default branch main)"
        );
        let v = json_of(&run(&["repo", "set", "org/app", "--description", "renamed", "--json"]));
        assert_eq!(v["description"], "renamed");
        assert_eq!(
            run(&["repo", "set", "org/app"]).status.code(),
            Some(2),
            "nothing to change"
        );

        // 提交后建分支、列出、删除
        json_of(&run(&["clone", "org/app", "--json"]));
        let wc = root.join("app");
        std::fs::write(wc.join("a.txt"), "a").expect("write");
        json_of(&scn_in(&wc, &["commit", "-m", "a", "--json"]));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let v = json_of(&scn_in(&wc, &["branch", "create", "dev", "--json"]));
        assert_eq!(
            (v["action"].as_str(), v["version"].as_u64()),
            (Some("created"), Some(1))
        );
        let out = scn_in(&wc, &["branch", "ls"]);
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert_eq!(text, "  dev\n* main\n");
        let out = scn_in(&wc, &["branch", "--show-current"]);
        assert_eq!(String::from_utf8_lossy(&out.stdout), "main\n");
        let text = String::from_utf8_lossy(&scn_in(&wc, &["branch", "-av"]).stdout).into_owned();
        assert!(text.contains("* main c1") && text.contains("  dev  c1"), "{text}");

        assert_eq!(
            scn_in(&wc, &["branch", "rm", "main"]).status.code(),
            Some(1),
            "default branch"
        );
        let v = json_of(&scn_in(&wc, &["branch", "rm", "dev", "--json"]));
        assert_eq!(v["action"], "deleted");
        let v = json_of(&run(&["branch", "ls", "--repo", "org/app", "--json"]));
        assert_eq!(v["branches"].as_array().map(Vec::len), Some(1));

        // fork、删除、恢复
        let v = json_of(&run(&["repo", "fork", "org/app", "me/app", "--json"]));
        assert_eq!(v["forked_from"], "org/app");
        let v = json_of(&run(&["repo", "rm", "org/app", "--json"]));
        assert!(v["purge_after_ms"].as_i64().is_some_and(|p| p > 0));
        let out = run(&["repo", "ls", "--deleted"]);
        assert!(String::from_utf8_lossy(&out.stdout).contains("deleted, purged after"));
        let v = json_of(&run(&["repo", "info", "org/app", "--json"]));
        assert!(v["deleted_at_ms"].is_i64());
        let v = json_of(&run(&["repo", "restore", "org/app", "--json"]));
        assert!(v["deleted_at_ms"].is_null());
        let out = run(&["repo", "info", "org/app"]);
        assert!(String::from_utf8_lossy(&out.stdout).contains("renamed"));
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn repository_urls_name_the_server() {
    let store = Arc::new(MemStore::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(ServerConfig::default()),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        let repo_url = format!("{u}/org/app");
        // No --endpoint anywhere: the server comes from the URL
        json_of(&scn_in(&root, &["repo", "create", &repo_url, "--json"]));
        let v = json_of(&scn_in(&root, &["repo", "info", &repo_url, "--json"]));
        assert_eq!(v["name"], "org/app");
        let v = json_of(&scn_in(&root, &["repo", "ls", &format!("{u}/"), "--json"]));
        assert_eq!(v["repos"].as_array().map(Vec::len), Some(1));
        json_of(&scn_in(&root, &["doctor", &u, "--json"]));
        assert_ne!(
            scn_in(&root, &["whoami", &u]).status.code(),
            Some(2),
            "a server URL is accepted"
        );
        assert_eq!(scn_in(&root, &["whoami", "host:1"]).status.code(), Some(2));

        let v = json_of(&scn_in(&root, &["clone", &format!("{repo_url}/"), "--json"]));
        assert_eq!(v["repo"], "org/app");
        let wc = root.join("app");

        // Inside the working copy, the server it was cloned from is remembered
        std::fs::write(wc.join("README.md"), "hello\n").expect("write");
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "readme", "--author", "Ada <ada@example.com>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let v = json_of(&scn_in(&wc, &["log", "--json"]));
        assert_eq!(v["entries"].as_array().map(Vec::len), Some(1));

        // --repo takes a URL too, outside a working copy
        let out = scn_in(&root, &["cat", "README.md", "--repo", &repo_url]);
        assert_eq!(out.stdout, b"hello\n");
        json_of(&scn_in(
            &root,
            &["repo", "fork", &repo_url, &format!("{u}/org/fork"), "--json"],
        ));
        let out = scn_in(&root, &["cat", "README.md", "--repo", &format!("{u}/org/fork")]);
        assert_eq!(out.stdout, b"hello\n");

        // scn remote: show the URL, point the working copy at the fork
        let text = |args: &[&str]| String::from_utf8_lossy(&scn_in(&wc, args).stdout).into_owned();
        assert_eq!(text(&["remote"]), "origin\n");
        assert_eq!(
            text(&["remote", "-v"]),
            format!("origin\t{repo_url} (fetch)\norigin\t{repo_url} (push)\n")
        );
        assert_eq!(text(&["remote", "get-url", "origin"]), format!("{repo_url}\n"));
        assert_eq!(text(&["remote", "show", "origin"]), text(&["remote", "-v"]));
        // git commands scn does without explain themselves
        for args in [
            &["remote", "add", "up", "http://x:1/y"][..],
            &["init"],
            &["fetch", "origin"],
        ] {
            let out = scn_in(&wc, args);
            assert_eq!(out.status.code(), Some(2), "{args:?}");
            assert!(
                String::from_utf8_lossy(&out.stderr).contains('`'),
                "says what to use: {out:?}"
            );
        }
        assert_eq!(scn_in(&wc, &["remote", "get-url", "upstream"]).status.code(), Some(2));
        let out = scn_in(&wc, &["fetch"]);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("`scn log main`"),
            "{out:?}"
        );
        let log = |rev: &str| json_of(&scn_in(&wc, &["log", rev, "-n", "1", "--json"]))["entries"][0]["id"].clone();
        assert_eq!(log("origin/main"), log("main"));
        assert_eq!(scn_in(&wc, &["remote", "set-url", "not-a-url"]).status.code(), Some(2));
        assert_eq!(
            scn_in(&wc, &["remote", "set-url", &u]).status.code(),
            Some(2),
            "names no repository"
        );
        json_of(&scn_in(&root, &["repo", "create", &format!("{u}/org/empty"), "--json"]));
        let out = scn_in(&wc, &["remote", "set-url", &format!("{u}/org/empty")]);
        assert_eq!(out.status.code(), Some(2), "{out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("does not have commit"),
            "{out:?}"
        );
        assert_eq!(
            scn_in(&wc, &["remote", "set-url", &format!("{u}/org/missing")])
                .status
                .code(),
            Some(1)
        );
        assert_eq!(text(&["remote", "get-url"]), format!("{repo_url}\n"), "unchanged");
        let fork_url = format!("{u}/org/fork");
        let out = scn_in(&wc, &["remote", "set-url", "origin", &fork_url]);
        assert!(out.status.success() && out.stdout.is_empty(), "{out:?}");
        let v = json_of(&scn_in(&wc, &["remote", "--json"]));
        assert_eq!(
            (v["repo"].as_str(), v["endpoint"].as_str()),
            (Some("org/fork"), Some(u.as_str()))
        );
        std::fs::write(wc.join("fork.txt"), "f\n").expect("write");
        json_of(&scn_in(
            &wc,
            &["commit", "-m", "fork", "--author", "Ada <ada@example.com>", "--json"],
        ));
        json_of(&scn_in(&wc, &["land", "--json"]));
        let out = scn_in(&root, &["cat", "fork.txt", "--repo", &fork_url]);
        assert_eq!(out.stdout, b"f\n", "landed on the fork");
        assert_eq!(
            scn_in(&root, &["cat", "fork.txt", "--repo", &repo_url]).status.code(),
            Some(1)
        );

        // Malformed URLs are usage errors
        assert_eq!(scn_in(&root, &["clone", &format!("{u}/a?b")]).status.code(), Some(2));
        assert_eq!(scn_in(&root, &["clone", &u]).status.code(), Some(2));
        assert_eq!(
            scn_in(&root, &["repo", "fork", &repo_url, "http://elsewhere:1/x"])
                .status
                .code(),
            Some(2)
        );
    })
    .await
    .expect("join");
    let _ = tx.send(());
}

#[tokio::test(flavor = "multi_thread")]
async fn organizations_from_the_command_line() {
    let (url, guard) = auth_server_with("[authorization]\nchain = [\"policy\", \"orgs\", \"personal\"]").await;
    let token = |subject: &'static str| {
        let guard = guard.clone();
        async move {
            guard
                .create_token("pat", subject, vec![], "", None)
                .await
                .expect("pat")
                .0
        }
    };
    let (alice, bob) = (token("alice").await, token("bob").await);
    let u = url.clone();
    tokio::task::spawn_blocking(move || {
        let scn = |token: &str, args: &[&str]| {
            Command::new(env!("CARGO_BIN_EXE_scn"))
                .args(args)
                .env("SCION_ENDPOINT", &u)
                .env("SCION_TOKEN", token)
                .env("SCION_NO_DAEMON", "1")
                .output()
                .expect("run scn")
        };
        let text = |token: &str, args: &[&str]| {
            let out = scn(token, args);
            assert!(out.status.success(), "{args:?}: {out:?}");
            String::from_utf8_lossy(&out.stdout).into_owned()
        };
        assert_eq!(
            text(&alice, &["org", "create", "acme"]),
            "created organization acme (private); you are its owner\n"
        );
        assert_eq!(
            text(&bob, &["org", "create", &format!("{u}/open"), "--internal"]),
            "created organization open (internal); you are its owner\n"
        );
        assert_eq!(text(&bob, &["org", "ls"]), "acme  private\nopen  internal\n");
        assert_eq!(text(&bob, &["org", "ls", "--mine"]), "open  internal\n");
        assert!(text(&bob, &["org", "info", "acme"]).starts_with("acme (private)\ncreated by alice on "));
        assert_eq!(
            text(&alice, &["org", "member", "add", "acme", "bob"]),
            "bob is developer of acme\n",
            "developer by default"
        );
        assert_eq!(
            text(&alice, &["org", "member", "add", "acme", "bob", "--role", "maintainer"]),
            "bob is maintainer of acme\n"
        );
        assert_eq!(
            text(&bob, &["org", "member", "ls", "acme"]),
            "alice  owner\nbob    maintainer\n"
        );
        let v: serde_json::Value =
            serde_json::from_slice(&scn(&bob, &["org", "member", "ls", "acme", "--json"]).stdout).expect("json");
        assert_eq!(v["members"][1]["role"], "maintainer");
        assert_eq!(
            scn(&bob, &["org", "member", "add", "acme", "eve"]).status.code(),
            Some(5),
            "not an owner"
        );
        assert_eq!(
            text(&alice, &["org", "visibility", "acme", "internal"]),
            "acme is now internal\n"
        );
        assert_eq!(
            text(&bob, &["org", "member", "rm", "acme", "bob"]),
            "removed bob from acme\n"
        );
        assert_eq!(text(&bob, &["org", "rm", "open"]), "deleted organization open\n");
        let out = scn(&alice, &["org", "info", "open"]);
        assert_eq!(out.status.code(), Some(1), "{out:?}");
        let v: serde_json::Value = serde_json::from_slice(&scn(&alice, &["org", "ls", "--json"]).stdout).expect("json");
        assert_eq!(v["orgs"][0]["name"], "acme");
    })
    .await
    .expect("join");

    // Without orgs in the chain the command says how to turn them on
    let (url, guard) = auth_server_with("").await;
    let t = guard
        .create_token("pat", "alice", vec![], "", None)
        .await
        .expect("pat")
        .0;
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args(["org", "ls"])
        .env("SCION_ENDPOINT", &url)
        .env("SCION_TOKEN", &t)
        .env("SCION_NO_DAEMON", "1")
        .output()
        .expect("run scn");
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("[authorization] chain"),
        "{out:?}"
    );
}
