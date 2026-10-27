//! `scn workspace`: more working copies of a repository, made from one that has the files.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

fn scn_in(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_scn"))
        .current_dir(dir)
        .args(args)
        .env_remove("SCION_ENDPOINT")
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_REPO")
        .env("SCION_AUTHOR", "Ada <ada@x>")
        .env("SCION_NO_DAEMON", "1")
        .output()
        .expect("run scn")
}

fn json_of(out: &std::process::Output) -> serde_json::Value {
    assert!(out.status.success(), "{out:?}");
    serde_json::from_slice(&out.stdout).expect("json")
}

fn text(dir: &Path, args: &[&str]) -> String {
    let out = scn_in(dir, args);
    assert!(out.status.success(), "{out:?}");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn workspaces_are_made_from_this_working_copy() {
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
    let root = tmp.path().canonicalize().expect("canonical");
    tokio::task::spawn_blocking(move || {
        json_of(&scn_in(&root, &["clone", &format!("{url}/app"), "main-wc", "--json"]));
        let wc = root.join("main-wc");
        for i in 0..20 {
            std::fs::create_dir_all(wc.join(format!("dir{}", i % 4))).expect("mkdir");
            std::fs::write(wc.join(format!("dir{}/f{i}.txt", i % 4)), format!("file {i}\n")).expect("write");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::write(wc.join("run.sh"), "#!/bin/sh\n").expect("write");
            std::fs::set_permissions(wc.join("run.sh"), std::fs::Permissions::from_mode(0o755)).expect("chmod");
            std::os::unix::fs::symlink("run.sh", wc.join("link")).expect("symlink");
        }
        json_of(&scn_in(&wc, &["commit", "-m", "base", "--json"]));
        json_of(&scn_in(&wc, &["land", "--json"]));

        // A new branch named after the directory, at HEAD: every file copied
        let v = json_of(&scn_in(&wc, &["workspace", "add", "../agent-1", "--json"]));
        assert_eq!(v["schema"], "scion.cli.workspace/v1");
        assert_eq!(
            (v["ref"].as_str(), v["new_branch"].as_bool()),
            (Some("agent-1"), Some(true))
        );
        assert_eq!(
            (v["copied"].as_u64(), v["fetched"].as_u64()),
            (Some(21), Some(1)),
            "the link is fetched: {v}"
        );
        let a1 = root.join("agent-1");
        assert_eq!(
            std::fs::read_to_string(a1.join("dir3/f7.txt")).expect("read"),
            "file 7\n"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(a1.join("run.sh")).expect("meta").permissions().mode();
            assert_eq!(mode & 0o111, 0o111, "executable kept");
            assert_eq!(
                std::fs::read_link(a1.join("link")).expect("link").to_str(),
                Some("run.sh")
            );
        }
        assert_eq!(text(&a1, &["status", "-s"]), "", "clean");
        assert!(text(&a1, &["status"]).starts_with("On branch agent-1"));

        // A file changed here is fetched as committed, not copied
        std::fs::write(wc.join("dir0/f0.txt"), "local edit\n").expect("write");
        let v = json_of(&scn_in(&wc, &["workspace", "add", "../on-main", "main", "--json"]));
        assert_eq!(
            (v["copied"].as_u64(), v["fetched"].as_u64(), v["new_branch"].as_bool()),
            (Some(20), Some(2), Some(false))
        );
        let main2 = root.join("on-main");
        assert_eq!(
            std::fs::read_to_string(main2.join("dir0/f0.txt")).expect("read"),
            "file 0\n"
        );
        // -b at an older commit; --detach; the text output
        let out = text(&wc, &["workspace", "add", "-b", "feat", "../feat", "HEAD"]);
        assert!(
            out.starts_with("Preparing workspace (new branch 'feat')\nHEAD is now at c1"),
            "{out}"
        );
        let v = json_of(&scn_in(&wc, &["workspace", "add", "--detach", "../look", "--json"]));
        assert_eq!(v["detached"], true);
        assert!(text(&root.join("look"), &["status"]).starts_with("HEAD detached at"));
        // Refused: a branch that exists for -b, one that does not without it, a non-empty directory
        for (args, want) in [
            (vec!["workspace", "add", "-b", "main", "../x"], "already exists"),
            (vec!["workspace", "add", "../y", "nope"], "no branch \"nope\""),
            (vec!["workspace", "add", "../agent-1"], "not empty"),
            (
                vec!["workspace", "add", "-b", "z", "--detach", "../z"],
                "do not go together",
            ),
        ] {
            let out = scn_in(&wc, &args);
            assert!(!out.status.success(), "{args:?}");
            assert!(String::from_utf8_lossy(&out.stderr).contains(want), "{args:?}: {out:?}");
        }

        // Work in a workspace: commit and land make its branch on the server
        std::fs::write(a1.join("dir1/new.txt"), "from agent-1\n").expect("write");
        json_of(&scn_in(&a1, &["commit", "-m", "agent-1 work", "--json"]));
        json_of(&scn_in(&a1, &["land", "--json"]));
        assert!(text(&wc, &["branch", "ls"]).contains("agent-1"));
        // Two working copies on main land at once: the server rebases the second
        std::fs::write(main2.join("dir2/a.txt"), "a\n").expect("write");
        json_of(&scn_in(&main2, &["commit", "-m", "from on-main", "--json"]));
        json_of(&scn_in(&wc, &["restore", ".", "--json"]));
        std::fs::write(wc.join("dir3/b.txt"), "b\n").expect("write");
        json_of(&scn_in(&wc, &["commit", "-m", "from main-wc", "--json"]));
        let (m2, m1) = (main2.clone(), wc.clone());
        let t1 = std::thread::spawn(move || json_of(&scn_in(&m2, &["land", "--json"])));
        let t2 = std::thread::spawn(move || json_of(&scn_in(&m1, &["land", "--json"])));
        t1.join().expect("land");
        t2.join().expect("land");
        let log = text(&wc, &["log", "--oneline", "main", "-n", "3"]);
        assert!(log.contains("from on-main") && log.contains("from main-wc"), "{log}");

        // The set, from any of them
        let listed = text(&a1, &["workspace", "list"]);
        let lines: Vec<&str> = listed.lines().collect();
        assert_eq!(lines.len(), 5, "{listed}");
        assert!(
            lines[0].starts_with(&wc.display().to_string()) && lines[0].ends_with("[main]"),
            "{listed}"
        );
        assert!(
            lines[1].ends_with("[agent-1]") && lines[4].ends_with("(detached HEAD)"),
            "{listed}"
        );
        let v = json_of(&scn_in(&wc, &["workspace", "list", "--json"]));
        assert_eq!(v["workspaces"].as_array().map(Vec::len), Some(5));

        // Remove: refused with local changes, then forced; never the one they were made from
        std::fs::write(main2.join("dir2/a.txt"), "changed\n").expect("write");
        let out = scn_in(&wc, &["workspace", "remove", "../on-main"]);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("local changes"),
            "{out:?}"
        );
        assert_eq!(text(&wc, &["workspace", "remove", "--force", "../on-main"]), "");
        assert!(!main2.exists());
        assert_eq!(text(&a1, &["workspace", "rm", "../feat"]), "");
        let out = scn_in(&a1, &["workspace", "remove", "../main-wc"]);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("not a workspace"),
            "{out:?}"
        );
        // A directory deleted by hand is listed as prunable, and can be removed
        std::fs::remove_dir_all(root.join("look")).expect("rm");
        assert!(
            text(&wc, &["workspace", "list"])
                .lines()
                .any(|l| l.ends_with("prunable"))
        );
        assert_eq!(text(&wc, &["workspace", "remove", "../look"]), "");
        assert_eq!(text(&wc, &["workspace", "list"]).lines().count(), 2);
    })
    .await
    .expect("test");
}
