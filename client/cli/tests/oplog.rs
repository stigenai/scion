//! `scn undo` and `scn op`: every command that changes the working copy can be undone.

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

fn ok(dir: &Path, args: &[&str]) -> String {
    let out = scn_in(dir, args);
    assert!(out.status.success(), "{args:?}: {out:?}");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn read(p: &Path) -> String {
    std::fs::read_to_string(p).unwrap_or_else(|_| "<missing>".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn undo_puts_back_what_a_command_changed() {
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
        ok(&root, &["clone", &format!("{url}/app"), "-q"]);
        let wc = root.join("app");
        let out = scn_in(&wc, &["undo"]);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("nothing to undo"),
            "{out:?}"
        );
        std::fs::write(wc.join("a.txt"), "one\ntwo\n").expect("write");
        std::fs::write(wc.join("b.txt"), "b\n").expect("write");
        ok(&wc, &["commit", "-m", "base", "-q"]);
        ok(&wc, &["land", "-q"]);

        // restore loses a local edit; undo brings it back
        std::fs::write(wc.join("a.txt"), "one\nTWO\n").expect("write");
        ok(&wc, &["restore", "a.txt"]);
        assert_eq!(read(&wc.join("a.txt")), "one\ntwo\n");
        let out = ok(&wc, &["undo"]);
        assert!(
            out.starts_with("Undid operation ") && out.contains(": scn restore a.txt\nOn branch main at c1"),
            "{out}"
        );
        assert_eq!(read(&wc.join("a.txt")), "one\nTWO\n");

        // commit, undone: the change is local again; undone again: committed again
        ok(&wc, &["commit", "-m", "edit", "-q"]);
        assert!(ok(&wc, &["status"]).contains("is not landed yet"));
        ok(&wc, &["undo", "-q"]);
        assert_eq!(ok(&wc, &["status", "-s"]), "M  a.txt\n");
        assert!(!ok(&wc, &["status"]).contains("not landed"));
        ok(&wc, &["undo", "-q"]);
        assert_eq!(ok(&wc, &["status", "-s"]), "");
        assert!(ok(&wc, &["status"]).contains("is not landed yet"), "redo");
        ok(&wc, &["uncommit", "-q"]);

        // clean deletes a new file; stash puts changes away; both undone
        std::fs::write(wc.join("new.txt"), "new\n").expect("write");
        ok(&wc, &["clean", "-f", "-q"]);
        assert_eq!(read(&wc.join("new.txt")), "<missing>");
        ok(&wc, &["undo", "-q"]);
        assert_eq!(read(&wc.join("new.txt")), "new\n");
        ok(&wc, &["stash", "-q"]);
        assert_eq!(ok(&wc, &["status", "-s"]), "");
        ok(&wc, &["undo", "-q"]);
        assert_eq!(ok(&wc, &["status", "-s"]), "A  new.txt\nM  a.txt\n");
        assert_eq!(ok(&wc, &["stash", "list"]), "", "the stash entry it made is gone too");

        // A failed command is not logged
        let before = ok(&wc, &["op", "log"]);
        assert!(!scn_in(&wc, &["switch", "nope"]).status.success());
        assert_eq!(ok(&wc, &["op", "log"]), before);
        let lines: Vec<&str> = before.lines().collect();
        assert!(
            lines[0].ends_with("scn undo -q") && lines.last().is_some_and(|l| l.ends_with("scn commit -m base -q")),
            "{before}"
        );
        let v: serde_json::Value = serde_json::from_str(&ok(&wc, &["op", "log", "-n", "2", "--json"])).expect("json");
        assert_eq!(v["schema"], "scion.cli.op-log/v1");
        assert_eq!(v["operations"].as_array().map(Vec::len), Some(2));

        // A sync that stops on a conflict, undone: pending commit and files as before
        ok(&wc, &["restore", ".", "-q"]);
        std::fs::remove_file(wc.join("new.txt")).expect("rm");
        ok(&root, &["clone", &format!("{url}/app"), "other", "-q"]);
        let other = root.join("other");
        std::fs::write(other.join("a.txt"), "one\nOTHER\n").expect("write");
        ok(&other, &["commit", "-m", "other", "-q"]);
        ok(&other, &["land", "-q"]);
        std::fs::write(wc.join("a.txt"), "one\nMINE\n").expect("write");
        ok(&wc, &["commit", "-m", "mine", "-q"]);
        assert_eq!(scn_in(&wc, &["sync"]).status.code(), Some(3));
        assert!(read(&wc.join("a.txt")).contains("<<<<<<<"));
        ok(&wc, &["undo", "-q"]);
        assert_eq!(read(&wc.join("a.txt")), "one\nMINE\n");
        assert!(ok(&wc, &["status"]).contains("is not landed yet"));
        assert_eq!(ok(&wc, &["status", "-s"]), "");

        // op restore goes back several steps at once: before the commit of "mine"
        let v: serde_json::Value = serde_json::from_str(&ok(&wc, &["op", "log", "--json"])).expect("json");
        let id = v["operations"]
            .as_array()
            .expect("ops")
            .iter()
            .find(|o| o["command"] == "scn commit -m mine -q")
            .and_then(|o| o["id"].as_u64())
            .expect("the commit");
        ok(&wc, &["op", "restore", &id.to_string(), "-q"]);
        assert_eq!(ok(&wc, &["status", "-s"]), "M  a.txt\n");
        assert!(!ok(&wc, &["status"]).contains("not landed"));
        let out = scn_in(&wc, &["op", "restore", "999"]);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("no operation 999"),
            "{out:?}"
        );

        // Undoing a switch and a land; a land's commits stay on the server
        ok(&wc, &["restore", ".", "-q"]);
        ok(&wc, &["sync", "-q"]);
        ok(&wc, &["switch", "-c", "feat", "-q"]);
        ok(&wc, &["undo", "-q"]);
        assert!(ok(&wc, &["status"]).starts_with("On branch main"));
        std::fs::write(wc.join("b.txt"), "b2\n").expect("write");
        ok(&wc, &["commit", "-m", "b2", "-q"]);
        ok(&wc, &["land", "-q"]);
        let out = scn_in(&wc, &["undo"]);
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("stay on the server"),
            "{out:?}"
        );
        assert!(ok(&wc, &["log", "--oneline", "main", "-n", "1"]).ends_with(" b2\n"));
    })
    .await
    .expect("test");
}
