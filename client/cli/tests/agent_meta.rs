//! Agent metadata on commits: trailers (`--trailer`, SCION_TRAILERS, the MCP commit tool) and
//! finding the commits by them (`log --grep`, `--author`).

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_store::{MemStore, MetaStore};
use serde_json::{Value, json};
use tokio::net::TcpListener;

fn scn_in(dir: &Path, envs: &[(&str, &str)], args: &[&str]) -> std::process::Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_scn"));
    c.current_dir(dir)
        .args(args)
        .env_remove("SCION_ENDPOINT")
        .env_remove("SCION_TOKEN")
        .env_remove("SCION_REPO")
        .env_remove("SCION_TRAILERS")
        .env("SCION_AUTHOR", "Ada <ada@x>")
        .env("SCION_NO_DAEMON", "1");
    for (k, v) in envs {
        c.env(k, v);
    }
    c.output().expect("run scn")
}

fn ok(dir: &Path, envs: &[(&str, &str)], args: &[&str]) -> String {
    let out = scn_in(dir, envs, args);
    assert!(out.status.success(), "{args:?}: {out:?}");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn subjects(dir: &Path, args: &[&str]) -> Vec<String> {
    ok(dir, &[], &[&["log", "--oneline"][..], args].concat())
        .lines()
        .map(|l| l.split_once(' ').expect("id subject").1.to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn trailers_mark_an_agents_commits_and_log_finds_them() {
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
    let repo = format!("{url}/app");
    tokio::task::spawn_blocking(move || {
        ok(&root, &[], &["clone", &repo, "-q"]);
        let wc = root.join("app");
        let commit = |envs: &[(&str, &str)], args: &[&str], file: &str| {
            std::fs::write(wc.join(file), file).expect("write");
            ok(&wc, envs, &[&["commit", "-q"][..], args].concat());
            ok(&wc, &[], &["land", "-q"]);
        };
        commit(&[], &["-m", "by hand"], "a");
        commit(&[], &["-m", "agent one", "--trailer", "Agent: claude", "--trailer", "Agent-Session=s-1"], "b");
        let session = [("SCION_TRAILERS", "Agent: claude; Agent-Session: s-2"), ("SCION_AUTHOR", "Bot <bot@agents>")];
        commit(&session, &["-m", "agent two"], "c");
        commit(&session, &["-m", "agent two again", "--trailer", "Reviewed-by: Ada"], "d");
        let show = ok(&wc, &[], &["show", "--name-only"]);
        assert!(
            show.contains("    agent two again\n    \n    Reviewed-by: Ada\n    Agent: claude\n    Agent-Session: s-2\n"),
            "{show}"
        );
        // Finding them: by trailer, by author, both, ignoring case
        assert_eq!(subjects(&wc, &["--grep", "Agent-Session: s-2"]), ["agent two again", "agent two"]);
        assert_eq!(subjects(&wc, &["--grep", "^Agent: claude$"]).len(), 0, "the whole message, so no ^…$ match");
        assert_eq!(subjects(&wc, &["--grep", "(?m)^Agent: claude$"]).len(), 3);
        assert_eq!(subjects(&wc, &["--author", "@agents>$"]), ["agent two again", "agent two"]);
        assert_eq!(subjects(&wc, &["--author", "ada", "-i", "--grep", "session"]), ["agent one"]);
        assert_eq!(subjects(&wc, &["--grep", "s-1", "-n", "5", "--", "b"]), ["agent one"]);
        // Mistakes
        for (args, want) in [
            (vec!["commit", "--trailer", "Agent: x"], "--trailer goes with -m"),
            (vec!["commit", "-m", "m", "--trailer", "nonsense"], "is not KEY: VALUE"),
            (vec!["log", "--grep", "("], "bad pattern"),
        ] {
            let out = scn_in(&wc, &[], &args);
            assert!(String::from_utf8_lossy(&out.stderr).contains(want), "{args:?}: {out:?}");
        }

        // The same through MCP: trailers on the commit tool, grep on the log tool
        let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["mcp", "--repo", &repo])
            .env("SCION_NO_DAEMON", "1")
            .env("SCION_AUTHOR", "Mcp Agent <mcp@agents>")
            .env("SCION_TRAILERS", "Agent: mcp")
            .env_remove("SCION_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("mcp");
        let mut stdin = child.stdin.take().expect("stdin");
        for (id, name, args) in [
            (1, "commit", json!({"message": "via mcp", "land": true, "trailers": {"Agent-Session": "m-1"},
                "files": [{"path": "e", "content": "e"}]})),
            (2, "log", json!({"grep": "Agent-Session: m-1"})),
            (3, "log", json!({"author": "MCP AGENT", "ignore_case": true})),
        ] {
            let msg = json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": {"name": name, "arguments": args}});
            writeln!(stdin, "{msg}").expect("write");
        }
        drop(stdin);
        let out = child.wait_with_output().expect("wait");
        let replies: Vec<Value> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|l| serde_json::from_str(l).expect("json"))
            .collect();
        let result = |i: usize| -> Value {
            serde_json::from_str(replies[i]["result"]["content"][0]["text"].as_str().expect("text")).expect("json")
        };
        assert!(replies[0]["result"]["isError"].is_null(), "{:?}", replies[0]);
        for i in [1, 2] {
            let log = result(i);
            assert_eq!(log["entries"].as_array().map(Vec::len), Some(1), "{log}");
            assert_eq!(log["entries"][0]["message"], "via mcp\n\nAgent: mcp\nAgent-Session: m-1");
        }
    })
    .await
    .expect("test");
}
