//! `scn mcp`: the MCP server an agent talks to over stdio, against a real server.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_store::{MemStore, MetaStore};
use serde_json::{Value, json};
use tokio::net::TcpListener;

async fn server(repo: &str) -> String {
    let store = Arc::new(MemStore::new());
    store.create_repo(repo).await.expect("repo");
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

/// Sends `messages` to one `scn mcp` and returns its replies, in order.
fn session(args: &[&str], author: &str, messages: &[Value]) -> Vec<Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_scn"))
        .arg("mcp")
        .args(args)
        .env_remove("SCION_ENDPOINT")
        .env_remove("SCION_TOKEN")
        .env("SCION_NO_DAEMON", "1")
        .env("SCION_AUTHOR", author)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("run scn mcp");
    let mut stdin = child.stdin.take().expect("stdin");
    for m in messages {
        writeln!(stdin, "{m}").expect("write");
    }
    writeln!(stdin, "not json").expect("write");
    drop(stdin);
    let out = child.wait_with_output().expect("wait");
    assert!(out.status.success(), "{out:?}");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| serde_json::from_str(l).expect("a JSON reply"))
        .collect()
}

fn call(id: u64, name: &str, arguments: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": {"name": name, "arguments": arguments}})
}

/// A tool's result: its JSON, and whether it is an error.
fn result(reply: &Value) -> (Value, bool) {
    let r = &reply["result"];
    let text = r["content"][0]["text"].as_str().expect("text");
    (serde_json::from_str(text).expect("json"), r["isError"] == json!(true))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_reads_and_commits_without_a_checkout() {
    let url = server("app").await;
    let repo = format!("{url}/app");
    let r = tokio::task::spawn_blocking(move || {
        let args = ["--repo", repo.as_str()];
        let replies = session(
            &args,
            "Agent One <a1@x>",
            &[
                json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}}),
                json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
                json!({"jsonrpc": "2.0", "id": 3, "method": "ping"}),
                // The first commit makes the branch
                call(4, "commit", json!({"message": "first", "land": true, "files": [
                    {"path": "src/main.rs", "content": "fn main() {\n    println!(\"hi\");\n}\n"},
                    {"path": "README.md", "content": "# app\n"},
                ]})),
                call(5, "read_file", json!({"path": "src/main.rs"})),
                call(6, "list_dir", json!({"path": "src"})),
                call(7, "grep", json!({"pattern": "PRINTLN", "ignore_case": true})),
                call(8, "commit", json!({"message": "second", "files": [{"path": "README.md", "content": "# app\nmore\n"}], "delete": ["src/main.rs"]})),
                call(9, "log", json!({"max_count": 5})),
                call(10, "blame", json!({"path": "README.md"})),
                call(11, "read_file", json!({"path": "nope.txt"})),
                call(12, "nope", json!({})),
                call(13, "commit", json!({"message": "empty"})),
                call(14, "read_file", json!({})),
                json!({"jsonrpc": "2.0", "id": 15, "method": "resources/list"}),
            ],
        );
        (args.iter().map(|s| s.to_string()).collect::<Vec<_>>(), replies)
    })
    .await
    .expect("session");
    let (args, replies) = r;
    assert_eq!(
        replies.len(),
        16,
        "a reply to each request and the bad line, none to the notification"
    );
    assert_eq!(replies[0]["result"]["protocolVersion"], "2025-03-26");
    assert_eq!(replies[0]["result"]["serverInfo"]["name"], "scn");
    let names: Vec<&str> = replies[1]["result"]["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|t| t["name"].as_str().expect("name"))
        .collect();
    assert_eq!(
        names,
        [
            "read_file",
            "list_dir",
            "grep",
            "log",
            "blame",
            "diff",
            "show",
            "commit",
            "edit_file",
            "land"
        ]
    );
    assert_eq!(replies[2]["result"], json!({}));
    let (first, err) = result(&replies[3]);
    assert!(!err, "{first}");
    assert_eq!(first["parent"], Value::Null);
    assert_eq!(first["landed"]["branch"], "main");
    let (file, _) = result(&replies[4]);
    assert!(file["content"].as_str().expect("content").contains("println!"));
    assert_eq!(file["truncated"], false);
    let (ls, _) = result(&replies[5]);
    assert_eq!(ls["entries"][0]["name"], "main.rs");
    let (grep, _) = result(&replies[6]);
    assert_eq!(grep["matches"][0]["path"], "src/main.rs");
    // Not landed: main stays at the first commit, which the log shows
    let (second, err) = result(&replies[7]);
    assert!(!err && second.get("landed").is_none(), "{second}");
    assert_eq!(second["parent"], first["changeset"]);
    let (log, _) = result(&replies[8]);
    assert_eq!(log["entries"].as_array().map(Vec::len), Some(1));
    assert_eq!(log["entries"][0]["author"], "Agent One");
    let (blame, _) = result(&replies[9]);
    assert_eq!(blame["lines"].as_array().map(Vec::len), Some(1));
    for (i, want) in [(10, "PATH_NOT_FOUND"), (11, ""), (12, ""), (13, "")] {
        let (e, err) = result(&replies[i]);
        assert!(err, "{e}");
        if !want.is_empty() {
            assert_eq!(e["reason"], want);
        }
    }
    assert_eq!(replies[14]["error"]["code"], -32601);
    assert_eq!(replies[15]["error"]["code"], -32700);

    // Land the second commit, then two agents change the same line from the same base: the one
    // that lands second is refused with the conflict; another file merges on the server
    let second_id = second["changeset"].as_str().expect("id").to_owned();
    let args2 = args.clone();
    let replies = tokio::task::spawn_blocking(move || {
        let a: Vec<&str> = args2.iter().map(String::as_str).collect();
        session(
            &a,
            "Agent One <a1@x>",
            &[
                call(1, "land", json!({"changeset": second_id})),
                call(2, "commit", json!({"message": "one", "land": true, "files": [{"path": "README.md", "content": "# app\none\n"}]})),
                call(3, "show", json!({})),
                call(4, "diff", json!({"to": "main"})),
            ],
        )
    })
    .await
    .expect("session");
    let (landed, err) = result(&replies[0]);
    assert!(!err, "{landed}");
    let base = landed["tip"].as_str().expect("tip").to_owned();
    let (one, err) = result(&replies[1]);
    assert!(!err, "{one}");
    let (show, _) = result(&replies[2]);
    assert!(show.to_string().contains("one"), "{show}");
    let (diff, _) = result(&replies[3]);
    assert_eq!(diff["files"][0]["path"], "README.md");
    let replies = tokio::task::spawn_blocking(move || {
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        session(
            &a,
            "Agent Two <a2@x>",
            &[
                call(1, "commit", json!({"message": "two", "base": base, "land": true, "files": [{"path": "README.md", "content": "# app\ntwo\n"}]})),
                call(2, "commit", json!({"message": "elsewhere", "base": base, "land": true, "files": [{"path": "other.txt", "content": "x\n"}]})),
            ],
        )
    })
    .await
    .expect("session");
    let (conflict, err) = result(&replies[0]);
    assert!(err, "{conflict}");
    assert!(conflict.to_string().to_lowercase().contains("conflict"), "{conflict}");
    let (merged, err) = result(&replies[1]);
    assert!(!err, "{merged}");
    assert_eq!(
        merged["landed"]["rebased"], true,
        "rebased onto the other agent's commit"
    );
}

/// Several agents commit and land at once; the server rebases each onto the others.
#[tokio::test(flavor = "multi_thread")]
async fn agents_land_at_the_same_time() {
    let url = server("app").await;
    let repo = format!("{url}/app");
    let r = repo.clone();
    tokio::task::spawn_blocking(move || {
        session(
            &["--repo", &r],
            "Seed <s@x>",
            &[call(
                1,
                "commit",
                json!({"message": "seed", "land": true, "files": [{"path": "seed", "content": "s\n"}]}),
            )],
        )
    })
    .await
    .expect("seed");
    let agents: Vec<_> = (0..4)
        .map(|n| {
            let repo = repo.clone();
            tokio::task::spawn_blocking(move || {
                let msgs: Vec<Value> = (0..3)
                    .map(|i| {
                        call(
                            i,
                            "commit",
                            json!({"message": format!("agent {n} step {i}"), "land": true,
                            "files": [{"path": format!("agent{n}/step{i}.txt"), "content": format!("{n} {i}\n")}]}),
                        )
                    })
                    .collect();
                session(&["--repo", &repo], &format!("Agent {n} <a{n}@x>"), &msgs)
            })
        })
        .collect();
    for a in agents {
        for reply in a.await.expect("agent").iter().take(3) {
            let (v, err) = result(reply);
            assert!(!err, "{v}");
        }
    }
    let replies = tokio::task::spawn_blocking(move || {
        session(
            &["--repo", &repo],
            "Reader <r@x>",
            &[
                call(1, "log", json!({"max_count": 100})),
                call(2, "list_dir", json!({})),
            ],
        )
    })
    .await
    .expect("read");
    let (log, _) = result(&replies[0]);
    assert_eq!(log["entries"].as_array().map(Vec::len), Some(13));
    let (ls, _) = result(&replies[1]);
    assert_eq!(ls["entries"].as_array().map(Vec::len), Some(5), "agent0..3 and seed");
}

/// Edits applied on the server: no file is read or sent whole, and agents editing different
/// parts of one file at once both land.
#[tokio::test(flavor = "multi_thread")]
async fn agents_edit_files_on_the_server() {
    let url = server("app").await;
    let repo = format!("{url}/app");
    let lines: String = (0..20).map(|i| format!("line {i}\n")).collect();
    let r = repo.clone();
    let replies = tokio::task::spawn_blocking(move || {
        session(
            &["--repo", &r],
            "Agent <a@x>",
            &[
                // Nothing to edit in an empty repository
                call(
                    1,
                    "edit_file",
                    json!({"path": "f.txt", "old_string": "a", "new_string": "b"}),
                ),
                call(
                    2,
                    "commit",
                    json!({"message": "seed", "land": true, "files": [
                        {"path": "f.txt", "content": lines}, {"path": "g.txt", "content": "x = 1\nx = 1\n"},
                    ]}),
                ),
                call(
                    3,
                    "edit_file",
                    json!({"path": "f.txt", "old_string": "line 3\n", "new_string": "line three\n"}),
                ),
                call(
                    4,
                    "commit",
                    json!({"message": "two files", "land": true, "edits": [
                        {"path": "g.txt", "old": "x = 1", "new": "x = 2", "all": true},
                        {"path": "f.txt", "old": "line 19\n", "new": ""},
                    ]}),
                ),
                call(
                    5,
                    "edit_file",
                    json!({"path": "g.txt", "old_string": "x = 2", "new_string": "y"}),
                ),
                call(
                    6,
                    "edit_file",
                    json!({"path": "f.txt", "old_string": "nowhere", "new_string": "y"}),
                ),
                call(
                    7,
                    "edit_file",
                    json!({"path": "nope.txt", "old_string": "a", "new_string": "b"}),
                ),
                call(8, "read_file", json!({"path": "f.txt"})),
                call(9, "read_file", json!({"path": "g.txt"})),
            ],
        )
    })
    .await
    .expect("session");
    let (v, err) = result(&replies[0]);
    assert!(err && v.to_string().contains("parent"), "{v}");
    for i in [1, 2, 3] {
        let (v, err) = result(&replies[i]);
        assert!(!err, "{i}: {v}");
    }
    for (i, want) in [(4, "appears 2 times"), (5, "not in the file"), (6, "not a file")] {
        let (v, err) = result(&replies[i]);
        assert!(err && v.to_string().contains(want), "{i}: {v}");
    }
    let (f, _) = result(&replies[7]);
    let content = f["content"].as_str().expect("content");
    assert!(
        content.contains("line three\n") && !content.contains("line 19"),
        "{content}"
    );
    let (g, _) = result(&replies[8]);
    assert_eq!(g["content"], "x = 2\nx = 2\n");

    // Two agents edit far apart lines of one file from the same base, at once: both land
    let agents: Vec<_> = [(1, "line 1\n", "first\n"), (2, "line 17\n", "seventeenth\n")]
        .into_iter()
        .map(|(n, old, new)| {
            let repo = repo.clone();
            tokio::task::spawn_blocking(move || {
                session(
                    &["--repo", &repo],
                    &format!("Agent {n} <a{n}@x>"),
                    &[call(
                        1,
                        "edit_file",
                        json!({"path": "f.txt", "old_string": old, "new_string": new}),
                    )],
                )
            })
        })
        .collect();
    for a in agents {
        let (v, err) = result(&a.await.expect("agent")[0]);
        assert!(!err, "{v}");
    }
    let replies = tokio::task::spawn_blocking(move || {
        session(
            &["--repo", &repo],
            "Reader <r@x>",
            &[call(1, "read_file", json!({"path": "f.txt"}))],
        )
    })
    .await
    .expect("read");
    let (f, _) = result(&replies[0]);
    let content = f["content"].as_str().expect("content");
    assert!(
        content.contains("first\n") && content.contains("seventeenth\n") && content.contains("line three\n"),
        "{content}"
    );
}
