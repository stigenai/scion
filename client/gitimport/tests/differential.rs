//! 差分测试：用真实 git 构造历史，导入 Scion 后逐个提交与 git 比对文件列表、mode、内容与元数据。

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_client::Client;
use scion_gitimport::{Error, GIT_SHA1_EXTENSION, Options, import, import_with, parse_commit};
use scion_object::{Changeset, EntryMode, ObjectId, Tree};
use scion_store::{MemStore, MetaStore};
use tokio::net::TcpListener;

struct Server {
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn server() -> Server {
    server_with(ServerConfig::default()).await
}

async fn server_with(config: ServerConfig) -> Server {
    let store = Arc::new(MemStore::new());
    store.create_repo("imported").await.expect("repo");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let app = App {
        config: Arc::new(config),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    Server { url, _shutdown: tx }
}

/// 在 `dir` 中运行 git；时间和身份固定，不读取用户的全局配置。
fn git(dir: &Path, args: &[&str]) -> Vec<u8> {
    git_at(dir, args, "1700000000 +0800")
}

fn git_at(dir: &Path, args: &[&str], date: &str) -> Vec<u8> {
    let out = Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Ada Lovelace")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_NAME", "CI Bot")
        .env("GIT_COMMITTER_EMAIL", "ci@example.com")
        .env("GIT_AUTHOR_DATE", date)
        .env("GIT_COMMITTER_DATE", date)
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn write(dir: &Path, rel: &str, content: &[u8]) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
    std::fs::write(p, content).expect("write");
}

fn commit(dir: &Path, msg: &str, date: &str) {
    git(dir, &["add", "-A"]);
    git_at(dir, &["commit", "-q", "--allow-empty", "-m", msg], date);
}

/// 构造覆盖各种边界情况的 git 历史。
fn build_history(dir: &Path) {
    git(dir, &["init", "-q", "-b", "main"]);
    write(dir, "README.md", b"hello\n");
    write(dir, "src/a.rs", b"fn a() {}\n");
    write(dir, "data/blob.bin", &[0, 159, 146, 150, 255, 0, 1]);
    write(dir, "模型/权重.txt", "中文路径\n".as_bytes());
    commit(dir, "init", "1700000000 +0800");

    write(dir, "src/a.rs", b"fn a() { println!(\"changed\"); }\n");
    write(dir, "run.sh", b"#!/bin/sh\necho run\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.join("run.sh"), std::fs::Permissions::from_mode(0o755)).expect("chmod");
        std::os::unix::fs::symlink("README.md", dir.join("link")).expect("symlink");
    }
    commit(dir, "modify, exec, symlink", "1700000100 -0500");

    git(dir, &["mv", "src/a.rs", "src/b.rs"]);
    std::fs::remove_file(dir.join("data/blob.bin")).expect("rm");
    commit(dir, "rename and delete", "1700000200 +0000");

    // 分支与合并
    git(dir, &["checkout", "-q", "-b", "feature"]);
    write(dir, "feature.txt", b"feature\n");
    commit(dir, "feature work", "1700000300 +0100");
    git(dir, &["checkout", "-q", "main"]);
    write(dir, "main.txt", b"main\n");
    commit(dir, "main work", "1700000400 +0100");
    git_at(
        dir,
        &["merge", "-q", "--no-ff", "-m", "merge feature", "feature"],
        "1700000500 +0100",
    );

    // 目录变文件、文件变目录
    write(dir, "x/1", b"1");
    write(dir, "x/sub/2", b"2");
    commit(dir, "x is a dir", "1700000600 +0000");
    std::fs::remove_dir_all(dir.join("x")).expect("rm dir");
    write(dir, "x", b"now a file");
    commit(dir, "x is a file", "1700000700 +0000");
    std::fs::remove_file(dir.join("x")).expect("rm file");
    write(dir, "x/y", b"dir again");
    commit(dir, "x is a dir again", "1700000800 +0000");

    // 子模块（gitlink）与空提交
    git(
        dir,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000,1111111111111111111111111111111111111111,vendor/sub",
        ],
    );
    git_at(dir, &["commit", "-q", "-m", "add submodule"], "1700000900 +0000");
    git_at(
        dir,
        &["commit", "-q", "--allow-empty", "-m", "empty commit\n\nwith body"],
        "1700001000 +0000",
    );
}

/// git 某个提交的全部文件：路径 → (mode, 内容的 Scion blob id)。子模块跳过。
fn git_files(dir: &Path, sha: &str) -> BTreeMap<String, (EntryMode, ObjectId)> {
    let out = git(dir, &["ls-tree", "-r", "-z", sha]);
    let mut files = BTreeMap::new();
    for rec in out.split(|b| *b == 0).filter(|r| !r.is_empty()) {
        let rec = String::from_utf8(rec.to_vec()).expect("utf8");
        let (meta, path) = rec.split_once('\t').expect("tab");
        let parts: Vec<&str> = meta.split(' ').collect();
        let mode = match parts[0] {
            "100644" => EntryMode::File,
            "100755" => EntryMode::Executable,
            "120000" => EntryMode::Symlink,
            "160000" => continue,
            other => panic!("unexpected mode {other}"),
        };
        let content = git(dir, &["cat-file", "blob", parts[2]]);
        files.insert(path.to_owned(), (mode, ObjectId::of_blob(&content)));
    }
    files
}

/// Scion 某个提交的全部文件。
async fn scion_files(c: &Client, cs: ObjectId) -> BTreeMap<String, (EntryMode, ObjectId)> {
    let root = c.root_tree(&cs.to_string()).await.expect("root");
    let mut out = BTreeMap::new();
    let mut stack = vec![(String::new(), root)];
    while let Some((prefix, id)) = stack.pop() {
        let tree = Tree::decode(&c.get_object(&id).await.expect("tree")).expect("decode");
        for e in tree.entries() {
            let p = if prefix.is_empty() {
                e.name.clone()
            } else {
                format!("{prefix}/{}", e.name)
            };
            if e.mode == EntryMode::Tree {
                stack.push((p, e.id));
            } else {
                out.insert(p, (e.mode, e.id));
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn import_matches_git_commit_by_commit() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    build_history(dir);

    let mut calls = 0;
    let summary = import(&c, dir, "main", "main", |done, total| {
        calls += 1;
        assert!(done <= total);
    })
    .await
    .expect("import");
    let shas: Vec<String> = String::from_utf8(git(dir, &["rev-list", "--reverse", "--topo-order", "main"]))
        .expect("utf8")
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(summary.commits, shas.len());
    assert_eq!(calls, shas.len());
    assert_eq!(summary.skipped_gitlinks, 1);

    for sha in &shas {
        let cs = summary.mapping[sha];
        // 文件列表、mode、内容
        assert_eq!(scion_files(&c, cs).await, git_files(dir, sha), "tree mismatch at {sha}");
        // 元数据
        let git_commit = parse_commit(&git(dir, &["cat-file", "commit", sha])).expect("parse");
        let scion = Changeset::decode(&c.get_object(&cs).await.expect("changeset")).expect("decode");
        assert_eq!(scion.message, git_commit.message);
        assert_eq!(
            (scion.author.time_ms, scion.author.tz_offset_minutes),
            (git_commit.author.time_ms, git_commit.author.tz_offset_minutes)
        );
        assert_eq!(scion.committer.name, "CI Bot");
        assert_eq!(scion.extensions[GIT_SHA1_EXTENSION], hex::decode(sha).expect("hex"));
        let parents: Vec<ObjectId> = git_commit.parents.iter().map(|p| summary.mapping[p]).collect();
        assert_eq!(scion.parents, parents, "parents at {sha}");
        assert_eq!(
            c.git_sha(&cs.to_string()).await.expect("git sha").as_deref(),
            Some(sha.as_str())
        );
        // 40 位 git sha 可以直接当作版本号使用
        assert_eq!(
            c.root_tree(sha).await.expect("resolve by git sha"),
            c.root_tree(&cs.to_string()).await.expect("by id")
        );
    }

    let tip = c.resolve_ref("main").await.expect("resolve").expect("main exists");
    assert_eq!(Some(tip.changeset), summary.tip);
    assert_eq!(summary.mapping[shas.last().expect("tip")], tip.changeset);
}

#[tokio::test(flavor = "multi_thread")]
async fn reimport_is_idempotent_and_incremental_import_fast_forwards() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    git(dir, &["init", "-q", "-b", "main"]);
    write(dir, "a", b"1");
    commit(dir, "one", "1700000000 +0000");

    let first = import(&c, dir, "main", "main", |_, _| {}).await.expect("first");
    let again = import(&c, dir, "main", "main", |_, _| {}).await.expect("again");
    assert_eq!(
        (first.new_commits, again.new_commits),
        (1, 0),
        "nothing new the second time"
    );
    assert_eq!((again.commits, again.tip), (1, first.tip));
    assert_eq!(
        c.resolve_ref("main").await.expect("ok").expect("ref").version,
        1,
        "nothing to land"
    );

    // 新增两个提交：一次快进到新的 tip
    write(dir, "a", b"2");
    commit(dir, "two", "1700000100 +0000");
    write(dir, "b", b"b");
    commit(dir, "three", "1700000200 +0000");
    let third = import(&c, dir, "main", "main", |_, _| {}).await.expect("incremental");
    assert_eq!(
        (third.commits, third.new_commits),
        (3, 2),
        "only the new commits are created"
    );
    let tip = c.resolve_ref("main").await.expect("ok").expect("ref");
    assert_eq!(Some(tip.changeset), third.tip);
    assert_eq!(tip.version, 2, "one fast-forward for both commits");
    let log = c.log("main", None, 10, None).await.expect("log");
    assert_eq!(log.entries.len(), 3, "every imported commit is in the history");

    // 改写历史后再导入：拒绝
    git(dir, &["reset", "-q", "--hard", "HEAD~2"]);
    write(dir, "a", b"rewritten");
    commit(dir, "rewritten", "1700000300 +0000");
    let err = import(&c, dir, "main", "main", |_, _| {}).await.expect_err("diverged");
    assert!(matches!(err, Error::Diverged { .. }), "{err:?}");
}

/// A branch that shares history with an imported one only creates its own commits, and merging
/// it back only creates the merge commit.
#[tokio::test(flavor = "multi_thread")]
async fn shared_history_is_not_imported_twice() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    git(dir, &["init", "-q", "-b", "main"]);
    for (i, t) in ["1700000000", "1700000100", "1700000200"].iter().enumerate() {
        write(dir, "a", format!("{i}").as_bytes());
        commit(dir, &format!("main {i}"), &format!("{t} +0000"));
    }
    let main = import(&c, dir, "main", "main", |_, _| {}).await.expect("main");
    assert_eq!(main.new_commits, 3);
    git(dir, &["checkout", "-q", "-b", "feat"]);
    for (i, t) in ["1700000300", "1700000400"].iter().enumerate() {
        write(dir, "f", format!("{i}").as_bytes());
        commit(dir, &format!("feat {i}"), &format!("{t} +0000"));
    }
    let feat = import(&c, dir, "feat", "feat", |_, _| {}).await.expect("feat");
    assert_eq!((feat.commits, feat.new_commits), (5, 2));
    assert_eq!(
        c.log("feat", None, 10, None).await.expect("log").entries.len(),
        5,
        "full history on the new ref"
    );
    git(dir, &["checkout", "-q", "main"]);
    git(
        dir,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@x",
            "merge",
            "-q",
            "--no-ff",
            "-m",
            "merge feat",
            "feat",
        ],
    );
    let merged = import(&c, dir, "main", "main", |_, _| {}).await.expect("merge");
    assert_eq!((merged.commits, merged.new_commits), (6, 1));
    let tip = c.log("main", None, 1, None).await.expect("log").entries.remove(0);
    assert_eq!(tip.parents.len(), 2);
    assert_eq!(c.read_file("main", "f").await.expect("f").as_ref(), b"1");
    // Importing a branch whose every commit is already there only moves the ref
    let copy = import(&c, dir, "main", "main-copy", |_, _| {}).await.expect("copy");
    assert_eq!((copy.new_commits, copy.tip), (0, merged.tip));
    assert_eq!(
        c.resolve_ref("main-copy").await.expect("ok").map(|r| r.changeset),
        merged.tip
    );
}

/// A git tag imported into `tags/…` is created at its commit in one step (the server never
/// moves tags); importing it again is a no-op, a tag pointing elsewhere is reported.
#[tokio::test(flavor = "multi_thread")]
async fn tags_are_created_at_the_tagged_commit() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    git(dir, &["init", "-q", "-b", "main"]);
    write(dir, "a", b"1");
    commit(dir, "one", "1700000000 +0000");
    write(dir, "a", b"2");
    commit(dir, "two", "1700000100 +0000");
    git(dir, &["tag", "v1"]);
    let s = import(&c, dir, "v1", "tags/v1", |_, _| {}).await.expect("tag");
    let tag = c.resolve_ref("tags/v1").await.expect("ok").expect("tag");
    assert_eq!((Some(tag.changeset), tag.version), (s.tip, 1));
    assert_eq!(
        c.log("v1", None, 10, None).await.expect("log").entries.len(),
        2,
        "the short name works"
    );
    import(&c, dir, "v1", "tags/v1", |_, _| {}).await.expect("again: no-op");
    git(dir, &["tag", "-d", "v1"]);
    write(dir, "a", b"3");
    commit(dir, "three", "1700000200 +0000");
    git(dir, &["tag", "v1"]);
    let err = import(&c, dir, "v1", "tags/v1", |_, _| {})
        .await
        .expect_err("moved tag");
    assert!(matches!(err, Error::Diverged { .. }), "{err:?}");

    // An annotated tag keeps its message and tagger; imported as a branch it is just the commit
    git(dir, &["tag", "-a", "v2", "-m", "Second release"]);
    let s = import(&c, dir, "v2", "tags/v2", |_, _| {}).await.expect("annotated");
    let tags = c.list_tags().await.expect("tags");
    let v2 = tags.iter().find(|t| t.name == "v2").expect("v2");
    let note = v2.annotation.as_ref().expect("annotated");
    assert_eq!(Some(v2.target), s.tip);
    assert_eq!((note.message.as_str(), note.tagger.name.as_str()), ("Second release\n", "CI Bot"));
    import(&c, dir, "v2", "from-tag", |_, _| {}).await.expect("as a branch");
    assert_eq!(c.resolve_ref("from-tag").await.expect("ok").map(|r| r.changeset), s.tip);
}

/// 历史超过服务端单次合入上限时分段快进，每个提交的 change id 都被记录。
#[tokio::test(flavor = "multi_thread")]
async fn long_history_lands_in_steps() {
    let mut config = ServerConfig::default();
    config.land.stack.max_stack = 2;
    let srv = server_with(config).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    git(dir, &["init", "-q", "-b", "main"]);
    for i in 0..5 {
        write(dir, "a", format!("{i}").as_bytes());
        commit(dir, &format!("c{i}"), &format!("{} +0000", 1_700_000_000 + i));
    }
    // 一次快进 5 个提交超过上限
    let err = import(&c, dir, "main", "main", |_, _| {}).await.expect_err("too many");
    assert!(err.to_string().contains("more than 2"), "{err}");
    let summary = import_with(&c, dir, "main", "main", Options { land_step: 2 }, |_, _| {})
        .await
        .expect("in steps");
    let tip = c.resolve_ref("main").await.expect("ok").expect("ref");
    assert_eq!(Some(tip.changeset), summary.tip);
    assert_eq!(tip.version, 3, "2 + 2 + 1 commits");
    assert_eq!(c.log("main", None, 10, None).await.expect("log").entries.len(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn import_errors() {
    let srv = server().await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("imported")
        .build()
        .await
        .expect("connect");
    let tmp = tempfile::tempdir().expect("tempdir");
    // 不是 git 仓库
    assert!(matches!(
        import(&c, tmp.path(), "main", "main", |_, _| {}).await,
        Err(Error::Git(_))
    ));
    // 引用不存在
    git(tmp.path(), &["init", "-q", "-b", "main"]);
    assert!(matches!(
        import(&c, tmp.path(), "nope", "main", |_, _| {}).await,
        Err(Error::Git(_))
    ));
    // 服务端仓库不存在
    write(tmp.path(), "a", b"1");
    commit(tmp.path(), "one", "1700000000 +0000");
    let other = Client::builder()
        .endpoint(&srv.url)
        .repo("missing")
        .build()
        .await
        .expect("connect");
    assert!(matches!(
        import(&other, tmp.path(), "main", "main", |_, _| {}).await,
        Err(Error::Client(_))
    ));
}
