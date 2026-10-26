//! 工作副本端到端：进程内启动服务端，两个工作副本模拟两个开发者协作。

use std::path::Path;
use std::sync::Arc;

use scion_api::{App, ServerConfig, serve};
use scion_client::Client;
use scion_proto::v1::ErrorReason;
use scion_store::{MemStore, MetaStore};
use scion_wc::{
    ConflictKind, Error, MergeOutcome, PickKind, PickOutcome, Side, Sparse, Status, SyncOutcome, WorkingCopy,
};
use tokio::net::TcpListener;

struct Server {
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn server() -> Server {
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
    Server { url, _shutdown: tx }
}

async fn client(url: &str) -> Client {
    Client::builder()
        .endpoint(url)
        .repo("mono")
        .build()
        .await
        .expect("connect")
}

fn write(root: &Path, rel: &str, content: &str) {
    let p = root.join(rel);
    std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
    std::fs::write(p, content).expect("write");
}

fn status(added: &[&str], modified: &[&str], deleted: &[&str]) -> Status {
    let v = |x: &[&str]| x.iter().map(|s| s.to_string()).collect();
    Status {
        added: v(added),
        modified: v(modified),
        deleted: v(deleted),
        conflicted: Vec::new(),
    }
}

#[tokio::test]
async fn two_developers_collaborate() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("alice"), tmp.path().join("bob"));

    // Alice 克隆空仓库，写文件，提交并合入
    let mut alice = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone empty");
    assert!(alice.status().expect("status").is_clean());
    write(&a_dir, "src/main.rs", "fn main() {}\n");
    write(&a_dir, "README.md", "hello\n");
    assert_eq!(
        alice.status().expect("status"),
        status(&["README.md", "src/main.rs"], &[], &[])
    );
    let first = alice
        .commit(&c, "init", Some(("Alice", "alice@example.com")))
        .await
        .expect("commit")
        .expect("changes");
    assert!(
        alice.status().expect("status").is_clean(),
        "committed changes are part of the snapshot"
    );
    // ref 还没动：没有需要同步的
    assert_eq!(alice.sync(&c).await.expect("sync"), SyncOutcome::UpToDate);
    let landed = alice.land(&c).await.expect("land");
    assert_eq!(landed.tip, first.id);
    assert!(matches!(alice.land(&c).await, Err(Error::NothingToLand)));
    assert_eq!(alice.commit(&c, "noop", None).await.expect("noop"), None);

    // Bob 克隆，看到 Alice 的文件
    let mut bob = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");
    assert_eq!(
        std::fs::read_to_string(b_dir.join("src/main.rs")).expect("read"),
        "fn main() {}\n"
    );
    assert!(bob.status().expect("status").is_clean());
    assert_eq!(bob.state().base.as_deref(), Some(first.id.to_string().as_str()));

    // Bob 修改、删除、新增，并合入
    write(&b_dir, "src/main.rs", "fn main() { println!(\"hi\"); }\n");
    std::fs::remove_file(b_dir.join("README.md")).expect("rm");
    write(&b_dir, "docs/guide.md", "guide\n");
    assert_eq!(
        bob.status().expect("status"),
        status(&["docs/guide.md"], &["src/main.rs"], &["README.md"])
    );
    bob.commit(&c, "edit", None).await.expect("commit").expect("changes");
    bob.land(&c).await.expect("land");

    // Alice 同步，得到 Bob 的改动；被删的文件和变空的目录消失
    let outcome = alice.sync(&c).await.expect("sync");
    assert!(
        matches!(
            outcome,
            SyncOutcome::Updated {
                written: 2,
                removed: 1,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(
        std::fs::read_to_string(a_dir.join("docs/guide.md")).expect("read"),
        "guide\n"
    );
    assert!(!a_dir.join("README.md").exists());
    assert!(alice.status().expect("status").is_clean());
    assert_eq!(alice.sync(&c).await.expect("again"), SyncOutcome::UpToDate);

    // Bob 落后于 ref：有未提交的改动时只允许快进，被拒；sync 做文件级 rebase 后即可合入
    write(&b_dir, "late.txt", "late\n");
    write(&a_dir, "early.txt", "early\n");
    alice.commit(&c, "a", None).await.expect("commit").expect("changes");
    alice.land(&c).await.expect("alice lands first");
    bob.commit(&c, "bob's change", Some(("Bob", "bob@example.com")))
        .await
        .expect("commit")
        .expect("changes");
    write(&b_dir, "scratch.txt", "uncommitted\n");
    let err = bob.land(&c).await.expect_err("bob is behind with local edits");
    assert!(
        matches!(&err, Error::Client(e) if e.reason() == Some(ErrorReason::NotFastForward)),
        "{err:?}"
    );
    std::fs::remove_file(b_dir.join("scratch.txt")).expect("rm");
    let outcome = bob.sync(&c).await.expect("rebase");
    assert!(
        matches!(
            outcome,
            SyncOutcome::Rebased {
                written: 1,
                removed: 0,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert!(b_dir.join("early.txt").exists(), "upstream change applied locally");
    assert!(bob.status().expect("status").is_clean());
    let landed = bob.land(&c).await.expect("land after rebase");
    // 合入后两边的文件一致
    alice.sync(&c).await.expect("alice syncs");
    assert_eq!(std::fs::read_to_string(a_dir.join("late.txt")).expect("read"), "late\n");
    assert_eq!(alice.state().base.as_deref(), Some(landed.tip.to_string().as_str()));
    let meta = c
        .read_file(&landed.tip.to_string(), "late.txt")
        .await
        .expect("server has it");
    assert_eq!(meta.as_ref(), b"late\n");
}

/// 工作区干净时，落后的提交直接合入：服务端 rebase，合入后工作区包含上游改动。
#[tokio::test]
async fn land_while_behind_rebases_on_server_and_updates_working_copy() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "shared.txt", "1\n2\n3\n");
    a.commit(&c, "base", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");

    write(&a_dir, "alice.txt", "alice\n");
    write(&a_dir, "shared.txt", "one\n2\n3\n");
    a.commit(&c, "a", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("alice lands");

    write(&b_dir, "bob.txt", "bob\n");
    write(&b_dir, "shared.txt", "1\n2\nthree\n");
    b.commit(&c, "b", None).await.expect("commit").expect("changes");
    let landed = b.land(&c).await.expect("server-side rebase");
    assert!(landed.rebased());
    assert_eq!(b.state().base.as_deref(), Some(landed.tip.to_string().as_str()));
    assert!(b.state().head.is_none());
    // 上游新增的文件与三方合并的结果都写入了工作区，状态干净
    assert_eq!(
        std::fs::read_to_string(b_dir.join("alice.txt")).expect("read"),
        "alice\n"
    );
    assert_eq!(
        std::fs::read_to_string(b_dir.join("shared.txt")).expect("read"),
        "one\n2\nthree\n"
    );
    assert!(b.status().expect("status").is_clean());

    // 与 ref 上的改动冲突：返回冲突详情，本地状态不变
    a.sync(&c).await.expect("alice syncs");
    write(&a_dir, "shared.txt", "uno\n2\nthree\n");
    a.commit(&c, "a2", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("alice lands");
    write(&b_dir, "shared.txt", "eins\n2\nthree\n");
    b.commit(&c, "b2", None).await.expect("commit").expect("changes");
    let err = b.land(&c).await.expect_err("conflict");
    let conflict = match &err {
        Error::Client(e) => e.merge_conflict().expect("details"),
        other => panic!("{other:?}"),
    };
    assert_eq!(conflict.paths[0].path, "shared.txt");
    assert!(b.state().head.is_some(), "commit kept for resolution");
}

#[tokio::test]
async fn rebase_conflict_reported_and_identical_changes_merge() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "shared.txt", "base\n");
    a.commit(&c, "base", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");

    // 双方都改了同一个文件且内容不同：冲突
    write(&a_dir, "shared.txt", "alice\n");
    a.commit(&c, "a", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    write(&b_dir, "shared.txt", "bob\n");
    b.commit(&c, "b", None).await.expect("commit").expect("changes");
    let err = b.sync(&c).await.expect_err("conflict");
    assert!(
        matches!(&err, Error::Conflict(p) if p == &vec!["shared.txt".to_string()]),
        "{err:?}"
    );
    // The working copy moved to the new tip; the file holds conflict markers
    let marked = std::fs::read_to_string(b_dir.join("shared.txt")).expect("read");
    assert!(
        marked.contains("<<<<<<<") && marked.contains("bob") && marked.contains("alice"),
        "{marked}"
    );
    assert_eq!(b.state().base, a.state().base);
    assert!(b.state().head.is_none());
    let st = b.status().expect("status");
    assert_eq!(
        (st.modified.as_slice(), st.conflicted.as_slice()),
        (&["shared.txt".to_string()][..], &["shared.txt".to_string()][..])
    );
    // Nothing moves on until it is resolved
    assert!(matches!(b.commit(&c, "", None).await, Err(Error::Unresolved(_))));
    assert!(matches!(b.sync(&c).await, Err(Error::Unresolved(_))));
    assert!(matches!(
        b.resolve(&c, &["shared.txt".into()], None, false).await,
        Err(Error::MarkersRemain(_))
    ));
    assert!(matches!(
        b.resolve(&c, &["other.txt".into()], None, false).await,
        Err(Error::NotConflicted(_))
    ));
    write(&b_dir, "shared.txt", "alice and bob\n");
    assert!(
        b.resolve(&c, &["shared.txt".into()], None, false)
            .await
            .expect("resolve")
            .is_empty()
    );
    assert!(matches!(b.resolve(&c, &[".".into()], None, false).await, Ok(r) if r.is_empty()));
    // Commit without a message reuses the pending commit's
    b.commit(&c, "", None).await.expect("commit").expect("changes");
    assert!(b.state().conflict.is_none());
    b.land(&c).await.expect("land");
    let tip = c.log("main", None, 1, None).await.expect("log").entries.remove(0);
    assert_eq!(tip.message, "b");
    assert!(matches!(
        b.resolve(&c, &[".".into()], None, false).await,
        Err(Error::NoConflict)
    ));

    // Different lines of one file merge cleanly during sync
    a.sync(&c).await.expect("sync a");
    write(&a_dir, "lines.txt", "1\n2\n3\n4\n5\n");
    a.commit(&c, "lines", None).await.expect("commit");
    a.land(&c).await.expect("land");
    b.sync(&c).await.expect("sync b");
    write(&a_dir, "lines.txt", "one\n2\n3\n4\n5\n");
    a.commit(&c, "a lines", None).await.expect("commit");
    a.land(&c).await.expect("land");
    write(&b_dir, "lines.txt", "1\n2\n3\n4\nfive\n");
    b.commit(&c, "b lines", None).await.expect("commit");
    assert!(matches!(b.sync(&c).await, Ok(SyncOutcome::Rebased { .. })));
    assert_eq!(
        std::fs::read_to_string(b_dir.join("lines.txt")).expect("read"),
        "one\n2\n3\n4\nfive\n"
    );
    assert!(b.status().expect("status").is_clean());
    b.land(&c).await.expect("land merged lines");

    // --theirs takes upstream's version; a delete/modify conflict can be resolved by choosing
    a.sync(&c).await.expect("sync a");
    write(&a_dir, "shared.txt", "from a\n");
    write(&a_dir, "lines.txt", "gone soon\n");
    a.commit(&c, "a again", None).await.expect("commit");
    a.land(&c).await.expect("land");
    write(&b_dir, "shared.txt", "from b\n");
    std::fs::remove_file(b_dir.join("lines.txt")).expect("rm");
    b.commit(&c, "b again", None).await.expect("commit");
    assert!(matches!(b.sync(&c).await, Err(Error::Conflict(p)) if p.len() == 2));
    assert_eq!(b.status().expect("status").conflicted, ["lines.txt", "shared.txt"]);
    let left = b
        .resolve(&c, &["shared.txt".into()], Some(scion_wc::Side::Theirs), false)
        .await
        .expect("theirs");
    assert_eq!(left, ["lines.txt"]);
    assert_eq!(
        std::fs::read_to_string(b_dir.join("shared.txt")).expect("read"),
        "from a\n"
    );
    b.resolve(&c, &["lines.txt".into()], Some(scion_wc::Side::Ours), false)
        .await
        .expect("ours: deleted");
    assert!(!b_dir.join("lines.txt").exists());
    // The only remaining change is the deletion
    assert_eq!(b.status().expect("status"), status(&[], &[], &["lines.txt"]));
    b.commit(&c, "", None).await.expect("commit").expect("changes");
    b.land(&c).await.expect("land");

    // 双方做了完全相同的改动：rebase 后没有剩余改动，直接同步
    let c_dir = tmp.path().join("c");
    let mut cc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &c_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "same.txt", "same\n");
    a.commit(&c, "same", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    write(&c_dir, "same.txt", "same\n");
    cc.commit(&c, "same too", None).await.expect("commit").expect("changes");
    let outcome = cc.sync(&c).await.expect("sync");
    assert!(matches!(outcome, SyncOutcome::Updated { .. }), "{outcome:?}");
    assert!(cc.state().head.is_none());
    assert!(matches!(cc.land(&c).await, Err(Error::NothingToLand)));
}

#[tokio::test]
async fn local_changes_report_old_versions_and_new_content() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    write(&dir, "keep", "k");
    write(&dir, "edit", "v1");
    write(&dir, "gone", "g");
    wc.commit(&c, "base", None).await.expect("commit").expect("changes");
    write(&dir, "edit", "v2");
    std::fs::remove_file(dir.join("gone")).expect("rm");
    write(&dir, "new", "n");
    let mut changes = wc.local_changes().expect("changes");
    changes.sort_by(|a, b| a.path.cmp(&b.path));
    let paths: Vec<_> = changes.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(paths, ["edit", "gone", "new"]);
    let edit = &changes[0];
    assert_eq!(edit.old.map(|(_, id)| id), Some(scion_object::ObjectId::of_blob(b"v1")));
    assert_eq!(edit.new.as_ref().map(|(_, b)| b.as_ref()), Some(&b"v2"[..]));
    assert!(changes[1].new.is_none() && changes[1].old.is_some(), "deleted");
    assert!(changes[2].old.is_none() && changes[2].new.is_some(), "added");
}

#[tokio::test]
async fn sync_refuses_dirty_tree() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    assert_eq!(wc.sync(&c).await.expect("empty repo, no ref"), SyncOutcome::UpToDate);
    write(&dir, "a/b/c.txt", "x");
    assert!(matches!(wc.sync(&c).await, Err(Error::Dirty)));
}

#[tokio::test]
async fn open_from_subdirectory() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    write(&dir, "a/b/c.txt", "x");
    let wc = WorkingCopy::open(&dir.join("a/b")).expect("open from subdir");
    assert_eq!(wc.root(), dir.as_path());
    assert_eq!(wc.state().repo, "mono");
    assert_eq!(wc.state().endpoint, srv.url);
    assert!(matches!(WorkingCopy::open(tmp.path()), Err(Error::NotAWorkingCopy(_))));
}

#[tokio::test]
async fn clone_reports_progress() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let src = tmp.path().join("src");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &src, Sparse::full())
        .await
        .expect("clone");
    write(&src, "a.txt", "aa");
    write(&src, "d/b.txt", "bbb");
    wc.commit(&c, "two files", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    let seen = std::sync::Mutex::new(Vec::new());
    WorkingCopy::clone_with_progress(
        &c,
        &srv.url,
        "mono",
        "main",
        &tmp.path().join("dst"),
        Sparse::full(),
        &|p| seen.lock().expect("lock").push(p),
    )
    .await
    .expect("clone");
    let seen = seen.into_inner().expect("lock");
    assert!(seen.contains(&scion_wc::Progress::Listing { files: 2 }), "{seen:?}");
    assert_eq!(
        seen.last(),
        Some(&scion_wc::Progress::Checkout {
            done: 2,
            total: 2,
            bytes: 5
        })
    );
}

#[tokio::test]
async fn restore_from_an_older_commit() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    write(&dir, "a.txt", "1");
    write(&dir, "d/b.txt", "b");
    let v1 = wc.commit(&c, "v1", None).await.expect("commit").expect("created");
    wc.land(&c).await.expect("land");
    write(&dir, "a.txt", "2");
    std::fs::remove_file(dir.join("d/b.txt")).expect("rm");
    write(&dir, "c.txt", "c");
    wc.commit(&c, "v2", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    write(&dir, "new.txt", "untracked");

    let root = c.root_tree(&v1.id.to_string()).await.expect("root");
    let r = wc.restore_from(&c, root, &["d".into()]).await.expect("restore d");
    assert_eq!(r.restored, ["d/b.txt"]);
    assert_eq!(std::fs::read_to_string(dir.join("d/b.txt")).expect("b"), "b");
    let r = wc.restore_from(&c, root, &[".".into()]).await.expect("restore all");
    assert_eq!(r.restored, ["a.txt", "c.txt"], "d/b.txt is already v1's");
    assert_eq!(r.kept_added, ["d/b.txt", "new.txt"]);
    assert_eq!(std::fs::read_to_string(dir.join("a.txt")).expect("a"), "1");
    assert!(!dir.join("c.txt").exists());
    assert!(dir.join("new.txt").exists(), "added files are kept");
    assert_eq!(
        wc.status().expect("status"),
        status(&["d/b.txt", "new.txt"], &["a.txt"], &["c.txt"]),
        "local changes on top of v2"
    );
}

#[tokio::test]
async fn clone_into_non_empty_dir_rejected() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    write(tmp.path(), "existing", "x");
    let err = WorkingCopy::clone(&c, &srv.url, "mono", "main", tmp.path(), Sparse::full())
        .await
        .err()
        .expect("rejected");
    assert!(matches!(err, Error::TargetNotEmpty(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn modes_roundtrip_and_mode_change_detected() {
    use std::os::unix::fs::PermissionsExt;
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "run.sh", "#!/bin/sh\n");
    std::fs::set_permissions(a_dir.join("run.sh"), std::fs::Permissions::from_mode(0o755)).expect("chmod");
    std::os::unix::fs::symlink("run.sh", a_dir.join("latest")).expect("symlink");
    a.commit(&c, "modes", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");

    WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");
    let mode = std::fs::metadata(b_dir.join("run.sh"))
        .expect("meta")
        .permissions()
        .mode();
    assert_eq!(mode & 0o111, 0o111, "executable bit restored");
    assert_eq!(
        std::fs::read_link(b_dir.join("latest")).expect("link").to_str(),
        Some("run.sh")
    );

    // 只改权限也算修改
    std::fs::set_permissions(a_dir.join("run.sh"), std::fs::Permissions::from_mode(0o644)).expect("chmod");
    assert_eq!(a.status().expect("status"), status(&[], &["run.sh"], &[]));
}

#[tokio::test]
async fn racy_same_size_edit_detected_by_hash() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    write(&dir, "f.txt", "aaaa");
    wc.commit(&c, "v1", None).await.expect("commit").expect("changes");
    // 同样大小的改动，并把修改时间改回与记录完全一致：只靠时间戳无法发现
    let recorded = std::fs::metadata(dir.join("f.txt"))
        .expect("meta")
        .modified()
        .expect("mtime");
    std::fs::write(dir.join("f.txt"), "bbbb").expect("edit");
    std::fs::File::options()
        .write(true)
        .open(dir.join("f.txt"))
        .expect("open")
        .set_modified(recorded)
        .expect("touch");
    // 记录的修改时间早于 dirstate 写入时间时，按规则视为未修改（与 git 相同的取舍）；
    // 但如果修改时间不早于写入时间（同一时间粒度内），必须重新计算哈希并发现改动。
    // 这里把 dirstate 的写入时间改回过去，模拟这种情况。
    let ds_path = dir.join(".scion/dirstate.json");
    let mut ds: serde_json::Value = serde_json::from_slice(&std::fs::read(&ds_path).expect("read")).expect("json");
    ds["written_at_ns"] = serde_json::json!(0);
    std::fs::write(&ds_path, serde_json::to_vec(&ds).expect("ser")).expect("write");
    assert_eq!(wc.status().expect("status"), status(&[], &["f.txt"], &[]));
}

#[tokio::test]
async fn concurrent_operations_are_locked_out() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    std::fs::write(dir.join(".scion/lock"), "12345").expect("simulate other process");
    write(&dir, "a", "a");
    assert!(matches!(wc.commit(&c, "m", None).await, Err(Error::Locked(_))));
    std::fs::remove_file(dir.join(".scion/lock")).expect("unlock");
    assert!(wc.commit(&c, "m", None).await.expect("commit").is_some());
}

fn read(root: &Path, rel: &str) -> String {
    std::fs::read_to_string(root.join(rel)).expect("read")
}

fn sparse(dirs: &[&str]) -> Sparse {
    Sparse::dirs(dirs).expect("sparse")
}

/// 稀疏检出：只写范围内的文件；提交、合入、sync、rebase 不影响范围外的内容；范围可以调整。
#[tokio::test]
async fn sparse_checkout() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let seed = c
        .changeset(None)
        .put("top.txt", "top")
        .put("a/x.txt", "ax")
        .put("a/b/y.txt", "y1")
        .put("a/bc/w.txt", "w")
        .put("c/z.txt", "z1")
        .create()
        .await
        .expect("seed");
    c.land("main", &seed.id).await.expect("land seed");
    let tmp = tempfile::tempdir().expect("tempdir");
    let (dir, full_dir) = (tmp.path().join("sparse"), tmp.path().join("full"));

    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, sparse(&["a/b/"]))
        .await
        .expect("sparse clone");
    assert_eq!(read(&dir, "top.txt"), "top", "root files are always checked out");
    assert_eq!(read(&dir, "a/b/y.txt"), "y1");
    for outside in ["a/x.txt", "a/bc", "c"] {
        assert!(!dir.join(outside).exists(), "{outside}");
    }
    assert_eq!(wc.state().sparse, Some(vec!["a/b".to_string()]));
    assert_eq!(wc.sparse().expect("sparse").list(), Some(&["a/b".to_string()][..]));
    assert!(
        wc.status().expect("status").is_clean(),
        "unfetched files are not deleted"
    );
    let reopened = WorkingCopy::open(&dir.join("a/b")).expect("open");
    assert_eq!(reopened.sparse().expect("sparse"), sparse(&["a/b"]));

    // 范围外的新文件：拒绝，避免覆盖服务端上没有检出的内容
    write(&dir, "c/z.txt", "shadow");
    let e = wc.status().expect_err("outside");
    assert!(
        matches!(&e, Error::OutsideSparse(p) if p == &["c/z.txt".to_string()]),
        "{e}"
    );
    assert!(matches!(wc.commit(&c, "x", None).await, Err(Error::OutsideSparse(_))));
    std::fs::remove_dir_all(dir.join("c")).expect("rm");

    // 提交并合入范围内的改动：服务端保留范围外的文件
    write(&dir, "a/b/y.txt", "y2");
    wc.commit(&c, "edit y", None).await.expect("commit").expect("changes");
    wc.land(&c).await.expect("land");
    assert_eq!(c.read_file("main", "c/z.txt").await.expect("kept").as_ref(), b"z1");
    assert_eq!(c.read_file("main", "a/x.txt").await.expect("kept").as_ref(), b"ax");

    // 别人改了范围内外的文件：sync 只写范围内的
    let mut full = WorkingCopy::clone(&c, &srv.url, "mono", "main", &full_dir, Sparse::full())
        .await
        .expect("full clone");
    assert!(full.sparse().expect("sparse").is_full());
    write(&full_dir, "c/z.txt", "z2");
    write(&full_dir, "a/b/new.txt", "new");
    full.commit(&c, "upstream", None)
        .await
        .expect("commit")
        .expect("changes");
    full.land(&c).await.expect("land");
    let o = wc.sync(&c).await.expect("sync");
    assert!(
        matches!(
            o,
            SyncOutcome::Updated {
                written: 1,
                removed: 0,
                ..
            }
        ),
        "{o:?}"
    );
    assert_eq!(read(&dir, "a/b/new.txt"), "new");
    assert!(!dir.join("c").exists());

    // 未合入的提交 + 上游前进：客户端 rebase 只比较范围内的文件，不会把范围外的当作删除
    write(&dir, "a/b/y.txt", "y3");
    wc.commit(&c, "edit y again", None)
        .await
        .expect("commit")
        .expect("changes");
    full.sync(&c).await.expect("sync full");
    write(&full_dir, "c/z.txt", "z3");
    write(&full_dir, "a/b/other.txt", "o");
    full.commit(&c, "upstream 2", None)
        .await
        .expect("commit")
        .expect("changes");
    full.land(&c).await.expect("land");
    let o = wc.sync(&c).await.expect("rebase");
    assert!(
        matches!(
            o,
            SyncOutcome::Rebased {
                written: 1,
                removed: 0,
                ..
            }
        ),
        "{o:?}"
    );
    wc.land(&c).await.expect("land rebased");
    assert_eq!(c.read_file("main", "c/z.txt").await.expect("kept").as_ref(), b"z3");
    assert_eq!(c.read_file("main", "a/b/y.txt").await.expect("mine").as_ref(), b"y3");

    // 合入时服务端 rebase：把上游在范围内的改动写到本地
    write(&dir, "a/b/y.txt", "y4");
    wc.commit(&c, "edit y 4", None).await.expect("commit").expect("changes");
    full.sync(&c).await.expect("sync full");
    write(&full_dir, "a/b/third.txt", "t");
    write(&full_dir, "c/z.txt", "z4");
    full.commit(&c, "upstream 3", None)
        .await
        .expect("commit")
        .expect("changes");
    full.land(&c).await.expect("land");
    wc.land(&c).await.expect("land with server rebase");
    assert_eq!(read(&dir, "a/b/third.txt"), "t");
    assert!(!dir.join("c").exists());
    assert_eq!(c.read_file("main", "c/z.txt").await.expect("kept").as_ref(), b"z4");

    // 调整范围
    let added = wc.sparse().expect("sparse").add(&["c"]).expect("add");
    assert_eq!(wc.set_sparse(&c, added).await.expect("add c"), (1, 0));
    assert_eq!(read(&dir, "c/z.txt"), "z4");
    let removed = wc.sparse().expect("sparse").remove(&["a/b"]).expect("rm");
    assert_eq!(wc.set_sparse(&c, removed).await.expect("rm a/b"), (0, 4));
    assert!(!dir.join("a").exists(), "emptied directories are removed");
    assert_eq!(wc.state().sparse, Some(vec!["c".to_string()]));
    write(&dir, "c/z.txt", "dirty");
    assert!(matches!(wc.set_sparse(&c, Sparse::full()).await, Err(Error::Dirty)));
    write(&dir, "c/z.txt", "z4");
    assert_eq!(wc.set_sparse(&c, Sparse::full()).await.expect("full"), (6, 0));
    assert_eq!(read(&dir, "a/x.txt"), "ax");
    assert_eq!(read(&dir, "a/bc/w.txt"), "w");
    assert_eq!(wc.state().sparse, None);
    assert!(wc.status().expect("status").is_clean());
}

#[tokio::test]
async fn sparse_settings_on_an_empty_repository() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, sparse(&[]))
        .await
        .expect("clone");
    assert_eq!(wc.set_sparse(&c, sparse(&["src"])).await.expect("set"), (0, 0));
    assert_eq!(wc.state().sparse, Some(vec!["src".to_string()]));
}

/// 调整范围时，旧范围外的未跟踪文件按新范围判断，不覆盖内容不同的本地文件。
#[tokio::test]
async fn sparse_changes_judge_untracked_files_by_the_new_scope() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let seed = c
        .changeset(None)
        .put("a/x.txt", "x")
        .put("c/z.txt", "z")
        .put("c/w.txt", "w")
        .create()
        .await
        .expect("seed");
    c.land("main", &seed.id).await.expect("land");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, sparse(&["a"]))
        .await
        .expect("clone");

    // 和服务端版本不同：拒绝，不覆盖，范围不变
    write(&dir, "c/z.txt", "local edit");
    let e = wc
        .set_sparse(&c, sparse(&["a", "c"]))
        .await
        .expect_err("would overwrite");
    assert!(
        matches!(&e, Error::WouldOverwrite(p) if p == &["c/z.txt".to_string()]),
        "{e}"
    );
    assert_eq!(read(&dir, "c/z.txt"), "local edit");
    assert_eq!(wc.state().sparse, Some(vec!["a".to_string()]));
    // 新范围也不包含的：仍是范围外
    let e = wc.set_sparse(&c, sparse(&["a", "q"])).await.expect_err("still outside");
    assert!(
        matches!(&e, Error::OutsideSparse(p) if p == &["c/z.txt".to_string()]),
        "{e}"
    );

    // 和服务端版本相同的直接接管；服务端没有的保留为新增文件
    write(&dir, "c/z.txt", "z");
    write(&dir, "c/new.txt", "n");
    let added = wc.sparse().expect("sparse").add(&["c"]).expect("add");
    assert_eq!(
        wc.set_sparse(&c, added).await.expect("add c"),
        (1, 0),
        "only c/w.txt is written"
    );
    assert_eq!(read(&dir, "c/w.txt"), "w");
    assert_eq!(wc.status().expect("status"), status(&["c/new.txt"], &[], &[]));
    wc.commit(&c, "add new", None).await.expect("commit").expect("changes");
    wc.land(&c).await.expect("land");
    assert_eq!(c.read_file("main", "c/new.txt").await.expect("landed").as_ref(), b"n");

    // 已跟踪的改动仍要求先提交
    write(&dir, "a/x.txt", "edited");
    assert!(matches!(wc.set_sparse(&c, Sparse::full()).await, Err(Error::Dirty)));
}

#[tokio::test]
async fn ignored_files_stay_local_and_tracked_files_stay_tracked() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("alice"), tmp.path().join("bob"));
    let mut alice = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "src/lib.rs", "pub fn f() {}\n");
    write(&a_dir, "config.local", "secret\n");
    write(&a_dir, ".gitignore", "target/\n*.local\n");
    write(&a_dir, "target/debug/app", "binary\n");
    assert_eq!(
        alice.status().expect("status"),
        status(&[".gitignore", "src/lib.rs"], &[], &[]),
        "build output and local config are ignored"
    );
    alice.commit(&c, "init", None).await.expect("commit");
    alice.land(&c).await.expect("land");

    // Bob adds a .scionignore rule for a file that is already tracked: it stays tracked
    let bob = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone bob");
    assert!(!b_dir.join("target").exists() && !b_dir.join("config.local").exists());
    write(&b_dir, ".scionignore", "src/\n");
    write(&b_dir, "src/lib.rs", "pub fn f() -> u8 { 1 }\n");
    write(&b_dir, "src/new.rs", "// untracked and ignored\n");
    assert_eq!(
        bob.status().expect("status"),
        status(&[".scionignore"], &["src/lib.rs"], &[])
    );
    // Ignored local files do not make the tree dirty, so sync works
    write(&a_dir, "README.md", "hi\n");
    alice.commit(&c, "readme", None).await.expect("commit 2");
    alice.land(&c).await.expect("land 2");
    write(&a_dir, "target/release/app", "more output\n");
    assert!(alice.status().expect("status").is_clean());
    assert!(matches!(alice.sync(&c).await, Ok(SyncOutcome::UpToDate)));
}

#[tokio::test]
async fn restore_puts_tracked_files_back_and_keeps_new_ones() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    write(&dir, "a.txt", "a\n");
    write(&dir, "src/x.rs", "x\n");
    write(&dir, "src/y.rs", "y\n");
    wc.commit(&c, "init", None).await.expect("commit");
    // Restoring needs no landed base: the pending commit is the snapshot
    write(&dir, "a.txt", "changed\n");
    std::fs::remove_file(dir.join("src/x.rs")).expect("rm");
    write(&dir, "src/y.rs", "changed\n");
    write(&dir, "src/new.rs", "new\n");
    let r = wc.restore(&c, &["src/".into()]).await.expect("restore src");
    assert_eq!(r.restored, ["src/x.rs", "src/y.rs"]);
    assert_eq!(r.kept_added, ["src/new.rs"]);
    assert_eq!(std::fs::read_to_string(dir.join("src/x.rs")).expect("x"), "x\n");
    assert_eq!(wc.status().expect("status"), status(&["src/new.rs"], &["a.txt"], &[]));
    let r = wc.restore(&c, &[".".into()]).await.expect("restore all");
    assert_eq!(
        (r.restored.as_slice(), r.kept_added.as_slice()),
        (&["a.txt".to_string()][..], &["src/new.rs".to_string()][..])
    );
    let r = wc
        .restore(&c, &["src/x".into(), "nope".into()])
        .await
        .expect("no match");
    assert!(
        r.restored.is_empty() && r.kept_added.is_empty(),
        "src/x does not match src/x.rs"
    );
    assert_eq!(wc.status().expect("status"), status(&["src/new.rs"], &[], &[]));
}

#[tokio::test]
async fn uncommit_and_amend_rewrite_the_pending_commit() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    assert!(matches!(wc.uncommit(&c).await, Err(Error::NoPendingCommit)));
    write(&dir, "keep.txt", "1\n");
    write(&dir, "gone.txt", "g\n");
    wc.commit(&c, "base", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    assert!(
        matches!(wc.amend(&c, None, None).await, Err(Error::NoPendingCommit)),
        "landed"
    );

    // A pending commit that edits, deletes and adds
    write(&dir, "keep.txt", "2\n");
    std::fs::remove_file(dir.join("gone.txt")).expect("rm");
    write(&dir, "new.txt", "n\n");
    let first = wc
        .commit(&c, "work", Some(("Ada", "ada@example.com")))
        .await
        .expect("commit")
        .expect("created");
    assert!(wc.status().expect("status").is_clean());
    // Uncommit: the same changes are back as uncommitted, the files untouched
    assert_eq!(wc.uncommit(&c).await.expect("uncommit"), first.id);
    assert_eq!(
        wc.status().expect("status"),
        status(&["new.txt"], &["keep.txt"], &["gone.txt"])
    );
    assert_eq!(std::fs::read_to_string(dir.join("keep.txt")).expect("keep"), "2\n");
    assert!(matches!(wc.land(&c).await, Err(Error::NothingToLand)));

    // Commit again, then amend with one more change: old message and author are kept
    wc.commit(&c, "work", Some(("Ada", "ada@example.com")))
        .await
        .expect("again");
    write(&dir, "more.txt", "m\n");
    let amended = wc.amend(&c, None, None).await.expect("amend").expect("created");
    assert_ne!(amended.id, first.id);
    assert!(wc.status().expect("status").is_clean());
    let log = c.log("main", None, 5, None).await.expect("log");
    assert_eq!(log.entries.len(), 1, "nothing landed yet");
    let landed = wc.land(&c).await.expect("land");
    let tip = c.log("main", None, 1, None).await.expect("log").entries.remove(0);
    assert_eq!(tip.id, landed.tip);
    assert_eq!((tip.message.as_str(), tip.author.name.as_str()), ("work", "Ada"));
    let changed: Vec<String> = c
        .diff(Some(&tip.parents[0].to_string()), &tip.id.to_string(), None)
        .await
        .expect("diff")
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert_eq!(changed, ["gone.txt", "keep.txt", "more.txt", "new.txt"]);

    // Amending away every change drops the pending commit
    write(&dir, "keep.txt", "3\n");
    wc.commit(&c, "tmp", None).await.expect("tmp");
    write(&dir, "keep.txt", "2\n");
    assert!(wc.amend(&c, Some("renamed"), None).await.expect("amend").is_none());
    assert!(wc.status().expect("status").is_clean());
    assert!(matches!(wc.land(&c).await, Err(Error::NothingToLand)));
}

#[tokio::test]
async fn switch_between_branches() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    assert!(matches!(wc.switch(&c, "feat", true).await, Err(Error::EmptyBranch(_))));
    write(&dir, "a.txt", "main\n");
    wc.commit(&c, "init", None).await.expect("commit");
    assert!(matches!(wc.switch(&c, "feat", true).await, Err(Error::PendingCommit)));
    wc.land(&c).await.expect("land");

    // Create feat at the current snapshot, change it there
    let s = wc.switch(&c, "feat", true).await.expect("create feat");
    assert_eq!((s.written, s.removed), (0, 0));
    assert_eq!(wc.state().reference, "feat");
    write(&dir, "f.txt", "feature\n");
    wc.commit(&c, "feature", None).await.expect("commit");
    wc.land(&c).await.expect("land on feat");
    assert!(c.resolve_ref("main").await.expect("main").is_some());

    // Back to main: f.txt goes away, an untracked file is carried along
    write(&dir, "notes.txt", "mine\n");
    let s = wc.switch(&c, "main", false).await.expect("switch");
    assert_eq!((s.written, s.removed), (0, 1));
    assert!(!dir.join("f.txt").exists());
    assert_eq!(wc.status().expect("status"), status(&["notes.txt"], &[], &[]));
    // An untracked file where the target has one is not overwritten
    write(&dir, "f.txt", "local\n");
    assert!(matches!(wc.switch(&c, "feat", false).await, Err(Error::WouldOverwrite(p)) if p == ["f.txt"]));
    std::fs::remove_file(dir.join("f.txt")).expect("rm");
    write(&dir, "a.txt", "edited\n");
    assert!(matches!(wc.switch(&c, "feat", false).await, Err(Error::Dirty)));
    write(&dir, "a.txt", "main\n");
    assert!(matches!(wc.switch(&c, "nope", false).await, Err(Error::RefNotFound(r)) if r == "nope"));
    wc.switch(&c, "feat", false).await.expect("switch to feat");
    assert_eq!(std::fs::read_to_string(dir.join("f.txt")).expect("f"), "feature\n");
}

#[tokio::test]
async fn merge_branches() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let other_dir = tmp.path().join("other");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    write(&dir, "a.txt", "1\n2\n3\n4\n5\n");
    write(&dir, "c.txt", "c\n");
    wc.commit(&c, "init", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    wc.switch(&c, "feat", true).await.expect("feat");
    write(&dir, "a.txt", "one\n2\n3\n4\n5\n");
    write(&dir, "f.txt", "feature\n");
    std::fs::remove_file(dir.join("c.txt")).expect("rm");
    wc.commit(&c, "feature", None).await.expect("commit");
    wc.land(&c).await.expect("land feat");
    wc.switch(&c, "main", false).await.expect("main");
    write(&dir, "a.txt", "1\n2\n3\n4\nfive\n");
    wc.commit(&c, "main edit", None).await.expect("commit");
    wc.land(&c).await.expect("land main");

    // A clean merge: lines from both sides, theirs' new and deleted files
    let MergeOutcome::Merged { head, .. } = wc
        .merge(&c, "feat", None, Some(("Ada", "ada@example.com")))
        .await
        .expect("merge")
    else {
        panic!("expected a merge commit");
    };
    assert_eq!(
        std::fs::read_to_string(dir.join("a.txt")).expect("a"),
        "one\n2\n3\n4\nfive\n"
    );
    assert!(dir.join("f.txt").exists() && !dir.join("c.txt").exists());
    assert!(wc.status().expect("status").is_clean());
    assert!(matches!(
        wc.merge(&c, "feat", None, None).await,
        Err(Error::PendingCommit)
    ));

    // main moved after the merge: land refuses, sync redoes the merge on top, keeping both parents
    let mut other = WorkingCopy::clone(&c, &srv.url, "mono", "main", &other_dir, Sparse::full())
        .await
        .expect("clone other");
    write(&other_dir, "o.txt", "other\n");
    other.commit(&c, "other", None).await.expect("commit");
    other.land(&c).await.expect("land other");
    assert!(matches!(wc.land(&c).await, Err(Error::MergeBehind(r)) if r == "main"));
    let SyncOutcome::Rebased { head: rebased, .. } = wc.sync(&c).await.expect("sync") else {
        panic!("expected a rebase");
    };
    assert_ne!(rebased, head);
    let landed = wc.land(&c).await.expect("land merge");
    let tip = c.log("main", None, 1, None).await.expect("log").entries.remove(0);
    assert_eq!(tip.id, landed.tip);
    assert_eq!(tip.parents.len(), 2, "still a merge commit");
    assert_eq!(tip.message, "Merge feat into main");
    assert_eq!(tip.author.name, "Ada");
    let feat_tip = c.resolve_ref("feat").await.expect("feat").expect("tip").changeset;
    assert_eq!(tip.parents[1], feat_tip);
    assert!(matches!(
        wc.merge(&c, "feat", None, None).await,
        Ok(MergeOutcome::UpToDate)
    ));

    // Overlapping edits conflict and change nothing
    wc.switch(&c, "feat", false).await.expect("feat");
    write(&dir, "a.txt", "ONE\n2\n3\n4\n5\n");
    wc.commit(&c, "feat edit", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    wc.switch(&c, "main", false).await.expect("main");
    write(&dir, "a.txt", "uno\n2\n3\n4\nfive\n");
    wc.commit(&c, "main edit 2", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    // Overlapping edits stop the merge with markers; --abort puts everything back
    write(&dir, "new-on-feat.txt", "n\n");
    wc.commit(&c, "noise", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    assert!(matches!(wc.merge(&c, "feat", Some("m"), None).await, Err(Error::Conflict(p)) if p == ["a.txt"]));
    let st = wc.status().expect("status");
    assert_eq!(st.conflicted, ["a.txt"]);
    assert!(
        std::fs::read_to_string(dir.join("a.txt"))
            .expect("a")
            .contains("<<<<<<<")
    );
    assert!(matches!(wc.switch(&c, "feat", false).await, Err(Error::Unresolved(_))));
    wc.abort_merge(&c).await.expect("abort");
    assert!(wc.status().expect("status").is_clean());
    assert!(matches!(wc.abort_merge(&c).await, Err(Error::NoMergeInProgress)));
    // Again, resolved this time: the commit is a merge commit with the saved message
    assert!(matches!(
        wc.merge(&c, "feat", Some("m"), None).await,
        Err(Error::Conflict(_))
    ));
    write(&dir, "a.txt", "uno ONE\n2\n3\n4\nfive\n");
    wc.resolve(&c, &["a.txt".into()], None, false).await.expect("resolve");
    wc.commit(&c, "", None).await.expect("commit").expect("changes");
    wc.land(&c).await.expect("land");
    let tip = c.log("main", None, 1, None).await.expect("log").entries.remove(0);
    assert_eq!((tip.parents.len(), tip.message.as_str()), (2, "m"));
    assert!(matches!(
        wc.merge(&c, "feat", None, None).await,
        Ok(MergeOutcome::UpToDate)
    ));
    assert!(matches!(
        wc.merge(&c, "nope", None, None).await,
        Err(Error::RefNotFound(_))
    ));

    // A file on one side and a directory on the other; theirs' new file is removed by --abort
    wc.switch(&c, "feat", false).await.expect("feat");
    write(&dir, "p/q.txt", "dir on feat\n");
    write(&dir, "added-on-feat.txt", "x\n");
    wc.commit(&c, "dir", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    wc.switch(&c, "main", false).await.expect("main");
    write(&dir, "p", "file on main\n");
    wc.commit(&c, "file", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    match wc.merge(&c, "feat", None, None).await {
        Err(Error::Conflict(p)) => assert!(p.contains(&"p".to_string()), "{p:?}"),
        other => panic!("expected a path conflict, got {other:?}"),
    }
    assert!(dir.join("added-on-feat.txt").exists());
    wc.abort_merge(&c).await.expect("abort");
    assert!(!dir.join("added-on-feat.txt").exists());
    assert!(wc.status().expect("status").is_clean());
    // Resolve by force, keeping our side of the path conflict (p stays a file)
    assert!(wc.merge(&c, "feat", None, None).await.is_err());
    assert!(
        wc.resolve(&c, &[".".into()], None, true)
            .await
            .expect("force")
            .is_empty()
    );
    assert!(dir.join("p").is_file());
    wc.commit(&c, "", None).await.expect("commit").expect("changes");
    // Amending the pending merge keeps it a merge
    write(&dir, "extra.txt", "e\n");
    wc.amend(&c, Some("merge feat, amended"), None).await.expect("amend");
    wc.land(&c).await.expect("land");
    let tip = c.log("main", None, 1, None).await.expect("log").entries.remove(0);
    assert_eq!((tip.parents.len(), tip.message.as_str()), (2, "merge feat, amended"));
}

#[tokio::test]
async fn merge_in_a_sparse_checkout_covers_the_whole_tree() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (full_dir, sparse_dir) = (tmp.path().join("full"), tmp.path().join("sparse"));
    let mut full = WorkingCopy::clone(&c, &srv.url, "mono", "main", &full_dir, Sparse::full())
        .await
        .expect("clone");
    write(&full_dir, "a/x.txt", "x\n");
    write(&full_dir, "b/y.txt", "y\n");
    full.commit(&c, "init", None).await.expect("commit");
    full.land(&c).await.expect("land");
    // feat changes b/, outside the sparse scope used below
    full.switch(&c, "feat", true).await.expect("feat");
    write(&full_dir, "b/y.txt", "feature\n");
    full.commit(&c, "feat", None).await.expect("commit");
    full.land(&c).await.expect("land feat");
    full.switch(&c, "main", false).await.expect("main");

    let mut sparse = WorkingCopy::clone(
        &c,
        &srv.url,
        "mono",
        "main",
        &sparse_dir,
        Sparse::dirs(&["a"]).expect("dirs"),
    )
    .await
    .expect("sparse clone");
    write(&full_dir, "a/x.txt", "main\n");
    full.commit(&c, "main", None).await.expect("commit");
    full.land(&c).await.expect("land main");
    sparse.sync(&c).await.expect("sync");
    sparse.merge(&c, "feat", None, None).await.expect("merge");
    assert!(!sparse_dir.join("b").exists(), "nothing written outside the scope");
    // main moves again: sync redoes the merge over the whole tree
    write(&full_dir, "a/z.txt", "z\n");
    full.commit(&c, "more", None).await.expect("commit");
    full.land(&c).await.expect("land more");
    assert!(matches!(sparse.sync(&c).await, Ok(SyncOutcome::Rebased { .. })));
    let landed = sparse.land(&c).await.expect("land merge");
    let tip = landed.tip.to_string();
    assert_eq!(c.read_file(&tip, "b/y.txt").await.expect("b").as_ref(), b"feature\n");
    assert_eq!(c.read_file(&tip, "a/x.txt").await.expect("a").as_ref(), b"main\n");
    assert_eq!(c.read_file(&tip, "a/z.txt").await.expect("z").as_ref(), b"z\n");
    assert!(sparse_dir.join("a/z.txt").exists());

    // A conflicting merge that also changes files outside the scope cannot be finished here
    full.sync(&c).await.expect("sync full");
    full.switch(&c, "feat", false).await.expect("feat");
    full.sync(&c).await.expect("sync feat");
    write(&full_dir, "a/x.txt", "feat edit\n");
    write(&full_dir, "b/w.txt", "outside\n");
    full.commit(&c, "feat 2", None).await.expect("commit");
    full.land(&c).await.expect("land feat 2");
    full.switch(&c, "main", false).await.expect("main");
    write(&full_dir, "a/x.txt", "main edit\n");
    full.commit(&c, "main 2", None).await.expect("commit");
    full.land(&c).await.expect("land main 2");
    sparse.sync(&c).await.expect("sync sparse");
    match sparse.merge(&c, "feat", None, None).await {
        Err(Error::MergeConflicts(cs)) => {
            assert_eq!((cs[0].path.as_str(), cs[0].kind), ("a/x.txt", ConflictKind::Content))
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(sparse.status().expect("status").is_clean() && sparse.state().conflict.is_none());
}

#[tokio::test]
async fn pick_reverts_and_repeats_a_commit() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (full_dir, sparse_dir) = (tmp.path().join("full"), tmp.path().join("sparse"));
    let mut full = WorkingCopy::clone(&c, &srv.url, "mono", "main", &full_dir, Sparse::full())
        .await
        .expect("clone");
    write(&full_dir, "a/x.txt", "x\n");
    write(&full_dir, "b/y.txt", "y\n");
    full.commit(&c, "init", None).await.expect("commit");
    full.land(&c).await.expect("land");
    write(&full_dir, "a/x.txt", "x2\n");
    let edit = full
        .commit(&c, "edit", None)
        .await
        .expect("commit")
        .expect("changes")
        .id;
    full.land(&c).await.expect("land");
    let parent = c.resolve_revision(&edit.to_string()).await.expect("rev").parents[0];
    let read = || std::fs::read_to_string(full_dir.join("a/x.txt")).expect("read");

    let (revert, again) = (PickKind::Revert, PickKind::CherryPick);
    let o = full.pick(&c, revert, edit, Some(parent), "m".into(), None, None).await;
    assert!(matches!(o, Ok(PickOutcome::Picked { written: 1, .. })), "{o:?}");
    assert_eq!(read(), "x\n");
    full.land(&c).await.expect("land revert");
    let o = full.pick(&c, revert, edit, Some(parent), "m".into(), None, None).await;
    assert!(matches!(o, Ok(PickOutcome::Empty)), "{o:?}");
    let o = full.pick(&c, again, edit, Some(parent), "m".into(), None, None).await;
    assert!(matches!(o, Ok(PickOutcome::Picked { .. })), "{o:?}");
    assert_eq!(read(), "x2\n");
    full.land(&c).await.expect("land pick");

    // A commit that changed files a sparse checkout leaves out cannot be reverted there
    let mut sparse = WorkingCopy::clone(
        &c,
        &srv.url,
        "mono",
        "main",
        &sparse_dir,
        Sparse::dirs(&["a"]).expect("dirs"),
    )
    .await
    .expect("sparse clone");
    write(&full_dir, "b/y.txt", "y2\n");
    let outside = full.commit(&c, "b", None).await.expect("commit").expect("changes").id;
    full.land(&c).await.expect("land b");
    sparse.sync(&c).await.expect("sync");
    let parent = c.resolve_revision(&outside.to_string()).await.expect("rev").parents[0];
    match sparse
        .pick(&c, PickKind::Revert, outside, Some(parent), "m".into(), None, None)
        .await
    {
        Err(e @ Error::PickOutsideSparse(_)) => {
            assert_eq!(
                e.to_string(),
                "the commit changes files outside the sparse checkout: b/y.txt"
            )
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn several_pending_commits_stack_and_rebase_one_by_one() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "x.txt", "x\n");
    write(&a_dir, "y.txt", "y\n");
    a.commit(&c, "base", None).await.expect("commit");
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");
    let messages = |a: &WorkingCopy| {
        let c = c.clone();
        let pending = a.state().pending().expect("pending");
        async move {
            let mut out = Vec::new();
            for id in pending {
                let e = c
                    .log(&id.to_string(), None, 1, None)
                    .await
                    .expect("log")
                    .entries
                    .remove(0);
                out.push(e.message);
            }
            out
        }
    };

    // Two commits stack; uncommit and amend touch only the newest
    write(&a_dir, "x.txt", "x1\n");
    let one = a.commit(&c, "one", None).await.expect("commit").expect("changes").id;
    write(&a_dir, "y.txt", "y1\n");
    a.commit(&c, "two", None).await.expect("commit");
    assert_eq!(messages(&a).await, ["one", "two"]);
    a.uncommit(&c).await.expect("uncommit");
    assert_eq!(a.state().pending().expect("pending"), [one]);
    assert_eq!(a.status().expect("status").modified, ["y.txt"]);
    write(&a_dir, "y.txt", "y2\n");
    a.commit(&c, "two", None).await.expect("commit");
    write(&a_dir, "y.txt", "y3\n");
    a.amend(&c, Some("two, amended"), None).await.expect("amend");
    assert_eq!(messages(&a).await, ["one", "two, amended"]);

    // Upstream moves elsewhere: each commit is rebased and keeps its message
    write(&b_dir, "z.txt", "z\n");
    b.commit(&c, "z", None).await.expect("commit");
    b.land(&c).await.expect("land");
    assert!(matches!(a.sync(&c).await, Ok(SyncOutcome::Rebased { .. })));
    assert_eq!(messages(&a).await, ["one", "two, amended"]);
    assert_eq!(read(&a_dir, "z.txt"), "z\n");

    // Upstream changes the first commit's line: the sync stops there, and the commit that
    // finishes it goes on with the second
    b.sync(&c).await.expect("sync");
    write(&b_dir, "x.txt", "bob\n");
    b.commit(&c, "bob", None).await.expect("commit");
    b.land(&c).await.expect("land");
    let err = a.sync(&c).await.expect_err("conflict");
    assert!(
        matches!(&err, Error::Conflict(p) if p == &vec!["x.txt".to_string()]),
        "{err:?}"
    );
    assert!(a.state().pending().expect("pending").is_empty());
    assert_eq!(a.state().conflict.as_ref().expect("conflict").rest.len(), 1);
    assert_eq!(read(&a_dir, "y.txt"), "y\n", "the second commit waits");
    a.resolve(&c, &["x.txt".into()], Some(Side::Ours), false)
        .await
        .expect("resolve");
    a.commit(&c, "", None).await.expect("commit");
    assert_eq!(messages(&a).await, ["one", "two, amended"]);
    assert_eq!(
        (read(&a_dir, "x.txt"), read(&a_dir, "y.txt")),
        ("x1\n".into(), "y3\n".into())
    );
    assert!(a.state().conflict.is_none());

    // A commit whose change is upstream already is dropped
    b.sync(&c).await.expect("sync");
    write(&b_dir, "y.txt", "y3\n");
    b.commit(&c, "same", None).await.expect("commit");
    b.land(&c).await.expect("land");
    a.sync(&c).await.expect("sync");
    assert_eq!(messages(&a).await, ["one"]);

    // The stack lands at once
    write(&a_dir, "w.txt", "w\n");
    a.commit(&c, "three", None).await.expect("commit");
    let landed = a.land(&c).await.expect("land");
    assert_eq!(landed.landed.len(), 2);
    assert!(a.state().pending().expect("pending").is_empty());

    // Nothing goes on top of a pending merge
    a.switch(&c, "feat", true).await.expect("feat");
    write(&a_dir, "f.txt", "f\n");
    a.commit(&c, "f", None).await.expect("commit");
    a.land(&c).await.expect("land");
    a.switch(&c, "main", false).await.expect("main");
    write(&a_dir, "m.txt", "m\n");
    a.commit(&c, "m", None).await.expect("commit");
    a.land(&c).await.expect("land");
    a.merge(&c, "feat", None, None).await.expect("merge");
    write(&a_dir, "after.txt", "after\n");
    assert!(matches!(a.commit(&c, "on top", None).await, Err(Error::OnPendingMerge)));
    let o = a.pick(&c, PickKind::Revert, one, None, "r".into(), None, None).await;
    assert!(matches!(o, Err(Error::OnPendingMerge)), "{o:?}");
}

#[tokio::test]
async fn sync_abort_goes_back_to_before_the_sync() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "x.txt", "x\n");
    write(&a_dir, "y.txt", "y\n");
    a.commit(&c, "base", None).await.expect("commit");
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");
    assert!(matches!(a.abort_sync(&c).await, Err(Error::NoSyncInProgress)));

    write(&a_dir, "x.txt", "x1\n");
    write(&a_dir, "new.txt", "new\n");
    a.commit(&c, "one", None).await.expect("commit");
    write(&a_dir, "y.txt", "y1\n");
    a.commit(&c, "two", None).await.expect("commit");
    let before = a.state().clone();
    write(&b_dir, "x.txt", "bob x\n");
    write(&b_dir, "y.txt", "bob y\n");
    write(&b_dir, "z.txt", "z\n");
    b.commit(&c, "bob", None).await.expect("commit");
    b.land(&c).await.expect("land");

    // Stopped on the first commit, with its new file written: abort puts everything back
    let err = a.sync(&c).await.expect_err("conflict");
    assert!(
        matches!(&err, Error::Conflict(p) if p == &vec!["x.txt".to_string()]),
        "{err:?}"
    );
    assert_eq!(read(&a_dir, "new.txt"), "new\n");
    a.abort_sync(&c).await.expect("abort");
    assert_eq!(a.state(), &before);
    assert!(a.status().expect("status").is_clean());
    assert_eq!(
        (read(&a_dir, "x.txt"), read(&a_dir, "y.txt"), read(&a_dir, "new.txt")),
        ("x1\n".into(), "y1\n".into(), "new\n".into())
    );
    assert!(!a_dir.join("z.txt").exists());

    // Stopped again on the second commit after the first was finished: still back to the start
    a.sync(&c).await.expect_err("conflict");
    a.resolve(&c, &["x.txt".into()], Some(Side::Ours), false)
        .await
        .expect("resolve");
    let err = a.commit(&c, "", None).await.expect_err("second conflict");
    assert!(
        matches!(&err, Error::Conflict(p) if p == &vec!["y.txt".to_string()]),
        "{err:?}"
    );
    a.abort_sync(&c).await.expect("abort");
    assert_eq!(a.state(), &before);
    assert_eq!(read(&a_dir, "x.txt"), "x1\n");
    assert!(!a_dir.join("z.txt").exists());

    // A pending merge that conflicts with the new tip comes back too
    a.sync(&c).await.expect_err("conflict");
    a.resolve(&c, &["x.txt".into()], Some(Side::Theirs), false)
        .await
        .expect("resolve");
    a.commit(&c, "", None).await.expect_err("second conflict");
    a.resolve(&c, &["y.txt".into()], Some(Side::Theirs), false)
        .await
        .expect("resolve");
    a.commit(&c, "", None).await.expect("commit");
    a.land(&c).await.expect("land");
    a.switch(&c, "feat", true).await.expect("feat");
    write(&a_dir, "x.txt", "feat\n");
    a.commit(&c, "feat", None).await.expect("commit");
    a.land(&c).await.expect("land");
    a.switch(&c, "main", false).await.expect("main");
    a.merge(&c, "feat", None, None).await.expect("merge");
    let before = a.state().clone();
    assert!(before.head_merge.is_some());
    b.sync(&c).await.expect("sync");
    write(&b_dir, "x.txt", "bob again\n");
    b.commit(&c, "bob again", None).await.expect("commit");
    b.land(&c).await.expect("land");
    a.sync(&c).await.expect_err("conflict");
    a.abort_sync(&c).await.expect("abort");
    assert_eq!(a.state(), &before);
    assert_eq!(read(&a_dir, "x.txt"), "feat\n");

    // Only a sync's conflicts: a merge's are given up with merge --abort
    a.uncommit(&c).await.expect("uncommit");
    a.restore(&c, &[".".into()]).await.expect("restore");
    a.sync(&c).await.expect("sync");
    a.merge(&c, "feat", None, None).await.expect_err("conflict");
    assert!(matches!(a.abort_sync(&c).await, Err(Error::NoSyncInProgress)));
    a.abort_merge(&c).await.expect("merge --abort");
}

#[tokio::test]
async fn commit_paths_takes_only_their_changes() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    for f in ["a.txt", "d/b.txt", "d/c.txt", "e.txt", "x.txt"] {
        write(&dir, f, "1\n");
    }
    wc.commit(&c, "base", None).await.expect("commit");
    wc.land(&c).await.expect("land");

    write(&dir, "a.txt", "2\n");
    write(&dir, "d/b.txt", "2\n");
    write(&dir, "d/new.txt", "new\n");
    std::fs::remove_file(dir.join("e.txt")).expect("rm");
    let paths = |p: &[&str]| p.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    // A path without changes refuses the whole commit
    for (only, missing) in [(&["nope"][..], &["nope"][..]), (&["a.txt", "d/c.txt"], &["d/c.txt"])] {
        let e = wc.commit_paths_with_stat(&c, "m", None, &paths(only)).await;
        assert!(
            matches!(&e, Err(Error::NoChangesIn(p)) if *p == paths(missing)),
            "{e:?}"
        );
    }
    assert!(wc.state().head.is_none());

    // A directory takes the changes under it, a file its own; the rest stay local
    let (_, stat) = wc
        .commit_paths_with_stat(&c, "d", None, &paths(&["d/"]))
        .await
        .expect("commit")
        .expect("changes");
    let committed: Vec<&str> = stat.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(committed, ["d/b.txt", "d/new.txt"]);
    assert_eq!(wc.status().expect("status"), status(&[], &["a.txt"], &["e.txt"]));
    wc.commit_paths_with_stat(&c, "e", None, &paths(&["e.txt"]))
        .await
        .expect("commit")
        .expect("changes");
    assert_eq!(wc.status().expect("status"), status(&[], &["a.txt"], &[]));
    wc.land(&c).await.expect("land");
    assert_eq!(c.read_file("main", "d/new.txt").await.expect("cat").as_ref(), b"new\n");
    assert_eq!(c.read_file("main", "a.txt").await.expect("cat").as_ref(), b"1\n");
    assert_eq!(read(&dir, "a.txt"), "2\n");

    // The commit that finishes a revert takes every change
    wc.restore(&c, &[".".into()]).await.expect("restore");
    write(&dir, "x.txt", "2\n");
    let first = wc.commit(&c, "x2", None).await.expect("commit").expect("changes").id;
    write(&dir, "x.txt", "3\n");
    wc.commit(&c, "x3", None).await.expect("commit");
    wc.land(&c).await.expect("land");
    let o = wc.pick(&c, PickKind::Revert, first, None, "r".into(), None, None).await;
    assert!(matches!(o, Err(Error::Conflict(_))), "{o:?}");
    let e = wc.commit_paths_with_stat(&c, "", None, &paths(&["x.txt"])).await;
    assert!(matches!(e, Err(Error::PartialFinish)), "{e:?}");
    wc.abort_merge(&c).await.expect("abort");
}

#[tokio::test]
async fn clean_removes_untracked_files_like_git() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    write(&dir, ".gitignore", "target/\n*.log\n");
    write(&dir, "src/lib.rs", "lib");
    wc.commit(&c, "base", None).await.expect("commit");
    for f in [
        "notes.txt",
        "src/new.rs",
        "fresh/a/b.txt",
        "fresh/c.txt",
        "mixed/x.txt",
        "mixed/run.log",
        "app.log",
        "target/debug/out",
        "vendor/dep/.git/HEAD",
        "vendor/dep/x.rs",
    ] {
        write(&dir, f, "x");
    }
    std::fs::create_dir_all(dir.join("empty/inner")).expect("mkdir");
    let paths = |p: &[&str]| p.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    // A dry run lists and keeps everything; untracked directories that go whole end in /
    let listed = wc.clean(&paths(&["."]), false, true).expect("dry run");
    assert_eq!(
        listed,
        ["empty/", "fresh/", "mixed/x.txt", "notes.txt", "src/new.rs"],
        "ignored files, and a nested repository, stay"
    );
    assert!(dir.join("notes.txt").exists());

    // Paths limit it
    assert_eq!(
        wc.clean(&paths(&["fresh/a"]), false, false).expect("clean"),
        ["fresh/a/"]
    );
    assert!(!dir.join("fresh/a").exists() && dir.join("fresh/c.txt").exists());

    assert_eq!(
        wc.clean(&paths(&["."]), false, false).expect("clean"),
        ["empty/", "fresh/", "mixed/x.txt", "notes.txt", "src/new.rs"]
    );
    assert!(dir.join("mixed/run.log").exists() && dir.join("src/lib.rs").exists());
    assert_eq!(wc.status().expect("status"), status(&["vendor/dep/x.rs"], &[], &[]));

    // -x takes ignored files too, never the nested repository
    assert_eq!(
        wc.clean(&paths(&["."]), true, false).expect("clean -x"),
        ["app.log", "mixed/", "target/"]
    );
    assert!(dir.join("vendor/dep/.git/HEAD").exists() && dir.join(".scion").exists());
    assert!(dir.join(".gitignore").exists());
    assert!(wc.clean(&paths(&["."]), true, false).expect("again").is_empty());

    // A link to a directory goes, not what it points to
    #[cfg(unix)]
    {
        let outside = tmp.path().join("outside");
        write(&outside, "keep.txt", "keep");
        std::os::unix::fs::symlink(&outside, dir.join("link")).expect("symlink");
        assert_eq!(wc.clean(&paths(&["."]), false, false).expect("clean"), ["link"]);
        assert!(outside.join("keep.txt").exists() && !dir.join("link").exists());
    }
}

#[tokio::test]
async fn stash_puts_changes_aside_and_back() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "a.txt", "1\n2\n3\n");
    write(&a_dir, "b.txt", "b\n");
    let base = a.commit(&c, "base", None).await.expect("commit").expect("changes").id;
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");
    assert_eq!(a.stash_push(&c, None, None).await.expect("nothing"), None);

    // Every kind of change goes aside, and the tree is the commit's again
    write(&a_dir, "a.txt", "one\n2\n3\n");
    std::fs::remove_file(a_dir.join("b.txt")).expect("rm");
    write(&a_dir, "new.txt", "new\n");
    let first = a.stash_push(&c, None, None).await.expect("stash").expect("saved");
    assert_eq!(first.message, format!("WIP on main: {} base", &base.to_string()[..12]));
    assert!(a.status().expect("status").is_clean());
    assert_eq!(read(&a_dir, "b.txt"), "b\n");
    assert!(!a_dir.join("new.txt").exists());
    write(&a_dir, "b.txt", "two\n");
    a.stash_push(&c, Some("two"), None)
        .await
        .expect("stash")
        .expect("saved");
    let messages: Vec<&str> = a.state().stash.iter().map(|e| e.message.as_str()).collect();
    assert_eq!(messages, ["two", first.message.as_str()]);

    // Applying needs a clean tree; pop drops the entry
    write(&a_dir, "a.txt", "dirty\n");
    assert!(matches!(a.stash_apply(&c, 0, true).await, Err(Error::Dirty)));
    a.restore(&c, &[".".into()]).await.expect("restore");
    assert!(matches!(a.stash_apply(&c, 2, true).await, Err(Error::NoStash(2))));
    let popped = a.stash_apply(&c, 1, true).await.expect("pop");
    assert!(popped.dropped && popped.conflicts.is_empty());
    assert_eq!(popped.entry, first);
    assert_eq!(
        a.status().expect("status"),
        status(&["new.txt"], &["a.txt"], &["b.txt"])
    );
    a.restore(&c, &[".".into()]).await.expect("restore");
    a.clean(&[".".into()], false, false).expect("clean");

    // After others landed: merged against the commit it was made on
    write(&b_dir, "a.txt", "1\n2\nthree\n");
    b.commit(&c, "three", None).await.expect("commit");
    b.land(&c).await.expect("land");
    a.sync(&c).await.expect("sync");
    let applied = a.stash_apply(&c, 0, false).await.expect("apply");
    assert!(!applied.dropped && applied.conflicts.is_empty());
    assert_eq!(read(&a_dir, "b.txt"), "two\n");
    assert_eq!(a.state().stash.len(), 1, "apply keeps it");
    a.restore(&c, &[".".into()]).await.expect("restore");

    // A conflict keeps the entry and marks the file
    write(&a_dir, "a.txt", "1\nmine\nthree\n");
    a.stash_push(&c, None, None).await.expect("stash").expect("saved");
    b.sync(&c).await.expect("sync");
    write(&b_dir, "a.txt", "1\ntheirs\nthree\n");
    b.commit(&c, "theirs", None).await.expect("commit");
    b.land(&c).await.expect("land");
    a.sync(&c).await.expect("sync");
    let applied = a.stash_apply(&c, 0, true).await.expect("pop");
    assert_eq!(applied.conflicts, ["a.txt"]);
    assert!(!applied.dropped && read(&a_dir, "a.txt").contains("<<<<<<<"));
    assert_eq!(a.state().stash.len(), 2);

    assert!(matches!(a.stash_drop(2), Err(Error::NoStash(2))));
    assert_eq!(a.stash_drop(1).expect("drop").message, "two");
    assert_eq!(a.state().stash.len(), 1);
}

#[tokio::test]
async fn detach_looks_at_an_old_commit_on_no_branch() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("wc");
    let mut wc = WorkingCopy::clone(&c, &srv.url, "mono", "main", &dir, Sparse::full())
        .await
        .expect("clone");
    let mut ids = Vec::new();
    for (i, extra) in [None, Some("b.txt"), None].into_iter().enumerate() {
        write(&dir, "a.txt", &format!("{i}\n"));
        if let Some(f) = extra {
            write(&dir, f, "b\n");
        }
        ids.push(
            wc.commit(&c, &format!("c{i}"), None)
                .await
                .expect("commit")
                .expect("changes")
                .id,
        );
        wc.land(&c).await.expect("land");
    }

    // An added file that the old commit also has is not overwritten
    std::fs::remove_file(dir.join("b.txt")).expect("rm");
    assert!(matches!(wc.detach(&c, ids[1]).await, Err(Error::Dirty)));
    wc.restore(&c, &[".".into()]).await.expect("restore");
    let s = wc.detach(&c, ids[0]).await.expect("detach");
    assert_eq!(s.tip, Some(ids[0]));
    assert!(wc.state().detached && wc.state().reference == "main");
    assert_eq!(read(&dir, "a.txt"), "0\n");
    assert!(!dir.join("b.txt").exists());
    write(&dir, "b.txt", "mine\n");
    assert!(matches!(wc.detach(&c, ids[1]).await, Err(Error::WouldOverwrite(_))));
    std::fs::remove_file(dir.join("b.txt")).expect("rm");

    // Nothing that makes or publishes commits
    write(&dir, "a.txt", "edit\n");
    let e = wc.commit(&c, "no", None).await;
    assert!(
        matches!(&e, Err(Error::Detached(id)) if *id == ids[0].to_string()),
        "{e:?}"
    );
    assert!(
        e.expect_err("detached")
            .to_string()
            .starts_with("not on a branch (detached at c1")
    );
    wc.restore(&c, &[".".into()]).await.expect("restore");
    assert!(matches!(wc.land(&c).await, Err(Error::Detached(_))));
    assert!(matches!(wc.sync(&c).await, Err(Error::Detached(_))));
    assert!(matches!(
        wc.merge(&c, "main", None, None).await,
        Err(Error::Detached(_))
    ));
    let o = wc
        .pick(&c, PickKind::Revert, ids[1], None, "r".into(), None, None)
        .await;
    assert!(matches!(o, Err(Error::Detached(_))), "{o:?}");

    // Back to the branch, or a new branch from here
    wc.switch(&c, "main", false).await.expect("main");
    assert!(!wc.state().detached);
    assert_eq!(read(&dir, "a.txt"), "2\n");
    wc.detach(&c, ids[1]).await.expect("detach");
    wc.switch(&c, "fix", true).await.expect("new branch");
    assert!(!wc.state().detached && wc.state().reference == "fix");
    assert_eq!(c.resolve_ref("fix").await.expect("ref").expect("fix").changeset, ids[1]);
    write(&dir, "a.txt", "fix\n");
    wc.commit(&c, "fix", None).await.expect("commit").expect("changes");
}

#[cfg(unix)]
fn chmod_x(root: &Path, rel: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root.join(rel), std::fs::Permissions::from_mode(0o755)).expect("chmod");
}

#[cfg(unix)]
#[tokio::test]
async fn sync_rebases_pending_changes_of_every_kind() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    for (f, body) in [
        ("keep.txt", "k\n"),
        ("del.txt", "d\n"),
        ("run.sh", "a\n"),
        ("shared.txt", "s\n"),
        ("up.txt", "u\n"),
    ] {
        write(&a_dir, f, body);
    }
    a.commit(&c, "base", None).await.expect("commit");
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");
    // Upstream: edits shared.txt, makes run.sh executable, deletes up.txt
    write(&a_dir, "shared.txt", "s2\n");
    chmod_x(&a_dir, "run.sh");
    std::fs::remove_file(a_dir.join("up.txt")).expect("rm");
    a.commit(&c, "upstream", None).await.expect("commit");
    a.land(&c).await.expect("land");
    // Pending: adds new.txt, deletes del.txt, edits run.sh's content
    write(&b_dir, "new.txt", "n\n");
    std::fs::remove_file(b_dir.join("del.txt")).expect("rm");
    write(&b_dir, "run.sh", "b\n");
    b.commit(&c, "mine", None).await.expect("commit");
    assert!(matches!(b.sync(&c).await, Ok(SyncOutcome::Rebased { .. })));
    assert!(b.status().expect("status").is_clean());
    let landed = b.land(&c).await.expect("land").tip.to_string();
    for (f, want) in [
        ("shared.txt", "s2\n"),
        ("run.sh", "b\n"),
        ("new.txt", "n\n"),
        ("keep.txt", "k\n"),
    ] {
        assert_eq!(c.read_file(&landed, f).await.expect(f).as_ref(), want.as_bytes(), "{f}");
    }
    assert!(c.read_file(&landed, "del.txt").await.is_err() && c.read_file(&landed, "up.txt").await.is_err());
    let run = c.stat(&landed, "run.sh").await.expect("stat");
    assert_eq!(
        run.mode,
        scion_client::EntryMode::Executable,
        "mode from upstream, content from mine"
    );
    assert!(!b_dir.join("up.txt").exists() && !b_dir.join("del.txt").exists());

    // A conflict whose resolution leaves nothing to commit, while upstream also deletes a file
    a.sync(&c).await.expect("sync a");
    write(&a_dir, "keep.txt", "from a\n");
    std::fs::remove_file(a_dir.join("new.txt")).expect("rm");
    a.commit(&c, "a", None).await.expect("commit");
    a.land(&c).await.expect("land");
    write(&b_dir, "keep.txt", "from b\n");
    write(&b_dir, "new.txt", "n2\n");
    b.commit(&c, "b", None).await.expect("commit");
    assert!(matches!(b.sync(&c).await, Err(Error::Conflict(p)) if p == ["keep.txt", "new.txt"]));
    b.resolve(&c, &["keep.txt".into()], Some(scion_wc::Side::Theirs), false)
        .await
        .expect("theirs");
    // Resolved but not committed: other commands still wait
    write(&b_dir, "x.txt", "stray\n");
    std::fs::remove_file(b_dir.join("x.txt")).expect("rm");
    b.resolve(&c, &["new.txt".into()], Some(scion_wc::Side::Theirs), true)
        .await
        .expect("theirs: deleted");
    assert!(!b_dir.join("new.txt").exists());
    assert!(matches!(b.sync(&c).await, Err(Error::Dirty)));
    assert!(b.commit(&c, "", None).await.expect("commit").is_none(), "nothing left");
    assert!(b.state().conflict.is_none());
    assert!(matches!(b.sync(&c).await, Ok(SyncOutcome::UpToDate)));
}

/// A commit that holds the working copy's own metadata path cannot overwrite it on checkout.
#[tokio::test]
async fn checkout_refuses_paths_reserved_for_the_working_copy() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let created = c
        .changeset(None)
        .message("hostile")
        .author("Mallory", "mallory@example.com")
        .put("README.md", "hello\n")
        .put(".scion/state.json", "{}")
        .create()
        .await
        .expect("create");
    c.create_ref("main", &created.id).await.expect("ref");
    let tmp = tempfile::tempdir().expect("tempdir");
    let err = WorkingCopy::clone(&c, &srv.url, "mono", "main", &tmp.path().join("wc"), Sparse::full())
        .await
        .err()
        .expect("refused");
    assert!(matches!(err, Error::UnsafePath { .. }), "{err}");
    assert!(err.to_string().contains(".scion/state.json"), "{err}");
}

#[tokio::test]
async fn files_added_on_both_sides_conflict_with_markers() {
    let srv = server().await;
    let c = client(&srv.url).await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a_dir, b_dir) = (tmp.path().join("a"), tmp.path().join("b"));
    let mut a = WorkingCopy::clone(&c, &srv.url, "mono", "main", &a_dir, Sparse::full())
        .await
        .expect("clone");
    write(&a_dir, "seed.txt", "seed\n");
    a.commit(&c, "seed", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    let mut b = WorkingCopy::clone(&c, &srv.url, "mono", "main", &b_dir, Sparse::full())
        .await
        .expect("clone");

    // Both add the same new file with different text: both sides appear between markers, as in git
    write(&a_dir, "new.txt", "from alice\n");
    a.commit(&c, "a", None).await.expect("commit").expect("changes");
    a.land(&c).await.expect("land");
    write(&b_dir, "new.txt", "from bob\n");
    b.commit(&c, "b", None).await.expect("commit").expect("changes");
    let err = b.sync(&c).await.expect_err("conflict");
    assert!(
        matches!(&err, Error::Conflict(p) if p == &vec!["new.txt".to_string()]),
        "{err:?}"
    );
    let marked = std::fs::read_to_string(b_dir.join("new.txt")).expect("read");
    assert!(
        marked.contains("<<<<<<<") && marked.contains("from bob") && marked.contains("from alice"),
        "{marked}"
    );
    assert_eq!(
        b.state()
            .conflict
            .as_ref()
            .map(|c| c.paths.values().map(|s| s.kind.clone()).collect::<Vec<_>>()),
        Some(vec![ConflictKind::AddAdd.as_str().to_owned()])
    );
    assert!(matches!(
        b.resolve(&c, &["new.txt".into()], None, false).await,
        Err(Error::MarkersRemain(_))
    ));
    write(&b_dir, "new.txt", "from both\n");
    assert!(
        b.resolve(&c, &["new.txt".into()], None, false)
            .await
            .expect("resolve")
            .is_empty()
    );
    b.commit(&c, "", None).await.expect("commit").expect("changes");
    b.land(&c).await.expect("land");
    let tip = c.log("main", None, 1, None).await.expect("log").entries.remove(0);
    let content = c.read_file(&tip.id.to_string(), "new.txt").await.expect("read");
    assert_eq!(&content[..], b"from both\n");
}
