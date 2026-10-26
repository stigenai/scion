//! SDK ↔ 服务端端到端测试（M0 验收：能连上并完成能力协商）。

use std::sync::Arc;

use bytes::Bytes;
use scion_api::{App, Limits, ServerConfig, serve};
use scion_client::{Auth, Client, Code, Error, ObjectId, ProtocolVersion, Role};
use scion_proto::v1::ErrorReason;
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};
use tokio::net::TcpListener;

struct TestServer {
    url: String,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn start(limits: Limits, blobs: &[&'static [u8]]) -> TestServer {
    let store = Arc::new(MemStore::new());
    store.create_repo("mono").await.expect("repo");
    for b in blobs {
        let id = ObjectId::of_blob(b);
        store.put(id, Bytes::from_static(b)).await.expect("put");
        store
            .index_objects(
                "mono",
                &[IndexedObject {
                    id,
                    size: b.len() as u64,
                }],
            )
            .await
            .expect("index");
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let config = ServerConfig {
        limits,
        ..ServerConfig::default()
    };
    let app = App {
        config: Arc::new(config),
        meta: store.clone(),
        blobs: store,
    };
    tokio::spawn(serve(listener, app, async {
        let _ = rx.await;
    }));
    TestServer { url, _shutdown: tx }
}

#[tokio::test]
async fn connect_negotiates_capabilities() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .auth(Auth::Bearer("dev-token".into()))
        .client_name("e2e")
        .build()
        .await
        .expect("connect");
    let info = c.server_info();
    assert_eq!(info.protocol_version, ProtocolVersion::client());
    assert_eq!(info.role, Role::Server);
    assert!(c.has_feature(scion_proto::features::OBJECTS_GET));
    assert!(!c.has_feature("land.batch"));
    assert_eq!(info.limits.max_batch_objects, 1_000);
}

#[tokio::test]
async fn get_object_multi_chunk_verified() {
    static BIG: [u8; 10_000] = [42; 10_000];
    let srv = start(
        Limits {
            max_chunk_bytes: 1_024,
            ..Limits::default()
        },
        &[&BIG],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let got = c.get_object(&ObjectId::of_blob(&BIG)).await.expect("get");
    assert_eq!(got.as_ref(), &BIG[..]);
}

#[tokio::test]
async fn missing_objects_batches_by_server_limit() {
    let srv = start(
        Limits {
            max_batch_objects: 2,
            ..Limits::default()
        },
        &[b"a", b"c"],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let ids: Vec<_> = [b"a", b"b", b"c", b"d", b"e"]
        .iter()
        .map(|d| ObjectId::of_blob(*d))
        .collect();
    let missing = c.missing_objects(&ids).await.expect("has");
    assert_eq!(missing, vec![ids[1], ids[3], ids[4]]);
    assert_eq!(c.missing_objects(&[]).await.expect("empty"), vec![]);
}

#[tokio::test]
async fn errors_carry_reasons() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let err = c.get_object(&ObjectId::of_blob(b"absent")).await.expect_err("missing");
    assert_eq!(err.reason(), Some(ErrorReason::ObjectMissing));
    assert!(err.is_not_found());
    assert!(!err.is_retryable());

    let other = Client::builder()
        .endpoint(&srv.url)
        .repo("nope")
        .build()
        .await
        .expect("connect");
    let err = other
        .missing_objects(&[ObjectId::of_blob(b"")])
        .await
        .expect_err("no repo");
    assert_eq!(err.reason(), Some(ErrorReason::RepoNotFound));

    let norepo = Client::builder().endpoint(&srv.url).build().await.expect("connect");
    assert!(matches!(
        norepo.get_object(&ObjectId::of_blob(b"")).await,
        Err(Error::MissingRepo)
    ));
}

#[tokio::test]
async fn connect_refused_is_retryable_connect_error() {
    // A port bound and released has nobody listening, unless the server of a test running
    // alongside took it in between: then try another
    for _ in 0..5 {
        let addr = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind")
            .local_addr()
            .expect("addr");
        let built = Client::builder()
            .endpoint(format!("http://{addr}"))
            .connect_timeout(std::time::Duration::from_secs(2))
            .build()
            .await;
        if let Err(err) = built {
            assert!(matches!(err, Error::Connect { .. }), "{err:?}");
            assert!(err.is_retryable());
            return;
        }
    }
    panic!("every free port was taken by another test's server");
}

#[tokio::test]
async fn write_land_and_query_end_to_end() {
    // 小分块：强制上传和读取都走多块路径
    let srv = start(
        Limits {
            max_chunk_bytes: 4,
            ..Limits::default()
        },
        &[],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert!(c.has_feature(scion_proto::features::WRITE_CHANGESET));
    assert_eq!(c.resolve_ref("main").await.expect("resolve"), None);

    // 根提交：两个文件，其中一个可执行
    let root = c
        .changeset(None)
        .put("src/main.rs", "fn main() {}\n")
        .put_with_mode("bin/run.sh", scion_client::EntryMode::Executable, "#!/bin/sh\n")
        .message("init")
        .author("Ada", "ada@example.com")
        .create()
        .await
        .expect("create root");
    assert_eq!(root.generation, 1);
    let landed = c.land("main", &root.id).await.expect("land root");
    assert_eq!((landed.tip, landed.version), (root.id, 1));
    assert_eq!(landed.landed.len(), 1);
    assert!(!landed.rebased());
    // 重试安全：已在历史中，返回当前状态，不再合入
    let retry = c.land("main", &root.id).await.expect("retry");
    assert_eq!((retry.tip, retry.version, retry.landed.len()), (root.id, 1, 0));

    let tip = c.resolve_ref("main").await.expect("resolve").expect("exists");
    assert_eq!((tip.changeset, tip.root_tree), (root.id, root.root_tree));
    assert_eq!(
        c.read_file("main", "src/main.rs").await.expect("read").as_ref(),
        b"fn main() {}\n"
    );
    let top: Vec<_> = c
        .list_dir("main", "")
        .await
        .expect("ls")
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(top, ["bin", "src"]);
    let bin = c.list_dir("main", "bin").await.expect("ls bin");
    assert_eq!(bin[0].mode, scion_client::EntryMode::Executable);

    // 子提交：修改 + 删除；同样的内容已上传，不会重复上传
    let child = c
        .changeset(Some(root.id))
        .put("src/main.rs", "fn main() { println!(\"hi\"); }\n")
        .delete("bin/run.sh")
        .message("edit")
        .create()
        .await
        .expect("create child");
    assert_eq!(child.generation, 2);
    c.land("main", &child.id).await.expect("land child");
    assert_eq!(
        c.list_dir("main", "").await.expect("ls").len(),
        1,
        "bin/ removed when emptied"
    );
    // 历史版本仍可按提交 id 读取
    assert_eq!(
        c.read_file(&root.id.to_string(), "bin/run.sh")
            .await
            .expect("old")
            .as_ref(),
        b"#!/bin/sh\n"
    );
    assert_eq!(
        c.root_tree(&root.id.to_string()).await.expect("root tree"),
        root.root_tree
    );
    assert_eq!(c.root_tree("main").await.expect("root tree"), child.root_tree);
    let refs = c.list_refs().await.expect("list refs");
    assert_eq!(refs, vec![("main".to_string(), child.id, 2)]);

    // 基于旧提交的改动：只允许快进时被拒，默认由服务端 rebase
    let stale = c.changeset(Some(root.id)).put("x", "x").create().await.expect("stale");
    let err = c
        .land_fast_forward("main", &stale.id)
        .await
        .expect_err("not fast forward");
    assert_eq!(err.reason(), Some(ErrorReason::NotFastForward));
    let rebased = c.land("main", &stale.id).await.expect("rebased");
    assert!(rebased.rebased());
    assert_eq!(rebased.landed.len(), 1);
    assert_eq!(rebased.landed[0].changeset, stale.id);
    assert_eq!(rebased.landed[0].landed, rebased.tip);
    assert_eq!(c.read_file("main", "x").await.expect("x").as_ref(), b"x");
    // 指定 request id：重复调用返回同一结果，可以按 id 查询；同一 change 换 id 再合入不会重复
    let tracked = c
        .changeset(Some(rebased.tip))
        .put("t", "t")
        .create()
        .await
        .expect("tracked");
    assert_eq!(c.land_status("req-tracked").await.expect("status"), None);
    let first = c
        .land_with_id("main", &tracked.id, "req-tracked", false)
        .await
        .expect("land");
    let again = c
        .land_with_id("main", &tracked.id, "req-tracked", false)
        .await
        .expect("replay");
    assert_eq!(again, first);
    assert_eq!(c.land_status("req-tracked").await.expect("status"), Some(first.clone()));
    let err = c
        .land_with_id("main", &stale.id, "req-tracked", false)
        .await
        .expect_err("id reused");
    assert_eq!(err.reason(), None);
    assert!(
        matches!(
            err,
            Error::Rpc {
                code: Code::InvalidArgument,
                ..
            }
        ),
        "{err:?}"
    );
    let dup = c.land("main", &stale.id).await.expect("change already landed");
    assert_eq!((dup.tip, dup.landed.len()), (first.tip, 0));
    // stale 本身不在历史中（进入历史的是 rebase 后的提交），按 change id 认出它已合入
    assert_eq!(dup.previously_landed.len(), 1);
    assert_eq!(
        (dup.previously_landed[0].changeset, dup.previously_landed[0].landed),
        (stale.id, rebased.tip)
    );
    // 与 ref 上的改动冲突：返回冲突详情
    let clash = c.changeset(Some(root.id)).put("x", "y").create().await.expect("clash");
    let err = c.land("main", &clash.id).await.expect_err("conflict");
    let conflict = err.merge_conflict().expect("details");
    assert_eq!(
        (conflict.changeset, conflict.paths[0].path.as_str()),
        (Some(clash.id), "x")
    );
    assert_eq!(conflict.paths[0].kind, "add_add");

    // 错误映射
    let err = c.read_file("main", "nope").await.expect_err("missing path");
    assert_eq!(err.reason(), Some(ErrorReason::PathNotFound));
    let err = c
        .changeset(Some(root.id))
        .delete("nope")
        .create()
        .await
        .expect_err("bad delta");
    assert_eq!(err.reason(), Some(ErrorReason::InvalidChangeset));
}

#[tokio::test]
async fn explicit_signatures_and_change_id_make_ids_deterministic() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let sig = |name: &str, t| scion_client::Signature {
        name: name.into(),
        email: format!("{name}@example.com"),
        time_ms: t,
        tz_offset_minutes: 480,
    };
    let make = || {
        c.changeset(None)
            .put("a", "a")
            .message("imported")
            .author_signature(sig("ada", 1_000))
            .committer_signature(sig("bot", 2_000))
            .change_id([9; 16])
            .extension("git.sha1", vec![0xab; 20])
    };
    let first = make().create().await.expect("first");
    let second = make().create().await.expect("second");
    assert_eq!(first.id, second.id, "same input gives the same changeset id");
    let other = c
        .changeset(None)
        .put("a", "a")
        .message("imported")
        .create()
        .await
        .expect("random ids");
    assert_ne!(other.id, first.id);
}

#[tokio::test]
async fn upload_above_server_limit_rejected_client_side() {
    let srv = start(
        Limits {
            max_upload_object_bytes: 8,
            ..Limits::default()
        },
        &[],
    )
    .await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    let err = c
        .upload_blobs(&[Bytes::from_static(b"far too large")])
        .await
        .expect_err("too big");
    assert!(matches!(err, Error::Protocol(_)));
    // 已存在的内容不会再上传
    let ids = c.upload_blobs(&[Bytes::from_static(b"ok")]).await.expect("small");
    assert_eq!(c.upload_blobs(&[Bytes::from_static(b"ok")]).await.expect("again"), ids);
}

#[tokio::test]
async fn history_queries_end_to_end() {
    let srv = start(Limits::default(), &[]).await;
    let c = Client::builder()
        .endpoint(&srv.url)
        .repo("mono")
        .build()
        .await
        .expect("connect");
    assert!(c.has_feature(scion_proto::features::QUERY_LOG));
    let a = c
        .changeset(None)
        .put("README", "1")
        .message("a")
        .create()
        .await
        .expect("a");
    let b = c
        .changeset(Some(a.id))
        .put("src/x.rs", "2")
        .message("b")
        .create()
        .await
        .expect("b");
    let d = c
        .changeset(Some(a.id))
        .put("other", "3")
        .message("d")
        .create()
        .await
        .expect("d");
    // 合并提交的 delta 相对第一个父提交：从 d 合入的 other 也要写进 delta
    let m = c
        .changeset(Some(b.id))
        .parent(d.id)
        .put("README", "4")
        .put("other", "3")
        .message("merge")
        .create()
        .await
        .expect("m");
    for x in [a.id, b.id, m.id] {
        c.land("main", &x).await.expect("land");
    }

    // 分页遍历全部历史
    let mut token: Option<String> = None;
    let mut seen = Vec::new();
    loop {
        let page = c.log("main", None, 2, token.as_deref()).await.expect("log");
        seen.extend(page.entries.iter().map(|e| e.id));
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(seen.len(), 4);
    assert_eq!(seen[0], m.id);
    // 大量文件的 diff 分多批流式返回
    let mut big = c.changeset(Some(m.id));
    for i in 0..1203 {
        big = big.put(format!("many/f{i:04}"), format!("{i}"));
    }
    let big = big.create().await.expect("many files");
    let changes = c
        .diff(Some(&m.id.to_string()), &big.id.to_string(), None)
        .await
        .expect("big diff");
    assert_eq!(changes.len(), 1203);
    assert!(
        changes.windows(2).all(|w| w[0].path < w[1].path),
        "batches arrive in path order"
    );
    let first = c.log("main", None, 1, None).await.expect("log");
    assert_eq!(first.entries[0].parents, vec![b.id, d.id]);
    assert_eq!(first.entries[0].message, "merge");
    // 按路径过滤
    let readme = c.log("main", Some("README"), 10, None).await.expect("log");
    assert_eq!(
        readme.entries.iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![m.id, a.id]
    );
    let other = c.log("main", Some("other"), 10, None).await.expect("log");
    assert_eq!(
        other.entries.iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![d.id],
        "d reached via the second parent; the merge took the file from it, so git leaves it out"
    );

    assert!(c.is_ancestor(&d.id.to_string(), "main").await.expect("ok"));
    assert!(!c.is_ancestor("main", &a.id.to_string()).await.expect("ok"));
    assert_eq!(
        c.merge_bases(&b.id.to_string(), &d.id.to_string()).await.expect("ok"),
        vec![a.id]
    );

    let changes = c.diff(Some(&a.id.to_string()), "main", None).await.expect("diff");
    let paths: Vec<_> = changes.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, ["README", "other", "src/x.rs"]);
    assert!(changes[1].old.is_none() && changes[1].new.is_some());
    assert_eq!(c.diff(None, "main", Some("src")).await.expect("diff").len(), 1);
    let blame = c.blame("main", "README").await.expect("blame");
    assert_eq!(blame.lines.len(), 1);
    assert_eq!(blame.commits[blame.lines[0]].id, m.id);
    assert_eq!(blame.commits[0].summary, "merge");
    let err = c.blame("main", "nope").await.expect_err("missing path");
    assert_eq!(err.reason(), Some(ErrorReason::PathNotFound));
    let rev = c.resolve_revision("main").await.expect("resolve");
    assert_eq!((rev.id, rev.parents.clone()), (m.id, vec![b.id, d.id]));
    assert_eq!(
        c.stat("main", "").await.expect("root").mode,
        scion_client::EntryMode::Tree
    );
    assert_eq!(c.stat("main", "src/x.rs").await.expect("file").size, 1);
    assert_eq!(
        c.stat("main", "nope").await.expect_err("missing").reason(),
        Some(ErrorReason::PathNotFound)
    );
    // 合并提交也有 git sha，并且可以当作版本号
    let sha = c.git_sha("main").await.expect("sha").expect("computed");
    assert_eq!(c.git_shas(&[m.id]).await.expect("batch"), vec![sha.clone()]);
    assert_eq!(c.resolve_revision(&sha).await.expect("by sha").id, m.id);
    // 每个条目的最后提交：合并带进来的 other 归到合并提交
    assert!(c.has_feature(scion_proto::features::QUERY_LAST_COMMITS));
    let last = c.last_commits("main", "", 0, 0).await.expect("last commits");
    let want = vec![
        ("README".to_string(), Some(m.id)),
        ("other".to_string(), Some(m.id)),
        ("src".to_string(), Some(b.id)),
    ];
    assert_eq!((last.total, last.entries.clone()), (3, want.clone()));
    assert_eq!(last.commits.iter().map(|e| e.id).collect::<Vec<_>>(), vec![m.id, b.id]);
    assert_eq!(last.commits[1].message, "b");
    assert_eq!(c.last_commits("main", "", 0, 0).await.expect("cached").entries, want);
    let page = c.last_commits("main", "", 1, 1).await.expect("page");
    assert_eq!((page.total, page.entries), (3, vec![("other".to_string(), Some(m.id))]));
    assert!(
        c.last_commits("main", "", 9, 0)
            .await
            .expect("past end")
            .entries
            .is_empty()
    );
    assert!(matches!(
        c.last_commits("main", "README", 0, 0).await.expect_err("file"),
        scion_client::Error::Rpc {
            code: scion_client::Code::FailedPrecondition,
            ..
        }
    ));
    // 目录统计
    let st = c.tree_stats("main", "").await.expect("stats");
    assert_eq!((st.files, st.directories, st.bytes, st.changeset), (3, 1, 3, m.id));
    assert_eq!(c.tree_stats(&b.id.to_string(), "src").await.expect("src").files, 1);
    assert_eq!(
        c.tree_stats("main", "nope").await.expect_err("missing").reason(),
        Some(ErrorReason::PathNotFound)
    );
    assert_eq!(c.generation("main").await.expect("gen"), (3, 3));
    let err = c.diff(Some("nope"), "main", None).await.expect_err("bad revision");
    assert_eq!(err.reason(), Some(ErrorReason::RefNotFound));
}
