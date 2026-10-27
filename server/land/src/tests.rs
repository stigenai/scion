use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use scion_merge::ConflictKind;
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, ObjectKind, REBASED_FROM_EXTENSION, Signature};
use scion_query::{GIT_SHA1_EXTENSION, load_changeset};
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};
use scion_tree::{BlobTreeSource, lookup};

use super::*;

const REPO: &str = "r";

fn req<'a>(repo: &'a str, reference: &'a str, tip: ObjectId, fast_forward_only: bool) -> LandRequest<'a> {
    LandRequest {
        repo,
        reference,
        tip,
        fast_forward_only,
        request_id: None,
        actor: "",
    }
}

struct Fx {
    store: Arc<MemStore>,
    w: Arc<ChangesetWriter>,
    n: AtomicU8,
}

fn sig() -> Signature {
    Signature {
        name: "Ada".into(),
        email: "ada@example.com".into(),
        time_ms: 1,
        tz_offset_minutes: 0,
    }
}

impl Fx {
    async fn new() -> Self {
        let store = Arc::new(MemStore::new());
        store.create_repo(REPO).await.expect("repo");
        Self {
            w: Arc::new(ChangesetWriter::new(store.clone(), store.clone())),
            store,
            n: AtomicU8::new(0),
        }
    }

    async fn upload(&self, data: &[u8]) -> ObjectId {
        let id = ObjectId::of_blob(data);
        self.store.put(id, Bytes::copy_from_slice(data)).await.expect("blob");
        self.store
            .index_objects(
                REPO,
                &[IndexedObject {
                    id,
                    size: data.len() as u64,
                }],
            )
            .await
            .expect("index");
        id
    }

    async fn changeset(&self, parents: &[ObjectId], files: &[(&str, Option<&str>)]) -> Changeset {
        self.changeset_ext(parents, files, BTreeMap::new()).await
    }

    async fn changeset_ext(
        &self,
        parents: &[ObjectId],
        files: &[(&str, Option<&str>)],
        ext: BTreeMap<String, Vec<u8>>,
    ) -> Changeset {
        let mut delta = Vec::new();
        for (path, content) in files {
            delta.push(match content {
                Some(c) => {
                    let id = self.upload(c.as_bytes()).await;
                    FileChange::put(*path, EntryMode::File, id, c.len() as u64)
                }
                None => FileChange::delete(*path),
            });
        }
        let n = self.n.fetch_add(1, Ordering::Relaxed);
        Changeset::new(parents.to_vec(), sig(), sig(), format!("c{n}"), [n; 16], delta, ext).expect("cs")
    }

    async fn commit(&self, parents: &[ObjectId], files: &[(&str, Option<&str>)]) -> ObjectId {
        let cs = self.changeset(parents, files).await;
        self.w.create(REPO, &cs).await.expect("create").id
    }

    async fn read(&self, cs: ObjectId, path: &str) -> Option<String> {
        let root = self.store.get_changeset(REPO, &cs).await.expect("get")?.root_tree;
        let e = lookup(&BlobTreeSource(self.store.clone()), root, path)
            .await
            .expect("lookup")?;
        let data = self.store.get(&e.id).await.expect("blob")?;
        Some(String::from_utf8(data.to_vec()).expect("utf8"))
    }

    async fn prepare(&self, tip: ObjectId, onto: Option<ObjectId>) -> Result<Prepared> {
        prepare(&self.w, REPO, "main", tip, onto, StackOptions::default()).await
    }

    fn lander(&self) -> Lander {
        Lander::new(self.w.clone())
    }

    /// 从 tip 沿第一父提交往回的全部提交（新到旧）。
    async fn chain(&self, tip: ObjectId) -> Vec<ObjectId> {
        let mut out = Vec::new();
        let mut cur = Some(tip);
        while let Some(id) = cur {
            out.push(id);
            cur = self
                .store
                .get_changeset(REPO, &id)
                .await
                .expect("get")
                .expect("rec")
                .first_parent();
        }
        out
    }
}

// ---- 创建提交 ----

#[tokio::test]
async fn create_validates_parents_objects_and_delta() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let rec = fx.store.get_changeset(REPO, &a).await.expect("get").expect("rec");
    assert_eq!((rec.generation, rec.fp_depth), (1, 1));
    assert_eq!(fx.read(a, "f").await.as_deref(), Some("1"));

    let ghost = ObjectId::hash(ObjectKind::Changeset, b"ghost");
    let cs = fx.changeset(&[ghost], &[]).await;
    assert_eq!(
        fx.w.create(REPO, &cs).await,
        Err(LandError::ObjectsNotUploaded(vec![ghost]))
    );

    let missing = ObjectId::of_blob(b"never uploaded");
    let cs = Changeset::new(
        vec![a],
        sig(),
        sig(),
        "m",
        [9; 16],
        vec![FileChange::put("g", EntryMode::File, missing, 14)],
        BTreeMap::new(),
    )
    .expect("cs");
    assert_eq!(
        fx.w.create(REPO, &cs).await,
        Err(LandError::ObjectsNotUploaded(vec![missing]))
    );

    let id = fx.upload(b"abc").await;
    let cs = Changeset::new(
        vec![a],
        sig(),
        sig(),
        "m",
        [9; 16],
        vec![FileChange::put("g", EntryMode::File, id, 99)],
        BTreeMap::new(),
    )
    .expect("cs");
    assert!(matches!(fx.w.create(REPO, &cs).await, Err(LandError::InvalidChangeset { path, .. }) if path == "g"));

    let cs = fx.changeset(&[a], &[("nope", None)]).await;
    assert!(matches!(fx.w.create(REPO, &cs).await, Err(LandError::InvalidChangeset { path, .. }) if path == "nope"));

    let cs = fx.changeset(&[], &[]).await;
    assert_eq!(
        fx.w.create("missing", &cs).await,
        Err(LandError::RepoNotFound("missing".into()))
    );
}

// ---- prepare ----

#[tokio::test]
async fn prepare_new_ref_and_already_included() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("f", Some("2"))]).await;
    let p = fx.prepare(b, None).await.expect("new ref");
    assert_eq!(p.tip, b);
    // 新建 ref：整段第一父历史都记录下来（供之后按 change id 去重）
    assert_eq!(p.landed.iter().map(|l| l.landed).collect::<Vec<_>>(), [a, b]);
    assert!(p.landed.iter().all(|l| l.original == l.landed));
    let shallow = StackOptions {
        max_stack: 1,
        ..StackOptions::default()
    };
    assert_eq!(
        prepare(&fx.w, REPO, "main", b, None, shallow).await,
        Err(LandError::StackTooDeep(1))
    );
    // tip 已经在历史中
    for tip in [a, b] {
        let p = fx.prepare(tip, Some(b)).await.expect("included");
        assert_eq!((p.tip, p.landed.len()), (b, 0));
    }
}

#[tokio::test]
async fn prepare_fast_forward_records_the_stack() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("f", Some("2"))]).await;
    let c = fx.commit(&[b], &[("g", Some("3"))]).await;
    let p = fx.prepare(c, Some(a)).await.expect("ff");
    assert_eq!(p.tip, c);
    assert_eq!(p.landed.iter().map(|l| l.landed).collect::<Vec<_>>(), [b, c]);
    assert!(p.landed.iter().all(|l| l.original == l.landed));
    // 超过 max_stack 时拒绝，而不是只记录最近的一部分
    let opts = StackOptions {
        max_stack: 2,
        ..StackOptions::default()
    };
    assert_eq!(
        prepare(&fx.w, REPO, "main", c, Some(a), opts).await.map(|p| p.tip),
        Ok(c)
    );
    let opts = StackOptions {
        max_stack: 1,
        ..StackOptions::default()
    };
    assert_eq!(
        prepare(&fx.w, REPO, "main", c, Some(a), opts).await,
        Err(LandError::StackTooDeep(1))
    );
    // 合并提交（第一父是 ref 当前提交）可以快进
    let side = fx.commit(&[a], &[("side", Some("s"))]).await;
    let merge = fx.commit(&[c, side], &[("side", Some("s"))]).await;
    let p = fx.prepare(merge, Some(c)).await.expect("merge ff");
    assert_eq!(p.tip, merge);
}

#[tokio::test]
async fn prepare_rebases_disjoint_stack() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[("a", Some("1")), ("b", Some("1"))]).await;
    let head = fx.commit(&[base], &[("b", Some("2"))]).await;
    let mut ext = BTreeMap::new();
    ext.insert(GIT_SHA1_EXTENSION.to_string(), vec![7; 20]);
    ext.insert("other".to_string(), b"kept".to_vec());
    let cs1 = fx.changeset_ext(&[base], &[("a", Some("2"))], ext).await;
    let s1 = fx.w.create(REPO, &cs1).await.expect("s1").id;
    let s2 = fx.commit(&[s1], &[("a", Some("3")), ("c", Some("new"))]).await;

    let p = fx.prepare(s2, Some(head)).await.expect("rebase");
    assert_eq!(p.landed.len(), 2);
    assert_eq!(p.landed.iter().map(|l| l.original).collect::<Vec<_>>(), [s1, s2]);
    assert!(p.landed.iter().all(|l| l.original != l.landed));
    assert_eq!(p.tip, p.landed[1].landed);
    assert_eq!(fx.chain(p.tip).await[1..], [p.landed[0].landed, head, base]);
    assert_eq!(fx.read(p.tip, "a").await.as_deref(), Some("3"));
    assert_eq!(fx.read(p.tip, "b").await.as_deref(), Some("2"));
    assert_eq!(fx.read(p.tip, "c").await.as_deref(), Some("new"));
    // 保留作者、说明与 change id；去掉 git sha，保留其它扩展
    let r1 = load_changeset(fx.store.as_ref(), p.landed[0].landed)
        .await
        .expect("load");
    assert_eq!(
        (r1.message.as_str(), r1.change_id, r1.author.clone()),
        ("c2", cs1.change_id, sig())
    );
    assert_eq!(p.landed[0].change_id, cs1.change_id);
    assert!(!r1.extensions.contains_key(GIT_SHA1_EXTENSION));
    // Each rebased commit names the commit it was made as
    let r2 = load_changeset(fx.store.as_ref(), p.landed[1].landed)
        .await
        .expect("load");
    assert_eq!(
        r1.extensions.get(REBASED_FROM_EXTENSION).map(Vec::as_slice),
        Some(&s1.to_bytes()[..])
    );
    assert_eq!(
        r2.extensions.get(REBASED_FROM_EXTENSION).map(Vec::as_slice),
        Some(&s2.to_bytes()[..])
    );
    assert_eq!(r1.extensions.get("other").map(Vec::as_slice), Some(&b"kept"[..]));
    // 相同输入得到相同结果（rebase 是确定的）
    assert_eq!(fx.prepare(s2, Some(head)).await.expect("again").tip, p.tip);
}

#[tokio::test]
async fn prepare_merges_text_and_reports_conflicts() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[("f", Some("1\n2\n3\n4\n"))]).await;
    let head = fx.commit(&[base], &[("f", Some("one\n2\n3\n4\n"))]).await;
    let ours = fx.commit(&[base], &[("f", Some("1\n2\n3\nfour\n"))]).await;
    let p = fx.prepare(ours, Some(head)).await.expect("merge");
    assert_eq!(fx.read(p.tip, "f").await.as_deref(), Some("one\n2\n3\nfour\n"));
    // 合并产生的 blob 已写入并登记，可以被引用
    let rec = fx.store.get_changeset(REPO, &p.tip).await.expect("get").expect("rec");
    let e = lookup(&BlobTreeSource(fx.store.clone()), rec.root_tree, "f")
        .await
        .expect("lookup")
        .expect("entry");
    assert!(
        fx.store
            .object_sizes(REPO, &[e.id])
            .await
            .expect("sizes")
            .contains_key(&e.id)
    );

    let clash = fx.commit(&[base], &[("f", Some("uno\n2\n3\n4\n"))]).await;
    match fx.prepare(clash, Some(head)).await {
        Err(LandError::Conflict { changeset, conflicts }) => {
            assert_eq!(changeset, clash);
            assert_eq!(
                (conflicts[0].path.as_str(), conflicts[0].kind),
                ("f", ConflictKind::Content)
            );
        }
        other => panic!("expected conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn prepare_rejections() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[("f", Some("1"))]).await;
    let head = fx.commit(&[base], &[("g", Some("1"))]).await;
    let s1 = fx.commit(&[base], &[("h", Some("1"))]).await;
    let s2 = fx.commit(&[s1], &[("i", Some("1"))]).await;

    let ff_only = StackOptions {
        fast_forward_only: true,
        ..StackOptions::default()
    };
    assert_eq!(
        prepare(&fx.w, REPO, "main", s1, Some(head), ff_only).await,
        Err(LandError::NotFastForward { tip: head })
    );
    let shallow = StackOptions {
        max_stack: 1,
        ..StackOptions::default()
    };
    assert_eq!(
        prepare(&fx.w, REPO, "main", s2, Some(head), shallow).await,
        Err(LandError::StackTooDeep(1))
    );
    let other_root = fx.commit(&[], &[("x", Some("x"))]).await;
    assert_eq!(
        fx.prepare(other_root, Some(head)).await,
        Err(LandError::UnrelatedHistory { tip: other_root })
    );
    let merge = fx.commit(&[s1, head], &[]).await;
    assert_eq!(fx.prepare(merge, Some(s2)).await, Err(LandError::MergeInStack(merge)));
    let ghost = ObjectId::hash(ObjectKind::Changeset, b"ghost");
    assert_eq!(
        fx.prepare(ghost, Some(head)).await,
        Err(LandError::ObjectsNotUploaded(vec![ghost]))
    );
}

// ---- Lander ----

#[tokio::test]
async fn land_creates_fast_forwards_rebases_and_is_idempotent() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("f", Some("2"))]).await;
    let side = fx.commit(&[a], &[("g", Some("1"))]).await;

    let o = l.land(req(REPO, "main", a, false)).await.expect("create");
    assert_eq!((o.state.target, o.state.version, o.landed.len()), (a, 1, 1));
    let o = l.land(req(REPO, "main", b, false)).await.expect("ff");
    assert_eq!((o.state.target, o.state.version), (b, 2));
    // 重复请求：不再写入
    let again = l.land(req(REPO, "main", b, false)).await.expect("retry");
    assert_eq!((again.state, again.landed.len()), (o.state, 0));
    // 落后的提交：服务端 rebase
    let o = l.land(req(REPO, "main", side, false)).await.expect("rebase");
    assert_eq!(o.state.version, 3);
    assert_ne!(o.state.target, side);
    assert_eq!(fx.read(o.state.target, "f").await.as_deref(), Some("2"));
    assert_eq!(fx.read(o.state.target, "g").await.as_deref(), Some("1"));
    assert_eq!(
        l.land(req(REPO, "main", fx.commit(&[a], &[]).await, true)).await,
        Err(LandError::NotFastForward { tip: o.state.target })
    );
}

/// After a rebase the committer time is no earlier than the new parent's, so commit times
/// along the branch follow the landing order; the author's time stays.
#[tokio::test]
async fn rebased_commits_follow_the_parent_in_time() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let at = |time_ms| Signature { time_ms, ..sig() };
    let commit = |parent: ObjectId, path: &'static str, time_ms: i64, n: u8| {
        let fx = &fx;
        async move {
            let id = fx.upload(path.as_bytes()).await;
            let delta = vec![FileChange::put(path, EntryMode::File, id, path.len() as u64)];
            let cs = Changeset::new(
                vec![parent],
                at(time_ms),
                at(time_ms),
                "m".to_string(),
                [200 + n; 16],
                delta,
                BTreeMap::new(),
            )
            .expect("cs");
            fx.w.create(REPO, &cs).await.expect("create").id
        }
    };
    let base = fx.commit(&[], &[("a", Some("1"))]).await;
    l.land(req(REPO, "main", base, false)).await.expect("base");
    let later = commit(base, "later", 5_000, 0).await;
    l.land(req(REPO, "main", later, false)).await.expect("later");
    // Written before `later` but landed after it, as a stack of two
    let first = commit(base, "x", 2_000, 1).await;
    let second = commit(first, "y", 3_000, 2).await;
    let o = l.land(req(REPO, "main", second, false)).await.expect("rebase");
    let store = fx.store.clone();
    let load = |id: ObjectId| {
        let store = store.clone();
        async move {
            let data = store.get(&id).await.expect("get").expect("changeset");
            Changeset::decode(&data).expect("decode")
        }
    };
    let (x, y) = (load(o.landed[0].landed).await, load(o.landed[1].landed).await);
    assert_eq!((x.author.time_ms, y.author.time_ms), (2_000, 3_000));
    assert_eq!((x.committer.time_ms, y.committer.time_ms), (5_000, 5_000));
    // A commit written after its new parent keeps its own time
    let newest = commit(base, "z", 9_000, 3).await;
    let o = l.land(req(REPO, "main", newest, false)).await.expect("rebase");
    assert_eq!(load(o.state.target).await.committer.time_ms, 9_000);
}

#[tokio::test]
async fn request_id_replays_first_result_and_rejects_reuse() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("g", Some("1"))]).await;
    l.land(req(REPO, "main", a, false)).await.expect("a");
    let with_id = |tip, id| LandRequest {
        request_id: Some(id),
        actor: "",
        ..req(REPO, "main", tip, false)
    };
    let first = l.land(with_id(b, "req-1")).await.expect("b");
    assert_eq!((first.state.version, first.landed.len()), (2, 1));
    // ref 之后又前进了：重放仍返回第一次的结果（而不是"已在历史中"的当前状态）
    let c = fx.commit(&[b], &[("h", Some("1"))]).await;
    l.land(req(REPO, "main", c, false)).await.expect("c");
    assert_eq!(l.land(with_id(b, "req-1")).await, Ok(first.clone()));
    assert_eq!(l.status(REPO, "req-1").await, Ok(Some(first)));
    // 同一个 id 用于不同的请求
    assert_eq!(
        l.land(with_id(c, "req-1")).await,
        Err(LandError::RequestIdReused("req-1".into()))
    );
    let ff_only = LandRequest {
        fast_forward_only: true,
        ..with_id(b, "req-1")
    };
    assert_eq!(l.land(ff_only).await, Err(LandError::RequestIdReused("req-1".into())));
    // 没有记录：未收到、失败或无需写入的请求
    assert_eq!(l.status(REPO, "never").await, Ok(None));
    l.land(with_id(c, "noop")).await.expect("already included");
    assert_eq!(l.status(REPO, "noop").await, Ok(None));
    let conflict_base = fx.commit(&[c], &[("f", Some("2"))]).await;
    l.land(req(REPO, "main", conflict_base, false)).await.expect("f=2");
    let clash = fx.commit(&[c], &[("f", Some("3"))]).await;
    assert!(matches!(
        l.land(with_id(clash, "failed")).await,
        Err(LandError::Conflict { .. })
    ));
    assert_eq!(l.status(REPO, "failed").await, Ok(None), "failures are not recorded");
}

/// 同一个 change 换了 request id 再次合入（例如客户端超时后重试）：不会重复合入。
#[tokio::test]
async fn change_ids_land_only_once_per_ref() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let base = fx.commit(&[], &[("a", Some("1"))]).await;
    l.land(req(REPO, "main", base, false)).await.expect("base");
    let other = fx.commit(&[base], &[("b", Some("1"))]).await;
    l.land(req(REPO, "main", other, false)).await.expect("other");
    // s1、s2 基于旧的 base：第一次合入时 rebase
    let s1 = fx.commit(&[base], &[("c", Some("1"))]).await;
    let first = l.land(req(REPO, "main", s1, false)).await.expect("s1");
    assert!(first.landed[0].original != first.landed[0].landed);
    // 再次合入 s1：change 已合入，不产生新提交
    let again = l.land(req(REPO, "main", s1, false)).await.expect("s1 again");
    assert_eq!((again.state, again.landed.len()), (first.state, 0));
    assert_eq!(again.previously_landed, first.landed);
    // 栈 [s1, s2]：s1 跳过，只合入 s2
    let s2 = fx.commit(&[s1], &[("d", Some("1"))]).await;
    let o = l.land(req(REPO, "main", s2, false)).await.expect("s2");
    assert_eq!(o.landed.len(), 1);
    assert_eq!(o.landed[0].original, s2);
    assert_eq!(o.previously_landed[0].original, s1);
    assert_eq!(fx.chain(o.state.target).await.len(), 4, "base, other, s1', s2'");
    assert_eq!(fx.read(o.state.target, "d").await.as_deref(), Some("1"));
    // 不同的 ref 各自独立
    let dev = l.land(req(REPO, "dev", other, false)).await.expect("dev");
    let on_dev = l.land(req(REPO, "dev", s1, false)).await.expect("s1 on dev");
    assert_eq!(on_dev.landed.len(), 1);
    assert_eq!(on_dev.state.version, dev.state.version + 1);
}

/// 新建 ref 和快进时整段历史的 change id 都记录下来：之后同一个 change 换了提交再合入时能去重。
#[tokio::test]
async fn change_ids_of_whole_new_ref_history_are_recorded() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let root = fx.commit(&[], &[("a", Some("1"))]).await;
    let mid = fx.changeset(&[root], &[("b", Some("1"))]).await;
    let mid_id = fx.w.create(REPO, &mid).await.expect("mid").id;
    let tip = fx.commit(&[mid_id], &[("c", Some("1"))]).await;
    let created = l.land(req(REPO, "main", tip, true)).await.expect("new ref");
    assert_eq!(created.landed.len(), 3);
    // 同一个 change（mid）在 root 上重新提交了一次：不再合入
    let copy = Changeset::new(
        vec![root],
        sig(),
        sig(),
        "mid, amended",
        mid.change_id,
        vec![FileChange::put("b", EntryMode::File, fx.upload(b"2").await, 1)],
        BTreeMap::new(),
    )
    .expect("cs");
    let copy_id = fx.w.create(REPO, &copy).await.expect("copy").id;
    let o = l.land(req(REPO, "main", copy_id, false)).await.expect("deduped");
    assert_eq!((o.state, o.landed.len()), (created.state, 0));
    assert_eq!(o.previously_landed[0].landed, mid_id);
}

#[tokio::test]
async fn corrupt_stored_result_is_reported() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[]).await;
    fx.store
        .land_commit(
            REPO,
            "main",
            scion_store::RefExpect::Absent,
            a,
            &[],
            &[scion_store::IdempotencyRecord {
                request_id: "bad".into(),
                fingerprint: record::fingerprint("main", &a, false),
                response: vec![9, 9],
                created_at_ms: 0,
            }],
        )
        .await
        .expect("seed");
    let l = fx.lander();
    assert!(matches!(l.status(REPO, "bad").await, Err(LandError::Corrupt(_))));
    let r = LandRequest {
        request_id: Some("bad"),
        actor: "",
        ..req(REPO, "main", a, false)
    };
    assert!(matches!(l.land(r).await, Err(LandError::Corrupt(_))));
}

#[tokio::test]
async fn land_rejects_bad_input() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let a = fx.commit(&[], &[]).await;
    assert_eq!(
        l.land(req(REPO, "bad//ref", a, false)).await,
        Err(LandError::InvalidRef("bad//ref".into()))
    );
    assert_eq!(
        l.land(req("nope", "main", a, false)).await,
        Err(LandError::RepoNotFound("nope".into()))
    );
    let ghost = ObjectId::hash(ObjectKind::Changeset, b"ghost");
    assert_eq!(
        l.land(req(REPO, "main", ghost, false)).await,
        Err(LandError::ObjectsNotUploaded(vec![ghost]))
    );
}

/// 同一基础上的多个互不冲突的提交并发合入：全部成功，每个改动在最终历史中恰好出现一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_lands_all_succeed_with_rebase() {
    let fx = Arc::new(Fx::new().await);
    let base = fx.commit(&[], &[("base", Some("0"))]).await;
    fx.lander().land(req(REPO, "main", base, false)).await.expect("base");
    let mut tips = Vec::new();
    for i in 0..16 {
        let path = format!("f{i}");
        tips.push(fx.commit(&[base], &[(path.as_str(), Some("x"))]).await);
    }
    let lander = Arc::new(fx.lander().with_max_attempts(64));
    let tasks: Vec<_> = tips
        .into_iter()
        .map(|t| {
            let l = lander.clone();
            tokio::spawn(async move { l.land(req(REPO, "main", t, false)).await })
        })
        .collect();
    for t in tasks {
        t.await.expect("join").expect("land");
    }
    let head = fx.store.get_ref(REPO, "main").await.expect("get").expect("ref");
    assert_eq!(head.version, 17);
    let chain = fx.chain(head.target).await;
    assert_eq!(chain.len(), 17);
    for i in 0..16 {
        assert_eq!(fx.read(head.target, &format!("f{i}")).await.as_deref(), Some("x"));
    }
}

/// 可以注入故障的存储：让接下来若干次提交以版本不符失败、返回后端错误，或在提交前等待放行。
struct Hooked {
    inner: Arc<MemStore>,
    mismatches: std::sync::atomic::AtomicU32,
    backend_error: std::sync::atomic::AtomicBool,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    commits: std::sync::atomic::AtomicU32,
}

/// 计数大于 0 时减 1 并返回 true。
fn take_one(n: &std::sync::atomic::AtomicU32) -> bool {
    let mut cur = n.load(Ordering::SeqCst);
    while cur > 0 {
        match n.compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
    false
}

impl Hooked {
    fn new(inner: Arc<MemStore>) -> Self {
        Self {
            inner,
            mismatches: 0.into(),
            backend_error: false.into(),
            gate: None,
            commits: 0.into(),
        }
    }
}

#[async_trait::async_trait]
impl MetaStore for Hooked {
    async fn create_repo(&self, r: &str) -> scion_store::Result<()> {
        self.inner.create_repo(r).await
    }
    async fn repo_exists(&self, r: &str) -> scion_store::Result<bool> {
        self.inner.repo_exists(r).await
    }
    async fn list_repos(&self) -> scion_store::Result<Vec<String>> {
        self.inner.list_repos().await
    }
    async fn get_ref(&self, r: &str, n: &str) -> scion_store::Result<Option<scion_store::RefState>> {
        self.inner.get_ref(r, n).await
    }
    async fn land_commit_as(
        &self,
        r: &str,
        n: &str,
        e: scion_store::RefExpect,
        t: ObjectId,
        l: &[scion_store::LandedChange],
        q: &[scion_store::IdempotencyRecord],
        a: &str,
    ) -> scion_store::Result<scion_store::RefState> {
        if let Some(gate) = &self.gate {
            gate.acquire().await.expect("gate").forget();
        }
        self.commits.fetch_add(1, Ordering::SeqCst);
        if self.backend_error.load(Ordering::SeqCst) {
            return Err(scion_store::Error::Backend("injected".into()));
        }
        if take_one(&self.mismatches) {
            return Err(scion_store::Error::RefVersionMismatch {
                name: n.to_owned(),
                current: None,
            });
        }
        self.inner.land_commit_as(r, n, e, t, l, q, a).await
    }
    async fn landed_changes(
        &self,
        r: &str,
        n: &str,
        c: &[[u8; 16]],
    ) -> scion_store::Result<Vec<scion_store::LandedChange>> {
        self.inner.landed_changes(r, n, c).await
    }
    async fn get_idempotency(&self, r: &str, id: &str) -> scion_store::Result<Option<scion_store::IdempotencyRecord>> {
        self.inner.get_idempotency(r, id).await
    }
    async fn expire_idempotency(&self, before: i64) -> scion_store::Result<u64> {
        self.inner.expire_idempotency(before).await
    }
    async fn acquire_lease(&self, s: &str, h: &str, ttl: i64, now: i64) -> scion_store::Result<scion_store::Lease> {
        self.inner.acquire_lease(s, h, ttl, now).await
    }
    async fn release_lease(&self, s: &str, h: &str) -> scion_store::Result<()> {
        self.inner.release_lease(s, h).await
    }
    async fn index_objects(&self, r: &str, o: &[IndexedObject]) -> scion_store::Result<()> {
        self.inner.index_objects(r, o).await
    }
    async fn missing_objects(&self, r: &str, ids: &[ObjectId]) -> scion_store::Result<Vec<ObjectId>> {
        self.inner.missing_objects(r, ids).await
    }
    async fn get_redactions(&self, r: &str, ids: &[ObjectId]) -> scion_store::Result<Vec<scion_store::Redaction>> {
        self.inner.get_redactions(r, ids).await
    }
    async fn object_sizes(
        &self,
        r: &str,
        ids: &[ObjectId],
    ) -> scion_store::Result<std::collections::HashMap<ObjectId, u64>> {
        self.inner.object_sizes(r, ids).await
    }
    async fn put_changeset(&self, r: &str, rec: &scion_store::ChangesetRecord) -> scion_store::Result<()> {
        self.inner.put_changeset(r, rec).await
    }
    async fn get_changeset(&self, r: &str, id: &ObjectId) -> scion_store::Result<Option<scion_store::ChangesetRecord>> {
        self.inner.get_changeset(r, id).await
    }
    async fn list_refs(&self, r: &str) -> scion_store::Result<Vec<(String, scion_store::RefState)>> {
        self.inner.list_refs(r).await
    }
    async fn changesets_below(
        &self,
        r: &str,
        g: u64,
        l: usize,
    ) -> scion_store::Result<Vec<scion_store::ChangesetRecord>> {
        self.inner.changesets_below(r, g, l).await
    }
    async fn repair_lineage(
        &self,
        r: &str,
        id: &ObjectId,
        p: &[ObjectId],
        d: u64,
        j: Option<ObjectId>,
    ) -> scion_store::Result<()> {
        self.inner.repair_lineage(r, id, p, d, j).await
    }
    async fn index_paths(
        &self,
        r: &str,
        cs: ObjectId,
        g: u64,
        c: &[scion_store::PathChange],
    ) -> scion_store::Result<()> {
        self.inner.index_paths(r, cs, g, c).await
    }
    async fn path_history(
        &self,
        r: &str,
        p: &str,
        g: u64,
        l: usize,
    ) -> scion_store::Result<Vec<scion_store::PathHistoryEntry>> {
        self.inner.path_history(r, p, g, l).await
    }
    async fn get_derived(&self, r: &str, k: &str, v: u32, key: &[u8]) -> scion_store::Result<Option<Vec<u8>>> {
        self.inner.get_derived(r, k, v, key).await
    }
    async fn put_derived(&self, r: &str, k: &str, v: u32, key: &[u8], val: &[u8]) -> scion_store::Result<()> {
        self.inner.put_derived(r, k, v, key, val).await
    }
}

#[tokio::test]
async fn contention_is_reported_after_max_attempts() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[]).await;
    let hooked = Hooked::new(fx.store.clone());
    hooked.mismatches.store(u32::MAX, Ordering::SeqCst);
    let w = Arc::new(ChangesetWriter::new(Arc::new(hooked), fx.store.clone()));
    let l = Lander::new(w).with_max_attempts(2);
    assert_eq!(l.land(req(REPO, "main", a, false)).await, Err(LandError::Contention));
}

#[test]
fn error_conversions() {
    use scion_merge::MergeError;
    use scion_query::QueryError;
    use scion_tree::TreeError;
    let id = ObjectId::of_blob(b"x");
    assert_eq!(
        LandError::from(scion_store::Error::RepoNotFound("r".into())),
        LandError::RepoNotFound("r".into())
    );
    assert_eq!(
        LandError::from(scion_store::Error::RefVersionMismatch {
            name: "m".into(),
            current: None
        }),
        LandError::Contention
    );
    assert_eq!(
        LandError::from(scion_store::Error::Backend("down".into())),
        LandError::Unavailable("down".into())
    );
    assert!(matches!(
        LandError::from(scion_store::Error::InvalidRepoName("x".into())),
        LandError::Corrupt(_)
    ));
    assert!(matches!(
        LandError::from(TreeError::PathConflict("p".into())),
        LandError::InvalidChangeset { path, .. } if path == "p"
    ));
    assert!(matches!(
        LandError::from(TreeError::BadTree(id, "x".into())),
        LandError::Corrupt(_)
    ));
    assert_eq!(
        LandError::from(TreeError::Storage("s".into())),
        LandError::Unavailable("s".into())
    );
    assert!(matches!(
        LandError::from(MergeError::MissingBlob(id)),
        LandError::Corrupt(_)
    ));
    assert_eq!(
        LandError::from(MergeError::Storage("s".into())),
        LandError::Unavailable("s".into())
    );
    assert_eq!(
        LandError::from(MergeError::Redacted {
            id,
            redaction_id: "rd".into()
        }),
        LandError::Redacted {
            id,
            redaction_id: "rd".into()
        }
    );
    assert_eq!(
        LandError::from(scion_store::Error::Redacted {
            id,
            redaction_id: "rd".into()
        }),
        LandError::Redacted {
            id,
            redaction_id: "rd".into()
        }
    );
    assert_eq!(
        LandError::from(MergeError::Tree(TreeError::Storage("t".into()))),
        LandError::Unavailable("t".into())
    );
    assert_eq!(
        LandError::from(QueryError::NotFound(id)),
        LandError::ObjectsNotUploaded(vec![id])
    );
    assert_eq!(
        LandError::from(QueryError::Store(scion_store::Error::Backend("b".into()))),
        LandError::Unavailable("b".into())
    );
    assert_eq!(
        LandError::from(QueryError::Tree(TreeError::Storage("t".into()))),
        LandError::Unavailable("t".into())
    );
    assert!(matches!(
        LandError::from(QueryError::Corrupt(id, "bad".into())),
        LandError::Corrupt(_)
    ));
    assert!(validate_ref_name("feature/x").is_ok());
    assert!(is_tag("tags/v1") && !is_tag("tagsx") && !is_tag("main"));
    assert_eq!(validate_ref_name(""), Err(LandError::InvalidRef(String::new())));
    // 错误信息
    assert_eq!(
        LandError::StackTooDeep(3).to_string(),
        "more than 3 changesets to land at once; land them in smaller steps"
    );
    assert!(
        LandError::Conflict {
            changeset: id,
            conflicts: vec![]
        }
        .to_string()
        .contains("0 paths")
    );
}

// ---- Coordinator ----

fn coordinator(w: Arc<ChangesetWriter>) -> Coordinator {
    Coordinator::new(w, CoordinatorConfig::default())
}

fn with_id<'a>(tip: ObjectId, id: &'a str) -> LandRequest<'a> {
    LandRequest {
        request_id: Some(id),
        actor: "",
        ..req(REPO, "main", tip, false)
    }
}

#[tokio::test]
async fn tags_never_take_lands() {
    let fx = Fx::new().await;
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let tag = LandError::TagImmutable("tags/v1".into());
    assert_eq!(fx.lander().land(req(REPO, "tags/v1", a, false)).await, Err(tag.clone()));
    assert_eq!(
        coordinator(fx.w.clone()).land(req(REPO, "tags/v1", a, false)).await,
        Err(tag)
    );
}

#[tokio::test]
async fn coordinator_lands_like_lander() {
    let fx = Fx::new().await;
    let c = coordinator(fx.w.clone());
    let a = fx.commit(&[], &[("f", Some("1"))]).await;
    let b = fx.commit(&[a], &[("f", Some("2"))]).await;
    let side = fx.commit(&[a], &[("g", Some("1"))]).await;
    let o = c.land(req(REPO, "main", a, false)).await.expect("create");
    assert_eq!((o.state.version, o.landed.len()), (1, 1));
    let o = c.land(with_id(b, "rb")).await.expect("ff");
    assert_eq!((o.state.target, o.state.version), (b, 2));
    assert_eq!(c.status(REPO, "rb").await, Ok(Some(o.clone())));
    assert_eq!(
        c.land(with_id(b, "rb")).await,
        Ok(o.clone()),
        "replayed without queueing"
    );
    let r = c.land(req(REPO, "main", side, false)).await.expect("rebase");
    assert_eq!(fx.read(r.state.target, "f").await.as_deref(), Some("2"));
    assert_eq!(fx.read(r.state.target, "g").await.as_deref(), Some("1"));
    let noop = c.land(req(REPO, "main", b, false)).await.expect("included");
    assert_eq!((noop.state, noop.landed.len()), (r.state, 0));
    assert_eq!(c.status(REPO, "nope").await, Ok(None));
    assert_eq!(
        c.land(with_id(a, "rb")).await,
        Err(LandError::RequestIdReused("rb".into()))
    );
    assert_eq!(
        c.land(req(REPO, "bad//ref", a, false)).await,
        Err(LandError::InvalidRef("bad//ref".into()))
    );
    assert_eq!(
        c.land(req("nope", "main", a, false)).await,
        Err(LandError::RepoNotFound("nope".into()))
    );
    assert_eq!(
        c.land(req(REPO, "main", fx.commit(&[a], &[]).await, true)).await,
        Err(LandError::NotFastForward { tip: r.state.target })
    );
    assert_eq!(c.writer().meta().list_repos().await.expect("repos"), [REPO]);
}

/// 并发的互不冲突的合入被攒成批：全部成功、每个 change 恰好一次，ref 版本数远少于请求数。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_batches_concurrent_lands() {
    let fx = Arc::new(Fx::new().await);
    let base = fx.commit(&[], &[("base", Some("0"))]).await;
    let c = coordinator(fx.w.clone());
    c.land(req(REPO, "main", base, false)).await.expect("base");
    const N: usize = 100;
    let mut tips = Vec::new();
    for i in 0..N {
        let path = format!("f{i}");
        tips.push(fx.commit(&[base], &[(path.as_str(), Some("x"))]).await);
    }
    let tasks: Vec<_> = tips
        .into_iter()
        .enumerate()
        .map(|(i, t)| {
            let c = c.clone();
            tokio::spawn(async move {
                let id = format!("req-{i}");
                let actor = format!("agent-{}", i % 2);
                c.land(LandRequest {
                    request_id: Some(&id),
                    actor: &actor,
                    ..req(REPO, "main", t, false)
                })
                .await
            })
        })
        .collect();
    let mut versions = std::collections::BTreeSet::new();
    for t in tasks {
        let o = t.await.expect("join").expect("land");
        assert_eq!(o.landed.len(), 1);
        versions.insert(o.state.version);
    }
    let head = fx.store.get_ref(REPO, "main").await.expect("get").expect("ref");
    assert_eq!(fx.chain(head.target).await.len(), N + 1);
    // One move per batch, naming everyone whose request it carried out
    let log = fx.store.ref_log(REPO, "main", None, 1000).await.expect("log");
    let actors: std::collections::BTreeSet<&str> = log[..log.len() - 1].iter().map(|e| e.actor.as_str()).collect();
    assert!(
        actors
            .iter()
            .all(|a| ["agent-0", "agent-1", "agent-0, agent-1", "agent-1, agent-0"].contains(a)),
        "{actors:?}"
    );
    assert!(actors.iter().any(|a| a.contains(", ")), "{actors:?}");
    for i in 0..N {
        assert_eq!(fx.read(head.target, &format!("f{i}")).await.as_deref(), Some("x"));
    }
    assert!(
        head.version < (N as u64) / 2,
        "batched: {} versions for {N} lands",
        head.version
    );
    assert_eq!(
        versions.len() as u64,
        head.version - 1,
        "every batch reports its own version"
    );
    // 每个请求都有幂等记录
    assert!(c.status(REPO, "req-0").await.expect("status").is_some());
    assert!(
        c.status(REPO, &format!("req-{}", N - 1))
            .await
            .expect("status")
            .is_some()
    );
}

/// 批内某个请求与前面已接受的请求冲突：只有它失败，其余照常合入。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_isolates_conflicts_within_a_batch() {
    let fx = Arc::new(Fx::new().await);
    let base = fx.commit(&[], &[("shared", Some("0\n"))]).await;
    let c = coordinator(fx.w.clone());
    c.land(req(REPO, "main", base, false)).await.expect("base");
    let mut tips = Vec::new();
    for i in 0..8 {
        let path = format!("own{i}");
        tips.push(fx.commit(&[base], &[(path.as_str(), Some("x"))]).await);
    }
    // 两个请求改了同一行：最多一个成功
    tips.push(fx.commit(&[base], &[("shared", Some("left\n"))]).await);
    tips.push(fx.commit(&[base], &[("shared", Some("right\n"))]).await);
    let tasks: Vec<_> = tips
        .into_iter()
        .map(|t| {
            let c = c.clone();
            tokio::spawn(async move { c.land(req(REPO, "main", t, false)).await })
        })
        .collect();
    let (mut ok, mut conflicts) = (0, 0);
    for t in tasks {
        match t.await.expect("join") {
            Ok(_) => ok += 1,
            Err(LandError::Conflict { .. }) => conflicts += 1,
            Err(e) => panic!("unexpected {e:?}"),
        }
    }
    assert_eq!((ok, conflicts), (9, 1));
}

/// 同一批中的重复请求：相同 request id 共用结果；不同 id 的同一个 change 只合入一次。
#[tokio::test]
async fn coordinator_dedupes_within_a_batch() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[("a", Some("1"))]).await;
    let c = Coordinator::new(
        fx.w.clone(),
        CoordinatorConfig {
            max_wait: Duration::from_millis(200),
            ..CoordinatorConfig::default()
        },
    );
    c.land(req(REPO, "main", base, false)).await.expect("base");
    let other = fx.commit(&[base], &[("b", Some("1"))]).await;
    let stale = fx.commit(&[base], &[("c", Some("1"))]).await;
    let (r1, r2, r3, r4, r5) = tokio::join!(
        c.land(req(REPO, "main", other, false)),
        c.land(with_id(stale, "same")),
        c.land(with_id(stale, "same")),
        c.land(with_id(stale, "different")),
        c.land(with_id(other, "same")),
    );
    let first = r2.expect("stale");
    assert_eq!(r3, Ok(first.clone()), "same request id shares the result");
    let r4 = r4.expect("different id");
    assert_eq!((r4.landed.len(), r4.previously_landed.len()), (0, 1));
    assert_eq!(r5, Err(LandError::RequestIdReused("same".into())));
    let head = r1.expect("other").state;
    assert_eq!(head.version, 2, "one batch");
    assert_eq!(fx.chain(head.target).await.len(), 3, "base, other, stale' (once)");
}

#[tokio::test]
async fn coordinator_redoes_batch_when_ref_moves_and_reports_store_errors() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[]).await;
    let hooked = Arc::new(Hooked::new(fx.store.clone()));
    let w = Arc::new(ChangesetWriter::new(hooked.clone(), fx.store.clone()));
    let c = coordinator(w);
    c.land(req(REPO, "main", base, false)).await.expect("base");
    // 两次条件更新失败后成功
    hooked.mismatches.store(2, Ordering::SeqCst);
    let t = fx.commit(&[base], &[("x", Some("x"))]).await;
    let before = hooked.commits.load(Ordering::SeqCst);
    let o = c.land(req(REPO, "main", t, false)).await.expect("redone");
    assert_eq!(o.state.version, 2);
    assert_eq!(hooked.commits.load(Ordering::SeqCst) - before, 3);
    // 一直失败：Contention
    let cfg = CoordinatorConfig {
        max_attempts: 2,
        ..CoordinatorConfig::default()
    };
    let c2 = Coordinator::new(c.writer().clone(), cfg);
    hooked.mismatches.store(u32::MAX, Ordering::SeqCst);
    let t2 = fx.commit(&[t], &[("y", Some("y"))]).await;
    assert_eq!(c2.land(req(REPO, "main", t2, false)).await, Err(LandError::Contention));
    hooked.mismatches.store(0, Ordering::SeqCst);
    // 存储故障：整批失败
    hooked.backend_error.store(true, Ordering::SeqCst);
    assert_eq!(
        c.land(req(REPO, "main", t2, false)).await,
        Err(LandError::Unavailable("injected".into()))
    );
}

#[tokio::test]
async fn coordinator_sheds_load_when_queue_is_full() {
    let fx = Fx::new().await;
    let base = fx.commit(&[], &[]).await;
    let mut hooked = Hooked::new(fx.store.clone());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    hooked.gate = Some(gate.clone());
    let w = Arc::new(ChangesetWriter::new(Arc::new(hooked), fx.store.clone()));
    let c = Coordinator::new(
        w,
        CoordinatorConfig {
            max_batch: 1,
            queue_capacity: 1,
            ..CoordinatorConfig::default()
        },
    );
    let tips = [
        fx.commit(&[base], &[("a", Some("1"))]).await,
        fx.commit(&[base], &[("b", Some("1"))]).await,
    ];
    // 第一个请求卡在提交上，第二个占满队列，第三个被拒
    let first = tokio::spawn({
        let c = c.clone();
        async move { c.land(req(REPO, "main", base, false)).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = tokio::spawn({
        let c = c.clone();
        let t = tips[0];
        async move { c.land(req(REPO, "main", t, false)).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        c.land(req(REPO, "main", tips[1], false)).await,
        Err(LandError::Overloaded)
    );
    gate.add_permits(10);
    first.await.expect("join").expect("first");
    second.await.expect("join").expect("second");
}

#[tokio::test]
async fn coordinator_tasks_exit_when_idle_and_restart() {
    let fx = Fx::new().await;
    let c = Coordinator::new(
        fx.w.clone(),
        CoordinatorConfig {
            idle_timeout: Duration::from_millis(30),
            ..CoordinatorConfig::default()
        },
    );
    let a = fx.commit(&[], &[]).await;
    c.land(req(REPO, "main", a, false)).await.expect("a");
    c.land(req(REPO, "dev", a, false)).await.expect("dev");
    assert_eq!(c.active_refs(), 2);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(c.active_refs(), 0);
    let b = fx.commit(&[a], &[]).await;
    assert_eq!(c.land(req(REPO, "main", b, false)).await.expect("b").state.version, 2);
}

// ---- 集群模式 ----

fn cluster(node: &str, ttl_ms: u64) -> ClusterConfig {
    ClusterConfig {
        node: node.into(),
        lease_ttl: Duration::from_millis(ttl_ms),
        renew_every: Duration::from_millis(ttl_ms / 4),
    }
}

#[tokio::test]
async fn clustered_nodes_split_refs_by_lease() {
    let fx = Fx::new().await;
    let a = Coordinator::clustered(fx.w.clone(), CoordinatorConfig::default(), cluster("http://a", 5_000));
    let b = Coordinator::clustered(fx.w.clone(), CoordinatorConfig::default(), cluster("http://b", 5_000));
    let base = fx.commit(&[], &[]).await;
    a.land(req(REPO, "main", base, false)).await.expect("a leads main");
    let x = fx.commit(&[base], &[("x", Some("x"))]).await;
    assert_eq!(
        b.land(req(REPO, "main", x, false)).await,
        Err(LandError::NotLeader {
            leader: "http://a".into()
        })
    );
    b.land(req(REPO, "dev", base, false)).await.expect("b leads dev");
    assert_eq!(
        a.land(req(REPO, "dev", x, false)).await,
        Err(LandError::NotLeader {
            leader: "http://b".into()
        })
    );
    // 已成功的请求在任意节点都能重放和查询
    a.land(with_id(x, "rx")).await.expect("x via leader");
    let replay = b.land(with_id(x, "rx")).await.expect("replay on another node");
    assert_eq!(replay.state.version, 2);
    assert!(b.status(REPO, "rx").await.expect("status").is_some());
}

#[tokio::test]
async fn idle_leader_releases_lease_for_others() {
    let fx = Fx::new().await;
    let idle = CoordinatorConfig {
        idle_timeout: Duration::from_millis(40),
        ..CoordinatorConfig::default()
    };
    let a = Coordinator::clustered(fx.w.clone(), idle, cluster("http://a", 5_000));
    let b = Coordinator::clustered(fx.w.clone(), idle, cluster("http://b", 5_000));
    let base = fx.commit(&[], &[]).await;
    a.land(req(REPO, "main", base, false)).await.expect("a");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(a.active_refs(), 0);
    let x = fx.commit(&[base], &[("x", Some("x"))]).await;
    b.land(req(REPO, "main", x, false)).await.expect("b takes over at once");
}

/// leader 进程被杀（租约没有释放）：租约到期前其它节点被告知 leader，到期后接管；
/// 客户端用同一个 request id 重试，已提交的结果被重放，不会重复合入。
#[test]
fn crashed_leader_is_replaced_after_lease_expiry() {
    let main_rt = tokio::runtime::Runtime::new().expect("runtime");
    let fx = main_rt.block_on(Fx::new());
    let (base, x) = main_rt.block_on(async {
        let base = fx.commit(&[], &[]).await;
        let x = fx.commit(&[base], &[("x", Some("x"))]).await;
        (base, x)
    });
    let ttl = 300;
    let leader_rt = tokio::runtime::Runtime::new().expect("runtime");
    let a = Coordinator::clustered(fx.w.clone(), CoordinatorConfig::default(), cluster("http://a", ttl));
    let first = leader_rt
        .block_on(async {
            a.land(req(REPO, "main", base, false)).await?;
            a.land(with_id(x, "rx")).await
        })
        .expect("a lands");
    // 杀掉 leader：任务随运行时一起消失，租约留在存储里
    drop(leader_rt);
    main_rt.block_on(async {
        let b = Coordinator::clustered(fx.w.clone(), CoordinatorConfig::default(), cluster("http://b", ttl));
        let y = fx.commit(&[base], &[("y", Some("y"))]).await;
        assert_eq!(
            b.land(req(REPO, "main", y, false)).await,
            Err(LandError::NotLeader {
                leader: "http://a".into()
            })
        );
        tokio::time::sleep(Duration::from_millis(ttl + 100)).await;
        // 客户端没收到 a 的回复，用同一个 request id 在新 leader 上重试：返回同一结果
        assert_eq!(b.land(with_id(x, "rx")).await, Ok(first));
        let o = b.land(req(REPO, "main", y, false)).await.expect("b leads");
        assert_eq!(o.state.version, 3);
        assert_eq!(fx.chain(o.state.target).await.len(), 3, "base, x, y' — x landed once");
    });
}

/// 租约被别人拿走（例如本节点长时间停顿）：下一次续约时让位，之后的请求得到 NotLeader。
#[tokio::test]
async fn leader_steps_down_when_lease_is_taken() {
    let fx = Fx::new().await;
    let ttl = 200;
    let a = Coordinator::clustered(fx.w.clone(), CoordinatorConfig::default(), cluster("http://a", ttl));
    let base = fx.commit(&[], &[]).await;
    a.land(req(REPO, "main", base, false)).await.expect("a");
    let scope = format!("land:{}:{REPO}:main", REPO.len());
    fx.store.release_lease(&scope, "http://a").await.expect("revoke");
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("ms");
    let l = fx
        .store
        .acquire_lease(&scope, "http://b", 60_000, now)
        .await
        .expect("b grabs");
    assert_eq!(l.holder, "http://b");
    // a 的任务在下一次续约时发现租约已易主
    tokio::time::sleep(Duration::from_millis(ttl)).await;
    assert_eq!(a.active_refs(), 0);
    let x = fx.commit(&[base], &[("x", Some("x"))]).await;
    assert_eq!(
        a.land(req(REPO, "main", x, false)).await,
        Err(LandError::NotLeader {
            leader: "http://b".into()
        })
    );
}

#[tokio::test]
async fn shutdown_releases_leases_for_immediate_takeover() {
    let fx = Fx::new().await;
    let a = Coordinator::clustered(fx.w.clone(), CoordinatorConfig::default(), cluster("http://a", 60_000));
    let b = Coordinator::clustered(fx.w.clone(), CoordinatorConfig::default(), cluster("http://b", 60_000));
    let base = fx.commit(&[], &[]).await;
    a.land(req(REPO, "main", base, false)).await.expect("a");
    a.shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let x = fx.commit(&[base], &[("x", Some("x"))]).await;
    b.land(req(REPO, "main", x, false))
        .await
        .expect("b takes over without waiting for the ttl");
    // 关闭后的节点再收到请求时重新申请租约（此时属于 b）
    let y = fx.commit(&[base], &[("y", Some("y"))]).await;
    assert_eq!(
        a.land(req(REPO, "main", y, false)).await,
        Err(LandError::NotLeader {
            leader: "http://b".into()
        })
    );
}

#[tokio::test]
async fn check_says_what_a_land_would_do_and_writes_nothing() {
    let fx = Fx::new().await;
    let l = fx.lander();
    let base = fx.commit(&[], &[("f", Some("1\n2\n3\n")), ("g", Some("g"))]).await;
    // A new branch, then a fast-forward
    let out = l.check(REPO, "main", base, false).await.expect("new branch");
    assert_eq!((out.state.target, out.state.version, out.landed.len()), (base, 1, 1));
    assert_eq!(
        fx.store.get_ref(REPO, "main").await.expect("get"),
        None,
        "nothing written"
    );
    l.land(req(REPO, "main", base, false)).await.expect("land");
    let a = fx.commit(&[base], &[("f", Some("1\n2\n3\nA\n"))]).await;
    let out = l.check(REPO, "main", a, false).await.expect("fast-forward");
    assert_eq!((out.state.target, out.state.version), (a, 2));
    // Someone lands first: a rebase, which a fast-forward-only land refuses
    let other = fx.commit(&[base], &[("g", Some("g2"))]).await;
    l.land(req(REPO, "main", other, false)).await.expect("other");
    let out = l.check(REPO, "main", a, false).await.expect("rebase");
    assert_ne!(out.landed[0].landed, a, "would be rebased");
    assert_eq!(out.state.version, 3);
    assert!(matches!(
        l.check(REPO, "main", a, true).await,
        Err(LandError::NotFastForward { .. })
    ));
    let head = fx.store.get_ref(REPO, "main").await.expect("get").expect("main");
    assert_eq!((head.target, head.version), (other, 2), "still nothing written");
    // The same lines changed on the branch: the conflict a land would report
    let mine = fx.commit(&[base], &[("g", Some("mine"))]).await;
    assert!(matches!(
        l.check(REPO, "main", mine, false).await,
        Err(LandError::Conflict { .. })
    ));
    // Already there, a tag, a repository that does not exist
    let out = l.check(REPO, "main", other, false).await.expect("already there");
    assert!(out.landed.is_empty() && out.state.version == 2);
    assert!(matches!(
        l.check(REPO, "tags/v1", a, false).await,
        Err(LandError::TagImmutable(_))
    ));
    assert!(matches!(
        l.check("nope", "main", a, false).await,
        Err(LandError::RepoNotFound(_))
    ));
    // The coordinator answers the same
    let c = coordinator(fx.w.clone());
    let out = c.check(REPO, "main", a, false).await.expect("coordinator");
    assert_eq!(out.state.version, 3);
}
