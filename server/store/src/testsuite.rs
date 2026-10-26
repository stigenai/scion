//! 存储后端一致性测试套件。每个后端（内存、SQLite、PostgreSQL、YugabyteDB、S3……）
//! 都必须在自己的测试中调用这里的全部函数并通过。
//!
//! 函数在断言失败时 panic，直接用于 `#[tokio::test]`。

use std::sync::Arc;

use bytes::Bytes;
use scion_object::{ObjectId, ObjectKind};

use crate::{
    BlobStore, ChangesetRecord, Error, IdempotencyRecord, IndexedObject, LandedChange, Lease, MetaStore, OrgMember,
    OrgRecord, PackLocation, PackedObject, PathChange, PathHistoryEntry, Redaction, RedactionFilter, RefExpect,
    RefState,
};

fn cs(n: u8) -> ObjectId {
    ObjectId::hash(ObjectKind::Changeset, &[n])
}

/// MetaStore 的语义：仓库生命周期、ref 的 CAS 与版本号。
pub async fn meta_store<S: MetaStore + ?Sized>(s: &S) {
    // 仓库
    assert_eq!(s.list_repos().await, Ok(vec![]));
    assert_eq!(s.repo_exists("b/two").await, Ok(false));
    s.create_repo("b/two").await.expect("create b/two");
    s.create_repo("a").await.expect("create a");
    assert_eq!(s.repo_exists("b/two").await, Ok(true));
    assert_eq!(s.list_repos().await, Ok(vec!["a".to_string(), "b/two".to_string()]));
    assert_eq!(s.create_repo("a").await, Err(Error::RepoAlreadyExists("a".into())));
    assert!(matches!(s.create_repo("../x").await, Err(Error::InvalidRepoName(_))));

    // 不存在的仓库
    assert_eq!(s.get_ref("nope", "main").await, Err(Error::RepoNotFound("nope".into())));
    assert_eq!(
        s.cas_ref("nope", "main", RefExpect::Absent, cs(1)).await,
        Err(Error::RepoNotFound("nope".into()))
    );

    // 创建 ref
    assert_eq!(s.get_ref("a", "main").await, Ok(None));
    assert_eq!(
        s.cas_ref("a", "main", RefExpect::Version(1), cs(1)).await,
        Err(Error::RefVersionMismatch {
            name: "main".into(),
            current: None
        })
    );
    let v1 = s
        .cas_ref("a", "main", RefExpect::Absent, cs(1))
        .await
        .expect("create ref");
    assert_eq!(
        v1,
        RefState {
            target: cs(1),
            version: 1
        }
    );
    assert_eq!(s.get_ref("a", "main").await, Ok(Some(v1)));

    // 重复创建、版本不符
    assert_eq!(
        s.cas_ref("a", "main", RefExpect::Absent, cs(2)).await,
        Err(Error::RefVersionMismatch {
            name: "main".into(),
            current: Some(1)
        })
    );
    assert_eq!(
        s.cas_ref("a", "main", RefExpect::Version(7), cs(2)).await,
        Err(Error::RefVersionMismatch {
            name: "main".into(),
            current: Some(1)
        })
    );

    // 更新：版本递增；即使 target 不变也递增
    let v2 = s
        .cas_ref("a", "main", RefExpect::Version(1), cs(2))
        .await
        .expect("update");
    assert_eq!(
        v2,
        RefState {
            target: cs(2),
            version: 2
        }
    );
    let v3 = s
        .cas_ref("a", "main", RefExpect::Version(2), cs(2))
        .await
        .expect("same target");
    assert_eq!(v3.version, 3);

    // 仓库之间互相隔离
    assert_eq!(s.get_ref("b/two", "main").await, Ok(None));
    // ref 名字按原样区分
    assert_eq!(s.get_ref("a", "Main").await, Ok(None));
}

/// BlobStore 的语义：全局、幂等写入、读取、存在性。
pub async fn blob_store<S: BlobStore + ?Sized>(s: &S) {
    let a = Bytes::from_static(b"alpha");
    let b = Bytes::from_static(b"");
    let ida = ObjectId::of_blob(&a);
    let idb = ObjectId::of_blob(&b);

    assert_eq!(s.get(&ida).await, Ok(None));
    assert_eq!(s.exists(&ida).await, Ok(false));

    s.put(ida, a.clone()).await.expect("put a");
    s.put(ida, a.clone()).await.expect("put a again is idempotent");
    s.put(idb, b.clone()).await.expect("put empty blob");

    assert_eq!(s.get(&ida).await, Ok(Some(a)));
    assert_eq!(s.get(&idb).await, Ok(Some(b.clone())));
    assert_eq!(s.exists(&ida).await, Ok(true));

    // 字节与 id 不符：拒绝，且不留下任何对象
    let idc = ObjectId::of_blob(b"claimed");
    assert!(matches!(
        s.put(idc, Bytes::from_static(b"actual")).await,
        Err(Error::Backend(_))
    ));
    assert_eq!(s.exists(&idc).await, Ok(false));

    // 大对象（跨越常见缓冲区边界）
    let big = Bytes::from(vec![0x5a; 3 << 20]);
    let idbig = ObjectId::of_blob(&big);
    s.put(idbig, big.clone()).await.expect("put big");
    assert_eq!(s.get(&idbig).await, Ok(Some(big)));

    // 物理删除（redaction 清除）：删除后读不到，重复删除和删除不存在的对象都不是错误
    s.delete(&ida).await.expect("delete");
    assert_eq!(s.get(&ida).await, Ok(None));
    assert_eq!(s.exists(&ida).await, Ok(false));
    s.delete(&ida).await.expect("delete again");
    s.delete(&idc).await.expect("delete missing");
    assert_eq!(s.get(&idb).await, Ok(Some(b)), "other objects untouched");

    // Several objects at once: all readable afterwards, and writing them again is idempotent
    let many: Vec<(ObjectId, Bytes)> = (0..40u32)
        .map(|i| {
            let d = Bytes::from(format!("object {i} {}", "x".repeat(i as usize * 50)));
            (ObjectId::of_blob(&d), d)
        })
        .collect();
    s.put_many(&many).await.expect("put many");
    s.put_many(&many[10..20]).await.expect("put many again");
    for (id, d) in &many {
        assert_eq!(s.get(id).await.as_ref(), Ok(&Some(d.clone())));
        assert_eq!(s.exists(id).await, Ok(true));
    }
    let (gone, kept) = (many[3].0, many[4].0);
    s.delete(&gone).await.expect("delete one of many");
    assert_eq!(s.get(&gone).await, Ok(None));
    assert_eq!(s.exists(&gone).await, Ok(false));
    assert_eq!(s.get(&kept).await, Ok(Some(many[4].1.clone())), "neighbours untouched");
    s.put_many(&many[3..5]).await.expect("rewrite a deleted object");
    assert_eq!(s.get(&gone).await, Ok(Some(many[3].1.clone())));
    let bad = [(ObjectId::of_blob(b"claimed"), Bytes::from_static(b"actual"))];
    assert!(s.put_many(&bad).await.is_err());
    s.put_many(&[]).await.expect("nothing to write");
}

/// Pack locations: global, the latest record wins, forgotten on delete.
pub async fn pack_locations<S: MetaStore + ?Sized>(s: &S) {
    let (a, b, c) = (
        ObjectId::of_blob(b"a"),
        ObjectId::of_blob(b"b"),
        ObjectId::of_blob(b"c"),
    );
    let (p1, p2) = ([1u8; 32], [2u8; 32]);
    assert!(s.pack_locations(&[a]).await.expect("empty").is_empty());
    let objs = [
        PackedObject {
            id: a,
            offset: 5,
            len: 10,
            base: None,
        },
        PackedObject {
            id: b,
            offset: 15,
            len: 1 << 30,
            base: Some(c),
        },
    ];
    s.put_pack_locations(&p1, &objs).await.expect("record");
    s.put_pack_locations(&p1, &objs).await.expect("record again");
    let got = s.pack_locations(&[a, b, c, a]).await.expect("lookup");
    assert_eq!(got.len(), 2);
    assert_eq!(
        got[&a],
        PackLocation {
            pack: p1,
            offset: 5,
            len: 10
        }
    );
    assert_eq!(got[&b].len, 1 << 30);
    s.put_pack_locations(
        &p2,
        &[PackedObject {
            id: a,
            offset: 1,
            len: 2,
            base: Some(c),
        }],
    )
    .await
    .expect("move");
    assert_eq!(s.pack_locations(&[a]).await.expect("lookup")[&a].pack, p2);
    // Deltas: who depends on a base, and re-recording an object as full drops the dependency
    assert_eq!(s.delta_dependents(&c).await.expect("dependents"), {
        let mut v = vec![a, b];
        v.sort();
        v
    });
    s.put_pack_locations(
        &p1,
        &[PackedObject {
            id: b,
            offset: 15,
            len: 3,
            base: None,
        }],
    )
    .await
    .expect("full again");
    assert_eq!(s.delta_dependents(&c).await.expect("dependents"), vec![a]);
    assert!(s.delta_dependents(&b).await.expect("none").is_empty());
    s.delete_pack_locations(&[a, c]).await.expect("forget");
    assert!(s.delta_dependents(&c).await.expect("forgotten").is_empty());
    let got = s.pack_locations(&[a, b]).await.expect("lookup");
    assert_eq!(got.keys().copied().collect::<Vec<_>>(), vec![b]);
    s.put_pack_locations(&p1, &[]).await.expect("nothing to record");
    s.delete_pack_locations(&[]).await.expect("nothing to forget");
    let many: Vec<PackedObject> = (0..1_234u32)
        .map(|i| PackedObject {
            id: ObjectId::of_blob(&i.to_le_bytes()),
            offset: u64::from(i) * 10,
            len: 10,
            base: None,
        })
        .collect();
    s.delete_packs(&[p1, [7; 32]]).await.expect("forget packs");
    s.delete_packs(&[]).await.expect("forget no packs");
    s.compact_after_repack().await.expect("compact");
    s.put_pack_locations(&p2, &many).await.expect("large batch");
    let ids: Vec<ObjectId> = many.iter().map(|o| o.id).collect();
    assert_eq!(s.pack_locations(&ids).await.expect("large lookup").len(), many.len());
}

/// 对象索引的语义：按仓库隔离、幂等登记、FindMissing。
pub async fn object_index<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("idx/a").await.expect("create");
    s.create_repo("idx/b").await.expect("create");
    let o = |d: &[u8]| IndexedObject {
        id: ObjectId::of_blob(d),
        size: d.len() as u64,
    };
    let (x, y, z) = (o(b"x"), o(b"y"), o(b"z"));

    assert_eq!(s.missing_objects("idx/a", &[]).await, Ok(vec![]));
    assert_eq!(s.missing_objects("idx/a", &[x.id, y.id]).await, Ok(vec![x.id, y.id]));
    s.index_objects("idx/a", &[x, y]).await.expect("index");
    s.index_objects("idx/a", &[x]).await.expect("index again is idempotent");
    s.index_objects("idx/a", &[]).await.expect("empty batch");
    // 保持顺序、去重
    assert_eq!(
        s.missing_objects("idx/a", &[z.id, x.id, z.id, y.id]).await,
        Ok(vec![z.id])
    );
    // 仓库之间互相隔离
    assert_eq!(s.missing_objects("idx/b", &[x.id]).await, Ok(vec![x.id]));
    // 一批内的重复条目
    s.index_objects("idx/b", &[z, z]).await.expect("duplicates in batch");
    assert_eq!(s.missing_objects("idx/b", &[z.id]).await, Ok(vec![]));

    assert!(matches!(
        s.index_objects("nope", &[x]).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.missing_objects("nope", &[x.id]).await,
        Err(Error::RepoNotFound(_))
    ));
}

/// changeset 记录与 ref 列表的语义。
pub async fn changesets_and_refs<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("cs/a").await.expect("create");
    s.create_repo("cs/b").await.expect("create");
    let tree = ObjectId::hash(ObjectKind::Tree, b"t");
    let root = ChangesetRecord {
        id: cs(1),
        root_tree: tree,
        parents: vec![],
        generation: 1,
        fp_depth: 0,
        fp_jump: None,
    };
    // 合并提交：多个父提交，顺序必须保留
    let child = ChangesetRecord {
        id: cs(2),
        root_tree: tree,
        parents: vec![cs(1), cs(9), cs(5)],
        generation: u64::from(u32::MAX) + 7,
        fp_depth: 2,
        fp_jump: Some(cs(1)),
    };

    assert_eq!(s.get_changeset("cs/a", &cs(1)).await, Ok(None));
    s.put_changeset("cs/a", &root).await.expect("put root");
    s.put_changeset("cs/a", &child).await.expect("put child");
    assert_eq!(s.get_changeset("cs/a", &cs(1)).await, Ok(Some(root.clone())));
    assert_eq!(s.get_changeset("cs/a", &cs(2)).await, Ok(Some(child.clone())));
    assert_eq!(child.first_parent(), Some(cs(1)));
    assert_eq!(root.first_parent(), None);
    // 幂等：保留第一次写入
    let other = ChangesetRecord {
        root_tree: ObjectId::hash(ObjectKind::Tree, b"other"),
        ..root.clone()
    };
    s.put_changeset("cs/a", &other).await.expect("idempotent put");
    assert_eq!(s.get_changeset("cs/a", &cs(1)).await, Ok(Some(root.clone())));
    // 仓库隔离
    assert_eq!(s.get_changeset("cs/b", &cs(1)).await, Ok(None));
    assert!(matches!(
        s.put_changeset("nope", &root).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.get_changeset("nope", &cs(1)).await,
        Err(Error::RepoNotFound(_))
    ));

    // ref 列表：字节序
    assert_eq!(s.list_refs("cs/a").await, Ok(vec![]));
    for name in ["main", "Zeta", "feature/x"] {
        s.cas_ref("cs/a", name, RefExpect::Absent, cs(1))
            .await
            .expect("create ref");
    }
    s.cas_ref("cs/a", "main", RefExpect::Version(1), cs(2))
        .await
        .expect("update");
    let refs = s.list_refs("cs/a").await.expect("list");
    let names: Vec<_> = refs.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["Zeta", "feature/x", "main"]);
    assert_eq!(
        refs[2].1,
        RefState {
            target: cs(2),
            version: 2
        }
    );
    assert_eq!(s.list_refs("cs/b").await, Ok(vec![]));
    assert!(matches!(s.list_refs("nope").await, Err(Error::RepoNotFound(_))));
}

/// 派生数据：按 (kind, version, key) 存取，幂等，仓库隔离。
pub async fn derived<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("der/a").await.expect("create");
    s.create_repo("der/b").await.expect("create");
    let big = vec![7u8; 100_000];
    assert_eq!(s.get_derived("der/a", "blame", 1, b"k").await, Ok(None));
    s.put_derived("der/a", "blame", 1, b"k", b"v1").await.expect("put");
    s.put_derived("der/a", "blame", 1, b"k", b"other")
        .await
        .expect("idempotent");
    assert_eq!(s.get_derived("der/a", "blame", 1, b"k").await, Ok(Some(b"v1".to_vec())));
    // 版本、种类、键都参与区分
    assert_eq!(s.get_derived("der/a", "blame", 2, b"k").await, Ok(None));
    assert_eq!(s.get_derived("der/a", "other", 1, b"k").await, Ok(None));
    assert_eq!(s.get_derived("der/a", "blame", 1, b"k2").await, Ok(None));
    assert_eq!(s.get_derived("der/b", "blame", 1, b"k").await, Ok(None));
    // 空键、空值、大值、二进制键
    s.put_derived("der/a", "x", 1, b"", b"").await.expect("empty");
    assert_eq!(s.get_derived("der/a", "x", 1, b"").await, Ok(Some(vec![])));
    s.put_derived("der/a", "x", 1, &[0, 255, 0], &big).await.expect("big");
    assert_eq!(s.get_derived("der/a", "x", 1, &[0, 255, 0]).await, Ok(Some(big)));
    // 批量读取：顺序与输入一致，缺失为 None，重复键各自返回
    let keys = vec![b"k2".to_vec(), b"k".to_vec(), vec![], b"k".to_vec()];
    assert_eq!(
        s.get_derived_many("der/a", "blame", 1, &keys).await,
        Ok(vec![None, Some(b"v1".to_vec()), None, Some(b"v1".to_vec())])
    );
    assert_eq!(s.get_derived_many("der/a", "blame", 1, &[]).await, Ok(vec![]));
    let many: Vec<Vec<u8>> = (0..2000u32).map(|i| i.to_be_bytes().to_vec()).collect();
    for k in many.iter().step_by(7) {
        s.put_derived("der/a", "many", 1, k, k).await.expect("put");
    }
    let got = s.get_derived_many("der/a", "many", 1, &many).await.expect("many");
    for (i, (k, v)) in many.iter().zip(&got).enumerate() {
        assert_eq!(v.as_ref(), (i % 7 == 0).then_some(k), "key {i}");
    }
    assert!(matches!(
        s.get_derived_many("nope", "x", 1, &keys).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.get_derived("nope", "x", 1, b"k").await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.put_derived("nope", "x", 1, b"k", b"v").await,
        Err(Error::RepoNotFound(_))
    ));
}

/// 批量读取提交记录与 PathHistory 索引。
pub async fn history_index<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("hist/a").await.expect("create");
    s.create_repo("hist/b").await.expect("create");
    let tree = ObjectId::hash(ObjectKind::Tree, b"t");
    for g in 1..=5u8 {
        let rec = ChangesetRecord {
            id: cs(g),
            root_tree: tree,
            parents: vec![],
            generation: u64::from(g),
            fp_depth: 0,
            fp_jump: None,
        };
        s.put_changeset("hist/a", &rec).await.expect("put");
    }
    let gens = |v: Vec<ChangesetRecord>| v.into_iter().map(|r| r.generation).collect::<Vec<_>>();
    assert_eq!(
        gens(s.changesets_below("hist/a", 4, 2).await.expect("below")),
        vec![4, 3]
    );
    assert_eq!(
        gens(s.changesets_below("hist/a", 100, 100).await.expect("below")),
        vec![5, 4, 3, 2, 1]
    );
    assert_eq!(
        gens(s.changesets_below("hist/a", 0, 10).await.expect("below")),
        Vec::<u64>::new()
    );
    assert_eq!(s.changesets_below("hist/b", 10, 10).await.expect("below"), vec![]);
    // 大代数（超过 i32）
    let big = ChangesetRecord {
        id: cs(9),
        root_tree: tree,
        parents: vec![cs(5)],
        generation: u64::from(u32::MAX) + 1,
        fp_depth: 0,
        fp_jump: None,
    };
    s.put_changeset("hist/a", &big).await.expect("put");
    assert_eq!(
        s.changesets_below("hist/a", u64::from(u32::MAX) + 1, 1)
            .await
            .expect("below"),
        vec![big]
    );

    let ch = |p: &str, deleted| PathChange {
        path: p.into(),
        deleted,
    };
    s.index_paths("hist/a", cs(1), 1, &[ch("src/a.rs", false), ch("README", false)])
        .await
        .expect("index");
    s.index_paths("hist/a", cs(2), 2, &[ch("src/a.rs", false), ch("src/b/c.rs", false)])
        .await
        .expect("index");
    s.index_paths(
        "hist/a",
        cs(3),
        3,
        &[ch("src/a.rs", true), ch("srcx/d.rs", false), ch("src.txt", false)],
    )
    .await
    .expect("index");
    s.index_paths("hist/a", cs(3), 3, &[ch("src/a.rs", true)])
        .await
        .expect("idempotent");
    s.index_paths("hist/a", cs(4), 4, &[]).await.expect("empty");
    let hist = |v: Vec<PathHistoryEntry>| {
        v.into_iter()
            .map(|e| (e.generation, e.path, e.deleted))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        hist(s.path_history("hist/a", "src/a.rs", 10, 10).await.expect("file")),
        vec![
            (3, "src/a.rs".into(), true),
            (2, "src/a.rs".into(), false),
            (1, "src/a.rs".into(), false)
        ]
    );
    assert_eq!(
        hist(s.path_history("hist/a", "src", 10, 10).await.expect("dir")),
        vec![
            (3, "src/a.rs".into(), true),
            (2, "src/a.rs".into(), false),
            (2, "src/b/c.rs".into(), false),
            (1, "src/a.rs".into(), false)
        ],
        "directory matches descendants only, not src.txt or srcx/"
    );
    assert_eq!(s.path_history("hist/a", "src", 2, 1).await.expect("limit").len(), 1);
    assert_eq!(
        hist(s.path_history("hist/a", "src/a.rs", 2, 10).await.expect("max gen")).len(),
        2
    );
    assert!(s.path_history("hist/a", "nope", 10, 10).await.expect("none").is_empty());
    assert!(
        s.path_history("hist/b", "src", 10, 10)
            .await
            .expect("isolated")
            .is_empty()
    );
    assert!(matches!(
        s.path_history("nope", "src", 1, 1).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.index_paths("nope", cs(1), 1, &[]).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(matches!(
        s.changesets_below("nope", 1, 1).await,
        Err(Error::RepoNotFound(_))
    ));
}

/// 第一父提交链跳表：线性历史上 fp_skip 的深度与跳跃指针满足 skew-binary 规则。
pub async fn fp_skip_chain<S: MetaStore>(s: &S) {
    s.create_repo("skip").await.expect("create");
    let tree = ObjectId::hash(ObjectKind::Tree, b"t");
    let mut prev: Option<ObjectId> = None;
    let mut recs = Vec::new();
    for n in 0..40u8 {
        let (fp_depth, fp_jump) = crate::fp_skip(s, "skip", prev).await.expect("skip");
        let rec = ChangesetRecord {
            id: cs(100 + n),
            root_tree: tree,
            parents: prev.into_iter().collect(),
            generation: u64::from(n) + 1,
            fp_depth,
            fp_jump,
        };
        s.put_changeset("skip", &rec).await.expect("put");
        prev = Some(rec.id);
        recs.push(rec);
    }
    assert_eq!((recs[0].fp_depth, recs[0].fp_jump), (1, None), "root jumps to itself");
    let depth_of = |id: ObjectId| recs.iter().find(|r| r.id == id).map(|r| r.fp_depth).expect("known");
    for r in &recs[1..] {
        assert_eq!(r.fp_depth, r.generation);
        let j = depth_of(r.fp_jump.expect("jump"));
        assert!(j < r.fp_depth, "jump goes strictly up the chain");
    }
    // 跳跃长度覆盖多个尺度（skew-binary：1, 3, 7, 15, …）
    let longest = recs
        .iter()
        .skip(1)
        .map(|r| r.fp_depth - depth_of(r.fp_jump.expect("jump")))
        .max()
        .expect("max");
    assert!(longest >= 15, "longest jump {longest}");
    // 父提交深度未知：结果也未知
    let legacy = ChangesetRecord {
        id: cs(250),
        root_tree: tree,
        parents: vec![],
        generation: 1,
        fp_depth: 0,
        fp_jump: None,
    };
    s.put_changeset("skip", &legacy).await.expect("put");
    assert_eq!(crate::fp_skip(s, "skip", Some(cs(250))).await, Ok((0, None)));
    // 修复谱系：父提交与跳表一起覆盖，其它字段不变
    s.repair_lineage("skip", &cs(250), &[cs(1)], 2, Some(cs(1)))
        .await
        .expect("repair");
    let r = s.get_changeset("skip", &cs(250)).await.expect("ok").expect("rec");
    assert_eq!(
        (r.parents.clone(), r.fp_depth, r.fp_jump),
        (vec![cs(1)], 2, Some(cs(1)))
    );
    assert_eq!((r.root_tree, r.generation), (tree, 1));
    // 错误的值可以再次修正（包括多个父提交、清空跳跃指针）
    s.repair_lineage("skip", &cs(250), &[cs(1), cs(2)], 1, None)
        .await
        .expect("re-repair");
    let r = s.get_changeset("skip", &cs(250)).await.expect("ok").expect("rec");
    assert_eq!((r.parents, r.fp_depth, r.fp_jump), (vec![cs(1), cs(2)], 1, None));
    s.repair_lineage("skip", &cs(251), &[], 1, None)
        .await
        .expect("unknown changeset is a no-op");
    assert_eq!(s.get_changeset("skip", &cs(251)).await, Ok(None));
    assert!(matches!(
        s.repair_lineage("nope", &cs(250), &[], 1, None).await,
        Err(Error::RepoNotFound(_))
    ));
    assert!(
        matches!(crate::fp_skip(s, "skip", Some(cs(251))).await, Err(Error::Backend(_))),
        "parent not recorded"
    );
}

/// 并发 CAS：同一版本上的多个竞争者恰好只有一个成功，且版本严格连续。
/// 批量写入记录与路径索引：结果与逐条写入相同，跨越 SQL 分块边界，已存在的记录保留第一次的值。
pub async fn batch_writes<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("batch").await.expect("create");
    let tree = ObjectId::hash(ObjectKind::Tree, b"t");
    let id = |n: u32| ObjectId::hash(ObjectKind::Changeset, &n.to_be_bytes());
    let recs: Vec<ChangesetRecord> = (1..=200u32)
        .map(|n| ChangesetRecord {
            id: id(n),
            root_tree: tree,
            parents: if n == 1 { vec![] } else { vec![id(n - 1), id(1)] },
            generation: u64::from(n),
            fp_depth: u64::from(n),
            fp_jump: (n > 1).then(|| id(1)),
        })
        .collect();
    // 第一条先单独写入一个不同的值：批量写入不覆盖
    let first = ChangesetRecord {
        fp_depth: 0,
        ..recs[0].clone()
    };
    s.put_changeset("batch", &first).await.expect("put");
    s.put_changesets("batch", &recs).await.expect("batch put");
    s.put_changesets("batch", &[]).await.expect("empty");
    assert_eq!(s.get_changeset("batch", &id(1)).await, Ok(Some(first)));
    for r in &recs[1..] {
        assert_eq!(s.get_changeset("batch", &r.id).await.as_ref(), Ok(&Some(r.clone())));
    }
    // 路径索引：300 条，跨越多个提交
    let entries: Vec<(ObjectId, u64, Vec<PathChange>)> = (1..=30u32)
        .map(|n| {
            let changes = (0..10)
                .map(|k| PathChange {
                    path: format!("d{k}/f"),
                    deleted: n % 7 == 0,
                })
                .collect();
            (id(n), u64::from(n), changes)
        })
        .collect();
    s.index_paths_many("batch", &entries).await.expect("index");
    s.index_paths_many("batch", &entries).await.expect("idempotent");
    s.index_paths_many("batch", &[]).await.expect("empty");
    s.index_paths_many("batch", &[(id(31), 31, vec![])])
        .await
        .expect("no changes");
    let hist = s.path_history("batch", "d3", 100, 100).await.expect("history");
    assert_eq!(hist.len(), 30);
    assert_eq!((hist[0].cs, hist[0].generation), (id(30), 30));
    assert!(hist.iter().find(|e| e.cs == id(7)).expect("7").deleted);
    assert_eq!(s.path_history("batch", "d0/f", 100, 100).await.expect("file").len(), 30);
    for r in [
        s.put_changesets("nope", &recs[..1]).await.err(),
        s.index_paths_many("nope", &entries[..1]).await.err(),
    ] {
        assert!(matches!(r, Some(Error::RepoNotFound(_))), "{r:?}");
    }
}

/// 租约：空闲时可以申请、持有者续约不换 token、未过期时别人申请不到、过期后被接管（token 加 1）、释放后立即可被接管。
pub async fn leases<S: MetaStore + ?Sized>(s: &S) {
    let l = s
        .acquire_lease("land/r/main", "node-a", 1_000, 10_000)
        .await
        .expect("acquire");
    assert_eq!(
        l,
        Lease {
            holder: "node-a".into(),
            token: 1,
            expires_at_ms: 11_000
        }
    );
    // 续约
    let l = s
        .acquire_lease("land/r/main", "node-a", 1_000, 10_500)
        .await
        .expect("renew");
    assert_eq!((l.token, l.expires_at_ms), (1, 11_500));
    // 未过期：别人申请到的是现有租约
    let l = s
        .acquire_lease("land/r/main", "node-b", 1_000, 11_000)
        .await
        .expect("contend");
    assert_eq!((l.holder.as_str(), l.token, l.expires_at_ms), ("node-a", 1, 11_500));
    // 其它 scope 互不影响
    let other = s
        .acquire_lease("land/r/dev", "node-b", 1_000, 11_000)
        .await
        .expect("other scope");
    assert_eq!((other.holder.as_str(), other.token), ("node-b", 1));
    // 到期（含边界）后被接管
    let l = s
        .acquire_lease("land/r/main", "node-b", 1_000, 11_500)
        .await
        .expect("take over");
    assert_eq!((l.holder.as_str(), l.token, l.expires_at_ms), ("node-b", 2, 12_500));
    // 非持有者释放无效；持有者释放后立即可被接管
    s.release_lease("land/r/main", "node-a").await.expect("noop release");
    let l = s
        .acquire_lease("land/r/main", "node-a", 1_000, 11_600)
        .await
        .expect("still b");
    assert_eq!(l.holder, "node-b");
    s.release_lease("land/r/main", "node-b").await.expect("release");
    let l = s
        .acquire_lease("land/r/main", "node-a", 1_000, 11_700)
        .await
        .expect("after release");
    assert_eq!((l.holder.as_str(), l.token), ("node-a", 3));
    s.release_lease("missing", "node-a")
        .await
        .expect("release missing scope");
    let all = s.list_leases().await.expect("list");
    assert_eq!(
        all.iter()
            .map(|(k, l)| (k.as_str(), l.holder.as_str()))
            .collect::<Vec<_>>(),
        [("land/r/dev", "node-b"), ("land/r/main", "node-a")]
    );
}

/// redaction 记录：按 (仓库, 对象) 覆盖、全局递增的 seq、各种过滤条件；按 id 查找对象所在的仓库。
pub async fn redactions<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("red/a").await.expect("create");
    s.create_repo("red/b").await.expect("create");
    let x = ObjectId::of_blob(b"secret-x");
    let y = ObjectId::of_blob(b"secret-y");
    s.index_objects(
        "red/a",
        &[IndexedObject { id: x, size: 8 }, IndexedObject { id: y, size: 8 }],
    )
    .await
    .expect("index");
    s.index_objects("red/b", &[IndexedObject { id: x, size: 8 }])
        .await
        .expect("index");
    assert_eq!(
        s.object_repos(&x).await,
        Ok(vec!["red/a".to_string(), "red/b".to_string()])
    );
    assert_eq!(s.object_repos(&y).await, Ok(vec!["red/a".to_string()]));
    assert_eq!(s.object_repos(&ObjectId::of_blob(b"none")).await, Ok(vec![]));

    let rec = |repo: &str, id: ObjectId, rid: &str| Redaction {
        repo: repo.into(),
        id,
        redaction_id: rid.into(),
        reason: "leaked key".into(),
        created_by: "root".into(),
        created_at_ms: 1_000,
        active: true,
        purge_after_ms: None,
        purged_at_ms: None,
        seq: 0,
    };
    assert_eq!(s.get_redactions("red/a", &[x, y]).await, Ok(vec![]));
    assert_eq!(
        s.put_redactions(&[rec("nope", x, "rd1")]).await,
        Err(Error::RepoNotFound("nope".into()))
    );
    s.put_redactions(&[rec("red/a", x, "rd1"), rec("red/a", y, "rd1")])
        .await
        .expect("put");
    s.put_redactions(&[rec("red/b", x, "rd2")]).await.expect("put");
    let all = s.list_redactions(&RedactionFilter::default()).await.expect("list");
    assert_eq!(all.len(), 3);
    assert!(all.windows(2).all(|w| w[0].seq < w[1].seq), "ascending seq");
    assert!(all[0].seq > 0);
    assert_eq!(
        (
            all[2].repo.as_str(),
            all[2].id,
            all[2].reason.as_str(),
            all[2].created_by.as_str()
        ),
        ("red/b", x, "leaked key", "root")
    );
    let mut got = s.get_redactions("red/a", &[x, y, x]).await.expect("get");
    got.sort_by_key(|r| r.seq);
    assert_eq!(got, all[..2]);
    assert_eq!(s.get_redactions("red/b", &[y]).await, Ok(vec![]));

    // 过滤
    let f = |repo: Option<&str>, rid: Option<&str>, after: u64| RedactionFilter {
        repo: repo.map(str::to_owned),
        redaction_id: rid.map(str::to_owned),
        after_seq: after,
        ..RedactionFilter::default()
    };
    assert_eq!(
        s.list_redactions(&f(Some("red/b"), None, 0)).await.expect("repo").len(),
        1
    );
    assert_eq!(s.list_redactions(&f(None, Some("rd1"), 0)).await.expect("rid").len(), 2);
    assert_eq!(
        s.list_redactions(&f(None, None, all[0].seq)).await.expect("after"),
        all[1..]
    );
    let limited = RedactionFilter {
        limit: 1,
        ..RedactionFilter::default()
    };
    assert_eq!(s.list_redactions(&limited).await.expect("limit"), all[..1]);

    // 覆盖：撤销、安排清除、标记已清除，每次都分配新的 seq
    let mut undo = all[1].clone();
    undo.active = false;
    let mut due = all[0].clone();
    due.purge_after_ms = Some(5_000);
    let mut purged = all[2].clone();
    purged.purge_after_ms = Some(1_000);
    purged.purged_at_ms = Some(2_000);
    s.put_redactions(&[undo, due, purged]).await.expect("update");
    let after = s.list_redactions(&f(None, None, all[2].seq)).await.expect("changed");
    assert_eq!(after.len(), 3, "every update gets a new seq");
    assert_eq!(
        s.list_redactions(&RedactionFilter::default()).await.expect("all").len(),
        3
    );
    let due_at = |t: i64| RedactionFilter {
        purge_due_at: Some(t),
        ..RedactionFilter::default()
    };
    assert_eq!(s.list_redactions(&due_at(4_999)).await, Ok(vec![]));
    let due = s.list_redactions(&due_at(5_000)).await.expect("due");
    assert_eq!(due.len(), 1, "inactive and purged records are not due");
    assert_eq!(
        (due[0].repo.as_str(), due[0].id, due[0].purge_after_ms),
        ("red/a", x, Some(5_000))
    );
    let y_rec = s.get_redactions("red/a", &[y]).await.expect("get");
    assert!(!y_rec[0].active);
}

/// 合入的原子提交：ref CAS、landed 行与幂等记录同成同败；已存在的记录保留第一次的值。
pub async fn landing<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("land").await.expect("create");
    s.create_repo("land2").await.expect("create");
    let change = |n: u8, v: u64| LandedChange {
        change_id: [n; 16],
        changeset: cs(n),
        landed: cs(n + 100),
        ref_version: v,
    };
    let req = |id: &str, fp: &[u8], t: i64| IdempotencyRecord {
        request_id: id.to_owned(),
        fingerprint: fp.to_vec(),
        response: format!("resp-{id}").into_bytes(),
        created_at_ms: t,
    };
    assert_eq!(s.landed_changes("land", "main", &[[1; 16]]).await, Ok(vec![]));
    assert_eq!(s.get_idempotency("land", "r1").await, Ok(None));

    let st = s
        .land_commit(
            "land",
            "main",
            RefExpect::Absent,
            cs(101),
            &[change(1, 1)],
            &[req("r1", b"fp1", 1_000)],
        )
        .await
        .expect("first land");
    assert_eq!(
        st,
        RefState {
            target: cs(101),
            version: 1
        }
    );
    assert_eq!(s.get_ref("land", "main").await, Ok(Some(st)));
    assert_eq!(
        s.landed_changes("land", "main", &[[1; 16], [2; 16]]).await,
        Ok(vec![change(1, 1)])
    );
    assert_eq!(
        s.get_idempotency("land", "r1").await,
        Ok(Some(req("r1", b"fp1", 1_000)))
    );
    // 作用域：ref 与仓库都参与区分
    assert_eq!(s.landed_changes("land", "dev", &[[1; 16]]).await, Ok(vec![]));
    assert_eq!(s.get_idempotency("land2", "r1").await, Ok(None));

    // 批量：多个 change 与请求；已存在的 change / 请求保留第一次的记录
    let st = s
        .land_commit(
            "land",
            "main",
            RefExpect::Version(1),
            cs(103),
            &[change(1, 2), change(2, 2), change(3, 2)],
            &[
                req("r1", b"other", 2_000),
                req("r2", b"fp2", 2_000),
                req("r3", b"fp3", 3_000),
            ],
        )
        .await
        .expect("batch");
    assert_eq!(st.version, 2);
    let mut got = s
        .landed_changes("land", "main", &[[1; 16], [2; 16], [3; 16]])
        .await
        .expect("landed");
    got.sort_by_key(|c| c.change_id);
    assert_eq!(got, vec![change(1, 1), change(2, 2), change(3, 2)]);
    assert_eq!(
        s.get_idempotency("land", "r1").await,
        Ok(Some(req("r1", b"fp1", 1_000)))
    );
    assert_eq!(
        s.get_idempotency("land", "r3").await,
        Ok(Some(req("r3", b"fp3", 3_000)))
    );

    // 条件不满足：什么都不写
    let err = s
        .land_commit(
            "land",
            "main",
            RefExpect::Version(1),
            cs(104),
            &[change(4, 2)],
            &[req("r4", b"fp4", 4_000)],
        )
        .await;
    assert!(matches!(err, Err(Error::RefVersionMismatch { current: Some(2), .. })));
    assert_eq!(s.landed_changes("land", "main", &[[4; 16]]).await, Ok(vec![]));
    assert_eq!(s.get_idempotency("land", "r4").await, Ok(None));
    assert_eq!(
        s.get_ref("land", "main").await.expect("get").map(|r| r.version),
        Some(2)
    );
    // cas_ref 等价于不带记录的 land_commit
    assert_eq!(
        s.cas_ref("land", "main", RefExpect::Version(2), cs(105))
            .await
            .map(|r| r.version),
        Ok(3)
    );

    // 过期清理：只删除早于界限的记录，跨仓库生效
    s.land_commit("land2", "main", RefExpect::Absent, cs(1), &[], &[req("old", b"x", 500)])
        .await
        .expect("land2");
    assert_eq!(s.expire_idempotency(2_000).await, Ok(2), "r1 (1000) and old (500)");
    assert_eq!(s.get_idempotency("land", "r1").await, Ok(None));
    assert_eq!(s.get_idempotency("land2", "old").await, Ok(None));
    assert!(
        s.get_idempotency("land", "r2").await.expect("ok").is_some(),
        "boundary is exclusive"
    );
    assert_eq!(s.expire_idempotency(0).await, Ok(0));

    for r in [
        s.landed_changes("nope", "main", &[[1; 16]]).await.err(),
        s.get_idempotency("nope", "r").await.err(),
        s.land_commit("nope", "main", RefExpect::Absent, cs(1), &[], &[])
            .await
            .err(),
    ] {
        assert!(matches!(r, Some(Error::RepoNotFound(_))), "{r:?}");
    }
}

pub async fn concurrent_cas<S: MetaStore>(s: Arc<S>) {
    const WRITERS: u8 = 16;
    const ROUNDS: u64 = 20;
    s.create_repo("race").await.expect("create repo");
    s.cas_ref("race", "main", RefExpect::Absent, cs(0))
        .await
        .expect("create ref");

    for round in 1..=ROUNDS {
        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let s = s.clone();
            tasks.push(tokio::spawn(async move {
                s.cas_ref("race", "main", RefExpect::Version(round), cs(w)).await
            }));
        }
        let mut ok = 0;
        for t in tasks {
            match t.await.expect("task") {
                Ok(state) => {
                    assert_eq!(state.version, round + 1);
                    ok += 1;
                }
                Err(Error::RefVersionMismatch { current, .. }) => assert_eq!(current, Some(round + 1)),
                Err(e) => panic!("unexpected error {e}"),
            }
        }
        assert_eq!(ok, 1, "round {round}: exactly one writer must win");
    }
    let last = s.get_ref("race", "main").await.expect("get").expect("exists");
    assert_eq!(last.version, ROUNDS + 1);
}

fn token(id: &str, subject: &str, created_at_ms: i64) -> crate::TokenRecord {
    let mut hash = [7u8; 32];
    hash[..id.len()].copy_from_slice(id.as_bytes());
    crate::TokenRecord {
        id: id.into(),
        hash,
        kind: "pat".into(),
        subject: subject.into(),
        groups: vec!["eng".into(), "ops".into()],
        description: format!("token {id}"),
        created_at_ms,
        expires_at_ms: if id == "t1" { None } else { Some(created_at_ms + 1000) },
    }
}

/// 令牌与全局设置。
/// Organizations and memberships.
pub async fn orgs_and_members<S: MetaStore + ?Sized>(s: &S) {
    let org = |name: &str| OrgRecord {
        name: name.into(),
        visibility: "private".into(),
        created_at_ms: 10,
        created_by: "root".into(),
    };
    let member = |org: &str, subject: &str, role: &str| OrgMember {
        org: org.into(),
        subject: subject.into(),
        role: role.into(),
        added_at_ms: 20,
        added_by: "root".into(),
    };
    s.create_org(&org("opencsg")).await.expect("create");
    s.create_org(&org("acme")).await.expect("create");
    assert_eq!(
        s.create_org(&org("acme")).await,
        Err(Error::OrgAlreadyExists("acme".into()))
    );
    assert_eq!(s.get_org("opencsg").await.expect("get"), Some(org("opencsg")));
    assert_eq!(s.get_org("nope").await.expect("get"), None);
    let names: Vec<String> = s.list_orgs().await.expect("list").into_iter().map(|o| o.name).collect();
    assert_eq!(names, ["acme", "opencsg"]);
    assert!(s.set_org_visibility("acme", "internal").await.expect("set"));
    assert!(!s.set_org_visibility("nope", "internal").await.expect("set"));
    assert_eq!(
        s.get_org("acme").await.expect("get").map(|o| o.visibility).as_deref(),
        Some("internal")
    );

    s.put_org_member(&member("opencsg", "bob", "developer"))
        .await
        .expect("add");
    s.put_org_member(&member("opencsg", "alice", "owner"))
        .await
        .expect("add");
    s.put_org_member(&member("acme", "alice", "reporter"))
        .await
        .expect("add");
    assert_eq!(
        s.put_org_member(&member("nope", "alice", "owner")).await,
        Err(Error::OrgNotFound("nope".into()))
    );
    // A role change keeps when and by whom the member was added
    let mut promoted = member("opencsg", "bob", "maintainer");
    promoted.added_at_ms = 99;
    promoted.added_by = "alice".into();
    s.put_org_member(&promoted).await.expect("role");
    let members = s.list_org_members("opencsg").await.expect("members");
    assert_eq!(
        members,
        [
            member("opencsg", "alice", "owner"),
            member("opencsg", "bob", "maintainer")
        ]
    );
    let mine = s.list_memberships("alice").await.expect("memberships");
    assert_eq!(
        mine,
        [member("acme", "alice", "reporter"), member("opencsg", "alice", "owner")]
    );
    assert!(s.delete_org_member("opencsg", "bob").await.expect("remove"));
    assert!(!s.delete_org_member("opencsg", "bob").await.expect("remove"));
    assert!(s.delete_org("acme").await.expect("delete"));
    assert!(!s.delete_org("acme").await.expect("delete"));
    assert_eq!(
        s.list_memberships("alice").await.expect("memberships"),
        [member("opencsg", "alice", "owner")],
        "memberships go with the organization"
    );
    s.create_org(&org("acme")).await.expect("the name can be used again");
    assert!(s.list_org_members("acme").await.expect("members").is_empty());
}

pub async fn tokens_and_settings<S: MetaStore + ?Sized>(s: &S) {
    let (t1, t2, t3) = (
        token("t1", "alice", 10),
        token("t2", "bob", 20),
        token("t3", "alice", 30),
    );
    for t in [&t2, &t1, &t3] {
        s.put_token(t).await.expect("put token");
    }
    assert!(s.put_token(&t1).await.is_err(), "duplicate id rejected");
    assert_eq!(s.get_token(&t1.hash).await.expect("get"), Some(t1.clone()));
    assert_eq!(s.get_token(&[0; 32]).await.expect("get"), None);
    assert_eq!(
        s.list_tokens(None).await.expect("list"),
        [t1.clone(), t2.clone(), t3.clone()]
    );
    assert_eq!(
        s.list_tokens(Some("alice")).await.expect("list"),
        [t1.clone(), t3.clone()]
    );
    assert!(s.delete_token("t1").await.expect("delete"));
    assert!(!s.delete_token("t1").await.expect("delete again"));
    assert_eq!(s.get_token(&t1.hash).await.expect("get"), None);
    let no_groups = crate::TokenRecord {
        groups: vec![],
        ..token("t4", "carol", 40)
    };
    s.put_token(&no_groups).await.expect("put");
    assert_eq!(s.get_token(&no_groups.hash).await.expect("get"), Some(no_groups));

    assert_eq!(s.get_setting("acl").await.expect("get"), None);
    s.put_setting("acl", b"v1").await.expect("put");
    s.put_setting("acl", b"v2").await.expect("overwrite");
    assert_eq!(s.get_setting("acl").await.expect("get").as_deref(), Some(&b"v2"[..]));
}

/// 仓库管理：属性、列出与分页、软删除与恢复、清除、fork、删除 ref。
pub async fn repo_admin<S: MetaStore + ?Sized>(s: &S) {
    use crate::{RepoFilter, RepoInfo, RepoPatch};
    let info = |name: &str| RepoInfo {
        default_branch: "dev".into(),
        description: "a model".into(),
        ..RepoInfo::new(name, 1_000)
    };
    s.create_repo_with(&info("adm/src")).await.expect("create");
    s.create_repo("adm/plain").await.expect("create");
    s.create_repo("adm_other").await.expect("create");
    assert_eq!(
        s.create_repo_with(&info("adm/src")).await,
        Err(Error::RepoAlreadyExists("adm/src".into()))
    );
    assert!(matches!(
        s.create_repo_with(&info("../x")).await,
        Err(Error::InvalidRepoName(_))
    ));
    let got = s.get_repo("adm/src").await.expect("get").expect("exists");
    assert_eq!(got, info("adm/src"));
    let plain = s.get_repo("adm/plain").await.expect("get").expect("exists");
    assert_eq!(
        (plain.default_branch.as_str(), plain.description.as_str()),
        ("main", "")
    );
    assert!(plain.created_at_ms > 0);
    assert_eq!(s.get_repo("adm/none").await, Ok(None));

    // 列出：前缀、分页、按名字排序
    let names = |v: Vec<RepoInfo>| v.into_iter().map(|r| r.name).collect::<Vec<_>>();
    let filter = |prefix: &str, after: Option<&str>, limit: usize| RepoFilter {
        prefix: prefix.into(),
        after: after.map(str::to_owned),
        limit,
        deleted: false,
    };
    assert_eq!(
        names(s.list_repo_infos(&filter("adm/", None, 10)).await.expect("list")),
        ["adm/plain", "adm/src"]
    );
    assert_eq!(
        names(s.list_repo_infos(&filter("adm", None, 10)).await.expect("list")),
        ["adm/plain", "adm/src", "adm_other"]
    );
    assert_eq!(
        names(s.list_repo_infos(&filter("adm", None, 1)).await.expect("page 1")),
        ["adm/plain"]
    );
    assert_eq!(
        names(
            s.list_repo_infos(&filter("adm", Some("adm/plain"), 1))
                .await
                .expect("page 2")
        ),
        ["adm/src"]
    );
    assert_eq!(
        names(
            s.list_repo_infos(&filter("adm", Some("a"), 10))
                .await
                .expect("cursor before prefix")
        ),
        ["adm/plain", "adm/src", "adm_other"]
    );

    // 修改属性
    let patch = RepoPatch {
        description: Some("updated".into()),
        default_branch: None,
    };
    let updated = s.update_repo("adm/src", &patch).await.expect("update");
    assert_eq!(
        (updated.description.as_str(), updated.default_branch.as_str()),
        ("updated", "dev")
    );
    let patch = RepoPatch {
        default_branch: Some("main".into()),
        description: None,
    };
    assert_eq!(
        s.update_repo("adm/src", &patch).await.expect("update").default_branch,
        "main"
    );
    assert_eq!(
        s.update_repo("adm/none", &patch).await,
        Err(Error::RepoNotFound("adm/none".into()))
    );

    // 准备数据：对象、提交、派生、路径历史、ref、合入记录、redaction
    let blob = ObjectId::of_blob(b"weights");
    let gone = ObjectId::of_blob(b"unredacted");
    s.index_objects(
        "adm/src",
        &[
            IndexedObject { id: blob, size: 7 },
            IndexedObject { id: gone, size: 10 },
        ],
    )
    .await
    .expect("index");
    let root = ChangesetRecord {
        id: cs(201),
        root_tree: ObjectId::hash(ObjectKind::Tree, b"t"),
        parents: vec![],
        generation: 1,
        fp_depth: 1,
        fp_jump: None,
    };
    s.put_changeset("adm/src", &root).await.expect("changeset");
    s.put_derived("adm/src", "k", 1, b"key", b"value")
        .await
        .expect("derived");
    s.index_paths(
        "adm/src",
        root.id,
        1,
        &[PathChange {
            path: "model.bin".into(),
            deleted: false,
        }],
    )
    .await
    .expect("paths");
    let landed = LandedChange {
        change_id: [7; 16],
        changeset: root.id,
        landed: root.id,
        ref_version: 1,
    };
    s.land_commit("adm/src", "main", RefExpect::Absent, root.id, &[landed], &[])
        .await
        .expect("land");
    s.cas_ref("adm/src", "dev", RefExpect::Absent, root.id)
        .await
        .expect("dev");
    let red = |id: ObjectId, active: bool| Redaction {
        repo: "adm/src".into(),
        id,
        redaction_id: "rd_adm".into(),
        reason: "leak".into(),
        created_by: "root".into(),
        created_at_ms: 5,
        active,
        purge_after_ms: None,
        purged_at_ms: None,
        seq: 0,
    };
    s.put_redactions(&[red(blob, true), red(gone, false)])
        .await
        .expect("redact");
    let src_seq = s.get_redactions("adm/src", &[blob]).await.expect("get")[0].seq;

    // fork：复制属性、对象、提交、派生、路径历史、ref 与有效的 redaction；不复制合入记录
    assert_eq!(
        s.fork_repo("adm/none", "adm/fork", 9).await,
        Err(Error::RepoNotFound("adm/none".into()))
    );
    assert_eq!(
        s.fork_repo("adm/src", "adm/plain", 9).await,
        Err(Error::RepoAlreadyExists("adm/plain".into()))
    );
    assert!(matches!(
        s.fork_repo("adm/src", "../f", 9).await,
        Err(Error::InvalidRepoName(_))
    ));
    s.fork_repo("adm/src", "adm/fork", 9).await.expect("fork");
    let fork = s.get_repo("adm/fork").await.expect("get").expect("fork exists");
    assert_eq!(fork.forked_from.as_deref(), Some("adm/src"));
    assert_eq!((fork.created_at_ms, fork.description.as_str()), (9, "updated"));
    assert_eq!(s.missing_objects("adm/fork", &[blob, gone]).await, Ok(vec![]));
    assert_eq!(s.get_changeset("adm/fork", &root.id).await, Ok(Some(root.clone())));
    assert_eq!(
        s.get_derived("adm/fork", "k", 1, b"key").await,
        Ok(Some(b"value".to_vec()))
    );
    assert_eq!(
        s.path_history("adm/fork", "model.bin", u64::MAX >> 1, 10)
            .await
            .expect("history")
            .len(),
        1
    );
    let refs = s.list_refs("adm/fork").await.expect("refs");
    assert_eq!(
        refs.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["dev", "main"]
    );
    assert_eq!(s.landed_changes("adm/fork", "main", &[[7; 16]]).await, Ok(vec![]));
    let fr = s.get_redactions("adm/fork", &[blob, gone]).await.expect("redactions");
    assert_eq!(fr.len(), 1, "only active redactions are copied");
    assert!(fr[0].active && fr[0].id == blob && fr[0].repo == "adm/fork");
    assert!(fr[0].seq > src_seq, "fresh seq so caches sync the fork");

    // 删除 ref
    assert_eq!(
        s.delete_ref("adm/fork", "dev", Some(9)).await,
        Err(Error::RefVersionMismatch {
            name: "dev".into(),
            current: Some(1)
        })
    );
    s.delete_ref("adm/fork", "dev", Some(1)).await.expect("delete dev");
    assert_eq!(s.get_ref("adm/fork", "dev").await, Ok(None));
    assert_eq!(
        s.delete_ref("adm/fork", "dev", None).await,
        Err(Error::RefVersionMismatch {
            name: "dev".into(),
            current: None
        })
    );
    s.delete_ref("adm/fork", "main", None).await.expect("unconditional");
    // 删除后重新创建：版本从 1 开始，之后照常前进（审计日志不能因旧版本号冲突）
    let again = s
        .cas_ref("adm/fork", "dev", RefExpect::Absent, root.id)
        .await
        .expect("recreate");
    assert_eq!(again.version, 1);
    s.cas_ref("adm/fork", "dev", RefExpect::Version(1), root.id)
        .await
        .expect("advance");
    s.delete_ref("adm/fork", "dev", None).await.expect("delete again");
    assert_eq!(
        s.get_ref("adm/src", "main")
            .await
            .expect("source untouched")
            .map(|r| r.version),
        Some(1)
    );
    assert_eq!(
        s.delete_ref("adm/none", "main", None).await,
        Err(Error::RepoNotFound("adm/none".into()))
    );

    // 软删除：按名字找不到，数据保留；名字被占用直到清除
    s.delete_repo("adm/src", 50).await.expect("delete");
    assert_eq!(s.repo_exists("adm/src").await, Ok(false));
    assert!(!s.list_repos().await.expect("list").contains(&"adm/src".to_string()));
    assert_eq!(
        s.get_ref("adm/src", "main").await,
        Err(Error::RepoNotFound("adm/src".into()))
    );
    assert_eq!(
        s.delete_repo("adm/src", 51).await,
        Err(Error::RepoNotFound("adm/src".into()))
    );
    assert_eq!(s.object_repos(&blob).await, Ok(vec!["adm/fork".to_string()]));
    assert_eq!(
        s.create_repo("adm/src").await,
        Err(Error::RepoDeleted("adm/src".into()))
    );
    assert_eq!(
        s.fork_repo("adm/fork", "adm/src", 9).await,
        Err(Error::RepoDeleted("adm/src".into()))
    );
    assert_eq!(
        s.get_repo("adm/src").await.expect("get").and_then(|r| r.deleted_at_ms),
        Some(50)
    );
    let deleted = RepoFilter {
        deleted: true,
        ..filter("adm", None, 10)
    };
    assert_eq!(names(s.list_repo_infos(&deleted).await.expect("deleted")), ["adm/src"]);
    assert!(!names(s.list_repo_infos(&filter("adm", None, 10)).await.expect("live")).contains(&"adm/src".into()));
    assert_eq!(
        s.purge_repo("adm/fork", 10).await,
        Err(Error::RepoNotFound("adm/fork".into()))
    );

    // 恢复
    assert_eq!(
        s.restore_repo("adm/fork").await,
        Err(Error::RepoNotFound("adm/fork".into()))
    );
    s.restore_repo("adm/src").await.expect("restore");
    assert_eq!(
        s.get_ref("adm/src", "main").await.expect("ref").map(|r| r.target),
        Some(root.id)
    );
    assert_eq!(
        s.get_repo("adm/src").await.expect("get").and_then(|r| r.deleted_at_ms),
        None
    );

    // 清除：分批，完成后名字可以重新使用，新仓库是空的。清除开始后不能再恢复
    s.delete_repo("adm/src", 60).await.expect("delete again");
    let mut done = s.purge_repo("adm/src", 1).await.expect("first batch");
    if !done {
        assert_eq!(
            s.restore_repo("adm/src").await,
            Err(Error::RepoPurging("adm/src".into()))
        );
        assert!(
            s.get_repo("adm/src")
                .await
                .expect("get")
                .is_some_and(|r| r.deleted_at_ms.is_some())
        );
    }
    let mut rounds = 0;
    while !done {
        done = s.purge_repo("adm/src", 1).await.expect("purge");
        rounds += 1;
        assert!(rounds < 100, "purge must finish");
    }
    assert_eq!(s.get_repo("adm/src").await, Ok(None));
    assert_eq!(
        s.purge_repo("adm/src", 1).await,
        Err(Error::RepoNotFound("adm/src".into()))
    );
    s.create_repo("adm/src").await.expect("name reusable after purge");
    assert_eq!(s.list_refs("adm/src").await, Ok(vec![]));
    assert_eq!(s.missing_objects("adm/src", &[blob]).await, Ok(vec![blob]));
    assert_eq!(s.get_changeset("adm/src", &root.id).await, Ok(None));
    assert_eq!(s.get_derived("adm/src", "k", 1, b"key").await, Ok(None));
    assert_eq!(s.get_redactions("adm/src", &[blob, gone]).await, Ok(vec![]));
    assert_eq!(s.landed_changes("adm/src", "main", &[[7; 16]]).await, Ok(vec![]));
    // fork 不受来源清除的影响
    assert_eq!(s.missing_objects("adm/fork", &[blob]).await, Ok(vec![]));
}

/// ref 变化的事件流：与 ref 修改同一事务写入，按 seq 递增；失败的 CAS 不产生事件；fork 为每个 ref 产生创建事件。
/// Tag annotations: saved, replaced, copied by a fork, gone with the ref.
pub async fn tag_annotations<S: MetaStore + ?Sized>(s: &S) {
    use crate::TagAnnotation;
    s.create_repo("tags/a").await.expect("create");
    let note = |target, message: &str, git_object: Option<Vec<u8>>| TagAnnotation {
        target,
        tagger: scion_object::Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 1_700_000_000_000,
            tz_offset_minutes: -300,
        },
        message: message.into(),
        git_object,
    };
    assert!(s.tag_annotations("tags/a").await.expect("none").is_empty());
    s.cas_ref("tags/a", "tags/v1", RefExpect::Absent, cs(121))
        .await
        .expect("tag");
    s.put_tag_annotation("tags/a", "tags/v1", &note(cs(121), "first", None))
        .await
        .expect("put");
    s.put_tag_annotation(
        "tags/a",
        "tags/v1",
        &note(cs(121), "v1\n\nnotes", Some(b"object ...".to_vec())),
    )
    .await
    .expect("replace");
    s.cas_ref("tags/a", "tags/v2", RefExpect::Absent, cs(122))
        .await
        .expect("tag");
    s.put_tag_annotation("tags/a", "tags/v2", &note(cs(122), "v2", None))
        .await
        .expect("put");
    let all = s.tag_annotations("tags/a").await.expect("list");
    assert_eq!(all.len(), 2);
    assert_eq!(
        all["tags/v1"],
        note(cs(121), "v1\n\nnotes", Some(b"object ...".to_vec()))
    );
    s.delete_ref("tags/a", "tags/v2", None).await.expect("delete");
    let left: Vec<String> = s.tag_annotations("tags/a").await.expect("list").into_keys().collect();
    assert_eq!(left, ["tags/v1"]);
    s.fork_repo("tags/a", "tags/fork", 1).await.expect("fork");
    let forked: Vec<String> = s
        .tag_annotations("tags/fork")
        .await
        .expect("list")
        .into_keys()
        .collect();
    assert_eq!(forked, ["tags/v1"]);
}

/// The ref log: one entry per move, newest first, paged by version, gone with the ref.
pub async fn ref_log<S: MetaStore + ?Sized>(s: &S) {
    s.create_repo("log/a").await.expect("create");
    let (x, y, z) = (cs(111), cs(112), cs(113));
    s.cas_ref("log/a", "main", RefExpect::Absent, x).await.expect("create");
    s.land_commit("log/a", "main", RefExpect::Version(1), y, &[], &[])
        .await
        .expect("land");
    s.cas_ref("log/a", "main", RefExpect::Version(2), z)
        .await
        .expect("move");
    assert!(s.cas_ref("log/a", "main", RefExpect::Version(1), x).await.is_err());
    s.cas_ref("log/a", "other", RefExpect::Absent, x).await.expect("other");

    let brief = |v: Vec<crate::RefLogEntry>| v.into_iter().map(|e| (e.version, e.target)).collect::<Vec<_>>();
    let all = s.ref_log("log/a", "main", None, 10).await.expect("log");
    assert!(all.iter().all(|e| e.updated_at_ms > 0));
    assert_eq!(brief(all), [(3, z), (2, y), (1, x)]);
    assert_eq!(
        brief(s.ref_log("log/a", "main", None, 1).await.expect("page")),
        [(3, z)]
    );
    assert_eq!(
        brief(s.ref_log("log/a", "main", Some(3), 10).await.expect("before")),
        [(2, y), (1, x)]
    );
    assert!(s.ref_log("log/a", "nope", None, 10).await.expect("none").is_empty());
    s.delete_ref("log/a", "main", None).await.expect("delete");
    assert!(s.ref_log("log/a", "main", None, 10).await.expect("deleted").is_empty());
    assert_eq!(
        brief(s.ref_log("log/a", "other", None, 10).await.expect("other")),
        [(1, x)]
    );
}

pub async fn ref_events<S: MetaStore + ?Sized>(s: &S) {
    use crate::RefEvent;
    let start = s.ref_event_bounds().await.expect("bounds").1;
    s.create_repo("ev/a").await.expect("create");
    s.create_repo("ev/b").await.expect("create");
    let (x, y) = (cs(101), cs(102));
    s.cas_ref("ev/a", "main", RefExpect::Absent, x)
        .await
        .expect("create main");
    s.cas_ref("ev/b", "main", RefExpect::Absent, y)
        .await
        .expect("create b main");
    s.land_commit("ev/a", "main", RefExpect::Version(1), y, &[], &[])
        .await
        .expect("advance");
    assert!(
        s.cas_ref("ev/a", "main", RefExpect::Version(1), x).await.is_err(),
        "stale CAS"
    );
    s.delete_ref("ev/a", "main", None).await.expect("delete");

    let all = s.list_ref_events(start, 100, None).await.expect("list");
    let brief = |e: &RefEvent| (e.repo.clone(), e.name.clone(), e.old_target, e.new_target, e.version);
    assert_eq!(
        all.iter().map(brief).collect::<Vec<_>>(),
        [
            ("ev/a".to_string(), "main".to_string(), None, Some(x), 1),
            ("ev/b".to_string(), "main".to_string(), None, Some(y), 1),
            ("ev/a".to_string(), "main".to_string(), Some(x), Some(y), 2),
            ("ev/a".to_string(), "main".to_string(), Some(y), None, 2),
        ]
    );
    assert!(all.windows(2).all(|w| w[1].seq == w[0].seq + 1), "consecutive seqs");
    assert!(all.iter().all(|e| e.created_at_ms > 0));
    // 分页与按仓库过滤
    let first = s.list_ref_events(start, 1, None).await.expect("page");
    assert_eq!(first.len(), 1);
    let rest = s.list_ref_events(first[0].seq, 100, None).await.expect("rest");
    assert_eq!(rest.len(), 3);
    let only_b = s.list_ref_events(start, 100, Some("ev/b")).await.expect("repo");
    assert_eq!(
        only_b.iter().map(brief).collect::<Vec<_>>(),
        [all[1].clone()].iter().map(brief).collect::<Vec<_>>()
    );
    let (lo, hi) = s.ref_event_bounds().await.expect("bounds");
    assert!(lo.is_some_and(|lo| lo <= all[0].seq) && hi == all[3].seq);

    // 重新创建删除过的 ref：旧值为空
    s.cas_ref("ev/a", "main", RefExpect::Absent, x).await.expect("recreate");
    // fork：每个 ref 一个创建事件
    s.fork_repo("ev/b", "ev/fork", 1).await.expect("fork");
    let tail = s.list_ref_events(hi, 100, None).await.expect("tail");
    assert_eq!(
        tail.iter().map(brief).collect::<Vec<_>>(),
        [
            ("ev/a".to_string(), "main".to_string(), None, Some(x), 1),
            ("ev/fork".to_string(), "main".to_string(), None, Some(y), 1),
        ]
    );

    // 清理：按时间删除
    assert_eq!(s.prune_ref_events(0).await, Ok(0));
    assert!(s.prune_ref_events(i64::MAX).await.expect("prune") >= 6);
    assert_eq!(
        s.ref_event_bounds().await,
        Ok((None, tail[1].seq)),
        "counter survives pruning"
    );
    assert_eq!(s.list_ref_events(0, 100, None).await, Ok(vec![]));
    // 清理后序号继续递增
    s.cas_ref("ev/b", "main", RefExpect::Version(1), x)
        .await
        .expect("advance b");
    let after = s.list_ref_events(0, 100, None).await.expect("after prune");
    assert_eq!(after.len(), 1);
    assert!(after[0].seq > tail[1].seq);
}
