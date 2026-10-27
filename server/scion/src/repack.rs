//! `scion admin repack`: rewrite reachable objects into large packs, storing older versions of
//! each file and directory as deltas against the next newer version, then remove the packs and
//! loose objects this superseded. Like `git gc`, run it now and then; it is safe while the server
//! is running.
//!
//! The walk goes through every repository's commits newest first and compares each commit's
//! tree with its first parent's. An entry that changed pairs the parent's version (older) with
//! the commit's (newer): the older one is stored against the newer one. The newest versions,
//! which are read most, stay whole, and each version's base is written before it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use scion_object::{EntryMode, ObjectId, Tree};
use scion_store::{BlobStore, MetaStore};
use scion_store_blob::{ObjectBlobStore, PruneStats, RepackItem, RepackStats};

#[derive(Debug, Clone)]
pub struct RepackOptions {
    /// Only these repositories; all when empty. Objects shared with other repositories are
    /// repacked too, since packs are global.
    pub repos: Vec<String>,
    /// Keep packs younger than this when pruning (a writer may not have recorded them yet).
    pub prune_after: Duration,
    /// Only report what would be repacked.
    pub dry_run: bool,
    /// Only what was written since the last repack, into new packs; older objects stay where
    /// they are (a full repack later stores their older versions as deltas).
    pub incremental: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepackReport {
    pub repos: usize,
    pub changesets: usize,
    /// Objects found, and how many of them have a newer version to be stored against.
    pub objects: usize,
    pub with_base: usize,
    pub repack: RepackStats,
    pub prune: PruneStats,
}

impl RepackReport {
    pub fn lines(&self, dry_run: bool) -> Vec<String> {
        let mut out = vec![format!(
            "{} repositories, {} changesets: {} objects, {} with a newer version to store against",
            self.repos, self.changesets, self.objects, self.with_base
        )];
        if dry_run {
            return out;
        }
        let r = &self.repack;
        out.push(format!(
            "repacked {} objects ({} as deltas) into {} packs of {} bytes; {} too large to pack, {} missing",
            r.packed, r.deltas, r.packs, r.bytes, r.large, r.missing
        ));
        let p = &self.prune;
        out.push(format!(
            "removed {} superseded packs and {} loose objects ({} bytes)",
            p.packs_removed, p.loose_removed, p.bytes
        ));
        out
    }
}

/// For an incremental repack: the objects of `new` in order, walking only the commits among them
/// (newest first, with their parents' trees to pair versions); objects of `new` they do not
/// reach follow without a base. A tree that is not new has no new objects below it.
pub async fn plan_new(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repos: &[String],
    new: &HashSet<ObjectId>,
) -> anyhow::Result<(Vec<RepackItem>, usize)> {
    let mut walk = Walk {
        only: Some(new.clone()),
        ..Walk::default()
    };
    let mut changesets = 0;
    for repo in repos {
        let mut records: Vec<_> = scion_query::all_records(meta, repo, 4096)
            .await?
            .into_iter()
            .filter(|r| new.contains(&r.id))
            .collect();
        records.sort_by(|a, b| b.generation.cmp(&a.generation).then(a.id.cmp(&b.id)));
        for rec in &records {
            changesets += 1;
            walk.emit(rec.id);
            let mut parents = Vec::new();
            for p in &rec.parents {
                if let Some(r) = meta.get_changeset(repo, p).await? {
                    parents.push(r.root_tree);
                }
            }
            walk.trees(blobs, rec.root_tree, parents.first().copied()).await?;
            for root in parents.iter().skip(1) {
                walk.trees(blobs, rec.root_tree, Some(*root)).await?;
            }
        }
    }
    let mut rest: Vec<ObjectId> = new.iter().filter(|id| !walk.emitted.contains(id)).copied().collect();
    rest.sort();
    for id in rest {
        walk.emit(id);
    }
    Ok((walk.items, changesets))
}

/// The objects to repack, in order: each repository's commits newest first, each object with the
/// next newer version of the same path as its base.
pub async fn plan(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repos: &[String],
) -> anyhow::Result<(Vec<RepackItem>, usize)> {
    let mut walk = Walk::default();
    let mut changesets = 0;
    for repo in repos {
        let mut records = scion_query::all_records(meta, repo, 4096).await?;
        records.sort_by(|a, b| b.generation.cmp(&a.generation).then(a.id.cmp(&b.id)));
        let roots: HashMap<ObjectId, ObjectId> = records.iter().map(|r| (r.id, r.root_tree)).collect();
        for rec in &records {
            changesets += 1;
            walk.emit(rec.id);
            let parent = rec.first_parent().and_then(|p| roots.get(&p).copied());
            walk.trees(blobs, rec.root_tree, parent).await?;
            // A merge's other parents: their versions are stored against the merge's too, or
            // every merged branch's last versions would have nothing newer to pair with
            for p in rec.parents.iter().skip(1) {
                if let Some(&root) = roots.get(p) {
                    walk.trees(blobs, rec.root_tree, Some(root)).await?;
                }
            }
        }
    }
    Ok((walk.items, changesets))
}

#[derive(Default)]
struct Walk {
    items: Vec<RepackItem>,
    emitted: HashSet<ObjectId>,
    /// Older version → the newer version it is stored against.
    base: HashMap<ObjectId, ObjectId>,
    /// Tree pairs compared already. A tree reached again (a merge has the same tree as its
    /// branch's last commit) is still compared with each new parent version.
    compared: HashSet<(ObjectId, Option<ObjectId>)>,
    /// Incremental: only these objects are emitted, and trees outside them are not entered.
    only: Option<HashSet<ObjectId>>,
}

impl Walk {
    fn emit(&mut self, id: ObjectId) -> bool {
        if self.only.as_ref().is_some_and(|o| !o.contains(&id)) || !self.emitted.insert(id) {
            return false;
        }
        self.items.push(RepackItem {
            id,
            base: self.base.get(&id).copied(),
        });
        true
    }

    /// `older` will be stored against `newer` (already emitted), unless it was emitted first.
    fn pair(&mut self, older: ObjectId, newer: ObjectId) {
        if older != newer && !self.emitted.contains(&older) {
            self.base.entry(older).or_insert(newer);
        }
    }

    /// Emits the objects of `new` that `old` does not have, pairing changed entries.
    async fn trees(&mut self, blobs: &dyn BlobStore, new: ObjectId, old: Option<ObjectId>) -> anyhow::Result<()> {
        let mut stack = vec![(new, old)];
        while let Some((new, old)) = stack.pop() {
            if Some(new) == old {
                continue;
            }
            if !self.compared.insert((new, old)) {
                continue;
            }
            if let Some(o) = old {
                self.pair(o, new);
            }
            // Below a tree that is not new, nothing is
            if self.only.as_ref().is_some_and(|o| !o.contains(&new)) {
                continue;
            }
            if !self.emit(new) && old.is_none() {
                continue;
            }
            let new_tree = load(blobs, &new).await?;
            let old_tree = match old {
                Some(o) => load(blobs, &o).await?,
                None => Tree::default(),
            };
            let before: HashMap<&str, _> = old_tree.entries().iter().map(|e| (e.name.as_str(), e)).collect();
            for e in new_tree.entries() {
                let prev = before.get(e.name.as_str()).filter(|p| p.id != e.id);
                let same_kind =
                    |p: &&&scion_object::TreeEntry| (p.mode == EntryMode::Tree) == (e.mode == EntryMode::Tree);
                let prev = prev.filter(same_kind).map(|p| p.id);
                if e.mode == EntryMode::Tree {
                    stack.push((e.id, prev));
                } else {
                    if let Some(p) = prev {
                        self.pair(p, e.id);
                    }
                    self.emit(e.id);
                }
            }
        }
        Ok(())
    }
}

async fn load(blobs: &dyn BlobStore, id: &ObjectId) -> anyhow::Result<Tree> {
    let bytes = blobs
        .get(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("tree {id} is missing"))?;
    Ok(Tree::decode(&bytes)?)
}

pub async fn repack(
    meta: &Arc<dyn MetaStore>,
    objects: &ObjectBlobStore,
    opts: &RepackOptions,
) -> anyhow::Result<RepackReport> {
    let repos = if opts.repos.is_empty() {
        meta.list_repos().await?
    } else {
        opts.repos.clone()
    };
    let started = std::time::SystemTime::now();
    let last = objects.last_repack().await?;
    let (items, changesets) = if opts.incremental {
        let new: HashSet<ObjectId> = objects.new_objects(last.as_ref()).await?.into_iter().collect();
        plan_new(meta.as_ref(), objects, &repos, &new).await?
    } else {
        plan(meta.as_ref(), objects, &repos).await?
    };
    let mut report = RepackReport {
        repos: repos.len(),
        changesets,
        objects: items.len(),
        with_base: items.iter().filter(|i| i.base.is_some()).count(),
        ..Default::default()
    };
    if opts.dry_run {
        return Ok(report);
    }
    report.repack = objects.repack(&items).await?;
    report.prune = objects.prune(opts.prune_after).await?;
    if !opts.incremental {
        meta.compact_after_repack().await?;
    }
    // What the next repack measures new writes from
    let full = if opts.incremental {
        last.and_then(|m| m.full)
    } else {
        Some(started)
    };
    objects
        .mark_repacked(&scion_store_blob::RepackMarker {
            started,
            full,
            packs: report.repack.written.iter().copied().collect(),
        })
        .await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsck::tests::fixture;

    #[tokio::test]
    async fn walk_pairs_versions_newest_first() {
        let fx = fixture().await;
        let (items, changesets) = plan(fx.meta.as_ref(), fx.blobs.as_ref(), &["r".to_owned()])
            .await
            .expect("plan");
        assert!(changesets >= 2);
        let pos: HashMap<ObjectId, usize> = items.iter().enumerate().map(|(i, it)| (it.id, i)).collect();
        assert_eq!(pos.len(), items.len(), "each object once");
        assert!(items.iter().any(|i| i.base.is_some()), "some versions pair up");
        for (i, it) in items.iter().enumerate() {
            if let Some(b) = it.base {
                assert!(pos[&b] < i, "a base comes before the objects stored against it");
            }
        }
    }

    /// A merged branch's last tree is stored against the merge's, not left whole.
    #[tokio::test]
    async fn merged_branch_versions_pair_with_the_merge() {
        use bytes::Bytes;
        use scion_object::{Changeset, FileChange, Signature};
        use scion_store::{IndexedObject, MemStore};
        let store = Arc::new(MemStore::new());
        store.create_repo("r").await.expect("repo");
        let writer = scion_land::ChangesetWriter::new(store.clone(), store.clone());
        let sig = Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 1,
            tz_offset_minutes: 0,
        };
        let mut n = 0u8;
        let mut commit = async |parents: Vec<ObjectId>, files: &[(&str, &str)]| {
            n += 1;
            let mut delta = Vec::new();
            for &(path, content) in files {
                let id = ObjectId::of_blob(content.as_bytes());
                let data = Bytes::copy_from_slice(content.as_bytes());
                store.put(id, data).await.expect("put");
                let obj = IndexedObject {
                    id,
                    size: content.len() as u64,
                };
                store.index_objects("r", &[obj]).await.expect("index");
                delta.push(FileChange::put(path, EntryMode::File, id, content.len() as u64));
            }
            let cs = Changeset::new(
                parents,
                sig.clone(),
                sig.clone(),
                format!("c{n}"),
                [n; 16],
                delta,
                Default::default(),
            )
            .expect("changeset");
            writer.create("r", &cs).await.expect("create")
        };
        let a = commit(vec![], &[("a/x", "1"), ("b/y", "1")]).await;
        let branch = commit(vec![a.id], &[("a/x", "2")]).await;
        let main = commit(vec![a.id], &[("b/y", "2")]).await;
        let merge = commit(vec![main.id, branch.id], &[("a/x", "2")]).await;
        let (items, _) = plan(store.as_ref(), store.as_ref(), &["r".to_owned()])
            .await
            .expect("plan");
        let base: HashMap<ObjectId, Option<ObjectId>> = items.iter().map(|i| (i.id, i.base)).collect();
        assert_eq!(base[&merge.root_tree], None, "the newest version is whole");
        assert_eq!(base[&branch.root_tree], Some(merge.root_tree));
        assert_eq!(base[&main.root_tree], Some(merge.root_tree));
    }

    /// Commits versions `ns` of a few files on top of `parent`, each a 300-line file with line n
    /// changed.
    async fn add_versions(
        store: &scion_store::MemStore,
        blobs: &Arc<dyn BlobStore>,
        writer: &scion_land::ChangesetWriter,
        parent: &mut Vec<ObjectId>,
        versions: &mut Vec<(ObjectId, bytes::Bytes)>,
        ns: std::ops::Range<u8>,
    ) {
        use bytes::Bytes;
        use scion_object::{Changeset, FileChange, Signature};
        use scion_store::IndexedObject;
        let sig = Signature {
            name: "Ada".into(),
            email: "ada@example.com".into(),
            time_ms: 1,
            tz_offset_minutes: 0,
        };
        for n in ns {
            let body: String = (0..300)
                .map(|i| format!("line {i} {}\n", if i == usize::from(n) { n } else { 0 }))
                .collect();
            let data = Bytes::from(body);
            let id = ObjectId::of_blob(&data);
            blobs.put(id, data.clone()).await.expect("put");
            store
                .index_objects(
                    "r",
                    &[IndexedObject {
                        id,
                        size: data.len() as u64,
                    }],
                )
                .await
                .expect("index");
            versions.push((id, data.clone()));
            let path = format!("src/dir{}/file.txt", n % 3);
            let delta = vec![FileChange::put(&path, EntryMode::File, id, data.len() as u64)];
            let cs = Changeset::new(
                parent.clone(),
                sig.clone(),
                sig.clone(),
                format!("c{n}"),
                [n; 16],
                delta,
                Default::default(),
            )
            .expect("changeset");
            *parent = vec![writer.create("r", &cs).await.expect("create").id];
        }
    }

    /// Many versions of a few files, committed into a packed local store, then repacked.
    #[tokio::test]
    async fn repack_end_to_end_keeps_every_version_readable() {
        use scion_store::MemStore;
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(MemStore::new());
        let meta: Arc<dyn MetaStore> = store.clone();
        let objects = Arc::new(
            ObjectBlobStore::local(dir.path())
                .expect("open")
                .with_packs(meta.clone()),
        );
        let blobs: Arc<dyn BlobStore> = objects.clone();
        store.create_repo("r").await.expect("repo");
        let writer = scion_land::ChangesetWriter::new(meta.clone(), blobs.clone());
        let mut parent = Vec::new();
        let mut versions = Vec::new();
        add_versions(&store, &blobs, &writer, &mut parent, &mut versions, 0..30).await;
        store
            .cas_ref("r", "main", scion_store::RefExpect::Absent, parent[0])
            .await
            .expect("ref");
        let opts = RepackOptions {
            repos: vec![],
            prune_after: Duration::ZERO,
            dry_run: false,
            incremental: false,
        };
        assert_eq!(objects.last_repack().await.expect("marker"), None);
        let report = repack(&meta, &objects, &opts).await.expect("repack");
        assert!(objects.last_repack().await.expect("marker").is_some());
        assert_eq!(report.changesets, 30);
        assert!(report.repack.deltas >= 25, "the file versions: {report:?}");
        assert!(
            report.prune.loose_removed >= 30 && report.prune.packs_removed >= 30,
            "{report:?}"
        );
        assert_eq!(report.lines(false).len(), 3);
        let fresh = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(meta.clone());
        for (id, data) in &versions {
            assert_eq!(fresh.get(id).await.expect("read").as_ref(), Some(data));
        }
        let fresh: Arc<dyn BlobStore> = Arc::new(fresh);
        let check = crate::fsck::fsck(
            &meta,
            &fresh,
            &crate::fsck::FsckOptions {
                repos: vec![],
                verify: true,
            },
        )
        .await
        .expect("fsck");
        assert!(check.ok(), "{:?}", check.lines());
        // Incremental: only what was written since, in new packs; its own packs do not count again
        let first = objects.last_repack().await.expect("marker").expect("written");
        assert_eq!(objects.written_since(Some(&first)).await.expect("since"), 0);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        add_versions(&store, &blobs, &writer, &mut parent, &mut versions, 30..40).await;
        store
            .cas_ref("r", "main", scion_store::RefExpect::Version(1), parent[0])
            .await
            .expect("ref");
        assert!(objects.written_since(Some(&first)).await.expect("since") > 0);
        let inc = RepackOptions {
            incremental: true,
            ..opts.clone()
        };
        let report = repack(&meta, &objects, &inc).await.expect("incremental");
        // 10 commits, their files, and 3 trees each
        assert_eq!((report.changesets, report.objects), (10, 50), "{report:?}");
        assert!(report.repack.deltas >= 5, "versions among the new ones: {report:?}");
        let second = objects.last_repack().await.expect("marker").expect("written");
        assert_eq!(second.full, first.full, "the last full repack stays the first");
        assert!(second.started > first.started);
        assert_eq!(objects.written_since(Some(&second)).await.expect("since"), 0);
        let fresh = ObjectBlobStore::local(dir.path())
            .expect("open")
            .with_packs(meta.clone());
        for (id, data) in &versions {
            assert_eq!(fresh.get(id).await.expect("read").as_ref(), Some(data));
        }
        let check = crate::fsck::fsck(
            &meta,
            &(Arc::new(fresh) as Arc<dyn BlobStore>),
            &crate::fsck::FsckOptions {
                repos: vec![],
                verify: true,
            },
        )
        .await
        .expect("fsck");
        assert!(check.ok(), "{:?}", check.lines());
        // Nothing new: nothing to do
        let report = repack(&meta, &objects, &inc).await.expect("incremental");
        assert_eq!(report.objects, 0);
        // Unknown repository
        let missing = RepackOptions {
            repos: vec!["nope".into()],
            ..opts
        };
        assert!(repack(&meta, &objects, &missing).await.is_err());
    }
}
