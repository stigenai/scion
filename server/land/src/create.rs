//! 创建提交（服务端设计文档 §6.1）。
//!
//! 分成三步：校验（只对客户端提交）、构建（建树、编码、算出记录，不写存储）、写入（批量）。
//! 合入批处理先在内存中构建整批 rebase 结果，最后一次写入，见 [`crate::Coordinator`]。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use scion_object::{Changeset, FileOp, ObjectId, ObjectKind};
use scion_store::{BlobStore, ChangesetRecord, IndexedObject, MetaStore, PathChange, fp_skip_step};
use scion_tree::{BuildResult, apply_delta};

use crate::pending::Pending;
use crate::{LandError, Result};

/// 构建好、尚未写入的提交。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Built {
    pub record: ChangesetRecord,
    pub changeset: Changeset,
    /// 需要写入的对象：新目录树、提交对象，以及（rebase 时）合并产生的文件内容。
    pub objects: Vec<(ObjectId, Bytes)>,
    /// PathHistory 索引条目。
    pub paths: Vec<PathChange>,
}

/// 校验并创建提交：父提交必须已存在，引用的文件必须已上传，delta 必须能应用到第一个父提交上。
pub struct ChangesetWriter {
    meta: Arc<dyn MetaStore>,
    blobs: Arc<dyn BlobStore>,
}

impl ChangesetWriter {
    pub fn new(meta: Arc<dyn MetaStore>, blobs: Arc<dyn BlobStore>) -> Self {
        Self { meta, blobs }
    }

    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta
    }

    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    /// 创建客户端提交的 changeset。
    pub async fn create(&self, repo: &str, cs: &Changeset) -> Result<ChangesetRecord> {
        let mut built = self.create_many(repo, &[], std::slice::from_ref(cs)).await?;
        Ok(built.remove(0).record)
    }

    /// Creates the changesets `css` (parents first) and writes them in one batch with the
    /// `files` they bring. A parent can be an earlier changeset of the batch. The store calls
    /// do not depend on the batch size, except one record read for each parent that is not in
    /// the batch. The result has one [`Built`] for each changeset, with the trees it made.
    pub async fn create_many(&self, repo: &str, files: &[(ObjectId, Bytes)], css: &[Changeset]) -> Result<Vec<Built>> {
        if !self.meta.repo_exists(repo).await? {
            return Err(LandError::RepoNotFound(repo.to_owned()));
        }
        self.check_blobs(repo, files, css).await?;
        let meta = self.meta.as_ref();
        let mut pending = Pending::new(self.blobs.clone());
        let mut built = Vec::with_capacity(css.len());
        for cs in css {
            let mut base = None;
            let mut missing_parents = Vec::new();
            for (i, p) in cs.parents.iter().enumerate() {
                match pending.find_record(meta, repo, *p).await? {
                    Some(rec) if i == 0 => base = Some(rec.root_tree),
                    Some(_) => {}
                    None => missing_parents.push(*p),
                }
            }
            if !missing_parents.is_empty() {
                return Err(LandError::ObjectsNotUploaded(missing_parents));
            }
            let tree = apply_delta(&pending, base, &cs.delta).await?;
            // Later changesets of the batch build on these trees before they are written.
            for (id, data) in &tree.new_trees {
                pending.add_object(*id, data.clone());
            }
            let b = self.build(repo, &pending, cs, tree, Vec::new()).await?;
            pending.add_record(b.record.clone());
            built.push(b);
        }
        self.write(repo, files, &built).await?;
        Ok(built)
    }

    /// 引用的文件必须已上传，声明的大小必须与实际一致（目录树里的 size 来自客户端声明）。
    /// A file in `files` is not written yet, so its size comes from memory, not the index.
    async fn check_blobs(&self, repo: &str, files: &[(ObjectId, Bytes)], css: &[Changeset]) -> Result<()> {
        let puts: Vec<(&str, ObjectId, u64)> = css
            .iter()
            .flat_map(|cs| &cs.delta)
            .filter_map(|c| match &c.op {
                FileOp::Put { id, size, .. } => Some((c.path.as_str(), *id, *size)),
                FileOp::Delete => None,
            })
            .collect();
        let mut sizes: HashMap<ObjectId, u64> = files.iter().map(|(id, b)| (*id, b.len() as u64)).collect();
        let mut seen = HashSet::new();
        let stored: Vec<ObjectId> = puts
            .iter()
            .map(|(_, id, _)| *id)
            .filter(|id| !sizes.contains_key(id) && seen.insert(*id))
            .collect();
        if !stored.is_empty() {
            let found = self.meta.object_sizes(repo, &stored).await?;
            let missing: Vec<ObjectId> = stored.iter().filter(|id| !found.contains_key(id)).copied().collect();
            if !missing.is_empty() {
                return Err(LandError::ObjectsNotUploaded(missing));
            }
            sizes.extend(found);
        }
        for (path, id, declared) in &puts {
            let actual = sizes[id];
            if actual != *declared {
                return Err(LandError::InvalidChangeset {
                    path: (*path).to_owned(),
                    message: format!("{path:?} declares {declared} bytes but {id} is {actual} bytes"),
                });
            }
        }
        Ok(())
    }

    /// 构建提交但不写入：编码、算代数与跳表、整理索引条目。父提交可以是批内尚未写入的提交。
    /// `tree` 是 delta 应用到第一个父提交上的结果，`extra` 是额外需要写入的对象（合并产生的内容）。
    pub(crate) async fn build(
        &self,
        repo: &str,
        pending: &Pending,
        cs: &Changeset,
        tree: BuildResult,
        extra: Vec<(ObjectId, Bytes)>,
    ) -> Result<Built> {
        let meta = self.meta.as_ref();
        let mut generation = 0u64;
        let mut first = None;
        for (i, p) in cs.parents.iter().enumerate() {
            let rec = pending.record(meta, repo, *p).await?;
            generation = generation.max(rec.generation);
            if i == 0 {
                first = Some(rec);
            }
        }
        let (fp_depth, fp_jump) = match first {
            None => (1, None),
            Some(p) if p.fp_depth == 0 => (0, None),
            Some(p) => {
                let j = pending.record(meta, repo, p.jump()).await?;
                let jj = pending.record(meta, repo, j.jump()).await?;
                fp_skip_step(&p, &j, &jj)
            }
        };
        let cs_bytes = Bytes::from(cs.encode());
        let cs_id = ObjectId::hash(ObjectKind::Changeset, &cs_bytes);
        let mut objects = extra;
        objects.extend(tree.new_trees);
        objects.push((cs_id, cs_bytes));
        Ok(Built {
            record: ChangesetRecord {
                id: cs_id,
                root_tree: tree.root,
                parents: cs.parents.clone(),
                generation: generation.saturating_add(1),
                fp_depth,
                fp_jump,
            },
            changeset: cs.clone(),
            objects,
            paths: cs
                .delta
                .iter()
                .map(|c| PathChange {
                    path: c.path.clone(),
                    deleted: matches!(c.op, FileOp::Delete),
                })
                .collect(),
        })
    }

    /// 批量写入构建好的提交（按顺序，父提交在前）。写入顺序保证不变式：对象字节 → 对象索引 →
    /// 路径索引 → 提交记录 → git sha 映射；记录存在即表示它引用的一切都已就绪。
    pub async fn persist(&self, repo: &str, built: &[Built]) -> Result<()> {
        self.write(repo, &[], built).await
    }

    /// [`ChangesetWriter::persist`] with extra `files` that go in the same object write.
    async fn write(&self, repo: &str, files: &[(ObjectId, Bytes)], built: &[Built]) -> Result<()> {
        let objects: Vec<(ObjectId, Bytes)> = files
            .iter()
            .cloned()
            .chain(built.iter().flat_map(|b| b.objects.iter().cloned()))
            .collect();
        self.store(repo, &objects).await?;
        let entries: Vec<(ObjectId, u64, Vec<PathChange>)> = built
            .iter()
            .map(|b| (b.record.id, b.record.generation, b.paths.clone()))
            .collect();
        self.meta.index_paths_many(repo, &entries).await?;
        let records: Vec<ChangesetRecord> = built.iter().map(|b| b.record.clone()).collect();
        self.meta.put_changesets(repo, &records).await?;
        // The git sha map comes last: a push treats a mapped commit as stored, so the record must exist
        // first. A failure here leaves a record with no map, and the next push of that commit adds the map.
        let shas: Vec<(Vec<u8>, Vec<u8>)> = built
            .iter()
            .filter_map(|b| {
                Some((
                    scion_query::git_sha_of(&b.changeset)?.to_vec(),
                    b.record.id.to_bytes().to_vec(),
                ))
            })
            .collect();
        if !shas.is_empty() {
            self.meta
                .put_derived_many(repo, scion_query::GITSHA_KIND, scion_query::GITSHA_VERSION, &shas)
                .await?;
        }
        Ok(())
    }

    /// 写入对象字节并登记到仓库索引。顺序：先字节、后索引（索引里有的对象一定可读）。
    /// The objects are written as one unit, so a store with packs keeps them in one pack.
    pub async fn store(&self, repo: &str, objects: &[(ObjectId, Bytes)]) -> Result<()> {
        self.blobs.put_many(objects).await?;
        let index: Vec<_> = objects
            .iter()
            .map(|(id, b)| IndexedObject {
                id: *id,
                size: b.len() as u64,
            })
            .collect();
        Ok(self.meta.index_objects(repo, &index).await?)
    }
}
