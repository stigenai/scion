//! 创建提交（服务端设计文档 §6.1）。
//!
//! 分成三步：校验（只对客户端提交）、构建（建树、编码、算出记录，不写存储）、写入（批量）。
//! 合入批处理先在内存中构建整批 rebase 结果，最后一次写入，见 [`crate::Coordinator`]。

use std::sync::Arc;

use bytes::Bytes;
use scion_object::{Changeset, FileOp, ObjectId, ObjectKind};
use scion_store::{BlobStore, ChangesetRecord, IndexedObject, MetaStore, PathChange, fp_skip_step};
use scion_tree::{BlobTreeSource, BuildResult, apply_delta};

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
        if !self.meta.repo_exists(repo).await? {
            return Err(LandError::RepoNotFound(repo.to_owned()));
        }
        let pending = Pending::new(self.blobs.clone());
        let mut base = None;
        let mut missing_parents = Vec::new();
        for (i, p) in cs.parents.iter().enumerate() {
            match self.meta.get_changeset(repo, p).await? {
                Some(rec) if i == 0 => base = Some(rec.root_tree),
                Some(_) => {}
                None => missing_parents.push(*p),
            }
        }
        if !missing_parents.is_empty() {
            return Err(LandError::ObjectsNotUploaded(missing_parents));
        }
        self.check_blobs(repo, cs).await?;
        let built = apply_delta(&BlobTreeSource(self.blobs.clone()), base, &cs.delta).await?;
        let b = self.build(repo, &pending, cs, built, Vec::new()).await?;
        let rec = b.record.clone();
        self.persist(repo, &[b]).await?;
        Ok(rec)
    }

    /// 引用的文件必须已上传，声明的大小必须与实际一致（目录树里的 size 来自客户端声明）。
    async fn check_blobs(&self, repo: &str, cs: &Changeset) -> Result<()> {
        let puts: Vec<(&str, ObjectId, u64)> = cs
            .delta
            .iter()
            .filter_map(|c| match &c.op {
                FileOp::Put { id, size, .. } => Some((c.path.as_str(), *id, *size)),
                FileOp::Delete => None,
            })
            .collect();
        let ids: Vec<ObjectId> = puts.iter().map(|(_, id, _)| *id).collect();
        let sizes = self.meta.object_sizes(repo, &ids).await?;
        let mut missing: Vec<ObjectId> = ids.iter().filter(|id| !sizes.contains_key(id)).copied().collect();
        missing.dedup();
        if !missing.is_empty() {
            return Err(LandError::ObjectsNotUploaded(missing));
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
        let objects: Vec<(ObjectId, Bytes)> = built.iter().flat_map(|b| b.objects.iter().cloned()).collect();
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
        for b in built {
            if scion_query::git_sha_of(&b.changeset).is_some() {
                scion_query::index_git_sha(self.meta.as_ref(), repo, b.record.id, &b.changeset).await?;
            }
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
