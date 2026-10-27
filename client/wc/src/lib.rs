//! Scion 工作副本（客户端设计文档 §5）。
//!
//! 检出（可以是稀疏检出，见 [`Sparse`]）、status、commit、land、sync（含客户端 rebase）。
//! 操作日志在后续里程碑加入。
//!
//! 工作副本只依赖公开协议：所有数据通过 [`scion_client::Client`] 取得。

mod error;
mod fsutil;
mod merge;
mod oplog;
mod sparse;
mod state;
mod workspace;

use std::collections::BTreeMap;
use std::path::Path;

use bytes::Bytes;
use scion_client::{ChangesetBuilder, Client, CreatedChangeset, DirEntry, Landed, Signature};
use scion_object::{EntryMode, ObjectId, Tree};

pub use error::{Error, Result};
pub use merge::{ConflictKind, MergeConflict};
pub use oplog::{Operation, SavedChange};
pub use sparse::Sparse;
pub use state::{
    ConflictState, DirState, FileRecord, Mode, Pending, Pick, PickKind, Sides, StashEntry, State, Version,
};
pub use workspace::{AddedWorkspace, WorkspaceInfo};

use crate::fsutil::Found;
use crate::state::Paths;

/// 本地改动。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Status {
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
    /// Paths with unresolved conflicts (they also appear as added/modified/deleted).
    pub conflicted: Vec<String>,
}

impl Status {
    pub fn is_clean(&self) -> bool {
        self.added.is_empty() && self.modified.is_empty() && self.deleted.is_empty()
    }
}

/// 一个本地改动文件：`old` 为当前快照中的版本（新增时为 None），`new` 为本地内容（删除时为 None）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalChange {
    pub path: String,
    pub old: Option<(Mode, ObjectId)>,
    pub new: Option<(Mode, Bytes)>,
}

/// sync 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    UpToDate,
    Updated {
        from: Option<ObjectId>,
        to: ObjectId,
        written: usize,
        removed: usize,
    },
    /// 未合入的提交被重新基于 `onto` 创建为 `head`，并把上游改动更新到了本地。
    Rebased {
        onto: ObjectId,
        head: ObjectId,
        written: usize,
        removed: usize,
    },
}

/// 一个待提交的改动（内部使用）。
enum Change {
    Put {
        path: String,
        mode: Mode,
        content: Bytes,
        found_mtime: u64,
    },
    Delete {
        path: String,
    },
}

impl Change {
    fn path(&self) -> &str {
        match self {
            Change::Put { path, .. } | Change::Delete { path } => path,
        }
    }
}

/// 目录树中的一个文件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TreeFile {
    mode: Mode,
    id: ObjectId,
}

/// One path's result of a three-way merge.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Final {
    /// A version that exists on the server (None: no file).
    Version(Option<TreeFile>),
    /// Line-merged content, not on the server yet.
    Data(TreeFile, Bytes),
    Conflict {
        kind: ConflictKind,
        /// Text with conflict markers, for content conflicts.
        marked: Option<Bytes>,
        ours: Option<TreeFile>,
        theirs: Option<TreeFile>,
    },
}

/// Three-way merges `paths` of `ours` and `theirs` against `base` (see [`merge`] for the rules),
/// fetching file contents only for paths both sides changed.
async fn three_way(
    client: &Client,
    paths: impl IntoIterator<Item = String>,
    base: &BTreeMap<String, TreeFile>,
    ours: &BTreeMap<String, TreeFile>,
    theirs: &BTreeMap<String, TreeFile>,
) -> Result<BTreeMap<String, Final>> {
    use merge::{Resolution, Step};
    let mut out = BTreeMap::new();
    let mut lines = Vec::new();
    for path in paths {
        let (b, o, t) = (
            base.get(&path).copied(),
            ours.get(&path).copied(),
            theirs.get(&path).copied(),
        );
        let f = match merge::step(b, o, t) {
            Step::Same => Final::Version(o),
            Step::Done(Resolution::Take(x)) => Final::Version(x),
            Step::Conflict(kind) => Final::Conflict {
                kind,
                marked: None,
                ours: o,
                theirs: t,
            },
            Step::Lines { mode } => {
                lines.push((path, mode, [b, o, t]));
                continue;
            }
        };
        out.insert(path, f);
    }
    for part in lines.chunks(FETCH_BATCH / 3) {
        let ids: Vec<ObjectId> = part
            .iter()
            .flat_map(|(_, _, files)| files.iter().flatten().map(|f| f.id))
            .collect();
        let mut data = client.get_objects(&ids).await?.into_iter();
        for (path, mode, [b, o, t]) in part {
            // Added on both sides: no base, so an empty one
            let bd = if b.is_some() { data.next() } else { Some(Bytes::new()) };
            let (Some(bd), Some(od), Some(td)) = (bd, data.next(), data.next()) else {
                return Err(Error::Corrupt(format!("no content fetched for {path}")));
            };
            let f = match merge::merge_lines(&bd, &od, &td) {
                Ok(merged) => {
                    let merged = Bytes::from(merged);
                    let file = TreeFile {
                        mode: *mode,
                        id: ObjectId::of_blob(&merged),
                    };
                    // Theirs brought nothing ours lacks
                    if Some(file) == *o {
                        Final::Version(*o)
                    } else {
                        Final::Data(file, merged)
                    }
                }
                Err((kind, marked)) => Final::Conflict {
                    kind: if b.is_none() && kind == ConflictKind::Content {
                        ConflictKind::AddAdd
                    } else {
                        kind
                    },
                    marked: marked.map(Bytes::from),
                    ours: *o,
                    theirs: *t,
                },
            };
            out.insert(path.clone(), f);
        }
    }
    // A file on one side where the other has a directory
    let mut result: std::collections::BTreeSet<String> = ours.keys().cloned().collect();
    for (path, f) in &out {
        match f {
            Final::Version(None) => {
                result.remove(path);
            }
            Final::Conflict { ours: None, .. } => {}
            _ => {
                result.insert(path.clone());
            }
        }
    }
    for path in merge::path_clashes(&result) {
        out.insert(
            path.clone(),
            Final::Conflict {
                kind: ConflictKind::Path,
                marked: None,
                ours: ours.get(&path).copied(),
                theirs: theirs.get(&path).copied(),
            },
        );
    }
    Ok(out)
}

fn version(f: Option<TreeFile>) -> Option<Version> {
    f.map(|f| Version {
        mode: f.mode,
        id: f.id.to_string(),
    })
}

/// The conflicted paths of a merge result, as stored in [`ConflictState::paths`].
fn sides_of(finals: &BTreeMap<String, Final>) -> BTreeMap<String, Sides> {
    finals
        .iter()
        .filter_map(|(p, f)| match f {
            Final::Conflict { kind, ours, theirs, .. } => Some((
                p.clone(),
                Sides {
                    kind: kind.as_str().to_owned(),
                    ours: version(*ours),
                    theirs: version(*theirs),
                },
            )),
            _ => None,
        })
        .collect()
}

/// Which side `scn resolve` takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Ours,
    Theirs,
}

/// Result of [`WorkingCopy::merge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    /// The other branch has nothing this one lacks.
    UpToDate,
    /// A merge commit is pending (`scn land` publishes it); local files were updated.
    Merged {
        head: ObjectId,
        written: usize,
        removed: usize,
    },
}

/// Result of [`WorkingCopy::pick`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickOutcome {
    /// The current snapshot already is what the revert or cherry-pick would make.
    Empty,
    /// A commit is pending (`scn land` publishes it); local files were updated.
    Picked {
        head: ObjectId,
        written: usize,
        removed: usize,
    },
}

/// Result of [`WorkingCopy::stash_apply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StashApplied {
    pub entry: StashEntry,
    /// Whether the entry was dropped: when asked to, and only without conflicts.
    pub dropped: bool,
    /// Paths that conflicted: text ones hold conflict markers, the others the current version.
    pub conflicts: Vec<String>,
}

/// Result of [`WorkingCopy::switch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Switched {
    /// The new ref's tip; None for a ref on an empty repository.
    pub tip: Option<ObjectId>,
    pub written: usize,
    pub removed: usize,
}

/// Result of [`WorkingCopy::restore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restored {
    /// Files put back to the snapshot version.
    pub restored: Vec<String>,
    /// Added files under the given paths, left untouched (restore never deletes files).
    pub kept_added: Vec<String>,
}

pub struct WorkingCopy {
    paths: Paths,
    state: State,
}

/// How far a clone has got, reported as it goes (see [`WorkingCopy::clone_with_progress`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// Files found so far while reading the directory trees.
    Listing { files: usize },
    /// Files written so far, of `total`, and the bytes downloaded.
    Checkout { done: usize, total: usize, bytes: u64 },
}

/// One file of a commit made by [`WorkingCopy::commit_with_stat`]: its mode before (None: added)
/// and after (None: deleted), and for counting changed lines the old version's id and size and
/// the new content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStat {
    pub path: String,
    pub old_mode: Option<Mode>,
    pub new_mode: Option<Mode>,
    pub old: Option<(ObjectId, u64)>,
    pub new_content: Option<Bytes>,
}

/// Receives [`Progress`] reports.
pub type OnProgress<'a> = &'a (dyn Fn(Progress) + Sync);

/// 每次批量读取的对象数（SDK 还会按服务端上限再分批）。
const FETCH_BATCH: usize = 256;

/// 递归展开目录树，得到稀疏范围内的 `路径 → 文件`。按层批量读取目录树；范围外的子树不读取。
async fn flatten(client: &Client, root: ObjectId, sparse: &Sparse) -> Result<BTreeMap<String, TreeFile>> {
    flatten_with(client, root, sparse, &|_| {}).await
}

async fn flatten_with(
    client: &Client,
    root: ObjectId,
    sparse: &Sparse,
    progress: OnProgress<'_>,
) -> Result<BTreeMap<String, TreeFile>> {
    let mut out = BTreeMap::new();
    let mut level = vec![(String::new(), root)];
    while !level.is_empty() {
        let mut next = Vec::new();
        for part in level.chunks(FETCH_BATCH) {
            let ids: Vec<ObjectId> = part.iter().map(|(_, id)| *id).collect();
            let trees = client.get_objects(&ids).await?;
            for ((prefix, id), bytes) in part.iter().zip(trees) {
                let tree = Tree::decode(&bytes).map_err(|e| Error::Corrupt(format!("tree {id}: {e}")))?;
                for e in tree.entries() {
                    let path = if prefix.is_empty() {
                        e.name.clone()
                    } else {
                        format!("{prefix}/{}", e.name)
                    };
                    match Mode::from_entry(e.mode) {
                        Some(mode) if sparse.includes_file(&path) => {
                            out.insert(path, TreeFile { mode, id: e.id });
                        }
                        None if sparse.descends(&path) => next.push((path, e.id)),
                        _ => {}
                    }
                }
            }
            progress(Progress::Listing { files: out.len() });
        }
        level = next;
    }
    Ok(out)
}

/// 同时在途的批量读取数（检出时下载与写文件重叠）。
const FETCHES_IN_FLIGHT: usize = 8;

/// 检出：批量下载（多批并发）并在后台线程并行写入新文件，返回每个文件的 dirstate 记录。
async fn checkout_files(
    client: &Client,
    root: &Path,
    files: &BTreeMap<String, TreeFile>,
    progress: OnProgress<'_>,
) -> Result<Vec<(String, FileRecord)>> {
    use futures::StreamExt;
    fsutil::create_parents(root, files.keys().map(String::as_str))?;
    let list: Vec<(String, TreeFile)> = files.iter().map(|(p, f)| (p.clone(), *f)).collect();
    let mut fetches = futures::stream::iter(list.chunks(FETCH_BATCH).map(<[_]>::to_vec))
        .map(|part| async move {
            let ids: Vec<ObjectId> = part.iter().map(|(_, f)| f.id).collect();
            client.get_objects(&ids).await.map(|data| (part, data))
        })
        .buffered(FETCHES_IN_FLIGHT);
    let mut writes = tokio::task::JoinSet::new();
    let mut out = Vec::with_capacity(files.len());
    let mut bytes = 0;
    let report = |done: usize, bytes: u64| {
        progress(Progress::Checkout {
            done,
            total: files.len(),
            bytes,
        })
    };
    while let Some(batch) = fetches.next().await {
        let (part, data) = batch?;
        bytes += data.iter().map(|d| d.len() as u64).sum::<u64>();
        report(out.len(), bytes);
        let root = root.to_path_buf();
        writes.spawn_blocking(move || -> Result<Vec<(String, FileRecord)>> {
            part.into_iter()
                .zip(data)
                .map(|((path, f), d)| {
                    let mtime_ns = fsutil::create_file(&root, &path, f.mode, &d)?;
                    Ok((path, record(f.mode, f.id, d.len() as u64, mtime_ns)))
                })
                .collect()
        });
        // 写入跟不上下载时等一等，限制内存中的批数
        while writes.len() >= FETCHES_IN_FLIGHT {
            if let Some(done) = writes.join_next().await {
                out.extend(done.map_err(|e| Error::Corrupt(format!("checkout task: {e}")))??);
                report(out.len(), bytes);
            }
        }
    }
    while let Some(done) = writes.join_next().await {
        out.extend(done.map_err(|e| Error::Corrupt(format!("checkout task: {e}")))??);
        report(out.len(), bytes);
    }
    Ok(out)
}

impl WorkingCopy {
    /// 把 `repo` 的 `reference` 检出到 `dir`（目录必须不存在或为空），只检出 `sparse` 范围内的文件。
    /// ref 不存在时得到空的工作副本。
    pub async fn clone(
        client: &Client,
        endpoint: &str,
        repo: &str,
        reference: &str,
        dir: &Path,
        sparse: Sparse,
    ) -> Result<Self> {
        Self::clone_with_progress(client, endpoint, repo, reference, dir, sparse, &|_| {}).await
    }

    /// [`WorkingCopy::clone`], reporting [`Progress`] as it goes.
    pub async fn clone_with_progress(
        client: &Client,
        endpoint: &str,
        repo: &str,
        reference: &str,
        dir: &Path,
        sparse: Sparse,
        progress: OnProgress<'_>,
    ) -> Result<Self> {
        if dir.exists() && std::fs::read_dir(dir).map_err(error::io(dir))?.next().is_some() {
            return Err(Error::TargetNotEmpty(dir.to_path_buf()));
        }
        let paths = Paths::new(dir);
        std::fs::create_dir_all(paths.meta_dir()).map_err(error::io(paths.meta_dir()))?;
        let _lock = paths.lock()?;
        let mut state = State::new(endpoint, repo, reference);
        state.sparse = sparse.list().map(<[String]>::to_vec);
        let mut dirstate = DirState::default();
        if let Some(tip) = client.resolve_ref(reference).await? {
            let files = flatten_with(client, tip.root_tree, &sparse, progress).await?;
            dirstate
                .files
                .extend(checkout_files(client, dir, &files, progress).await?);
            state.base = Some(tip.changeset.to_string());
        }
        paths.save_dirstate(&mut dirstate)?;
        paths.save_state(&state)?;
        Ok(Self { paths, state })
    }

    /// 打开包含 `start` 的工作副本（向上查找 `.scion/`）。
    pub fn open(start: &Path) -> Result<Self> {
        let paths = Paths::discover(start)?;
        let state = paths.load_state()?;
        Ok(Self { paths, state })
    }

    pub fn root(&self) -> &Path {
        &self.paths.root
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    /// Points the working copy at another server address or repository (`scn remote set-url`).
    /// The caller checks that the new one has the commits the working copy is on.
    pub fn set_remote(&mut self, endpoint: &str, repo: &str) -> Result<()> {
        let _lock = self.paths.lock()?;
        self.state.endpoint = endpoint.to_owned();
        self.state.repo = repo.to_owned();
        self.paths.save_state(&self.state)
    }

    /// 稀疏检出的范围。
    pub fn sparse(&self) -> Result<Sparse> {
        Sparse::from_state(self.state.sparse.as_deref())
    }

    /// 改变稀疏检出的范围：检出新加入的目录，删除移出范围的文件。要求没有未提交的改动。
    /// 返回 (写入数, 删除数)。
    ///
    /// 旧范围外的未跟踪文件按新范围判断：新范围仍不包含的报 [`Error::OutsideSparse`]；新范围包含的，
    /// 与当前快照中同一路径的版本相同则直接接管，不同则报 [`Error::WouldOverwrite`]（不覆盖本地文件），
    /// 快照中没有的保留为新增文件。
    pub async fn set_sparse(&mut self, client: &Client, sparse: Sparse) -> Result<(usize, usize)> {
        let _lock = self.paths.lock()?;
        let old = self.sparse()?;
        let mut dirstate = self.paths.load_dirstate()?;
        let (untracked, found): (BTreeMap<String, Found>, BTreeMap<String, Found>) =
            fsutil::scan(self.root(), &dirstate.files)?
                .into_iter()
                .partition(|(p, _)| !old.includes_file(p) && !dirstate.files.contains_key(p));
        let stray: Vec<String> = untracked.keys().filter(|p| !sparse.includes_file(p)).cloned().collect();
        if !stray.is_empty() {
            return Err(Error::OutsideSparse(stray));
        }
        if !self.changes_of(&dirstate, &found)?.0.is_clean() {
            return Err(Error::Dirty);
        }
        let mut counts = (0, 0);
        if let Some(current) = self.state.current()? {
            // 检出的是当前快照（有未合入的提交时为它），范围内外的文件版本一致，差异只有增删
            let target = flatten(client, client.root_tree(&current.to_string()).await?, &sparse).await?;
            let mut differ = Vec::new();
            for (path, f) in &untracked {
                let Some(t) = target.get(path) else { continue };
                let content = fsutil::read_content(self.root(), path, f.mode)?;
                if ObjectId::of_blob(&content) == t.id && f.mode == t.mode {
                    dirstate
                        .files
                        .insert(path.clone(), record(t.mode, t.id, content.len() as u64, f.mtime_ns));
                } else {
                    differ.push(path.clone());
                }
            }
            if !differ.is_empty() {
                return Err(Error::WouldOverwrite(differ));
            }
            let delta = diff(&snapshot(&dirstate)?, &target);
            counts = self.apply(client, &mut dirstate, &delta).await?;
            self.paths.save_dirstate(&mut dirstate)?;
        }
        self.state.sparse = sparse.list().map(<[String]>::to_vec);
        self.paths.save_state(&self.state)?;
        Ok(counts)
    }

    /// 计算相对当前快照的改动，同时返回提交所需的内容。
    fn changes(&self) -> Result<(Status, Vec<Change>)> {
        let dirstate = self.paths.load_dirstate()?;
        let found = fsutil::scan(self.root(), &dirstate.files)?;
        let sparse = self.sparse()?;
        let outside: Vec<String> = found
            .keys()
            .filter(|p| !sparse.includes_file(p) && !dirstate.files.contains_key(*p))
            .cloned()
            .collect();
        if !outside.is_empty() {
            return Err(Error::OutsideSparse(outside));
        }
        self.changes_of(&dirstate, &found)
    }

    /// 扫描结果相对 dirstate 的改动。
    fn changes_of(&self, dirstate: &DirState, found: &BTreeMap<String, Found>) -> Result<(Status, Vec<Change>)> {
        let mut status = Status::default();
        let mut changes = Vec::new();
        for (path, f) in found {
            match dirstate.files.get(path) {
                None => {
                    let content = fsutil::read_content(self.root(), path, f.mode)?;
                    status.added.push(path.clone());
                    changes.push(Change::Put {
                        path: path.clone(),
                        mode: f.mode,
                        content,
                        found_mtime: f.mtime_ns,
                    });
                }
                Some(rec) if unchanged_by_stat(rec, f, dirstate.written_at_ns) => {}
                Some(rec) => {
                    let content = fsutil::read_content(self.root(), path, f.mode)?;
                    if ObjectId::of_blob(&content) != rec.object_id()? || rec.mode != f.mode {
                        status.modified.push(path.clone());
                        changes.push(Change::Put {
                            path: path.clone(),
                            mode: f.mode,
                            content,
                            found_mtime: f.mtime_ns,
                        });
                    }
                }
            }
        }
        for path in dirstate.files.keys().filter(|p| !found.contains_key(*p)) {
            status.deleted.push(path.clone());
            changes.push(Change::Delete { path: path.clone() });
        }
        Ok((status, changes))
    }

    pub fn status(&self) -> Result<Status> {
        let mut status = self.changes()?.0;
        if let Some(c) = &self.state.conflict {
            status.conflicted = c.paths.keys().cloned().collect();
        }
        Ok(status)
    }

    /// 本地改动的明细（供 diff 使用）：每个改动文件在当前快照中的版本，以及本地的新内容。
    pub fn local_changes(&self) -> Result<Vec<LocalChange>> {
        let (_, changes) = self.changes()?;
        let dirstate = self.paths.load_dirstate()?;
        let old_of = |path: &str| -> Result<Option<(Mode, ObjectId)>> {
            dirstate
                .files
                .get(path)
                .map(|r| Ok((r.mode, r.object_id()?)))
                .transpose()
        };
        changes
            .into_iter()
            .map(|c| match c {
                Change::Put {
                    path, mode, content, ..
                } => Ok(LocalChange {
                    old: old_of(&path)?,
                    new: Some((mode, content)),
                    path,
                }),
                Change::Delete { path } => Ok(LocalChange {
                    old: old_of(&path)?,
                    new: None,
                    path,
                }),
            })
            .collect()
    }

    /// Removes untracked files at or under `paths` (`.` for everything), and with `ignored_too`
    /// ignored ones as well, like `git clean -fd` (`-x`); returns what it removed, or with
    /// `dry_run` what it would remove, directories that go as a whole ending in `/`. Tracked files,
    /// `.scion/`, `.git/` and nested repositories or working copies are never touched.
    pub fn clean(&self, paths: &[String], ignored_too: bool, dry_run: bool) -> Result<Vec<String>> {
        let _lock = self.paths.lock()?;
        let dirstate = self.paths.load_dirstate()?;
        let gone = fsutil::untracked(self.root(), &dirstate.files, paths, ignored_too)?;
        if !dry_run {
            for p in &gone {
                match p.strip_suffix('/') {
                    Some(dir) => fsutil::remove_dir(self.root(), dir)?,
                    None => fsutil::remove_file(self.root(), p)?,
                }
            }
        }
        Ok(gone)
    }

    /// `scn stash`: saves every local change as a commit on the server, on no branch, with
    /// `message` (default: `WIP on BRANCH: ID SUBJECT`, as git does), and puts the working tree
    /// back to the current commit, added files removed. Returns None when there is nothing to save.
    pub async fn stash_push(
        &mut self,
        client: &Client,
        message: Option<&str>,
        author: Option<(&str, &str)>,
    ) -> Result<Option<StashEntry>> {
        let _lock = self.paths.lock()?;
        self.check_no_conflict()?;
        let (status, changes) = self.changes()?;
        if changes.is_empty() {
            return Ok(None);
        }
        let current = self
            .state
            .current()?
            .ok_or_else(|| Error::EmptyBranch(self.state.reference.clone()))?;
        let message = match message {
            Some(m) => m.to_owned(),
            None => {
                let subject = client
                    .log(&current.to_string(), None, 1, None)
                    .await?
                    .entries
                    .into_iter()
                    .next()
                    .map(|e| e.message.lines().next().unwrap_or_default().to_owned())
                    .unwrap_or_default();
                let id = current.to_string();
                format!("WIP on {}: {} {subject}", self.state.reference, &id[..id.len().min(12)])
            }
        };
        let mut b = client.changeset(Some(current)).message(&message);
        if let Some((name, email)) = author {
            b = b.author(name, email);
        }
        for c in &changes {
            b = match c {
                Change::Put {
                    path, mode, content, ..
                } => b.put_with_mode(path, mode.to_entry(), content.clone()),
                Change::Delete { path } => b.delete(path),
            };
        }
        let created = b.create().await?;
        let entry = StashEntry {
            commit: created.id.to_string(),
            message,
        };
        // Saved before the files go, so a crash loses nothing
        self.state.stash.insert(0, entry.clone());
        self.paths.save_state(&self.state)?;
        for p in &status.added {
            fsutil::remove_file(self.root(), p)?;
        }
        self.restore_locked(client, &[".".to_owned()]).await?;
        Ok(Some(entry))
    }

    /// `scn stash apply` / `pop` (`drop`): merges stash entry `index` (0 is the newest) into the
    /// working tree as local changes, three-way against the commit it was made on, so it also
    /// applies after a sync or on another branch. Needs a clean tree. On conflicts the files get
    /// markers and the entry is kept.
    pub async fn stash_apply(&mut self, client: &Client, index: usize, drop: bool) -> Result<StashApplied> {
        let _lock = self.paths.lock()?;
        self.check_no_conflict()?;
        let entry = self.state.stash.get(index).cloned().ok_or(Error::NoStash(index))?;
        if !self.changes()?.0.is_clean() {
            return Err(Error::Dirty);
        }
        let current = self
            .state
            .current()?
            .ok_or_else(|| Error::EmptyBranch(self.state.reference.clone()))?;
        let stashed: ObjectId = entry
            .commit
            .parse()
            .map_err(|e| Error::Corrupt(format!("stash commit: {e}")))?;
        let parent = client
            .log(&entry.commit, None, 1, None)
            .await?
            .entries
            .into_iter()
            .next()
            .and_then(|e| e.parents.first().copied());
        let (b, o, t, paths) = three_sides(client, parent, current, stashed).await?;
        let changes: BTreeMap<String, Final> = three_way(client, paths, &b, &o, &t)
            .await?
            .into_iter()
            .filter(|(p, f)| *f != Final::Version(o.get(p).copied()))
            .collect();
        let sparse = self.sparse()?;
        let outside: Vec<String> = changes.keys().filter(|p| !sparse.includes_file(p)).cloned().collect();
        if !outside.is_empty() {
            return Err(Error::PickOutsideSparse(outside));
        }
        let conflicts: Vec<String> = sides_of(&changes).into_keys().collect();
        self.write_results(client, &changes, &o).await?;
        let dropped = drop && conflicts.is_empty();
        if dropped {
            self.state.stash.remove(index);
            self.paths.save_state(&self.state)?;
        }
        Ok(StashApplied {
            entry,
            dropped,
            conflicts,
        })
    }

    /// `scn stash drop`: forgets stash entry `index` (0 is the newest).
    pub fn stash_drop(&mut self, index: usize) -> Result<StashEntry> {
        let _lock = self.paths.lock()?;
        if index >= self.state.stash.len() {
            return Err(Error::NoStash(index));
        }
        let entry = self.state.stash.remove(index);
        self.paths.save_state(&self.state)?;
        Ok(entry)
    }

    /// 把本地改动创建为提交（尚未合入）。没有改动时返回 `None`。
    pub async fn commit(
        &mut self,
        client: &Client,
        message: &str,
        author: Option<(&str, &str)>,
    ) -> Result<Option<CreatedChangeset>> {
        Ok(self.commit_with_stat(client, message, author).await?.map(|(c, _)| c))
    }

    /// [`WorkingCopy::commit`], also returning the files it changed.
    pub async fn commit_with_stat(
        &mut self,
        client: &Client,
        message: &str,
        author: Option<(&str, &str)>,
    ) -> Result<Option<(CreatedChangeset, Vec<FileStat>)>> {
        let _lock = self.paths.lock()?;
        self.commit_locked(client, message, author, None, &[]).await
    }

    /// Like [`WorkingCopy::commit_with_stat`], but commits only the changes at or under `paths`
    /// (`git commit PATH...`); the others stay local changes. Every path must have a change,
    /// and finishing a sync, merge, revert or cherry-pick takes every change, so it refuses then.
    pub async fn commit_paths_with_stat(
        &mut self,
        client: &Client,
        message: &str,
        author: Option<(&str, &str)>,
        paths: &[String],
    ) -> Result<Option<(CreatedChangeset, Vec<FileStat>)>> {
        let _lock = self.paths.lock()?;
        if self.state.conflict.is_some() {
            return Err(Error::PartialFinish);
        }
        self.commit_locked(client, message, author, None, paths).await
    }

    async fn commit_locked(
        &mut self,
        client: &Client,
        message: &str,
        author: Option<(&str, &str)>,
        merge: Option<ObjectId>,
        only: &[String],
    ) -> Result<Option<(CreatedChangeset, Vec<FileStat>)>> {
        self.check_attached()?;
        // Finishing a sync or merge that stopped on conflicts: its message, author and merge
        // parent are the defaults
        let conflict = self.state.conflict.clone();
        if let Some(c) = &conflict {
            if !c.paths.is_empty() {
                return Err(Error::Unresolved(c.paths.keys().cloned().collect()));
            }
        }
        if merge.is_none() && self.state.head_merge.is_some() {
            return Err(Error::OnPendingMerge);
        }
        let (_, mut changes) = self.changes()?;
        if !only.is_empty() {
            let unchanged: Vec<String> = only
                .iter()
                .filter(|p| {
                    !changes
                        .iter()
                        .any(|c| fsutil::selects(std::slice::from_ref(p), c.path()))
                })
                .cloned()
                .collect();
            if !unchanged.is_empty() {
                return Err(Error::NoChangesIn(unchanged));
            }
            changes.retain(|c| fsutil::selects(only, c.path()));
        }
        if changes.is_empty() {
            if conflict.is_some() {
                self.state.conflict = None;
                self.paths.save_state(&self.state)?;
                // A sync that stopped on a commit now left out goes on with the rest
                if let Some(c) = conflict {
                    self.continue_sync(client, c.rest, c.before).await?;
                }
            }
            return Ok(None);
        }
        let saved_message = conflict.as_ref().and_then(|c| c.message.clone()).unwrap_or_default();
        let message = if message.is_empty() {
            saved_message.as_str()
        } else {
            message
        };
        let saved_author = conflict.as_ref().and_then(|c| c.author.clone());
        let author = author.or(saved_author.as_ref().map(|(n, e)| (n.as_str(), e.as_str())));
        let merge = match merge {
            Some(m) => Some(m),
            None => conflict
                .as_ref()
                .and_then(|c| c.merge.as_deref())
                .map(|m| {
                    m.parse::<ObjectId>()
                        .map_err(|e| Error::Corrupt(format!("conflict merge: {e}")))
                })
                .transpose()?,
        };
        let mut b = client.changeset(self.state.current()?).message(message);
        if let Some(m) = merge {
            b = b.parent(m);
        }
        if let Some((name, email)) = author {
            b = b.author(name, email);
        }
        for c in &changes {
            b = match c {
                Change::Put {
                    path, mode, content, ..
                } => b.put_with_mode(path, mode.to_entry(), content.clone()),
                Change::Delete { path } => b.delete(path),
            };
        }
        let created = b.create().await?;

        let mut dirstate = self.paths.load_dirstate()?;
        let stat = changes
            .iter()
            .map(|c| {
                let (path, new_mode, new_content) = match c {
                    Change::Put {
                        path, mode, content, ..
                    } => (path, Some(*mode), Some(content.clone())),
                    Change::Delete { path } => (path, None, None),
                };
                let old = dirstate.files.get(path);
                FileStat {
                    path: path.clone(),
                    old_mode: old.map(|r| r.mode),
                    new_mode,
                    old: old.and_then(|r| Some((r.id.parse().ok()?, r.size))),
                    new_content,
                }
            })
            .collect();
        for c in changes {
            match c {
                Change::Put {
                    path,
                    mode,
                    content,
                    found_mtime,
                } => {
                    let id = ObjectId::of_blob(&content);
                    dirstate
                        .files
                        .insert(path, record(mode, id, content.len() as u64, found_mtime));
                }
                Change::Delete { path } => {
                    dirstate.files.remove(&path);
                }
            }
        }
        // 先写 dirstate 再写 state：崩溃时最多「已提交的改动在本地显示为未提交」，重新提交得到同一个 id
        self.paths.save_dirstate(&mut dirstate)?;
        self.push_head(created.id);
        self.state.head_message = Some(message.to_owned());
        self.state.head_author = author.map(|(n, e)| (n.to_owned(), e.to_owned()));
        self.state.head_merge = merge.map(|m| m.to_string());
        self.state.conflict = None;
        self.paths.save_state(&self.state)?;
        if let Some(c) = conflict {
            self.continue_sync(client, c.rest, c.before).await?;
        }
        Ok(Some((created, stat)))
    }

    /// Makes `id` the newest pending commit, on top of the others.
    fn push_head(&mut self, id: ObjectId) {
        if let Some(h) = self.state.head.replace(id.to_string()) {
            self.state.stack.push(h);
        }
    }

    /// After the commit that finished a sync stopped on conflicts: rebases the pending
    /// commits it had not got to yet onto it. `before` is where the sync started, for
    /// `scn sync --abort` should it stop again.
    async fn continue_sync(&mut self, client: &Client, rest: Vec<String>, before: Option<Box<Pending>>) -> Result<()> {
        if rest.is_empty() {
            return Ok(());
        }
        let rest = rest
            .iter()
            .map(|s| s.parse().map_err(|e| Error::Corrupt(format!("conflict rest: {e}"))))
            .collect::<Result<Vec<ObjectId>>>()?;
        let (Some(base), Some(at)) = (self.state.base.clone(), self.state.current()?) else {
            return Err(Error::Corrupt("sync to continue has no base".into()));
        };
        let base = base.parse().map_err(|e| Error::Corrupt(format!("base: {e}")))?;
        let made = self.state.pending()?;
        let before = before.map_or_else(|| self.state.pending_state(), |b| *b);
        self.replay(client, rest, base, at, made, at, before).await.map(|_| ())
    }

    /// Replaces the pending commit with one that also includes the current local changes, with
    /// `message` (default: the old message) and `author` (default: the old author). Returns None
    /// when the result has no changes, which drops the pending commit.
    pub async fn amend(
        &mut self,
        client: &Client,
        message: Option<&str>,
        author: Option<(&str, &str)>,
    ) -> Result<Option<CreatedChangeset>> {
        Ok(self.amend_with_stat(client, message, author).await?.map(|(c, _)| c))
    }

    /// [`WorkingCopy::amend`], also returning the files the new commit changes.
    pub async fn amend_with_stat(
        &mut self,
        client: &Client,
        message: Option<&str>,
        author: Option<(&str, &str)>,
    ) -> Result<Option<(CreatedChangeset, Vec<FileStat>)>> {
        let _lock = self.paths.lock()?;
        let old_message = self.state.head_message.clone().unwrap_or_default();
        let old_author = self.state.head_author.clone();
        let old_merge = self.head_merge()?;
        self.uncommit_locked(client).await?;
        let author = author.or(old_author.as_ref().map(|(n, e)| (n.as_str(), e.as_str())));
        self.commit_locked(client, message.unwrap_or(&old_message), author, old_merge, &[])
            .await
    }

    /// Drops the pending commit and keeps its changes in the working tree as uncommitted changes.
    /// Returns the dropped commit.
    pub async fn uncommit(&mut self, client: &Client) -> Result<ObjectId> {
        let _lock = self.paths.lock()?;
        self.uncommit_locked(client).await
    }

    async fn uncommit_locked(&mut self, client: &Client) -> Result<ObjectId> {
        let head = match &self.state.head {
            Some(h) => h
                .parse::<ObjectId>()
                .map_err(|e| Error::Corrupt(format!("head: {e}")))?,
            None => return Err(Error::NoPendingCommit),
        };
        let parent = self.state.stack.last().or(self.state.base.as_ref()).cloned();
        let sparse = self.sparse()?;
        // The snapshot goes back to the parent; the files on disk stay as they are. Records of
        // changed paths get mtime 0 so the next status re-hashes them and reports them as modified.
        let mut dirstate = self.paths.load_dirstate()?;
        for c in client.diff(parent.as_deref(), &head.to_string(), None).await? {
            if !sparse.includes_file(&c.path) {
                continue;
            }
            match tree_file(&c.old) {
                Some(f) => {
                    dirstate.files.insert(c.path, record(f.mode, f.id, 0, 0));
                }
                None => {
                    dirstate.files.remove(&c.path);
                }
            }
        }
        // dirstate first: a crash in between leaves the changes visible as uncommitted on top of
        // the old head, never hidden
        self.paths.save_dirstate(&mut dirstate)?;
        match self.state.stack.pop() {
            Some(next) => {
                // The commit under it becomes the newest; amend reuses its message and author
                let e = client
                    .log(&next, None, 1, None)
                    .await?
                    .entries
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::Corrupt(format!("pending commit {next} has no history")))?;
                self.state.head = Some(next);
                self.state.head_message = Some(e.message);
                self.state.head_author = Some((e.author.name, e.author.email));
                self.state.head_merge = None;
            }
            None => self.clear_head(),
        }
        self.paths.save_state(&self.state)?;
        Ok(head)
    }

    /// Discards uncommitted changes to tracked files at or under `paths` (`.` or an empty path
    /// means everything): modified and deleted files go back to the current snapshot. Added
    /// (untracked) files are left alone and returned in [`Restored::kept_added`].
    pub async fn restore(&mut self, client: &Client, paths: &[String]) -> Result<Restored> {
        let _lock = self.paths.lock()?;
        self.restore_locked(client, paths).await
    }

    /// `restore --source REV`: makes the files under `paths` what they are in the commit whose
    /// root tree is `root`, as local changes (the working copy stays on its commit); tracked files
    /// under them that `root` lacks are deleted. Added files are left alone, as in
    /// [`WorkingCopy::restore`].
    pub async fn restore_from(&mut self, client: &Client, root: ObjectId, paths: &[String]) -> Result<Restored> {
        let _lock = self.paths.lock()?;
        let matches = |p: &str| fsutil::selects(paths, p);
        let (status, _) = self.changes()?;
        let changed = |p: &String| status.modified.contains(p) || status.deleted.contains(p);
        let dirstate = self.paths.load_dirstate()?;
        let target = flatten(client, root, &self.sparse()?).await?;
        let mut delta = BTreeMap::new();
        for (path, f) in target.iter().filter(|(p, _)| matches(p)) {
            let same = dirstate
                .files
                .get(path)
                .is_some_and(|r| r.mode == f.mode && r.id == f.id.to_string());
            // A changed or untracked file may already hold that version
            let on_disk =
                || fsutil::read_content(self.root(), path, f.mode).is_ok_and(|b| ObjectId::of_blob(&b) == f.id);
            let up_to_date = if dirstate.files.contains_key(path) && !changed(path) {
                same
            } else {
                on_disk()
            };
            if !up_to_date {
                delta.insert(path.clone(), Some(*f));
            }
        }
        for path in dirstate.files.keys().filter(|p| matches(p) && !target.contains_key(*p)) {
            if !status.deleted.contains(path) {
                delta.insert(path.clone(), None);
            }
        }
        // Written through a copy of the dirstate that is not saved, so they show as local changes
        self.apply(client, &mut dirstate.clone(), &delta).await?;
        Ok(Restored {
            restored: delta.into_keys().collect(),
            kept_added: status.added.into_iter().filter(|p| matches(p)).collect(),
        })
    }

    async fn restore_locked(&mut self, client: &Client, paths: &[String]) -> Result<Restored> {
        let matches = |p: &str| fsutil::selects(paths, p);
        let (status, _) = self.changes()?;
        let mut dirstate = self.paths.load_dirstate()?;
        let mut delta = BTreeMap::new();
        for path in status.modified.iter().chain(&status.deleted).filter(|p| matches(p)) {
            let rec = dirstate
                .files
                .get(path)
                .ok_or_else(|| Error::Corrupt(format!("{path} changed but is not tracked")))?;
            delta.insert(
                path.clone(),
                Some(TreeFile {
                    mode: rec.mode,
                    id: rec.object_id()?,
                }),
            );
        }
        self.apply(client, &mut dirstate, &delta).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        Ok(Restored {
            restored: delta.into_keys().collect(),
            kept_added: status.added.into_iter().filter(|p| matches(p)).collect(),
        })
    }

    /// 合入已提交的改动。ref 已前进时由服务端 rebase，随后把上游改动更新到工作区。
    ///
    /// 有未提交的改动时只允许快进：rebase 后要把上游改动写入工作区，不能和本地未提交的改动交织。
    pub async fn land(&mut self, client: &Client) -> Result<Landed> {
        let _lock = self.paths.lock()?;
        self.check_attached()?;
        let head = match &self.state.head {
            Some(h) => h
                .parse::<ObjectId>()
                .map_err(|e| Error::Corrupt(format!("head: {e}")))?,
            None => return Err(Error::NothingToLand),
        };
        // The server lands a merge commit only as a fast-forward (it does not rebase merges)
        if self.state.head_merge.is_some() {
            let tip = client
                .resolve_ref(&self.state.reference)
                .await?
                .map(|t| t.changeset.to_string());
            if tip != self.state.base {
                return Err(Error::MergeBehind(self.state.reference.clone()));
            }
        }
        let landed = if self.changes()?.0.is_clean() {
            client.land(&self.state.reference, &head).await?
        } else {
            client.land_fast_forward(&self.state.reference, &head).await?
        };
        if landed.tip != head {
            // ref 上包含了别人的改动（rebase 或提交早已在历史中）：工作区从 head 更新到新的 tip
            let sparse = self.sparse()?;
            let from = flatten(client, client.root_tree(&head.to_string()).await?, &sparse).await?;
            let to = flatten(client, client.root_tree(&landed.tip.to_string()).await?, &sparse).await?;
            let mut dirstate = self.paths.load_dirstate()?;
            self.apply(client, &mut dirstate, &diff(&from, &to)).await?;
            self.paths.save_dirstate(&mut dirstate)?;
        }
        self.state.base = Some(landed.tip.to_string());
        self.clear_head();
        self.paths.save_state(&self.state)?;
        Ok(landed)
    }

    /// Forgets every pending commit.
    fn clear_head(&mut self) {
        self.state.head = None;
        self.state.stack.clear();
        self.state.head_message = None;
        self.state.head_author = None;
        self.state.head_merge = None;
    }

    fn head_merge(&self) -> Result<Option<ObjectId>> {
        self.state
            .head_merge
            .as_deref()
            .map(|m| m.parse().map_err(|e| Error::Corrupt(format!("head_merge: {e}"))))
            .transpose()
    }

    /// Writes merge results that differ from `ours` (what is on disk) into the working tree:
    /// versions and merged text as they are, content conflicts with markers; other conflicts
    /// keep our file. Returns the paths it created.
    async fn write_results(
        &self,
        client: &Client,
        finals: &BTreeMap<String, Final>,
        ours: &BTreeMap<String, TreeFile>,
    ) -> Result<Vec<String>> {
        let fetch: Vec<(&String, TreeFile)> = finals
            .iter()
            .filter_map(|(p, f)| match f {
                Final::Version(Some(v)) if ours.get(p) != Some(v) => Some((p, *v)),
                _ => None,
            })
            .collect();
        for part in fetch.chunks(FETCH_BATCH) {
            let ids: Vec<ObjectId> = part.iter().map(|(_, f)| f.id).collect();
            for ((path, f), data) in part.iter().zip(client.get_objects(&ids).await?) {
                fsutil::write_file(self.root(), path, f.mode, &data)?;
            }
        }
        let mut created = Vec::new();
        for (path, f) in finals {
            match f {
                Final::Version(None) if ours.contains_key(path) => fsutil::remove_file(self.root(), path)?,
                Final::Data(v, data) => {
                    fsutil::write_file(self.root(), path, v.mode, data)?;
                }
                Final::Conflict {
                    marked: Some(data),
                    ours: o,
                    ..
                } => {
                    let mode = o.map_or(Mode::File, |o| o.mode);
                    fsutil::write_file(self.root(), path, mode, data)?;
                }
                _ => {}
            }
            let exists = !matches!(
                f,
                Final::Version(None)
                    | Final::Conflict {
                        ours: None,
                        marked: None,
                        ..
                    }
            );
            if exists && !ours.contains_key(path) {
                created.push(path.clone());
            }
        }
        Ok(created)
    }

    /// Fails while a sync or merge waits for its conflicts to be resolved and committed.
    fn check_no_conflict(&self) -> Result<()> {
        match &self.state.conflict {
            Some(c) if !c.paths.is_empty() => Err(Error::Unresolved(c.paths.keys().cloned().collect())),
            Some(_) => Err(Error::Dirty),
            None => Ok(()),
        }
    }

    /// Marks conflicted `paths` (`.` for all) resolved. With `side`, first replaces each file
    /// with that side's version (deleting it when that side has none); without, refuses files
    /// that still contain conflict markers unless `force`. Returns the paths still unresolved.
    pub async fn resolve(
        &mut self,
        client: &Client,
        paths: &[String],
        side: Option<Side>,
        force: bool,
    ) -> Result<Vec<String>> {
        let _lock = self.paths.lock()?;
        let Some(conflict) = &self.state.conflict else {
            return Err(Error::NoConflict);
        };
        let wanted: Vec<String> = if paths.iter().any(|p| p == ".") {
            conflict.paths.keys().cloned().collect()
        } else {
            for p in paths {
                if !conflict.paths.contains_key(p) {
                    return Err(Error::NotConflicted(p.clone()));
                }
            }
            paths.to_vec()
        };
        match side {
            Some(side) => {
                let mut writes = Vec::new();
                for p in &wanted {
                    let sides = &conflict.paths[p];
                    let v = match side {
                        Side::Ours => &sides.ours,
                        Side::Theirs => &sides.theirs,
                    };
                    match v {
                        Some(v) => writes.push((
                            p.clone(),
                            v.mode,
                            v.id.parse::<ObjectId>()
                                .map_err(|e| Error::Corrupt(format!("conflict {p}: {e}")))?,
                        )),
                        None => fsutil::remove_file(self.root(), p)?,
                    }
                }
                let ids: Vec<ObjectId> = writes.iter().map(|(_, _, id)| *id).collect();
                for ((p, mode, _), data) in writes.iter().zip(client.get_objects(&ids).await?) {
                    fsutil::write_file(self.root(), p, *mode, &data)?;
                }
            }
            None if !force => {
                let mut marked = Vec::new();
                for p in &wanted {
                    let path = self.root().join(p);
                    if let Ok(data) = std::fs::read(&path) {
                        if merge::has_markers(&data) {
                            marked.push(p.clone());
                        }
                    }
                }
                if !marked.is_empty() {
                    return Err(Error::MarkersRemain(marked));
                }
            }
            None => {}
        }
        let conflict = self.state.conflict.as_mut().ok_or(Error::NoConflict)?;
        for p in &wanted {
            conflict.paths.remove(p);
        }
        let remaining = conflict.paths.keys().cloned().collect();
        self.paths.save_state(&self.state)?;
        Ok(remaining)
    }

    /// Abandons a merge, revert or cherry-pick that stopped on conflicts: tracked files go back
    /// to the snapshot and files it created are removed.
    pub async fn abort_merge(&mut self, client: &Client) -> Result<()> {
        let _lock = self.paths.lock()?;
        let created = match &self.state.conflict {
            Some(c) if c.merge.is_some() || c.pick.is_some() => c.created.clone(),
            _ => return Err(Error::NoMergeInProgress),
        };
        for p in &created {
            fsutil::remove_file(self.root(), p)?;
        }
        self.restore_locked(client, &[".".to_owned()]).await?;
        self.state.conflict = None;
        self.paths.save_state(&self.state)?;
        Ok(())
    }

    /// Abandons a sync that stopped on conflicts: the working copy goes back to the commit it
    /// was on, with its pending commits as they were, and files the sync created are removed.
    /// The commits the sync had already rebased stay unused on the server.
    pub async fn abort_sync(&mut self, client: &Client) -> Result<()> {
        let _lock = self.paths.lock()?;
        let (before, created) = match &self.state.conflict {
            Some(ConflictState {
                before: Some(b),
                created,
                ..
            }) => ((**b).clone(), created.clone()),
            _ => return Err(Error::NoSyncInProgress),
        };
        for p in &created {
            fsutil::remove_file(self.root(), p)?;
        }
        self.restore_locked(client, &[".".to_owned()]).await?;
        let from = self.state.current()?;
        self.state.set_pending_state(before);
        if let (Some(from), Some(to)) = (from, self.state.current()?) {
            let mut dirstate = self.paths.load_dirstate()?;
            let delta = tree_delta(client, from, to, &self.sparse()?).await?;
            self.apply(client, &mut dirstate, &delta).await?;
            self.paths.save_dirstate(&mut dirstate)?;
        }
        self.state.conflict = None;
        self.paths.save_state(&self.state)?;
        Ok(())
    }

    /// Moves the working copy to another ref (creating it at the current snapshot when
    /// `create`). Needs no pending commit and no modified or deleted files; added files are
    /// kept unless the target has a file at the same path.
    pub async fn switch(&mut self, client: &Client, reference: &str, create: bool) -> Result<Switched> {
        let _lock = self.paths.lock()?;
        let added = self.movable()?;
        let tip = if create {
            let from = self
                .state
                .current()?
                .ok_or_else(|| Error::EmptyBranch(self.state.reference.clone()))?;
            client.create_ref(reference, &from).await?;
            Some(from)
        } else {
            match client.resolve_ref(reference).await? {
                Some(t) => Some(t.changeset),
                None => return Err(Error::RefNotFound(reference.to_owned())),
            }
        };
        let (written, removed) = self.move_files(client, tip, &added).await?;
        self.state.reference = reference.to_owned();
        self.state.base = tip.map(|t| t.to_string());
        self.state.detached = false;
        self.paths.save_state(&self.state)?;
        Ok(Switched { tip, written, removed })
    }

    /// `scn switch --detach REV`: puts the files of `commit` in the working tree to look at,
    /// build or test, on no branch: commands that make commits refuse until `scn switch BRANCH`
    /// (or `scn switch -c NAME`, which starts a branch there). Needs what [`WorkingCopy::switch`] does.
    pub async fn detach(&mut self, client: &Client, commit: ObjectId) -> Result<Switched> {
        let _lock = self.paths.lock()?;
        let added = self.movable()?;
        let (written, removed) = self.move_files(client, Some(commit), &added).await?;
        self.state.base = Some(commit.to_string());
        self.state.detached = true;
        self.paths.save_state(&self.state)?;
        Ok(Switched {
            tip: Some(commit),
            written,
            removed,
        })
    }

    /// Checks that the working copy can move to another commit: no conflict, no pending commit,
    /// no modified or deleted files. Returns the added files, which move along.
    fn movable(&self) -> Result<Vec<String>> {
        self.check_no_conflict()?;
        if self.state.head.is_some() {
            return Err(Error::PendingCommit);
        }
        let (status, _) = self.changes()?;
        if !status.modified.is_empty() || !status.deleted.is_empty() {
            return Err(Error::Dirty);
        }
        Ok(status.added)
    }

    /// Makes the tracked files those of `tip` (none for None), refusing when that would
    /// overwrite an added file. Returns (written, removed).
    async fn move_files(&self, client: &Client, tip: Option<ObjectId>, added: &[String]) -> Result<(usize, usize)> {
        let target = match tip {
            Some(t) => flatten(client, client.root_tree(&t.to_string()).await?, &self.sparse()?).await?,
            None => BTreeMap::new(),
        };
        let clash: Vec<String> = added.iter().filter(|p| target.contains_key(*p)).cloned().collect();
        if !clash.is_empty() {
            return Err(Error::WouldOverwrite(clash));
        }
        let mut dirstate = self.paths.load_dirstate()?;
        let delta = diff(&snapshot(&dirstate)?, &target);
        let moved = self.apply(client, &mut dirstate, &delta).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        Ok(moved)
    }

    /// Fails on a detached working copy, which has no branch to commit to.
    fn check_attached(&self) -> Result<()> {
        if self.state.detached {
            return Err(Error::Detached(self.state.base.clone().unwrap_or_default()));
        }
        Ok(())
    }

    /// Merges `other` (a ref) into the current branch as a pending merge commit with parents
    /// (current snapshot, other's tip), and updates local files. `scn land` publishes it.
    /// The merge covers the whole tree, also outside a sparse checkout. On conflicts the working
    /// tree gets the merged files, text conflicts with markers, for `scn resolve` and
    /// `scn commit`; when a sparse checkout leaves out a path the merge changes, it fails
    /// without touching anything, since the commit that finishes it is made from local files.
    pub async fn merge(
        &mut self,
        client: &Client,
        other: &str,
        message: Option<&str>,
        author: Option<(&str, &str)>,
    ) -> Result<MergeOutcome> {
        let _lock = self.paths.lock()?;
        self.check_attached()?;
        self.check_no_conflict()?;
        if self.state.head.is_some() {
            return Err(Error::PendingCommit);
        }
        if !self.changes()?.0.is_clean() {
            return Err(Error::Dirty);
        }
        let theirs = client
            .resolve_ref(other)
            .await?
            .ok_or_else(|| Error::RefNotFound(other.to_owned()))?
            .changeset;
        let message = message
            .map(str::to_owned)
            .unwrap_or_else(|| format!("Merge {other} into {}", self.state.reference));
        self.merge_locked(client, theirs, message, author).await
    }

    /// Merges commit `theirs` into the current snapshot (no pending commit, clean tree).
    async fn merge_locked(
        &mut self,
        client: &Client,
        theirs: ObjectId,
        message: String,
        author: Option<(&str, &str)>,
    ) -> Result<MergeOutcome> {
        let ours = self
            .state
            .current()?
            .ok_or_else(|| Error::EmptyBranch(self.state.reference.clone()))?;
        if theirs == ours || client.is_ancestor(&theirs.to_string(), &ours.to_string()).await? {
            return Ok(MergeOutcome::UpToDate);
        }
        let base = client
            .merge_bases(&ours.to_string(), &theirs.to_string())
            .await?
            .first()
            .copied();
        let full = Sparse::full();
        let o = flatten(client, client.root_tree(&ours.to_string()).await?, &full).await?;
        let t = flatten(client, client.root_tree(&theirs.to_string()).await?, &full).await?;
        let b = match base {
            Some(id) => flatten(client, client.root_tree(&id.to_string()).await?, &full).await?,
            None => BTreeMap::new(),
        };
        let paths: Vec<String> = merge::all_paths([&b, &o, &t]).into_iter().cloned().collect();
        let changes: BTreeMap<String, Final> = three_way(client, paths, &b, &o, &t)
            .await?
            .into_iter()
            .filter(|(p, f)| *f != Final::Version(o.get(p).copied()))
            .collect();
        let sparse = self.sparse()?;
        let sides = sides_of(&changes);
        if !sides.is_empty() {
            // The commit that finishes the merge is made from local files, so every change has
            // to be inside the checkout
            if changes.keys().any(|p| !sparse.includes_file(p)) {
                return Err(Error::MergeConflicts(
                    changes
                        .iter()
                        .filter_map(|(path, f)| match f {
                            Final::Conflict { kind, .. } => Some(MergeConflict {
                                path: path.clone(),
                                kind: *kind,
                            }),
                            _ => None,
                        })
                        .collect(),
                ));
            }
            let created = self.write_results(client, &changes, &o).await?;
            let paths: Vec<String> = sides.keys().cloned().collect();
            self.state.conflict = Some(ConflictState {
                paths: sides,
                message: Some(message),
                author: author.map(|(n, e)| (n.to_owned(), e.to_owned())),
                merge: Some(theirs.to_string()),
                created,
                pick: None,
                rest: Vec::new(),
                before: None,
            });
            self.paths.save_state(&self.state)?;
            return Err(Error::Conflict(paths));
        }

        let mut builder = client.changeset(Some(ours)).parent(theirs).message(message.clone());
        if let Some((name, email)) = author {
            builder = builder.author(name, email);
        }
        let (created, written, removed) = self.commit_results(client, builder, changes, &sparse).await?;
        self.state.head = Some(created.id.to_string());
        self.state.head_message = Some(message);
        self.state.head_author = author.map(|(n, e)| (n.to_owned(), e.to_owned()));
        self.state.head_merge = Some(theirs.to_string());
        self.paths.save_state(&self.state)?;
        Ok(MergeOutcome::Merged {
            head: created.id,
            written,
            removed,
        })
    }

    /// Makes the commit `builder` starts, with the merge results `changes` (no conflicts), and
    /// writes the ones inside the sparse checkout to the working tree. Returns the commit and
    /// the number of files written and removed.
    async fn commit_results(
        &self,
        client: &Client,
        builder: ChangesetBuilder<'_>,
        changes: BTreeMap<String, Final>,
        sparse: &Sparse,
    ) -> Result<(CreatedChangeset, usize, usize)> {
        let (builder, files) = add_results(client, builder, changes).await?;
        let created = builder.create().await?;
        let local: BTreeMap<String, Option<TreeFile>> =
            files.into_iter().filter(|(p, _)| sparse.includes_file(p)).collect();
        let mut dirstate = self.paths.load_dirstate()?;
        let (written, removed) = self.apply(client, &mut dirstate, &local).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        Ok((created, written, removed))
    }

    /// `scn revert` / `scn cherry-pick`: makes a pending commit on the current snapshot that
    /// undoes (`Revert`) or repeats (`CherryPick`) what `commit` changed relative to `parent`
    /// (None for a root commit), on top of any pending commits. Needs a clean tree, and the
    /// paths the commit changed inside the sparse checkout. Only those paths are merged, three-way, so
    /// later changes to them are kept (or conflict, like `merge`: the working tree then gets
    /// the results, text conflicts with markers, for `scn resolve` and `scn commit`).
    /// `author` and `committer` default as for any commit; a cherry-pick passes the original
    /// author, with its time, and who applies it as the committer.
    #[allow(clippy::too_many_arguments)]
    pub async fn pick(
        &mut self,
        client: &Client,
        kind: PickKind,
        commit: ObjectId,
        parent: Option<ObjectId>,
        message: String,
        author: Option<Signature>,
        committer: Option<Signature>,
    ) -> Result<PickOutcome> {
        let _lock = self.paths.lock()?;
        self.check_attached()?;
        self.check_no_conflict()?;
        if self.state.head_merge.is_some() {
            return Err(Error::OnPendingMerge);
        }
        if !self.changes()?.0.is_clean() {
            return Err(Error::Dirty);
        }
        let current = self
            .state
            .current()?
            .ok_or_else(|| Error::EmptyBranch(self.state.reference.clone()))?;
        let parent = parent.map(|p| p.to_string());
        let changed = client.diff(parent.as_deref(), &commit.to_string(), None).await?;
        let sparse = self.sparse()?;
        let outside: Vec<String> = changed
            .iter()
            .filter(|c| !sparse.includes_file(&c.path))
            .map(|c| c.path.clone())
            .collect();
        if !outside.is_empty() {
            return Err(Error::PickOutsideSparse(outside));
        }
        let file = tree_file;
        let (mut before, mut after) = (BTreeMap::new(), BTreeMap::new());
        for c in &changed {
            if let Some(f) = file(&c.old) {
                before.insert(c.path.clone(), f);
            }
            if let Some(f) = file(&c.new) {
                after.insert(c.path.clone(), f);
            }
        }
        let (base, theirs) = match kind {
            PickKind::Revert => (&after, &before),
            PickKind::CherryPick => (&before, &after),
        };
        let ours = snapshot(&self.paths.load_dirstate()?)?;
        let paths = changed.into_iter().map(|c| c.path);
        let changes: BTreeMap<String, Final> = three_way(client, paths, base, &ours, theirs)
            .await?
            .into_iter()
            .filter(|(p, f)| *f != Final::Version(ours.get(p).copied()))
            .collect();
        if changes.is_empty() {
            return Ok(PickOutcome::Empty);
        }
        let names = author.as_ref().map(|a| (a.name.clone(), a.email.clone()));
        let sides = sides_of(&changes);
        if !sides.is_empty() {
            let created = self.write_results(client, &changes, &ours).await?;
            let paths: Vec<String> = sides.keys().cloned().collect();
            self.state.conflict = Some(ConflictState {
                paths: sides,
                message: Some(message),
                author: names,
                merge: None,
                created,
                pick: Some(Pick {
                    kind,
                    commit: commit.to_string(),
                }),
                rest: Vec::new(),
                before: None,
            });
            self.paths.save_state(&self.state)?;
            return Err(Error::Conflict(paths));
        }
        let mut builder = client.changeset(Some(current)).message(message.clone());
        if let Some(a) = author {
            builder = builder.author_signature(a);
        }
        if let Some(c) = committer {
            builder = builder.committer_signature(c);
        }
        let (created, written, removed) = self.commit_results(client, builder, changes, &sparse).await?;
        self.push_head(created.id);
        self.state.head_message = Some(message);
        self.state.head_author = names;
        self.state.head_merge = None;
        self.paths.save_state(&self.state)?;
        Ok(PickOutcome::Picked {
            head: created.id,
            written,
            removed,
        })
    }

    /// 更新到 ref 的最新提交。要求本地没有未提交的改动。
    ///
    /// With a pending commit and a ref that moved, rebases the commit on the client: paths both
    /// sides changed get a three-way merge (line level for text, as on the server). Without
    /// conflicts the commit is made again on the new tip and the upstream changes are written
    /// locally. Otherwise the working tree gets the merged files, text conflicts with markers,
    /// as uncommitted changes on the new tip, and [`Error::Conflict`] lists the paths;
    /// `scn resolve` and `scn commit` finish it.
    pub async fn sync(&mut self, client: &Client) -> Result<SyncOutcome> {
        let _lock = self.paths.lock()?;
        self.check_attached()?;
        self.check_no_conflict()?;
        if !self.changes()?.0.is_clean() {
            return Err(Error::Dirty);
        }
        let Some(tip) = client.resolve_ref(&self.state.reference).await? else {
            return match self.state.base {
                None => Ok(SyncOutcome::UpToDate),
                Some(_) => Err(Error::RefNotFound(self.state.reference.clone())),
            };
        };
        if self.state.head.is_some() {
            if self.state.base.as_deref() == Some(tip.changeset.to_string().as_str()) {
                return Ok(SyncOutcome::UpToDate);
            }
            let before = self.state.pending_state();
            if let Some(theirs) = self.head_merge()? {
                return self
                    .redo_merge(client, tip.changeset, tip.root_tree, theirs, before)
                    .await;
            }
            let local = self.state.current()?.ok_or(Error::NothingToLand)?;
            let commits = self.state.pending()?;
            return self
                .replay(client, commits, tip.changeset, tip.changeset, Vec::new(), local, before)
                .await;
        }
        let from = self.state.current()?;
        if from == Some(tip.changeset) {
            return Ok(SyncOutcome::UpToDate);
        }
        let target = flatten(client, tip.root_tree, &self.sparse()?).await?;
        let mut dirstate = self.paths.load_dirstate()?;
        let delta = diff(&snapshot(&dirstate)?, &target);
        let (written, removed) = self.apply(client, &mut dirstate, &delta).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        self.state.base = Some(tip.changeset.to_string());
        self.paths.save_state(&self.state)?;
        Ok(SyncOutcome::Updated {
            from,
            to: tip.changeset,
            written,
            removed,
        })
    }

    /// 把一组改动写到工作区并同步 dirstate，返回 (写入数, 删除数)。
    async fn apply(
        &self,
        client: &Client,
        dirstate: &mut DirState,
        delta: &BTreeMap<String, Option<TreeFile>>,
    ) -> Result<(usize, usize)> {
        let (mut written, mut removed) = (0, 0);
        let entries: Vec<_> = delta.iter().collect();
        // 按原顺序处理（删除与写入的先后影响文件/目录互换），每段的文件内容一次批量读取
        for part in entries.chunks(FETCH_BATCH) {
            let ids: Vec<ObjectId> = part.iter().filter_map(|(_, c)| c.as_ref().map(|f| f.id)).collect();
            let mut contents = client.get_objects(&ids).await?.into_iter();
            for (path, change) in part {
                self.apply_one(
                    dirstate,
                    path,
                    change.as_ref(),
                    &mut contents,
                    &mut written,
                    &mut removed,
                )?;
            }
        }
        Ok((written, removed))
    }

    fn apply_one(
        &self,
        dirstate: &mut DirState,
        path: &str,
        change: Option<&TreeFile>,
        contents: &mut impl Iterator<Item = bytes::Bytes>,
        written: &mut usize,
        removed: &mut usize,
    ) -> Result<()> {
        {
            match change {
                Some(f) => {
                    let data = contents
                        .next()
                        .ok_or_else(|| Error::Corrupt(format!("no content fetched for {path}")))?;
                    let mtime_ns = fsutil::write_file(self.root(), path, f.mode, &data)?;
                    dirstate
                        .files
                        .insert(path.to_owned(), record(f.mode, f.id, data.len() as u64, mtime_ns));
                    *written += 1;
                }
                None => {
                    fsutil::remove_file(self.root(), path)?;
                    dirstate.files.remove(path);
                    *removed += 1;
                }
            }
        }
        Ok(())
    }

    /// A pending merge on an old base is not rebased file by file (that would only cover the
    /// sparse scope and silently drop the other branch's changes outside it). Instead the
    /// working copy moves to the new tip and the merge is computed again over the whole tree.
    /// If it now conflicts, the working copy stays at the tip without the merge.
    async fn redo_merge(
        &mut self,
        client: &Client,
        onto: ObjectId,
        onto_root: ObjectId,
        theirs: ObjectId,
        before: Pending,
    ) -> Result<SyncOutcome> {
        let message = self.state.head_message.clone().unwrap_or_default();
        let author = self.state.head_author.clone();
        let target = flatten(client, onto_root, &self.sparse()?).await?;
        let mut dirstate = self.paths.load_dirstate()?;
        let delta = diff(&snapshot(&dirstate)?, &target);
        let (mut written, mut removed) = self.apply(client, &mut dirstate, &delta).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        self.clear_head();
        self.state.base = Some(onto.to_string());
        self.paths.save_state(&self.state)?;
        let author = author.as_ref().map(|(n, e)| (n.as_str(), e.as_str()));
        let merged = match self.merge_locked(client, theirs, message, author).await {
            Err(Error::Conflict(paths)) => {
                if let Some(c) = &mut self.state.conflict {
                    c.before = Some(Box::new(before));
                    self.paths.save_state(&self.state)?;
                }
                return Err(Error::Conflict(paths));
            }
            r => r?,
        };
        match merged {
            MergeOutcome::Merged {
                head,
                written: w,
                removed: r,
            } => {
                written += w;
                removed += r;
                Ok(SyncOutcome::Rebased {
                    onto,
                    head,
                    written,
                    removed,
                })
            }
            // Someone else already merged it
            MergeOutcome::UpToDate => Ok(SyncOutcome::Updated {
                from: None,
                to: onto,
                written,
                removed,
            }),
        }
    }

    /// Rebases pending `commits` (as they were made, oldest first) one by one onto `onto`,
    /// after the already rebased `made`, as `git rebase` does: each is merged three-way with
    /// its parent as the base, over the paths it or `onto` changed (line level for text). A
    /// commit whose changes are all upstream already is dropped. `base` becomes the working
    /// copy's base and `local` is the commit whose files the working tree holds.
    ///
    /// On a conflict the working tree moves to the last commit made, with the conflicting
    /// commit's merged files (text conflicts with markers) as local changes; the commits after
    /// it wait in [`ConflictState::rest`] for the `scn commit` that finishes it.
    #[allow(clippy::too_many_arguments)]
    async fn replay(
        &mut self,
        client: &Client,
        commits: Vec<ObjectId>,
        base: ObjectId,
        onto: ObjectId,
        mut made: Vec<ObjectId>,
        local: ObjectId,
        before: Pending,
    ) -> Result<SyncOutcome> {
        let sparse = self.sparse()?;
        let mut tip = onto;
        for (i, commit) in commits.iter().enumerate() {
            let e = client
                .log(&commit.to_string(), None, 1, None)
                .await?
                .entries
                .into_iter()
                .next()
                .ok_or_else(|| Error::Corrupt(format!("pending commit {commit} has no history")))?;
            let (b, o, t, paths) = three_sides(client, e.parents.first().copied(), *commit, tip).await?;
            let changes: BTreeMap<String, Final> = three_way(client, paths, &b, &o, &t)
                .await?
                .into_iter()
                .filter(|(p, f)| *f != Final::Version(t.get(p).copied()))
                .collect();
            let sides = sides_of(&changes);
            if !sides.is_empty() {
                // Finished from local files, so every change has to be inside the checkout
                if changes.keys().any(|p| !sparse.includes_file(p)) {
                    return Err(Error::MergeConflicts(
                        changes
                            .iter()
                            .filter_map(|(path, f)| match f {
                                Final::Conflict { kind, .. } => Some(MergeConflict {
                                    path: path.clone(),
                                    kind: *kind,
                                }),
                                _ => None,
                            })
                            .collect(),
                    ));
                }
                let mut dirstate = self.paths.load_dirstate()?;
                self.apply(client, &mut dirstate, &tree_delta(client, local, tip, &sparse).await?)
                    .await?;
                self.paths.save_dirstate(&mut dirstate)?;
                let created = self.write_results(client, &changes, &t).await?;
                let conflicted: Vec<String> = sides.keys().cloned().collect();
                self.set_pending(base, &made);
                self.state.conflict = Some(ConflictState {
                    paths: sides,
                    message: Some(e.message),
                    author: Some((e.author.name, e.author.email)),
                    merge: None,
                    created,
                    pick: None,
                    rest: commits[i + 1..].iter().map(ToString::to_string).collect(),
                    before: Some(Box::new(before)),
                });
                self.paths.save_state(&self.state)?;
                return Err(Error::Conflict(conflicted));
            }
            if changes.is_empty() {
                continue;
            }
            let builder = client
                .changeset(Some(tip))
                .message(e.message.clone())
                .author_signature(e.author.clone());
            let (builder, _) = add_results(client, builder, changes).await?;
            tip = builder.create().await?.id;
            made.push(tip);
        }
        let mut dirstate = self.paths.load_dirstate()?;
        let (written, removed) = self
            .apply(client, &mut dirstate, &tree_delta(client, local, tip, &sparse).await?)
            .await?;
        self.paths.save_dirstate(&mut dirstate)?;
        self.set_pending(base, &made);
        self.paths.save_state(&self.state)?;
        Ok(match made.last() {
            Some(&head) => SyncOutcome::Rebased {
                onto: base,
                head,
                written,
                removed,
            },
            None => SyncOutcome::Updated {
                from: None,
                to: base,
                written,
                removed,
            },
        })
    }

    /// The working copy is on `base` with the pending commits `made` (oldest first).
    fn set_pending(&mut self, base: ObjectId, made: &[ObjectId]) {
        self.clear_head();
        self.state.base = Some(base.to_string());
        for id in made {
            self.push_head(*id);
        }
    }
}

/// dirstate 中的快照（路径 → 文件）。
fn snapshot(d: &DirState) -> Result<BTreeMap<String, TreeFile>> {
    d.files
        .iter()
        .map(|(p, r)| {
            Ok((
                p.clone(),
                TreeFile {
                    mode: r.mode,
                    id: r.object_id()?,
                },
            ))
        })
        .collect()
}

/// 从 `from` 到 `to` 的改动：`Some` 为新增或修改后的文件，`None` 为删除。
fn diff(from: &BTreeMap<String, TreeFile>, to: &BTreeMap<String, TreeFile>) -> BTreeMap<String, Option<TreeFile>> {
    let mut out = BTreeMap::new();
    for (p, f) in to {
        if from.get(p) != Some(f) {
            out.insert(p.clone(), Some(*f));
        }
    }
    for p in from.keys().filter(|p| !to.contains_key(*p)) {
        out.insert(p.clone(), None);
    }
    out
}

/// A directory entry as a file (None for no entry, or a directory).
fn tree_file(e: &Option<DirEntry>) -> Option<TreeFile> {
    e.as_ref().and_then(|e| {
        Some(TreeFile {
            mode: Mode::from_entry(e.mode)?,
            id: e.id,
        })
    })
}

type Files = BTreeMap<String, TreeFile>;

/// For a three-way merge of `ours` and `theirs` against `base`: the three versions of every
/// path that either side changed, and those paths, from two diffs instead of tree walks.
async fn three_sides(
    client: &Client,
    base: Option<ObjectId>,
    ours: ObjectId,
    theirs: ObjectId,
) -> Result<(Files, Files, Files, Vec<String>)> {
    let base = base.map(|b| b.to_string());
    let (mut b, mut o, mut t) = (Files::new(), Files::new(), Files::new());
    let mine = client.diff(base.as_deref(), &ours.to_string(), None).await?;
    let other = client.diff(base.as_deref(), &theirs.to_string(), None).await?;
    let (mut in_mine, mut in_other) = (std::collections::BTreeSet::new(), std::collections::BTreeSet::new());
    for c in &mine {
        in_mine.insert(c.path.clone());
        if let Some(f) = tree_file(&c.old) {
            b.insert(c.path.clone(), f);
        }
        if let Some(f) = tree_file(&c.new) {
            o.insert(c.path.clone(), f);
        }
    }
    for c in &other {
        in_other.insert(c.path.clone());
        if let Some(f) = tree_file(&c.old) {
            b.insert(c.path.clone(), f);
        }
        if let Some(f) = tree_file(&c.new) {
            t.insert(c.path.clone(), f);
        }
    }
    // A path only one side changed is the base's on the other
    for (path, f) in &b {
        if !in_mine.contains(path) {
            o.insert(path.clone(), *f);
        }
        if !in_other.contains(path) {
            t.insert(path.clone(), *f);
        }
    }
    let paths = in_mine.union(&in_other).cloned().collect();
    Ok((b, o, t, paths))
}

/// The changes from commit `from` to commit `to` inside the sparse checkout, for [`WorkingCopy::apply`].
async fn tree_delta(
    client: &Client,
    from: ObjectId,
    to: ObjectId,
    sparse: &Sparse,
) -> Result<BTreeMap<String, Option<TreeFile>>> {
    if from == to {
        return Ok(BTreeMap::new());
    }
    Ok(client
        .diff(Some(&from.to_string()), &to.to_string(), None)
        .await?
        .into_iter()
        .filter(|c| sparse.includes_file(&c.path))
        .map(|c| {
            let f = tree_file(&c.new);
            (c.path, f)
        })
        .collect())
}

/// Adds merge results without conflicts to a commit, fetching the versions it takes from the
/// server. Returns the builder and each path's new file (None: deleted).
async fn add_results<'a>(
    client: &Client,
    mut builder: ChangesetBuilder<'a>,
    changes: BTreeMap<String, Final>,
) -> Result<(ChangesetBuilder<'a>, BTreeMap<String, Option<TreeFile>>)> {
    let taken: Vec<(String, TreeFile)> = changes
        .iter()
        .filter_map(|(p, f)| match f {
            Final::Version(Some(v)) => Some((p.clone(), *v)),
            _ => None,
        })
        .collect();
    for part in taken.chunks(FETCH_BATCH) {
        let ids: Vec<ObjectId> = part.iter().map(|(_, f)| f.id).collect();
        for ((path, f), data) in part.iter().zip(client.get_objects(&ids).await?) {
            builder = builder.put_with_mode(path, f.mode.to_entry(), data);
        }
    }
    let mut files = BTreeMap::new();
    for (path, f) in changes {
        let file = match f {
            Final::Version(v) => {
                if v.is_none() {
                    builder = builder.delete(&path);
                }
                v
            }
            Final::Data(v, data) => {
                builder = builder.put_with_mode(&path, v.mode.to_entry(), data);
                Some(v)
            }
            Final::Conflict { .. } => unreachable!("callers return conflicts first"),
        };
        files.insert(path, file);
    }
    Ok((builder, files))
}

fn record(mode: Mode, id: ObjectId, size: u64, mtime_ns: u64) -> FileRecord {
    FileRecord {
        mode,
        id: id.to_string(),
        size,
        mtime_ns,
    }
}

/// 只看元数据就能确定文件没改：类型、大小、修改时间都一致，且修改时间早于 dirstate 写入时间。
/// 修改时间不早于写入时间的文件可能在同一时间粒度内被改过（racy），必须重新计算哈希。
fn unchanged_by_stat(rec: &FileRecord, f: &Found, written_at_ns: u64) -> bool {
    rec.mode == f.mode && rec.size == f.size && rec.mtime_ns == f.mtime_ns && f.mtime_ns < written_at_ns
}

/// 方便 CLI：把 [`EntryMode`] 转成文件类型（目录返回 `None`）。
pub fn file_mode(m: EntryMode) -> Option<Mode> {
    Mode::from_entry(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(size: u64, mtime_ns: u64) -> FileRecord {
        record(Mode::File, ObjectId::of_blob(b"x"), size, mtime_ns)
    }

    #[test]
    fn unchanged_by_stat_rules() {
        let f = |size, mtime_ns| Found {
            mode: Mode::File,
            size,
            mtime_ns,
        };
        assert!(unchanged_by_stat(&rec(1, 100), &f(1, 100), 200));
        assert!(!unchanged_by_stat(&rec(1, 100), &f(2, 100), 200), "size differs");
        assert!(!unchanged_by_stat(&rec(1, 100), &f(1, 101), 200), "mtime differs");
        assert!(
            !unchanged_by_stat(&rec(1, 200), &f(1, 200), 200),
            "racy: mtime == written_at"
        );
        let mut r = rec(1, 100);
        r.mode = Mode::Executable;
        assert!(!unchanged_by_stat(&r, &f(1, 100), 200), "mode differs");
    }

    #[test]
    fn diff_cases() {
        let f = |c: &[u8]| TreeFile {
            mode: Mode::File,
            id: ObjectId::of_blob(c),
        };
        let from = BTreeMap::from([
            ("same".to_string(), f(b"s")),
            ("mod".to_string(), f(b"1")),
            ("del".to_string(), f(b"d")),
        ]);
        let mut to = BTreeMap::from([
            ("same".to_string(), f(b"s")),
            ("mod".to_string(), f(b"2")),
            ("new".to_string(), f(b"n")),
        ]);
        let d = diff(&from, &to);
        assert_eq!(d.keys().collect::<Vec<_>>(), ["del", "mod", "new"]);
        assert_eq!(d["del"], None);
        assert_eq!(d["mod"], Some(f(b"2")));
        to.insert(
            "same".into(),
            TreeFile {
                mode: Mode::Executable,
                id: ObjectId::of_blob(b"s"),
            },
        );
        assert!(diff(&from, &to).contains_key("same"), "mode change counts");
        assert!(diff(&from, &from).is_empty());
    }

    #[test]
    fn snapshot_rejects_corrupt_ids() {
        let mut d = DirState::default();
        d.files.insert(
            "a".into(),
            FileRecord {
                mode: Mode::File,
                id: "bad".into(),
                size: 0,
                mtime_ns: 0,
            },
        );
        assert!(matches!(snapshot(&d), Err(Error::Corrupt(_))));
    }

    #[test]
    fn status_is_clean() {
        assert!(Status::default().is_clean());
        assert!(
            !Status {
                added: vec!["a".into()],
                ..Default::default()
            }
            .is_clean()
        );
        assert!(
            !Status {
                deleted: vec!["a".into()],
                ..Default::default()
            }
            .is_clean()
        );
        assert!(
            !Status {
                modified: vec!["a".into()],
                ..Default::default()
            }
            .is_clean()
        );
    }

    #[test]
    fn file_mode_maps_entries() {
        assert_eq!(file_mode(EntryMode::Executable), Some(Mode::Executable));
        assert_eq!(file_mode(EntryMode::Tree), None);
    }
}
