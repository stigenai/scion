//! `.scion/` 下的状态文件：`state.json`（仓库与提交）和 `dirstate.json`（文件快照）。

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use scion_object::{EntryMode, ObjectId};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, io};

pub const DIR: &str = ".scion";
const STATE: &str = "state.json";
const DIRSTATE: &str = "dirstate.json";
const LOCK: &str = "lock";
/// 状态文件格式版本；不兼容的改动时递增。
const FORMAT: u32 = 1;

/// 工作副本对应的仓库、ref 与提交。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub format: u32,
    pub endpoint: String,
    pub repo: String,
    #[serde(rename = "ref")]
    pub reference: String,
    /// 最近一次同步到的 ref 提交（空仓库时为 None）。
    pub base: Option<String>,
    /// 已提交但尚未合入的提交（有多个时为最新的一个）。
    pub head: Option<String>,
    /// The other pending commits, under `head`, oldest first: each one's parent is the one
    /// before it, and the first one's is `base`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stack: Vec<String>,
    /// 未合入提交的说明与作者；rebase 时用于重建提交。
    #[serde(default)]
    pub head_message: Option<String>,
    #[serde(default)]
    pub head_author: Option<(String, String)>,
    /// The other parent when the pending commit is a merge (`scn merge`); kept across rebases.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_merge: Option<String>,
    /// Set while a sync or merge stopped on conflicts, until the result is committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<ConflictState>,
    /// 稀疏检出的目录；None 为全部检出。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sparse: Option<Vec<String>>,
    /// Stashed local changes (`scn stash`), newest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stash: Vec<StashEntry>,
}

/// Local changes `scn stash` put aside: a commit on the server, on no branch, whose parent is
/// the commit they were made on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StashEntry {
    pub commit: String,
    pub message: String,
}

impl State {
    pub fn new(endpoint: &str, repo: &str, reference: &str) -> Self {
        Self {
            format: FORMAT,
            endpoint: endpoint.into(),
            repo: repo.into(),
            reference: reference.into(),
            base: None,
            head: None,
            stack: Vec::new(),
            head_message: None,
            head_author: None,
            head_merge: None,
            conflict: None,
            sparse: None,
            stash: Vec::new(),
        }
    }

    /// 当前快照对应的提交：有未合入的提交时为它，否则为 base。
    pub fn current(&self) -> Result<Option<ObjectId>> {
        self.head.as_deref().or(self.base.as_deref()).map(parse_id).transpose()
    }

    /// Where the working copy is and what it has pending.
    pub fn pending_state(&self) -> Pending {
        Pending {
            base: self.base.clone(),
            head: self.head.clone(),
            stack: self.stack.clone(),
            head_message: self.head_message.clone(),
            head_author: self.head_author.clone(),
            head_merge: self.head_merge.clone(),
        }
    }

    /// Puts back what [`State::pending_state`] returned.
    pub fn set_pending_state(&mut self, p: Pending) {
        self.base = p.base;
        self.head = p.head;
        self.stack = p.stack;
        self.head_message = p.head_message;
        self.head_author = p.head_author;
        self.head_merge = p.head_merge;
    }

    /// The pending (committed, not landed) commits, oldest first.
    pub fn pending(&self) -> Result<Vec<ObjectId>> {
        self.stack.iter().chain(&self.head).map(|s| parse_id(s)).collect()
    }
}

fn parse_id(s: &str) -> Result<ObjectId> {
    s.parse()
        .map_err(|e| Error::Corrupt(format!("bad object id {s:?}: {e}")))
}

/// A sync or merge that stopped on conflicts. Conflicted files hold conflict markers (text) or
/// our version (everything else); the rest of the result is already in the working tree.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictState {
    /// Paths not resolved yet.
    pub paths: BTreeMap<String, Sides>,
    /// Message and author for the commit that finishes it (the pending commit's, or the merge's).
    pub message: Option<String>,
    pub author: Option<(String, String)>,
    /// For a merge: the other parent of the commit that finishes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge: Option<String>,
    /// Files the merge created, which `scn merge --abort` removes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created: Vec<String>,
    /// For a revert or cherry-pick: what it applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pick: Option<Pick>,
    /// For a sync: the pending commits still to rebase after this one (their ids before the
    /// rebase, oldest first), which the commit that finishes it goes on with.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rest: Vec<String>,
    /// For a sync: the working copy as it was before, which `scn sync --abort` goes back to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Box<Pending>>,
}

/// Where a working copy was and what it had pending, as [`State`] records them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    pub base: Option<String>,
    pub head: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stack: Vec<String>,
    pub head_message: Option<String>,
    pub head_author: Option<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_merge: Option<String>,
}

/// A revert or cherry-pick of one commit (see [`crate::WorkingCopy::pick`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pick {
    pub kind: PickKind,
    pub commit: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PickKind {
    /// Undo the commit's changes.
    Revert,
    /// Apply the commit's changes again.
    CherryPick,
}

/// The two versions of a conflicted path (None: that side has no file).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sides {
    /// Conflict kind, e.g. `content` or `modify_delete`.
    pub kind: String,
    pub ours: Option<Version>,
    pub theirs: Option<Version>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub mode: Mode,
    pub id: String,
}

/// 一个已检出文件的记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRecord {
    pub mode: Mode,
    pub id: String,
    pub size: u64,
    /// 检出或提交时文件的修改时间（Unix 纳秒）。
    pub mtime_ns: u64,
}

impl FileRecord {
    pub fn object_id(&self) -> Result<ObjectId> {
        parse_id(&self.id)
    }
}

/// 可序列化的文件类型（不含目录）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    File,
    Executable,
    Symlink,
}

impl Mode {
    pub fn to_entry(self) -> EntryMode {
        match self {
            Mode::File => EntryMode::File,
            Mode::Executable => EntryMode::Executable,
            Mode::Symlink => EntryMode::Symlink,
        }
    }

    pub fn from_entry(m: EntryMode) -> Option<Self> {
        match m {
            EntryMode::File => Some(Mode::File),
            EntryMode::Executable => Some(Mode::Executable),
            EntryMode::Symlink => Some(Mode::Symlink),
            EntryMode::Tree => None,
        }
    }
}

/// 当前快照中全部文件。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirState {
    pub format: u32,
    /// 写入本文件的时间（Unix 纳秒）。修改时间不早于它的文件无法靠时间戳判断是否改过，需要重新计算哈希。
    pub written_at_ns: u64,
    pub files: BTreeMap<String, FileRecord>,
}

/// 原子写入：先写临时文件并 fsync，再 rename 覆盖。崩溃时要么是旧文件要么是新文件。
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp).map_err(io(&tmp))?;
    f.write_all(bytes).map_err(io(&tmp))?;
    f.sync_all().map_err(io(&tmp))?;
    std::fs::rename(&tmp, path).map_err(io(path))?;
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = std::fs::read(path).map_err(io(path))?;
    serde_json::from_slice(&bytes).map_err(|e| Error::Corrupt(format!("{}: {e}", path.display())))
}

fn write_json<T: Serialize>(path: &Path, v: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(v).map_err(|e| Error::Corrupt(e.to_string()))?;
    write_atomic(path, &bytes)
}

/// `.scion/` 目录的路径集合。
#[derive(Debug, Clone)]
pub struct Paths {
    pub root: PathBuf,
}

impl Paths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn meta_dir(&self) -> PathBuf {
        self.root.join(DIR)
    }

    pub fn load_state(&self) -> Result<State> {
        let s: State = read_json(&self.meta_dir().join(STATE))?;
        if s.format != FORMAT {
            return Err(Error::Corrupt(format!("unsupported state format {}", s.format)));
        }
        Ok(s)
    }

    pub fn save_state(&self, s: &State) -> Result<()> {
        write_json(&self.meta_dir().join(STATE), s)
    }

    pub fn load_dirstate(&self) -> Result<DirState> {
        read_json(&self.meta_dir().join(DIRSTATE))
    }

    pub fn save_dirstate(&self, d: &mut DirState) -> Result<()> {
        d.format = FORMAT;
        d.written_at_ns = crate::fsutil::now_ns();
        write_json(&self.meta_dir().join(DIRSTATE), d)
    }

    /// 从 `start` 向上查找包含 `.scion/` 的目录。
    pub fn discover(start: &Path) -> Result<Self> {
        let mut dir = Some(start);
        while let Some(d) = dir {
            if d.join(DIR).join(STATE).is_file() {
                return Ok(Self::new(d));
            }
            dir = d.parent();
        }
        Err(Error::NotAWorkingCopy(start.to_path_buf()))
    }

    /// 取得工作副本锁；守卫释放时删除锁文件。
    pub fn lock(&self) -> Result<Lock> {
        let path = self.meta_dir().join(LOCK);
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut f) => {
                let _ = writeln!(f, "{}", std::process::id());
                Ok(Lock { path })
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(Error::Locked(path)),
            Err(e) => Err(io(path)(e)),
        }
    }
}

/// 工作副本锁守卫。
#[derive(Debug)]
pub struct Lock {
    path: PathBuf,
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_paths() -> (tempfile::TempDir, Paths) {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join(DIR)).expect("mkdir");
        let p = Paths::new(dir.path());
        (dir, p)
    }

    #[test]
    fn state_roundtrip_and_current() {
        let (_d, p) = tmp_paths();
        let mut s = State::new("http://h:1", "mono", "main");
        assert_eq!(s.current().expect("ok"), None);
        let base = ObjectId::hash(scion_object::ObjectKind::Changeset, b"b");
        let head = ObjectId::hash(scion_object::ObjectKind::Changeset, b"h");
        s.base = Some(base.to_string());
        assert_eq!(s.current().expect("ok"), Some(base));
        s.head = Some(head.to_string());
        assert_eq!(s.current().expect("ok"), Some(head));
        p.save_state(&s).expect("save");
        assert_eq!(p.load_state().expect("load"), s);
        s.head = Some("garbage".into());
        assert!(matches!(s.current(), Err(Error::Corrupt(_))));
    }

    #[test]
    fn state_format_and_corruption_detected() {
        let (_d, p) = tmp_paths();
        let mut s = State::new("e", "r", "main");
        s.format = 99;
        p.save_state(&s).expect("save");
        assert!(matches!(p.load_state(), Err(Error::Corrupt(_))));
        std::fs::write(p.meta_dir().join(STATE), b"{not json").expect("write");
        assert!(matches!(p.load_state(), Err(Error::Corrupt(_))));
        assert!(matches!(p.load_dirstate(), Err(Error::Io { .. })), "missing file");
    }

    #[test]
    fn dirstate_roundtrip_sets_timestamp() {
        let (_d, p) = tmp_paths();
        let mut d = DirState::default();
        let id = ObjectId::of_blob(b"x");
        d.files.insert(
            "a".into(),
            FileRecord {
                mode: Mode::Executable,
                id: id.to_string(),
                size: 1,
                mtime_ns: 5,
            },
        );
        p.save_dirstate(&mut d).expect("save");
        assert!(d.written_at_ns > 0);
        let back = p.load_dirstate().expect("load");
        assert_eq!(back, d);
        assert_eq!(back.files["a"].object_id().expect("id"), id);
    }

    #[test]
    fn mode_conversions() {
        for m in [Mode::File, Mode::Executable, Mode::Symlink] {
            assert_eq!(Mode::from_entry(m.to_entry()), Some(m));
        }
        assert_eq!(Mode::from_entry(EntryMode::Tree), None);
    }

    #[test]
    fn discover_walks_up() {
        let (d, p) = tmp_paths();
        p.save_state(&State::new("e", "r", "main")).expect("save");
        let deep = d.path().join("a/b");
        std::fs::create_dir_all(&deep).expect("mkdir");
        assert_eq!(Paths::discover(&deep).expect("found").root, d.path());
        let other = tempfile::tempdir().expect("tempdir");
        assert!(matches!(Paths::discover(other.path()), Err(Error::NotAWorkingCopy(_))));
    }

    #[test]
    fn lock_is_exclusive_and_released() {
        let (_d, p) = tmp_paths();
        let l = p.lock().expect("lock");
        assert!(matches!(p.lock(), Err(Error::Locked(_))));
        drop(l);
        assert!(p.lock().is_ok());
    }

    #[test]
    fn lock_in_missing_dir_is_io_error() {
        let p = Paths::new("/nonexistent-scion-dir");
        assert!(matches!(p.lock(), Err(Error::Io { .. })));
    }

    #[test]
    fn write_atomic_replaces_and_reports_errors() {
        let d = tempfile::tempdir().expect("tempdir");
        let f = d.path().join("x.json");
        write_atomic(&f, b"1").expect("write");
        write_atomic(&f, b"2").expect("overwrite");
        assert_eq!(std::fs::read(&f).expect("read"), b"2");
        assert!(!d.path().join("x.tmp").exists());
        assert!(matches!(
            write_atomic(&d.path().join("missing/x.json"), b"1"),
            Err(Error::Io { .. })
        ));
    }
}
