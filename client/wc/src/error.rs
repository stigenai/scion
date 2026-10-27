//! 工作副本错误。

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("not a scion working copy (no .scion directory in {0} or its parents)")]
    NotAWorkingCopy(PathBuf),
    #[error("{0} already exists and is not empty")]
    TargetNotEmpty(PathBuf),
    /// `scn workspace remove` of a directory that is not one of this set's (or is the one they
    /// were made from).
    #[error("{0} is not a workspace made from this working copy's set")]
    NotAWorkspace(PathBuf),
    #[error("{0} has local changes or commits not landed; land or drop them first, or pass --force")]
    WorkspaceInUse(PathBuf),
    /// A path the working copy refuses to write: it names the working copy's own metadata
    /// (`.scion`, `.git`), is not a plain relative path, or leads through a symbolic link.
    #[error("refusing to write {path}: {reason}")]
    UnsafePath { path: String, reason: &'static str },
    #[error("working copy is locked by another scn process ({0}); remove it if that process is gone")]
    Locked(PathBuf),
    #[error("you have uncommitted changes; commit them first")]
    Dirty,
    #[error("nothing to land")]
    NothingToLand,
    /// `switch` / `merge` while a commit is pending.
    #[error("you have a pending commit; land it or `scn uncommit` it first")]
    PendingCommit,
    /// A new commit on top of a pending merge commit: rebasing commits one by one would lose
    /// the merge's second parent.
    #[error("your pending commit is a merge; land it first, or add these changes to it with `scn commit --amend`")]
    OnPendingMerge,
    /// `merge` into a branch with no commits.
    #[error("{0} has no commits yet; land something on it first")]
    EmptyBranch(String),
    /// A pending merge commit can only land as a fast-forward; the ref moved since.
    #[error("{0} moved since the merge; run `scn sync` to redo the merge on top of it, then land")]
    MergeBehind(String),
    /// Paths the merge could not combine.
    #[error("merge conflict: {}", .0.iter().map(|c| format!("{} ({:?})", c.path, c.kind)).collect::<Vec<_>>().join(", "))]
    MergeConflicts(Vec<crate::MergeConflict>),
    /// `uncommit` / `commit --amend` without a pending (committed, not landed) commit.
    #[error("no pending commit; landed commits cannot be changed")]
    NoPendingCommit,
    /// A sync or merge stopped on conflicts; the files hold conflict markers.
    #[error("conflict in {}", .0.join(", "))]
    Conflict(Vec<String>),
    /// Conflicts are not resolved yet.
    #[error("unresolved conflicts: {}", .0.join(", "))]
    Unresolved(Vec<String>),
    #[error("there are no conflicts to resolve")]
    NoConflict,
    #[error("{0} is not conflicted")]
    NotConflicted(String),
    /// `resolve` on files that still contain conflict markers.
    #[error("conflict markers remain in {}", .0.join(", "))]
    MarkersRemain(Vec<String>),
    #[error("no merge is in progress")]
    NoMergeInProgress,
    #[error("no sync is in progress")]
    NoSyncInProgress,
    #[error("no stash entry stash@{{{0}}}")]
    NoStash(usize),
    /// A command that makes or publishes commits on a detached working copy.
    #[error("not on a branch (detached at {})", &.0[..12.min(.0.len())])]
    Detached(String),
    /// `commit PATH...` with a path that has no local change.
    #[error("no local changes in {}", .0.join(", "))]
    NoChangesIn(Vec<String>),
    /// `commit PATH...` while finishing a sync, merge, revert or cherry-pick.
    #[error("the commit that finishes a sync, merge, revert or cherry-pick takes every change; leave out the paths")]
    PartialFinish,
    /// `revert` / `cherry-pick` of a commit that changed files the sparse checkout leaves out:
    /// the commit that finishes it is made from local files.
    #[error("the commit changes files outside the sparse checkout: {}", .0.join(", "))]
    PickOutsideSparse(Vec<String>),
    #[error("{0}")]
    Sparse(String),
    /// 稀疏检出范围外出现了文件：提交它们可能覆盖服务端上没有检出的内容。
    #[error("files outside the sparse checkout: {}; move them, or add their directories with `scn sparse add`", .0.join(", "))]
    OutsideSparse(Vec<String>),
    /// 调整范围时，新范围要检出的路径上已有内容不同的本地文件。
    #[error("local files would be overwritten by the sparse checkout: {}; move or remove them first", .0.join(", "))]
    WouldOverwrite(Vec<String>),
    #[error("ref {0:?} does not exist on the server")]
    RefNotFound(String),
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("corrupt working copy state: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Client(#[from] scion_client::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 给 IO 错误附上路径。
pub(crate) fn io(path: impl Into<PathBuf>) -> impl FnOnce(std::io::Error) -> Error {
    let path = path.into();
    move |source| Error::Io { path, source }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages() {
        assert!(Error::NotAWorkingCopy("/x".into()).to_string().contains(".scion"));
        assert!(Error::Locked("/x/.scion/lock".into()).to_string().contains("locked"));
        assert!(Error::Dirty.to_string().contains("commit"));
        assert_eq!(Error::NothingToLand.to_string(), "nothing to land");
        assert_eq!(
            Error::Conflict(vec!["a".into(), "b".into()]).to_string(),
            "conflict in a, b"
        );
        assert!(Error::RefNotFound("main".into()).to_string().contains("main"));
        assert!(Error::TargetNotEmpty("/d".into()).to_string().contains("not empty"));
        assert!(Error::Corrupt("x".into()).to_string().contains("corrupt"));
        assert_eq!(Error::Sparse("bad".into()).to_string(), "bad");
        assert!(Error::OutsideSparse(vec!["z/x".into()]).to_string().contains("z/x"));
        assert!(
            Error::WouldOverwrite(vec!["c/z".into()])
                .to_string()
                .contains("overwritten by the sparse checkout: c/z")
        );
        let e = io("/p")(std::io::Error::other("boom"));
        assert_eq!(e.to_string(), "/p: boom");
    }
}
