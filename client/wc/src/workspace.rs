//! More working copies of one repository on one machine (`scn workspace`, git's worktrees), for
//! agents that each need their own. A new one takes the files it shares with this working copy
//! from here, as copy-on-write clones where the file system can, and fetches only the rest: it is
//! ready in about the time it takes to list the files.
//!
//! The working copy that made others keeps their paths in `.scion/workspaces.json`; each of them
//! names it in `.scion/main`, so any of them can list them all.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use scion_client::Client;
use scion_object::ObjectId;

use crate::state::{DirState, Paths, State, read_json, write_json};
use crate::{Error, OnProgress, Result, TreeFile, WorkingCopy, checkout_files, error, flatten_with, fsutil, record};

const REGISTRY: &str = "workspaces.json";
const MAIN: &str = "main";

/// What [`WorkingCopy::add_workspace`] did.
pub struct AddedWorkspace {
    pub workspace: WorkingCopy,
    /// Files copied from this working copy, and fetched from the server.
    pub copied: usize,
    pub fetched: usize,
}

/// One working copy of the set ([`WorkingCopy::workspaces`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceInfo {
    pub root: PathBuf,
    /// The branch, and the commit it is on (None for an empty one); None when the directory is
    /// gone.
    pub reference: Option<String>,
    pub commit: Option<ObjectId>,
    pub detached: bool,
    /// The working copy the others were made from.
    pub main: bool,
}

impl WorkingCopy {
    /// Another working copy of this repository at `dir` (missing or empty), on `reference` at
    /// commit `at` (None: the reference's tip on the server; an empty working copy when it has
    /// none), with this one's sparse paths. Files this working copy has, unchanged, with the
    /// same content are copied from here; the rest are fetched.
    pub async fn add_workspace(
        &self,
        client: &Client,
        dir: &Path,
        reference: &str,
        at: Option<ObjectId>,
        detached: bool,
        progress: OnProgress<'_>,
    ) -> Result<AddedWorkspace> {
        if dir.exists() && std::fs::read_dir(dir).map_err(error::io(dir))?.next().is_some() {
            return Err(Error::TargetNotEmpty(dir.to_path_buf()));
        }
        let target = match at {
            Some(id) => Some((id, client.resolve_revision(&id.to_string()).await?.root_tree)),
            None => client.resolve_ref(reference).await?.map(|t| (t.changeset, t.root_tree)),
        };
        let paths = Paths::new(dir);
        std::fs::create_dir_all(paths.meta_dir()).map_err(error::io(paths.meta_dir()))?;
        let _lock = paths.lock()?;
        let mut state = State::new(&self.state.endpoint, &self.state.repo, reference);
        state.sparse = self.state.sparse.clone();
        state.detached = detached;
        let mut dirstate = DirState::default();
        let (mut copied, mut fetched) = (0, 0);
        if let Some((commit, root)) = target {
            let files = flatten_with(client, root, &self.sparse()?, progress).await?;
            let ours = self.paths.load_dirstate()?;
            let mut fetch: BTreeMap<String, TreeFile> = BTreeMap::new();
            let mut copy = Vec::new();
            for (path, f) in &files {
                let same = ours.files.get(path).filter(|rec| {
                    rec.id == f.id.to_string()
                        && rec.mode == f.mode
                        && f.mode != crate::Mode::Symlink
                        && fsutil::found(self.root(), path)
                            .is_some_and(|found| crate::unchanged_by_stat(rec, &found, ours.written_at_ns))
                });
                match same {
                    Some(rec) => copy.push((path.clone(), *f, rec.size)),
                    None => {
                        fetch.insert(path.clone(), *f);
                    }
                }
            }
            fsutil::create_parents(dir, files.keys().map(String::as_str))?;
            let (from, to) = (self.root().to_path_buf(), dir.to_path_buf());
            let copies = tokio::task::spawn_blocking(move || -> Result<Vec<(String, crate::FileRecord)>> {
                copy.into_iter()
                    .map(|(path, f, size)| {
                        let mtime_ns = fsutil::copy_file(&from, &to, &path)?;
                        Ok((path, record(f.mode, f.id, size, mtime_ns)))
                    })
                    .collect()
            })
            .await
            .map_err(|e| Error::Corrupt(format!("copy task: {e}")))??;
            copied = copies.len();
            fetched = fetch.len();
            dirstate.files.extend(copies);
            dirstate
                .files
                .extend(checkout_files(client, dir, &fetch, progress).await?);
            state.base = Some(commit.to_string());
        }
        paths.save_dirstate(&mut dirstate)?;
        paths.save_state(&state)?;
        // Register it with the working copy the set started from
        let main = self.main_root();
        let added = canonical(dir);
        write_json(&paths.meta_dir().join(MAIN), &main.to_string_lossy())?;
        let mut list = registry(&main);
        if !list.contains(&added) {
            list.push(added);
        }
        write_json(&Paths::new(&main).meta_dir().join(REGISTRY), &list)?;
        Ok(AddedWorkspace {
            workspace: WorkingCopy { paths, state },
            copied,
            fetched,
        })
    }

    /// The working copy the others were made from (this one when it was not made by another).
    fn main_root(&self) -> PathBuf {
        read_json::<String>(&self.paths.meta_dir().join(MAIN))
            .map(PathBuf::from)
            .unwrap_or_else(|_| canonical(self.root()))
    }

    /// The working copies of this one's set: the one they were made from, then the others in the
    /// order they were made.
    pub fn workspaces(&self) -> Vec<WorkspaceInfo> {
        let main = self.main_root();
        std::iter::once(main.clone())
            .chain(registry(&main))
            .map(|root| {
                let wc = WorkingCopy::open_at(&root).ok();
                let state = wc.as_ref().map(WorkingCopy::state);
                WorkspaceInfo {
                    main: root == main,
                    reference: state.map(|s| s.reference.clone()),
                    commit: state.and_then(|s| s.current().ok().flatten()),
                    detached: state.is_some_and(|s| s.detached),
                    root,
                }
            })
            .collect()
    }

    /// Deletes the working copy at `dir`, one of this set's other than the one they were made
    /// from, and forgets it. Refused while it has local changes or commits not landed, unless
    /// `force`.
    pub fn remove_workspace(&self, dir: &Path, force: bool) -> Result<()> {
        let main = self.main_root();
        let target = canonical(dir);
        let mut list = registry(&main);
        if target == main || !list.contains(&target) {
            return Err(Error::NotAWorkspace(dir.to_path_buf()));
        }
        if let Ok(wc) = WorkingCopy::open_at(&target) {
            if !force && (!wc.status()?.is_clean() || wc.state.head.is_some()) {
                return Err(Error::WorkspaceInUse(dir.to_path_buf()));
            }
            std::fs::remove_dir_all(&target).map_err(error::io(&target))?;
        }
        list.retain(|p| *p != target);
        write_json(&Paths::new(&main).meta_dir().join(REGISTRY), &list)
    }

    /// The working copy whose root is `root` (not one above it).
    fn open_at(root: &Path) -> Result<Self> {
        let paths = Paths::new(root);
        let state = paths.load_state()?;
        Ok(Self { paths, state })
    }
}

fn registry(main: &Path) -> Vec<PathBuf> {
    read_json(&Paths::new(main).meta_dir().join(REGISTRY)).unwrap_or_default()
}

/// `p` with links and `..` resolved; for one that is gone, through its parent.
fn canonical(p: &Path) -> PathBuf {
    p.canonicalize()
        .or_else(|e| match (p.parent(), p.file_name()) {
            (Some(dir), Some(name)) => dir.canonicalize().map(|d| d.join(name)),
            _ => Err(e),
        })
        .unwrap_or_else(|_| p.to_path_buf())
}
