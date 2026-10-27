//! The operation log (`scn op log`, `scn undo`): before each command that changes the working
//! copy, what the working copy was, so that any of them can be undone, including what it did to
//! local changes. An entry keeps the state (branch, commits, pending and stashed ones, a conflict
//! in progress) and the local changes, whose contents are kept once each under
//! `.scion/ops/objects`. Going back to an entry makes the working tree that commit's files plus
//! those changes, and the state what it was; it is an operation itself, so undoing twice redoes.
//!
//! Only the working copy goes back: a land has put commits on the server, which stay there.

use std::collections::BTreeSet;

use scion_client::Client;
use scion_object::ObjectId;
use serde::{Deserialize, Serialize};

use crate::state::{Mode, State, read_json, write_json};
use crate::{Error, Result, TreeFile, WorkingCopy, diff, error, flatten, fsutil, snapshot};

const DIR: &str = "ops";
const LOG: &str = "log.json";
const OBJECTS: &str = "objects";
/// Entries kept; older ones, and contents only they refer to, are dropped.
pub const KEEP: usize = 100;

/// A local change as it was: the content (by id, in the log's objects) and mode of a file, or
/// none for one deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedChange {
    pub path: String,
    pub file: Option<(Mode, String)>,
}

/// One entry of the log: the working copy before `command` ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: u64,
    pub time_ms: i64,
    pub command: String,
    pub before: State,
    pub changes: Vec<SavedChange>,
    /// The branch and commit it left the working copy on.
    #[serde(default)]
    pub after: Option<(String, Option<String>)>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Log {
    next: u64,
    /// Oldest first.
    ops: Vec<Operation>,
}

impl WorkingCopy {
    fn ops_dir(&self) -> std::path::PathBuf {
        self.paths.meta_dir().join(DIR)
    }

    fn load_log(&self) -> Log {
        read_json(&self.ops_dir().join(LOG)).unwrap_or_default()
    }

    /// The local changes now, their contents kept in the log's objects.
    fn save_changes(&self) -> Result<Vec<SavedChange>> {
        let objects = self.ops_dir().join(OBJECTS);
        std::fs::create_dir_all(&objects).map_err(error::io(&objects))?;
        self.local_changes()?
            .into_iter()
            .map(|c| {
                let file = match c.new {
                    Some((mode, data)) => {
                        let id = ObjectId::of_blob(&data).to_string();
                        let path = objects.join(&id);
                        if !path.exists() {
                            crate::state::write_atomic(&path, &data)?;
                        }
                        Some((mode, id))
                    }
                    None => None,
                };
                Ok(SavedChange { path: c.path, file })
            })
            .collect()
    }

    /// What the working copy is before `command` (the command line, for the log) changes it.
    pub fn begin_op(&self, command: &str) -> Result<Operation> {
        Ok(Operation {
            id: 0,
            time_ms: now_ms(),
            command: command.to_owned(),
            before: self.state.clone(),
            changes: self.save_changes()?,
            after: None,
        })
    }

    /// Adds `op` to the log once its command has run, unless it changed nothing (a command that
    /// failed, say). Whether it was added.
    pub fn finish_op(&self, mut op: Operation) -> Result<bool> {
        let now = WorkingCopy::open(self.root())?;
        if now.state == op.before {
            let changes: Vec<(String, Option<(Mode, String)>)> = now
                .local_changes()?
                .into_iter()
                .map(|c| (c.path, c.new.map(|(m, d)| (m, ObjectId::of_blob(&d).to_string()))))
                .collect();
            let before: Vec<(String, Option<(Mode, String)>)> =
                op.changes.iter().map(|c| (c.path.clone(), c.file.clone())).collect();
            if changes == before {
                return Ok(false);
            }
        }
        let mut log = self.load_log();
        op.id = log.next.max(1);
        log.next = op.id + 1;
        op.after = Some((now.state.reference.clone(), now.state.current()?.map(|c| c.to_string())));
        log.ops.push(op);
        let drop = log.ops.len().saturating_sub(KEEP);
        log.ops.drain(..drop);
        write_json(&self.ops_dir().join(LOG), &log)?;
        if drop > 0 {
            self.drop_unused(&log)?;
        }
        Ok(true)
    }

    /// Removes the kept contents no entry refers to any more.
    fn drop_unused(&self, log: &Log) -> Result<()> {
        let used: BTreeSet<&str> = log
            .ops
            .iter()
            .flat_map(|o| {
                o.changes
                    .iter()
                    .filter_map(|c| c.file.as_ref().map(|(_, id)| id.as_str()))
            })
            .collect();
        let objects = self.ops_dir().join(OBJECTS);
        for e in std::fs::read_dir(&objects).map_err(error::io(&objects))?.flatten() {
            if !used.contains(e.file_name().to_string_lossy().as_ref()) {
                let _ = std::fs::remove_file(e.path());
            }
        }
        Ok(())
    }

    /// The log, newest first.
    pub fn operations(&self) -> Vec<Operation> {
        let mut ops = self.load_log().ops;
        ops.reverse();
        ops
    }

    /// Puts the working copy back as it was before operation `id` (the newest when None): the
    /// state, that commit's files, and the local changes it had. What it has now is lost unless
    /// the caller logged it first, as `scn undo` does.
    pub async fn restore_op(&mut self, client: &Client, id: Option<u64>) -> Result<Operation> {
        let log = self.load_log();
        let op = match id {
            Some(id) => log.ops.iter().find(|o| o.id == id),
            None => log.ops.last(),
        }
        .cloned()
        .ok_or(Error::NoOperation(id))?;
        let _lock = self.paths.lock()?;
        let objects = self.ops_dir().join(OBJECTS);
        // Every content needed is at hand before anything changes
        let mut contents = Vec::new();
        for c in &op.changes {
            if let Some((mode, blob)) = &c.file {
                let path = objects.join(blob);
                let data = std::fs::read(&path).map_err(error::io(&path))?;
                contents.push((c.path.clone(), *mode, data));
            }
        }
        // Drop what is local now: added files go, the rest is the commit's again
        let (status, _) = self.changes()?;
        for p in &status.added {
            fsutil::remove_file(self.root(), p)?;
        }
        self.restore_locked(client, &[String::new()]).await?;
        // That commit's files, in that state's sparse directories
        self.state.sparse = op.before.sparse.clone();
        let target = match op.before.current()? {
            Some(c) => flatten(client, client.root_tree(&c.to_string()).await?, &self.sparse()?).await?,
            None => Default::default(),
        };
        let mut dirstate = self.paths.load_dirstate()?;
        let delta: std::collections::BTreeMap<String, Option<TreeFile>> = diff(&snapshot(&dirstate)?, &target);
        self.apply(client, &mut dirstate, &delta).await?;
        self.paths.save_dirstate(&mut dirstate)?;
        // Its local changes on top
        fsutil::create_parents(self.root(), contents.iter().map(|(p, _, _)| p.as_str()))?;
        for (path, mode, data) in &contents {
            fsutil::write_file(self.root(), path, *mode, data)?;
        }
        for c in op.changes.iter().filter(|c| c.file.is_none()) {
            fsutil::remove_file(self.root(), &c.path)?;
        }
        self.state = op.before.clone();
        self.paths.save_state(&self.state)?;
        Ok(op)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{DirState, Paths};

    #[test]
    fn the_log_keeps_the_newest_and_what_they_refer_to() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(dir.path());
        std::fs::create_dir_all(paths.meta_dir()).expect("mkdir");
        paths.save_state(&State::new("http://x", "r", "main")).expect("state");
        paths.save_dirstate(&mut DirState::default()).expect("dirstate");
        let wc = WorkingCopy::open(dir.path()).expect("open");
        for i in 0..KEEP + 5 {
            std::fs::write(dir.path().join("f.txt"), format!("{i}\n")).expect("write");
            let op = wc.begin_op(&format!("scn step {i}")).expect("begin");
            std::fs::write(dir.path().join("f.txt"), format!("{i} changed\n")).expect("write");
            assert!(wc.finish_op(op).expect("finish"));
        }
        let ops = wc.operations();
        assert_eq!(ops.len(), KEEP);
        assert_eq!((ops[0].id, ops[0].command.as_str()), (KEEP as u64 + 5, "scn step 104"));
        assert_eq!(ops.last().map(|o| o.command.as_str()), Some("scn step 5"));
        // The contents of the dropped entries went with them
        let kept = std::fs::read_dir(paths.meta_dir().join(DIR).join(OBJECTS))
            .expect("objects")
            .count();
        assert_eq!(kept, KEEP);
        // Nothing changed: nothing logged
        let op = wc.begin_op("scn status").expect("begin");
        assert!(!wc.finish_op(op).expect("finish"));
        assert_eq!(wc.operations().len(), KEEP);
    }
}
