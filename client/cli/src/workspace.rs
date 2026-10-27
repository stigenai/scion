//! `scn workspace`: more working copies of a repository on one machine, as `git worktree`, made
//! in seconds by copying (cloning, where the file system can) the files this one has.

use std::path::Path;

use scion_wc::{Progress, WorkingCopy};
use serde::Serialize;

use crate::commands::{Ctx, connect, note};
use crate::output::{CliError, Render};
use crate::progress::Meter;

#[derive(Debug, Serialize)]
pub struct WorkspaceOut {
    pub schema: &'static str,
    pub path: String,
    /// The branch; with `detached`, the one it was made from.
    #[serde(rename = "ref")]
    pub reference: String,
    pub commit: Option<String>,
    pub new_branch: bool,
    pub detached: bool,
    /// Files copied from this working copy, and fetched from the server.
    pub copied: usize,
    pub fetched: usize,
}

/// git's `Preparing worktree (new branch 'x')` and `HEAD is now at …`.
impl Render for WorkspaceOut {
    fn text(&self) -> String {
        let what = if self.detached {
            "detached HEAD".to_owned()
        } else if self.new_branch {
            format!("new branch '{}'", self.reference)
        } else {
            format!("checking out '{}'", self.reference)
        };
        let mut out = format!("Preparing workspace ({what})");
        if let Some(c) = &self.commit {
            out.push_str(&format!("\nHEAD is now at {}", &c[..c.len().min(12)]));
        }
        out
    }

    const REPORTS: bool = true;
}

/// `scn workspace add PATH [BRANCH]`, as `git worktree add`: without a branch, the one named
/// after PATH (made at HEAD when it does not exist); `-b NEW [START]` makes a branch at START
/// (default HEAD); `--detach [REV]` is on no branch.
pub async fn add(
    ctx: &Ctx,
    path: &Path,
    branch: Option<&str>,
    new_branch: Option<&str>,
    detach: bool,
) -> Result<WorkspaceOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let client = connect(&ctx.endpoint_or(Some(&wc)), Some(&wc.state().repo)).await?;
    let head = || ctx.expand_rev("HEAD");
    let named = || {
        path.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .ok_or_else(|| CliError::Usage(format!("cannot name a branch after {}", path.display())))
    };
    // (branch, commit to start at, whether the branch is new)
    let (reference, start, made) = match (new_branch, branch, detach) {
        (Some(new), start, false) => (
            new.to_owned(),
            Some(start.map_or_else(head, |s| ctx.expand_rev(s))?),
            true,
        ),
        (None, rev, true) => (
            wc.state().reference.clone(),
            Some(rev.map_or_else(head, |s| ctx.expand_rev(s))?),
            false,
        ),
        (None, Some(b), false) => {
            if client.resolve_ref(b).await?.is_none() {
                return Err(CliError::Usage(format!(
                    "no branch {b:?} on the server; use -b {b} to make it"
                )));
            }
            (b.to_owned(), None, false)
        }
        (None, None, false) => {
            let name = named()?;
            match client.resolve_ref(&name).await? {
                Some(_) => (name, None, false),
                None => (name, Some(head()?), true),
            }
        }
        (Some(_), _, true) => return Err(CliError::Usage("-b and --detach do not go together".into())),
    };
    if made && client.resolve_ref(&reference).await?.is_some() {
        return Err(CliError::Usage(format!("a branch named {reference:?} already exists")));
    }
    let at = match start {
        Some(rev) => match wc.state().current()? {
            Some(_) => Some(client.resolve_revision(&rev).await?.id),
            // An empty working copy: so is the new one
            None => None,
        },
        None => None,
    };
    let target = ctx.cwd.join(path);
    let meter = crate::progress::Meter::stderr(ctx.chatty);
    let added = wc
        .add_workspace(&client, &target, &reference, at, detach, &|p| progress(&meter, p))
        .await?;
    meter.done();
    note(
        ctx,
        &format!(
            "{} files copied from this working copy, {} fetched",
            added.copied, added.fetched
        ),
    );
    Ok(WorkspaceOut {
        schema: "scion.cli.workspace/v1",
        path: target.display().to_string(),
        reference,
        commit: added.workspace.state().current()?.map(|c| c.to_string()),
        new_branch: made,
        detached: detach,
        copied: added.copied,
        fetched: added.fetched,
    })
}

fn progress(meter: &Meter, p: Progress) {
    match p {
        Progress::Listing { files } => meter.update("Listing files", files as u64, None, None),
        Progress::Checkout { done, total, bytes } => {
            meter.update("Receiving objects", done as u64, Some(total as u64), Some(bytes));
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WorkspaceListOut {
    pub schema: &'static str,
    pub workspaces: Vec<WorkspaceLine>,
}

#[derive(Debug, Serialize)]
pub struct WorkspaceLine {
    pub path: String,
    #[serde(rename = "ref")]
    pub reference: Option<String>,
    pub commit: Option<String>,
    pub detached: bool,
    /// The directory is gone.
    pub missing: bool,
}

/// git worktree list's `PATH  COMMIT [BRANCH]`, `(detached HEAD)`, `prunable` for a missing one.
impl Render for WorkspaceListOut {
    fn text(&self) -> String {
        let width = self.workspaces.iter().map(|w| w.path.len()).max().unwrap_or(0);
        self.workspaces
            .iter()
            .map(|w| {
                let commit = w
                    .commit
                    .as_deref()
                    .map_or("(empty)".to_owned(), |c| c[..c.len().min(12)].to_owned());
                let what = match (&w.reference, w.detached, w.missing) {
                    (_, _, true) => "prunable".to_owned(),
                    (_, true, _) => "(detached HEAD)".to_owned(),
                    (Some(r), false, _) => format!("[{r}]"),
                    (None, false, _) => String::new(),
                };
                format!("{:<width$}  {commit:<12} {what}", w.path).trim_end().to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

pub fn list(ctx: &Ctx) -> Result<WorkspaceListOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    Ok(WorkspaceListOut {
        schema: "scion.cli.workspace-list/v1",
        workspaces: wc
            .workspaces()
            .into_iter()
            .map(|w| WorkspaceLine {
                path: w.root.display().to_string(),
                missing: w.reference.is_none(),
                reference: w.reference,
                commit: w.commit.map(|c| c.to_string()),
                detached: w.detached,
            })
            .collect(),
    })
}

#[derive(Debug, Serialize)]
pub struct WorkspaceRemoveOut {
    pub schema: &'static str,
    pub path: String,
}

/// git worktree remove prints nothing.
impl Render for WorkspaceRemoveOut {
    fn text(&self) -> String {
        String::new()
    }
}

pub fn remove(ctx: &Ctx, path: &Path, force: bool) -> Result<WorkspaceRemoveOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let target = ctx.cwd.join(path);
    wc.remove_workspace(&target, force)?;
    Ok(WorkspaceRemoveOut {
        schema: "scion.cli.workspace-remove/v1",
        path: target.display().to_string(),
    })
}
