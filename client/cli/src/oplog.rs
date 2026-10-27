//! `scn undo` and `scn op`: the working copy's operation log. Every command that changes the
//! working copy is logged with what it was before ([`scion_wc::Operation`]), so an agent (or a
//! person) can go back after a mistake, local changes included.

use scion_wc::{Operation, WorkingCopy};
use serde::Serialize;

use crate::commands::{Ctx, connect, note};
use crate::history::format_time;
use crate::output::{CliError, Render};

/// The command line, as the log shows it.
fn command_line() -> String {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .map(|a| {
            if a.contains(char::is_whitespace) {
                format!("{a:?}")
            } else {
                a
            }
        })
        .collect();
    format!("scn {}", args.join(" "))
}

/// Before a command that changes the working copy: what it is now. None outside one, or when it
/// cannot be read (the command then reports the problem itself).
pub fn begin(ctx: &Ctx) -> Option<(WorkingCopy, Operation)> {
    let wc = ctx.working_copy()?;
    let op = wc.begin_op(&command_line()).ok()?;
    Some((wc, op))
}

/// After it: logs the entry, unless the command changed nothing.
pub fn finish(op: Option<(WorkingCopy, Operation)>) {
    if let Some((wc, op)) = op {
        let _ = wc.finish_op(op);
    }
}

#[derive(Debug, Serialize)]
pub struct UndoOut {
    pub schema: &'static str,
    /// The operation gone back before.
    pub operation: u64,
    pub command: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub commit: Option<String>,
}

/// jj's `Undid operation N: COMMAND`, then where the working copy is.
impl Render for UndoOut {
    fn text(&self) -> String {
        let at = self
            .commit
            .as_deref()
            .map_or("(no commits)".to_owned(), |c| c[..c.len().min(12)].to_owned());
        format!(
            "Undid operation {}: {}\nOn branch {} at {at}",
            self.operation, self.command, self.reference
        )
    }

    const REPORTS: bool = true;
}

/// `scn undo` (None: the newest operation) and `scn op restore ID`.
pub async fn restore(ctx: &Ctx, id: Option<u64>) -> Result<UndoOut, CliError> {
    let mut wc = WorkingCopy::open(&ctx.cwd)?;
    let client = connect(&ctx.endpoint_or(Some(&wc)), Some(&wc.state().repo)).await?;
    let op = wc.restore_op(&client, id).await?;
    let first = op.command.split_whitespace().nth(1).unwrap_or_default();
    if matches!(first, "land" | "push") {
        note(
            ctx,
            "note: the commits it landed stay on the server; `scn revert` undoes them there",
        );
    }
    Ok(UndoOut {
        schema: "scion.cli.undo/v1",
        operation: op.id,
        command: op.command,
        reference: wc.state().reference.clone(),
        commit: wc.state().current()?.map(|c| c.to_string()),
    })
}

#[derive(Debug, Serialize)]
pub struct OpLogOut {
    pub schema: &'static str,
    pub operations: Vec<OpLine>,
}

#[derive(Debug, Serialize)]
pub struct OpLine {
    pub id: u64,
    pub time_ms: i64,
    pub command: String,
    /// The branch and commit before, and after.
    pub before: (String, Option<String>),
    pub after: Option<(String, Option<String>)>,
    /// Local changes it started with.
    pub changes: usize,
}

/// `ID  TIME  COMMAND`, newest first.
impl Render for OpLogOut {
    fn text(&self) -> String {
        let width = self.operations.first().map_or(1, |o| o.id.to_string().len());
        self.operations
            .iter()
            .map(|o| format!("{:>width$}  {}  {}", o.id, format_time(o.time_ms, 0), o.command))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

pub fn log(ctx: &Ctx, limit: usize) -> Result<OpLogOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    Ok(OpLogOut {
        schema: "scion.cli.op-log/v1",
        operations: wc
            .operations()
            .into_iter()
            .take(limit)
            .map(|o| OpLine {
                id: o.id,
                time_ms: o.time_ms,
                before: (
                    o.before.reference.clone(),
                    o.before.current().ok().flatten().map(|c| c.to_string()),
                ),
                changes: o.changes.len(),
                after: o.after,
                command: o.command,
            })
            .collect(),
    })
}
