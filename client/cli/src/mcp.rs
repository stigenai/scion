//! `scn mcp`: the repository as tools for an agent, over the Model Context Protocol (JSON-RPC 2.0
//! on standard input and output, one message per line). Reading, searching and history run on the
//! server at any revision, with no checkout; `commit` creates a commit there from file contents and
//! can land it, so an agent changes the repository without a working tree at all.
//!
//! Tool results are the JSON that the same `scn` command prints with `--json`.

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::commands::{Ctx, query_client};
use crate::output::{CliError, error_out};

/// The protocol versions this server speaks, newest first.
const PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];
/// Longest file content `read_file` returns; the rest is cut (with `truncated`).
const MAX_READ: usize = 256 << 10;

const INSTRUCTIONS: &str = "Tools for a Scion repository on its server. Paths are from the \
repository root. `revision` is a branch, tag, commit id, git SHA or HEAD~N, and defaults to the \
current branch. Reading needs no checkout. `commit` creates a commit from file contents on top \
of a revision (the branch by default) and, with land, puts it on the branch: the server rebases \
it onto what others landed meanwhile and reports conflicts if the same lines changed.";

/// Serves until standard input ends.
pub async fn serve(ctx: &Ctx) -> Result<(), CliError> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut out = tokio::io::stdout();
    while let Some(line) = lines.next_line().await.map_err(io)? {
        if line.trim().is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Value>(&line) {
            Ok(msg) => handle(ctx, msg).await,
            Err(e) => Some(error(Value::Null, -32700, &format!("parse error: {e}"))),
        };
        if let Some(r) = reply {
            let mut text = r.to_string();
            text.push('\n');
            out.write_all(text.as_bytes()).await.map_err(io)?;
            out.flush().await.map_err(io)?;
        }
    }
    Ok(())
}

fn io(e: std::io::Error) -> CliError {
    CliError::Usage(format!("standard input or output: {e}"))
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// The reply to one message; none for notifications.
async fn handle(ctx: &Ctx, msg: Value) -> Option<Value> {
    let id = msg.get("id").cloned();
    let method = msg.get("method").and_then(Value::as_str).unwrap_or_default();
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let id = id?;
    let result = match method {
        "initialize" => {
            let asked = params.get("protocolVersion").and_then(Value::as_str);
            let version = asked
                .filter(|v| PROTOCOL_VERSIONS.contains(v))
                .unwrap_or(PROTOCOL_VERSIONS[0]);
            json!({
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "scn", "version": env!("CARGO_PKG_VERSION")},
                "instructions": INSTRUCTIONS,
            })
        }
        "ping" => json!({}),
        "tools/list" => json!({"tools": tools()}),
        "tools/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or_default();
            let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            match call(ctx, name, &args).await {
                Ok(v) => json!({"content": [{"type": "text", "text": v.to_string()}], "structuredContent": v}),
                Err(e) => {
                    let v = serde_json::to_value(error_out(&e)).unwrap_or(Value::Null);
                    json!({"content": [{"type": "text", "text": v.to_string()}], "isError": true})
                }
            }
        }
        _ => return Some(error(id, -32601, &format!("unknown method {method:?}"))),
    };
    Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": {"type": "object", "properties": properties, "required": required},
    })
}

fn tools() -> Vec<Value> {
    let revision = json!({"type": "string", "description": "Branch, tag, commit id, git SHA or HEAD~N; default the current branch"});
    let path = json!({"type": "string", "description": "Path from the repository root"});
    vec![
        tool(
            "read_file",
            "A file's content at a revision.",
            json!({"path": path, "revision": revision}),
            &["path"],
        ),
        tool(
            "list_dir",
            "The entries of a directory at a revision (the root when no path).",
            json!({"path": path, "revision": revision}),
            &[],
        ),
        tool(
            "grep",
            "Lines matching a regular expression (Rust syntax) in the files at a revision.",
            json!({
                "pattern": {"type": "string"},
                "revision": revision,
                "paths": {"type": "array", "items": {"type": "string"}, "description": "Only under these paths"},
                "ignore_case": {"type": "boolean"},
                "fixed": {"type": "boolean", "description": "The pattern is a literal string"},
                "max_matches": {"type": "integer", "description": "Default 1000, at most 10000"},
            }),
            &["pattern"],
        ),
        tool(
            "log",
            "Commits newest first, optionally only those that changed a path (follow: across renames).",
            json!({
                "revision": revision,
                "path": path,
                "max_count": {"type": "integer", "description": "Default 20"},
                "follow": {"type": "boolean"},
            }),
            &[],
        ),
        tool(
            "blame",
            "For each line of a file, the commit that last changed it.",
            json!({"path": path, "revision": revision}),
            &["path"],
        ),
        tool(
            "diff",
            "The changes between two revisions (from defaults to to's first parent), with patches.",
            json!({"from": revision, "to": revision, "path": path}),
            &["to"],
        ),
        tool(
            "show",
            "A commit: its message, author and the changes it made.",
            json!({"revision": revision, "path": path}),
            &[],
        ),
        tool(
            "commit",
            "Create a commit on top of a revision from whole file contents and deletions, without a \
             checkout; with land, also put it on the branch (rebased by the server if others landed).",
            json!({
                "message": {"type": "string"},
                "files": {"type": "array", "description": "Files to write", "items": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
                    "required": ["path", "content"],
                }},
                "delete": {"type": "array", "items": {"type": "string"}, "description": "Paths to delete"},
                "edits": {"type": "array", "description": "Text replacements in files of base, applied on the server", "items": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"}, "old": {"type": "string"}, "new": {"type": "string"},
                        "all": {"type": "boolean", "description": "Every occurrence; otherwise old must appear exactly once"},
                    },
                    "required": ["path", "old", "new"],
                }},
                "base": revision,
                "land": {"type": "boolean", "description": "Land it on branch (default false)"},
                "branch": {"type": "string", "description": "The branch to land on; default the current one"},
            }),
            &["message"],
        ),
        tool(
            "edit_file",
            "Replace old_string with new_string in a file and commit it, without reading or sending the \
             whole file: the server applies the edit to the file at base (the branch by default). \
             old_string must appear exactly once unless replace_all. Lands it unless land is false.",
            json!({
                "path": path,
                "old_string": {"type": "string"},
                "new_string": {"type": "string"},
                "replace_all": {"type": "boolean"},
                "message": {"type": "string", "description": "Default: Edit PATH"},
                "base": revision,
                "land": {"type": "boolean", "description": "Default true"},
                "branch": {"type": "string"},
            }),
            &["path", "old_string", "new_string"],
        ),
        tool(
            "land",
            "Put a commit on a branch; the server rebases it onto what others landed, and refuses on conflicts. \
             With dry_run, only say what it would do (fast-forward, rebase, or the conflicts), landing nothing.",
            json!({"changeset": {"type": "string"}, "branch": {"type": "string"}, "dry_run": {"type": "boolean"}}),
            &["changeset"],
        ),
    ]
}

fn arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn required<'a>(args: &'a Value, key: &str) -> Result<&'a str, CliError> {
    arg(args, key).ok_or_else(|| CliError::Usage(format!("{key} is required")))
}

/// `REV:PATH`, or the path alone at the default revision.
fn spec(revision: Option<&str>, path: &str) -> String {
    match revision {
        Some(r) => format!("{r}:{path}"),
        None => path.to_owned(),
    }
}

fn to_json<T: serde::Serialize>(v: T) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

/// The branch writes go to: the working copy's, else the default.
fn branch(ctx: &Ctx, args: &Value) -> String {
    match arg(args, "branch") {
        Some(b) => b.to_owned(),
        None => ctx.working_copy().map_or_else(
            || crate::commands::DEFAULT_REF.to_owned(),
            |w| w.state().reference.clone(),
        ),
    }
}

async fn call(ctx: &Ctx, name: &str, args: &Value) -> Result<Value, CliError> {
    let rev = arg(args, "revision");
    Ok(match name {
        "read_file" => {
            let out = crate::commands::cat(ctx, &spec(rev, required(args, "path")?)).await?;
            let truncated = out.content.len() > MAX_READ;
            let mut end = out.content.len().min(MAX_READ);
            while !out.content.is_char_boundary(end) {
                end -= 1;
            }
            json!({
                "revision": out.revision, "path": out.path, "size": out.size, "binary": out.binary,
                "content": &out.content[..end], "truncated": truncated,
            })
        }
        "list_dir" => to_json(crate::commands::ls(ctx, Some(&spec(rev, arg(args, "path").unwrap_or("")))).await?),
        "grep" => {
            let paths = args
                .get("paths")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).map(str::to_owned).collect())
                .unwrap_or_default();
            let query = scion_client::GrepQuery {
                pattern: required(args, "pattern")?.to_owned(),
                fixed: args.get("fixed").and_then(Value::as_bool).unwrap_or(false),
                ignore_case: args.get("ignore_case").and_then(Value::as_bool).unwrap_or(false),
                paths,
                max_matches: args
                    .get("max_matches")
                    .and_then(Value::as_u64)
                    .map_or(0, |n| u32::try_from(n).unwrap_or(u32::MAX)),
            };
            to_json(crate::history::grep(ctx, query, rev, true, false).await?)
        }
        "log" => {
            let max = args.get("max_count").and_then(Value::as_u64).unwrap_or(20);
            let follow = args.get("follow").and_then(Value::as_bool).unwrap_or(false);
            let max = usize::try_from(max).unwrap_or(usize::MAX);
            to_json(crate::history::log(ctx, rev, arg(args, "path"), follow, max, false).await?)
        }
        "blame" => to_json(crate::history::blame(ctx, &spec(rev, required(args, "path")?), &[]).await?),
        "diff" => {
            let to = required(args, "to")?.to_owned();
            let revs = match arg(args, "from") {
                Some(f) => vec![f.to_owned(), to],
                None => vec![to],
            };
            to_json(crate::history::diff(ctx, &revs, arg(args, "path"), false).await?)
        }
        "show" => to_json(crate::history::show(ctx, rev, arg(args, "path"), false).await?),
        "commit" => commit(ctx, args).await?,
        "edit_file" => {
            let path = required(args, "path")?;
            let mut c = json!({
                "message": arg(args, "message").map_or_else(|| format!("Edit {path}"), str::to_owned),
                "edits": [{
                    "path": path,
                    "old": args.get("old_string").and_then(Value::as_str).unwrap_or_default(),
                    "new": args.get("new_string").and_then(Value::as_str).unwrap_or_default(),
                    "all": args.get("replace_all").and_then(Value::as_bool).unwrap_or(false),
                }],
                "land": args.get("land").and_then(Value::as_bool).unwrap_or(true),
            });
            for k in ["base", "branch"] {
                if let Some(v) = arg(args, k) {
                    c[k] = json!(v);
                }
            }
            commit(ctx, &c).await?
        }
        "land" => {
            let (client, _) = query_client(ctx).await?;
            let id = required(args, "changeset")?;
            let id = client.resolve_revision(id).await?.id;
            let branch = branch(ctx, args);
            if args.get("dry_run").and_then(Value::as_bool).unwrap_or(false) {
                let mut v = landed(&branch, &client.land_check(&branch, &id).await?);
                v["dry_run"] = json!(true);
                v
            } else {
                landed(&branch, &client.land(&branch, &id).await?)
            }
        }
        other => return Err(CliError::Usage(format!("unknown tool {other:?}"))),
    })
}

fn landed(branch: &str, l: &scion_client::Landed) -> Value {
    json!({
        "branch": branch,
        "tip": l.tip.to_string(),
        "rebased": l.rebased(),
        "landed": l.landed.iter().map(|c| json!({"changeset": c.changeset.to_string(), "landed": c.landed.to_string()})).collect::<Vec<_>>(),
    })
}

async fn commit(ctx: &Ctx, args: &Value) -> Result<Value, CliError> {
    let (client, _) = query_client(ctx).await?;
    let message = required(args, "message")?;
    let branch = branch(ctx, args);
    let base = arg(args, "base").map_or_else(|| branch.clone(), str::to_owned);
    let parent = match client.resolve_revision(&base).await {
        Ok(r) => Some(r.id),
        // A branch that does not exist yet: the first commit
        Err(e) if e.is_not_found() && arg(args, "base").is_none() => None,
        Err(e) => return Err(e.into()),
    };
    let mut b = client.changeset(parent).message(message);
    if let Some((name, email)) = crate::commands::own_author() {
        b = b.author(name, email);
    }
    let mut changed = 0;
    for f in args.get("files").and_then(Value::as_array).into_iter().flatten() {
        let path = f.get("path").and_then(Value::as_str).filter(|p| !p.is_empty());
        let content = f.get("content").and_then(Value::as_str);
        let (Some(path), Some(content)) = (path, content) else {
            return Err(CliError::Usage("each file needs a path and a content".into()));
        };
        b = b.put(path.to_owned(), content.as_bytes().to_vec());
        changed += 1;
    }
    for p in args.get("delete").and_then(Value::as_array).into_iter().flatten() {
        let Some(p) = p.as_str() else {
            return Err(CliError::Usage("delete takes paths".into()));
        };
        b = b.delete(p.to_owned());
        changed += 1;
    }
    for e in args.get("edits").and_then(Value::as_array).into_iter().flatten() {
        let field = |k: &str| e.get(k).and_then(Value::as_str);
        let (Some(path), Some(old), Some(new)) = (field("path"), field("old"), field("new")) else {
            return Err(CliError::Usage("each edit needs a path, old and new".into()));
        };
        let (path, old, new) = (path.to_owned(), old.as_bytes().to_vec(), new.as_bytes().to_vec());
        b = if e.get("all").and_then(Value::as_bool).unwrap_or(false) {
            b.edit_all(path, old, new)
        } else {
            b.edit(path, old, new)
        };
        changed += 1;
    }
    if changed == 0 {
        return Err(CliError::Usage("nothing to commit: give files, edits or delete".into()));
    }
    let created = b.create().await?;
    let mut out = json!({
        "changeset": created.id.to_string(),
        "parent": parent.map(|p| p.to_string()),
    });
    if args.get("land").and_then(Value::as_bool).unwrap_or(false) {
        out["landed"] = landed(&branch, &client.land(&branch, &created.id).await?);
    }
    Ok(out)
}
