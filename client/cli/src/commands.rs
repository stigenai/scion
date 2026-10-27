//! 各子命令的实现。每个命令返回可渲染为文本或 JSON 的结果。

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use scion_client::{Client, EntryMode};
use scion_wc::{PickKind, Progress, Sparse, Status, SyncOutcome, WorkingCopy};
use serde::Serialize;

use crate::output::{CliError, ConflictOp, ConflictOut, DoctorOut, Render, doctor_out};
use crate::progress::Meter;

pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7080";
pub(crate) const DEFAULT_REF: &str = "main";

/// 命令行给出的全局选项。
#[derive(Debug, Clone, Default)]
pub struct Ctx {
    pub endpoint: Option<String>,
    pub repo: Option<String>,
    pub cwd: PathBuf,
    /// Print progress and git-style notes on standard error (not with `--json` or `--quiet`).
    pub chatty: bool,
}

impl Ctx {
    /// 当前目录所在的工作副本（如果有）。
    pub(crate) fn working_copy(&self) -> Option<WorkingCopy> {
        WorkingCopy::open(&self.cwd).ok()
    }

    pub fn has_working_copy(&self) -> bool {
        self.working_copy().is_some()
    }

    /// Expands git's `HEAD` / `@` (alone or followed by `~N` / `^N`, e.g. `HEAD~1`) to the
    /// working copy's current commit: the pending commit if any, else the synced one. Other
    /// revisions, including suffixes on refs and ids, are resolved by the server. `origin/NAME`
    /// is the branch NAME: the server is the only remote, and its branches are always current.
    pub fn expand_rev(&self, rev: &str) -> Result<String, CliError> {
        if let Some(name) = rev.strip_prefix("origin/").filter(|n| !n.is_empty()) {
            return Ok(name.to_owned());
        }
        let rest = match rev.strip_prefix("HEAD").or_else(|| rev.strip_prefix('@')) {
            Some(r) if r.is_empty() || r.starts_with(['~', '^']) => r,
            _ => return Ok(rev.to_owned()),
        };
        let wc = self
            .working_copy()
            .ok_or_else(|| CliError::Usage(format!("{rev}: HEAD needs a working copy")))?;
        let current = wc
            .state()
            .current()?
            .ok_or_else(|| CliError::Usage(format!("{rev}: the working copy has no commits yet")))?;
        Ok(format!("{current}{rest}"))
    }

    /// 服务端地址：命令行或环境变量 > 当前工作副本记录 > 默认值。
    pub fn endpoint_or_default(&self) -> String {
        self.endpoint_or(self.working_copy().as_ref())
    }

    /// 服务端地址：命令行或环境变量 > 工作副本记录 > 默认值。
    pub(crate) fn endpoint_or(&self, wc: Option<&WorkingCopy>) -> String {
        self.endpoint
            .clone()
            .or_else(|| wc.map(|w| w.state().endpoint.clone()))
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned())
    }
}

/// Splits a repository URL, `http(s)://host[:port]/REPO`, into the server's address and the
/// repository name (empty when the URL names only the server). Anything else is `None`: a plain
/// repository name.
pub fn split_repo_url(s: &str) -> Result<Option<(String, String)>, CliError> {
    let Some(scheme_end) = ["http://", "https://"]
        .iter()
        .find_map(|p| s.starts_with(p).then_some(p.len()))
    else {
        return Ok(None);
    };
    let (host, path) = s[scheme_end..].split_once('/').unwrap_or((&s[scheme_end..], ""));
    if host.is_empty() || host.contains('@') || s.contains(['?', '#']) {
        return Err(CliError::Usage(format!(
            "{s}: expected http(s)://host[:port]/REPOSITORY"
        )));
    }
    let repo = path.trim_matches('/');
    if repo.split('/').any(str::is_empty) && !repo.is_empty() {
        return Err(CliError::Usage(format!("{s}: empty segment in the repository name")));
    }
    Ok(Some((s[..scheme_end + host.len()].to_owned(), repo.to_owned())))
}

impl Ctx {
    /// A repository argument that may be a URL: the server's address in it replaces
    /// `--endpoint` / `SCION_ENDPOINT` for this command, and the repository name is returned.
    pub fn repo_arg(&mut self, arg: &str) -> Result<String, CliError> {
        match split_repo_url(arg)? {
            Some((endpoint, repo)) => {
                self.endpoint = Some(endpoint);
                Ok(repo)
            }
            None => Ok(arg.to_owned()),
        }
    }

    /// Like [`Ctx::repo_arg`], for arguments that must name a repository.
    pub fn named_repo_arg(&mut self, arg: &str) -> Result<String, CliError> {
        let repo = self.repo_arg(arg)?;
        if repo.is_empty() {
            return Err(CliError::Usage(format!("{arg}: the URL names no repository")));
        }
        Ok(repo)
    }

    /// `repo fork SOURCE TARGET`: either may be a URL, and URLs must name the same server,
    /// since a fork stays on one server.
    pub fn fork_args(&mut self, source: &str, target: &str) -> Result<(String, String), CliError> {
        if let (Some((a, _)), Some((b, _))) = (split_repo_url(source)?, split_repo_url(target)?)
            && a != b
        {
            return Err(CliError::Usage(format!(
                "a fork stays on one server: {a} and {b} differ"
            )));
        }
        Ok((self.named_repo_arg(source)?, self.named_repo_arg(target)?))
    }

    /// A server given as an argument (`scn login URL`): its address, without a path.
    pub fn server_arg(&mut self, arg: &str) -> Result<(), CliError> {
        match split_repo_url(arg)? {
            Some((endpoint, _)) => {
                self.endpoint = Some(endpoint);
                Ok(())
            }
            None => Err(CliError::Usage(format!("{arg}: expected http(s)://host[:port]"))),
        }
    }
}

/// 连接服务端：默认经本机 daemon（必要时拉起它），daemon 不可用时直连。
pub async fn connect(endpoint: &str, repo: Option<&str>) -> Result<Client, CliError> {
    let mut b = Client::builder()
        .endpoint(endpoint)
        .auth(crate::login::auth_for(endpoint, |k| std::env::var(k).ok()))
        .client_name("scn")
        .tls(scion_client::Tls::from_env()?)
        .connect_timeout(Duration::from_secs(5));
    if let Some(r) = repo {
        b = b.repo(r);
    }
    let env = |k: &str| std::env::var(k).ok();
    if crate::daemon::enabled(env) {
        let socket = crate::daemon::config(env).socket;
        if let Some(c) = crate::daemon::connect(b.clone(), &socket).await {
            return Ok(c);
        }
    }
    Ok(b.build().await?)
}

/// 打开当前工作副本并连接到它记录的服务端。
async fn open_wc(ctx: &Ctx) -> Result<(WorkingCopy, Client), CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let client = connect(&ctx.endpoint_or(Some(&wc)), Some(&wc.state().repo)).await?;
    Ok((wc, client))
}

pub async fn doctor(ctx: &Ctx) -> Result<DoctorOut, CliError> {
    let endpoint = ctx.endpoint_or(ctx.working_copy().as_ref());
    let client = connect(&endpoint, None).await?;
    let mut out = doctor_out(&endpoint, client.server_info());
    out.objects = if client.via_daemon() {
        format!(
            "daemon ({})",
            crate::daemon::config(|k| std::env::var(k).ok()).socket.display()
        )
    } else {
        client.object_endpoint().to_owned()
    };
    Ok(out)
}

/// 解析 `Name <email>`。
pub fn parse_author(s: &str) -> Result<(String, String), CliError> {
    let bad = || CliError::Usage(format!("invalid author {s:?}; expected \"Name <email>\""));
    let (name, rest) = s.split_once('<').ok_or_else(bad)?;
    let email = rest.strip_suffix('>').ok_or_else(bad)?.trim();
    let name = name.trim();
    if name.is_empty() || email.is_empty() {
        return Err(bad());
    }
    Ok((name.to_owned(), email.to_owned()))
}

/// A git-style note on standard error, unless `--json` or `--quiet`.
pub fn note(ctx: &Ctx, msg: &str) {
    if ctx.chatty {
        eprintln!("{msg}");
    }
}

fn short(id: &str) -> &str {
    &id[..id.len().min(12)]
}

// ---------- clone ----------

#[derive(Debug, Serialize)]
pub struct CloneOut {
    pub schema: &'static str,
    pub repo: String,
    pub path: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub base: Option<String>,
    /// 稀疏检出的目录；全部检出时省略。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse: Option<Vec<String>>,
}

/// Like `git clone`, nothing on standard output: progress and notes go to standard error.
impl Render for CloneOut {
    fn text(&self) -> String {
        String::new()
    }

    const REPORTS: bool = true;
}

/// 默认目录名：仓库名的最后一段。
pub fn default_dir(repo: &str) -> &str {
    repo.rsplit('/').next().unwrap_or(repo)
}

pub async fn clone(
    ctx: &Ctx,
    repo: &str,
    dir: Option<&Path>,
    reference: &str,
    sparse: Option<&[String]>,
) -> Result<CloneOut, CliError> {
    let sparse = match sparse {
        Some(dirs) => Sparse::dirs(dirs)?,
        None => Sparse::full(),
    };
    let endpoint = ctx.endpoint.clone().unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned());
    let client = connect(&endpoint, Some(repo)).await?;
    let dir = dir.unwrap_or_else(|| Path::new(default_dir(repo)));
    let target = ctx.cwd.join(dir);
    note(ctx, &format!("Cloning into '{}'...", dir.display()));
    let meter = Meter::stderr(ctx.chatty);
    let wc = WorkingCopy::clone_with_progress(&client, &endpoint, repo, reference, &target, sparse, &|p| match p {
        Progress::Listing { files } => meter.update("Listing files", files as u64, None, None),
        Progress::Checkout { done, total, bytes } => {
            meter.update("Receiving objects", done as u64, Some(total as u64), Some(bytes));
        }
    })
    .await?;
    meter.done();
    if wc.state().base.is_none() {
        note(
            ctx,
            &format!("warning: You appear to have cloned an empty repository ({reference} does not exist yet)."),
        );
    }
    Ok(CloneOut {
        schema: "scion.cli.clone/v1",
        repo: repo.to_owned(),
        path: target.display().to_string(),
        reference: reference.to_owned(),
        base: wc.state().base.clone(),
        sparse: wc.state().sparse.clone(),
    })
}

// ---------- sparse ----------

#[derive(Debug, Serialize)]
pub struct SparseOut {
    pub schema: &'static str,
    /// 范围内的目录；全部检出时为 null。
    pub dirs: Option<Vec<String>>,
    pub written: usize,
    pub removed: usize,
}

impl Render for SparseOut {
    fn text(&self) -> String {
        let mut s = match &self.dirs {
            None => "full checkout".to_owned(),
            Some(d) if d.is_empty() => "sparse checkout: root files only".to_owned(),
            Some(d) => format!("sparse checkout:\n  {}", d.join("\n  ")),
        };
        if self.written + self.removed > 0 {
            s.push_str(&format!("\n({} written, {} removed)", self.written, self.removed));
        }
        s
    }
}

pub async fn sparse(ctx: &Ctx, cmd: crate::SparseCmd) -> Result<SparseOut, CliError> {
    use crate::SparseCmd;
    let mut wc = WorkingCopy::open(&ctx.cwd)?;
    let current = wc.sparse()?;
    let target = match cmd {
        SparseCmd::Ls => None,
        SparseCmd::Set { dirs } => Some(Sparse::dirs(&dirs)?),
        SparseCmd::Add { dirs } => Some(current.add(&dirs)?),
        SparseCmd::Rm { dirs } => Some(current.remove(&dirs)?),
        SparseCmd::Disable => Some(Sparse::full()),
    };
    let (written, removed) = match target {
        Some(t) if t != current => {
            let client = connect(&ctx.endpoint_or(Some(&wc)), Some(&wc.state().repo)).await?;
            wc.set_sparse(&client, t).await?
        }
        _ => (0, 0),
    };
    Ok(SparseOut {
        schema: "scion.cli.sparse/v1",
        dirs: wc.state().sparse.clone(),
        written,
        removed,
    })
}

// ---------- status ----------

#[derive(Debug, Clone, Serialize)]
pub struct StatusOut {
    pub schema: &'static str,
    #[serde(rename = "ref")]
    pub reference: String,
    pub base: Option<String>,
    /// 已提交未合入的提交（有多个时为最新的一个）。
    pub pending: Option<String>,
    /// How many commits are pending (committed, not landed).
    pub ahead: usize,
    /// On no branch, looking at `base` (`scn switch --detach`).
    pub detached: bool,
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
    /// Paths with unresolved conflicts (see `scn resolve`).
    pub conflicted: Vec<String>,
    /// How many commits the branch on the server has that the working copy does not; absent
    /// when unknown (no server reached, `--no-ahead-behind`, or the short format without -b).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behind: Option<Behind>,
    /// Conflict kind of each conflicted path.
    #[serde(skip)]
    pub kinds: std::collections::BTreeMap<String, String>,
    /// Finishing a merge (true) or a sync (false) that stopped on conflicts.
    #[serde(skip)]
    pub merging: Option<bool>,
    /// Finishing a revert or cherry-pick that stopped on conflicts.
    #[serde(skip)]
    pub pick: Option<scion_wc::Pick>,
    /// `scn status -s`.
    #[serde(skip)]
    pub short: bool,
    /// `-b`: the short format starts with `## BRANCH`.
    #[serde(skip)]
    pub branch: bool,
    /// The current directory inside the working copy (`/`-separated, empty at the root): text
    /// output shows paths relative to it, like git.
    #[serde(skip)]
    pub cwd: String,
}

/// Commits on the server's branch past the working copy's base: at least `count`, exactly
/// when `exact`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Behind {
    pub count: usize,
    pub exact: bool,
}

impl Behind {
    fn commits(self) -> String {
        let n = if self.exact {
            self.count.to_string()
        } else {
            format!("{}+", self.count)
        };
        format!("{n} commit{}", if self.count == 1 && self.exact { "" } else { "s" })
    }
}

/// `path` (relative to the working copy root) as seen from `cwd` (also relative to the root).
pub(crate) fn relative_to(cwd: &str, path: &str) -> String {
    let (mut from, mut to): (Vec<&str>, Vec<&str>) = (
        cwd.split('/').filter(|s| !s.is_empty()).collect(),
        path.split('/').collect(),
    );
    while !from.is_empty() && to.len() > 1 && from[0] == to[0] {
        from.remove(0);
        to.remove(0);
    }
    let mut parts = vec![".."; from.len()];
    parts.extend(to);
    parts.join("/")
}

/// git's label and `--short` code for a conflict kind.
fn conflict_label(kind: &str) -> (&'static str, &'static str) {
    match kind {
        "add_add" | "path" => ("both added", "AA"),
        "modify_delete" => ("deleted by them", "UD"),
        "delete_modify" => ("deleted by us", "DU"),
        _ => ("both modified", "UU"),
    }
}

/// `git status`'s long format, or with `-s` its short one (`XY PATH`; scn has no staging area,
/// so every change shows as staged).
impl Render for StatusOut {
    fn text(&self) -> String {
        let unmerged: Vec<(String, &str, &str)> = self
            .conflicted
            .iter()
            .map(|p| {
                let (label, code) = conflict_label(self.kinds.get(p).map_or("content", String::as_str));
                (relative_to(&self.cwd, p), label, code)
            })
            .collect();
        let is_unmerged = |p: &String| self.conflicted.contains(p);
        let changes: Vec<(&str, &str, String)> = [
            ("new file:", "A ", &self.added),
            ("modified:", "M ", &self.modified),
            ("deleted:", "D ", &self.deleted),
        ]
        .into_iter()
        .flat_map(|(label, code, paths)| {
            paths
                .iter()
                .filter(|p| !is_unmerged(p))
                .map(move |p| (label, code, relative_to(&self.cwd, p)))
        })
        .collect();
        if self.short {
            let mut lines: Vec<String> = Vec::new();
            if self.branch {
                let ahead = (self.ahead > 0).then(|| format!("ahead {}", self.ahead));
                let behind = self
                    .behind
                    .filter(|b| b.count > 0)
                    .map(|b| format!("behind {}{}", b.count, if b.exact { "" } else { "+" }));
                let marks: Vec<String> = ahead.into_iter().chain(behind).collect();
                lines.push(if self.detached {
                    "## HEAD (no branch)".to_owned()
                } else if marks.is_empty() {
                    format!("## {}", self.reference)
                } else {
                    format!("## {} [{}]", self.reference, marks.join(", "))
                });
            }
            lines.extend(unmerged.iter().map(|(p, _, code)| format!("{code} {p}")));
            lines.extend(changes.iter().map(|(_, code, p)| format!("{code} {p}")));
            return lines.join("\n");
        }
        let mut head = vec![if self.detached {
            format!(
                "HEAD detached at {}\n  (use \"scn switch {}\" to go back, \"scn switch -c NAME\" to start a branch here)",
                short(self.base.as_deref().unwrap_or_default()),
                self.reference
            )
        } else {
            format!("On branch {}", self.reference)
        }];
        match (&self.pending, &self.base) {
            (Some(p), _) if self.ahead > 1 => head.push(format!(
                "Your {} commits are not landed yet; the newest is {}.\n  (use \"scn land\" to publish them)",
                self.ahead,
                short(p)
            )),
            (Some(p), _) => head.push(format!(
                "Your commit {} is not landed yet.\n  (use \"scn land\" to publish your commit)",
                short(p)
            )),
            (None, None) => head.push("\nNo commits yet".to_owned()),
            _ => {}
        }
        match (self.behind, self.pending.is_some()) {
            (Some(b), false) if b.count > 0 => head.push(format!(
                "Your branch is behind '{}' on the server by {}.\n  (use \"scn sync\" to update the working copy)",
                self.reference,
                b.commits()
            )),
            (Some(b), true) if b.count > 0 => {
                let (yours, is) = if self.ahead > 1 {
                    ("your commits", "are")
                } else {
                    ("your commit", "is")
                };
                head.push(format!(
                    "'{}' on the server has {} that {yours} {is} not based on.\n  \
                     (\"scn land\" rebases {yours} onto them; \"scn sync\" brings them here first)",
                    self.reference,
                    b.commits()
                ))
            }
            _ => {}
        }
        if let Some(p) = &self.pick {
            let (doing, cmd) = pick_words(p.kind);
            let next = if unmerged.is_empty() {
                "all conflicts fixed: run \"scn commit\""
            } else {
                "fix conflicts and run \"scn commit\""
            };
            head.push(format!(
                "You are currently {doing} commit {}.\n  ({next})\n  (use \"scn {cmd} --abort\" to cancel the {cmd})",
                short(&p.commit)
            ));
        } else {
            match (self.merging, unmerged.is_empty()) {
                (Some(true), false) => head.push(
                    "You have unmerged paths.\n  (fix conflicts and run \"scn commit\")\n  \
                     (use \"scn merge --abort\" to abort the merge)"
                        .to_owned(),
                ),
                (Some(true), true) => head.push(
                    "All conflicts fixed but you are still merging.\n  (use \"scn commit\" to conclude merge)"
                        .to_owned(),
                ),
                (Some(false), false) => head.push(format!(
                    "You are currently rebasing your commit onto {}.\n  (fix conflicts and then run \"scn commit\")\n  \
                     (use \"scn sync --abort\" to go back to before the sync)",
                    self.reference
                )),
                (Some(false), true) => head.push(format!(
                    "You are currently rebasing your commit onto {}.\n  (all conflicts fixed: run \"scn commit\")\n  \
                     (use \"scn sync --abort\" to go back to before the sync)",
                    self.reference
                )),
                (None, _) => {}
            }
        }
        let mut blocks = vec![head.join("\n")];
        if !changes.is_empty() {
            let mut b = "Changes to be committed:\n  (every change is included: scn has no \"add\" step; ignore files with .gitignore)\n  \
                         (use \"scn restore <file>...\" to discard changes to tracked files)"
                .to_owned();
            for (label, _, p) in &changes {
                b.push_str(&format!("\n\t{label:<12}{p}"));
            }
            blocks.push(b);
        }
        if !unmerged.is_empty() {
            let mut b = "Unmerged paths:\n  (use \"scn resolve <file>...\" to mark resolution)".to_owned();
            for (p, label, _) in &unmerged {
                b.push_str(&format!("\n\t{:<17}{p}", format!("{label}:")));
            }
            blocks.push(b);
        }
        if changes.is_empty() && unmerged.is_empty() {
            blocks.push("nothing to commit, working tree clean".to_owned());
        }
        blocks.join("\n\n")
    }
}

pub fn status_out(wc: &WorkingCopy, s: Status) -> StatusOut {
    let conflict = wc.state().conflict.as_ref();
    StatusOut {
        schema: "scion.cli.status/v1",
        reference: wc.state().reference.clone(),
        base: wc.state().base.clone(),
        pending: wc.state().head.clone(),
        ahead: wc.state().stack.len() + usize::from(wc.state().head.is_some()),
        detached: wc.state().detached,
        added: s.added,
        modified: s.modified,
        deleted: s.deleted,
        conflicted: s.conflicted,
        behind: None,
        kinds: conflict
            .map(|c| {
                c.paths
                    .iter()
                    .map(|(p, sides)| (p.clone(), sides.kind.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        merging: conflict.map(|c| c.merge.is_some()),
        pick: conflict.and_then(|c| c.pick.clone()),
        short: false,
        branch: false,
        cwd: String::new(),
    }
}

/// `ahead_behind`: also ask the server how far its branch has moved past the working copy (as
/// git compares with `origin/BRANCH`). That costs a round trip, two when it moved; offline, or
/// when the server is slow, the status simply leaves it out.
pub async fn status(ctx: &Ctx, short: bool, ahead_behind: bool) -> Result<StatusOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let s = wc.status()?;
    let cwd = wc_relative(wc.root(), &ctx.cwd, &[".".to_owned()])?.remove(0);
    let behind = if ahead_behind && !wc.state().detached {
        tokio::time::timeout(std::time::Duration::from_secs(2), behind(ctx, &wc))
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    Ok(StatusOut {
        short,
        cwd: if cwd == "." { String::new() } else { cwd },
        behind,
        ..status_out(&wc, s)
    })
}

/// Pages of log read from the server's tip to find the working copy's base.
const BEHIND_PAGE: u32 = 100;

async fn behind(ctx: &Ctx, wc: &WorkingCopy) -> Option<Behind> {
    let base = wc.state().base.clone()?;
    let client = connect(&ctx.endpoint_or(Some(wc)), Some(&wc.state().repo)).await.ok()?;
    let tip = client
        .resolve_ref(&wc.state().reference)
        .await
        .ok()??
        .changeset
        .to_string();
    if tip == base {
        return Some(Behind { count: 0, exact: true });
    }
    // History runs newest first, so the commits before the base are the ones it lacks
    let page = client.log(&tip, None, BEHIND_PAGE, None).await.ok()?;
    Some(match page.entries.iter().position(|e| e.id.to_string() == base) {
        Some(count) => Behind { count, exact: true },
        None => Behind {
            count: page.entries.len(),
            exact: false,
        },
    })
}

// ---------- conflicts ----------

#[derive(Debug, Serialize)]
pub struct ResolveOut {
    pub schema: &'static str,
    pub resolved: Vec<String>,
    /// Paths still unresolved.
    pub remaining: Vec<String>,
}

impl Render for ResolveOut {
    fn text(&self) -> String {
        if self.remaining.is_empty() {
            "all conflicts resolved; run `scn commit`".to_owned()
        } else {
            format!("still conflicted: {}", self.remaining.join(", "))
        }
    }

    const REPORTS: bool = true;
}

/// `scn resolve`: paths are relative to the current directory; none lists the conflicts.
pub async fn resolve(
    ctx: &Ctx,
    paths: &[String],
    side: Option<scion_wc::Side>,
    force: bool,
) -> Result<ResolveOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    if paths.is_empty() {
        let remaining: Vec<String> = match &wc.state().conflict {
            Some(c) => c.paths.keys().cloned().collect(),
            None => Vec::new(),
        };
        return Ok(ResolveOut {
            schema: "scion.cli.resolve/v1",
            resolved: Vec::new(),
            remaining,
        });
    }
    let rel = wc_relative(wc.root(), &ctx.cwd, paths)?;
    let before: Vec<String> = match &wc.state().conflict {
        Some(c) => c.paths.keys().cloned().collect(),
        None => Vec::new(),
    };
    let remaining = wc.resolve(&client, &rel, side, force).await?;
    Ok(ResolveOut {
        schema: "scion.cli.resolve/v1",
        resolved: before.into_iter().filter(|p| !remaining.contains(p)).collect(),
        remaining,
    })
}

#[derive(Debug, Serialize)]
pub struct AbortOut {
    pub schema: &'static str,
}

/// Silent, like `git merge --abort`.
impl Render for AbortOut {
    fn text(&self) -> String {
        String::new()
    }

    const REPORTS: bool = true;
}

pub async fn abort_merge(ctx: &Ctx) -> Result<AbortOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    wc.abort_merge(&client).await?;
    Ok(AbortOut {
        schema: "scion.cli.merge-abort/v1",
    })
}

// ---------- commit / land / sync ----------

#[derive(Debug, Serialize)]
pub struct CommitOut {
    pub schema: &'static str,
    /// 没有改动时为 None。
    pub changeset: Option<String>,
    pub generation: Option<u64>,
    /// Lines added and removed in text files; absent when not counted (see [`COUNT_LINES_FILES`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insertions: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletions: Option<usize>,
    /// For the text: the branch, the subject, whether it is the branch's first commit, and the files.
    #[serde(skip)]
    pub summary: Option<CommitSummary>,
}

#[derive(Debug, Clone, Default)]
pub struct CommitSummary {
    pub reference: String,
    pub subject: String,
    pub root: bool,
    pub files: Vec<scion_wc::FileStat>,
}

fn git_mode(m: scion_wc::Mode) -> &'static str {
    match m {
        scion_wc::Mode::File => "100644",
        scion_wc::Mode::Executable => "100755",
        scion_wc::Mode::Symlink => "120000",
    }
}

/// Like git: `[main abc123456789] subject`, ` 2 files changed, 3 insertions(+), 1 deletion(-)`,
/// then created, deleted and mode-changed files.
impl Render for CommitOut {
    fn text(&self) -> String {
        let (Some(c), Some(sum)) = (&self.changeset, &self.summary) else {
            return "nothing to commit, working tree clean".to_owned();
        };
        let root = if sum.root { " (root-commit)" } else { "" };
        let n = sum.files.len();
        let mut s = format!(
            "[{}{root} {}] {}\n {n} file{} changed",
            sum.reference,
            short(c),
            sum.subject,
            if n == 1 { "" } else { "s" }
        );
        let plural = |n: usize| if n == 1 { "" } else { "s" };
        if let Some(add) = self.insertions.filter(|n| *n > 0) {
            s.push_str(&format!(", {add} insertion{}(+)", plural(add)));
        }
        if let Some(del) = self.deletions.filter(|n| *n > 0) {
            s.push_str(&format!(", {del} deletion{}(-)", plural(del)));
        }
        for f in &sum.files {
            match (f.old_mode, f.new_mode) {
                (None, Some(m)) => s.push_str(&format!("\n create mode {} {}", git_mode(m), f.path)),
                (Some(m), None) => s.push_str(&format!("\n delete mode {} {}", git_mode(m), f.path)),
                (Some(a), Some(b)) if a != b => {
                    s.push_str(&format!("\n mode change {} => {} {}", git_mode(a), git_mode(b), f.path));
                }
                _ => {}
            }
        }
        s
    }

    const REPORTS: bool = true;
}

/// Lines are counted only for commits up to this many files, whose old versions (fetched in one
/// batch) add up to at most [`COUNT_LINES_BYTES`], so that a large commit does not wait for
/// downloads; a commit that only adds files needs none.
const COUNT_LINES_FILES: usize = 1000;
const COUNT_LINES_BYTES: u64 = 4 << 20;

/// Lines added and removed by a commit's files, like git's summary; None when over the limits
/// or the old versions cannot be read.
async fn count_lines(client: &Client, files: &[scion_wc::FileStat]) -> Option<(usize, usize)> {
    let old_bytes: u64 = files.iter().filter_map(|f| f.old.map(|(_, size)| size)).sum();
    if files.len() > COUNT_LINES_FILES || old_bytes > COUNT_LINES_BYTES {
        return None;
    }
    let ids: Vec<scion_client::ObjectId> = files.iter().filter_map(|f| f.old.map(|(id, _)| id)).collect();
    let mut olds = if ids.is_empty() {
        Vec::new()
    } else {
        client.get_objects(&ids).await.ok()?
    }
    .into_iter();
    let mut total = (0, 0);
    for f in files {
        let old = if f.old.is_some() { olds.next()? } else { Bytes::new() };
        let new = f.new_content.clone().unwrap_or_default();
        if let Some((add, del)) = crate::history::changed_lines(&old, &new) {
            total = (total.0 + add, total.1 + del);
        }
    }
    Some(total)
}

async fn commit_out(
    client: &Client,
    wc: &WorkingCopy,
    root: bool,
    created: Option<(scion_client::CreatedChangeset, Vec<scion_wc::FileStat>)>,
) -> CommitOut {
    let lines = match &created {
        Some((_, files)) => count_lines(client, files).await,
        None => None,
    };
    let subject = wc
        .state()
        .head_message
        .as_deref()
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("");
    CommitOut {
        schema: "scion.cli.commit/v1",
        changeset: created.as_ref().map(|(c, _)| c.id.to_string()),
        generation: created.as_ref().map(|(c, _)| c.generation),
        insertions: lines.map(|(add, _)| add),
        deletions: lines.map(|(_, del)| del),
        summary: created.map(|(_, files)| CommitSummary {
            reference: wc.state().reference.clone(),
            subject: subject.to_owned(),
            root,
            files,
        }),
    }
}

/// `scn commit --amend`: replaces the pending commit. Unlike a new commit, the default author is
/// the old commit's author, not the configured one.
pub async fn amend(ctx: &Ctx, message: Option<&str>, author: Option<&str>) -> Result<CommitOut, CliError> {
    let author = author.map(parse_author).transpose()?;
    let (mut wc, client) = open_wc(ctx).await?;
    let root = wc.state().base.is_none();
    let created = wc
        .amend_with_stat(&client, message, author.as_ref().map(|(n, e)| (n.as_str(), e.as_str())))
        .await?;
    Ok(commit_out(&client, &wc, root, created).await)
}

#[derive(Debug, Serialize)]
pub struct UncommitOut {
    pub schema: &'static str,
    /// The dropped commit.
    pub changeset: String,
}

impl Render for UncommitOut {
    fn text(&self) -> String {
        format!(
            "uncommitted {}; its changes are now uncommitted local changes",
            short(&self.changeset)
        )
    }

    const REPORTS: bool = true;
}

pub async fn uncommit(ctx: &Ctx) -> Result<UncommitOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let id = wc.uncommit(&client).await?;
    Ok(UncommitOut {
        schema: "scion.cli.uncommit/v1",
        changeset: id.to_string(),
    })
}

#[derive(Debug, Serialize)]
pub struct RestoreOut {
    pub schema: &'static str,
    pub restored: Vec<String>,
    /// Added files under the given paths; restore leaves them alone.
    pub kept_added: Vec<String>,
}

/// Silent like `git restore`, except for a note about added files it left alone.
impl Render for RestoreOut {
    fn text(&self) -> String {
        if self.kept_added.is_empty() {
            return String::new();
        }
        format!(
            "kept {} added file(s) (restore never deletes files): {}",
            self.kept_added.len(),
            self.kept_added.join(", ")
        )
    }

    const REPORTS: bool = true;
}

#[derive(Debug, Serialize)]
pub struct StashOut {
    pub schema: &'static str,
    /// What was saved, applied or dropped; for a list, every entry, newest first.
    pub entries: Vec<StashItem>,
    /// `pop`: whether the entry was dropped after applying.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropped: Option<bool>,
    #[serde(skip)]
    pub action: StashAction,
}

#[derive(Debug, Serialize)]
pub struct StashItem {
    pub index: usize,
    pub commit: String,
    pub message: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum StashAction {
    #[default]
    Push,
    List,
    Apply,
    Drop,
}

/// git's lines: `Saved working directory and index state MESSAGE`, `stash@{N}: MESSAGE`,
/// `Dropped stash@{N} (ID)`.
impl Render for StashOut {
    fn text(&self) -> String {
        let line = |e: &StashItem| match self.action {
            StashAction::Push => format!("Saved working directory and index state {}", e.message),
            StashAction::List => format!("stash@{{{}}}: {}", e.index, e.message),
            StashAction::Apply if self.dropped != Some(true) => format!("Applied stash@{{{}}}", e.index),
            StashAction::Apply | StashAction::Drop => format!("Dropped stash@{{{}}} ({})", e.index, short(&e.commit)),
        };
        match (self.action, self.entries.is_empty()) {
            (StashAction::Push, true) => "No local changes to save".to_owned(),
            _ => self.entries.iter().map(line).collect::<Vec<_>>().join("\n"),
        }
    }

    fn reports(&self) -> bool {
        !matches!(self.action, StashAction::List)
    }
}

/// `stash@{N}`, or just `N`; the newest (0) when absent.
pub fn stash_index(spec: Option<&str>) -> Result<usize, CliError> {
    let Some(spec) = spec else { return Ok(0) };
    let n = spec
        .strip_prefix("stash@{")
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(spec);
    n.parse()
        .map_err(|_| CliError::Usage(format!("{spec:?} is not a stash entry: use stash@{{N}} or N")))
}

fn stash_item(index: usize, e: &scion_wc::StashEntry) -> StashItem {
    StashItem {
        index,
        commit: e.commit.clone(),
        message: e.message.clone(),
    }
}

/// `scn stash [push] [-m MESSAGE]`.
pub async fn stash_push(ctx: &Ctx, message: Option<&str>) -> Result<StashOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let saved = wc.stash_push(&client, message, None).await?;
    Ok(StashOut {
        schema: "scion.cli.stash/v1",
        entries: saved.iter().map(|e| stash_item(0, e)).collect(),
        dropped: None,
        action: StashAction::Push,
    })
}

/// `scn stash list`: needs no server.
pub fn stash_list(ctx: &Ctx) -> Result<StashOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    Ok(StashOut {
        schema: "scion.cli.stash/v1",
        entries: wc
            .state()
            .stash
            .iter()
            .enumerate()
            .map(|(i, e)| stash_item(i, e))
            .collect(),
        dropped: None,
        action: StashAction::List,
    })
}

/// `scn stash apply` / `pop`: conflicts fail with the conflict exit code, the entry kept.
pub async fn stash_apply(ctx: &Ctx, spec: Option<&str>, pop: bool) -> Result<StashOut, CliError> {
    let index = stash_index(spec)?;
    let (mut wc, client) = open_wc(ctx).await?;
    let applied = wc.stash_apply(&client, index, pop).await?;
    if !applied.conflicts.is_empty() {
        return Err(CliError::Conflicted {
            op: ConflictOp::Stash(index),
            reference: wc.state().reference.clone(),
            conflicts: applied
                .conflicts
                .into_iter()
                .map(|path| ConflictOut {
                    kind: "content".to_owned(),
                    path,
                })
                .collect(),
        });
    }
    Ok(StashOut {
        schema: "scion.cli.stash/v1",
        entries: vec![stash_item(index, &applied.entry)],
        dropped: Some(applied.dropped),
        action: StashAction::Apply,
    })
}

/// `scn stash drop`: needs no server.
pub fn stash_drop(ctx: &Ctx, spec: Option<&str>) -> Result<StashOut, CliError> {
    let index = stash_index(spec)?;
    let mut wc = WorkingCopy::open(&ctx.cwd)?;
    let e = wc.stash_drop(index)?;
    Ok(StashOut {
        schema: "scion.cli.stash/v1",
        entries: vec![stash_item(index, &e)],
        dropped: Some(true),
        action: StashAction::Drop,
    })
}

/// The commit of stash entry `spec`, for `scn stash show`.
pub fn stash_commit(ctx: &Ctx, spec: Option<&str>) -> Result<String, CliError> {
    let index = stash_index(spec)?;
    let wc = WorkingCopy::open(&ctx.cwd)?;
    Ok(wc
        .state()
        .stash
        .get(index)
        .ok_or(scion_wc::Error::NoStash(index))?
        .commit
        .clone())
}

#[derive(Debug, Serialize)]
pub struct CleanOut {
    pub schema: &'static str,
    pub dry_run: bool,
    /// Removed (or, with dry_run, to be removed); directories removed whole end in `/`.
    pub removed: Vec<String>,
}

/// git's lines: `Would remove PATH` (-n), `Removing PATH`.
impl Render for CleanOut {
    fn text(&self) -> String {
        let verb = if self.dry_run { "Would remove" } else { "Removing" };
        self.removed
            .iter()
            .map(|p| format!("{verb} {p}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn reports(&self) -> bool {
        !self.dry_run
    }
}

/// `scn clean`: untracked files (with `ignored`, ignored ones too) under `paths`, relative to the
/// current directory; everything when there are none.
pub async fn clean(ctx: &Ctx, paths: &[String], ignored: bool, dry_run: bool) -> Result<CleanOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let rel = if paths.is_empty() {
        vec![".".to_owned()]
    } else {
        wc_relative(wc.root(), &ctx.cwd, paths)?
    };
    Ok(CleanOut {
        schema: "scion.cli.clean/v1",
        dry_run,
        removed: wc.clean(&rel, ignored, dry_run)?,
    })
}

/// Paths are relative to the current directory, like other commands that take local paths.
/// With `source`, from that revision instead of the working copy's commit (`--source`).
pub async fn restore(ctx: &Ctx, paths: &[String], source: Option<&str>) -> Result<RestoreOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let rel = wc_relative(wc.root(), &ctx.cwd, paths)?;
    let r = match source {
        Some(rev) => {
            let root = client.root_tree(&ctx.expand_rev(rev)?).await?;
            wc.restore_from(&client, root, &rel).await?
        }
        None => wc.restore(&client, &rel).await?,
    };
    Ok(RestoreOut {
        schema: "scion.cli.restore/v1",
        restored: r.restored,
        kept_added: r.kept_added,
    })
}

/// Converts paths given relative to `cwd` into working-copy paths (`/`-separated, no `..`).
pub(crate) fn wc_relative(root: &Path, cwd: &Path, paths: &[String]) -> Result<Vec<String>, CliError> {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let cwd = cwd.canonicalize().unwrap_or_else(|_| cwd.to_path_buf());
    let base = cwd
        .strip_prefix(&root)
        .map_err(|_| CliError::Usage(format!("{} is outside the working copy", cwd.display())))?;
    paths
        .iter()
        .map(|p| {
            let mut parts: Vec<String> = base
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect();
            for part in p.split('/') {
                match part {
                    "" | "." => {}
                    ".." => {
                        parts
                            .pop()
                            .ok_or_else(|| CliError::Usage(format!("{p:?} is outside the working copy")))?;
                    }
                    other => parts.push(other.to_owned()),
                }
            }
            Ok(if parts.is_empty() {
                ".".to_owned()
            } else {
                parts.join("/")
            })
        })
        .collect()
}

#[derive(Debug, Serialize)]
pub struct SwitchOut {
    pub schema: &'static str,
    #[serde(rename = "ref")]
    pub reference: String,
    pub created: bool,
    pub tip: Option<String>,
    pub written: usize,
    pub removed: usize,
    /// Already on that branch.
    #[serde(skip)]
    pub already: bool,
    /// `--detach`: on no branch, at `tip`.
    pub detached: bool,
}

/// Like `git switch`.
impl Render for SwitchOut {
    fn text(&self) -> String {
        if self.detached {
            format!("HEAD is now at {}", short(self.tip.as_deref().unwrap_or_default()))
        } else if self.created {
            format!("Switched to a new branch '{}'", self.reference)
        } else if self.already {
            format!("Already on '{}'", self.reference)
        } else {
            format!("Switched to branch '{}'", self.reference)
        }
    }

    const REPORTS: bool = true;
}

pub async fn switch(ctx: &Ctx, reference: &str, create: bool) -> Result<SwitchOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let already = wc.state().reference == reference && !wc.state().detached;
    let s = wc.switch(&client, reference, create).await?;
    Ok(SwitchOut {
        schema: "scion.cli.switch/v1",
        reference: reference.to_owned(),
        created: create,
        tip: s.tip.map(|t| t.to_string()),
        written: s.written,
        removed: s.removed,
        already,
        detached: false,
    })
}

/// `scn switch --detach REV`: the working tree becomes REV's files, on no branch.
pub async fn switch_detach(ctx: &Ctx, revision: &str) -> Result<SwitchOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let commit = client.resolve_revision(&ctx.expand_rev(revision)?).await?.id;
    let s = wc.detach(&client, commit).await?;
    Ok(SwitchOut {
        schema: "scion.cli.switch/v1",
        reference: wc.state().reference.clone(),
        created: false,
        tip: Some(commit.to_string()),
        written: s.written,
        removed: s.removed,
        already: false,
        detached: true,
    })
}

#[derive(Debug, Serialize)]
pub struct MergeOut {
    pub schema: &'static str,
    /// The pending merge commit; None when already up to date.
    pub changeset: Option<String>,
    pub written: usize,
    pub removed: usize,
}

/// Like `git merge`; the merge commit is pending until `scn land`.
impl Render for MergeOut {
    fn text(&self) -> String {
        if self.changeset.is_none() {
            return "Already up to date.".to_owned();
        }
        let n = self.written + self.removed;
        format!(
            "Merge made by the 'scion' strategy.\n {n} file{} changed",
            if n == 1 { "" } else { "s" }
        )
    }

    const REPORTS: bool = true;
}

pub async fn merge(ctx: &Ctx, other: &str, message: Option<&str>, author: Option<&str>) -> Result<MergeOut, CliError> {
    let author = resolve_author(author)?;
    let (mut wc, client) = open_wc(ctx).await?;
    let o = wc
        .merge(
            &client,
            other,
            message,
            author.as_ref().map(|(n, e)| (n.as_str(), e.as_str())),
        )
        .await
        .map_err(|e| conflicted(&wc, ConflictOp::Merge, e))?;
    Ok(match o {
        scion_wc::MergeOutcome::UpToDate => MergeOut {
            schema: "scion.cli.merge/v1",
            changeset: None,
            written: 0,
            removed: 0,
        },
        scion_wc::MergeOutcome::Merged { head, written, removed } => MergeOut {
            schema: "scion.cli.merge/v1",
            changeset: Some(head.to_string()),
            written,
            removed,
        },
    })
}

/// What status says a revert or cherry-pick is doing, and its command.
fn pick_words(kind: PickKind) -> (&'static str, &'static str) {
    match kind {
        PickKind::Revert => ("reverting", "revert"),
        PickKind::CherryPick => ("cherry-picking", "cherry-pick"),
    }
}

#[derive(Debug, Serialize)]
pub struct PickOut {
    pub schema: &'static str,
    /// The commit reverted or picked.
    pub commit: String,
    /// The pending commit; None when the branch already has the result.
    pub changeset: Option<String>,
    pub written: usize,
    pub removed: usize,
    /// For the text: the branch, the new commit's subject, and the kind.
    #[serde(skip)]
    pub reference: String,
    #[serde(skip)]
    pub subject: String,
    #[serde(skip)]
    pub kind: PickKind,
}

/// Like `git revert`: `[main ID] SUBJECT`, then how many files changed.
impl Render for PickOut {
    fn text(&self) -> String {
        let Some(id) = &self.changeset else {
            let (doing, _) = pick_words(self.kind);
            return format!(
                "nothing to commit: {} already has the result of {doing} {}",
                self.reference,
                short(&self.commit)
            );
        };
        let n = self.written + self.removed;
        format!(
            "[{} {}] {}\n {n} file{} changed",
            self.reference,
            short(id),
            self.subject,
            if n == 1 { "" } else { "s" }
        )
    }

    const REPORTS: bool = true;
}

/// The parent a revert or cherry-pick compares `commit` with: its only parent, or for a merge
/// the one `mainline` names (1 is the first), as git's `-m`.
fn pick_parent(
    commit: &scion_client::LogItem,
    mainline: Option<usize>,
) -> Result<Option<scion_client::ObjectId>, CliError> {
    let id = short(&commit.id.to_string()).to_owned();
    match (commit.parents.len(), mainline) {
        (0 | 1, None) => Ok(commit.parents.first().copied()),
        (0 | 1, Some(_)) => Err(CliError::Usage(format!(
            "{id} is not a merge commit; -m is only for merges"
        ))),
        (_, None) => Err(CliError::Usage(format!(
            "{id} is a merge commit; pass -m 1 to keep what its first parent had (the branch it was \
             merged into), or -m N for parent N"
        ))),
        (n, Some(m)) if m == 0 || m > n => Err(CliError::Usage(format!("{id} has {n} parents; -m takes 1 to {n}"))),
        (_, Some(m)) => Ok(Some(commit.parents[m - 1])),
    }
}

/// `scn revert` / `scn cherry-pick`: a pending commit that undoes or repeats `revision`. A
/// cherry-pick keeps the commit's author unless `author` is given, and with `record_origin`
/// (git's `-x`) names the commit in the message.
pub async fn pick(
    ctx: &Ctx,
    kind: PickKind,
    revision: &str,
    mainline: Option<usize>,
    author: Option<&str>,
    record_origin: bool,
) -> Result<PickOut, CliError> {
    let given = match (kind, author) {
        (PickKind::CherryPick, None) => None,
        (_, a) => resolve_author(a)?,
    };
    let (mut wc, client) = open_wc(ctx).await?;
    let rev = ctx.expand_rev(revision)?;
    let commit = client
        .log(&rev, None, 1, None)
        .await?
        .entries
        .into_iter()
        .next()
        .ok_or_else(|| CliError::Usage(format!("{revision} has no history")))?;
    let parent = pick_parent(&commit, mainline)?;
    let subject = crate::output::subject(&commit.message);
    let message = match kind {
        PickKind::Revert => format!("Revert \"{subject}\"\n\nThis reverts commit {}.", commit.id),
        PickKind::CherryPick if record_origin => format!(
            "{}\n\n(cherry picked from commit {})",
            commit.message.trim_end(),
            commit.id
        ),
        PickKind::CherryPick => commit.message.clone(),
    };
    // A cherry-pick keeps the author, with the time it was written; whoever applies it commits
    let (author, committer) = match given {
        Some((name, email)) => (Some(signature_now(name, email)), None),
        None if kind == PickKind::CherryPick => (
            Some(commit.author.clone()),
            own_author().map(|(name, email)| signature_now(name, email)),
        ),
        None => (None, None),
    };
    let new_subject = crate::output::subject(&message);
    let o = wc
        .pick(&client, kind, commit.id, parent, message, author, committer)
        .await
        .map_err(|e| conflicted(&wc, ConflictOp::Pick(kind, commit.id.to_string()), e))?;
    let (changeset, written, removed) = match o {
        scion_wc::PickOutcome::Empty => (None, 0, 0),
        scion_wc::PickOutcome::Picked { head, written, removed } => (Some(head.to_string()), written, removed),
    };
    Ok(PickOut {
        schema: "scion.cli.pick/v1",
        commit: commit.id.to_string(),
        changeset,
        written,
        removed,
        reference: wc.state().reference.clone(),
        subject: new_subject,
        kind,
    })
}

/// A signature made now, in UTC.
pub(crate) fn signature_now(name: String, email: String) -> scion_client::Signature {
    let time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    scion_client::Signature {
        name,
        email,
        time_ms,
        tz_offset_minutes: 0,
    }
}

/// Who makes new commits here: SCION_AUTHOR, else the configured author; no hint when unset.
pub(crate) fn own_author() -> Option<(String, String)> {
    if let Some(a) = std::env::var("SCION_AUTHOR").ok().and_then(|a| parse_author(&a).ok()) {
        return Some(a);
    }
    let settings = crate::config::load(&crate::config::path(|k| std::env::var(k).ok())).ok()?;
    crate::config::author(&settings, crate::config::git_config)
}

/// `scn sync --abort`: only while a sync waits on its conflicts.
pub async fn abort_sync(ctx: &Ctx) -> Result<AbortOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    wc.abort_sync(&client).await?;
    Ok(AbortOut {
        schema: "scion.cli.sync-abort/v1",
    })
}

/// `scn revert --abort`: only while a revert waits on its conflicts.
pub async fn abort_pick(ctx: &Ctx, kind: PickKind) -> Result<AbortOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let current = wc
        .state()
        .conflict
        .as_ref()
        .and_then(|c| c.pick.as_ref())
        .map(|p| p.kind);
    if current != Some(kind) {
        return Err(CliError::Usage(format!("no {} is in progress", pick_words(kind).1)));
    }
    wc.abort_merge(&client).await?;
    Ok(AbortOut {
        schema: "scion.cli.pick-abort/v1",
    })
}

/// A sync, merge, revert or cherry-pick that stopped on conflicts, with each path's kind from
/// the saved state.
fn conflicted(wc: &WorkingCopy, op: ConflictOp, e: scion_wc::Error) -> CliError {
    let scion_wc::Error::Conflict(paths) = e else {
        return e.into();
    };
    let kinds = wc.state().conflict.as_ref().map(|c| &c.paths);
    CliError::Conflicted {
        op,
        reference: wc.state().reference.clone(),
        conflicts: paths
            .into_iter()
            .map(|path| ConflictOut {
                kind: kinds
                    .and_then(|k| k.get(&path))
                    .map_or_else(|| "content".to_owned(), |s| s.kind.clone()),
                path,
            })
            .collect(),
    }
}

/// `--author`, else the configured author (scn config, then git); a hint when there is none.
fn resolve_author(author: Option<&str>) -> Result<Option<(String, String)>, CliError> {
    Ok(match author {
        Some(a) => Some(parse_author(a)?),
        None => {
            let settings = crate::config::load(&crate::config::path(|k| std::env::var(k).ok()))?;
            let found = crate::config::author(&settings, crate::config::git_config);
            if found.is_none() {
                eprintln!(
                    "hint: no author configured; set one with `scn config set user.name \"Your Name\"` \
                     and `scn config set user.email you@example.com` (or pass --author)"
                );
            }
            found
        }
    })
}

/// `message` may be omitted only when finishing a sync or merge that stopped on conflicts (it
/// then defaults to that commit's message).
pub async fn commit(ctx: &Ctx, message: Option<&str>, author: Option<&str>) -> Result<CommitOut, CliError> {
    commit_paths(ctx, message, author, &[]).await
}

/// [`commit`] of only the changes at or under `paths` (relative to the current directory);
/// every change when there are none.
pub async fn commit_paths(
    ctx: &Ctx,
    message: Option<&str>,
    author: Option<&str>,
    paths: &[String],
) -> Result<CommitOut, CliError> {
    if let Some(a) = author {
        parse_author(a)?;
    }
    let (mut wc, client) = open_wc(ctx).await?;
    let message = match (message, &wc.state().conflict) {
        (Some(m), _) => m.to_owned(),
        (None, Some(_)) => String::new(),
        (None, None) if paths.is_empty() => return Err(CliError::Usage("a commit message is required (-m)".into())),
        (None, None) => {
            return Err(CliError::Usage(
                "a commit message is required: it goes after -m (`scn commit -m \"...\" PATH...`)".into(),
            ));
        }
    };
    let message = message.as_str();
    // The finished sync or merge keeps its own author unless one is given
    let author = match (author, &wc.state().conflict) {
        (None, Some(c)) if c.author.is_some() => None,
        (a, _) => resolve_author(a)?,
    };
    let root = wc.state().base.is_none() && wc.state().head.is_none();
    // Finishing a sync goes on rebasing the commits after it, which may stop on conflicts again
    let author = author.as_ref().map(|(n, e)| (n.as_str(), e.as_str()));
    let created = if paths.is_empty() {
        wc.commit_with_stat(&client, message, author).await
    } else {
        let rel = wc_relative(wc.root(), &ctx.cwd, paths)?;
        wc.commit_paths_with_stat(&client, message, author, &rel).await
    }
    .map_err(|e| conflicted(&wc, ConflictOp::Sync, e))?;
    Ok(commit_out(&client, &wc, root, created).await)
}

#[derive(Debug, Serialize)]
pub struct LandOut {
    pub schema: &'static str,
    #[serde(rename = "ref")]
    pub reference: String,
    pub tip: String,
    pub version: u64,
    /// 服务端是否 rebase 了提交（ref 在提交之后前进过）。
    pub rebased: bool,
    /// The commits the server rebased: the id committed and the id it landed as.
    pub rewritten: Vec<Rewritten>,
    /// For the text: the repository's URL, the branch's commit before (None: a new branch), and
    /// whether the commit was already on the branch.
    #[serde(skip)]
    pub url: String,
    #[serde(skip)]
    pub old: Option<String>,
    #[serde(skip)]
    pub up_to_date: bool,
}

#[derive(Debug, Serialize)]
pub struct Rewritten {
    pub committed: String,
    pub landed: String,
}

/// Like `git push`: `To URL`, then `   old..new  main -> main`, then the commits that the
/// server rebased and so landed under a new id.
impl Render for LandOut {
    fn text(&self) -> String {
        if self.up_to_date {
            return "Everything up-to-date".to_owned();
        }
        let r = &self.reference;
        let update = match &self.old {
            Some(old) => format!("   {}..{}  {r} -> {r}", short(old), short(&self.tip)),
            None => format!(" * [new branch]      {r} -> {r}"),
        };
        let rebased = if self.rebased {
            format!(" (rebased onto the latest {r})")
        } else {
            String::new()
        };
        let mut out = format!("To {}\n{update}{rebased}", self.url);
        for r in &self.rewritten {
            out.push_str(&format!(
                "\nYour commit {} is now {}",
                short(&r.committed),
                short(&r.landed)
            ));
        }
        out
    }

    const REPORTS: bool = true;
}

pub async fn land(ctx: &Ctx) -> Result<LandOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let mut old = wc.state().base.clone();
    let landed = wc.land(&client).await?;
    // After a rebase the range starts where the branch was on the server, as git shows the
    // remote's previous tip: the parent of the first commit landed. Only the text needs it,
    // so a failed lookup keeps the local base
    if let Some(first) = landed.landed.first().filter(|_| landed.rebased()) {
        if let Ok(r) = client.resolve_revision(&first.landed.to_string()).await {
            old = r.parents.first().map(|p| p.to_string()).or(old);
        }
    }
    Ok(LandOut {
        schema: "scion.cli.land/v1",
        reference: wc.state().reference.clone(),
        tip: landed.tip.to_string(),
        version: landed.version,
        rebased: landed.rebased(),
        rewritten: landed
            .landed
            .iter()
            .filter(|l| l.changeset != l.landed)
            .map(|l| Rewritten {
                committed: l.changeset.to_string(),
                landed: l.landed.to_string(),
            })
            .collect(),
        url: format!(
            "{}/{}",
            ctx.endpoint_or(Some(&wc)).trim_end_matches('/'),
            wc.state().repo
        ),
        old,
        up_to_date: landed.landed.is_empty(),
    })
}

#[derive(Debug, Serialize)]
pub struct SyncOut {
    pub schema: &'static str,
    /// up_to_date | updated | rebased
    pub outcome: &'static str,
    pub base: Option<String>,
    pub pending: Option<String>,
    pub written: usize,
    pub removed: usize,
    /// For the text: the branch, and the commit the working copy was on before.
    #[serde(skip)]
    pub reference: String,
    #[serde(skip)]
    pub old: Option<String>,
}

/// Like `git pull`: `Already up to date.`, `Updating a..b` / `Fast-forward`, or for a pending
/// commit `git pull --rebase`'s `Successfully rebased and updated main.`
impl Render for SyncOut {
    fn text(&self) -> String {
        let n = self.written + self.removed;
        let stat = format!(" {n} file{} changed", if n == 1 { "" } else { "s" });
        let updating = match (&self.old, &self.base) {
            (Some(old), Some(new)) => format!("Updating {}..{}\n", short(old), short(new)),
            _ => String::new(),
        };
        match self.outcome {
            "up_to_date" => "Already up to date.".to_owned(),
            "rebased" => format!("{updating}{stat}\nSuccessfully rebased and updated {}.", self.reference),
            _ => format!("{updating}Fast-forward\n{stat}"),
        }
    }

    const REPORTS: bool = true;
}

pub fn sync_out(wc: &WorkingCopy, old: Option<String>, o: &SyncOutcome) -> SyncOut {
    let (outcome, written, removed) = match o {
        SyncOutcome::UpToDate => ("up_to_date", 0, 0),
        SyncOutcome::Updated { written, removed, .. } => ("updated", *written, *removed),
        SyncOutcome::Rebased { written, removed, .. } => ("rebased", *written, *removed),
    };
    SyncOut {
        schema: "scion.cli.sync/v1",
        outcome,
        base: wc.state().base.clone(),
        pending: wc.state().head.clone(),
        written,
        removed,
        reference: wc.state().reference.clone(),
        old,
    }
}

pub async fn sync(ctx: &Ctx) -> Result<SyncOut, CliError> {
    let (mut wc, client) = open_wc(ctx).await?;
    let old = wc.state().base.clone();
    let o = wc
        .sync(&client)
        .await
        .map_err(|e| conflicted(&wc, ConflictOp::Sync, e))?;
    Ok(sync_out(&wc, old, &o))
}

// ---------- import ----------

#[derive(Debug, Serialize)]
pub struct ImportOut {
    pub schema: &'static str,
    pub repo: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub commits: usize,
    /// Commits created by this import (the others were already on the server).
    pub new_commits: usize,
    pub skipped_gitlinks: usize,
    pub tip: Option<String>,
}

impl Render for ImportOut {
    fn text(&self) -> String {
        let mut s = match &self.tip {
            Some(t) => format!(
                "imported {} commits into {} ({} at {}; {} new, {} already there)",
                self.commits,
                self.repo,
                self.reference,
                short(t),
                self.new_commits,
                self.commits - self.new_commits
            ),
            None => format!("nothing to import into {}", self.repo),
        };
        if self.skipped_gitlinks > 0 {
            s.push_str(&format!(
                "\nskipped {} submodule entries (not supported yet)",
                self.skipped_gitlinks
            ));
        }
        s
    }

    const REPORTS: bool = true;
}

/// 把本地 git 仓库的历史导入 Scion。
pub async fn import_git(ctx: &Ctx, git_dir: &Path, git_ref: &str, target_ref: &str) -> Result<ImportOut, CliError> {
    let repo = ctx
        .repo
        .clone()
        .ok_or_else(|| CliError::Usage("--repo is required for import".into()))?;
    let endpoint = ctx.endpoint.clone().unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned());
    let client = connect(&endpoint, Some(&repo)).await?;
    let meter = Meter::stderr(ctx.chatty);
    let summary = scion_gitimport::import(&client, &ctx.cwd.join(git_dir), git_ref, target_ref, |done, total| {
        meter.update("Importing commits", done as u64, Some(total as u64), None);
    })
    .await?;
    meter.done();
    Ok(ImportOut {
        schema: "scion.cli.import/v1",
        repo,
        reference: target_ref.to_owned(),
        commits: summary.commits,
        new_commits: summary.new_commits,
        skipped_gitlinks: summary.skipped_gitlinks,
        tip: summary.tip.map(|t| t.to_string()),
    })
}

// ---------- cat / ls（不需要工作副本） ----------

/// 拆分 `rev:path`；没有 `:` 时整个视为路径，rev 用默认值。
pub(crate) fn split_spec<'a>(spec: &'a str, default_rev: &'a str) -> (&'a str, &'a str) {
    match spec.split_once(':') {
        Some((rev, path)) if !rev.is_empty() => (rev, path),
        Some((_, path)) => (default_rev, path),
        None => (default_rev, spec),
    }
}

/// `[REV:][DIR]` for commands that take a tree (`ls`, `archive`): like [`split_spec`], but a bare
/// argument that names a revision is the revision, as in `git archive main` / `git ls-tree main`.
/// `:DIR` forces a path.
pub(crate) async fn split_tree_spec(
    client: &Client,
    ctx: &Ctx,
    spec: &str,
    default_rev: &str,
) -> Result<(String, String), CliError> {
    if spec.is_empty() || spec.contains(':') {
        let (rev, path) = split_spec(spec, default_rev);
        return Ok((ctx.expand_rev(rev)?, path.to_owned()));
    }
    let rev = ctx.expand_rev(spec)?;
    match client.resolve_revision(&rev).await {
        Ok(_) => Ok((rev, String::new())),
        Err(e) if e.is_not_found() => Ok((ctx.expand_rev(default_rev)?, spec.to_owned())),
        Err(e) => Err(e.into()),
    }
}

/// 不需要工作副本的命令：仓库来自 --repo 或当前工作副本。
pub(crate) async fn query_client(ctx: &Ctx) -> Result<(Client, String), CliError> {
    let wc = ctx.working_copy();
    let repo = ctx
        .repo
        .clone()
        .or_else(|| wc.as_ref().map(|w| w.state().repo.clone()))
        .ok_or_else(|| CliError::Usage("--repo is required outside a working copy".into()))?;
    let endpoint = ctx.endpoint_or(wc.as_ref());
    // As in git, revisions default to the working copy's current commit (HEAD), which may not
    // be the branch's tip on the server yet; the branch's name (`main`) names that tip. Only for
    // the working copy's own repository: `--repo` can name another one
    let own = wc.as_ref().filter(|w| {
        w.state().repo == repo && w.state().endpoint.trim_end_matches('/') == endpoint.trim_end_matches('/')
    });
    let default_rev = match own {
        Some(w) if w.state().current()?.is_some() => "HEAD".to_owned(),
        Some(w) => w.state().reference.clone(),
        None => DEFAULT_REF.to_owned(),
    };
    Ok((connect(&endpoint, Some(&repo)).await?, default_rev))
}

#[derive(Debug, Serialize)]
pub struct CatOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: String,
    pub size: usize,
    /// 内容不是合法 UTF-8 时为 true，此时 `content` 为有损转换结果。
    pub binary: bool,
    pub content: String,
    #[serde(skip)]
    pub data: Bytes,
}

impl Render for CatOut {
    fn text(&self) -> String {
        self.content.clone()
    }
}

pub async fn cat(ctx: &Ctx, spec: &str) -> Result<CatOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let (rev, path) = split_spec(spec, &default_rev);
    let rev = ctx.expand_rev(rev)?;
    let rev = rev.as_str();
    let data = client.read_file(rev, path).await?;
    let binary = std::str::from_utf8(&data).is_err();
    Ok(CatOut {
        schema: "scion.cli.cat/v1",
        revision: rev.to_owned(),
        path: path.to_owned(),
        size: data.len(),
        binary,
        content: String::from_utf8_lossy(&data).into_owned(),
        data,
    })
}

#[derive(Debug, Serialize)]
pub struct ArchiveOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: String,
    pub changeset: String,
    pub format: String,
    pub bytes: u64,
    /// 输出文件；为空表示写到了标准输出。
    pub output: Option<String>,
}

/// Silent, like `git archive -o FILE`.
impl Render for ArchiveOut {
    fn text(&self) -> String {
        String::new()
    }
}

/// Passes writes through, showing how many bytes went by.
struct Counting<'a, W> {
    inner: W,
    bytes: u64,
    meter: &'a Meter,
}

impl<W: std::io::Write> std::io::Write for Counting<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes += n as u64;
        self.meter.update("Receiving archive", 0, None, Some(self.bytes));
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// 打包下载 `[REV:][DIR]`；`output` 为空时写到 `out`（标准输出）。
pub async fn archive(
    ctx: &Ctx,
    spec: Option<&str>,
    format: &str,
    prefix: &str,
    output: Option<&Path>,
    out: &mut (dyn std::io::Write + Send),
) -> Result<ArchiveOut, CliError> {
    let fmt = scion_client::ArchiveFormat::parse(format)
        .ok_or_else(|| CliError::Usage(format!("unknown archive format {format:?} (tar.gz, zip or tar)")))?;
    let (client, default_rev) = query_client(ctx).await?;
    let (rev, path) = split_tree_spec(&client, ctx, spec.unwrap_or(""), &default_rev).await?;
    let (rev, path) = (rev.as_str(), path.as_str());
    let meter = Meter::stderr(ctx.chatty);
    let (changeset, bytes) = match output {
        Some(file) => {
            let f =
                std::fs::File::create(file).map_err(|e| CliError::Usage(format!("create {}: {e}", file.display())))?;
            let mut w = Counting {
                inner: std::io::BufWriter::new(f),
                bytes: 0,
                meter: &meter,
            };
            let r = client.archive(rev, path, fmt, prefix, &mut w).await;
            let flushed = std::io::Write::flush(&mut w);
            if r.is_err() {
                // 不留下不完整的归档
                let _ = std::fs::remove_file(file);
            }
            let r = r?;
            flushed.map_err(|e| CliError::Usage(format!("write {}: {e}", file.display())))?;
            r
        }
        None => {
            let mut w = Counting {
                inner: out,
                bytes: 0,
                meter: &meter,
            };
            client.archive(rev, path, fmt, prefix, &mut w).await?
        }
    };
    meter.done();
    Ok(ArchiveOut {
        schema: "scion.cli.archive/v1",
        revision: rev.to_owned(),
        path: path.to_owned(),
        changeset: changeset.to_string(),
        format: format.to_owned(),
        bytes,
        output: output.map(|p| p.display().to_string()),
    })
}

#[derive(Debug, Serialize)]
pub struct LsEntry {
    pub name: String,
    /// file | executable | symlink | dir
    pub kind: &'static str,
    pub size: u64,
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct LsOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: String,
    pub entries: Vec<LsEntry>,
}

impl Render for LsOut {
    fn text(&self) -> String {
        self.entries
            .iter()
            .map(|e| match e.kind {
                "dir" => format!("{:>10}  {}/", "-", e.name),
                "executable" => format!("{:>10}  {}*", e.size, e.name),
                "symlink" => format!("{:>10}  {}@", e.size, e.name),
                _ => format!("{:>10}  {}", e.size, e.name),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

pub fn kind(m: EntryMode) -> &'static str {
    match m {
        EntryMode::File => "file",
        EntryMode::Executable => "executable",
        EntryMode::Symlink => "symlink",
        EntryMode::Tree => "dir",
    }
}

pub async fn ls(ctx: &Ctx, spec: Option<&str>) -> Result<LsOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let (rev, path) = split_tree_spec(&client, ctx, spec.unwrap_or(""), &default_rev).await?;
    let (rev, path) = (rev.as_str(), path.as_str());
    let entries = client.list_dir(rev, path).await?;
    Ok(LsOut {
        schema: "scion.cli.ls/v1",
        revision: rev.to_owned(),
        path: path.to_owned(),
        entries: entries
            .into_iter()
            .map(|e| LsEntry {
                name: e.name,
                kind: kind(e.mode),
                size: e.size,
                id: e.id.to_string(),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wc_relative_paths() {
        let d = tempfile::tempdir().expect("tempdir");
        let root = d.path().join("wc");
        std::fs::create_dir_all(root.join("src/a")).expect("mkdir");
        let rel =
            |cwd: &Path, p: &[&str]| wc_relative(&root, cwd, &p.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>());
        assert_eq!(
            rel(&root, &["src/x.rs", ".", "src/"]).expect("ok"),
            ["src/x.rs", ".", "src"]
        );
        assert_eq!(
            rel(&root.join("src/a"), &["../b.rs", ".", "..", "../.."]).expect("ok"),
            ["src/b.rs", "src/a", "src", "."]
        );
        assert!(matches!(rel(&root, &["../x"]), Err(CliError::Usage(_))));
        assert!(matches!(rel(d.path(), &["x"]), Err(CliError::Usage(_))), "cwd outside");
    }

    #[test]
    fn parse_author_cases() {
        assert_eq!(
            parse_author("Ada Lovelace <ada@example.com>").expect("ok"),
            ("Ada Lovelace".into(), "ada@example.com".into())
        );
        for bad in ["Ada", "<a@b>", "Ada <>", "Ada <a@b", ""] {
            assert!(matches!(parse_author(bad), Err(CliError::Usage(_))), "{bad:?}");
        }
    }

    #[test]
    fn expand_rev_outside_a_working_copy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = Ctx {
            cwd: dir.path().to_path_buf(),
            ..Default::default()
        };
        assert_eq!(ctx.expand_rev("main~1").ok().as_deref(), Some("main~1"));
        assert_eq!(ctx.expand_rev("origin/main~1").ok().as_deref(), Some("main~1"));
        assert_eq!(
            ctx.expand_rev("origin/").ok().as_deref(),
            Some("origin/"),
            "not a branch"
        );
        assert_eq!(
            ctx.expand_rev("HEADS-up").ok().as_deref(),
            Some("HEADS-up"),
            "a ref name"
        );
        assert_eq!(ctx.expand_rev("@home").ok().as_deref(), Some("@home"));
        for head in ["HEAD", "@", "HEAD~2", "@^"] {
            assert!(matches!(ctx.expand_rev(head), Err(CliError::Usage(_))), "{head}");
        }
        assert!(!ctx.has_working_copy());
    }

    #[test]
    fn split_spec_cases() {
        assert_eq!(split_spec("main:src/a.rs", "x"), ("main", "src/a.rs"));
        assert_eq!(split_spec("src/a.rs", "main"), ("main", "src/a.rs"));
        assert_eq!(split_spec(":src", "main"), ("main", "src"));
        assert_eq!(split_spec("", "main"), ("main", ""));
        assert_eq!(split_spec("c1abc:", "main"), ("c1abc", ""));
    }

    #[test]
    fn default_dir_is_last_segment() {
        assert_eq!(default_dir("opencsg/model"), "model");
        assert_eq!(default_dir("mono"), "mono");
    }

    #[test]
    fn short_truncates() {
        assert_eq!(short("c1abcdefghijklmn"), "c1abcdefghij");
        assert_eq!(short("c1"), "c1");
    }

    #[test]
    fn kind_all() {
        assert_eq!(kind(EntryMode::File), "file");
        assert_eq!(kind(EntryMode::Executable), "executable");
        assert_eq!(kind(EntryMode::Symlink), "symlink");
        assert_eq!(kind(EntryMode::Tree), "dir");
    }

    #[test]
    fn render_texts() {
        let c = CloneOut {
            schema: "s",
            repo: "r".into(),
            path: "/p".into(),
            reference: "main".into(),
            base: None,
            sparse: None,
        };
        assert_eq!(c.text(), "", "like git clone, nothing on stdout");
        let sp = SparseOut {
            schema: "s",
            dirs: None,
            written: 0,
            removed: 0,
        };
        assert_eq!(sp.text(), "full checkout");

        let s = StatusOut {
            schema: "s",
            reference: "main".into(),
            base: None,
            pending: None,
            ahead: 0,
            detached: false,
            added: vec![],
            modified: vec![],
            deleted: vec![],
            conflicted: vec![],
            behind: None,
            kinds: Default::default(),
            merging: None,
            pick: None,
            short: false,
            branch: false,
            cwd: String::new(),
        };
        assert_eq!(
            s.text(),
            "On branch main\n\nNo commits yet\n\nnothing to commit, working tree clean"
        );
        // The branch on the server moved past the working copy
        let moved = |count, exact, pending: Option<&str>| StatusOut {
            base: Some("c1base".into()),
            pending: pending.map(Into::into),
            ahead: usize::from(pending.is_some()),
            behind: Some(Behind { count, exact }),
            ..s.clone()
        };
        assert_eq!(
            moved(2, true, None).text(),
            "On branch main\nYour branch is behind 'main' on the server by 2 commits.\n  \
             (use \"scn sync\" to update the working copy)\n\nnothing to commit, working tree clean"
        );
        assert!(moved(1, true, None).text().contains("by 1 commit.\n"));
        assert!(
            moved(100, false, Some("c1pending0000"))
                .text()
                .contains("'main' on the server has 100+ commits that your commit is not based on.")
        );
        assert!(!moved(0, true, None).text().contains("behind"));
        let sb = |m: StatusOut| {
            StatusOut {
                short: true,
                branch: true,
                ..m
            }
            .text()
        };
        assert_eq!(sb(moved(2, true, None)), "## main [behind 2]");
        assert_eq!(sb(moved(100, false, Some("c1p"))), "## main [ahead 1, behind 100+]");
        assert_eq!(sb(moved(0, true, None)), "## main");
        assert_eq!(
            StatusOut {
                short: true,
                ..s.clone()
            }
            .text(),
            ""
        );
        let c = StatusOut {
            base: Some("c1base".into()),
            conflicted: vec!["x".into(), "y".into(), "z".into(), "w".into()],
            modified: vec!["x".into(), "y".into()],
            added: vec!["w".into()],
            kinds: [("y", "modify_delete"), ("z", "delete_modify"), ("w", "add_add")]
                .into_iter()
                .map(|(p, k)| (p.to_owned(), k.to_owned()))
                .collect(),
            merging: Some(true),
            ..s.clone()
        };
        assert_eq!(
            c.text(),
            "On branch main\nYou have unmerged paths.\n  (fix conflicts and run \"scn commit\")\n  \
             (use \"scn merge --abort\" to abort the merge)\n\nUnmerged paths:\n  \
             (use \"scn resolve <file>...\" to mark resolution)\n\tboth modified:   x\n\tdeleted by them: y\n\t\
             deleted by us:   z\n\tboth added:      w"
        );
        assert_eq!(
            StatusOut {
                short: true,
                ..c.clone()
            }
            .text(),
            "UU x\nUD y\nDU z\nAA w"
        );
        for (merging, want) in [
            (Some(true), "All conflicts fixed but you are still merging."),
            (Some(false), "(all conflicts fixed: run \"scn commit\")"),
        ] {
            let done = StatusOut {
                conflicted: vec![],
                merging,
                ..c.clone()
            };
            assert!(done.text().contains(want), "{}", done.text());
        }
        let rebasing = StatusOut {
            merging: Some(false),
            ..c.clone()
        };
        assert!(rebasing.text().contains(
            "(fix conflicts and then run \"scn commit\")\n  (use \"scn sync --abort\" to go back to before the sync)"
        ));
        for (kind, line) in [
            (PickKind::Revert, "You are currently reverting commit c1aaaaaaaaaa."),
            (
                PickKind::CherryPick,
                "You are currently cherry-picking commit c1aaaaaaaaaa.",
            ),
        ] {
            let picking = StatusOut {
                merging: Some(false),
                pick: Some(scion_wc::Pick {
                    kind,
                    commit: "c1aaaaaaaaaaaaaa".into(),
                }),
                ..c.clone()
            };
            let cmd = pick_words(kind).1;
            assert!(
                picking.text().contains(&format!(
                    "{line}\n  (fix conflicts and run \"scn commit\")\n  (use \"scn {cmd} --abort\" to cancel the {cmd})"
                )),
                "{}",
                picking.text()
            );
            let done = StatusOut {
                conflicted: vec![],
                ..picking
            };
            assert!(done.text().contains("(all conflicts fixed: run \"scn commit\")"));
        }
        let out = |changeset: Option<&str>, kind| PickOut {
            schema: "s",
            commit: "c1cccccccccccccc".into(),
            changeset: changeset.map(Into::into),
            written: 1,
            removed: 1,
            reference: "main".into(),
            subject: "Revert \"x\"".into(),
            kind,
        };
        assert_eq!(
            out(Some("c1dddddddddddddd"), PickKind::Revert).text(),
            "[main c1dddddddddd] Revert \"x\"\n 2 files changed"
        );
        assert_eq!(
            out(None, PickKind::CherryPick).text(),
            "nothing to commit: main already has the result of cherry-picking c1cccccccccc"
        );
        let s = StatusOut {
            added: vec!["a".into()],
            modified: vec!["m".into()],
            deleted: vec!["d".into()],
            base: Some("c1base".into()),
            pending: Some("c1pending0000".into()),
            ahead: 1,
            ..s
        };
        let three = StatusOut {
            ahead: 3,
            behind: Some(Behind { count: 2, exact: true }),
            ..s.clone()
        };
        assert!(three.text().starts_with(
            "On branch main\nYour 3 commits are not landed yet; the newest is c1pending000.\n  \
             (use \"scn land\" to publish them)\n'main' on the server has 2 commits that your commits are not based on."
        ));
        assert!(
            StatusOut {
                short: true,
                branch: true,
                ..three
            }
            .text()
            .starts_with("## main [ahead 3, behind 2]")
        );
        assert_eq!(
            s.text(),
            "On branch main\nYour commit c1pending000 is not landed yet.\n  (use \"scn land\" to publish your commit)\n\n\
             Changes to be committed:\n  (every change is included: scn has no \"add\" step; ignore files with .gitignore)\n  \
             (use \"scn restore <file>...\" to discard changes to tracked files)\n\tnew file:   a\n\t\
             modified:   m\n\tdeleted:    d"
        );
        assert_eq!(
            StatusOut {
                short: true,
                ..s.clone()
            }
            .text(),
            "A  a\nM  m\nD  d"
        );
        let nested = StatusOut {
            short: true,
            cwd: "src/x".into(),
            added: vec!["src/x/a".into(), "src/b".into(), "c".into(), "src".into()],
            modified: vec![],
            deleted: vec![],
            ..s
        };
        assert_eq!(nested.text(), "A  a\nA  ../b\nA  ../../c\nA  ../../src");

        let none = CommitOut {
            schema: "s",
            changeset: None,
            generation: None,
            insertions: None,
            deletions: None,
            summary: None,
        };
        assert_eq!(none.text(), "nothing to commit, working tree clean");
        use scion_wc::{FileStat, Mode};
        let stat = |path: &str, old_mode, new_mode| FileStat {
            path: path.into(),
            old_mode,
            new_mode,
            old: None,
            new_content: None,
        };
        let c = CommitOut {
            changeset: Some("c1abcdefghijklmn".into()),
            generation: Some(1),
            summary: Some(CommitSummary {
                reference: "main".into(),
                subject: "add things".into(),
                root: true,
                files: vec![
                    stat("a", None, Some(Mode::File)),
                    stat("b", Some(Mode::Symlink), None),
                    stat("c", Some(Mode::File), Some(Mode::Executable)),
                    stat("d", Some(Mode::File), Some(Mode::File)),
                ],
            }),
            ..none
        };
        assert_eq!(
            c.text(),
            "[main (root-commit) c1abcdefghij] add things\n 4 files changed\n create mode 100644 a\n \
             delete mode 120000 b\n mode change 100644 => 100755 c"
        );
        let one = CommitOut {
            summary: Some(CommitSummary {
                reference: "dev".into(),
                subject: "s".into(),
                root: false,
                files: vec![stat("d", Some(Mode::File), Some(Mode::File))],
            }),
            ..c
        };
        assert_eq!(one.text(), "[dev c1abcdefghij] s\n 1 file changed");
        let counted = |insertions, deletions| {
            CommitOut {
                schema: "s",
                changeset: one.changeset.clone(),
                generation: Some(1),
                insertions: Some(insertions),
                deletions: Some(deletions),
                summary: one.summary.clone(),
            }
            .text()
        };
        assert_eq!(
            counted(3, 1),
            "[dev c1abcdefghij] s\n 1 file changed, 3 insertions(+), 1 deletion(-)"
        );
        assert_eq!(counted(1, 0), "[dev c1abcdefghij] s\n 1 file changed, 1 insertion(+)");
        assert_eq!(counted(0, 2), "[dev c1abcdefghij] s\n 1 file changed, 2 deletions(-)");
        let land = LandOut {
            schema: "s",
            reference: "main".into(),
            tip: "c1tipabcdefghij".into(),
            version: 3,
            rebased: false,
            rewritten: vec![],
            url: "http://h:7080/mono".into(),
            old: None,
            up_to_date: false,
        };
        assert_eq!(land.text(), "To http://h:7080/mono\n * [new branch]      main -> main");
        let land = LandOut {
            old: Some("c1oldabcdefghij".into()),
            rebased: true,
            rewritten: vec![Rewritten {
                committed: "c1mineabcdefghij".into(),
                landed: "c1tipabcdefghij".into(),
            }],
            ..land
        };
        assert_eq!(
            land.text(),
            "To http://h:7080/mono\n   c1oldabcdefg..c1tipabcdefg  main -> main (rebased onto the latest main)\n\
             Your commit c1mineabcdef is now c1tipabcdefg"
        );
        let land = LandOut {
            up_to_date: true,
            ..land
        };
        assert_eq!(land.text(), "Everything up-to-date");

        let m = MergeOut {
            schema: "s",
            changeset: None,
            written: 2,
            removed: 0,
        };
        assert_eq!(m.text(), "Already up to date.");
        let m = MergeOut {
            changeset: Some("c1m".into()),
            ..m
        };
        assert_eq!(m.text(), "Merge made by the 'scion' strategy.\n 2 files changed");
        assert_eq!(AbortOut { schema: "s" }.text(), "");
        let r = RestoreOut {
            schema: "s",
            restored: vec!["a".into()],
            kept_added: vec![],
        };
        assert_eq!(r.text(), "");
        let r = RestoreOut {
            kept_added: vec!["n".into()],
            ..r
        };
        assert_eq!(r.text(), "kept 1 added file(s) (restore never deletes files): n");
        let sw = SwitchOut {
            schema: "s",
            reference: "dev".into(),
            created: true,
            tip: None,
            written: 0,
            removed: 0,
            already: false,
            detached: false,
        };
        assert_eq!(sw.text(), "Switched to a new branch 'dev'");
        let sw = SwitchOut { created: false, ..sw };
        assert_eq!(sw.text(), "Switched to branch 'dev'");
        assert_eq!(SwitchOut { already: true, ..sw }.text(), "Already on 'dev'");
        let mut so = SyncOut {
            schema: "s",
            outcome: "up_to_date",
            base: None,
            pending: None,
            written: 0,
            removed: 0,
            reference: "main".into(),
            old: None,
        };
        assert_eq!(so.text(), "Already up to date.");
        so.outcome = "updated";
        so.written = 1;
        assert_eq!(so.text(), "Fast-forward\n 1 file changed");
        so.outcome = "rebased";
        so.base = Some("c1base".into());
        so.old = Some("c1old".into());
        so.removed = 2;
        assert_eq!(
            so.text(),
            "Updating c1old..c1base\n 3 files changed\nSuccessfully rebased and updated main."
        );

        let ls = LsOut {
            schema: "s",
            revision: "main".into(),
            path: String::new(),
            entries: vec![
                LsEntry {
                    name: "d".into(),
                    kind: "dir",
                    size: 0,
                    id: "t1".into(),
                },
                LsEntry {
                    name: "x".into(),
                    kind: "executable",
                    size: 3,
                    id: "b1".into(),
                },
                LsEntry {
                    name: "l".into(),
                    kind: "symlink",
                    size: 1,
                    id: "b1".into(),
                },
                LsEntry {
                    name: "f".into(),
                    kind: "file",
                    size: 2,
                    id: "b1".into(),
                },
            ],
        };
        let t = ls.text();
        assert!(t.contains("d/") && t.contains("x*") && t.contains("l@") && t.ends_with("f"));

        let cat = CatOut {
            schema: "s",
            revision: "main".into(),
            path: "a".into(),
            size: 2,
            binary: false,
            content: "hi".into(),
            data: Bytes::from_static(b"hi"),
        };
        assert_eq!(cat.text(), "hi");
    }

    #[test]
    fn import_render() {
        let o = ImportOut {
            schema: "s",
            repo: "r".into(),
            reference: "main".into(),
            commits: 3,
            new_commits: 2,
            skipped_gitlinks: 1,
            tip: Some("c1tiptiptiptip".into()),
        };
        assert_eq!(
            o.text(),
            "imported 3 commits into r (main at c1tiptiptipt; 2 new, 1 already there)\nskipped 1 submodule entries (not supported yet)"
        );
        let o = ImportOut {
            tip: None,
            skipped_gitlinks: 0,
            ..o
        };
        assert_eq!(o.text(), "nothing to import into r");
    }

    #[test]
    fn split_repo_url_parses_server_and_repository() {
        let ok = |s: &str| split_repo_url(s).expect("valid");
        assert_eq!(ok("mono"), None);
        assert_eq!(ok("org/mono"), None);
        assert_eq!(
            ok("http://host:7080/org/mono"),
            Some(("http://host:7080".into(), "org/mono".into()))
        );
        assert_eq!(ok("https://h/mono/"), Some(("https://h".into(), "mono".into())));
        assert_eq!(ok("http://h"), Some(("http://h".into(), String::new())));
        assert_eq!(ok("http://h/"), Some(("http://h".into(), String::new())));
        for bad in [
            "http:///mono",
            "http://u@h/mono",
            "http://h/a?x=1",
            "http://h/a#f",
            "http://h/a//b",
        ] {
            assert!(matches!(split_repo_url(bad), Err(CliError::Usage(_))), "{bad}");
        }
    }

    #[test]
    fn repository_urls_replace_the_endpoint() {
        let mut ctx = Ctx {
            endpoint: Some("http://flag".into()),
            ..Default::default()
        };
        assert_eq!(ctx.named_repo_arg("mono").expect("name"), "mono");
        assert_eq!(ctx.endpoint.as_deref(), Some("http://flag"));
        assert_eq!(ctx.named_repo_arg("http://h:1/org/mono").expect("url"), "org/mono");
        assert_eq!(ctx.endpoint.as_deref(), Some("http://h:1"));
        assert!(matches!(ctx.named_repo_arg("http://h:2"), Err(CliError::Usage(_))));
        assert_eq!(ctx.repo_arg("http://h:3/").expect("prefix"), "");
        assert_eq!(ctx.endpoint.as_deref(), Some("http://h:3"));

        ctx.server_arg("https://s:4/ignored").expect("server");
        assert_eq!(ctx.endpoint.as_deref(), Some("https://s:4"));
        assert!(matches!(ctx.server_arg("s:4"), Err(CliError::Usage(_))));

        let mut ctx = Ctx::default();
        assert_eq!(
            ctx.fork_args("http://h/a", "b").expect("fork"),
            ("a".to_owned(), "b".to_owned())
        );
        assert_eq!(ctx.endpoint.as_deref(), Some("http://h"));
        assert_eq!(
            ctx.fork_args("http://h/a", "http://h/b").expect("same server"),
            ("a".to_owned(), "b".to_owned())
        );
        assert!(matches!(
            ctx.fork_args("http://h/a", "http://other/b"),
            Err(CliError::Usage(_))
        ));
    }

    #[test]
    fn endpoint_precedence() {
        let ctx = Ctx {
            endpoint: Some("http://flag".into()),
            ..Default::default()
        };
        assert_eq!(ctx.endpoint_or(None), "http://flag");
        assert_eq!(Ctx::default().endpoint_or(None), DEFAULT_ENDPOINT);
    }

    #[tokio::test]
    async fn query_outside_working_copy_needs_repo() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = Ctx {
            cwd: dir.path().to_path_buf(),
            ..Default::default()
        };
        assert!(matches!(cat(&ctx, "main:a").await, Err(CliError::Usage(_))));
        assert!(matches!(ls(&ctx, None).await, Err(CliError::Usage(_))));
        assert!(matches!(
            status(&ctx, false, true).await,
            Err(CliError::Wc(scion_wc::Error::NotAWorkingCopy(_)))
        ));
        assert!(matches!(
            commit(&ctx, Some("m"), Some("bad")).await,
            Err(CliError::Usage(_))
        ));
        assert!(matches!(
            import_git(&ctx, Path::new("."), "main", "main").await,
            Err(CliError::Usage(_))
        ));
    }
}
