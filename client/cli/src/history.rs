//! `scn log` 与 `scn diff`。

use bytes::Bytes;
use scion_client::{Client, FileChangeInfo, LogItem, ObjectId};
use scion_wc::{Mode, WorkingCopy};
use serde::Serialize;

use crate::commands::{Ctx, query_client, split_spec};
use crate::output::{CliError, Render};

/// Unix 毫秒 + 时区分钟 → `YYYY-MM-DD HH:MM +HHMM`（提交者所在时区的本地时间）。
pub fn format_time(time_ms: i64, tz_offset_minutes: i32) -> String {
    let secs = time_ms.div_euclid(1000) + i64::from(tz_offset_minutes) * 60;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    let sign = if tz_offset_minutes < 0 { '-' } else { '+' };
    let tz = tz_offset_minutes.unsigned_abs();
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02} {sign}{:02}{:02}",
        rem / 3600,
        rem % 3600 / 60,
        tz / 60,
        tz % 60
    )
}

/// git's default date format: `Wed Nov 15 06:13:20 2023 +0800`, in the author's time zone.
pub fn git_date(time_ms: i64, tz_offset_minutes: i32) -> String {
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let secs = time_ms.div_euclid(1000) + i64::from(tz_offset_minutes) * 60;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    let sign = if tz_offset_minutes < 0 { '-' } else { '+' };
    let tz = tz_offset_minutes.unsigned_abs();
    format!(
        "{} {} {d} {:02}:{:02}:{:02} {y} {sign}{:02}{:02}",
        DAYS[(days + 4).rem_euclid(7) as usize],
        MONTHS[m as usize - 1],
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        tz / 60,
        tz % 60
    )
}

/// 1970-01-01 起的天数 → 公历 (年, 月, 日)。算法来自 Howard Hinnant 的 `civil_from_days`。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn short(id: &ObjectId) -> String {
    id.to_string()[..12].to_owned()
}

// ---------- log ----------

#[derive(Debug, Clone, Serialize)]
pub struct LogLine {
    pub id: String,
    pub parents: Vec<String>,
    pub author: String,
    pub email: String,
    /// Author date (what `git log` shows).
    pub time_ms: i64,
    pub tz_offset_minutes: i32,
    pub committer: String,
    pub committer_time_ms: i64,
    pub committer_tz_offset_minutes: i32,
    /// The commit as it was made, when the server rebased it while landing it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rebased_from: Option<String>,
    pub message: String,
    pub generation: u64,
}

#[derive(Debug, Serialize)]
pub struct GrepOut {
    pub schema: &'static str,
    pub changeset: Option<String>,
    pub matches: Vec<GrepMatchOut>,
    /// It stopped at the limit (--max-count): there may be more.
    pub truncated: bool,
    pub files_searched: u64,
    pub files_skipped: u64,
    /// For the text: line numbers (-n), file names only (-l), and paths as seen from here.
    #[serde(skip)]
    pub line_numbers: bool,
    #[serde(skip)]
    pub names_only: bool,
    #[serde(skip)]
    pub cwd: String,
}

#[derive(Debug, Serialize)]
pub struct GrepMatchOut {
    pub path: String,
    pub line: u64,
    pub text: String,
}

/// git grep's `PATH:LINE` (`PATH:N:LINE` with -n, `PATH` once with -l), paths relative to the
/// current directory.
impl Render for GrepOut {
    fn text(&self) -> String {
        let shown = |p: &str| crate::commands::relative_to(&self.cwd, p);
        let mut lines: Vec<String> = Vec::new();
        for m in &self.matches {
            let path = shown(&m.path);
            if self.names_only {
                if lines.last() != Some(&path) {
                    lines.push(path);
                }
            } else if self.line_numbers {
                lines.push(format!("{path}:{}:{}", m.line, m.text));
            } else {
                lines.push(format!("{path}:{}", m.text));
            }
        }
        lines.join("\n")
    }
}

/// `scn grep`: searches a commit on the server. In a working copy without a revision it searches
/// the working tree, as git does: HEAD on the server, with the files changed locally searched here
/// in their place. In a working copy, paths are relative to the current directory, and with none
/// given it searches the current directory.
pub async fn grep(
    ctx: &Ctx,
    query: scion_client::GrepQuery,
    revision: Option<&str>,
    line_numbers: bool,
    names_only: bool,
) -> Result<GrepOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let worktree = revision.is_none();
    let revision = ctx.expand_rev(&revision.map_or(default_rev, str::to_owned))?;
    let wc = ctx.working_copy();
    let (paths, cwd) = match &wc {
        Some(w) => {
            let here = crate::commands::wc_relative(w.root(), &ctx.cwd, &[".".to_owned()])?.remove(0);
            let given = if query.paths.is_empty() {
                vec![here.clone()]
            } else {
                crate::commands::wc_relative(w.root(), &ctx.cwd, &query.paths)?
            };
            let paths = given.into_iter().filter(|p| p != ".").collect();
            (paths, if here == "." { String::new() } else { here })
        }
        None => (query.paths.clone(), String::new()),
    };
    let local = match (&wc, worktree) {
        (Some(w), true) => Some(w.local_changes()?),
        _ => None,
    };
    let mut found = client
        .grep(
            &revision,
            &scion_client::GrepQuery {
                paths: paths.clone(),
                ..query.clone()
            },
        )
        .await?;
    // After the server, which refuses a bad pattern first
    if let Some(changes) = local {
        overlay(&mut found, &changes, &paths, &local_pattern(&query)?, query.max_matches);
    }
    if found.truncated && ctx.chatty {
        eprintln!(
            "note: stopped at {} matching lines; narrow it with a path, or raise --max-count",
            found.matches.len()
        );
    }
    Ok(GrepOut {
        schema: "scion.cli.grep/v1",
        changeset: found.changeset.map(|c| c.to_string()),
        matches: found
            .matches
            .into_iter()
            .map(|m| GrepMatchOut {
                path: m.path,
                line: m.line,
                text: String::from_utf8_lossy(&m.text).into_owned(),
            })
            .collect(),
        truncated: found.truncated,
        files_searched: found.searched,
        files_skipped: found.skipped,
        line_numbers,
        names_only,
        cwd,
    })
}

/// The pattern as the server builds it, for searching local files.
fn local_pattern(query: &scion_client::GrepQuery) -> Result<regex::bytes::Regex, CliError> {
    let source = if query.fixed {
        regex::escape(&query.pattern)
    } else {
        query.pattern.clone()
    };
    regex::bytes::RegexBuilder::new(&source)
        .case_insensitive(query.ignore_case)
        .build()
        .map_err(|e| CliError::Usage(format!("bad pattern: {e}")))
}

/// Puts local changes over the server's matches at HEAD: files changed locally lose their matches
/// there and are searched here instead (deleted ones not at all), with the server's limits: text
/// files up to 1 MiB, lines cut to 1000 bytes.
fn overlay(
    found: &mut scion_client::GrepFound,
    changes: &[scion_wc::LocalChange],
    paths: &[String],
    pattern: &regex::bytes::Regex,
    max_matches: u32,
) {
    let limit = match max_matches {
        0 => 1000,
        n => n.min(10_000),
    } as usize;
    let wanted = |p: &str| paths.is_empty() || paths.iter().any(|d| in_path(p, Some(d)));
    let changed: std::collections::HashSet<&str> =
        changes.iter().map(|c| c.path.as_str()).filter(|p| wanted(p)).collect();
    found.matches.retain(|m| !changed.contains(m.path.as_str()));
    for c in changes.iter().filter(|c| changed.contains(c.path.as_str())) {
        let Some((mode, data)) = &c.new else { continue };
        let text =
            *mode != scion_wc::Mode::Symlink && data.len() <= 1 << 20 && !data[..data.len().min(8000)].contains(&0);
        if !text {
            found.skipped += 1;
            continue;
        }
        found.searched += 1;
        for (i, line) in data.split(|b| *b == b'\n').enumerate() {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if pattern.is_match(line) {
                found.matches.push(scion_client::GrepLine {
                    path: c.path.clone(),
                    line: i as u64 + 1,
                    text: bytes::Bytes::copy_from_slice(&line[..line.len().min(1000)]),
                });
            }
        }
    }
    // The server's order: depth first, by name
    found
        .matches
        .sort_by(|a, b| a.path.split('/').cmp(b.path.split('/')).then(a.line.cmp(&b.line)));
    if found.matches.len() > limit {
        found.matches.truncate(limit);
        found.truncated = true;
    }
}

#[derive(Debug, Serialize)]
pub struct ReflogOut {
    pub schema: &'static str,
    #[serde(rename = "ref")]
    pub reference: String,
    pub entries: Vec<ReflogLine>,
}

#[derive(Debug, Serialize)]
pub struct ReflogLine {
    pub version: u64,
    pub changeset: String,
    pub time_ms: i64,
    /// `created`, `fast-forward`, `forced`, or `unknown` (the entry before is gone).
    #[serde(rename = "move")]
    pub how: &'static str,
    /// Who moved it (comma-separated when several requests landed together); empty when the
    /// server did not record it.
    pub actor: String,
    pub subject: String,
}

/// git's `ID REF@{N}: WHAT: SUBJECT`, with the time of the move (UTC) after `REF@{N}` and who
/// made it after the move (`fast-forward by alice`).
impl Render for ReflogOut {
    fn text(&self) -> String {
        self.entries
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let by = if e.actor.is_empty() {
                    String::new()
                } else {
                    format!(" by {}", e.actor)
                };
                format!(
                    "{} {}@{{{i}}}: {} {}{by}: {}",
                    &e.changeset[..e.changeset.len().min(12)],
                    self.reference,
                    format_time(e.time_ms, 0),
                    e.how,
                    e.subject
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// `scn reflog [REF] [-n N]`: how a branch (default: the working copy's, else main) or tag
/// moved, newest first, each move with the subject of the commit it went to.
pub async fn reflog(ctx: &Ctx, reference: Option<&str>, limit: usize) -> Result<ReflogOut, CliError> {
    let (client, _) = query_client(ctx).await?;
    let wc = ctx.working_copy();
    let mut name = match reference {
        None | Some("HEAD" | "@") => wc.as_ref().map_or_else(
            || crate::commands::DEFAULT_REF.to_owned(),
            |w| w.state().reference.clone(),
        ),
        Some(r) => r.strip_prefix("origin/").unwrap_or(r).to_owned(),
    };
    if client.resolve_ref(&name).await?.is_none() {
        let tag = format!("tags/{name}");
        if client.resolve_ref(&tag).await?.is_none() {
            return Err(CliError::Usage(format!("no branch or tag {name:?}")));
        }
        name = tag;
    }
    let mut moves = Vec::new();
    let mut before = None;
    while moves.len() < limit {
        let want = u32::try_from(limit - moves.len()).unwrap_or(u32::MAX).min(1000);
        let (page, next) = client.ref_log(&name, before, want).await?;
        moves.extend(page);
        match next {
            Some(n) => before = Some(n),
            None => break,
        }
    }
    moves.truncate(limit);
    let mut entries = Vec::with_capacity(moves.len());
    for m in moves {
        let id = m.changeset.to_string();
        let subject = client
            .log(&id, None, 1, None)
            .await?
            .entries
            .into_iter()
            .next()
            .map(|e| crate::output::subject(&e.message))
            .unwrap_or_default();
        entries.push(ReflogLine {
            version: m.version,
            changeset: id,
            time_ms: m.updated_at_ms,
            how: match m.how {
                scion_proto::v1::RefMove::Created => "created",
                scion_proto::v1::RefMove::FastForward => "fast-forward",
                scion_proto::v1::RefMove::Forced => "forced",
                scion_proto::v1::RefMove::Unspecified => "unknown",
            },
            actor: m.actor,
            subject,
        });
    }
    Ok(ReflogOut {
        schema: "scion.cli.reflog/v1",
        reference: name.strip_prefix("tags/").unwrap_or(&name).to_owned(),
        entries,
    })
}

#[derive(Debug, Serialize)]
pub struct LogOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: Option<String>,
    pub entries: Vec<LogLine>,
    /// `--oneline`.
    #[serde(skip)]
    pub oneline: bool,
}

/// A commit as `git log` and `git show` print it: `commit`, `Merge:`, `Author:`, `Date:`, then
/// the message indented by four spaces. When the commit time differs from the author's, as after
/// a rebase on the server (which keeps commit times in landing order), a `CommitDate:` line
/// (git's `--format=fuller` name) follows `Date:`; a commit the server rebased also names the
/// commit it was made as (`Rebased-from:`).
fn commit_header(c: &LogLine) -> String {
    let mut out = format!("commit {}\n", c.id);
    if c.parents.len() > 1 {
        let short: Vec<&str> = c.parents.iter().map(|p| &p[..p.len().min(12)]).collect();
        out.push_str(&format!("Merge: {}\n", short.join(" ")));
    }
    out.push_str(&format!(
        "Author: {} <{}>\nDate:   {}",
        c.author,
        c.email,
        git_date(c.time_ms, c.tz_offset_minutes)
    ));
    let committed = git_date(c.committer_time_ms, c.committer_tz_offset_minutes);
    if committed != git_date(c.time_ms, c.tz_offset_minutes) {
        out.push_str(&format!("\nCommitDate: {committed}"));
    }
    if let Some(from) = &c.rebased_from {
        out.push_str(&format!("\nRebased-from: {}", &from[..from.len().min(12)]));
    }
    if !c.message.trim().is_empty() {
        out.push('\n');
        for line in c.message.lines() {
            out.push_str(&format!("\n    {line}"));
        }
    }
    out
}

/// Like `git log`, or `git log --oneline`.
impl Render for LogOut {
    fn text(&self) -> String {
        if self.oneline {
            return self
                .entries
                .iter()
                .map(|e| format!("{} {}", &e.id[..12.min(e.id.len())], crate::output::subject(&e.message)))
                .collect::<Vec<_>>()
                .join("\n");
        }
        self.entries.iter().map(commit_header).collect::<Vec<_>>().join("\n\n")
    }
}

fn log_line(e: LogItem) -> LogLine {
    LogLine {
        // Before the fields below move out of `e`
        rebased_from: e.rebased_from().map(|id| id.to_string()),
        id: e.id.to_string(),
        parents: e.parents.iter().map(ToString::to_string).collect(),
        // Author and author date, as `git log` shows them
        time_ms: e.author.time_ms,
        tz_offset_minutes: e.author.tz_offset_minutes,
        committer: e.committer.name,
        committer_time_ms: e.committer.time_ms,
        committer_tz_offset_minutes: e.committer.tz_offset_minutes,
        author: e.author.name,
        email: e.author.email,
        message: e.message,
        generation: e.generation,
    }
}

/// 自动翻页直到凑够 `limit` 条或历史结束。
/// A path argument as git takes it: in a working copy, relative to the current directory. The
/// working copy's root (or above it) means no path.
fn repo_path(ctx: &Ctx, path: Option<&str>) -> Result<Option<String>, CliError> {
    match (path, ctx.working_copy()) {
        (Some(p), Some(w)) => {
            let p = crate::commands::wc_relative(w.root(), &ctx.cwd, &[p.to_owned()])?.remove(0);
            Ok((p != ".").then_some(p))
        }
        (p, _) => Ok(p.map(str::to_owned)),
    }
}

/// `scn log`: `query.path` is relative to the current directory, as in git; `--follow`,
/// `--grep` and `--author` are the query's.
pub async fn log(
    ctx: &Ctx,
    revision: Option<&str>,
    mut query: scion_client::LogQuery,
    limit: usize,
    oneline: bool,
) -> Result<LogOut, CliError> {
    let path = repo_path(ctx, Some(&query.path).filter(|p| !p.is_empty()).map(String::as_str))?;
    if query.follow && path.is_none() {
        return Err(CliError::Usage("--follow needs one file path".into()));
    }
    query.path = path.clone().unwrap_or_default();
    let (client, default_rev) = query_client(ctx).await?;
    let revision = ctx.expand_rev(&revision.map_or(default_rev, str::to_owned))?;
    let mut entries = Vec::new();
    let mut token: Option<String> = None;
    while entries.len() < limit {
        let want = u32::try_from(limit - entries.len()).unwrap_or(u32::MAX).min(1000);
        let page = client.log_query(&revision, &query, want, token.as_deref()).await?;
        // With --follow, the name the file has in the commits still to come
        if let Some(old) = page.renamed_from {
            query.path = old;
        }
        entries.extend(page.entries.into_iter().map(log_line));
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    entries.truncate(limit);
    Ok(LogOut {
        schema: "scion.cli.log/v1",
        revision,
        path,
        entries,
        oneline,
    })
}

// ---------- blame ----------

#[derive(Debug, Serialize)]
pub struct BlameLineOut {
    pub line: usize,
    pub commit: String,
    pub author: String,
    pub time_ms: i64,
    pub tz_offset_minutes: i32,
    pub content: String,
}

#[derive(Debug, Serialize)]
pub struct BlameOut {
    pub schema: &'static str,
    pub revision: String,
    pub path: String,
    pub lines: Vec<BlameLineOut>,
}

/// Like `git blame`: `ID (AUTHOR DATE LINE) CONTENT`, authors padded to the longest.
impl Render for BlameOut {
    fn text(&self) -> String {
        let width = self.lines.last().map_or(1, |l| l.line.to_string().len());
        let author_width = self.lines.iter().map(|l| l.author.chars().count()).max().unwrap_or(0);
        self.lines
            .iter()
            .map(|l| {
                format!(
                    "{} ({:<author_width$} {} {:>width$}) {}",
                    &l.commit[..12.min(l.commit.len())],
                    l.author,
                    blame_time(l.time_ms, l.tz_offset_minutes),
                    l.line,
                    l.content
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// `2023-11-15 06:13:20 +0800`, as `git blame` prints dates.
fn blame_time(time_ms: i64, tz_offset_minutes: i32) -> String {
    let t = format_time(time_ms, tz_offset_minutes);
    let secs = time_ms.div_euclid(1000).rem_euclid(60);
    format!("{}:{secs:02}{}", &t[..16], &t[16..])
}

/// git's `blame -L` ranges: `N,M`, `N,+K`, `N` (to the end) and `,M` (from the start); lines
/// count from 1, both ends included.
pub fn parse_line_range(s: &str) -> Result<(usize, usize), String> {
    let bad = || "expected N,M, N,+K, N or ,M with lines counted from 1".to_owned();
    let num = |t: &str| t.parse::<usize>().ok().filter(|n| *n > 0).ok_or_else(bad);
    let (start, end) = match s.split_once(',') {
        None => (num(s)?, usize::MAX),
        Some(("", e)) => (1, num(e)?),
        Some((b, e)) => {
            let b = num(b)?;
            match e.strip_prefix('+') {
                Some(k) => (b, b + num(k)? - 1),
                None if e.is_empty() => (b, usize::MAX),
                None => (b, num(e)?),
            }
        }
    };
    if end < start {
        return Err(bad());
    }
    Ok((start, end))
}

/// 按行切分，与服务端一致：保留换行符，末尾没有换行的最后一行也算一行。
fn split_lines(data: &[u8]) -> Vec<&[u8]> {
    data.split_inclusive(|b| *b == b'\n').collect()
}

/// `ranges` (from `-L`) keep only those lines; none keeps them all.
pub async fn blame(ctx: &Ctx, spec: &str, ranges: &[(usize, usize)]) -> Result<BlameOut, CliError> {
    let (client, default_rev) = query_client(ctx).await?;
    let (rev, path) = split_spec(spec, &default_rev);
    // `REV:PATH` names a path from the root, as in git; a bare path is relative
    let path = match spec.contains(':') {
        true => path.to_owned(),
        false => repo_path(ctx, Some(path))?.unwrap_or_default(),
    };
    let rev = ctx.expand_rev(rev)?;
    let rev = rev.as_str();
    let info = client.blame(rev, &path).await?;
    let data = client.get_object(&info.blob).await?;
    let raw = split_lines(&data);
    if raw.len() != info.lines.len() {
        return Err(CliError::Client(scion_client::Error::Protocol(format!(
            "blame covers {} lines but the file has {}",
            info.lines.len(),
            raw.len()
        ))));
    }
    if let Some((start, _)) = ranges.iter().find(|(start, _)| *start > raw.len()) {
        return Err(CliError::Usage(format!(
            "-L {start}: {path} has only {} lines",
            raw.len()
        )));
    }
    let wanted = |line: usize| ranges.is_empty() || ranges.iter().any(|(a, b)| (*a..=*b).contains(&line));
    let lines = raw
        .iter()
        .zip(&info.lines)
        .enumerate()
        .filter(|(i, _)| wanted(i + 1))
        .map(|(i, (text, idx))| {
            let c = &info.commits[*idx];
            let content = String::from_utf8_lossy(text);
            BlameLineOut {
                line: i + 1,
                commit: c.id.to_string(),
                author: c.author.name.clone(),
                time_ms: c.author.time_ms,
                tz_offset_minutes: c.author.tz_offset_minutes,
                content: content.trim_end_matches(['\n', '\r']).to_owned(),
            }
        })
        .collect();
    Ok(BlameOut {
        schema: "scion.cli.blame/v1",
        revision: rev.to_owned(),
        path: path.to_owned(),
        lines,
    })
}

// ---------- diff ----------

#[derive(Debug, Serialize)]
pub struct DiffFile {
    pub path: String,
    /// A（新增）| M（修改）| D（删除）
    pub status: &'static str,
    pub old_id: Option<String>,
    pub new_id: Option<String>,
    pub old_mode: Option<&'static str>,
    pub new_mode: Option<&'static str>,
    pub binary: bool,
    /// unified diff；`--name-only` 或二进制文件时为 None。
    pub patch: Option<String>,
    /// Sizes of the old and new contents, when they were read (for `--stat` on binary files).
    #[serde(skip)]
    pub sizes: Option<(u64, u64)>,
}

#[derive(Debug, Serialize)]
pub struct DiffOut {
    pub schema: &'static str,
    pub from: Option<String>,
    pub to: String,
    pub files: Vec<DiffFile>,
    #[serde(skip)]
    pub name_only: bool,
    /// `--name-status`.
    #[serde(skip)]
    pub name_status: bool,
    /// `--stat`.
    #[serde(skip)]
    pub stat: bool,
}

/// Lines added and removed by a unified diff (its hunks, not its `---` / `+++` header).
fn line_counts(patch: &str) -> (usize, usize) {
    patch
        .lines()
        .skip_while(|l| !l.starts_with("@@"))
        .fold((0, 0), |(add, del), l| match l.as_bytes().first() {
            Some(b'+') => (add + 1, del),
            Some(b'-') => (add, del + 1),
            _ => (add, del),
        })
}

/// The widest `+++---` bar `--stat` draws; longer ones are scaled down.
const STAT_BAR: usize = 50;

/// git's `--stat`: ` PATH | N ++--` per file, then ` N files changed, X insertions(+), Y deletions(-)`.
fn diffstat(files: &[DiffFile]) -> String {
    let counts: Vec<(usize, usize)> = files
        .iter()
        .map(|f| f.patch.as_deref().map_or((0, 0), line_counts))
        .collect();
    let most = counts.iter().map(|(a, d)| a + d).max().unwrap_or(0);
    let scale = |n: usize| {
        if most <= STAT_BAR || n == 0 {
            n
        } else {
            (n * STAT_BAR / most).max(1)
        }
    };
    let name_width = files.iter().map(|f| f.path.chars().count()).max().unwrap_or(0);
    // As in git, binary files' "Bin" does not widen the count column
    let count_width = counts
        .iter()
        .zip(files)
        .filter(|(_, f)| !f.binary)
        .map(|((a, d), _)| (a + d).to_string().len())
        .max()
        .unwrap_or(1);
    let mut out: Vec<String> = files
        .iter()
        .zip(&counts)
        .map(|(f, (add, del))| {
            let what = if f.binary {
                match f.sizes {
                    Some((old, new)) => format!("Bin {old} -> {new} bytes"),
                    None => "Bin".to_owned(),
                }
            } else {
                format!(
                    "{:>count_width$} {}{}",
                    add + del,
                    "+".repeat(scale(*add)),
                    "-".repeat(scale(*del))
                )
            };
            format!(" {:<name_width$} | {}", f.path, what.trim_end())
        })
        .collect();
    let (add, del) = counts.iter().fold((0, 0), |(a, d), (x, y)| (a + x, d + y));
    let plural = |n: usize, one: &str, many: &str| format!("{n} {}", if n == 1 { one } else { many });
    let mut summary = format!(" {}", plural(files.len(), "file changed", "files changed"));
    if add > 0 {
        summary.push_str(&format!(", {}", plural(add, "insertion(+)", "insertions(+)")));
    }
    if del > 0 {
        summary.push_str(&format!(", {}", plural(del, "deletion(-)", "deletions(-)")));
    }
    if !files.is_empty() {
        out.push(summary);
    }
    out.join("\n")
}

/// A mode name in JSON (`file`, `executable`, `symlink`) as git's octal mode.
fn git_mode(name: &str) -> &'static str {
    match name {
        "executable" => "100755",
        "symlink" => "120000",
        _ => "100644",
    }
}

/// Like `git diff`: `diff --git` headers and unified diffs; `--name-only` lists paths and
/// `--name-status` adds the status letter; no differences print nothing.
impl Render for DiffOut {
    fn text(&self) -> String {
        if self.stat {
            return diffstat(&self.files);
        }
        if self.name_only || self.name_status {
            return self
                .files
                .iter()
                .map(|f| {
                    if self.name_status {
                        format!("{}\t{}", f.status, f.path)
                    } else {
                        f.path.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
        }
        let mut out = String::new();
        for f in &self.files {
            out.push_str(&format!("diff --git a/{0} b/{0}\n", f.path));
            match (f.old_mode, f.new_mode) {
                (None, Some(m)) => out.push_str(&format!("new file mode {}\n", git_mode(m))),
                (Some(m), None) => out.push_str(&format!("deleted file mode {}\n", git_mode(m))),
                (Some(a), Some(b)) if a != b => {
                    out.push_str(&format!("old mode {}\nnew mode {}\n", git_mode(a), git_mode(b)));
                }
                _ => {}
            }
            if f.old_id.is_some() || f.new_id.is_some() {
                let id = |i: &Option<String>| {
                    i.as_deref()
                        .map_or("000000000000", |i| &i[..i.len().min(12)])
                        .to_owned()
                };
                let mode = match (f.old_mode, f.new_mode) {
                    (Some(a), Some(b)) if a == b => format!(" {}", git_mode(a)),
                    _ => String::new(),
                };
                out.push_str(&format!("index {}..{}{mode}\n", id(&f.old_id), id(&f.new_id)));
            }
            if f.binary {
                let a = if f.old_mode.is_some() {
                    format!("a/{}", f.path)
                } else {
                    "/dev/null".to_owned()
                };
                let b = if f.new_mode.is_some() {
                    format!("b/{}", f.path)
                } else {
                    "/dev/null".to_owned()
                };
                out.push_str(&format!("Binary files {a} and {b} differ\n"));
            } else if let Some(p) = &f.patch {
                out.push_str(p);
            }
        }
        out.trim_end().to_owned()
    }
}

fn mode_name(m: Mode) -> &'static str {
    match m {
        Mode::File => "file",
        Mode::Executable => "executable",
        Mode::Symlink => "symlink",
    }
}

fn entry_mode_name(m: scion_client::EntryMode) -> &'static str {
    crate::commands::kind(m)
}

fn size(b: Option<&[u8]>) -> u64 {
    b.map_or(0, |b| b.len() as u64)
}

fn is_binary(b: &[u8]) -> bool {
    b.contains(&0) || std::str::from_utf8(b).is_err()
}

/// Lines added and removed between two versions, as `--stat` counts them; None when either is
/// binary.
pub(crate) fn changed_lines(old: &[u8], new: &[u8]) -> Option<(usize, usize)> {
    if is_binary(old) || is_binary(new) {
        return None;
    }
    let (a, b) = (String::from_utf8_lossy(old), String::from_utf8_lossy(new));
    let diff = similar::TextDiff::configure()
        .algorithm(diff_algorithm(a.as_bytes(), b.as_bytes()))
        .diff_lines(a.as_ref(), b.as_ref());
    Some(diff.iter_all_changes().fold((0, 0), |(add, del), c| match c.tag() {
        similar::ChangeTag::Insert => (add + 1, del),
        similar::ChangeTag::Delete => (add, del + 1),
        similar::ChangeTag::Equal => (add, del),
    }))
}

/// 生成 unified diff；任一侧为二进制时返回 (true, None)。
/// The line-diff algorithm to use: exact LCS (minimal, like `git diff --minimal`) when the part
/// between the common prefix and suffix is small enough for its table (about 32 MB at most),
/// else Myers. similar's Myers is not always minimal on large files.
fn diff_algorithm(old: &[u8], new: &[u8]) -> similar::Algorithm {
    const MAX_LCS_CELLS: usize = 4_000_000;
    let (a, b): (Vec<&[u8]>, Vec<&[u8]>) = (
        old.split_inclusive(|c| *c == b'\n').collect(),
        new.split_inclusive(|c| *c == b'\n').collect(),
    );
    let pre = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
    let suf = a[pre..]
        .iter()
        .rev()
        .zip(b[pre..].iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    if (a.len() - pre - suf).saturating_mul(b.len() - pre - suf) <= MAX_LCS_CELLS {
        similar::Algorithm::Lcs
    } else {
        similar::Algorithm::Myers
    }
}

pub fn render_patch(path: &str, old: Option<&[u8]>, new: Option<&[u8]>) -> (bool, Option<String>) {
    let (old_present, new_present) = (old.is_some(), new.is_some());
    let (old, new) = (old.unwrap_or_default(), new.unwrap_or_default());
    if is_binary(old) || is_binary(new) {
        return (true, None);
    }
    let (a, b) = (String::from_utf8_lossy(old), String::from_utf8_lossy(new));
    let diff = similar::TextDiff::configure()
        .algorithm(diff_algorithm(a.as_bytes(), b.as_bytes()))
        .diff_lines(a.as_ref(), b.as_ref());
    let side = |present: bool, prefix: &str| {
        if present {
            format!("{prefix}/{path}")
        } else {
            "/dev/null".to_owned()
        }
    };
    let patch = diff
        .unified_diff()
        .context_radius(3)
        .header(&side(old_present, "a"), &side(new_present, "b"))
        .to_string();
    (false, Some(patch))
}

fn status_of(old: bool, new: bool) -> &'static str {
    match (old, new) {
        (false, _) => "A",
        (_, false) => "D",
        _ => "M",
    }
}

async fn fetch(client: &Client, id: Option<ObjectId>) -> Result<Option<Bytes>, CliError> {
    match id {
        Some(id) => Ok(Some(client.get_object(&id).await?)),
        None => Ok(None),
    }
}

/// 服务端算出文件列表，客户端取内容生成 patch。
async fn remote_files(
    client: &Client,
    changes: Vec<FileChangeInfo>,
    name_only: bool,
) -> Result<Vec<DiffFile>, CliError> {
    let mut files = Vec::with_capacity(changes.len());
    for c in changes {
        let (old_id, new_id) = (c.old.as_ref().map(|e| e.id), c.new.as_ref().map(|e| e.id));
        let mut sizes = None;
        let (binary, patch) = if name_only {
            (false, None)
        } else {
            let (o, n) = (fetch(client, old_id).await?, fetch(client, new_id).await?);
            sizes = Some((size(o.as_deref()), size(n.as_deref())));
            render_patch(&c.path, o.as_deref(), n.as_deref())
        };
        files.push(DiffFile {
            status: status_of(c.old.is_some(), c.new.is_some()),
            old_id: old_id.map(|i| i.to_string()),
            new_id: new_id.map(|i| i.to_string()),
            old_mode: c.old.as_ref().map(|e| entry_mode_name(e.mode)),
            new_mode: c.new.as_ref().map(|e| entry_mode_name(e.mode)),
            binary,
            patch,
            sizes,
            path: c.path,
        });
    }
    Ok(files)
}

/// git's range forms: `A..B` → (A, B, false); `A...B` → (A, B, true), meaning "from the merge
/// base of A and B". An empty side means HEAD.
pub(crate) fn split_range(rev: &str) -> Option<(&str, &str, bool)> {
    if let Some((a, b)) = rev.split_once("...") {
        return Some((a, b, true));
    }
    rev.split_once("..").map(|(a, b)| (a, b, false))
}

/// - 不带版本：工作副本里尚未提交的改动；
/// - 一个版本：该提交相对第一个父提交引入的改动；
/// - 两个版本，或 `A..B`：两者之间的差异；`A...B`：B 相对两者的公共祖先的改动。
pub async fn diff(ctx: &Ctx, revs: &[String], path: Option<&str>, name_only: bool) -> Result<DiffOut, CliError> {
    let path = repo_path(ctx, path)?;
    let path = path.as_deref();
    let or_head = |r: &str| if r.is_empty() { "HEAD".to_owned() } else { r.to_owned() };
    let (from, to) = match revs {
        [] => return local_diff(ctx, path, name_only).await,
        [r] => match split_range(r) {
            Some((a, b, three_dots)) => {
                let (a, b) = (ctx.expand_rev(&or_head(a))?, ctx.expand_rev(&or_head(b))?);
                if three_dots {
                    let (client, _) = query_client(ctx).await?;
                    let base = client
                        .merge_bases(&a, &b)
                        .await?
                        .first()
                        .map(ToString::to_string)
                        .ok_or_else(|| CliError::Usage(format!("{a} and {b} have no common ancestor")))?;
                    (Some(base), b)
                } else {
                    (Some(a), b)
                }
            }
            None => {
                let rev = ctx.expand_rev(r)?;
                let (client, _) = query_client(ctx).await?;
                let page = client.log(&rev, None, 1, None).await?;
                let entry = page
                    .entries
                    .into_iter()
                    .next()
                    .ok_or_else(|| CliError::Usage(format!("{rev} has no history")))?;
                (entry.parents.first().map(ToString::to_string), entry.id.to_string())
            }
        },
        [a, b] => (Some(ctx.expand_rev(a)?), ctx.expand_rev(b)?),
        _ => return Err(CliError::Usage("scn diff takes at most two revisions".into())),
    };
    let (client, _) = query_client(ctx).await?;
    let changes = client.diff(from.as_deref(), &to, path).await?;
    Ok(DiffOut {
        schema: "scion.cli.diff/v1",
        files: remote_files(&client, changes, name_only).await?,
        from,
        to,
        name_only,
        name_status: false,
        stat: false,
    })
}

#[derive(Debug, Serialize)]
pub struct ShowOut {
    pub schema: &'static str,
    /// When the revision is an annotated tag: what it says.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<ShowTag>,
    pub commit: LogLine,
    pub diff: DiffOut,
}

#[derive(Debug, Serialize)]
pub struct ShowTag {
    pub name: String,
    pub tagger: String,
    pub date: String,
    pub message: String,
}

/// Like `git show`: an annotated tag's `tag`, `Tagger:`, `Date:` and message first, then the
/// commit as `git log` prints it, then its diff.
impl Render for ShowOut {
    fn text(&self) -> String {
        let mut out = String::new();
        if let Some(t) = &self.tag {
            out.push_str(&format!(
                "tag {}\nTagger: {}\nDate:   {}\n\n{}\n\n",
                t.name,
                t.tagger,
                t.date,
                t.message.trim_end()
            ));
        }
        out.push_str(&commit_header(&self.commit));
        let diff = self.diff.text();
        if !diff.is_empty() {
            out.push_str("\n\n");
            out.push_str(&diff);
        }
        out
    }
}

/// `scn show [REV] [-- PATH]`: the commit's metadata and the changes it introduced (relative to
/// its first parent). REV defaults to HEAD in a working copy, else to the default ref.
pub async fn show(ctx: &Ctx, revision: Option<&str>, path: Option<&str>, name_only: bool) -> Result<ShowOut, CliError> {
    let path = repo_path(ctx, path)?;
    let path = path.as_deref();
    let (client, default_rev) = query_client(ctx).await?;
    let rev = match revision {
        Some(r) => ctx.expand_rev(r)?,
        None if ctx.has_working_copy() => ctx.expand_rev("HEAD")?,
        None => default_rev,
    };
    let page = client.log(&rev, None, 1, None).await?;
    let entry = page
        .entries
        .into_iter()
        .next()
        .ok_or_else(|| CliError::Usage(format!("{rev} has no history")))?;
    let id = entry.id.to_string();
    let from = entry.parents.first().map(ToString::to_string);
    let changes = client.diff(from.as_deref(), &id, path).await?;
    // A name that is an annotated tag (`v1` or `tags/v1`) shows what it says first
    let tag = match rev.strip_prefix("tags/").unwrap_or(&rev) {
        name if !name.is_empty() && !name.contains(['~', '^']) && name.parse::<ObjectId>().is_err() => client
            .list_tags()
            .await?
            .into_iter()
            .find(|t| t.name == name)
            .and_then(|t| {
                let a = t.annotation?;
                Some(ShowTag {
                    name: t.name,
                    tagger: format!("{} <{}>", a.tagger.name, a.tagger.email),
                    date: git_date(a.tagger.time_ms, a.tagger.tz_offset_minutes),
                    message: a.message,
                })
            }),
        _ => None,
    };
    Ok(ShowOut {
        schema: "scion.cli.show/v1",
        tag,
        diff: DiffOut {
            schema: "scion.cli.diff/v1",
            files: remote_files(&client, changes, name_only).await?,
            from,
            to: id,
            name_only,
            name_status: false,
            stat: false,
        },
        commit: log_line(entry),
    })
}

fn in_path(file: &str, path: Option<&str>) -> bool {
    match path.filter(|p| !p.is_empty()) {
        None => true,
        Some(p) => file == p || file.strip_prefix(p).is_some_and(|rest| rest.starts_with('/')),
    }
}

async fn local_diff(ctx: &Ctx, path: Option<&str>, name_only: bool) -> Result<DiffOut, CliError> {
    let wc = WorkingCopy::open(&ctx.cwd)?;
    let (client, _) = query_client(ctx).await?;
    let mut changes = wc.local_changes()?;
    changes.retain(|c| in_path(&c.path, path));
    changes.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    let mut files = Vec::with_capacity(changes.len());
    for c in changes {
        let mut sizes = None;
        let (binary, patch) = if name_only {
            (false, None)
        } else {
            let old = fetch(&client, c.old.map(|(_, id)| id)).await?;
            let new = c.new.as_ref().map(|(_, b)| b.as_ref());
            sizes = Some((size(old.as_deref()), size(new)));
            render_patch(&c.path, old.as_deref(), new)
        };
        files.push(DiffFile {
            status: status_of(c.old.is_some(), c.new.is_some()),
            old_id: c.old.map(|(_, id)| id.to_string()),
            new_id: c.new.as_ref().map(|(_, b)| ObjectId::of_blob(b).to_string()),
            old_mode: c.old.map(|(m, _)| mode_name(m)),
            new_mode: c.new.as_ref().map(|(m, _)| mode_name(*m)),
            binary,
            patch,
            sizes,
            path: c.path,
        });
    }
    let from = wc.state().current()?.map(|id| short(&id));
    Ok(DiffOut {
        schema: "scion.cli.diff/v1",
        from,
        to: "working copy".to_owned(),
        files,
        name_only,
        name_status: false,
        stat: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(split_range("a..b"), Some(("a", "b", false)));
        assert_eq!(split_range("main...feat"), Some(("main", "feat", true)));
        assert_eq!(split_range("..b"), Some(("", "b", false)));
        assert_eq!(split_range("a..."), Some(("a", "", true)));
        assert_eq!(split_range("main~1"), None);
    }

    #[test]
    fn reflog_names_who_moved_it_when_known() {
        let line = |actor: &str| ReflogLine {
            version: 1,
            changeset: "c1abcdef0123456789".into(),
            time_ms: 0,
            how: "created",
            actor: actor.into(),
            subject: "first".into(),
        };
        let out = ReflogOut {
            schema: "scion.cli.reflog/v1",
            reference: "main".into(),
            entries: vec![line("alice, bob"), line("")],
        };
        assert_eq!(
            out.text(),
            "c1abcdef0123 main@{0}: 1970-01-01 00:00 +0000 created by alice, bob: first\n\
             c1abcdef0123 main@{1}: 1970-01-01 00:00 +0000 created: first"
        );
    }

    #[test]
    fn show_text() {
        let commit = LogLine {
            id: "c1aaaaaaaaaaaaaaaa".into(),
            parents: vec!["c1bbbbbbbbbbbbbbbb".into(), "c1cccccccccccccccc".into()],
            author: "Ada".into(),
            email: "ada@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
            committer: "c".into(),
            committer_time_ms: 0,
            committer_tz_offset_minutes: 0,
            rebased_from: None,
            message: "subject\n\nbody".into(),
            generation: 3,
        };
        let commit_copy = LogLine {
            parents: vec![],
            ..commit.clone()
        };
        let diff = DiffOut {
            schema: "d",
            from: None,
            to: "x".into(),
            files: vec![],
            name_only: true,
            name_status: false,
            stat: false,
        };
        let text = ShowOut {
            schema: "s",
            tag: None,
            commit,
            diff,
        }
        .text();
        assert!(
            text.starts_with(
                "commit c1aaaaaaaaaaaaaaaa\nMerge: c1bbbbbbbbbb c1cccccccccc\nAuthor: Ada <ada@x>\nDate:   Thu Jan 1 00:00:00 1970 +0000\n\n    subject\n    \n    body"
            ),
            "{text}"
        );
        // Rebased on the server when it landed, after a commit made later
        let landed = LogLine {
            committer_time_ms: 60_000,
            rebased_from: Some("c1dddddddddddddddd".into()),
            ..commit_copy
        };
        assert!(
            commit_header(&landed).contains(
                "Date:   Thu Jan 1 00:00:00 1970 +0000\nCommitDate: Thu Jan 1 00:01:00 1970 +0000\n\
                 Rebased-from: c1dddddddddd\n\n    subject"
            ),
            "{}",
            commit_header(&landed)
        );
    }

    #[test]
    fn git_dates() {
        assert_eq!(git_date(0, 0), "Thu Jan 1 00:00:00 1970 +0000");
        assert_eq!(git_date(1_700_000_000_000, 480), "Wed Nov 15 06:13:20 2023 +0800");
        assert_eq!(git_date(1_700_000_000_000, -330), "Tue Nov 14 16:43:20 2023 -0530");
        assert_eq!(git_date(-1_000, 0), "Wed Dec 31 23:59:59 1969 +0000");
    }

    #[test]
    fn format_time_known_values() {
        assert_eq!(format_time(0, 0), "1970-01-01 00:00 +0000");
        assert_eq!(format_time(1_700_000_000_000, 480), "2023-11-15 06:13 +0800");
        assert_eq!(format_time(1_700_000_000_000, -330), "2023-11-14 16:43 -0530");
        assert_eq!(format_time(951_782_400_000, 0), "2000-02-29 00:00 +0000", "leap day");
        assert_eq!(format_time(-1_000, 0), "1969-12-31 23:59 +0000", "before the epoch");
    }

    #[test]
    fn civil_from_days_boundaries() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(10_957), (2000, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }

    #[test]
    fn diff_algorithm_choice() {
        let lines = |n: usize, tag: &str| (0..n).map(|i| format!("{tag} {i}\n")).collect::<String>().into_bytes();
        assert_eq!(diff_algorithm(b"a\nb\n", b"a\nc\n"), similar::Algorithm::Lcs);
        assert_eq!(diff_algorithm(b"", b""), similar::Algorithm::Lcs);
        // Big files that differ everywhere: the table would be too large
        assert_eq!(
            diff_algorithm(&lines(3000, "old"), &lines(3000, "new")),
            similar::Algorithm::Myers
        );
        // Big files with a small changed middle: only the middle counts
        let (mut a, mut b) = (lines(20_000, "same"), lines(20_000, "same"));
        a.extend(lines(100, "old"));
        b.extend(lines(120, "new"));
        a.extend(lines(20_000, "tail"));
        b.extend(lines(20_000, "tail"));
        assert_eq!(diff_algorithm(&a, &b), similar::Algorithm::Lcs);
    }

    #[test]
    fn render_patch_text_and_binary() {
        let (bin, p) = render_patch("a.txt", Some(b"one\ntwo\n"), Some(b"one\nthree\n"));
        assert!(!bin);
        let p = p.expect("patch");
        assert!(p.contains("--- a/a.txt") && p.contains("+++ b/a.txt"));
        assert!(p.contains("-two") && p.contains("+three"));
        let (bin, p) = render_patch("new.txt", None, Some(b"hi\n"));
        assert!(!bin && p.expect("patch").contains("+hi"));
        assert_eq!(render_patch("x.bin", Some(&[0, 1, 2]), Some(b"t")), (true, None));
        assert_eq!(render_patch("x.bin", None, Some(&[0xff, 0xfe])), (true, None));
    }

    #[test]
    fn status_and_paths() {
        assert_eq!(status_of(false, true), "A");
        assert_eq!(status_of(true, false), "D");
        assert_eq!(status_of(true, true), "M");
        assert!(in_path("src/a.rs", Some("src")));
        assert!(in_path("src", Some("src")));
        assert!(!in_path("srcx/a", Some("src")));
        assert!(in_path("any", None));
        assert!(in_path("any", Some("")));
        assert_eq!(mode_name(Mode::Executable), "executable");
        assert_eq!(mode_name(Mode::Symlink), "symlink");
        assert_eq!(mode_name(Mode::File), "file");
    }

    fn file(status: &'static str, old: Option<&'static str>, new: Option<&'static str>, binary: bool) -> DiffFile {
        DiffFile {
            path: "p".into(),
            status,
            old_id: None,
            new_id: None,
            old_mode: old,
            new_mode: new,
            binary,
            patch: Some("@@ -1 +1 @@\n".into()),
            sizes: None,
        }
    }

    #[test]
    fn diffstat_like_git() {
        let patch = |p: &str| DiffFile {
            patch: Some(p.into()),
            ..file("M", Some("file"), Some("file"), false)
        };
        let files = vec![
            DiffFile {
                path: "src/lib.rs".into(),
                ..patch("--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1,2 +1,2 @@\n one\n-two\n+three\n+four\n")
            },
            DiffFile {
                path: "b.bin".into(),
                sizes: Some((0, 12)),
                ..file("A", None, Some("file"), true)
            },
            DiffFile {
                path: "x".into(),
                patch: None,
                ..file("M", Some("file"), Some("executable"), false)
            },
        ];
        assert_eq!(
            diffstat(&files),
            " src/lib.rs | 3 ++-\n b.bin      | Bin 0 -> 12 bytes\n x          | 0\n \
             3 files changed, 2 insertions(+), 1 deletion(-)"
        );
        let big = vec![patch(&format!("@@ -1 +1 @@\n{}", "+x\n".repeat(200)))];
        let text = diffstat(&big);
        assert!(
            text.starts_with(&format!(" p | 200 {}\n", "+".repeat(STAT_BAR))),
            "{text}"
        );
        assert!(text.ends_with(" 1 file changed, 200 insertions(+)"), "{text}");
        let bin = vec![file("M", Some("file"), Some("file"), true)];
        assert_eq!(diffstat(&bin), " p | Bin\n 1 file changed");
        assert_eq!(diffstat(&[]), "");
    }

    #[test]
    fn diff_render_text() {
        let out = DiffOut {
            schema: "s",
            from: None,
            to: "t".into(),
            files: vec![],
            name_only: false,
            name_status: false,
            stat: false,
        };
        assert_eq!(out.text(), "");
        let out = DiffOut {
            files: vec![
                file("A", None, Some("file"), false),
                file("D", Some("symlink"), None, false),
                file("M", Some("file"), Some("executable"), false),
                file("M", Some("file"), Some("file"), true),
                file("A", None, Some("file"), true),
            ],
            ..out
        };
        let t = out.text();
        assert!(t.starts_with("diff --git a/p b/p\nnew file mode 100644\n"), "{t}");
        assert!(t.contains("deleted file mode 120000\n"), "{t}");
        assert!(t.contains("old mode 100644\nnew mode 100755\n"), "{t}");
        assert!(t.contains("Binary files a/p and b/p differ"), "{t}");
        assert!(t.ends_with("Binary files /dev/null and b/p differ"), "{t}");
        let names = DiffOut { name_only: true, ..out };
        assert_eq!(names.text(), "p\np\np\np\np");
        let names = DiffOut {
            name_status: true,
            ..names
        };
        assert_eq!(names.text(), "A\tp\nD\tp\nM\tp\nM\tp\nA\tp");
        let ids = DiffFile {
            old_id: Some("b1oldoldoldoldold".into()),
            new_id: Some("b1newnewnewnewnew".into()),
            ..file("M", Some("file"), Some("file"), false)
        };
        let t = DiffOut {
            files: vec![ids],
            name_only: false,
            name_status: false,
            stat: false,
            ..names
        }
        .text();
        assert!(t.contains("\nindex b1oldoldoldo..b1newnewnewn 100644\n"), "{t}");
    }

    #[test]
    fn split_lines_matches_server() {
        assert_eq!(split_lines(b"a\nb"), vec![&b"a\n"[..], b"b"]);
        assert_eq!(split_lines(b"a\n"), vec![&b"a\n"[..]]);
        assert!(split_lines(b"").is_empty());
    }

    #[test]
    fn line_ranges() {
        assert_eq!(parse_line_range("3,7").ok(), Some((3, 7)));
        assert_eq!(parse_line_range("3,+2").ok(), Some((3, 4)));
        assert_eq!(parse_line_range("3").ok(), Some((3, usize::MAX)));
        assert_eq!(parse_line_range("3,").ok(), Some((3, usize::MAX)));
        assert_eq!(parse_line_range(",4").ok(), Some((1, 4)));
        for bad in ["", "0", "5,2", "a,b", "1,+0", "x"] {
            assert!(parse_line_range(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn blame_render_text() {
        let l = |n, author: &str| BlameLineOut {
            line: n,
            commit: "c1abcdefabcdef0".into(),
            author: author.into(),
            time_ms: 0,
            tz_offset_minutes: 0,
            content: "code".into(),
        };
        let out = BlameOut {
            schema: "s",
            revision: "main".into(),
            path: "f".into(),
            lines: vec![l(1, "Ada"), l(10, "A very long author name")],
        };
        assert_eq!(
            out.text(),
            "c1abcdefabcd (Ada                     1970-01-01 00:00:00 +0000  1) code\n\
             c1abcdefabcd (A very long author name 1970-01-01 00:00:00 +0000 10) code"
        );
        assert_eq!(blame_time(1_700_000_000_000, 480), "2023-11-15 06:13:20 +0800");
    }

    #[test]
    fn log_render_text() {
        let line = |parents: usize| LogLine {
            id: "c1aaaaaaaaaaaaaaaa".into(),
            parents: vec!["c1p".into(); parents],
            author: "Ada".into(),
            email: "a@x".into(),
            time_ms: 0,
            tz_offset_minutes: 0,
            committer: "c".into(),
            committer_time_ms: 0,
            committer_tz_offset_minutes: 0,
            rebased_from: None,
            message: "subject\n\nbody".into(),
            generation: 1,
        };
        let out = LogOut {
            schema: "s",
            revision: "main".into(),
            path: None,
            entries: vec![line(1), line(2)],
            oneline: false,
        };
        assert_eq!(
            out.text(),
            "commit c1aaaaaaaaaaaaaaaa\nAuthor: Ada <a@x>\nDate:   Thu Jan 1 00:00:00 1970 +0000\n\n    subject\n    \n    body\n\n\
             commit c1aaaaaaaaaaaaaaaa\nMerge: c1p c1p\nAuthor: Ada <a@x>\nDate:   Thu Jan 1 00:00:00 1970 +0000\n\n    \
             subject\n    \n    body"
        );
        let one = LogOut {
            oneline: true,
            entries: vec![line(1)],
            ..out
        };
        assert_eq!(one.text(), "c1aaaaaaaaaa subject");
        let out = LogOut { oneline: false, ..one };
        let empty = LogOut { entries: vec![], ..out };
        assert_eq!(empty.text(), "");
        let bare = LogLine {
            message: String::new(),
            ..line(1)
        };
        assert!(commit_header(&bare).ends_with("+0000"));
    }
}
