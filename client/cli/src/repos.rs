//! `scn repo` 与 `scn branch`：仓库与分支管理。

use scion_client::{NewRepo, RepoInfo};
use serde::Serialize;

use crate::commands::{Ctx, DEFAULT_REF, connect, query_client};
use crate::history::format_time;
use crate::output::{CliError, Render};

/// `scn repo ls` 默认最多列出的仓库数。
pub const DEFAULT_LIST_LIMIT: usize = 100;

#[derive(Debug, Serialize)]
pub struct RepoOut {
    pub schema: &'static str,
    pub name: String,
    pub default_branch: String,
    pub description: String,
    pub created_at_ms: i64,
    pub deleted_at_ms: Option<i64>,
    pub purge_after_ms: Option<i64>,
    pub forked_from: Option<String>,
}

impl From<RepoInfo> for RepoOut {
    fn from(r: RepoInfo) -> Self {
        Self {
            schema: "scion.cli.repo/v1",
            name: r.name,
            default_branch: r.default_branch,
            description: r.description,
            created_at_ms: r.created_at_ms,
            deleted_at_ms: r.deleted_at_ms,
            purge_after_ms: r.purge_after_ms,
            forked_from: r.forked_from,
        }
    }
}

impl RepoOut {
    /// 一行摘要：名字、默认分支、fork 来源、删除状态。
    fn line(&self) -> String {
        let mut s = format!("{}  (default branch {})", self.name, self.default_branch);
        if let Some(f) = &self.forked_from {
            s.push_str(&format!("  forked from {f}"));
        }
        if let Some(p) = self.purge_after_ms {
            s.push_str(&format!("  deleted, purged after {}", format_time(p, 0)));
        }
        s
    }
}

impl Render for RepoOut {
    fn text(&self) -> String {
        let mut s = self.line();
        if !self.description.is_empty() {
            s.push_str(&format!("\n  {}", self.description));
        }
        s
    }
}

#[derive(Debug, Serialize)]
pub struct RepoListOut {
    pub schema: &'static str,
    pub repos: Vec<RepoOut>,
    /// 达到 `--limit` 时还有更多的仓库。
    pub truncated: bool,
}

impl Render for RepoListOut {
    fn text(&self) -> String {
        let mut lines: Vec<String> = self.repos.iter().map(RepoOut::line).collect();
        if lines.is_empty() {
            lines.push("no repositories".into());
        }
        if self.truncated {
            lines.push("(more; raise --limit)".into());
        }
        lines.join("\n")
    }
}

/// 不需要仓库上下文的连接（仓库管理接口按名字操作）。
async fn admin_client(ctx: &Ctx) -> Result<scion_client::Client, CliError> {
    connect(&ctx.endpoint_or_default(), None).await
}

pub async fn create(
    ctx: &Ctx,
    name: &str,
    default_branch: Option<&str>,
    description: &str,
) -> Result<RepoOut, CliError> {
    let c = admin_client(ctx).await?;
    let opts = NewRepo {
        default_branch: default_branch.unwrap_or_default().to_owned(),
        description: description.to_owned(),
    };
    Ok(c.create_repo(name, opts).await?.into())
}

pub async fn info(ctx: &Ctx, name: &str) -> Result<RepoOut, CliError> {
    Ok(admin_client(ctx).await?.get_repo(name, true).await?.into())
}

pub async fn list(ctx: &Ctx, prefix: &str, deleted: bool, limit: usize) -> Result<RepoListOut, CliError> {
    let c = admin_client(ctx).await?;
    let mut repos = Vec::new();
    let mut token: Option<String> = None;
    let truncated = loop {
        let want = limit.saturating_sub(repos.len()).clamp(1, 1000);
        let page = c
            .list_repos(prefix, token.as_deref(), u32::try_from(want).unwrap_or(1000), deleted)
            .await?;
        repos.extend(page.repos.into_iter().map(RepoOut::from));
        token = page.next_page_token;
        if token.is_none() {
            break false;
        }
        if repos.len() >= limit {
            break true;
        }
    };
    repos.truncate(limit);
    Ok(RepoListOut {
        schema: "scion.cli.repos/v1",
        repos,
        truncated,
    })
}

pub async fn update(
    ctx: &Ctx,
    name: &str,
    default_branch: Option<&str>,
    description: Option<&str>,
) -> Result<RepoOut, CliError> {
    if default_branch.is_none() && description.is_none() {
        return Err(CliError::Usage(
            "nothing to change; pass --default-branch or --description".into(),
        ));
    }
    let c = admin_client(ctx).await?;
    Ok(c.update_repo(name, default_branch, description).await?.into())
}

pub async fn delete(ctx: &Ctx, name: &str) -> Result<RepoOut, CliError> {
    Ok(admin_client(ctx).await?.delete_repo(name).await?.into())
}

pub async fn restore(ctx: &Ctx, name: &str) -> Result<RepoOut, CliError> {
    Ok(admin_client(ctx).await?.restore_repo(name).await?.into())
}

pub async fn fork(ctx: &Ctx, source: &str, target: &str) -> Result<RepoOut, CliError> {
    Ok(admin_client(ctx).await?.fork_repo(source, target).await?.into())
}

// ---------- branch ----------

#[derive(Debug, Serialize)]
pub struct BranchOut {
    pub name: String,
    pub changeset: String,
    pub version: u64,
    /// 是否为仓库的默认分支。
    #[serde(rename = "default")]
    pub is_default: bool,
}

#[derive(Debug, Serialize)]
pub struct BranchListOut {
    pub schema: &'static str,
    pub repo: String,
    pub branches: Vec<BranchOut>,
    /// The working copy's branch, which the text marks with `*` (outside one: the default branch).
    #[serde(skip)]
    pub current: Option<String>,
    /// `-v`: each branch's commit too.
    #[serde(skip)]
    pub verbose: bool,
}

/// Like `git branch`.
impl Render for BranchListOut {
    fn text(&self) -> String {
        let width = self.branches.iter().map(|b| b.name.chars().count()).max().unwrap_or(0);
        self.branches
            .iter()
            .map(|b| {
                let current = match &self.current {
                    Some(c) => *c == b.name,
                    None => b.is_default,
                };
                let mark = if current { "*" } else { " " };
                if self.verbose {
                    format!("{mark} {:<width$} {}", b.name, short(&b.changeset))
                } else {
                    format!("{mark} {}", b.name)
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Debug, Serialize)]
pub struct BranchChangeOut {
    pub schema: &'static str,
    pub repo: String,
    pub name: String,
    /// created | deleted
    pub action: &'static str,
    /// The commit the branch points at (for a deleted one: pointed at).
    pub changeset: Option<String>,
    pub version: Option<u64>,
}

/// Like git: creating is silent, deleting prints `Deleted branch NAME (was ID).`
impl Render for BranchChangeOut {
    fn text(&self) -> String {
        match (self.action, &self.changeset) {
            ("deleted", Some(c)) => format!("Deleted branch {} (was {}).", self.name, short(c)),
            ("deleted", None) => format!("Deleted branch {}.", self.name),
            _ => String::new(),
        }
    }

    const REPORTS: bool = true;
}

fn short(id: &str) -> &str {
    &id[..id.len().min(12)]
}

pub async fn branches(ctx: &Ctx) -> Result<BranchListOut, CliError> {
    let (c, _) = query_client(ctx).await?;
    let current = ctx
        .working_copy()
        .map_or_else(|| DEFAULT_REF.to_owned(), |w| w.state().reference.clone());
    let repo = c.repo_name().unwrap_or_default().to_owned();
    let default = c.get_repo(&repo, false).await?.default_branch;
    let branches = c
        .list_refs()
        .await?
        .into_iter()
        .filter(|(name, _, _)| !name.starts_with(TAG_PREFIX))
        .map(|(name, id, version)| BranchOut {
            is_default: name == default,
            name,
            changeset: id.to_string(),
            version,
        })
        .collect();
    Ok(BranchListOut {
        schema: "scion.cli.branches/v1",
        repo,
        branches,
        current: (ctx.repo.is_none() && ctx.has_working_copy()).then_some(current),
        verbose: false,
    })
}

#[derive(Debug, Serialize)]
pub struct CurrentBranchOut {
    pub schema: &'static str,
    pub branch: String,
}

impl Render for CurrentBranchOut {
    fn text(&self) -> String {
        self.branch.clone()
    }
}

/// `scn branch --show-current`: the working copy's branch, without asking the server.
pub fn current_branch(ctx: &Ctx) -> Result<CurrentBranchOut, CliError> {
    Ok(CurrentBranchOut {
        schema: "scion.cli.current-branch/v1",
        branch: scion_wc::WorkingCopy::open(&ctx.cwd)?.state().reference.clone(),
    })
}

pub async fn create_branch(ctx: &Ctx, name: &str, revision: Option<&str>) -> Result<BranchChangeOut, CliError> {
    let (c, default_rev) = query_client(ctx).await?;
    let rev = c
        .resolve_revision(&ctx.expand_rev(revision.unwrap_or(&default_rev))?)
        .await?;
    let (target, version) = c.create_ref(name, &rev.id).await?;
    Ok(BranchChangeOut {
        schema: "scion.cli.branch/v1",
        repo: c.repo_name().unwrap_or_default().to_owned(),
        name: name.to_owned(),
        action: "created",
        changeset: Some(target.to_string()),
        version: Some(version),
    })
}

pub async fn delete_branch(ctx: &Ctx, name: &str) -> Result<BranchChangeOut, CliError> {
    let (c, _) = query_client(ctx).await?;
    let was = c.resolve_ref(name).await?.map(|t| t.changeset.to_string());
    c.delete_ref(name, None).await?;
    Ok(BranchChangeOut {
        schema: "scion.cli.branch/v1",
        repo: c.repo_name().unwrap_or_default().to_owned(),
        name: name.to_owned(),
        action: "deleted",
        changeset: was,
        version: None,
    })
}

/// Tags are refs under this prefix (the server refuses to move them).
pub const TAG_PREFIX: &str = "tags/";

#[derive(Debug, Serialize)]
pub struct TagOut {
    pub name: String,
    pub changeset: String,
    /// An annotated tag's message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TagListOut {
    pub schema: &'static str,
    pub repo: String,
    pub tags: Vec<TagOut>,
    /// `-n`: each name with the first line of its message.
    #[serde(skip)]
    pub notes: bool,
}

/// Like `git tag`: one name per line; with -n git's `NAME            SUBJECT`.
impl Render for TagListOut {
    fn text(&self) -> String {
        self.tags
            .iter()
            .map(|t| match (&t.message, self.notes) {
                (Some(m), true) => format!("{:<15} {}", t.name, m.lines().next().unwrap_or_default()),
                _ => t.name.clone(),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Debug, Serialize)]
pub struct TagChangeOut {
    pub schema: &'static str,
    pub name: String,
    /// created | deleted
    pub action: &'static str,
    /// The tagged commit (for a deleted tag: the one it named).
    pub changeset: Option<String>,
}

/// Like git: creating is silent, deleting prints `Deleted tag 'NAME' (was ID)`.
impl Render for TagChangeOut {
    fn text(&self) -> String {
        match (self.action, &self.changeset) {
            ("deleted", Some(c)) => format!("Deleted tag '{}' (was {})", self.name, short(c)),
            ("deleted", None) => format!("Deleted tag '{}'", self.name),
            _ => String::new(),
        }
    }

    const REPORTS: bool = true;
}

pub async fn tags(ctx: &Ctx, notes: bool) -> Result<TagListOut, CliError> {
    let (c, _) = query_client(ctx).await?;
    let tags = c
        .list_tags()
        .await?
        .into_iter()
        .map(|t| TagOut {
            name: t.name,
            changeset: t.target.to_string(),
            message: t.annotation.map(|a| a.message),
        })
        .collect();
    Ok(TagListOut {
        schema: "scion.cli.tags/v1",
        repo: c.repo_name().unwrap_or_default().to_owned(),
        tags,
        notes,
    })
}

/// `scn tag NAME [REV]`: a fixed name for a commit (default HEAD in a working copy). With a
/// message, an annotated tag, its tagger the configured author.
pub async fn create_tag(
    ctx: &Ctx,
    name: &str,
    revision: Option<&str>,
    message: Option<&str>,
) -> Result<TagChangeOut, CliError> {
    let (c, default_rev) = query_client(ctx).await?;
    let rev = match revision {
        Some(r) => ctx.expand_rev(r)?,
        None if ctx.has_working_copy() => ctx.expand_rev("HEAD")?,
        None => default_rev,
    };
    let target = c.resolve_revision(&rev).await?.id;
    let (target, _) = match message {
        Some(m) => {
            let tagger = crate::commands::own_author().map(|(n, e)| crate::commands::signature_now(n, e));
            c.create_annotated_tag(name, &target, m, tagger.as_ref()).await?
        }
        None => c.create_ref(&format!("{TAG_PREFIX}{name}"), &target).await?,
    };
    Ok(TagChangeOut {
        schema: "scion.cli.tag/v1",
        name: name.to_owned(),
        action: "created",
        changeset: Some(target.to_string()),
    })
}

pub async fn delete_tag(ctx: &Ctx, name: &str) -> Result<TagChangeOut, CliError> {
    let (c, _) = query_client(ctx).await?;
    let tag = format!("{TAG_PREFIX}{name}");
    let was = c.resolve_ref(&tag).await?.map(|t| t.changeset.to_string());
    c.delete_ref(&tag, None).await?;
    Ok(TagChangeOut {
        schema: "scion.cli.tag/v1",
        name: name.to_owned(),
        action: "deleted",
        changeset: was,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_texts() {
        let empty = TagListOut {
            schema: "s",
            repo: "r".into(),
            tags: vec![],
            notes: false,
        };
        assert_eq!(empty.text(), "");
        let list = TagListOut {
            tags: ["v1", "v2"]
                .map(|n| TagOut {
                    name: n.into(),
                    changeset: "c1abcdefabcdef00".into(),
                    message: None,
                })
                .into(),
            ..empty
        };
        assert_eq!(list.text(), "v1\nv2");
        let made = TagChangeOut {
            schema: "s",
            name: "v1".into(),
            action: "created",
            changeset: Some("c1abcdefabcdef00".into()),
        };
        assert_eq!(made.text(), "");
        let gone = TagChangeOut {
            action: "deleted",
            ..made
        };
        assert_eq!(gone.text(), "Deleted tag 'v1' (was c1abcdefabcd)");
        assert_eq!(
            TagChangeOut {
                changeset: None,
                ..gone
            }
            .text(),
            "Deleted tag 'v1'"
        );
    }

    fn repo(purge: Option<i64>, fork: Option<&str>, description: &str) -> RepoOut {
        RepoOut {
            schema: "s",
            name: "org/m".into(),
            default_branch: "main".into(),
            description: description.into(),
            created_at_ms: 0,
            deleted_at_ms: purge.map(|p| p - 1),
            purge_after_ms: purge,
            forked_from: fork.map(str::to_owned),
        }
    }

    #[test]
    fn renders_repos_and_branches() {
        assert_eq!(repo(None, None, "").text(), "org/m  (default branch main)");
        let t = repo(Some(86_400_000), Some("up/m"), "weights").text();
        assert!(
            t.contains("forked from up/m") && t.contains("deleted, purged after 1970-01-02"),
            "{t}"
        );
        assert!(t.ends_with("\n  weights"));
        let list = RepoListOut {
            schema: "s",
            repos: vec![],
            truncated: true,
        };
        assert_eq!(list.text(), "no repositories\n(more; raise --limit)");
        let b = BranchListOut {
            schema: "s",
            repo: "r".into(),
            branches: vec![
                BranchOut {
                    name: "main".into(),
                    changeset: "c1abcdefghijklmn".into(),
                    version: 3,
                    is_default: true,
                },
                BranchOut {
                    name: "dev".into(),
                    changeset: "c1zz".into(),
                    version: 1,
                    is_default: false,
                },
            ],
            current: None,
            verbose: false,
        };
        assert_eq!(b.text(), "* main\n  dev", "outside a working copy: the default branch");
        let b = BranchListOut { verbose: true, ..b };
        assert_eq!(b.text(), "* main c1abcdefghij\n  dev  c1zz");
        let b = BranchListOut { verbose: false, ..b };
        let b = BranchListOut {
            current: Some("dev".into()),
            ..b
        };
        assert_eq!(b.text(), "  main\n* dev");
        let c = BranchChangeOut {
            schema: "s",
            repo: "r".into(),
            name: "x".into(),
            action: "deleted",
            changeset: Some("c1abcdefghijklmn".into()),
            version: None,
        };
        assert_eq!(c.text(), "Deleted branch x (was c1abcdefghij).");
        let c = BranchChangeOut { changeset: None, ..c };
        assert_eq!(c.text(), "Deleted branch x.");
        assert_eq!(BranchChangeOut { action: "created", ..c }.text(), "");
    }
}
