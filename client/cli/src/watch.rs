//! `scn watch`: follows a repository's branches on the server and prints each commit as it
//! lands, as a feed for people and agents. It subscribes to the refs (`WatchRefs`), and for each
//! move reads the history from the new tip down to the old one, so commits are listed even when
//! the server coalesces several moves of a ref into one.

use scion_client::{Client, LogItem, ObjectId};
use serde::Serialize;

use crate::commands::{Ctx, query_client};
use crate::output::{CliError, Render};

/// Commits listed for one move of a ref; past that, `truncated` is set.
const MAX_COMMITS: usize = 100;

/// A ref's tip when the watch starts.
pub struct Tip {
    pub name: String,
    pub id: String,
}

/// One move of a ref: the commits it brought in, oldest first.
#[derive(Debug, Serialize)]
pub struct RefUpdate {
    pub schema: &'static str,
    #[serde(rename = "ref")]
    pub reference: String,
    /// The tip before; None when the ref is new to the watch.
    pub old: Option<String>,
    pub new: String,
    pub version: u64,
    pub commits: Vec<WatchCommit>,
    /// More than [`MAX_COMMITS`] came in, or the ref moved to a commit that does not descend
    /// from the old tip (a force-move): `commits` is not the whole list.
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct WatchCommit {
    pub id: String,
    pub author: String,
    pub email: String,
    pub subject: String,
    /// The commit as it was made, when the server rebased it while landing it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rebased_from: Option<String>,
}

fn short(id: &str) -> &str {
    &id[..id.len().min(12)]
}

/// One line per commit, `REF  ID  AUTHOR  SUBJECT`, like a `git log --oneline` that keeps
/// growing.
impl Render for RefUpdate {
    fn text(&self) -> String {
        let r = &self.reference;
        if self.old.is_none() {
            return format!("{r}  {}  (new)", short(&self.new));
        }
        let mut lines: Vec<String> = self
            .commits
            .iter()
            .map(|c| {
                let rebased = c
                    .rebased_from
                    .as_deref()
                    .map(|o| format!("  (rebased from {})", short(o)))
                    .unwrap_or_default();
                format!("{r}  {}  {}  {}{rebased}", short(&c.id), c.author, c.subject)
            })
            .collect();
        if self.truncated {
            let old = self.old.as_deref().unwrap_or_default();
            lines.push(format!(
                "{r}  {}..{}  (more commits not listed)",
                short(old),
                short(&self.new)
            ));
        }
        lines.join("\n")
    }
}

fn commit_of(e: &LogItem) -> WatchCommit {
    WatchCommit {
        id: e.id.to_string(),
        author: e.author.name.clone(),
        email: e.author.email.clone(),
        subject: crate::output::subject(&e.message),
        rebased_from: e.rebased_from().map(|id| id.to_string()),
    }
}

/// The commits from `old` (exclusive) up to `new`, oldest first, and whether the list stops
/// short of `old`.
async fn commits_between(
    client: &Client,
    old: Option<ObjectId>,
    new: ObjectId,
) -> Result<(Vec<WatchCommit>, bool), CliError> {
    let Some(old) = old else {
        return Ok((Vec::new(), false));
    };
    let page = client.log(&new.to_string(), None, MAX_COMMITS as u32 + 1, None).await?;
    let mut commits = Vec::new();
    for e in &page.entries {
        if e.id == old {
            commits.reverse();
            return Ok((commits, false));
        }
        if commits.len() == MAX_COMMITS {
            break;
        }
        commits.push(commit_of(e));
    }
    commits.reverse();
    Ok((commits, true))
}

/// Watches `refs` (all of the repository's when empty) and calls `emit` for each move, until
/// `count` moves (forever when None). The refs' tips when it starts go to `start` first.
pub async fn watch(
    ctx: &Ctx,
    refs: &[String],
    count: Option<usize>,
    start: &mut dyn FnMut(&[Tip]),
    emit: &mut dyn FnMut(&RefUpdate),
) -> Result<(), CliError> {
    let (client, _) = query_client(ctx).await?;
    // The tips to start from; the subscription's first answer repeats them (and any move since)
    let mut tips: std::collections::BTreeMap<String, ObjectId> = client
        .list_refs()
        .await?
        .into_iter()
        .filter(|(name, _, _)| refs.is_empty() || refs.contains(name))
        .map(|(name, tip, _)| (name, tip))
        .collect();
    let at: Vec<Tip> = tips
        .iter()
        .map(|(name, id)| Tip {
            name: name.clone(),
            id: id.to_string(),
        })
        .collect();
    start(&at);
    let names: Vec<&str> = refs.iter().map(String::as_str).collect();
    let mut w = client.watch_refs(&names).await?;
    let mut seen = 0;
    while count.is_none_or(|n| seen < n) {
        let change = w.next().await?;
        let old = tips.insert(change.name.clone(), change.changeset);
        if old == Some(change.changeset) {
            continue;
        }
        let (commits, truncated) = commits_between(&client, old, change.changeset).await?;
        emit(&RefUpdate {
            schema: "scion.cli.watch/v1",
            reference: change.name,
            old: old.map(|o| o.to_string()),
            new: change.changeset.to_string(),
            version: change.version,
            commits,
            truncated,
        });
        seen += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(commits: Vec<WatchCommit>, truncated: bool) -> RefUpdate {
        RefUpdate {
            schema: "s",
            reference: "main".into(),
            old: Some("c1oldoldoldold".into()),
            new: "c1newnewnewnew".into(),
            version: 2,
            commits,
            truncated,
        }
    }

    #[test]
    fn text() {
        let c = |id: &str, rebased: Option<&str>| WatchCommit {
            id: id.into(),
            author: "agent-1".into(),
            email: "a@x".into(),
            subject: "tb title".into(),
            rebased_from: rebased.map(Into::into),
        };
        assert_eq!(
            update(
                vec![
                    c("c1aaaaaaaaaaaaaa", None),
                    c("c1bbbbbbbbbbbbbb", Some("c1cccccccccccccc"))
                ],
                false
            )
            .text(),
            "main  c1aaaaaaaaaa  agent-1  tb title\nmain  c1bbbbbbbbbb  agent-1  tb title  (rebased from c1cccccccccc)"
        );
        assert_eq!(
            update(vec![], true).text(),
            "main  c1oldoldoldo..c1newnewnewn  (more commits not listed)"
        );
        let new_ref = RefUpdate {
            old: None,
            ..update(vec![], true)
        };
        assert_eq!(new_ref.text(), "main  c1newnewnewn  (new)");
        assert_eq!(update(vec![], false).text(), "");
        let v = serde_json::to_value(update(vec![], false)).expect("json");
        assert_eq!((v["ref"].as_str(), v["version"].as_u64()), (Some("main"), Some(2)));
    }
}
