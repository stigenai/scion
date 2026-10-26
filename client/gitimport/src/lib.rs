//! 把 git 历史导入 Scion（服务端设计文档 §14 M1「git 导入」）。
//!
//! 只通过公开协议写入：每个 git 提交变成一个 Scion 提交，delta 为相对第一父提交的改动。
//! 结果是确定的：作者、提交者、时间、时区、说明原样保留，`change_id` 取自 git sha，
//! git sha 记在扩展字段 `git.sha1` 中。因此重复导入同一历史得到相同的提交 id。

mod git;

use std::collections::HashMap;
use std::path::Path;

use scion_client::{Client, EntryMode, ObjectId, Signature};
use thiserror::Error;

pub use git::{Change, Commit, GitMode, Ident, parse_commit, parse_ident, parse_raw_diff};

/// Refs under this prefix are tags (the server never moves them).
pub const TAG_PREFIX: &str = "tags/";

/// 记录 git 提交 sha 的扩展字段。
pub const GIT_SHA1_EXTENSION: &str = "git.sha1";
/// The extension holding a commit's git object, for commits Scion's fields cannot rebuild.
pub const GIT_COMMIT_EXTENSION: &str = "git.commit";

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Git(String),
    #[error("cannot parse git output: {0}")]
    Parse(String),
    #[error("path is not valid UTF-8: {0:?}")]
    NonUtf8Path(String),
    #[error("ref {reference:?} already points to {current}, which is not in the imported first-parent history")]
    Diverged { reference: String, current: ObjectId },
    #[error(transparent)]
    Client(#[from] scion_client::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 导入结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    /// 导入的提交数（含此前已导入、本次复用的）。
    pub commits: usize,
    /// Commits that were not on the server yet and were created by this import.
    pub new_commits: usize,
    /// 因为是子模块而跳过的条目数。
    pub skipped_gitlinks: usize,
    /// git sha → Scion 提交 id, for the commits this import created and the already imported
    /// commits it looked up.
    pub mapping: HashMap<String, ObjectId>,
    /// 目标 ref 最终指向的提交；git 历史为空时为 None。
    pub tip: Option<ObjectId>,
}

fn signature(i: &Ident) -> Signature {
    Signature {
        name: i.name.clone(),
        email: i.email.clone(),
        time_ms: i.time_ms,
        tz_offset_minutes: i.tz_offset_minutes,
    }
}

fn entry_mode(m: GitMode) -> Option<EntryMode> {
    match m {
        GitMode::File => Some(EntryMode::File),
        GitMode::Executable => Some(EntryMode::Executable),
        GitMode::Symlink => Some(EntryMode::Symlink),
        GitMode::Gitlink => None,
    }
}

/// Whether the commit object comes back byte for byte from the fields Scion keeps, as the git
/// gateway rebuilds it. When not (a signature, an `encoding` header, a message that is not
/// UTF-8, an unusual identity), the import keeps the raw object too.
fn rebuilds(c: &Commit) -> bool {
    let sha = |s: &str| {
        let mut out = [0u8; 20];
        hex::decode_to_slice(s, &mut out).ok().map(|()| out)
    };
    let Some(tree) = sha(&c.tree) else { return false };
    let Some(parents) = c.parents.iter().map(|p| sha(p)).collect::<Option<Vec<_>>>() else {
        return false;
    };
    let sig = |i: &Ident| scion_object::Signature {
        name: i.name.clone(),
        email: i.email.clone(),
        time_ms: i.time_ms,
        tz_offset_minutes: i.tz_offset_minutes,
    };
    let body = scion_object::git::commit_body(None, &tree, &parents, &sig(&c.author), &sig(&c.committer), &c.message);
    body == c.raw
}

/// 由 git sha 推导 change_id 与扩展字段值。
fn sha_bytes(sha: &str) -> Result<([u8; 16], Vec<u8>)> {
    let raw = hex::decode(sha).map_err(|_| Error::Parse(format!("bad commit sha {sha:?}")))?;
    let change_id: [u8; 16] = raw
        .get(..16)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| Error::Parse(format!("short sha {sha:?}")))?;
    Ok((change_id, raw))
}

/// 每次合入的提交数上限，与服务端默认的单次合入上限一致：服务端要记录每个进入 ref 的提交的 change id，
/// 一次合入过多提交会被拒绝。
pub const LAND_STEP: usize = 256;

/// 导入选项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// 每次合入（快进）最多包含的提交数，见 [`LAND_STEP`]。
    pub land_step: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self { land_step: LAND_STEP }
    }
}

/// 导入 `repo_dir` 中 `git_ref` 可达的全部提交，并把 Scion 的 `target_ref` 快进到最新提交。
///
/// `progress` 在每个提交导入后被调用（已完成数, 总数）。
pub async fn import(
    client: &Client,
    repo_dir: &Path,
    git_ref: &str,
    target_ref: &str,
    progress: impl FnMut(usize, usize),
) -> Result<Summary> {
    import_with(client, repo_dir, git_ref, target_ref, Options::default(), progress).await
}

/// 同 [`import`]，可指定选项。
pub async fn import_with(
    client: &Client,
    repo_dir: &Path,
    git_ref: &str,
    target_ref: &str,
    options: Options,
    mut progress: impl FnMut(usize, usize),
) -> Result<Summary> {
    let repo = git::Repo::open(repo_dir)?;
    let all = repo.commits(git_ref)?;
    // Commits already on the server (an earlier import, or another branch sharing history) are
    // skipped: imports always create parents first, so excluding the newest known commits and
    // their ancestors leaves exactly the missing ones.
    let mut mapping: HashMap<String, ObjectId> = HashMap::new();
    let mut known: Vec<String> = Vec::new();
    let mut commits = all.clone();
    while let Some(found) = probe_known(client, &commits).await? {
        mapping.insert(found.0.clone(), found.1);
        known.push(found.0);
        commits = repo.commits_excluding(git_ref, &known)?;
    }
    let mut cat = repo.cat_file()?;
    let mut first_parent_of: HashMap<ObjectId, Option<ObjectId>> = HashMap::with_capacity(commits.len());
    let mut skipped_gitlinks = 0;

    for (i, sha) in commits.iter().enumerate() {
        let commit = cat.commit(sha)?;
        let mut parents: Vec<ObjectId> = Vec::with_capacity(commit.parents.len());
        for p in &commit.parents {
            let id = match mapping.get(p) {
                Some(id) => *id,
                // A parent that was already on the server
                None => {
                    let id = known_commit(client, p)
                        .await?
                        .ok_or_else(|| Error::Parse(format!("parent {p} of {sha} not imported yet")))?;
                    mapping.insert(p.clone(), id);
                    id
                }
            };
            parents.push(id);
        }
        let changes = repo.changes(sha, commit.parents.first().map(String::as_str))?;
        let (change_id, raw_sha) = sha_bytes(sha)?;

        let mut b = client
            .changeset(parents.first().copied())
            .message(commit.message.clone())
            .author_signature(signature(&commit.author))
            .committer_signature(signature(&commit.committer))
            .change_id(change_id)
            .extension(GIT_SHA1_EXTENSION, raw_sha);
        if !rebuilds(&commit) {
            b = b.extension(GIT_COMMIT_EXTENSION, commit.raw.clone());
        }
        for p in parents.iter().skip(1) {
            b = b.parent(*p);
        }
        for c in &changes {
            b = match &c.new {
                None => b.delete(&c.path),
                Some((mode, blob)) => match entry_mode(*mode) {
                    Some(m) => b.put_with_mode(&c.path, m, cat.blob(blob)?),
                    None => {
                        skipped_gitlinks += 1;
                        b
                    }
                },
            };
        }
        let created = b.create().await?;
        first_parent_of.insert(created.id, parents.first().copied());
        mapping.insert(sha.clone(), created.id);
        progress(i + 1, commits.len());
    }

    let tip = all.last().and_then(|sha| mapping.get(sha).copied());
    if let Some(tip) = tip {
        // An annotated tag imported as a tag keeps its tag object
        let annotation = if target_ref.starts_with(TAG_PREFIX) {
            repo.tag_object(&mut cat, git_ref)?
        } else {
            None
        };
        land_chain(client, target_ref, tip, &first_parent_of, options.land_step, annotation).await?;
    }
    Ok(Summary {
        commits: all.len(),
        new_commits: commits.len(),
        skipped_gitlinks,
        mapping,
        tip,
    })
}

/// The Scion commit imported from git commit `sha`, if the server has it.
async fn known_commit(client: &Client, sha: &str) -> Result<Option<ObjectId>> {
    match client.resolve_revision(sha).await {
        Ok(r) => Ok(Some(r.id)),
        Err(e) if e.is_not_found() => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// One commit of `commits` (topological order, parents first) that the server already has,
/// preferring the newest: probes the newest commit, then exponentially older ones, then the
/// oldest. None when none of the probed commits is known, so a fresh import costs a few
/// lookups, not one per commit.
async fn probe_known(client: &Client, commits: &[String]) -> Result<Option<(String, ObjectId)>> {
    let n = commits.len();
    let mut back = 0usize;
    let mut step = 1usize;
    while back < n {
        let sha = &commits[n - 1 - back];
        if let Some(id) = known_commit(client, sha).await? {
            return Ok(Some((sha.clone(), id)));
        }
        back += step;
        step *= 2;
    }
    match commits.first() {
        Some(oldest) if n > 1 => Ok(known_commit(client, oldest).await?.map(|id| (oldest.clone(), id))),
        _ => Ok(None),
    }
}

/// 把 ref 快进到 `tip`：ref 当前提交必须在 `tip` 的第一父提交链上（否则 git 历史被改写过）。
/// 沿第一父提交链分段快进，每段最多 `step` 个提交。
async fn land_chain(
    client: &Client,
    target_ref: &str,
    tip: ObjectId,
    first_parent_of: &HashMap<ObjectId, Option<ObjectId>>,
    step: usize,
    annotation: Option<Vec<u8>>,
) -> Result<()> {
    let current = client.resolve_ref(target_ref).await?.map(|t| t.changeset);
    // A tag (`tags/…`) is never moved by the server: create it at the tip in one step. Importing
    // the same tag again is a no-op; a tag that points elsewhere is reported, not moved.
    if target_ref.starts_with(TAG_PREFIX) {
        return match current {
            None => {
                match annotation {
                    Some(raw) => client.create_tag_from_git(target_ref, &tip, raw).await?,
                    None => client.create_ref(target_ref, &tip).await?,
                };
                Ok(())
            }
            Some(c) if c == tip => Ok(()),
            Some(c) => Err(Error::Diverged {
                reference: target_ref.to_owned(),
                current: c,
            }),
        };
    }
    let mut chain: Vec<ObjectId> = Vec::new();
    let mut cur = Some(tip);
    while let Some(c) = cur {
        if Some(c) == current {
            break;
        }
        chain.push(c);
        cur = match first_parent_of.get(&c) {
            Some(p) => *p,
            // Imported earlier: ask the server
            None => client.resolve_revision(&c.to_string()).await?.parents.first().copied(),
        };
    }
    if let Some(cur_tip) = current {
        if cur.is_none() {
            return Err(Error::Diverged {
                reference: target_ref.to_owned(),
                current: cur_tip,
            });
        }
    }
    // 只允许快进：导入的历史必须原样进入 ref，期间 ref 被别人移动时报错而不是 rebase
    chain.reverse();
    for part in chain.chunks(step.max(1)) {
        if let Some(last) = part.last() {
            client.land_fast_forward(target_ref, last).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_raw_object_only_when_fields_cannot_rebuild_it() {
        let plain = format!(
            "tree {}\nparent {}\nauthor A <a@x> 1700000000 +0800\ncommitter C <c@x> 1700000001 -0130\n\nmsg\n",
            "ab".repeat(20),
            "cd".repeat(20)
        );
        assert!(rebuilds(&parse_commit(plain.as_bytes()).expect("parse")));
        let signed = plain.replace("\n\nmsg", "\ngpgsig -----BEGIN-----\n x\n -----END-----\n\nmsg");
        assert!(!rebuilds(&parse_commit(signed.as_bytes()).expect("parse")));
        let encoded = plain.replace("\n\nmsg", "\nencoding ISO-8859-1\n\nmsg");
        assert!(!rebuilds(&parse_commit(encoded.as_bytes()).expect("parse")));
        // git writes -0000 for an unknown zone; Scion keeps +0000
        let minus = plain.replace("+0800", "-0000");
        assert!(!rebuilds(&parse_commit(minus.as_bytes()).expect("parse")));
        let bad_tree = plain.replace(&"ab".repeat(20), "nope");
        assert!(!rebuilds(&parse_commit(bad_tree.as_bytes()).expect("parse")));
        let bad_parent = plain.replace(&"cd".repeat(20), "nope");
        assert!(!rebuilds(&parse_commit(bad_parent.as_bytes()).expect("parse")));
    }

    #[test]
    fn sha_bytes_cases() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let (cid, raw) = sha_bytes(sha).expect("ok");
        assert_eq!(raw.len(), 20);
        assert_eq!(&cid[..], &raw[..16]);
        assert!(matches!(sha_bytes("zz"), Err(Error::Parse(_))));
        assert!(matches!(sha_bytes("0123"), Err(Error::Parse(_))));
    }

    #[test]
    fn entry_mode_mapping() {
        assert_eq!(entry_mode(GitMode::File), Some(EntryMode::File));
        assert_eq!(entry_mode(GitMode::Executable), Some(EntryMode::Executable));
        assert_eq!(entry_mode(GitMode::Symlink), Some(EntryMode::Symlink));
        assert_eq!(entry_mode(GitMode::Gitlink), None);
    }

    #[test]
    fn signature_copies_fields() {
        let i = Ident {
            name: "n".into(),
            email: "e".into(),
            time_ms: 5,
            tz_offset_minutes: -60,
        };
        let s = signature(&i);
        assert_eq!(
            (s.name.as_str(), s.email.as_str(), s.time_ms, s.tz_offset_minutes),
            ("n", "e", 5, -60)
        );
    }

    #[test]
    fn error_display() {
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        let e = Error::Diverged {
            reference: "main".into(),
            current: id,
        };
        assert!(e.to_string().contains("not in the imported"));
        assert!(Error::NonUtf8Path("p".into()).to_string().contains("UTF-8"));
        assert!(Error::Parse("x".into()).to_string().contains("parse"));
        assert_eq!(Error::Git("boom".into()).to_string(), "boom");
    }
}
