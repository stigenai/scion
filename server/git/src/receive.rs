//! `git-receive-pack` (design §5): ref commands, the pack, git commits turned into Scion commits,
//! and ref updates through the same services as `scn`.

use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::{Bytes, BytesMut};
use scion_auth::Principal;
use scion_object::git::{GitCommit, parse_commit, rebuilds};
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, Tree, validate_name};
use scion_query::{
    GIT_BLOB_ID_KIND, GIT_OBJECT_VERSION, GIT_SHA1_EXTENSION, GIT_TREE_ID_KIND, GITSHA_KIND, GITSHA_VERSION,
};
use scion_store::{BlobStore, ChangesetRecord, MetaStore};

use crate::objects::{GIT_COMMIT_EXTENSION, Objects};
use crate::pkt;
use crate::unpack::{ObjType, Unpacked};
use crate::upload::{Advertised, TAG_PREFIX};
use crate::{Error, Result};

/// What a push asks of a ref, after the gateway has checked the old value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefUpdate {
    /// A fast-forward: lands `new` on the branch, as `scn land` does.
    Land {
        name: String,
        new: ObjectId,
    },
    Create {
        name: String,
        new: ObjectId,
        /// For an annotated tag: its git tag object, as pushed.
        annotation: Option<Vec<u8>>,
    },
    /// A forced update to a commit that does not descend from the ref.
    Move {
        name: String,
        new: ObjectId,
        version: u64,
    },
    Delete {
        name: String,
        version: u64,
    },
}

/// Writes what a push brings, with the pusher's permissions. The server implements it with the
/// services `scn` uses, so the same authorization, pre-land checks and events apply.
#[async_trait::async_trait]
pub trait PushBackend: Send + Sync {
    /// Stores file contents in the repository.
    async fn store(
        &self,
        p: &Principal,
        repo: &str,
        objects: Vec<(ObjectId, Bytes)>,
    ) -> std::result::Result<(), String>;
    /// Creates a commit the gateway has built (it may carry `git.sha1` and `git.commit`).
    async fn create(&self, p: &Principal, repo: &str, cs: &Changeset) -> std::result::Result<ChangesetRecord, String>;
    /// Updates one ref; the error is the reason git prints after `ng`.
    async fn update(&self, p: &Principal, repo: &str, u: RefUpdate) -> std::result::Result<(), String>;
}

pub const ZERO: [u8; 20] = [0; 20];

/// The receive-pack advertisement after the service line.
pub fn advertise(adv: &Advertised, agent: &str) -> BytesMut {
    let caps = format!(
        "report-status report-status-v2 delete-refs side-band-64k quiet ofs-delta no-thin object-format=sha1 agent={agent}"
    );
    let mut out = BytesMut::new();
    if adv.refs.is_empty() {
        pkt::put(
            &mut out,
            format!("{} capabilities^{{}}\0{caps}\n", "0".repeat(40)).as_bytes(),
        );
    }
    for (i, (name, sha)) in adv.refs.iter().enumerate() {
        let line = if i == 0 {
            format!("{} {name}\0{caps}\n", hex::encode(sha))
        } else {
            format!("{} {name}\n", hex::encode(sha))
        };
        pkt::put(&mut out, line.as_bytes());
    }
    out.extend_from_slice(pkt::FLUSH);
    out
}

/// One command of a push: `old new ref`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub old: [u8; 20],
    pub new: [u8; 20],
    pub git_ref: String,
}

/// A parsed push: the commands and the client's capabilities. The pack follows them.
#[derive(Debug, Default)]
pub struct PushRequest {
    pub commands: Vec<Command>,
    pub side_band: bool,
    pub quiet: bool,
}

/// The next pkt-line of a stream, without its newline: `None` for a flush or the end.
fn read_line<R: std::io::Read>(r: &mut R) -> Result<Option<Vec<u8>>> {
    let mut head = [0u8; 4];
    match r.read_exact(&mut head) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(Error::Protocol(format!("reading the push: {e}"))),
    }
    let len = std::str::from_utf8(&head)
        .ok()
        .and_then(|h| usize::from_str_radix(h, 16).ok())
        .ok_or_else(|| {
            Error::Protocol(format!(
                "malformed pkt-line: bad length {:?}",
                String::from_utf8_lossy(&head)
            ))
        })?;
    match len {
        0 => Ok(None),
        1..=3 => Err(Error::Protocol("unexpected packet in commands".into())),
        _ => {
            let mut line = vec![0u8; len - 4];
            r.read_exact(&mut line)
                .map_err(|_| Error::Protocol("malformed pkt-line: truncated packet".into()))?;
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            Ok(Some(line))
        }
    }
}

fn oid(s: &str) -> Result<[u8; 20]> {
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out).map_err(|_| Error::Protocol(format!("bad object id {s:?}")))?;
    Ok(out)
}

/// Reads the commands, leaving `r` at the start of the pack.
pub fn parse_request<R: std::io::Read>(r: &mut R) -> Result<PushRequest> {
    let mut req = PushRequest::default();
    let mut first = true;
    while let Some(l) = read_line(r)? {
        let line = String::from_utf8_lossy(&l);
        if line.starts_with("shallow ") {
            return Err(Error::Unsupported(
                "pushing from a shallow clone is not supported".into(),
            ));
        }
        let (cmd, caps) = line.split_once('\0').unwrap_or((&line, ""));
        if first {
            for c in caps.split_whitespace() {
                match c {
                    "side-band-64k" | "side-band" => req.side_band = true,
                    "quiet" => req.quiet = true,
                    _ => {}
                }
            }
            first = false;
        }
        let mut it = cmd.split(' ');
        let (Some(old), Some(new), Some(git_ref), None) = (it.next(), it.next(), it.next(), it.next()) else {
            return Err(Error::Protocol(format!("bad command {cmd:?}")));
        };
        req.commands.push(Command {
            old: oid(old)?,
            new: oid(new)?,
            git_ref: git_ref.to_string(),
        });
    }
    Ok(req)
}

/// The Scion name of a pushed ref; other git refs cannot be pushed.
pub fn scion_ref_name(git_ref: &str) -> std::result::Result<String, String> {
    if let Some(b) = git_ref.strip_prefix("refs/heads/") {
        if b.starts_with(TAG_PREFIX) {
            return Err("branch names cannot start with tags/".into());
        }
        return Ok(b.to_string());
    }
    if let Some(t) = git_ref.strip_prefix("refs/tags/") {
        return Ok(format!("{TAG_PREFIX}{t}"));
    }
    Err("only refs/heads/* and refs/tags/* can be pushed".into())
}

/// The report git reads: `unpack ok` and `ok REF` or `ng REF reason` per command.
pub fn report(
    unpack: std::result::Result<(), String>,
    results: &[(String, std::result::Result<(), String>)],
) -> BytesMut {
    let mut out = BytesMut::new();
    match unpack {
        Ok(()) => pkt::put_line(&mut out, "unpack ok"),
        Err(e) => pkt::put_line(&mut out, &format!("unpack {}", one_line(&e))),
    }
    for (r, res) in results {
        match res {
            Ok(()) => pkt::put_line(&mut out, &format!("ok {r}")),
            Err(e) => pkt::put_line(&mut out, &format!("ng {r} {}", one_line(e))),
        }
    }
    out.extend_from_slice(pkt::FLUSH);
    out
}

fn one_line(s: &str) -> String {
    s.replace('\n', " ")
}

/// A pushed git tree's entry.
struct GitEntry {
    name: String,
    mode: u32,
    sha: [u8; 20],
}

fn parse_tree(body: &[u8]) -> Result<Vec<GitEntry>> {
    let bad = || Error::Protocol("malformed tree object".into());
    let mut out = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let sp = rest.iter().position(|&b| b == b' ').ok_or_else(bad)?;
        let mode = std::str::from_utf8(&rest[..sp])
            .ok()
            .and_then(|m| u32::from_str_radix(m, 8).ok())
            .ok_or_else(bad)?;
        let nul = rest[sp..].iter().position(|&b| b == 0).ok_or_else(bad)? + sp;
        let name = std::str::from_utf8(&rest[sp + 1..nul])
            .map_err(|_| Error::Unsupported("a path that is not UTF-8 cannot be pushed".into()))?
            .to_string();
        let sha: [u8; 20] = rest
            .get(nul + 1..nul + 21)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(bad)?;
        out.push(GitEntry { name, mode, sha });
        rest = &rest[nul + 21..];
    }
    Ok(out)
}

fn entry_mode(mode: u32, path: &str) -> Result<EntryMode> {
    match mode {
        0o100644 => Ok(EntryMode::File),
        0o100755 => Ok(EntryMode::Executable),
        0o120000 => Ok(EntryMode::Symlink),
        0o160000 => Err(Error::Unsupported(format!(
            "path {path} is a submodule; Scion repositories cannot hold submodules"
        ))),
        m => Err(Error::Unsupported(format!(
            "path {path} has mode {m:o}, which Scion cannot store"
        ))),
    }
}

/// Files are stored in batches of about this many bytes while a commit is converted.
const STORE_BATCH: usize = 16 << 20;

/// Who a push's files are stored for, and where.
struct Sink<'a> {
    p: &'a Principal,
    backend: &'a dyn PushBackend,
}

/// Turns the pushed commits into Scion commits, parents first.
pub struct Converter<'a> {
    pub meta: &'a dyn MetaStore,
    pub blobs: &'a dyn BlobStore,
    pub repo: &'a str,
    pub pack: &'a Unpacked,
    objects: Objects<'a>,
    /// git commit SHA → Scion commit, for known commits and the ones made so far.
    commits: HashMap<[u8; 20], ObjectId>,
    /// Pack files to store before the commit that needs them.
    pending_files: Vec<(ObjectId, Bytes)>,
    pending_bytes: usize,
    stored: HashSet<ObjectId>,
    /// Commits whose trees were indexed by git SHA, when a lookup missed.
    indexed: HashSet<ObjectId>,
}

impl<'a> Converter<'a> {
    pub fn new(meta: &'a dyn MetaStore, blobs: &'a dyn BlobStore, repo: &'a str, pack: &'a Unpacked) -> Self {
        Self {
            meta,
            blobs,
            repo,
            pack,
            objects: Objects::new(meta, blobs, repo),
            commits: HashMap::new(),
            pending_files: Vec::new(),
            pending_bytes: 0,
            stored: HashSet::new(),
            indexed: HashSet::new(),
        }
    }

    /// The Scion commit of a git SHA the repository has.
    pub async fn known(&mut self, sha: &[u8; 20]) -> Result<Option<ObjectId>> {
        if let Some(id) = self.commits.get(sha) {
            return Ok(Some(*id));
        }
        let id = self
            .meta
            .get_derived(self.repo, GITSHA_KIND, GITSHA_VERSION, sha)
            .await?
            .and_then(|v| ObjectId::from_bytes(&v).ok());
        if let Some(id) = id {
            self.commits.insert(*sha, id);
        }
        Ok(id)
    }

    /// The pack's commits that `tip` needs, parents first. A parent outside the pack must be known.
    async fn order(&mut self, tip: [u8; 20]) -> Result<Vec<[u8; 20]>> {
        let mut out = Vec::new();
        let mut done: HashSet<[u8; 20]> = HashSet::new();
        // (commit, its parents pushed already)
        let mut stack: Vec<([u8; 20], bool)> = vec![(tip, false)];
        while let Some((sha, expanded)) = stack.pop() {
            if done.contains(&sha) {
                continue;
            }
            if expanded {
                done.insert(sha);
                out.push(sha);
                continue;
            }
            if self.known(&sha).await?.is_some() {
                done.insert(sha);
                continue;
            }
            let c = self.git_commit(&sha)?;
            stack.push((sha, true));
            for p in c.parents.iter().rev() {
                if !done.contains(p) {
                    stack.push((*p, false));
                }
            }
        }
        Ok(out)
    }

    fn git_commit(&self, sha: &[u8; 20]) -> Result<GitCommit> {
        match self.pack.get(sha)? {
            Some((ObjType::Commit, raw)) => {
                parse_commit(&raw).ok_or_else(|| Error::Protocol(format!("malformed commit {}", hex::encode(sha))))
            }
            Some((ObjType::Tag, _)) => Err(Error::Unsupported(format!(
                "{} is a tag, not a commit: tags of tags are not supported",
                hex::encode(sha)
            ))),
            Some(_) => Err(Error::Protocol(format!("{} is not a commit", hex::encode(sha)))),
            None => Err(Error::Protocol(format!(
                "commit {} is neither in the pack nor in the repository",
                hex::encode(sha)
            ))),
        }
    }

    /// When `sha` is a tag object in the pack: the commit it names and the object itself. Tags
    /// of anything but a commit are refused.
    pub fn pushed_tag(&self, sha: &[u8; 20]) -> Result<Option<([u8; 20], Vec<u8>)>> {
        let Some((ObjType::Tag, raw)) = self.pack.get(sha)? else {
            return Ok(None);
        };
        let tag = scion_object::git::parse_tag(&raw)
            .ok_or_else(|| Error::Protocol(format!("malformed tag {}", hex::encode(sha))))?;
        if tag.kind != "commit" {
            return Err(Error::Unsupported(format!(
                "tag {} names a {}: only tags of commits are supported",
                tag.name, tag.kind
            )));
        }
        Ok(Some((tag.object, raw.to_vec())))
    }

    /// Makes the Scion commits for `tip` and returns its Scion commit.
    pub async fn convert(&mut self, p: &Principal, backend: &dyn PushBackend, tip: [u8; 20]) -> Result<ObjectId> {
        if let Some(id) = self.known(&tip).await? {
            return Ok(id);
        }
        for sha in self.order(tip).await? {
            let raw = match self.pack.get(&sha)? {
                Some((_, raw)) => raw,
                None => return Err(Error::Internal("ordered commit missing".into())),
            };
            let c = self.git_commit(&sha)?;
            let mut parents = Vec::with_capacity(c.parents.len());
            for ps in &c.parents {
                let id = self
                    .known(ps)
                    .await?
                    .ok_or_else(|| Error::Internal(format!("parent {} not converted", hex::encode(ps))))?;
                parents.push(id);
            }
            let base = match parents.first() {
                Some(id) => Some(self.record(*id).await?),
                None => None,
            };
            let mut delta = Vec::new();
            let root = base.as_ref().map(|r| r.root_tree);
            let sink = Sink { p, backend };
            self.diff(&sink, c.tree, root, "", base.as_ref().map(|r| r.id), &mut delta)
                .await?;
            self.flush(&sink).await?;
            let mut ext = BTreeMap::from([(GIT_SHA1_EXTENSION.to_string(), sha.to_vec())]);
            if !rebuilds(&raw, &c) {
                ext.insert(GIT_COMMIT_EXTENSION.to_string(), raw.to_vec());
            }
            let change_id: [u8; 16] = sha[..16].try_into().expect("16 of 20 bytes");
            let cs = Changeset::new(parents, c.author, c.committer, c.message, change_id, delta, ext)
                .map_err(|e| Error::Unsupported(format!("commit {}: {e}", hex::encode(sha))))?;
            let rec = backend
                .create(p, self.repo, &cs)
                .await
                .map_err(|e| Error::Unservable(format!("commit {}: {e}", hex::encode(sha))))?;
            // The SHA git sees must be the SHA of what Scion stores
            let tree = self.objects.tree_sha(rec.root_tree).await?;
            if tree != c.tree {
                return Err(Error::Unsupported(format!(
                    "commit {}: its tree cannot be stored exactly (an unusual entry order or mode)",
                    hex::encode(sha)
                )));
            }
            self.commits.insert(sha, rec.id);
        }
        self.known(&tip)
            .await?
            .ok_or_else(|| Error::Internal("tip not converted".into()))
    }

    async fn record(&self, id: ObjectId) -> Result<ChangesetRecord> {
        self.meta
            .get_changeset(self.repo, &id)
            .await?
            .ok_or_else(|| Error::Internal(format!("commit {id} has no record")))
    }

    async fn load(&self, id: ObjectId) -> Result<Tree> {
        let bytes = self
            .blobs
            .get(&id)
            .await?
            .ok_or_else(|| Error::Internal(format!("tree {id} is missing")))?;
        Tree::decode(&bytes).map_err(|e| Error::Internal(format!("tree {id}: {e}")))
    }

    /// A git tree's entries: from the pack, or rebuilt from a Scion directory with that SHA.
    async fn git_tree(&mut self, sha: [u8; 20], base_commit: Option<ObjectId>) -> Result<Vec<GitEntry>> {
        if let Some((t, body)) = self.pack.get(&sha)? {
            if t != ObjType::Tree {
                return Err(Error::Protocol(format!("{} is not a tree", hex::encode(sha))));
            }
            return parse_tree(&body);
        }
        let id = self.lookup(GIT_TREE_ID_KIND, sha, base_commit).await?;
        let tree = self.load(id).await?;
        let body = self.objects.tree(id, &tree).await?;
        parse_tree(&body)
    }

    /// Stores the pack files gathered so far.
    async fn flush(&mut self, sink: &Sink<'_>) -> Result<()> {
        let files = std::mem::take(&mut self.pending_files);
        self.pending_bytes = 0;
        if files.is_empty() {
            return Ok(());
        }
        sink.backend
            .store(sink.p, self.repo, files)
            .await
            .map_err(Error::Unservable)
    }

    /// A file by git SHA: from the pack (to be stored), or one the repository has.
    async fn file(&mut self, sink: &Sink<'_>, sha: [u8; 20], base_commit: Option<ObjectId>) -> Result<(ObjectId, u64)> {
        if let Some((t, content)) = self.pack.get(&sha)? {
            if t != ObjType::Blob {
                return Err(Error::Protocol(format!("{} is not a blob", hex::encode(sha))));
            }
            let id = ObjectId::of_blob(&content);
            let len = content.len() as u64;
            if self.stored.insert(id) {
                self.pending_bytes += content.len();
                self.pending_files.push((id, content));
                if self.pending_bytes >= STORE_BATCH {
                    self.flush(sink).await?;
                }
            }
            return Ok((id, len));
        }
        let id = self.lookup(GIT_BLOB_ID_KIND, sha, base_commit).await?;
        let size = self
            .meta
            .object_sizes(self.repo, &[id])
            .await?
            .get(&id)
            .copied()
            .ok_or_else(|| Error::Internal(format!("file {id} has no size")))?;
        Ok((id, size))
    }

    /// An object the push did not send because the repository has it. Its git SHA maps back
    /// through derived data; when that was derived before the reverse mapping existed, the base
    /// commit's tree is indexed once and the lookup retried.
    async fn lookup(&mut self, kind: &str, sha: [u8; 20], base_commit: Option<ObjectId>) -> Result<ObjectId> {
        for attempt in 0..2 {
            let found = self
                .meta
                .get_derived(self.repo, kind, GIT_OBJECT_VERSION, &sha)
                .await?
                .and_then(|v| ObjectId::from_bytes(&v).ok());
            if let Some(id) = found {
                return Ok(id);
            }
            match base_commit {
                Some(c) if attempt == 0 && self.indexed.insert(c) => {
                    let root = self.record(c).await?.root_tree;
                    self.objects.index_reverse(root).await?;
                }
                _ => break,
            }
        }
        Err(Error::Protocol(format!(
            "object {} is neither in the pack nor in the repository",
            hex::encode(sha)
        )))
    }

    /// The changes from the Scion directory `base` to the git tree `git`, at `prefix`.
    async fn diff(
        &mut self,
        sink: &Sink<'_>,
        git: [u8; 20],
        base: Option<ObjectId>,
        prefix: &str,
        base_commit: Option<ObjectId>,
        out: &mut Vec<FileChange>,
    ) -> Result<()> {
        // An explicit stack of (git tree, Scion directory, path prefix)
        let mut stack = vec![(git, base, prefix.to_string())];
        while let Some((git, base, prefix)) = stack.pop() {
            let (base_entries, base_shas) = match base {
                Some(b) => {
                    if self.objects.tree_sha(b).await? == git {
                        continue;
                    }
                    let t = self.load(b).await?;
                    let shas = self.objects.entry_git_shas(b, &t).await?;
                    (t.entries().to_vec(), shas)
                }
                None => (Vec::new(), Vec::new()),
            };
            let old: HashMap<&str, (usize, &scion_object::TreeEntry)> = base_entries
                .iter()
                .enumerate()
                .map(|(i, e)| (e.name.as_str(), (i, e)))
                .collect();
            let entries = self.git_tree(git, base_commit).await?;
            let mut names = HashSet::new();
            for e in &entries {
                let path = format!("{prefix}{}", e.name);
                validate_name(&e.name).map_err(|err| Error::Unsupported(format!("path {path:?}: {err}")))?;
                names.insert(e.name.as_str());
                let was = old.get(e.name.as_str()).copied();
                let was_dir = was.is_some_and(|(_, w)| w.mode == EntryMode::Tree);
                if e.mode == 0o40000 {
                    if let Some((_, w)) = was.filter(|_| !was_dir) {
                        let _ = w;
                        out.push(FileChange::delete(&path));
                    }
                    stack.push((e.sha, was.filter(|_| was_dir).map(|(_, w)| w.id), format!("{path}/")));
                    continue;
                }
                let mode = entry_mode(e.mode, &path)?;
                if let Some((i, w)) = was {
                    if !was_dir && w.mode == mode && base_shas[i] == e.sha {
                        continue;
                    }
                    if was_dir {
                        self.delete_all(w.id, &format!("{path}/"), out).await?;
                    }
                }
                let (id, size) = self.file(sink, e.sha, base_commit).await?;
                out.push(FileChange::put(&path, mode, id, size));
            }
            for e in &base_entries {
                if names.contains(e.name.as_str()) {
                    continue;
                }
                let path = format!("{prefix}{}", e.name);
                if e.mode == EntryMode::Tree {
                    self.delete_all(e.id, &format!("{path}/"), out).await?;
                } else {
                    out.push(FileChange::delete(&path));
                }
            }
        }
        Ok(())
    }

    /// Deletes every file under a directory (Scion's directories exist only through their files).
    async fn delete_all(&self, dir: ObjectId, prefix: &str, out: &mut Vec<FileChange>) -> Result<()> {
        let mut stack = vec![(dir, prefix.to_string())];
        while let Some((id, prefix)) = stack.pop() {
            for e in self.load(id).await?.entries() {
                let path = format!("{prefix}{}", e.name);
                if e.mode == EntryMode::Tree {
                    stack.push((e.id, format!("{path}/")));
                } else {
                    out.push(FileChange::delete(&path));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_push_requests() {
        let mut body = BytesMut::new();
        pkt::put(
            &mut body,
            format!(
                "{} {} refs/heads/main\0report-status side-band-64k quiet agent=git/2\n",
                "00".repeat(20),
                "ab".repeat(20)
            )
            .as_bytes(),
        );
        pkt::put_line(
            &mut body,
            &format!("{} {} refs/tags/v1", "cd".repeat(20), "00".repeat(20)),
        );
        body.extend_from_slice(pkt::FLUSH);
        body.extend_from_slice(b"PACK...");
        let mut rest = &body[..];
        let r = parse_request(&mut rest).expect("parse");
        assert_eq!(r.commands.len(), 2);
        assert_eq!(r.commands[0].new, [0xab; 20]);
        assert_eq!(r.commands[1].git_ref, "refs/tags/v1");
        assert!(r.side_band && r.quiet);
        assert_eq!(rest, b"PACK...");
        for bad in ["x y", &format!("{} zz refs/heads/x", "00".repeat(20))] {
            let mut b = BytesMut::new();
            pkt::put_line(&mut b, bad);
            assert!(parse_request(&mut &b[..]).is_err(), "{bad}");
        }
        let mut b = BytesMut::new();
        pkt::put_line(&mut b, &format!("shallow {}", "00".repeat(20)));
        assert!(matches!(parse_request(&mut &b[..]), Err(Error::Unsupported(_))));
        assert!(parse_request(&mut &b"0001"[..]).is_err());
        assert!(parse_request(&mut &b"zzzz"[..]).is_err());
        assert!(parse_request(&mut &b"0010abc"[..]).is_err());
    }

    #[test]
    fn maps_ref_names_and_reports() {
        assert_eq!(scion_ref_name("refs/heads/main"), Ok("main".into()));
        assert_eq!(scion_ref_name("refs/tags/v1"), Ok("tags/v1".into()));
        assert!(scion_ref_name("refs/heads/tags/x").is_err());
        assert!(scion_ref_name("refs/notes/x").is_err());
        let r = report(
            Ok(()),
            &[
                ("refs/heads/a".into(), Ok(())),
                ("refs/heads/b".into(), Err("non-fast-forward\nx".into())),
            ],
        );
        let s = String::from_utf8_lossy(&r).into_owned();
        assert!(s.contains("unpack ok\n") && s.contains("ok refs/heads/a\n"), "{s}");
        assert!(s.contains("ng refs/heads/b non-fast-forward x\n"), "{s}");
        let r = report(Err("bad pack".into()), &[]);
        assert!(String::from_utf8_lossy(&r).contains("unpack bad pack"));
    }

    #[test]
    fn parses_trees_and_modes() {
        let mut body = Vec::new();
        for (mode, name, b) in [("100644", "a.txt", 1u8), ("40000", "dir", 2), ("160000", "sub", 3)] {
            body.extend_from_slice(format!("{mode} {name}\0").as_bytes());
            body.extend_from_slice(&[b; 20]);
        }
        let entries = parse_tree(&body).expect("tree");
        assert_eq!(entries.len(), 3);
        assert_eq!((entries[1].name.as_str(), entries[1].mode), ("dir", 0o40000));
        assert_eq!(entry_mode(0o100755, "x"), Ok(EntryMode::Executable));
        assert_eq!(entry_mode(0o120000, "x"), Ok(EntryMode::Symlink));
        assert!(matches!(entry_mode(0o160000, "sub"), Err(Error::Unsupported(m)) if m.contains("submodule")));
        assert!(matches!(entry_mode(0o100664, "old"), Err(Error::Unsupported(m)) if m.contains("100664")));
        for bad in [&b"100644 a"[..], b"x a\0", b"100644 a\0short"] {
            assert!(parse_tree(bad).is_err(), "{bad:?}");
        }
        assert!(matches!(
            parse_tree(b"100644 \xff\0aaaaaaaaaaaaaaaaaaaa"),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn advertises_refs_for_push() {
        let adv = Advertised {
            refs: vec![("refs/heads/main".into(), [1; 20])],
            head: None,
            peeled: Default::default(),
        };
        let s = String::from_utf8_lossy(&advertise(&adv, "scion/1")).into_owned();
        assert!(
            s.contains("refs/heads/main\0report-status") && s.contains("no-thin"),
            "{s}"
        );
        let empty = Advertised {
            refs: vec![],
            head: None,
            peeled: Default::default(),
        };
        let s = String::from_utf8_lossy(&advertise(&empty, "scion/1")).into_owned();
        assert!(s.contains("capabilities^{}"), "{s}");
    }
}
