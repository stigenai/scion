//! `git-upload-pack` (design §4): ref advertisement, negotiation, choosing objects and the pack.

use std::collections::{BinaryHeap, HashMap, HashSet};

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use scion_object::{EntryMode, ObjectId, Tree, TreeEntry};
use scion_query::{GITSHA_KIND, GITSHA_VERSION};
use scion_store::{BlobStore, ChangesetRecord, MetaStore, RefState};
use tokio::sync::mpsc;

use crate::objects::{Objects, check_blob};
use crate::pack::{Kind, PackWriter};
use crate::pkt::{self, MAX_BAND_DATA, Pkt, Reader};
use crate::{Error, Result};

/// The tag prefix of Scion ref names.
pub const TAG_PREFIX: &str = "tags/";
/// How many haves without a common commit end the negotiation: the client is then sent
/// everything its wants need.
const GIVE_UP_HAVES: usize = 256;
/// How many file contents are read at once while writing a pack.
const READ_AHEAD: usize = 16;
/// How many bytes of compressed commits a fetch builds before it answers: enough for every
/// commit of most repositories, at a few hundred bytes each.
pub const COMMIT_BUFFER: usize = 16 << 20;

/// The git name of a Scion ref.
pub fn git_ref_name(name: &str) -> String {
    match name.strip_prefix(TAG_PREFIX) {
        Some(tag) => format!("refs/tags/{tag}"),
        None => format!("refs/heads/{name}"),
    }
}

/// The refs to advertise: sorted by git name, and the default branch if it exists.
pub struct Advertised {
    pub refs: Vec<(String, [u8; 20])>,
    /// `HEAD`: the default branch's git name and SHA.
    pub head: Option<(String, [u8; 20])>,
    /// Annotated tags (by git name): the commit their tag object names, git's `^{}`.
    pub peeled: HashMap<String, [u8; 20]>,
}

/// An annotated tag as git sees it: a tag object naming a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotatedTag {
    pub sha: [u8; 20],
    pub body: Bytes,
    pub commit: ObjectId,
    pub commit_sha: [u8; 20],
}

/// The repository's annotated tags by Scion ref name, those still on the commit they were
/// annotated on: the git object they were pushed or imported as, else one made from what the
/// tag says.
pub async fn annotated_tags(
    objects: &mut Objects<'_>,
    refs: &[(String, RefState)],
) -> Result<HashMap<String, AnnotatedTag>> {
    let mut out = HashMap::new();
    if !refs.iter().any(|(n, _)| n.starts_with(TAG_PREFIX)) {
        return Ok(out);
    }
    let notes = objects.meta.tag_annotations(objects.repo).await?;
    for (name, state) in refs {
        let (Some(note), Some(tag)) = (notes.get(name), name.strip_prefix(TAG_PREFIX)) else {
            continue;
        };
        if note.target != state.target {
            continue;
        }
        let commit_sha = objects.commit_sha(state.target).await?;
        let body = match &note.git_object {
            Some(raw) => Bytes::from(raw.clone()),
            None => Bytes::from(scion_object::git::tag_body(
                &commit_sha,
                tag,
                &note.tagger,
                &note.message,
            )),
        };
        let sha = scion_query::git_hash("tag", &body);
        out.insert(
            name.clone(),
            AnnotatedTag {
                sha,
                body,
                commit: state.target,
                commit_sha,
            },
        );
    }
    Ok(out)
}

pub async fn advertised(meta: &dyn MetaStore, blobs: &dyn BlobStore, repo: &str, default: &str) -> Result<Advertised> {
    let refs = meta.list_refs(repo).await?;
    let mut objects = Objects::new(meta, blobs, repo);
    let targets: Vec<ObjectId> = refs.iter().map(|(_, s)| s.target).collect();
    objects.prefetch(&targets, &[]).await?;
    let tags = annotated_tags(&mut objects, &refs).await?;
    let mut out = Vec::with_capacity(refs.len());
    let mut head = None;
    let mut peeled = HashMap::new();
    for (name, state) in &refs {
        let sha = objects.commit_sha(state.target).await?;
        let git = git_ref_name(name);
        if name == default {
            head = Some((git.clone(), sha));
        }
        match tags.get(name) {
            Some(t) => {
                peeled.insert(git.clone(), sha);
                out.push((git, t.sha));
            }
            None => out.push((git, sha)),
        }
    }
    out.sort();
    Ok(Advertised {
        refs: out,
        head,
        peeled,
    })
}

/// The v0 advertisement after the service line: refs with capabilities on the first line.
pub fn advertise_v0(adv: &Advertised, agent: &str) -> BytesMut {
    let mut caps = String::from(
        "multi_ack_detailed no-done side-band-64k thin-pack ofs-delta shallow deepen-since filter allow-tip-sha1-in-want allow-reachable-sha1-in-want object-format=sha1",
    );
    if let Some((target, _)) = &adv.head {
        caps.push_str(&format!(" symref=HEAD:{target}"));
    }
    caps.push_str(&format!(" agent={agent}"));
    let mut lines: Vec<(String, [u8; 20])> = Vec::new();
    if let Some((_, sha)) = &adv.head {
        lines.push(("HEAD".into(), *sha));
    }
    for (name, sha) in &adv.refs {
        lines.push((name.clone(), *sha));
        if let Some(commit) = adv.peeled.get(name) {
            lines.push((format!("{name}^{{}}"), *commit));
        }
    }
    let mut out = BytesMut::new();
    if lines.is_empty() {
        pkt::put(
            &mut out,
            format!("{} capabilities^{{}}\0{caps}\n", "0".repeat(40)).as_bytes(),
        );
    }
    for (i, (name, sha)) in lines.iter().enumerate() {
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

/// The v2 capability advertisement.
pub fn advertise_v2(agent: &str) -> BytesMut {
    let mut out = BytesMut::new();
    for line in [
        "version 2",
        &format!("agent={agent}"),
        "ls-refs",
        "fetch=shallow filter",
        "object-format=sha1",
    ] {
        pkt::put_line(&mut out, line);
    }
    out.extend_from_slice(pkt::FLUSH);
    out
}

/// One v2 request: the command and its arguments.
pub struct V2Request<'a> {
    pub command: String,
    pub args: Vec<&'a [u8]>,
}

pub fn parse_v2(body: &[u8]) -> Result<V2Request<'_>> {
    let mut r = Reader::new(body);
    let mut command = None;
    // Capabilities until the delimiter, then arguments until the flush
    loop {
        match r.read()? {
            Some(Pkt::Data(l)) => {
                if let Some(c) = l.strip_prefix(b"command=") {
                    command = Some(String::from_utf8_lossy(c).into_owned());
                }
            }
            Some(Pkt::Delim) => break,
            Some(Pkt::Flush) | None => {
                return Ok(V2Request {
                    command: command.unwrap_or_default(),
                    args: Vec::new(),
                });
            }
            Some(Pkt::End) => return Err(Error::Protocol("unexpected response-end packet".into())),
        }
    }
    let mut args = Vec::new();
    while let Some(p) = r.read()? {
        match p {
            Pkt::Data(l) => args.push(l),
            Pkt::Flush => break,
            Pkt::Delim | Pkt::End => return Err(Error::Protocol("unexpected packet in arguments".into())),
        }
    }
    Ok(V2Request {
        command: command.ok_or_else(|| Error::Protocol("no command".into()))?,
        args,
    })
}

/// `ls-refs`: the refs matching the prefixes, with `HEAD`'s target when asked for.
pub fn ls_refs(adv: &Advertised, args: &[&[u8]]) -> BytesMut {
    let symrefs = args.contains(&&b"symrefs"[..]);
    let peel = args.contains(&&b"peel"[..]);
    let prefixes: Vec<&[u8]> = args.iter().filter_map(|a| a.strip_prefix(b"ref-prefix ")).collect();
    let wanted = |name: &str| prefixes.is_empty() || prefixes.iter().any(|p| name.as_bytes().starts_with(p));
    let mut out = BytesMut::new();
    if let Some((target, sha)) = adv.head.as_ref().filter(|_| wanted("HEAD")) {
        let mut line = format!("{} HEAD", hex::encode(sha));
        if symrefs {
            line.push_str(&format!(" symref-target:{target}"));
        }
        pkt::put_line(&mut out, &line);
    }
    for (name, sha) in &adv.refs {
        if wanted(name) {
            let mut line = format!("{} {name}", hex::encode(sha));
            if let Some(commit) = adv.peeled.get(name).filter(|_| peel) {
                line.push_str(&format!(" peeled:{}", hex::encode(commit)));
            }
            pkt::put_line(&mut out, &line);
        }
    }
    out.extend_from_slice(pkt::FLUSH);
    out
}

/// What a fetch asks for, from either protocol version.
#[derive(Debug, Default)]
pub struct FetchRequest {
    pub wants: Vec<[u8; 20]>,
    pub haves: Vec<[u8; 20]>,
    pub done: bool,
    pub side_band: bool,
    pub no_progress: bool,
    /// v0: `multi_ack_detailed` and `no-done`.
    pub multi_ack: bool,
    pub no_done: bool,
    /// The client's shallow commits: it has them, but not their parents.
    pub shallow: Vec<[u8; 20]>,
    pub deepen: Option<Deepen>,
    /// v0: the request ended with its wants, before any haves. A shallow client asks this
    /// first, and expects only the shallow update back.
    pub wants_only: bool,
    /// Deltas may name objects the client has (`thin-pack`), and earlier entries by offset.
    pub thin_pack: bool,
    pub ofs_delta: bool,
    /// A partial clone's filter: files left out, fetched later by SHA.
    pub filter: Filter,
}

/// Which files a partial clone leaves out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Filter {
    #[default]
    None,
    /// `blob:none`: every file.
    BlobNone,
    /// `blob:limit=N`: files of more than N bytes.
    BlobLimit(u64),
}

impl Filter {
    fn parse(spec: &[u8]) -> Result<Filter> {
        let spec = String::from_utf8_lossy(spec);
        if spec == "blob:none" {
            return Ok(Filter::BlobNone);
        }
        if let Some(n) = spec.strip_prefix("blob:limit=") {
            let (digits, unit) = match n.char_indices().last() {
                Some((i, c)) if c.is_ascii_alphabetic() => (&n[..i], c.to_ascii_lowercase()),
                _ => (n, ' '),
            };
            let mult: u64 = match unit {
                ' ' => 1,
                'k' => 1 << 10,
                'm' => 1 << 20,
                'g' => 1 << 30,
                _ => 0,
            };
            if let (Ok(v), true) = (digits.parse::<u64>(), mult > 0) {
                return Ok(Filter::BlobLimit(v.saturating_mul(mult)));
            }
        }
        Err(Error::Unsupported(format!(
            "filter {spec:?} is not supported; use blob:none or blob:limit=<n>"
        )))
    }

    /// Whether a file of this size is sent.
    pub fn keeps(self, size: u64) -> bool {
        match self {
            Filter::None => true,
            Filter::BlobNone => false,
            Filter::BlobLimit(n) => size <= n,
        }
    }
}

/// How deep a shallow fetch goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deepen {
    /// `--depth N`: the wants and N - 1 generations of parents.
    Depth(u32),
    /// `--shallow-since`: the commits committed at or after this time (seconds).
    Since(i64),
}

fn oid(s: &[u8]) -> Result<[u8; 20]> {
    let s = s.get(..40).ok_or_else(|| Error::Protocol("short object id".into()))?;
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out).map_err(|_| Error::Protocol("bad object id".into()))?;
    Ok(out)
}

/// Refuses what this gateway does not do yet: deepening by excluded refs or relative to the
/// current boundary.
fn unsupported(line: &[u8]) -> Option<&'static str> {
    if line.starts_with(b"deepen-not ") {
        Some("--shallow-exclude is not supported; use --depth or --shallow-since")
    } else if line == b"deepen-relative" {
        Some("git fetch --deepen is not supported; use --depth or --unshallow")
    } else {
        None
    }
}

/// `shallow`, `deepen` and `deepen-since` lines, in either protocol version.
fn shallow_arg(req: &mut FetchRequest, line: &[u8]) -> Result<bool> {
    let number = |v: &[u8]| -> Result<i64> {
        std::str::from_utf8(v)
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .ok_or_else(|| Error::Protocol(format!("bad deepen {:?}", String::from_utf8_lossy(v))))
    };
    if let Some(f) = line.strip_prefix(b"filter ") {
        req.filter = Filter::parse(f)?;
    } else if let Some(s) = line.strip_prefix(b"shallow ") {
        req.shallow.push(oid(s)?);
    } else if let Some(n) = line.strip_prefix(b"deepen-since ") {
        req.deepen = Some(Deepen::Since(number(n)?));
    } else if let Some(n) = line.strip_prefix(b"deepen ") {
        let n = u32::try_from(number(n)?.max(1)).unwrap_or(u32::MAX);
        req.deepen = Some(Deepen::Depth(n));
    } else {
        return Ok(false);
    }
    Ok(true)
}

/// The arguments of a v2 `fetch`.
pub fn parse_fetch_v2(args: &[&[u8]]) -> Result<FetchRequest> {
    let mut req = FetchRequest {
        side_band: true,
        ..Default::default()
    };
    for a in args {
        if let Some(m) = unsupported(a) {
            return Err(Error::Unsupported(m.into()));
        }
        if shallow_arg(&mut req, a)? {
            continue;
        }
        if let Some(w) = a.strip_prefix(b"want ") {
            req.wants.push(oid(w)?);
        } else if let Some(h) = a.strip_prefix(b"have ") {
            req.haves.push(oid(h)?);
        } else if *a == b"done" {
            req.done = true;
        } else if *a == b"no-progress" {
            req.no_progress = true;
        } else if *a == b"thin-pack" {
            req.thin_pack = true;
        } else if *a == b"ofs-delta" {
            req.ofs_delta = true;
        }
    }
    Ok(req)
}

/// A v0 request: wants (the first with capabilities), a flush, haves, and `done` or a flush.
pub fn parse_fetch_v0(body: &[u8]) -> Result<FetchRequest> {
    let mut req = FetchRequest {
        wants_only: true,
        ..Default::default()
    };
    let mut r = Reader::new(body);
    let mut first = true;
    let mut flushes = 0;
    while let Some(p) = r.read()? {
        if flushes > 0 {
            req.wants_only = false;
        }
        let Pkt::Data(l) = p else {
            flushes += 1;
            continue;
        };
        if let Some(m) = unsupported(l) {
            return Err(Error::Unsupported(m.into()));
        }
        if shallow_arg(&mut req, l)? {
            continue;
        }
        if let Some(w) = l.strip_prefix(b"want ") {
            req.wants.push(oid(w)?);
            if first {
                let caps = String::from_utf8_lossy(w.get(40..).unwrap_or_default()).into_owned();
                for c in caps.split_whitespace() {
                    match c {
                        "side-band-64k" | "side-band" => req.side_band = true,
                        "no-progress" => req.no_progress = true,
                        "thin-pack" => req.thin_pack = true,
                        "ofs-delta" => req.ofs_delta = true,
                        "multi_ack_detailed" => req.multi_ack = true,
                        "no-done" => req.no_done = true,
                        _ => {}
                    }
                }
                first = false;
            }
        } else if let Some(h) = l.strip_prefix(b"have ") {
            req.haves.push(oid(h)?);
        } else if l == b"done" {
            req.done = true;
        }
    }
    Ok(req)
}

/// The haves that name commits of this repository, in request order.
pub async fn common(meta: &dyn MetaStore, repo: &str, haves: &[[u8; 20]]) -> Result<Vec<([u8; 20], ObjectId)>> {
    let mut out = Vec::new();
    for chunk in haves.chunks(1000) {
        let keys: Vec<Vec<u8>> = chunk.iter().map(|h| h.to_vec()).collect();
        let found = meta.get_derived_many(repo, GITSHA_KIND, GITSHA_VERSION, &keys).await?;
        for (sha, v) in chunk.iter().zip(found) {
            if let Some(id) = v.and_then(|v| ObjectId::from_bytes(&v).ok()) {
                out.push((*sha, id));
            }
        }
    }
    Ok(out)
}

/// Whether to stop negotiating and send the pack: a common commit was found, or the client has
/// nothing, or it has sent so many haves without one that it is unlikely to share history.
pub fn ready(req: &FetchRequest, common: usize) -> bool {
    common > 0 || req.haves.is_empty() || req.haves.len() >= GIVE_UP_HAVES
}

/// The wants' Scion commits; a want this repository does not have is an error.
/// What a fetch wants: commits, and (for a partial clone filling in) directories and files.
#[derive(Debug, Default, Clone)]
pub struct Wants {
    pub commits: Vec<ObjectId>,
    pub trees: Vec<ObjectId>,
    pub files: Vec<ObjectId>,
    /// Annotated tags' objects; their commits are in `commits`.
    pub tags: Vec<Bytes>,
}

async fn reverse(meta: &dyn MetaStore, repo: &str, kind: &str, sha: &[u8; 20]) -> Result<Option<ObjectId>> {
    Ok(meta
        .get_derived(repo, kind, scion_query::GIT_OBJECT_VERSION, sha)
        .await?
        .and_then(|v| ObjectId::from_bytes(&v).ok()))
}

/// The wants' Scion objects; a want this repository does not have is an error. A tree or file
/// want (a partial clone fetching what it left out) is found through the reverse git SHA
/// mappings, written for the refs' trees first when data derived before them misses it.
pub async fn resolve_wants(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    wants: &[[u8; 20]],
) -> Result<Wants> {
    let found: HashMap<[u8; 20], ObjectId> = common(meta, repo, wants).await?.into_iter().collect();
    let mut out = Wants::default();
    let mut indexed = false;
    let mut tags: Option<HashMap<[u8; 20], AnnotatedTag>> = None;
    for w in wants {
        if let Some(id) = found.get(w) {
            out.commits.push(*id);
            continue;
        }
        if tags.is_none() {
            let refs = meta.list_refs(repo).await?;
            let mut objects = Objects::new(meta, blobs, repo);
            let by_name = annotated_tags(&mut objects, &refs).await?;
            tags = Some(by_name.into_values().map(|t| (t.sha, t)).collect());
        }
        if let Some(t) = tags.as_ref().and_then(|t| t.get(w)) {
            out.tags.push(t.body.clone());
            out.commits.push(t.commit);
            continue;
        }
        loop {
            if let Some(id) = reverse(meta, repo, scion_query::GIT_TREE_ID_KIND, w).await? {
                out.trees.push(id);
                break;
            }
            if let Some(id) = reverse(meta, repo, scion_query::GIT_BLOB_ID_KIND, w).await? {
                out.files.push(id);
                break;
            }
            if indexed {
                return Err(Error::Protocol(format!("not our ref {}", hex::encode(w))));
            }
            let mut objects = Objects::new(meta, blobs, repo);
            for (_, state) in meta.list_refs(repo).await? {
                let root = record(meta, repo, state.target).await?.root_tree;
                objects.index_reverse(root).await?;
            }
            indexed = true;
        }
    }
    Ok(out)
}

/// What a pack holds: commits newest first, then trees, then files. Each tree and file comes
/// with the object at the same path in a parent, a base to send it as a delta against.
#[derive(Debug, Default)]
pub struct Plan {
    pub commits: Vec<ChangesetRecord>,
    pub trees: Vec<(ObjectId, Option<ObjectId>)>,
    pub files: Vec<(ObjectId, Option<ObjectId>)>,
    /// Annotated tags' objects, written after the commits.
    pub tags: Vec<Bytes>,
}

impl Plan {
    pub fn len(&self) -> usize {
        self.commits.len() + self.trees.len() + self.files.len() + self.tags.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

const WANTED: u8 = 1;
const HAVE: u8 = 2;

/// The commits reachable from `wants` and not from `haves`, newest first, taking the commits in
/// `cut` as having no parents (the shallow boundary). A walk in generation order: a commit is
/// popped only after every commit of a higher generation, so by then any path from a have has
/// marked it.
async fn commits(
    meta: &dyn MetaStore,
    repo: &str,
    wants: &[ObjectId],
    haves: &[ObjectId],
    cut: &HashSet<ObjectId>,
) -> Result<Vec<ChangesetRecord>> {
    let mut flags: HashMap<ObjectId, u8> = HashMap::new();
    let mut records: HashMap<ObjectId, ChangesetRecord> = HashMap::new();
    let mut queue: BinaryHeap<(u64, ObjectId)> = BinaryHeap::new();
    // Queued commits still wanted: the walk ends when none is left
    let mut wanted = 0usize;
    for (ids, flag) in [(haves, HAVE), (wants, WANTED)] {
        for id in ids {
            let f = flags.entry(*id).or_insert(0);
            if *f == 0 {
                let rec = record(meta, repo, *id).await?;
                queue.push((rec.generation, *id));
                records.insert(*id, rec);
            }
            *f |= flag;
        }
    }
    wanted += flags.values().filter(|f| **f == WANTED).count();
    let mut out = Vec::new();
    while wanted > 0 {
        let Some((_, id)) = queue.pop() else { break };
        let f = flags[&id];
        if f == WANTED {
            wanted -= 1;
        }
        let rec = records.remove(&id).expect("queued commits have records");
        let parents: &[ObjectId] = if cut.contains(&id) { &[] } else { &rec.parents };
        for p in parents {
            let old = flags.get(p).copied();
            let new = old.unwrap_or(0) | if f & HAVE != 0 { HAVE } else { WANTED };
            if old == Some(new) {
                continue;
            }
            flags.insert(*p, new);
            match old {
                None => {
                    let prec = record(meta, repo, *p).await?;
                    queue.push((prec.generation, *p));
                    records.insert(*p, prec);
                    if new == WANTED {
                        wanted += 1;
                    }
                }
                // A wanted commit still queued turns out to be had
                Some(WANTED) => wanted -= 1,
                Some(_) => {}
            }
        }
        if f == WANTED {
            out.push(rec);
        }
    }
    Ok(out)
}

async fn record(meta: &dyn MetaStore, repo: &str, id: ObjectId) -> Result<ChangesetRecord> {
    meta.get_changeset(repo, &id)
        .await?
        .ok_or_else(|| Error::Internal(format!("commit {id} has no record")))
}

async fn load_tree(blobs: &dyn BlobStore, id: ObjectId) -> Result<Tree> {
    let bytes = blobs
        .get(&id)
        .await?
        .ok_or_else(|| Error::Internal(format!("tree {id} is missing")))?;
    Tree::decode(&bytes).map_err(|e| Error::Internal(format!("tree {id}: {e}")))
}

/// Adds a tree and what it holds that the bases (the same path's trees in the parents) do not.
async fn walk_tree(
    blobs: &dyn BlobStore,
    root: ObjectId,
    root_bases: Vec<ObjectId>,
    filter: Filter,
    seen: &mut HashSet<ObjectId>,
    plan: &mut Plan,
) -> Result<()> {
    // An explicit stack of (tree, the same path's trees in the parents)
    let mut stack = vec![(root, root_bases)];
    while let Some((tree, bases)) = stack.pop() {
        if bases.contains(&tree) || !seen.insert(tree) {
            continue;
        }
        plan.trees.push((tree, bases.first().copied()));
        let t = load_tree(blobs, tree).await?;
        let mut base_entries: Vec<HashMap<String, TreeEntry>> = Vec::with_capacity(bases.len());
        for b in &bases {
            let bt = load_tree(blobs, *b).await?;
            base_entries.push(bt.entries().iter().map(|e| (e.name.clone(), e.clone())).collect());
        }
        for e in t.entries() {
            let same: Vec<&TreeEntry> = base_entries.iter().filter_map(|m| m.get(&e.name)).collect();
            if same
                .iter()
                .any(|b| b.id == e.id && (b.mode == EntryMode::Tree) == (e.mode == EntryMode::Tree))
            {
                continue;
            }
            if e.mode == EntryMode::Tree {
                let sub: Vec<ObjectId> = same
                    .iter()
                    .filter(|b| b.mode == EntryMode::Tree)
                    .map(|b| b.id)
                    .collect();
                stack.push((e.id, sub));
            } else if filter.keeps(e.size) && seen.insert(e.id) {
                check_blob(e.id)?;
                let base = same.iter().find(|b| b.mode != EntryMode::Tree).map(|b| b.id);
                plan.files.push((e.id, base));
            }
        }
    }
    Ok(())
}

/// Chooses the objects to send (design §4), oldest commit first. A tree or file whose id equals
/// the entry at the same path in one of the commit's parents is skipped: the parent was walked
/// before it, or the client has it. Walking the newest first would break that when a change is
/// reverted: the reverted tree, already seen, would not be walked again for the older commit.
pub async fn plan(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    wants: &Wants,
    haves: &[ObjectId],
    cut: &HashSet<ObjectId>,
    filter: Filter,
) -> Result<Plan> {
    let commits = commits(meta, repo, &wants.commits, haves, cut).await?;
    let mut roots: HashMap<ObjectId, ObjectId> = HashMap::new();
    let mut plan = Plan {
        tags: wants.tags.clone(),
        ..Plan::default()
    };
    let mut seen: HashSet<ObjectId> = HashSet::new();
    // Trees a partial clone asks for go whole, as roots without parents
    let roots_wanted: Vec<(ObjectId, Vec<ObjectId>)> = wants.trees.iter().map(|t| (*t, Vec::new())).collect();
    for (tree, bases) in roots_wanted {
        walk_tree(blobs, tree, bases, filter, &mut seen, &mut plan).await?;
    }
    for f in &wants.files {
        if seen.insert(*f) {
            check_blob(*f)?;
            plan.files.push((*f, None));
        }
    }
    for c in commits.iter().rev() {
        // A shallow commit's parents are not on the client: its whole tree goes
        let parents: &[ObjectId] = if cut.contains(&c.id) { &[] } else { &c.parents };
        let mut parent_trees = Vec::with_capacity(parents.len());
        for p in parents {
            let root = match roots.get(p) {
                Some(r) => *r,
                None => record(meta, repo, *p).await?.root_tree,
            };
            parent_trees.push(root);
        }
        roots.insert(c.id, c.root_tree);
        walk_tree(blobs, c.root_tree, parent_trees, filter, &mut seen, &mut plan).await?;
    }
    plan.commits = commits;
    Ok(plan)
}

/// The shallow boundary of a fetch: the commits whose parents are not sent, and the
/// `shallow` and `unshallow` lines that tell the client.
#[derive(Debug, Default)]
pub struct Shallow {
    /// Commits whose parents the walk does not follow: the new boundary, and every shallow
    /// commit of the client (it has no parents of them, whatever the haves say).
    pub cut: HashSet<ObjectId>,
    /// The parents of client commits that stop being shallow: wanted too.
    pub more_wants: Vec<ObjectId>,
    pub shallow: Vec<[u8; 20]>,
    pub unshallow: Vec<[u8; 20]>,
}

/// Computes the boundary. A client's shallow commit that `deepen` reaches past is an
/// `unshallow` line, and its parents are wanted; a new boundary commit with parents is a
/// `shallow` line.
pub async fn shallow(
    meta: &dyn MetaStore,
    blobs: &dyn BlobStore,
    repo: &str,
    wants: &[ObjectId],
    client: &[([u8; 20], ObjectId)],
    deepen: Option<Deepen>,
) -> Result<Shallow> {
    let client_ids: HashSet<ObjectId> = client.iter().map(|(_, id)| *id).collect();
    let Some(deepen) = deepen else {
        return Ok(Shallow {
            cut: client_ids,
            ..Default::default()
        });
    };
    // A walk from the wants, level by level: `inside` are the commits whose parents are sent
    let mut inside: HashSet<ObjectId> = HashSet::new();
    let mut boundary: Vec<ObjectId> = Vec::new();
    let mut seen: HashSet<ObjectId> = wants.iter().copied().collect();
    let mut level: Vec<ObjectId> = wants.to_vec();
    let mut depth = 1u32;
    while !level.is_empty() {
        let mut next = Vec::new();
        for id in level {
            let rec = record(meta, repo, id).await?;
            let keep = match deepen {
                Deepen::Depth(n) => depth < n,
                Deepen::Since(t) => {
                    let mut all = true;
                    for p in &rec.parents {
                        let cs = scion_query::load_changeset(blobs, *p).await?;
                        all &= cs.committer.time_ms.div_euclid(1000) >= t;
                    }
                    all
                }
            };
            if !keep {
                if !rec.parents.is_empty() {
                    boundary.push(id);
                }
                continue;
            }
            inside.insert(id);
            for p in &rec.parents {
                if seen.insert(*p) {
                    next.push(*p);
                }
            }
        }
        level = next;
        depth = depth.saturating_add(1);
    }
    let mut objects = Objects::new(meta, blobs, repo);
    let mut out = Shallow::default();
    for id in boundary {
        out.cut.insert(id);
        if !client_ids.contains(&id) {
            out.shallow.push(objects.commit_sha(id).await?);
        }
    }
    for (sha, id) in client {
        out.cut.insert(*id);
        if inside.contains(id) {
            out.unshallow.push(*sha);
            out.more_wants.extend(record(meta, repo, *id).await?.parents);
        }
    }
    Ok(out)
}

/// Refuses a pack with redacted files: their git SHAs cover content nobody may read any more.
pub async fn check_redactions(meta: &dyn MetaStore, repo: &str, files: &[(ObjectId, Option<ObjectId>)]) -> Result<()> {
    let files: Vec<ObjectId> = files.iter().map(|(id, _)| *id).collect();
    for chunk in files.chunks(1000) {
        let found = meta.get_redactions(repo, chunk).await?;
        if let Some(r) = found.iter().find(|r| r.active) {
            return Err(Error::Unservable(format!(
                "file {} was redacted ({}); git cannot fetch history that contains it, use scn",
                r.id, r.redaction_id
            )));
        }
    }
    Ok(())
}

/// Sends pack data, on side band 1 when the client asked for side bands.
pub struct Out {
    tx: mpsc::Sender<Bytes>,
    side_band: bool,
    progress: bool,
}

impl Out {
    pub fn new(tx: mpsc::Sender<Bytes>, side_band: bool, progress: bool) -> Self {
        Self {
            tx,
            side_band,
            progress,
        }
    }

    /// Sends bytes as they are (`false` when the client went away).
    pub async fn raw(&self, b: Bytes) -> bool {
        self.tx.send(b).await.is_ok()
    }

    pub async fn data(&self, b: Bytes) -> bool {
        if !self.side_band {
            return self.raw(b).await;
        }
        for chunk in b.chunks(MAX_BAND_DATA) {
            if !self.raw(pkt::band(1, chunk)).await {
                return false;
            }
        }
        true
    }

    pub async fn progress(&self, msg: &str) -> bool {
        if !self.side_band || !self.progress {
            return true;
        }
        self.raw(pkt::band(2, msg.as_bytes())).await
    }

    /// Reports an error the client prints as `remote: …`; without side bands the pack just ends.
    pub async fn error(&self, msg: &str) {
        if self.side_band {
            let _ = self.raw(pkt::band(3, format!("{msg}\n").as_bytes())).await;
        }
    }

    pub async fn flush(&self) {
        if self.side_band {
            let _ = self.raw(Bytes::from_static(pkt::FLUSH)).await;
        }
    }
}

/// A file's git blob SHA, from derived data or computed from its content.
async fn blob_sha(meta: &dyn MetaStore, repo: &str, id: ObjectId, content: &[u8]) -> Result<[u8; 20]> {
    let found = meta
        .get_derived(
            repo,
            scion_query::GIT_BLOB_KIND,
            scion_query::GIT_OBJECT_VERSION,
            &id.to_bytes(),
        )
        .await?;
    Ok(match found.and_then(|v| <[u8; 20]>::try_from(v.as_slice()).ok()) {
        Some(sha) => sha,
        None => scion_query::git_hash("blob", content),
    })
}

/// How a pack may refer to other objects.
#[derive(Debug, Clone, Copy, Default)]
pub struct PackOptions {
    /// Deltas against objects the client has.
    pub thin: bool,
    /// Deltas naming earlier entries by offset (otherwise by SHA).
    pub ofs: bool,
}

/// Contents written recently, for deltas against them, within a byte budget.
struct Recent {
    map: HashMap<ObjectId, (u64, Bytes)>,
    order: std::collections::VecDeque<ObjectId>,
    bytes: usize,
}

/// What the cache of written contents may hold.
const RECENT_BYTES: usize = 64 << 20;

impl Recent {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            order: Default::default(),
            bytes: 0,
        }
    }

    fn insert(&mut self, id: ObjectId, offset: u64, content: Bytes) {
        if content.len() > crate::delta::MAX_DELTA_INPUT {
            return;
        }
        self.bytes += content.len();
        self.map.insert(id, (offset, content));
        self.order.push_back(id);
        while self.bytes > RECENT_BYTES {
            let Some(old) = self.order.pop_front() else { break };
            if let Some((_, c)) = self.map.remove(&old) {
                self.bytes -= c.len();
            }
        }
    }
}

/// One tree or file entry: a delta when it pays off and the base can be named, else whole.
struct Deltas {
    opts: PackOptions,
    /// Everything the pack holds: a delta names one of them once it is written.
    planned: HashSet<ObjectId>,
    written: HashMap<ObjectId, u64>,
    recent: Recent,
}

impl Deltas {
    /// The pack entry for `content`, given its base's content and git SHA when known.
    fn entry(
        &mut self,
        w: &mut PackWriter,
        kind: Kind,
        id: ObjectId,
        content: &Bytes,
        base: Option<(ObjectId, &[u8], [u8; 20])>,
    ) -> Bytes {
        let at = w.offset();
        let entry = base
            .and_then(|(bid, bytes, sha)| {
                let written = self.written.get(&bid).copied();
                // The plan puts a base before what it is the base of; were one later, it might
                // be a delta against this very object, which git could never resolve
                if written.is_none() && self.planned.contains(&bid) {
                    return None;
                }
                let d = crate::delta::delta(bytes, content)?;
                match written {
                    Some(off) if self.opts.ofs => Some(w.ofs_delta(off, &d)),
                    _ => Some(w.ref_delta(&sha, &d)),
                }
            })
            .unwrap_or_else(|| w.object(kind, content));
        self.written.insert(id, at);
        self.recent.insert(id, at, content.clone());
        entry
    }

    /// Whether a delta may name `base`: it is in the pack (once written, which [`Deltas::entry`]
    /// checks), or the client has it and takes a thin pack.
    fn can_name(&self, base: ObjectId) -> bool {
        self.planned.contains(&base) || self.opts.thin
    }
}

/// A pack whose commits are built, up to a budget, before anything is
/// sent: a commit git cannot be given then fails the fetch with a plain error, not a broken pack.
pub struct Started<'a> {
    plan: Plan,
    objects: Objects<'a>,
    writer: PackWriter,
    head: Bytes,
    commits: Vec<Bytes>,
}

/// Starts the pack for this plan: builds its first commits, `budget` bytes of them.
pub async fn start<'a>(
    meta: &'a dyn MetaStore,
    blobs: &'a dyn BlobStore,
    repo: &'a str,
    plan: Plan,
    budget: usize,
) -> Result<Started<'a>> {
    let total = u32::try_from(plan.len()).map_err(|_| Error::Internal("too many objects".into()))?;
    let (mut writer, head) = PackWriter::new(total);
    let mut objects = Objects::new(meta, blobs, repo);
    let ids: Vec<ObjectId> = plan.commits.iter().map(|c| c.id).collect();
    let roots: Vec<ObjectId> = plan.commits.iter().map(|c| c.root_tree).collect();
    objects.prefetch(&ids, &roots).await?;
    let mut commits = Vec::new();
    let mut size = 0;
    for c in &plan.commits {
        if size >= budget {
            break;
        }
        let entry = writer.object(Kind::Commit, &objects.commit(c).await?);
        size += entry.len();
        commits.push(entry);
    }
    Ok(Started {
        plan,
        objects,
        writer,
        head,
        commits,
    })
}

/// Writes a started pack. Trees and files go in the plan's order, oldest first, so that a
/// delta's base comes before it. Returns early, with no error, when the client goes away.
pub async fn write_pack(started: Started<'_>, out: &Out, opts: PackOptions) -> Result<()> {
    let Started {
        plan,
        mut objects,
        writer: mut w,
        head,
        commits,
    } = started;
    let (meta, blobs, repo) = (objects.meta, objects.blobs, objects.repo);
    let plan = &plan;
    if !out
        .progress(&format!("Counting objects: {}, done.\n", plan.len()))
        .await
    {
        return Ok(());
    }
    if !out.data(head).await {
        return Ok(());
    }
    let built = commits.len();
    for entry in commits {
        if !out.data(entry).await {
            return Ok(());
        }
    }
    for c in &plan.commits[built..] {
        let body = objects.commit(c).await?;
        if !out.data(w.object(Kind::Commit, &body)).await {
            return Ok(());
        }
    }
    for t in &plan.tags {
        if !out.data(w.object(Kind::Tag, t)).await {
            return Ok(());
        }
    }
    let mut d = Deltas {
        opts,
        planned: plan.trees.iter().chain(&plan.files).map(|(id, _)| *id).collect(),
        written: HashMap::new(),
        recent: Recent::new(),
    };
    for (t, base) in &plan.trees {
        let tree = load_tree(blobs, *t).await?;
        let body = Bytes::from(objects.tree(*t, &tree).await?);
        let base = match base.filter(|b| d.can_name(*b)) {
            Some(b) => {
                let bytes = match d.recent.map.get(&b) {
                    Some((_, c)) => c.clone(),
                    None => Bytes::from(objects.tree(b, &load_tree(blobs, b).await?).await?),
                };
                Some((b, bytes, objects.tree_sha(b).await?))
            }
            None => None,
        };
        let entry = d.entry(
            &mut w,
            Kind::Tree,
            *t,
            &body,
            base.as_ref().map(|(b, c, s)| (*b, &c[..], *s)),
        );
        if !out.data(entry).await {
            return Ok(());
        }
    }
    // Each read also fetches the base and its SHA: the loop below must not touch storage while
    // reads are in flight, or they could hold every connection it needs
    let can_name = |b: &ObjectId| d.can_name(*b);
    let wanted: Vec<(ObjectId, Option<ObjectId>)> = plan
        .files
        .iter()
        .map(|(id, base)| (*id, base.filter(|b| can_name(b))))
        .collect();
    let files = futures::stream::iter(wanted)
        .map(|(id, base)| async move {
            let content = blobs
                .get(&id)
                .await?
                .ok_or_else(|| Error::Unservable(format!("the content of file {id} is missing (purged?)")))?;
            let base = match base.filter(|_| content.len() <= crate::delta::MAX_DELTA_INPUT) {
                Some(b) => match blobs.get(&b).await? {
                    Some(bytes) => {
                        let sha = blob_sha(meta, repo, b, &bytes).await?;
                        Some((b, bytes, sha))
                    }
                    None => None,
                },
                None => None,
            };
            Ok::<_, Error>((id, content, base))
        })
        .buffered(READ_AHEAD);
    let mut files = std::pin::pin!(files);
    let mut sent = 0usize;
    while let Some((id, content, base)) = files.try_next().await? {
        let entry = d.entry(
            &mut w,
            Kind::Blob,
            id,
            &content,
            base.as_ref().map(|(b, c, s)| (*b, &c[..], *s)),
        );
        if !out.data(entry).await {
            return Ok(());
        }
        sent += 1;
        if sent % 10_000 == 0 && !out.progress(&format!("Writing files: {sent}\r")).await {
            return Ok(());
        }
    }
    out.data(w.finish()).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_name_only_bases_already_written() {
        let old = Bytes::from("0123456789abcdef".repeat(8));
        let new = Bytes::from(format!("{}tail", "0123456789abcdef".repeat(8)));
        let (a, b) = (ObjectId::of_blob(&old), ObjectId::of_blob(&new));
        let mut d = Deltas {
            opts: PackOptions { thin: false, ofs: true },
            planned: [a, b].into_iter().collect(),
            written: HashMap::new(),
            recent: Recent::new(),
        };
        let (mut w, _) = PackWriter::new(2);
        // The base comes later in the pack: whole (type 3, a blob)
        let first = d.entry(&mut w, Kind::Blob, b, &new, Some((a, &old, [0; 20])));
        assert_eq!(first[0] >> 4 & 7, 3);
        // Now written: an ofs-delta (type 6)
        let second = d.entry(&mut w, Kind::Blob, a, &old, Some((b, &new, [0; 20])));
        assert_eq!(second[0] >> 4 & 7, 6);
    }

    fn sha(b: u8) -> [u8; 20] {
        [b; 20]
    }

    fn adv() -> Advertised {
        Advertised {
            refs: vec![("refs/heads/main".into(), sha(1)), ("refs/tags/v1".into(), sha(2))],
            head: Some(("refs/heads/main".into(), sha(1))),
            // v1 is an annotated tag of commit 9
            peeled: HashMap::from([("refs/tags/v1".to_string(), sha(9))]),
        }
    }

    fn lines(b: &[u8]) -> Vec<String> {
        let mut r = Reader::new(b);
        let mut out = Vec::new();
        while let Some(p) = r.read().expect("pkt") {
            out.push(match p {
                Pkt::Data(d) => String::from_utf8_lossy(d).into_owned(),
                Pkt::Flush => "0000".into(),
                Pkt::Delim => "0001".into(),
                Pkt::End => "0002".into(),
            });
        }
        out
    }

    #[test]
    fn names_refs_for_git() {
        assert_eq!(git_ref_name("main"), "refs/heads/main");
        assert_eq!(git_ref_name("feature/x"), "refs/heads/feature/x");
        assert_eq!(git_ref_name("tags/v1"), "refs/tags/v1");
    }

    #[test]
    fn advertises_v0_refs_and_an_empty_repository() {
        let got = lines(&advertise_v0(&adv(), "scion/1"));
        assert!(got[0].starts_with(&format!("{} HEAD\0multi_ack_detailed", "01".repeat(20))));
        assert!(got[0].ends_with("symref=HEAD:refs/heads/main agent=scion/1"));
        assert_eq!(got[1], format!("{} refs/heads/main", "01".repeat(20)));
        assert_eq!(got[2], format!("{} refs/tags/v1", "02".repeat(20)));
        assert_eq!(got[3], format!("{} refs/tags/v1^{{}}", "09".repeat(20)));
        assert_eq!(got[4], "0000");
        let empty = Advertised {
            refs: vec![],
            head: None,
            peeled: HashMap::new(),
        };
        let got = lines(&advertise_v0(&empty, "scion/1"));
        assert!(got[0].starts_with(&format!("{} capabilities^{{}}\0", "0".repeat(40))));
        assert!(!got[0].contains("symref"));
        assert_eq!(lines(&advertise_v2("scion/1"))[0], "version 2");
    }

    #[test]
    fn lists_refs_by_prefix() {
        let all = lines(&ls_refs(&adv(), &[b"symrefs"]));
        assert_eq!(
            all[0],
            format!("{} HEAD symref-target:refs/heads/main", "01".repeat(20))
        );
        assert_eq!(all.len(), 4);
        let tags = lines(&ls_refs(&adv(), &[b"ref-prefix refs/tags/"]));
        assert_eq!(tags, [format!("{} refs/tags/v1", "02".repeat(20)), "0000".into()]);
        let peeled = lines(&ls_refs(&adv(), &[b"ref-prefix refs/tags/", b"peel"]));
        assert_eq!(
            peeled[0],
            format!("{} refs/tags/v1 peeled:{}", "02".repeat(20), "09".repeat(20))
        );
        let head = lines(&ls_refs(&adv(), &[b"ref-prefix HEAD"]));
        assert_eq!(head, [format!("{} HEAD", "01".repeat(20)), "0000".into()]);
    }

    #[test]
    fn parses_v2_requests() {
        let mut body = BytesMut::new();
        pkt::put_line(&mut body, "command=fetch");
        pkt::put_line(&mut body, "agent=git/2");
        body.extend_from_slice(pkt::DELIM);
        pkt::put_line(&mut body, &format!("want {}", "ab".repeat(20)));
        pkt::put_line(&mut body, &format!("have {}", "cd".repeat(20)));
        pkt::put_line(&mut body, "no-progress");
        pkt::put_line(&mut body, "done");
        body.extend_from_slice(pkt::FLUSH);
        let req = parse_v2(&body).expect("parse");
        assert_eq!(req.command, "fetch");
        let f = parse_fetch_v2(&req.args).expect("fetch");
        assert_eq!(f.wants, [[0xab; 20]]);
        assert_eq!(f.haves, [[0xcd; 20]]);
        assert!(f.done && f.no_progress && f.side_band);

        // A command with no arguments; no command at all; a stray packet
        let mut b = BytesMut::new();
        pkt::put_line(&mut b, "command=ls-refs");
        b.extend_from_slice(pkt::FLUSH);
        let r = parse_v2(&b).expect("parse");
        assert_eq!((r.command.as_str(), r.args.len()), ("ls-refs", 0));
        assert!(parse_v2(b"00010000").is_err());
        assert!(parse_v2(b"0002").is_err());
        let mut b = BytesMut::new();
        pkt::put_line(&mut b, "command=fetch");
        b.extend_from_slice(pkt::DELIM);
        b.extend_from_slice(pkt::DELIM);
        assert!(parse_v2(&b).is_err());
    }

    #[test]
    fn parses_v0_requests() {
        let mut body = BytesMut::new();
        pkt::put_line(
            &mut body,
            &format!(
                "want {} multi_ack_detailed side-band-64k no-done no-progress ofs-delta",
                "ab".repeat(20)
            ),
        );
        pkt::put_line(&mut body, &format!("want {}", "ac".repeat(20)));
        body.extend_from_slice(pkt::FLUSH);
        pkt::put_line(&mut body, &format!("have {}", "cd".repeat(20)));
        pkt::put_line(&mut body, "done");
        let f = parse_fetch_v0(&body).expect("parse");
        assert_eq!(f.wants, [[0xab; 20], [0xac; 20]]);
        assert_eq!(f.haves, [[0xcd; 20]]);
        assert!(f.done && f.side_band && f.no_progress && f.multi_ack && f.no_done);
    }

    #[test]
    fn parses_shallow_requests() {
        let shallow = format!("shallow {}", "ab".repeat(20));
        let f = parse_fetch_v2(&[shallow.as_bytes(), b"deepen 3"]).expect("parse");
        assert_eq!(
            (f.shallow.clone(), f.deepen),
            (vec![[0xab; 20]], Some(Deepen::Depth(3)))
        );
        let f = parse_fetch_v2(&[b"deepen-since 1700000000"]).expect("parse");
        assert_eq!(f.deepen, Some(Deepen::Since(1_700_000_000)));
        // --unshallow asks for the largest depth; 0 means 1
        let f = parse_fetch_v2(&[b"deepen 2147483647"]).expect("parse");
        assert_eq!(f.deepen, Some(Deepen::Depth(2_147_483_647)));
        assert_eq!(
            parse_fetch_v2(&[b"deepen 0"]).expect("parse").deepen,
            Some(Deepen::Depth(1))
        );
        let mut b = BytesMut::new();
        pkt::put_line(&mut b, &format!("want {} shallow", "cd".repeat(20)));
        pkt::put_line(&mut b, &shallow);
        pkt::put_line(&mut b, "deepen 1");
        let f = parse_fetch_v0(&b).expect("parse");
        assert_eq!((f.shallow.len(), f.deepen), (1, Some(Deepen::Depth(1))));
        assert!(parse_fetch_v2(&[b"deepen soon"]).is_err());
        assert!(parse_fetch_v2(&[b"shallow abc"]).is_err());
    }

    #[test]
    fn parses_filters() {
        for (spec, want) in [
            ("blob:none", Filter::BlobNone),
            ("blob:limit=100", Filter::BlobLimit(100)),
            ("blob:limit=2k", Filter::BlobLimit(2048)),
            ("blob:limit=1M", Filter::BlobLimit(1 << 20)),
            ("blob:limit=1g", Filter::BlobLimit(1 << 30)),
        ] {
            let line = format!("filter {spec}");
            assert_eq!(parse_fetch_v2(&[line.as_bytes()]).expect(spec).filter, want, "{spec}");
        }
        for bad in ["blob:limit=x", "blob:limit=1q", "sparse:oid=abc"] {
            assert!(parse_fetch_v2(&[format!("filter {bad}").as_bytes()]).is_err(), "{bad}");
        }
        assert!(Filter::None.keeps(u64::MAX));
        assert!(!Filter::BlobNone.keeps(0));
        assert!(Filter::BlobLimit(10).keeps(10) && !Filter::BlobLimit(10).keeps(11));
    }

    #[test]
    fn refuses_unsupported_requests_and_bad_ids() {
        for line in ["deepen-not main", "deepen-relative", "filter tree:0"] {
            let e = parse_fetch_v2(&[line.as_bytes()]).expect_err(line);
            assert!(matches!(e, Error::Unsupported(_)), "{line}");
            let mut b = BytesMut::new();
            pkt::put_line(&mut b, line);
            assert!(parse_fetch_v0(&b).is_err(), "{line}");
        }
        assert!(parse_fetch_v2(&[b"want abc"]).is_err());
        assert!(parse_fetch_v2(&[format!("want {}", "zz".repeat(20)).as_bytes()]).is_err());
    }

    #[test]
    fn decides_when_to_send_the_pack() {
        let mut req = FetchRequest::default();
        assert!(ready(&req, 0));
        req.haves = vec![sha(1); 3];
        assert!(!ready(&req, 0));
        assert!(ready(&req, 1));
        req.haves = vec![sha(1); GIVE_UP_HAVES];
        assert!(ready(&req, 0));
        let p = Plan::default();
        assert!(p.is_empty());
    }

    #[tokio::test]
    async fn refuses_redacted_files_and_unknown_wants() {
        let s = scion_store::MemStore::new();
        s.create_repo("r").await.expect("repo");
        let id = ObjectId::of_blob(b"secret");
        assert_eq!(check_redactions(&s, "r", &[(id, None)]).await, Ok(()));
        s.put_redactions(&[scion_store::Redaction {
            id,
            repo: "r".into(),
            redaction_id: "red-1".into(),
            reason: String::new(),
            created_by: "root".into(),
            created_at_ms: 0,
            active: true,
            purge_after_ms: None,
            purged_at_ms: None,
            seq: 0,
        }])
        .await
        .expect("redact");
        let e = check_redactions(&s, "r", &[(id, None)]).await.expect_err("redacted");
        assert!(e.to_string().contains("red-1"), "{e}");
        let e = resolve_wants(&s, &s, "r", &[sha(7)]).await.expect_err("unknown");
        assert!(e.to_string().contains(&"07".repeat(20)), "{e}");
    }
}
