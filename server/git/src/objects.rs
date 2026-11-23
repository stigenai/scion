//! git objects built from Scion's (design §3): blobs are file contents, trees list their entries'
//! git SHAs, and a commit's object is its raw git object, the object rebuilt from its fields, or
//! the computed one with the `scion-id` header.

use std::collections::HashMap;

use scion_object::{Changeset, EntryMode, ObjectId, ObjectKind, Tree};
use scion_query::{
    GIT_BLOB_ID_KIND, GIT_BLOB_KIND, GIT_COMMIT_KIND, GIT_OBJECT_VERSION, GIT_TREE_ID_KIND, GIT_TREE_KIND,
    derive_git_sha, git_commit_body, git_commit_body_plain, git_hash, git_sha_of, git_tree_body, load_changeset,
    tree_git_sha,
};
use scion_store::{BlobStore, ChangesetRecord, MetaStore};

use crate::{Error, Result};

/// Directories whose reverse git SHA mappings were written by [`Objects::index_reverse`].
pub const REVERSE_INDEXED_KIND: &str = "git-reverse-indexed";

/// The extension holding a commit's git object exactly as git wrote it.
pub const GIT_COMMIT_EXTENSION: &str = "git.commit";

/// How many keys one derived-data query asks for.
const BATCH: usize = 1000;

/// Reads git objects out of one repository, remembering the SHAs it has looked up.
pub struct Objects<'a> {
    pub meta: &'a dyn MetaStore,
    pub blobs: &'a dyn BlobStore,
    pub repo: &'a str,
    commits: HashMap<ObjectId, [u8; 20]>,
    trees: HashMap<ObjectId, [u8; 20]>,
    files: HashMap<ObjectId, [u8; 20]>,
}

impl<'a> Objects<'a> {
    pub fn new(meta: &'a dyn MetaStore, blobs: &'a dyn BlobStore, repo: &'a str) -> Self {
        Self {
            meta,
            blobs,
            repo,
            commits: HashMap::new(),
            trees: HashMap::new(),
            files: HashMap::new(),
        }
    }

    /// Looks up the cached SHAs of these commits and root trees in a few batched queries, so
    /// [`Objects::commit`] rarely needs one of its own.
    pub async fn prefetch(&mut self, commits: &[ObjectId], trees: &[ObjectId]) -> Result<()> {
        let found = self.cached_many(GIT_COMMIT_KIND, commits).await?;
        self.commits.extend(found);
        let found = self.cached_many(GIT_TREE_KIND, trees).await?;
        self.trees.extend(found);
        Ok(())
    }

    /// Looks up the cached SHAs of the entries of these trees, and of the trees in `more`, in
    /// one batched query for each kind, so [`Objects::tree`] needs no query of its own for them.
    pub async fn prefetch_entries(&mut self, trees: &[&Tree], more: &[ObjectId]) -> Result<()> {
        let mut dirs: Vec<ObjectId> = more.to_vec();
        let mut files: Vec<ObjectId> = Vec::new();
        for e in trees.iter().flat_map(|t| t.entries()) {
            if e.mode == EntryMode::Tree {
                dirs.push(e.id);
            } else {
                files.push(e.id);
            }
        }
        self.lookup_trees(dirs).await?;
        self.lookup_files(files).await
    }

    /// Adds the cached SHAs of the trees not known yet.
    async fn lookup_trees(&mut self, mut ids: Vec<ObjectId>) -> Result<()> {
        ids.retain(|id| !self.trees.contains_key(id));
        ids.sort_unstable();
        ids.dedup();
        let found = self.cached_many(GIT_TREE_KIND, &ids).await?;
        self.trees.extend(found);
        Ok(())
    }

    /// Adds the cached SHAs of the files not known yet.
    async fn lookup_files(&mut self, mut ids: Vec<ObjectId>) -> Result<()> {
        ids.retain(|id| !self.files.contains_key(id));
        ids.sort_unstable();
        ids.dedup();
        let found = self.cached_many(GIT_BLOB_KIND, &ids).await?;
        self.files.extend(found);
        Ok(())
    }

    pub(crate) async fn cached_many(&self, kind: &str, ids: &[ObjectId]) -> Result<Vec<(ObjectId, [u8; 20])>> {
        let mut out = Vec::new();
        for chunk in ids.chunks(BATCH) {
            let keys: Vec<Vec<u8>> = chunk.iter().map(|id| id.to_bytes().to_vec()).collect();
            let values = self
                .meta
                .get_derived_many(self.repo, kind, GIT_OBJECT_VERSION, &keys)
                .await?;
            for (id, v) in chunk.iter().zip(values) {
                if let Some(sha) = v.and_then(|v| <[u8; 20]>::try_from(v.as_slice()).ok()) {
                    out.push((*id, sha));
                }
            }
        }
        Ok(out)
    }

    /// A commit's git SHA, derived first if it was not yet.
    pub async fn commit_sha(&mut self, id: ObjectId) -> Result<[u8; 20]> {
        if let Some(sha) = self.commits.get(&id) {
            return Ok(*sha);
        }
        let sha = derive_git_sha(self.meta, self.blobs, self.repo, id).await?;
        self.commits.insert(id, sha);
        Ok(sha)
    }

    /// A directory's git tree SHA, derived first if it was not yet.
    pub async fn tree_sha(&mut self, id: ObjectId) -> Result<[u8; 20]> {
        if let Some(sha) = self.trees.get(&id) {
            return Ok(*sha);
        }
        let sha = tree_git_sha(self.meta, self.blobs, self.repo, id).await?;
        self.trees.insert(id, sha);
        Ok(sha)
    }

    /// A commit's git object.
    pub async fn commit(&mut self, rec: &ChangesetRecord) -> Result<Vec<u8>> {
        let cs = load_changeset(self.blobs, rec.id).await?;
        let sha = self.commit_sha(rec.id).await?;
        let tree = self.tree_sha(rec.root_tree).await?;
        let mut parents = Vec::with_capacity(rec.parents.len());
        for p in &rec.parents {
            parents.push(self.commit_sha(*p).await?);
        }
        commit_object(rec.id, &cs, sha, &tree, &parents)
    }

    /// A directory's git tree object.
    pub async fn tree(&mut self, id: ObjectId, tree: &Tree) -> Result<Vec<u8>> {
        let shas = self.entry_git_shas(id, tree).await?;
        let entries: Vec<(String, EntryMode, [u8; 20])> = tree
            .entries()
            .iter()
            .zip(shas)
            .map(|(e, sha)| (e.name.clone(), e.mode, sha))
            .collect();
        Ok(git_tree_body(&entries))
    }

    /// The git SHAs of a directory's entries, in order, derived first if they were not yet.
    pub async fn entry_git_shas(&mut self, id: ObjectId, tree: &Tree) -> Result<Vec<[u8; 20]>> {
        Ok(match self.entry_shas(tree).await? {
            Some(shas) => shas,
            None => {
                // Not derived yet (a tree of an imported commit): derive it, which caches its
                // entries too.
                self.tree_sha(id).await?;
                self.entry_shas(tree)
                    .await?
                    .ok_or_else(|| Error::Internal(format!("git SHAs of tree {id} missing after deriving it")))?
            }
        })
    }

    /// Writes the reverse git SHA mappings (`git-tree-id`, `git-blob-id`) of every directory and
    /// file under `root`, for data derived before they existed. A marker records the directories
    /// done, so a tree is walked once.
    pub async fn index_reverse(&mut self, root: ObjectId) -> Result<()> {
        let marked = self
            .meta
            .get_derived(self.repo, REVERSE_INDEXED_KIND, GIT_OBJECT_VERSION, &root.to_bytes())
            .await?;
        if marked.is_some() {
            return Ok(());
        }
        let mut stack = vec![root];
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            let sha = self.tree_sha(id).await?;
            self.meta
                .put_derived(self.repo, GIT_TREE_ID_KIND, GIT_OBJECT_VERSION, &sha, &id.to_bytes())
                .await?;
            let bytes = self
                .blobs
                .get(&id)
                .await?
                .ok_or_else(|| Error::Internal(format!("tree {id} is missing")))?;
            let tree = Tree::decode(&bytes).map_err(|e| Error::Internal(format!("tree {id}: {e}")))?;
            let shas = self.entry_git_shas(id, &tree).await?;
            for (e, s) in tree.entries().iter().zip(shas) {
                if e.mode == EntryMode::Tree {
                    stack.push(e.id);
                } else {
                    self.meta
                        .put_derived(self.repo, GIT_BLOB_ID_KIND, GIT_OBJECT_VERSION, &s, &e.id.to_bytes())
                        .await?;
                }
            }
        }
        self.meta
            .put_derived(
                self.repo,
                REVERSE_INDEXED_KIND,
                GIT_OBJECT_VERSION,
                &root.to_bytes(),
                b"1",
            )
            .await?;
        Ok(())
    }

    /// The cached git SHAs of a tree's entries, in order; `None` when any is missing. Only the
    /// entries not known yet are looked up.
    async fn entry_shas(&mut self, tree: &Tree) -> Result<Option<Vec<[u8; 20]>>> {
        let (dirs, files): (Vec<_>, Vec<_>) = tree.entries().iter().partition(|e| e.mode == EntryMode::Tree);
        self.lookup_trees(dirs.iter().map(|e| e.id).collect()).await?;
        self.lookup_files(files.iter().map(|e| e.id).collect()).await?;
        Ok(tree
            .entries()
            .iter()
            .map(|e| {
                let known = if e.mode == EntryMode::Tree {
                    &self.trees
                } else {
                    &self.files
                };
                known.get(&e.id).copied()
            })
            .collect())
    }
}

/// Checks that a file can go into a pack: Scion's chunked large files have no git form yet.
pub fn check_blob(id: ObjectId) -> Result<()> {
    if id.kind() == ObjectKind::Blob {
        Ok(())
    } else {
        Err(Error::Unservable(format!(
            "file {id} is stored in chunks, which git cannot read yet"
        )))
    }
}

/// A commit's git object (design §3, rules 1 to 3), checked against its git SHA, tree and parents.
pub fn commit_object(
    id: ObjectId,
    cs: &Changeset,
    sha: [u8; 20],
    tree: &[u8; 20],
    parents: &[[u8; 20]],
) -> Result<Vec<u8>> {
    let body = if let Some(raw) = cs.extensions.get(GIT_COMMIT_EXTENSION) {
        if !raw_matches(raw, tree, parents) {
            return Err(Error::Unservable(format!(
                "commit {id}: its raw git object names another tree or other parents"
            )));
        }
        raw.clone()
    } else if git_sha_of(cs).is_some() {
        git_commit_body_plain(cs, tree, parents)
    } else {
        git_commit_body(id, cs, tree, parents)
    };
    if git_hash("commit", &body) != sha {
        return Err(Error::Unservable(format!(
            "git commit {} was imported without its raw git object, which git needs to check \
             it (signed commits and ones with unusual headers do); import the git repository \
             again into a new Scion repository with scn 0.1.11 or later (this one keeps serving the \
             old import), or clone it with scn (Scion commit {id})",
            hex::encode(sha)
        )));
    }
    Ok(body)
}

/// Whether a raw commit object's `tree` and `parent` headers are these.
fn raw_matches(raw: &[u8], tree: &[u8; 20], parents: &[[u8; 20]]) -> bool {
    let head = raw.split(|&b| b == b'\n').take_while(|l| !l.is_empty());
    let mut want = vec![format!("tree {}", hex::encode(tree))];
    want.extend(parents.iter().map(|p| format!("parent {}", hex::encode(p))));
    let got: Vec<&[u8]> = head
        .filter(|l| l.starts_with(b"tree ") || l.starts_with(b"parent "))
        .collect();
    got.len() == want.len() && got.iter().zip(&want).all(|(g, w)| *g == w.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_object::Signature;
    use std::collections::BTreeMap;

    fn cs(extensions: BTreeMap<String, Vec<u8>>) -> Changeset {
        let sig = Signature {
            name: "A".into(),
            email: "a@x".into(),
            time_ms: 1_700_000_000_000,
            tz_offset_minutes: 0,
        };
        Changeset::new(vec![], sig.clone(), sig, "m\n", [1; 16], vec![], extensions).expect("cs")
    }

    #[test]
    fn native_commits_carry_their_scion_id() {
        let c = cs(BTreeMap::new());
        let id = c.id();
        let body = git_commit_body(id, &c, &[1; 20], &[]);
        let sha = git_hash("commit", &body);
        assert_eq!(commit_object(id, &c, sha, &[1; 20], &[]), Ok(body));
        assert!(matches!(
            commit_object(id, &c, [0; 20], &[1; 20], &[]),
            Err(Error::Unservable(_))
        ));
    }

    #[test]
    fn imported_commits_are_rebuilt_or_refused() {
        let plain = git_commit_body_plain(&cs(BTreeMap::new()), &[1; 20], &[[2; 20]]);
        let sha = git_hash("commit", &plain);
        let c = cs(BTreeMap::from([("git.sha1".to_string(), sha.to_vec())]));
        assert_eq!(commit_object(c.id(), &c, sha, &[1; 20], &[[2; 20]]), Ok(plain));
        // A signed commit's SHA covers its signature, which the fields do not keep.
        let e = commit_object(c.id(), &c, [9; 20], &[1; 20], &[[2; 20]]);
        assert!(matches!(e, Err(Error::Unservable(m)) if m.contains("without its raw git object")));
    }

    #[test]
    fn raw_objects_are_used_when_they_match() {
        let raw = format!(
            "tree {}\nparent {}\nauthor A <a@x> 1 +0000\ncommitter A <a@x> 1 +0000\ngpgsig x\n y\n\nm\n",
            "01".repeat(20),
            "02".repeat(20)
        )
        .into_bytes();
        let sha = git_hash("commit", &raw);
        let c = cs(BTreeMap::from([
            ("git.sha1".to_string(), sha.to_vec()),
            (GIT_COMMIT_EXTENSION.to_string(), raw.clone()),
        ]));
        assert_eq!(commit_object(c.id(), &c, sha, &[1; 20], &[[2; 20]]), Ok(raw));
        // Another tree, or a parent missing
        assert!(commit_object(c.id(), &c, sha, &[3; 20], &[[2; 20]]).is_err());
        assert!(commit_object(c.id(), &c, sha, &[1; 20], &[]).is_err());
    }

    #[test]
    fn chunked_files_are_refused() {
        assert_eq!(check_blob(ObjectId::of_blob(b"x")), Ok(()));
        let chunked = ObjectId::hash(ObjectKind::ChunkList, b"x");
        assert!(check_blob(chunked).is_err());
    }
}
