//! Rename detection for `log --follow`: when a commit adds the followed file, find the file it
//! was renamed from, as git does.
//!
//! The source is a file the same commit deleted (relative to its first parent): one with the same
//! content, else the most similar one at or above [`MIN_SIMILARITY`]. Failing that, as git's
//! `--follow` also does, a file of the parent it was copied from, by the same measure.

use scion_object::{Changeset, EntryMode, FileOp, ObjectId};
use scion_store::BlobStore;
use scion_tree::TreeSource;
use scion_tree::lookup;

use crate::blame::Trees;
use crate::{Graph, Result};

/// Lowest similarity (bytes of unchanged lines over the larger file) that counts as a rename,
/// git's default `-M50%`.
pub const MIN_SIMILARITY: f64 = 0.5;
/// Files larger than this are only matched by identical content.
const MAX_COMPARED: u64 = 1 << 20;
/// At most this many deleted files are looked up, and this many of them compared by content.
const MAX_LOOKUPS: usize = 1000;
const MAX_CANDIDATES: usize = 100;
/// Files of the parent looked at for a copy.
const MAX_FILES: usize = 100_000;

/// The path `path` had before commit `cs` when `cs` renamed or copied it there; `None` when `cs`
/// did not add `path` or no file matches.
/// With `copies`, also a file of the parent that stays (git's `--follow`); without, renames only
/// (git blame).
pub(crate) async fn renamed_from(
    g: &mut Graph<'_>,
    blobs: &dyn BlobStore,
    cs: ObjectId,
    changeset: &Changeset,
    path: &str,
    copies: bool,
) -> Result<Option<String>> {
    let Some((new_id, new_size)) = changeset.delta.iter().find_map(|c| match &c.op {
        FileOp::Put { id, size, .. } if c.path == path => Some((*id, *size)),
        _ => None,
    }) else {
        return Ok(None);
    };
    let Some(parent) = g.get(cs).await?.first_parent() else {
        return Ok(None);
    };
    let root = g.get(parent).await?.root_tree;
    let trees = Trees(blobs);
    // Modified in place rather than added
    if lookup(&trees, root, path).await?.is_some() {
        return Ok(None);
    }
    let name = |p: &str| p.rsplit('/').next().map(str::to_owned);
    // Renames: files the same commit deleted, those of the same name first (moves keep it)
    let mut deletes: Vec<&str> = changeset
        .delta
        .iter()
        .filter(|c| c.op == FileOp::Delete)
        .map(|c| c.path.as_str())
        .collect();
    deletes.sort_by_key(|d| name(d) != name(path));
    deletes.truncate(MAX_LOOKUPS);
    let mut sources = Vec::new();
    for d in deletes {
        if let Some(e) = lookup(&trees, root, d).await? {
            sources.push((d.to_owned(), e.id, e.size));
        }
    }
    if let Some(p) = best_match(blobs, &sources, new_id, new_size).await? {
        return Ok(Some(p));
    }
    if !copies {
        return Ok(None);
    }
    // Copies, as git's --follow also finds: any file of the parent, those of the same name and
    // the closest size first
    let mut files = Vec::new();
    list_files(&trees, root, String::new(), &mut files).await?;
    files.sort_by_key(|(p, _, size)| (name(p) != name(path), size.abs_diff(new_size)));
    best_match(blobs, &files, new_id, new_size).await
}

/// Of `sources` (path, blob, size), one with the same content as `new_id`, else the most similar
/// at or above [`MIN_SIMILARITY`], comparing at most [`MAX_CANDIDATES`] in the order given.
async fn best_match(
    blobs: &dyn BlobStore,
    sources: &[(String, ObjectId, u64)],
    new_id: ObjectId,
    new_size: u64,
) -> Result<Option<String>> {
    if let Some((p, _, _)) = sources.iter().find(|(_, id, _)| *id == new_id) {
        return Ok(Some(p.clone()));
    }
    let comparable: Vec<(&str, ObjectId)> = sources
        .iter()
        .filter(|(_, _, size)| *size <= MAX_COMPARED && could_match(*size, new_size))
        .take(MAX_CANDIDATES)
        .map(|(p, id, _)| (p.as_str(), *id))
        .collect();
    if new_size > MAX_COMPARED || comparable.is_empty() {
        return Ok(None);
    }
    let Some(new) = blobs.get(&new_id).await? else {
        return Ok(None);
    };
    let mut best: Option<(f64, &str)> = None;
    for (p, id) in comparable {
        let Some(old) = blobs.get(&id).await? else { continue };
        let score = similarity(&old, &new);
        if score >= MIN_SIMILARITY && best.is_none_or(|(s, _)| score > s) {
            best = Some((score, p));
        }
    }
    Ok(best.map(|(_, p)| p.to_owned()))
}

/// The files under `tree` (at `base`, empty or ending in `/`), up to [`MAX_FILES`].
fn list_files<'a>(
    trees: &'a Trees<'_>,
    tree: ObjectId,
    base: String,
    out: &'a mut Vec<(String, ObjectId, u64)>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let t = trees.load_tree(&tree).await?;
        for e in t.entries() {
            if out.len() >= MAX_FILES {
                break;
            }
            let path = format!("{base}{}", e.name);
            if e.mode == EntryMode::Tree {
                list_files(trees, e.id, format!("{path}/"), out).await?;
            } else {
                out.push((path, e.id, e.size));
            }
        }
        Ok(())
    })
}

/// Whether two sizes leave room for [`MIN_SIMILARITY`]: the smaller must be at least that share
/// of the larger.
fn could_match(a: u64, b: u64) -> bool {
    a.min(b) as f64 >= a.max(b) as f64 * MIN_SIMILARITY
}

/// Bytes of the lines the two versions share (each line counted as often as both have it), over
/// the size of the larger one: git's estimate, which also ignores where the lines are.
pub(crate) fn similarity(old: &[u8], new: &[u8]) -> f64 {
    let larger = old.len().max(new.len());
    if larger == 0 {
        return 1.0;
    }
    let mut lines: std::collections::HashMap<&[u8], usize> = std::collections::HashMap::new();
    for line in old.split_inclusive(|b| *b == b'\n') {
        *lines.entry(line).or_default() += 1;
    }
    let mut same = 0;
    for line in new.split_inclusive(|b| *b == b'\n') {
        if let Some(n) = lines.get_mut(line).filter(|n| **n > 0) {
            *n -= 1;
            same += line.len();
        }
    }
    same as f64 / larger as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blame::tests::Repo;
    use crate::tests::REPO;
    use crate::{LogPage, log, log_follow};

    async fn follow(r: &Repo, start: ObjectId, path: &str) -> LogPage {
        log_follow(&r.store, &r.store, REPO, &[start], path, 100, 1000)
            .await
            .expect("log")
    }

    fn ids(page: &LogPage) -> Vec<ObjectId> {
        page.entries.iter().map(|e| e.record.id).collect()
    }

    async fn path_log(r: &Repo, start: ObjectId) -> Vec<ObjectId> {
        ids(&log(&r.store, &r.store, REPO, &[start], Some("f"), 100, 1000)
            .await
            .expect("log"))
    }

    const TEXT: &str = "one\ntwo\nthree\nfour\n";

    async fn renamed_history(indexed: bool) {
        let mut r = Repo::with_index(indexed).await;
        let c1 = r.commit(&[], &[("a.txt", Some(TEXT)), ("other", Some("o"))]).await;
        let c2 = r
            .commit(&[c1], &[("a.txt", Some("one\ntwo\nthree\nfour\nfive\n"))])
            .await;
        let c3 = r
            .commit(
                &[c2],
                &[("a.txt", None), ("b.txt", Some("one\ntwo\nthree\nfour\nfive\n"))],
            )
            .await;
        let c4 = r.commit(&[c3], &[("other", Some("p"))]).await;
        let c5 = r.commit(&[c4], &[("b.txt", Some("zero\n"))]).await;
        let page = follow(&r, c5, "b.txt").await;
        assert_eq!(ids(&page), [c5, c3]);
        assert_eq!(page.renamed_from.as_deref(), Some("a.txt"));
        let next = page.next.expect("more");
        let page = log_follow(&r.store, &r.store, REPO, &next, "a.txt", 100, 1000)
            .await
            .expect("log");
        assert_eq!(ids(&page), [c2, c1]);
        assert_eq!((page.renamed_from, page.next), (None, None));
        // Without following, history stops at the rename
        let plain = log(&r.store, &r.store, REPO, &[c5], Some("b.txt"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(ids(&plain), [c5, c3]);
        assert_eq!(plain.renamed_from, None);
    }

    #[tokio::test]
    async fn path_log_leaves_out_merges_that_took_the_file_from_a_branch() {
        for indexed in [true, false] {
            let mut r = Repo::with_index(indexed).await;
            let a = r.commit(&[], &[("f", Some("base\n")), ("g", Some("g"))]).await;
            let side = r.commit(&[a], &[("f", Some("side\n"))]).await;
            let main = r.commit(&[a], &[("g", Some("g2"))]).await;
            let m = r.commit(&[main, side], &[("f", Some("side\n"))]).await;
            assert_eq!(path_log(&r, m).await, [side, a]);
            // A merge that resolved the file differently from every parent stays
            let main2 = r.commit(&[m], &[("f", Some("main\n"))]).await;
            let side2 = r.commit(&[m], &[("f", Some("other\n"))]).await;
            let m2 = r.commit(&[main2, side2], &[("f", Some("both\n"))]).await;
            let got = path_log(&r, m2).await;
            assert_eq!((got[0], &got[3..]), (m2, &[side, a][..]));
            assert!(got[1..3].contains(&main2) && got[1..3].contains(&side2), "{got:?}");
        }
    }

    #[tokio::test]
    async fn follows_an_identical_rename_with_and_without_the_index() {
        renamed_history(true).await;
        renamed_history(false).await;
    }

    #[tokio::test]
    async fn follows_a_rename_with_edits_to_the_most_similar_file() {
        let mut r = Repo::new().await;
        let c1 = r
            .commit(&[], &[("a.txt", Some(TEXT)), ("b.txt", Some("one\ntwo\nx\ny\n"))])
            .await;
        let c2 = r
            .commit(
                &[c1],
                &[
                    ("a.txt", None),
                    ("b.txt", None),
                    ("c.txt", Some("one\ntwo\nthree\nFOUR\n")),
                ],
            )
            .await;
        let page = follow(&r, c2, "c.txt").await;
        assert_eq!(ids(&page), [c2]);
        assert_eq!(page.renamed_from.as_deref(), Some("a.txt"));
    }

    #[tokio::test]
    async fn a_dissimilar_or_in_place_change_is_not_a_rename() {
        let mut r = Repo::new().await;
        let c1 = r.commit(&[], &[("a.txt", Some(TEXT)), ("b.txt", Some("b\n"))]).await;
        // b.txt existed: modified in place although a.txt's content moved into it
        let c2 = r.commit(&[c1], &[("a.txt", None), ("b.txt", Some(TEXT))]).await;
        assert_eq!(follow(&r, c2, "b.txt").await.renamed_from, None);
        // Too different, too different in size, and added with nothing deleted
        let c3 = r
            .commit(&[c2], &[("b.txt", None), ("c.txt", Some("w\nx\ny\nz\n"))])
            .await;
        assert_eq!(follow(&r, c3, "c.txt").await.renamed_from, None);
        let c4 = r.commit(&[c3], &[("c.txt", None), ("d.txt", Some("w\n"))]).await;
        assert_eq!(follow(&r, c4, "d.txt").await.renamed_from, None);
        let c5 = r.commit(&[c4], &[("e.txt", Some("e"))]).await;
        assert_eq!(follow(&r, c5, "e.txt").await.renamed_from, None);
        // The root commit adds everything
        assert_eq!(follow(&r, c1, "a.txt").await.renamed_from, None);
    }

    #[tokio::test]
    async fn follows_a_copy_when_nothing_was_deleted_but_shows_no_merges() {
        let mut r = Repo::new().await;
        let c1 = r
            .commit(&[], &[("tmpl/base.go", Some(TEXT)), ("other", Some("o\n"))])
            .await;
        let c2 = r
            .commit(&[c1], &[("tmpl/base.go", Some("one\ntwo\nthree\nfour\nfive\n"))])
            .await;
        // Copied with an edit; the original stays
        let c3 = r
            .commit(&[c2], &[("cmd/new.go", Some("one\ntwo\nthree\nfour\nsix\n"))])
            .await;
        let page = follow(&r, c3, "cmd/new.go").await;
        assert_eq!(
            (ids(&page), page.renamed_from.as_deref()),
            (vec![c3], Some("tmpl/base.go"))
        );
        // A merge that changed the file is in the path log, but not when following
        let side = r
            .commit(&[c3], &[("cmd/new.go", Some("zero\none\ntwo\nthree\nfour\nsix\n"))])
            .await;
        let main = r
            .commit(&[c3], &[("cmd/new.go", Some("one\ntwo\nthree\nfour\nsix\nseven\n"))])
            .await;
        let m = r
            .commit(
                &[main, side],
                &[("cmd/new.go", Some("zero\none\ntwo\nthree\nfour\nsix\nseven\n"))],
            )
            .await;
        let page = follow(&r, m, "cmd/new.go").await;
        assert_eq!(page.entries.len(), 3);
        assert!(!ids(&page).contains(&m));
        let plain = log(&r.store, &r.store, REPO, &[m], Some("cmd/new.go"), 100, 1000)
            .await
            .expect("log");
        assert_eq!(ids(&plain)[0], m);
    }

    #[tokio::test]
    async fn follows_a_file_into_a_moved_directory() {
        let mut r = Repo::new().await;
        let c1 = r
            .commit(&[], &[("src/x/main.rs", Some(TEXT)), ("src/lib.rs", Some("l"))])
            .await;
        let c2 = r
            .commit(
                &[c1],
                &[
                    ("src/x/main.rs", None),
                    ("src/lib.rs", None),
                    ("crate/src/x/main.rs", Some("one\ntwo\nthree\nfour\n4\n")),
                    ("crate/src/lib.rs", Some("l")),
                ],
            )
            .await;
        let page = follow(&r, c2, "crate/src/x/main.rs").await;
        assert_eq!(page.renamed_from.as_deref(), Some("src/x/main.rs"));
    }

    #[tokio::test]
    async fn large_files_only_follow_identical_content() {
        let big = "line\n".repeat((MAX_COMPARED as usize / 5) + 1);
        let edited = format!("{big}more\n");
        let mut r = Repo::new().await;
        let c1 = r.commit(&[], &[("a", Some(&big)), ("b", Some(&big))]).await;
        let c2 = r.commit(&[c1], &[("a", None), ("a2", Some(&big))]).await;
        assert_eq!(follow(&r, c2, "a2").await.renamed_from.as_deref(), Some("a"));
        let c3 = r.commit(&[c2], &[("b", None), ("b2", Some(&edited))]).await;
        assert_eq!(follow(&r, c3, "b2").await.renamed_from, None);
    }

    #[test]
    fn similarity_counts_unchanged_bytes_over_the_larger_file() {
        assert_eq!(similarity(b"", b""), 1.0);
        assert_eq!(similarity(b"a\nb\n", b"a\nb\n"), 1.0);
        assert_eq!(similarity(b"a\nb\n", b"a\nc\n"), 0.5);
        assert_eq!(similarity(b"a\n", b"b\n"), 0.0);
        assert_eq!(similarity(b"a\nb\nc\nd\n", b"a\nb\n"), 0.5);
    }

    #[test]
    fn sizes_far_apart_cannot_match() {
        assert!(could_match(10, 20));
        assert!(could_match(20, 10));
        assert!(!could_match(9, 20));
        assert!(could_match(0, 0));
    }
}
