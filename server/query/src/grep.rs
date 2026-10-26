//! Searching file contents at a revision (`scn grep`): the files under the paths asked for are
//! read in parallel and matched line by line, in path order, as `git grep` prints them.

use std::future::Future;
use std::pin::Pin;

use futures::StreamExt;
use regex::bytes::{Regex, RegexBuilder};
use scion_object::{EntryMode, ObjectId, Tree};
use scion_store::BlobStore;

use crate::{QueryError, Result};

/// Files read at once.
const CONCURRENCY: usize = 16;
/// git calls a file binary when its first 8000 bytes hold a NUL.
const BINARY_PROBE: usize = 8000;
/// Longer lines are cut to this many bytes in a match.
pub const MAX_LINE: usize = 1000;

/// What to look for and how much of it.
#[derive(Debug, Clone)]
pub struct GrepOptions {
    pattern: Regex,
    /// Stop after this many matching lines.
    pub max_matches: usize,
    /// Files larger than this are skipped.
    pub max_file_size: u64,
}

impl GrepOptions {
    /// `pattern` is a regular expression (Rust's `regex` syntax, close to git's `-E`), or with
    /// `fixed` a literal string. Refuses a pattern that does not compile, or matches empty lines,
    /// which would match every line.
    pub fn new(
        pattern: &str,
        fixed: bool,
        ignore_case: bool,
        max_matches: usize,
        max_file_size: u64,
    ) -> std::result::Result<Self, String> {
        let source = if fixed {
            regex::escape(pattern)
        } else {
            pattern.to_owned()
        };
        let pattern = RegexBuilder::new(&source)
            .case_insensitive(ignore_case)
            .build()
            .map_err(|e| e.to_string())?;
        if pattern.is_match(b"") {
            return Err("the pattern matches an empty line, so every line".into());
        }
        Ok(Self {
            pattern,
            max_matches,
            max_file_size,
        })
    }
}

/// A matching line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepMatch {
    pub path: String,
    /// 1-based.
    pub line: u64,
    /// The line without its end, cut to [`MAX_LINE`] bytes.
    pub text: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrepResult {
    pub matches: Vec<GrepMatch>,
    /// It stopped at `max_matches`: there may be more.
    pub truncated: bool,
    /// Files searched, and files left out (binary, too large, redacted, or not regular files).
    pub searched: u64,
    pub skipped: u64,
}

/// A place to search: a directory (`mode` Tree) or one file, by its repository path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepStart {
    pub path: String,
    pub mode: EntryMode,
    pub id: ObjectId,
    pub size: u64,
}

/// Searches the files under `starts`, in order, for `opts`' pattern.
pub async fn grep(blobs: &dyn BlobStore, starts: &[GrepStart], opts: &GrepOptions) -> Result<GrepResult> {
    let mut files = Vec::new();
    let mut out = GrepResult::default();
    for s in starts {
        if s.mode == EntryMode::Tree {
            let base = if s.path.is_empty() {
                String::new()
            } else {
                format!("{}/", s.path)
            };
            collect(blobs, s.id, base, &mut files).await?;
        } else {
            files.push(s.clone());
        }
    }
    let wanted: Vec<GrepStart> = files
        .into_iter()
        .filter(|f| {
            let keep = matches!(f.mode, EntryMode::File | EntryMode::Executable) && f.size <= opts.max_file_size;
            if !keep {
                out.skipped += 1;
            }
            keep
        })
        .collect();
    let mut contents = futures::stream::iter(wanted)
        .map(|f| async move {
            let data = blobs.get(&f.id).await;
            (f, data)
        })
        .buffered(CONCURRENCY);
    while let Some((f, data)) = contents.next().await {
        let data = match data {
            Ok(Some(d)) => d,
            Ok(None) => return Err(QueryError::MissingObject(f.id)),
            Err(scion_store::Error::Redacted { .. }) => {
                out.skipped += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        if memchr::memchr(0, &data[..data.len().min(BINARY_PROBE)]).is_some() {
            out.skipped += 1;
            continue;
        }
        out.searched += 1;
        for (i, line) in data.split(|b| *b == b'\n').enumerate() {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if !opts.pattern.is_match(line) {
                continue;
            }
            if out.matches.len() == opts.max_matches {
                out.truncated = true;
                return Ok(out);
            }
            out.matches.push(GrepMatch {
                path: f.path.clone(),
                line: i as u64 + 1,
                text: line[..line.len().min(MAX_LINE)].to_vec(),
            });
        }
    }
    Ok(out)
}

/// The entries under directory `tree` (at repository path `base`, ending in `/` unless empty),
/// depth first in name order.
fn collect<'a>(
    blobs: &'a dyn BlobStore,
    tree: ObjectId,
    base: String,
    out: &'a mut Vec<GrepStart>,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let bytes = blobs.get(&tree).await?.ok_or(QueryError::MissingObject(tree))?;
        let t = Tree::decode(&bytes).map_err(|e| QueryError::Corrupt(tree, e.to_string()))?;
        for e in t.entries() {
            let path = format!("{base}{}", e.name);
            if e.mode == EntryMode::Tree {
                collect(blobs, e.id, format!("{path}/"), out).await?;
            } else {
                out.push(GrepStart {
                    path,
                    mode: e.mode,
                    id: e.id,
                    size: e.size,
                });
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_object::TreeEntry;
    use scion_store::MemStore;

    async fn blob(s: &MemStore, data: &[u8]) -> (ObjectId, u64) {
        let id = ObjectId::of_blob(data);
        s.put(id, Bytes::copy_from_slice(data)).await.expect("put");
        (id, data.len() as u64)
    }

    async fn tree(s: &MemStore, entries: Vec<TreeEntry>) -> ObjectId {
        let t = Tree::new(entries).expect("tree");
        let bytes = t.encode();
        let id = ObjectId::hash(scion_object::ObjectKind::Tree, &bytes);
        s.put(id, bytes.into()).await.expect("put");
        id
    }

    fn entry(name: &str, mode: EntryMode, (id, size): (ObjectId, u64)) -> TreeEntry {
        TreeEntry {
            name: name.into(),
            mode,
            id,
            size,
        }
    }

    fn opts(p: &str, max: usize) -> GrepOptions {
        GrepOptions::new(p, false, false, max, 1 << 20).expect("pattern")
    }

    #[tokio::test]
    async fn finds_lines_in_path_order_and_skips_what_it_cannot_read() {
        let s = MemStore::new();
        let a = blob(&s, b"fn main() {\r\n    todo!()\n}\n").await;
        let b = blob(&s, b"// TODO: later\nfn b() {}\n").await;
        let bin = blob(&s, b"todo\0binary").await;
        let big = blob(&s, &[b'x'; 64]).await;
        let link = blob(&s, b"a.rs").await;
        let sub = tree(&s, vec![entry("b.rs", EntryMode::File, b)]).await;
        let root = tree(
            &s,
            vec![
                entry("a.rs", EntryMode::File, a),
                entry("big.txt", EntryMode::File, big),
                entry("bin.dat", EntryMode::File, bin),
                entry("link", EntryMode::Symlink, link),
                entry("sub", EntryMode::Tree, (sub, 0)),
            ],
        )
        .await;
        let all = GrepStart {
            path: String::new(),
            mode: EntryMode::Tree,
            id: root,
            size: 0,
        };
        let o = GrepOptions::new("todo|fn", false, true, 100, 32).expect("pattern");
        let r = grep(&s, std::slice::from_ref(&all), &o).await.expect("grep");
        let found: Vec<(&str, u64, &[u8])> = r
            .matches
            .iter()
            .map(|m| (m.path.as_str(), m.line, m.text.as_slice()))
            .collect();
        assert_eq!(
            found,
            [
                ("a.rs", 1, &b"fn main() {"[..]),
                ("a.rs", 2, b"    todo!()"),
                ("sub/b.rs", 1, b"// TODO: later"),
                ("sub/b.rs", 2, b"fn b() {}"),
            ]
        );
        assert_eq!((r.searched, r.skipped, r.truncated), (2, 3, false));

        // A cap, and a start that is a directory or a single file
        let r = grep(&s, std::slice::from_ref(&all), &opts("fn", 1))
            .await
            .expect("grep");
        assert_eq!((r.matches.len(), r.truncated), (1, true));
        let starts = [
            GrepStart {
                path: "sub".into(),
                mode: EntryMode::Tree,
                id: sub,
                size: 0,
            },
            GrepStart {
                path: "a.rs".into(),
                mode: EntryMode::File,
                id: a.0,
                size: a.1,
            },
        ];
        let r = grep(&s, &starts, &opts("^fn", 10)).await.expect("grep");
        let paths: Vec<&str> = r.matches.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(paths, ["sub/b.rs", "a.rs"]);

        // Literal strings, long lines cut, and patterns that cannot work
        let long = blob(&s, format!("{}a.b", "y".repeat(2000)).as_bytes()).await;
        let one = GrepStart {
            path: "long.txt".into(),
            mode: EntryMode::File,
            id: long.0,
            size: long.1,
        };
        let fixed = GrepOptions::new("a.b", true, false, 10, 1 << 20).expect("fixed");
        let r = grep(&s, std::slice::from_ref(&one), &fixed).await.expect("grep");
        assert_eq!(r.matches[0].text.len(), MAX_LINE);
        assert!(
            grep(&s, &[one], &opts("a\\.c", 10))
                .await
                .expect("grep")
                .matches
                .is_empty()
        );
        assert!(GrepOptions::new("(", false, false, 1, 1).is_err());
        assert!(
            GrepOptions::new("x*", false, false, 1, 1)
                .expect_err("matches empty lines")
                .contains("empty line")
        );
        let missing = GrepStart {
            path: "gone".into(),
            mode: EntryMode::File,
            id: ObjectId::of_blob(b"never stored"),
            size: 1,
        };
        assert!(matches!(
            grep(&s, &[missing], &opts("x", 1)).await,
            Err(QueryError::MissingObject(_))
        ));
    }
}
