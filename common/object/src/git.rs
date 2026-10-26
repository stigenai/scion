//! git's commit object format, shared by the server (which computes git SHAs) and the importer
//! (which checks whether a git commit can be rebuilt from Scion's fields).

use crate::{ObjectId, Signature};

/// The header that names a commit's Scion id in its computed git object.
pub const SCION_ID_HEADER: &str = "scion-id";

/// `name <email> seconds ±hhmm`. git allows no `<`, `>` or newline in names and emails, so they
/// are dropped.
pub fn ident(s: &Signature) -> String {
    let clean = |v: &str| {
        v.chars()
            .filter(|c| !matches!(c, '<' | '>' | '\n'))
            .collect::<String>()
            .trim()
            .to_string()
    };
    let tz = s.tz_offset_minutes;
    let sign = if tz < 0 { '-' } else { '+' };
    let abs = tz.unsigned_abs();
    format!(
        "{} <{}> {} {sign}{:02}{:02}",
        clean(&s.name),
        clean(&s.email),
        s.time_ms.div_euclid(1000),
        abs / 60,
        abs % 60
    )
}

/// A git commit object built from these fields, with a `scion-id` header when `id` is given.
pub fn commit_body(
    id: Option<ObjectId>,
    tree: &[u8; 20],
    parents: &[[u8; 20]],
    author: &Signature,
    committer: &Signature,
    message: &str,
) -> Vec<u8> {
    let mut out = format!("tree {}\n", hex::encode(tree));
    for p in parents {
        out.push_str(&format!("parent {}\n", hex::encode(p)));
    }
    out.push_str(&format!("author {}\n", ident(author)));
    out.push_str(&format!("committer {}\n", ident(committer)));
    if let Some(id) = id {
        out.push_str(&format!("{SCION_ID_HEADER} {id}\n"));
    }
    out.push('\n');
    out.push_str(message);
    out.into_bytes()
}

/// A git commit object's fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommit {
    pub tree: [u8; 20],
    pub parents: Vec<[u8; 20]>,
    pub author: Signature,
    pub committer: Signature,
    /// The message; bytes that are not UTF-8 are replaced.
    pub message: String,
}

fn sha(s: &str) -> Option<[u8; 20]> {
    let mut out = [0u8; 20];
    (s.len() == 40).then_some(())?;
    hex::decode_to_slice(s, &mut out).ok().map(|()| out)
}

/// `Name <email> 1700000000 +0800`.
pub fn parse_ident(s: &str) -> Option<Signature> {
    let (name, rest) = s.split_once('<')?;
    let (email, rest) = rest.split_once('>')?;
    let mut it = rest.split_whitespace();
    let secs: i64 = it.next()?.parse().ok()?;
    let tz = it.next()?;
    let (sign, hhmm) = match tz.split_at_checked(1)? {
        ("+", d) => (1, d),
        ("-", d) => (-1, d),
        _ => return None,
    };
    if hhmm.len() != 4 || !hhmm.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hours: i32 = hhmm[..2].parse().ok()?;
    let mins: i32 = hhmm[2..].parse().ok()?;
    Some(Signature {
        name: name.trim().to_owned(),
        email: email.trim().to_owned(),
        time_ms: secs.checked_mul(1000)?,
        tz_offset_minutes: sign * (hours * 60 + mins),
    })
}

/// Parses a commit object; `None` when a required header is missing or malformed.
pub fn parse_commit(raw: &[u8]) -> Option<GitCommit> {
    let split = raw.windows(2).position(|w| w == b"\n\n");
    let (head, message) = match split {
        Some(i) => (&raw[..i], &raw[i + 2..]),
        None => (raw, &raw[raw.len()..]),
    };
    let head = std::str::from_utf8(head).ok()?;
    let (mut tree, mut author, mut committer) = (None, None, None);
    let mut parents = Vec::new();
    for line in head.lines() {
        match line.split_once(' ') {
            Some(("tree", v)) if tree.is_none() => tree = Some(sha(v)?),
            Some(("parent", v)) => parents.push(sha(v)?),
            Some(("author", v)) if author.is_none() => author = Some(parse_ident(v)?),
            Some(("committer", v)) if committer.is_none() => committer = Some(parse_ident(v)?),
            _ => {}
        }
    }
    Some(GitCommit {
        tree: tree?,
        parents,
        author: author?,
        committer: committer?,
        message: String::from_utf8_lossy(message).into_owned(),
    })
}

/// Whether [`commit_body`] gives back exactly this object from its parsed fields, so a Scion
/// commit needs no copy of it.
pub fn rebuilds(raw: &[u8], c: &GitCommit) -> bool {
    commit_body(None, &c.tree, &c.parents, &c.author, &c.committer, &c.message) == raw
}

/// The body of a git tag object naming commit `object` (an annotated tag, as `git tag -a` writes
/// it: the message ends in a newline).
pub fn tag_body(object: &[u8; 20], name: &str, tagger: &Signature, message: &str) -> Vec<u8> {
    let mut out = format!(
        "object {}\ntype commit\ntag {name}\ntagger {}\n\n{message}",
        hex::encode(object),
        ident(tagger)
    );
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.into_bytes()
}

/// A git tag object's fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitTag {
    pub object: [u8; 20],
    /// What it tags: `commit`, `tree`, `blob` or `tag`.
    pub kind: String,
    pub name: String,
    /// Old tags may have none.
    pub tagger: Option<Signature>,
    /// The message, a signature included; bytes that are not UTF-8 are replaced.
    pub message: String,
}

/// Parses a tag object; `None` when `object`, `type` or `tag` is missing or malformed.
pub fn parse_tag(raw: &[u8]) -> Option<GitTag> {
    let split = raw.windows(2).position(|w| w == b"\n\n");
    let (head, message) = match split {
        Some(i) => (&raw[..i], &raw[i + 2..]),
        None => (raw, &raw[raw.len()..]),
    };
    let head = std::str::from_utf8(head).ok()?;
    let (mut object, mut kind, mut name, mut tagger) = (None, None, None, None);
    for line in head.lines() {
        match line.split_once(' ') {
            Some(("object", v)) if object.is_none() => object = Some(sha(v)?),
            Some(("type", v)) if kind.is_none() => kind = Some(v.to_owned()),
            Some(("tag", v)) if name.is_none() => name = Some(v.to_owned()),
            Some(("tagger", v)) if tagger.is_none() => tagger = Some(parse_ident(v)?),
            _ => {}
        }
    }
    Some(GitTag {
        object: object?,
        kind: kind?,
        name: name?,
        tagger,
        message: String::from_utf8_lossy(message).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_objects_round_trip() {
        let tagger = sig("Ada", 1_700_000_000_000, 480);
        let body = tag_body(&[7; 20], "v1.0", &tagger, "Release 1.0");
        assert_eq!(
            String::from_utf8_lossy(&body),
            format!(
                "object {}\ntype commit\ntag v1.0\ntagger {}\n\nRelease 1.0\n",
                "07".repeat(20),
                ident(&tagger)
            )
        );
        let t = parse_tag(&body).expect("parse");
        assert_eq!(
            (t.object, t.kind.as_str(), t.name.as_str(), t.tagger, t.message.as_str()),
            ([7; 20], "commit", "v1.0", Some(tagger.clone()), "Release 1.0\n")
        );
        assert_eq!(tag_body(&[7; 20], "v1.0", &tagger, "Release 1.0\n"), body);
        let old = format!("object {}\ntype tree\ntag t", "07".repeat(20));
        let t = parse_tag(old.as_bytes()).expect("no tagger, no message");
        assert_eq!((t.kind.as_str(), t.tagger, t.message.as_str()), ("tree", None, ""));
        assert!(parse_tag(b"type commit\ntag x\n\nm").is_none());
        assert!(parse_tag(b"object zz\ntype commit\ntag x\n").is_none());
    }

    fn sig(name: &str, time_ms: i64, tz: i32) -> Signature {
        Signature {
            name: name.into(),
            email: "e@x".into(),
            time_ms,
            tz_offset_minutes: tz,
        }
    }

    #[test]
    fn formats_identities_and_commits() {
        assert_eq!(
            ident(&sig(" A <B>\n", 1_700_000_000_999, 480)),
            "A B <e@x> 1700000000 +0800"
        );
        // Before 1970, seconds round down
        assert_eq!(ident(&sig("B", -1, -90)), "B <e@x> -1 -0130");
        let body = commit_body(
            None,
            &[0xaa; 20],
            &[[0x11; 20]],
            &sig("A", 0, 0),
            &sig("C", 0, 0),
            "m\n",
        );
        let want = format!(
            "tree {}\nparent {}\nauthor A <e@x> 0 +0000\ncommitter C <e@x> 0 +0000\n\nm\n",
            "aa".repeat(20),
            "11".repeat(20)
        );
        assert_eq!(String::from_utf8(body).expect("utf8"), want);
        let id = ObjectId::of_blob(b"x");
        let body = commit_body(Some(id), &[0; 20], &[], &sig("A", 0, 0), &sig("A", 0, 0), "");
        assert!(
            String::from_utf8(body)
                .expect("utf8")
                .contains(&format!("\nscion-id {id}\n\n"))
        );
    }

    #[test]
    fn parses_commits() {
        let raw = format!(
            "tree {}\nparent {}\nparent {}\nauthor A <a@x> 1700000000 +0800\n\
             committer C <c@x> 1700000001 -0130\ngpgsig x\n y\n\nmsg\n",
            "ab".repeat(20),
            "cd".repeat(20),
            "ef".repeat(20)
        );
        let c = parse_commit(raw.as_bytes()).expect("parse");
        assert_eq!(c.tree, [0xab; 20]);
        assert_eq!(c.parents, [[0xcd; 20], [0xef; 20]]);
        assert_eq!(c.author.time_ms, 1_700_000_000_000);
        assert_eq!(c.committer.tz_offset_minutes, -90);
        assert_eq!(c.message, "msg\n");
        assert!(!rebuilds(raw.as_bytes(), &c));
        let plain = raw.replace("gpgsig x\n y\n", "");
        assert!(rebuilds(
            plain.as_bytes(),
            &parse_commit(plain.as_bytes()).expect("parse")
        ));
        // Missing or malformed headers
        for bad in [
            raw.replace("tree ", "tree x"),
            raw.replace(&format!("parent {}", "cd".repeat(20)), "parent zz"),
            raw.replace("author A <a@x> 1700000000 +0800\n", ""),
            raw.replace("+0800", "0800"),
            raw.replace("-0130", "-01x0"),
            raw.replace("1700000001", "soon"),
        ] {
            assert_eq!(parse_commit(bad.as_bytes()), None, "{bad}");
        }
        // No message at all
        let bare = format!(
            "tree {}\nauthor A <a@x> 1 +0000\ncommitter A <a@x> 1 +0000",
            "ab".repeat(20)
        );
        assert_eq!(parse_commit(bare.as_bytes()).expect("parse").message, "");
        assert_eq!(parse_ident("A <a@x> 1 *0000"), None);
    }
}
