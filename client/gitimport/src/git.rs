//! 通过 git 命令行读取仓库：提交列表、提交元数据、相对第一父提交的改动、对象内容。

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use crate::{Error, Result};

/// git 中的签名：`Name <email> 1700000000 +0800`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ident {
    pub name: String,
    pub email: String,
    pub time_ms: i64,
    pub tz_offset_minutes: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// The tree header: the git SHA of the commit's root directory.
    pub tree: String,
    pub parents: Vec<String>,
    pub author: Ident,
    pub committer: Ident,
    pub message: String,
    /// The commit object as git wrote it.
    pub raw: Vec<u8>,
}

/// 文件类型（git mode）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitMode {
    File,
    Executable,
    Symlink,
    /// 子模块（gitlink），Scion 目前不支持，导入时跳过。
    Gitlink,
}

impl GitMode {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "100644" | "100664" => Ok(GitMode::File),
            "100755" => Ok(GitMode::Executable),
            "120000" => Ok(GitMode::Symlink),
            "160000" => Ok(GitMode::Gitlink),
            other => Err(Error::Parse(format!("unsupported git mode {other}"))),
        }
    }
}

/// 一条改动：`new = None` 表示删除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub path: String,
    pub new: Option<(GitMode, String)>,
}

pub struct Repo {
    dir: PathBuf,
}

impl Repo {
    pub fn open(dir: &Path) -> Result<Self> {
        let repo = Self { dir: dir.to_path_buf() };
        repo.run(&["rev-parse", "--git-dir"])?;
        Ok(repo)
    }

    fn command(&self) -> Command {
        let mut c = Command::new("git");
        c.arg("-C").arg(&self.dir);
        c
    }

    fn run(&self, args: &[&str]) -> Result<Vec<u8>> {
        let out = self
            .command()
            .args(args)
            .output()
            .map_err(|e| Error::Git(format!("cannot run git: {e}")))?;
        if !out.status.success() {
            return Err(Error::Git(format!(
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(out.stdout)
    }

    /// The object `git_ref` names, when it is a tag object (an annotated tag): raw.
    pub fn tag_object(&self, cat: &mut CatFile, git_ref: &str) -> Result<Option<Vec<u8>>> {
        let sha = String::from_utf8_lossy(&self.run(&["rev-parse", git_ref])?)
            .trim()
            .to_owned();
        match cat.read(&sha)? {
            (k, data) if k == "tag" => Ok(Some(data)),
            _ => Ok(None),
        }
    }

    /// `git_ref` 可达的全部提交，父提交在前（拓扑序）。
    pub fn commits(&self, git_ref: &str) -> Result<Vec<String>> {
        let out = self.run(&["rev-list", "--reverse", "--topo-order", git_ref, "--"])?;
        Ok(String::from_utf8_lossy(&out).lines().map(str::to_owned).collect())
    }

    /// Like [`Repo::commits`], without the commits reachable from any of `known` (and `known`).
    pub fn commits_excluding(&self, git_ref: &str, known: &[String]) -> Result<Vec<String>> {
        let mut args = vec!["rev-list", "--reverse", "--topo-order", git_ref];
        if !known.is_empty() {
            args.push("--not");
            args.extend(known.iter().map(String::as_str));
        }
        args.push("--");
        let out = self.run(&args)?;
        Ok(String::from_utf8_lossy(&out).lines().map(str::to_owned).collect())
    }

    /// 相对第一父提交的改动；根提交相对空树。不检测改名（改名即删除加新增）。
    pub fn changes(&self, commit: &str, first_parent: Option<&str>) -> Result<Vec<Change>> {
        let out = match first_parent {
            Some(p) => self.run(&[
                "diff-tree",
                "-r",
                "-z",
                "--raw",
                "--no-renames",
                "--no-commit-id",
                p,
                commit,
            ])?,
            None => self.run(&[
                "diff-tree",
                "-r",
                "-z",
                "--raw",
                "--no-renames",
                "--no-commit-id",
                "--root",
                commit,
            ])?,
        };
        parse_raw_diff(&out)
    }

    /// 启动 `git cat-file --batch`，按 sha 逐个读取对象。
    pub fn cat_file(&self) -> Result<CatFile> {
        let mut child = self
            .command()
            .args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| Error::Git(format!("cannot run git cat-file: {e}")))?;
        let stdin = child.stdin.take().ok_or_else(|| Error::Git("cat-file stdin".into()))?;
        let stdout = BufReader::new(
            child
                .stdout
                .take()
                .ok_or_else(|| Error::Git("cat-file stdout".into()))?,
        );
        Ok(CatFile { child, stdin, stdout })
    }
}

/// 长驻的 `git cat-file --batch` 进程。
pub struct CatFile {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl CatFile {
    /// 读取对象，返回 (类型, 内容)。
    pub fn read(&mut self, sha: &str) -> Result<(String, Vec<u8>)> {
        writeln!(self.stdin, "{sha}")
            .and_then(|()| self.stdin.flush())
            .map_err(|e| Error::Git(format!("cat-file write: {e}")))?;
        let mut header = String::new();
        self.stdout
            .read_line(&mut header)
            .map_err(|e| Error::Git(format!("cat-file read: {e}")))?;
        let parts: Vec<&str> = header.split_whitespace().collect();
        let (kind, size) = match parts.as_slice() {
            [_, "missing"] => return Err(Error::Git(format!("object {sha} is missing"))),
            [_, kind, size] => (
                kind.to_string(),
                size.parse::<usize>()
                    .map_err(|_| Error::Parse(format!("bad cat-file header {header:?}")))?,
            ),
            _ => return Err(Error::Parse(format!("bad cat-file header {header:?}"))),
        };
        let mut data = vec![0u8; size + 1]; // 内容后跟一个换行
        self.stdout
            .read_exact(&mut data)
            .map_err(|e| Error::Git(format!("cat-file read: {e}")))?;
        data.truncate(size);
        Ok((kind, data))
    }

    pub fn commit(&mut self, sha: &str) -> Result<Commit> {
        match self.read(sha)? {
            (k, data) if k == "commit" => parse_commit(&data),
            (k, _) => Err(Error::Parse(format!("{sha} is a {k}, not a commit"))),
        }
    }

    pub fn blob(&mut self, sha: &str) -> Result<Vec<u8>> {
        match self.read(sha)? {
            (k, data) if k == "blob" => Ok(data),
            (k, _) => Err(Error::Parse(format!("{sha} is a {k}, not a blob"))),
        }
    }
}

impl Drop for CatFile {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 解析 `diff-tree -r -z --raw` 输出：`:old new oldsha newsha STATUS\0path\0` 重复。
pub fn parse_raw_diff(out: &[u8]) -> Result<Vec<Change>> {
    let mut changes = Vec::new();
    let mut fields = out.split(|b| *b == 0).filter(|f| !f.is_empty());
    while let Some(meta) = fields.next() {
        let meta = std::str::from_utf8(meta).map_err(|_| Error::Parse("non UTF-8 diff header".into()))?;
        let path = fields
            .next()
            .ok_or_else(|| Error::Parse(format!("diff entry without path: {meta}")))?;
        let path = String::from_utf8(path.to_vec())
            .map_err(|e| Error::NonUtf8Path(String::from_utf8_lossy(e.as_bytes()).into_owned()))?;
        let parts: Vec<&str> = meta.trim_start_matches(':').split(' ').collect();
        let [old_mode, new_mode, _, new_sha, status] = parts.as_slice() else {
            return Err(Error::Parse(format!("bad diff header {meta:?}")));
        };
        // Submodules are not imported: removing one changes nothing, and a file that becomes one
        // is removed
        let was_file = !matches!(*old_mode, "000000" | "160000");
        let new = if status.starts_with('D') {
            if !was_file {
                continue;
            }
            None
        } else {
            match GitMode::parse(new_mode)? {
                GitMode::Gitlink if was_file => None,
                mode => Some((mode, new_sha.to_string())),
            }
        };
        changes.push(Change { path, new });
    }
    Ok(changes)
}

/// 解析提交对象。
pub fn parse_commit(data: &[u8]) -> Result<Commit> {
    let text = String::from_utf8_lossy(data);
    let (headers, message) = text.split_once("\n\n").unwrap_or((&text, ""));
    let mut parents = Vec::new();
    let mut tree = String::new();
    let (mut author, mut committer) = (None, None);
    for line in headers.lines() {
        // 以空格开头的是上一个头部的续行（如 gpgsig），忽略
        if line.starts_with(' ') {
            continue;
        }
        match line.split_once(' ') {
            Some(("tree", sha)) => tree = sha.to_owned(),
            Some(("parent", sha)) => parents.push(sha.to_owned()),
            Some(("author", v)) => author = Some(parse_ident(v)?),
            Some(("committer", v)) => committer = Some(parse_ident(v)?),
            _ => {}
        }
    }
    Ok(Commit {
        tree,
        parents,
        author: author.ok_or_else(|| Error::Parse("commit without author".into()))?,
        committer: committer.ok_or_else(|| Error::Parse("commit without committer".into()))?,
        message: message.to_owned(),
        raw: data.to_vec(),
    })
}

/// 解析 `Name <email> 1700000000 +0800`。
pub fn parse_ident(s: &str) -> Result<Ident> {
    let bad = || Error::Parse(format!("bad identity {s:?}"));
    let (name, rest) = s.split_once('<').ok_or_else(bad)?;
    let (email, rest) = rest.split_once('>').ok_or_else(bad)?;
    let mut it = rest.split_whitespace();
    let secs: i64 = it.next().and_then(|t| t.parse().ok()).ok_or_else(bad)?;
    let tz = it.next().ok_or_else(bad)?;
    let (sign, hhmm) = match tz.split_at_checked(1) {
        Some(("+", d)) => (1, d),
        Some(("-", d)) => (-1, d),
        _ => return Err(bad()),
    };
    if hhmm.len() != 4 || !hhmm.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let hours: i32 = hhmm[..2].parse().map_err(|_| bad())?;
    let mins: i32 = hhmm[2..].parse().map_err(|_| bad())?;
    Ok(Ident {
        name: name.trim().to_owned(),
        email: email.trim().to_owned(),
        time_ms: secs.saturating_mul(1000),
        tz_offset_minutes: sign * (hours * 60 + mins),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ident_cases() {
        let i = parse_ident("Ada Lovelace <ada@example.com> 1700000000 +0800").expect("ok");
        assert_eq!((i.name.as_str(), i.email.as_str()), ("Ada Lovelace", "ada@example.com"));
        assert_eq!((i.time_ms, i.tz_offset_minutes), (1_700_000_000_000, 480));
        assert_eq!(parse_ident("A <a@b> 0 -0530").expect("ok").tz_offset_minutes, -330);
        assert_eq!(parse_ident(" <> 1 +0000").expect("empty name ok").name, "");
        for bad in [
            "A a@b 1 +0000",
            "A <a@b 1 +0000",
            "A <a@b> x +0000",
            "A <a@b> 1",
            "A <a@b> 1 0800",
            "A <a@b> 1 +08",
            "A <a@b> 1 +08x0",
        ] {
            assert!(matches!(parse_ident(bad), Err(Error::Parse(_))), "{bad:?}");
        }
    }

    #[test]
    fn parse_commit_with_signature_and_merge() {
        let raw = b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n\
parent 1111111111111111111111111111111111111111\n\
parent 2222222222222222222222222222222222222222\n\
author A <a@x> 10 +0000\n\
committer C <c@x> 20 -0100\n\
gpgsig -----BEGIN PGP SIGNATURE-----\n \n abc\n -----END PGP SIGNATURE-----\n\
\n\
subject\n\nbody\n";
        let c = parse_commit(raw).expect("ok");
        assert_eq!(c.parents.len(), 2);
        assert_eq!(c.author.name, "A");
        assert_eq!(c.committer.tz_offset_minutes, -60);
        assert_eq!(c.message, "subject\n\nbody\n");
    }

    #[test]
    fn parse_commit_missing_fields() {
        assert!(matches!(
            parse_commit(b"tree x\ncommitter C <c@x> 1 +0000\n\nm"),
            Err(Error::Parse(_))
        ));
        assert!(matches!(
            parse_commit(b"tree x\nauthor A <a@x> 1 +0000\n\nm"),
            Err(Error::Parse(_))
        ));
        let c = parse_commit(b"tree x\nauthor A <a@x> 1 +0000\ncommitter C <c@x> 1 +0000").expect("no message");
        assert_eq!(c.message, "");
    }

    #[test]
    fn parse_raw_diff_cases() {
        let out = b":000000 100644 0000 aaaa A\0new.txt\0:100644 000000 bbbb 0000 D\0old.txt\0:100644 100755 cccc dddd M\0run.sh\0:100644 120000 eeee ffff T\0link\0";
        let c = parse_raw_diff(out).expect("ok");
        assert_eq!(c.len(), 4);
        assert_eq!(
            c[0],
            Change {
                path: "new.txt".into(),
                new: Some((GitMode::File, "aaaa".into()))
            }
        );
        assert_eq!(
            c[1],
            Change {
                path: "old.txt".into(),
                new: None
            }
        );
        assert_eq!(c[2].new, Some((GitMode::Executable, "dddd".into())));
        assert_eq!(c[3].new, Some((GitMode::Symlink, "ffff".into())));
        assert!(parse_raw_diff(b"").expect("empty").is_empty());
    }

    #[test]
    fn parse_raw_diff_submodules() {
        // Added and removed submodules: the addition is skipped by the importer, the removal here
        let out = b":000000 160000 0000 aaaa A\0sub\0:160000 000000 aaaa 0000 D\0gone\0";
        let c = parse_raw_diff(out).expect("ok");
        assert_eq!(
            c,
            vec![Change {
                path: "sub".into(),
                new: Some((GitMode::Gitlink, "aaaa".into()))
            }]
        );
        // A file that becomes a submodule is removed; a submodule that becomes a file is added
        let out = b":100644 160000 bbbb aaaa T\0was-file\0:160000 100644 aaaa bbbb T\0now-file\0";
        let c = parse_raw_diff(out).expect("ok");
        assert_eq!(
            c[0],
            Change {
                path: "was-file".into(),
                new: None
            }
        );
        assert_eq!(c[1].new, Some((GitMode::File, "bbbb".into())));
        // A submodule moved to another commit stays skipped
        let c = parse_raw_diff(b":160000 160000 aaaa cccc M\0sub\0").expect("ok");
        assert_eq!(c[0].new, Some((GitMode::Gitlink, "cccc".into())));
    }

    #[test]
    fn parse_raw_diff_errors() {
        assert!(
            matches!(parse_raw_diff(b":000000 100644 0 a A\0"), Err(Error::Parse(_))),
            "missing path"
        );
        assert!(
            matches!(parse_raw_diff(b":bad\0p\0"), Err(Error::Parse(_))),
            "short header"
        );
        assert!(
            matches!(parse_raw_diff(b":0 040000 0 a A\0p\0"), Err(Error::Parse(_))),
            "tree mode"
        );
        assert!(matches!(
            parse_raw_diff(b":0 100644 0 a A\0\xff\0"),
            Err(Error::NonUtf8Path(_))
        ));
        assert!(matches!(parse_raw_diff(b"\xff\0p\0"), Err(Error::Parse(_))));
    }

    #[test]
    fn git_mode_parse() {
        assert_eq!(GitMode::parse("100664").expect("legacy"), GitMode::File);
        assert_eq!(GitMode::parse("160000").expect("gitlink"), GitMode::Gitlink);
        assert!(GitMode::parse("040000").is_err());
    }

    #[test]
    fn open_non_repo_fails() {
        let d = tempfile::tempdir().expect("tempdir");
        assert!(matches!(Repo::open(d.path()), Err(Error::Git(_))));
    }
}
