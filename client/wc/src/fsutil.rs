//! 工作区文件系统操作。

use std::collections::{BTreeMap, HashSet};
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::error::{Error, Result, io};
use crate::state::{DIR, Mode};

pub(crate) fn now_ns() -> u64 {
    to_ns(SystemTime::now())
}

fn to_ns(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub(crate) fn mtime_ns(meta: &Metadata) -> u64 {
    meta.modified().map(to_ns).unwrap_or(0)
}

/// 工作区中一个文件的类型与元数据（符号链接取链接本身的元数据）。
#[derive(Debug)]
pub(crate) struct Found {
    pub mode: Mode,
    pub size: u64,
    pub mtime_ns: u64,
}

fn mode_of(meta: &Metadata) -> Option<Mode> {
    let ft = meta.file_type();
    if ft.is_symlink() {
        return Some(Mode::Symlink);
    }
    if !ft.is_file() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o111 != 0 {
            return Some(Mode::Executable);
        }
    }
    Some(Mode::File)
}

/// Ignore-rule files read in every directory. Both use gitignore syntax; when both exist in one
/// directory, `.scionignore` is read last and wins.
pub const IGNORE_FILES: [&str; 2] = [".gitignore", ".scionignore"];

/// Ignore rules of one directory (`None` when it has no ignore files). `has` says whether the
/// directory lists a name, so directories without ignore files cost no extra lookup.
fn dir_rules(dir: &Path, has: impl Fn(&str) -> bool) -> Option<Arc<Gitignore>> {
    let mut b = GitignoreBuilder::new(dir);
    let mut any = false;
    for name in IGNORE_FILES {
        let f = dir.join(name);
        if has(name) && f.is_file() {
            // Lines that fail to parse are skipped, like git does
            let _ = b.add(&f);
            any = true;
        }
    }
    if !any {
        return None;
    }
    b.build().ok().map(Arc::new)
}

/// Whether `path` is ignored: the deepest directory with a matching rule decides, as in git.
fn is_ignored(rules: &[Arc<Gitignore>], path: &Path, is_dir: bool) -> bool {
    for r in rules.iter().rev() {
        match r.matched(path, is_dir) {
            ignore::Match::Ignore(_) => return true,
            ignore::Match::Whitelist(_) => return false,
            ignore::Match::None => {}
        }
    }
    false
}

/// Whether any tracked path lies under directory `dir` (relative, `/`-separated).
fn tracks_under<R>(tracked: &BTreeMap<String, R>, dir: &str) -> bool {
    let prefix = format!("{dir}/");
    tracked
        .range(prefix.clone()..)
        .next()
        .is_some_and(|(k, _)| k.starts_with(&prefix))
}

/// A directory still to scan.
struct Dir {
    path: PathBuf,
    prefix: String,
    rules: Vec<Arc<Gitignore>>,
    /// Inside an ignored directory that still holds tracked files: only those are returned.
    ignored: bool,
}

/// Threads scanning a working tree: its cost is one `lstat` per file, which threads overlap.
fn scan_threads() -> usize {
    std::thread::available_parallelism().map_or(4, |n| n.get()).clamp(1, 8)
}

/// Scans the working tree and returns `relative path (/-separated) → file info`.
///
/// Skips `.scion/`, `.git/`, directories themselves and special files, and untracked paths matched
/// by `.gitignore` / `.scionignore`. Tracked files (keys of `tracked`) are always returned, even
/// when an ignore rule matches them, so ignoring a tracked file never looks like deleting it.
///
/// Directories are scanned by several threads at once.
pub(crate) fn scan<R: Sync>(root: &Path, tracked: &BTreeMap<String, R>) -> Result<BTreeMap<String, Found>> {
    scan_with(root, tracked, scan_threads())
}

fn scan_with<R: Sync>(root: &Path, tracked: &BTreeMap<String, R>, threads: usize) -> Result<BTreeMap<String, Found>> {
    use std::sync::{Condvar, Mutex, MutexGuard};
    fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    struct Queue {
        dirs: Vec<Dir>,
        /// Directories taken and not finished: more may be pushed while any is
        busy: usize,
        failed: Option<Error>,
    }
    let queue = Mutex::new(Queue {
        dirs: vec![Dir {
            path: root.to_path_buf(),
            prefix: String::new(),
            rules: Vec::new(),
            ignored: false,
        }],
        busy: 0,
        failed: None,
    });
    let ready = Condvar::new();
    let worker = || {
        let mut found = Vec::new();
        loop {
            let dir = {
                let mut q = lock(&queue);
                loop {
                    if q.failed.is_some() {
                        return found;
                    }
                    if let Some(d) = q.dirs.pop() {
                        q.busy += 1;
                        break d;
                    }
                    if q.busy == 0 {
                        return found;
                    }
                    q = ready.wait(q).unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            };
            let mut subdirs = Vec::new();
            let result = scan_dir(dir, tracked, &mut found, &mut subdirs);
            let mut q = lock(&queue);
            q.busy -= 1;
            match result {
                Ok(()) => q.dirs.append(&mut subdirs),
                Err(e) => {
                    q.failed.get_or_insert(e);
                }
            }
            drop(q);
            ready.notify_all();
        }
    };
    let parts: Vec<Vec<(String, Found)>> = std::thread::scope(|s| {
        let handles: Vec<_> = (1..threads.max(1)).map(|_| s.spawn(worker)).collect();
        let mut parts = vec![worker()];
        parts.extend(handles.into_iter().map(|h| h.join().unwrap_or_default()));
        parts
    });
    if let Some(e) = lock(&queue).failed.take() {
        return Err(e);
    }
    Ok(parts.into_iter().flatten().collect())
}

/// Scans one directory: its files go to `found`, its subdirectories to `subdirs`.
fn scan_dir<R>(
    dir: Dir,
    tracked: &BTreeMap<String, R>,
    found: &mut Vec<(String, Found)>,
    subdirs: &mut Vec<Dir>,
) -> Result<()> {
    let Dir {
        path: dir,
        prefix,
        mut rules,
        ignored,
    } = dir;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(io(&dir))? {
        let entry = entry.map_err(io(&dir))?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| Error::Corrupt(format!("non UTF-8 file name in {}", dir.display())))?
            .to_owned();
        // The type comes with the listing on most file systems: no lookup for directories
        let ft = entry.file_type().map_err(io(&entry.path()))?;
        entries.push((name, ft));
    }
    if !ignored {
        rules.extend(dir_rules(&dir, |n| entries.iter().any(|(e, _)| e == n)));
    }
    for (name, ft) in entries {
        if prefix.is_empty() && name == DIR {
            continue;
        }
        let rel = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        let path = dir.join(&name);
        if ft.is_dir() {
            let skip = ignored || name == ".git" || is_ignored(&rules, &path, true);
            if skip && !tracks_under(tracked, &rel) {
                continue;
            }
            subdirs.push(Dir {
                path,
                prefix: rel,
                rules: rules.clone(),
                ignored: skip,
            });
            continue;
        }
        if !ft.is_file() && !ft.is_symlink() {
            continue;
        }
        let skip = ignored || is_ignored(&rules, &path, false);
        if skip && !tracked.contains_key(&rel) {
            continue;
        }
        let meta = std::fs::symlink_metadata(&path).map_err(io(&path))?;
        if let Some(mode) = mode_of(&meta) {
            found.push((
                rel,
                Found {
                    mode,
                    size: meta.len(),
                    mtime_ns: mtime_ns(&meta),
                },
            ));
        }
    }
    Ok(())
}

/// Directory names a working copy never writes into: its own metadata and git's. Compared
/// ignoring case, since macOS and Windows file systems do.
const RESERVED: [&str; 2] = [DIR, ".git"];

fn unsafe_path(rel: &str, reason: &'static str) -> Error {
    Error::UnsafePath {
        path: rel.to_owned(),
        reason,
    }
}

/// Checks a repository path before writing or removing it: plain relative components only, none
/// of them reserved. Paths come from trees, so a hostile repository (or server) must not be able
/// to reach outside the working copy or overwrite its metadata.
fn check_rel(rel: &str) -> Result<()> {
    for part in rel.split('/') {
        if part.is_empty() || part == "." || part == ".." || part.contains('\\') || part.contains('\0') {
            return Err(unsafe_path(rel, "not a plain relative path"));
        }
        if RESERVED.iter().any(|r| part.eq_ignore_ascii_case(r)) {
            return Err(unsafe_path(rel, "it is reserved for the working copy's metadata"));
        }
    }
    Ok(())
}

/// Checks that every directory on the way to `rel` (below `root`) is a real directory, not a
/// symbolic link, so a write cannot follow one out of the working copy. Missing directories are
/// fine: they are created as real directories.
fn check_parents(root: &Path, rel: &str) -> Result<()> {
    let mut dir = root.to_path_buf();
    let mut parts: Vec<&str> = rel.split('/').collect();
    parts.pop();
    for part in parts {
        dir.push(part);
        match std::fs::symlink_metadata(&dir) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(unsafe_path(rel, "a directory on its path is a symbolic link"));
            }
            Ok(m) if !m.is_dir() => return Ok(()),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io(&dir)(e)),
        }
    }
    Ok(())
}

/// Creates the directories of `rel` one by one, refusing to go through a symbolic link. `done`
/// holds the directories already created or checked, which are skipped.
fn create_dirs<'a>(root: &Path, rel: &'a str, done: &mut HashSet<&'a str>) -> Result<()> {
    let mut dir = root.to_path_buf();
    let mut end = 0;
    for part in rel.split('/') {
        dir.push(part);
        end += part.len();
        let prefix = &rel[..end];
        end += 1;
        if !done.insert(prefix) {
            continue;
        }
        match std::fs::create_dir(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let m = std::fs::symlink_metadata(&dir).map_err(io(&dir))?;
                if m.file_type().is_symlink() {
                    return Err(unsafe_path(rel, "a directory on its path is a symbolic link"));
                }
                if !m.is_dir() {
                    return Err(io(&dir)(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        "not a directory",
                    )));
                }
            }
            Err(e) => return Err(io(&dir)(e)),
        }
    }
    Ok(())
}

fn path_of(root: &Path, rel: &str) -> PathBuf {
    rel.split('/').fold(root.to_path_buf(), |p, part| p.join(part))
}

/// 读取文件内容：符号链接返回链接目标的字节。
pub(crate) fn read_content(root: &Path, rel: &str, mode: Mode) -> Result<Bytes> {
    let path = path_of(root, rel);
    match mode {
        Mode::Symlink => {
            let target = std::fs::read_link(&path).map_err(io(&path))?;
            let s = target
                .to_str()
                .ok_or_else(|| Error::Corrupt(format!("non UTF-8 symlink {}", path.display())))?;
            Ok(Bytes::copy_from_slice(s.as_bytes()))
        }
        Mode::File | Mode::Executable => Ok(Bytes::from(std::fs::read(&path).map_err(io(&path))?)),
    }
}

/// 写入文件并设置类型；返回写入后的修改时间。已有的同名文件、链接或目录会被替换。
pub(crate) fn write_file(root: &Path, rel: &str, mode: Mode, data: &[u8]) -> Result<u64> {
    check_rel(rel)?;
    let path = path_of(root, rel);
    if let Some((dirs, _)) = rel.rsplit_once('/') {
        create_dirs(root, dirs, &mut HashSet::new())?;
    }
    if let Ok(meta) = std::fs::symlink_metadata(&path) {
        if meta.is_dir() {
            std::fs::remove_dir_all(&path).map_err(io(&path))?;
        } else {
            std::fs::remove_file(&path).map_err(io(&path))?;
        }
    }
    match mode {
        Mode::Symlink => {
            let target =
                std::str::from_utf8(data).map_err(|_| Error::Corrupt(format!("symlink {rel} target is not UTF-8")))?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, &path).map_err(io(&path))?;
            #[cfg(not(unix))]
            std::fs::write(&path, target).map_err(io(&path))?;
        }
        Mode::File | Mode::Executable => {
            std::fs::write(&path, data).map_err(io(&path))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let bits = if mode == Mode::Executable { 0o755 } else { 0o644 };
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(bits)).map_err(io(&path))?;
            }
        }
    }
    let meta = std::fs::symlink_metadata(&path).map_err(io(&path))?;
    Ok(mtime_ns(&meta))
}

/// 在空目录里新建文件（检出用的快速路径）：不检查已有文件，上级目录由调用方事先建好，权限在创建
/// 时设置（受 umask 影响，与 git 相同），修改时间从打开的文件读取。已存在时报错。
pub(crate) fn create_file(root: &Path, rel: &str, mode: Mode, data: &[u8]) -> Result<u64> {
    use std::io::Write;
    check_rel(rel)?;
    let path = path_of(root, rel);
    if mode == Mode::Symlink {
        return write_file(root, rel, mode, data);
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(if mode == Mode::Executable { 0o755 } else { 0o644 });
    }
    let mut f = opts.open(&path).map_err(io(&path))?;
    f.write_all(data).map_err(io(&path))?;
    let meta = f.metadata().map_err(io(&path))?;
    Ok(mtime_ns(&meta))
}

/// 建好一组文件的全部上级目录。
pub(crate) fn create_parents<'a>(root: &Path, rels: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut dirs: Vec<&str> = Vec::new();
    for r in rels {
        check_rel(r)?;
        dirs.extend(r.rsplit_once('/').map(|(d, _)| d));
    }
    dirs.sort_unstable();
    dirs.dedup();
    let mut done = HashSet::new();
    for d in dirs {
        create_dirs(root, d, &mut done)?;
    }
    Ok(())
}

/// 删除文件，并逐级删除因此变空的上级目录（不删 root）。
pub(crate) fn remove_file(root: &Path, rel: &str) -> Result<()> {
    check_rel(rel)?;
    check_parents(root, rel)?;
    let path = path_of(root, rel);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(&path)(e)),
    }
    let mut dir = path.parent();
    while let Some(d) = dir {
        if d == root || std::fs::remove_dir(d).is_err() {
            break; // 非空或已到根目录
        }
        dir = d.parent();
    }
    Ok(())
}

/// Removes the directory `rel` and everything in it (symbolic links are removed, not followed),
/// then its parents that are left empty, as [`remove_file`] does.
pub(crate) fn remove_dir(root: &Path, rel: &str) -> Result<()> {
    check_rel(rel)?;
    check_parents(root, rel)?;
    let path = path_of(root, rel);
    match std::fs::remove_dir_all(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(&path)(e)),
    }
    let mut dir = path.parent();
    while let Some(d) = dir {
        if d == root || std::fs::remove_dir(d).is_err() {
            break;
        }
        dir = d.parent();
    }
    Ok(())
}

/// The untracked files at or under `wanted` (as [`selects`] reads paths), for `scn clean`:
/// files, and directories with nothing tracked in them as one entry ending in `/` when all of
/// them goes (empty ones too). Ignored ones only with `ignored_too`: otherwise an untracked
/// directory holding ignored files keeps them, and is listed file by file. Nested repositories
/// and working copies (directories holding `.git` or `.scion`) are never listed.
pub(crate) fn untracked<R>(
    root: &Path,
    tracked: &BTreeMap<String, R>,
    wanted: &[String],
    ignored_too: bool,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    // Untracked directories walked into, and those with something inside that stays
    let (mut whole, mut keeps) = (Vec::new(), std::collections::HashSet::new());
    let mut keep = |rel: &str| {
        let mut at = rel;
        while let Some(i) = at.rfind('/') {
            at = &at[..i];
            keeps.insert(at.to_owned());
        }
    };
    let mut dirs = vec![(root.to_path_buf(), String::new(), Vec::<Arc<Gitignore>>::new(), false)];
    while let Some((dir, prefix, mut rules, ignored)) = dirs.pop() {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&dir).map_err(io(&dir))? {
            let entry = entry.map_err(io(&dir))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            entries.push((name, entry.file_type().map_err(io(&entry.path()))?));
        }
        if !ignored {
            rules.extend(dir_rules(&dir, |n| entries.iter().any(|(e, _)| e == n)));
        }
        for (name, ft) in entries {
            if RESERVED.iter().any(|r| name.eq_ignore_ascii_case(r)) {
                keep(&format!("{prefix}/{name}"));
                continue;
            }
            let rel = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let path = dir.join(&name);
            let all = selects(wanted, &rel);
            if !all
                && !wanted
                    .iter()
                    .any(|w| selects(std::slice::from_ref(&rel), w.trim_end_matches('/')))
            {
                continue;
            }
            if ft.is_dir() {
                let skip = ignored || is_ignored(&rules, &path, true);
                let nested = RESERVED.iter().any(|r| path.join(r).exists());
                if (skip && !ignored_too) || nested {
                    keep(&rel);
                    continue;
                }
                // Walked into even when it goes whole, so nothing nested inside goes with it
                if all && !tracks_under(tracked, &rel) {
                    whole.push(rel.clone());
                }
                dirs.push((path, rel, rules.clone(), skip));
                continue;
            }
            if tracked.contains_key(&rel) || !all {
                continue;
            }
            if (ignored || is_ignored(&rules, &path, false)) && !ignored_too {
                keep(&rel);
                continue;
            }
            out.push(rel);
        }
    }
    // An untracked directory left with nothing inside goes as one, the outermost such
    whole.sort();
    let mut gone: Vec<String> = Vec::new();
    for d in whole {
        if !keeps.contains(&d) && !gone.iter().any(|g| d.starts_with(&format!("{g}/"))) {
            gone.push(d);
        }
    }
    out.retain(|p| !gone.iter().any(|g| p.starts_with(&format!("{g}/"))));
    out.extend(gone.into_iter().map(|g| format!("{g}/")));
    out.sort();
    Ok(out)
}

/// Whether `path` is one of `paths` or under one of them (`.` or an empty path is everything).
pub(crate) fn selects(paths: &[String], path: &str) -> bool {
    paths.iter().map(|w| w.trim_end_matches('/')).any(|w| {
        w.is_empty() || w == "." || path == w || (path.starts_with(w) && path.as_bytes().get(w.len()) == Some(&b'/'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file made directly, as a user or git would (the working copy itself refuses `.git`).
    fn put(root: &Path, rel: &str) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, b"x").expect("write");
    }

    fn none() -> BTreeMap<String, ()> {
        BTreeMap::new()
    }

    fn tracked(paths: &[&str]) -> BTreeMap<String, ()> {
        paths.iter().map(|p| ((*p).to_owned(), ())).collect()
    }

    fn keys(found: &BTreeMap<String, Found>) -> Vec<&str> {
        found.keys().map(String::as_str).collect()
    }

    #[test]
    fn ignore_rules_follow_git() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        for f in [
            "src/main.rs",
            "target/debug/app",
            "notes.tmp",
            "keep.tmp",
            "sub/build/out.o",
            "sub/build.rs",
            "sub/local.cfg",
            "docs/x.log",
            ".git/HEAD",
            "nested/.git/config",
        ] {
            put(r, f);
        }
        std::fs::write(r.join(".gitignore"), "target/\n*.tmp\n!keep.tmp\n*.log\n[bad\n").expect("gitignore");
        // .scionignore wins over .gitignore in the same directory; deeper directories win over parents
        std::fs::write(r.join(".scionignore"), "!docs/x.log\n").expect("scionignore");
        std::fs::write(r.join("sub/.gitignore"), "build/\nlocal.cfg\n").expect("sub");
        let found = scan(r, &none()).expect("scan");
        assert_eq!(
            keys(&found),
            [
                ".gitignore",
                ".scionignore",
                "docs/x.log",
                "keep.tmp",
                "src/main.rs",
                "sub/.gitignore",
                "sub/build.rs"
            ]
        );
    }

    /// Paths from a hostile tree cannot overwrite the working copy's metadata or git's, reach
    /// outside the root, or follow a symbolic link out of the working copy.
    #[test]
    fn writes_refuse_reserved_names_and_symlinked_directories() {
        let d = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let r = d.path();
        let unsafe_err = |res: Result<u64>| matches!(res, Err(Error::UnsafePath { .. }));
        for rel in [
            ".scion/state.json",
            ".SCION/state.json",
            "a/.Git/config",
            ".git",
            "a/../b",
            "a//b",
            "a\\b",
        ] {
            assert!(unsafe_err(write_file(r, rel, Mode::File, b"x")), "write {rel}");
            assert!(unsafe_err(create_file(r, rel, Mode::File, b"x")), "create {rel}");
            assert!(
                matches!(remove_file(r, rel), Err(Error::UnsafePath { .. })),
                "remove {rel}"
            );
        }
        assert!(matches!(create_parents(r, [".scion/x"]), Err(Error::UnsafePath { .. })));
        #[cfg(unix)]
        {
            // A symbolic link where a directory is expected: as after a switch, or a link
            // `A` next to `a/...` on a case-insensitive file system
            std::os::unix::fs::symlink(outside.path(), r.join("link")).expect("symlink");
            std::fs::write(outside.path().join("victim"), b"keep").expect("victim");
            assert!(unsafe_err(write_file(r, "link/victim", Mode::File, b"pwned")));
            assert!(unsafe_err(write_file(r, "link/new/file", Mode::File, b"pwned")));
            assert!(matches!(
                create_parents(r, ["link/sub/f"]),
                Err(Error::UnsafePath { .. })
            ));
            assert!(matches!(remove_file(r, "link/victim"), Err(Error::UnsafePath { .. })));
            // Directories checked once per batch: a link below a shared prefix is still caught
            std::fs::create_dir_all(r.join("deep/ok")).expect("deep");
            std::os::unix::fs::symlink(outside.path(), r.join("deep/x")).expect("symlink");
            assert!(matches!(
                create_parents(r, ["deep/ok/a/f", "deep/ok/b/f", "deep/x/c/f"]),
                Err(Error::UnsafePath { .. })
            ));
            assert!(r.join("deep/ok/b").is_dir() && !outside.path().join("c").exists());
            assert_eq!(std::fs::read(outside.path().join("victim")).expect("read"), b"keep");
            assert!(!outside.path().join("new").exists());
        }
        // Ordinary paths still work, and directories that exist are reused
        write_file(r, "src/a/b.txt", Mode::File, b"b").expect("write");
        create_parents(r, ["src/a/c.txt", "src/d/e.txt"]).expect("parents");
        create_file(r, "src/d/e.txt", Mode::File, b"e").expect("create");
        remove_file(r, "src/a/b.txt").expect("remove");
        remove_file(r, "missing/file").expect("missing is fine");
        // A file where a directory is expected is an error, not a write elsewhere
        std::fs::write(r.join("plain"), b"p").expect("plain");
        assert!(matches!(
            write_file(r, "plain/x", Mode::File, b"x"),
            Err(Error::Io { .. })
        ));
    }

    #[test]
    fn tracked_files_are_never_hidden_by_ignore_rules() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        for f in ["build/keep.txt", "build/junk.o", "a.log", "b.log", ".git/HEAD"] {
            put(r, f);
        }
        std::fs::write(r.join(".gitignore"), "build/\n*.log\n").expect("gitignore");
        let t = tracked(&["build/keep.txt", "a.log", ".git/HEAD"]);
        let found = scan(r, &t).expect("scan");
        // Untracked files inside an ignored directory stay ignored even when tracked siblings exist
        assert_eq!(keys(&found), [".git/HEAD", ".gitignore", "a.log", "build/keep.txt"]);
        assert!(tracks_under(&t, "build") && !tracks_under(&t, "buil") && !tracks_under(&t, "a.log"));
    }

    #[test]
    fn now_and_to_ns() {
        assert!(now_ns() > 1_577_836_800_000_000_000);
        assert_eq!(to_ns(UNIX_EPOCH), 0);
    }

    #[test]
    fn write_scan_read_roundtrip() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        write_file(r, "a/b.txt", Mode::File, b"hello").expect("file");
        write_file(r, "run.sh", Mode::Executable, b"#!/bin/sh").expect("exec");
        write_file(r, "link", Mode::Symlink, b"a/b.txt").expect("link");
        std::fs::create_dir(r.join(DIR)).expect("meta dir");
        std::fs::write(r.join(DIR).join("state.json"), b"{}").expect("ignored");
        let found = scan(r, &none()).expect("scan");
        assert_eq!(found.keys().collect::<Vec<_>>(), ["a/b.txt", "link", "run.sh"]);
        assert_eq!(found["a/b.txt"].mode, Mode::File);
        assert_eq!(found["a/b.txt"].size, 5);
        assert_eq!(found["link"].mode, Mode::Symlink);
        assert_eq!(found["run.sh"].mode, Mode::Executable);
        assert_eq!(read_content(r, "a/b.txt", Mode::File).expect("read").as_ref(), b"hello");
        assert_eq!(
            read_content(r, "link", Mode::Symlink).expect("read").as_ref(),
            b"a/b.txt"
        );
    }

    #[test]
    fn write_file_replaces_existing_kinds() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        write_file(r, "x", Mode::Symlink, b"target").expect("link");
        write_file(r, "x", Mode::File, b"now a file").expect("replace link");
        assert_eq!(scan(r, &none()).expect("scan")["x"].mode, Mode::File);
        write_file(r, "dir/inner", Mode::File, b"i").expect("dir");
        write_file(r, "dir", Mode::File, b"file replaces dir").expect("replace dir");
        assert_eq!(scan(r, &none()).expect("scan").keys().collect::<Vec<_>>(), ["dir", "x"]);
        assert!(matches!(
            write_file(r, "bad", Mode::Symlink, &[0xff]),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn create_file_fast_path() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        create_parents(r, ["a/b/c.txt", "a/d.txt", "top.txt", "a/b/e.sh"]).expect("dirs");
        assert!(r.join("a/b").is_dir());
        create_file(r, "a/b/c.txt", Mode::File, b"c").expect("file");
        create_file(r, "a/b/e.sh", Mode::Executable, b"#!/bin/sh").expect("exec");
        create_file(r, "top.txt", Mode::Symlink, b"a/d.txt").expect("link");
        let found = scan(r, &none()).expect("scan");
        assert_eq!(found["a/b/c.txt"].mode, Mode::File);
        assert_eq!(found["a/b/e.sh"].mode, Mode::Executable);
        assert_eq!(found["top.txt"].mode, Mode::Symlink);
        assert!(
            create_file(r, "a/b/c.txt", Mode::File, b"again").is_err(),
            "never overwrites"
        );
        assert!(
            create_file(r, "missing/x", Mode::File, b"x").is_err(),
            "parents must exist"
        );
    }

    #[test]
    fn remove_file_prunes_empty_dirs() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        write_file(r, "a/b/c", Mode::File, b"c").expect("c");
        write_file(r, "a/keep", Mode::File, b"k").expect("keep");
        remove_file(r, "a/b/c").expect("rm");
        assert!(!r.join("a/b").exists());
        assert!(r.join("a/keep").exists());
        remove_file(r, "a/keep").expect("rm");
        assert!(!r.join("a").exists());
        assert!(r.exists());
        remove_file(r, "never/existed").expect("missing is fine");
    }

    #[test]
    fn read_and_scan_errors() {
        let d = tempfile::tempdir().expect("tempdir");
        assert!(matches!(
            read_content(d.path(), "nope", Mode::File),
            Err(Error::Io { .. })
        ));
        assert!(matches!(
            read_content(d.path(), "nope", Mode::Symlink),
            Err(Error::Io { .. })
        ));
        assert!(matches!(
            scan(&d.path().join("missing"), &none()),
            Err(Error::Io { .. })
        ));
    }

    /// Many threads find what one finds, ignore rules and tracked files under ignored
    /// directories included; an unreadable directory fails the scan without hanging it.
    #[test]
    fn parallel_scan_matches_one_thread_and_reports_errors() {
        let d = tempfile::tempdir().expect("tempdir");
        let r = d.path();
        for i in 0..40 {
            for j in 0..5 {
                std::fs::create_dir_all(r.join(format!("d{i}/e{j}"))).expect("mkdir");
                std::fs::write(r.join(format!("d{i}/e{j}/f.txt")), b"x").expect("write");
            }
        }
        std::fs::write(r.join(".gitignore"), "build/\n*.log\n").expect("ignore");
        std::fs::create_dir_all(r.join("build/out")).expect("mkdir");
        std::fs::write(r.join("build/out/kept.bin"), b"k").expect("write");
        std::fs::write(r.join("build/out/other.bin"), b"o").expect("write");
        std::fs::write(r.join("d0/noise.log"), b"n").expect("write");
        let tracked: BTreeMap<String, ()> = [("build/out/kept.bin".to_owned(), ())].into();
        let one = scan_with(r, &tracked, 1).expect("one thread");
        let many = scan_with(r, &tracked, 8).expect("eight threads");
        assert_eq!(one.keys().collect::<Vec<_>>(), many.keys().collect::<Vec<_>>());
        assert_eq!(many.len(), 40 * 5 + 2, "{:?}", many.keys().take(3).collect::<Vec<_>>());
        assert!(many.contains_key("build/out/kept.bin") && !many.contains_key("build/out/other.bin"));
        assert!(!many.contains_key("d0/noise.log"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let locked = r.join("d7/e3");
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");
            let err = scan_with(r, &tracked, 8);
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod back");
            assert!(err.is_err(), "an unreadable directory fails the scan");
        }
    }

    #[cfg(unix)]
    #[test]
    fn scan_skips_special_files() {
        let d = tempfile::tempdir().expect("tempdir");
        let sock = d.path().join("s.sock");
        let _l = std::os::unix::net::UnixListener::bind(&sock).expect("socket");
        assert!(scan(d.path(), &none()).expect("scan").is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_names_rejected() {
        use std::os::unix::ffi::OsStrExt;
        let d = tempfile::tempdir().expect("tempdir");
        let name = std::ffi::OsStr::from_bytes(&[0x66, 0xff]);
        // 部分文件系统（如 APFS）不允许非 UTF-8 文件名，此时跳过
        if std::fs::write(d.path().join(name), b"x").is_ok() {
            assert!(matches!(scan(d.path(), &none()), Err(Error::Corrupt(_))));
        }
    }
}
