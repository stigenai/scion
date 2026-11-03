//! 归档下载（`QueryService.Archive`）：把一个目录树流式写成 tar、tar.gz 或 zip。
//!
//! 按目录树深度优先、名字顺序写入（结果确定）。文件内容逐个读取，写入的字节每积累一块就交给调用方，
//! 内存占用与单个文件大小相当，不随仓库大小增长。被 redact 的文件写入一段说明文字代替原内容。

use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use scion_object::{EntryMode, ObjectId, Tree};
use scion_store::BlobStore;
use zip::write::{SimpleFileOptions, StreamWriter};
use zip::{CompressionMethod, ZipWriter};

use crate::{QueryError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFormat {
    Tar,
    TarGz,
    Zip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveOptions {
    pub format: ArchiveFormat,
    /// 加在每个路径前面，例如 `repo-main/`；为空或以 `/` 结尾。
    pub prefix: String,
    /// 归档中条目的修改时间（Unix 秒），通常取提交时间。
    pub mtime: i64,
}

/// 被 redact 的文件在归档中的内容。
pub fn redacted_placeholder(redaction_id: &str) -> Vec<u8> {
    format!("This file has been redacted (redaction {redaction_id}).\n").into_bytes()
}

/// 写入目标：所有格式的写入器都写进这块共享缓冲，调用方定期取走。
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl Sink {
    fn take(&self) -> Vec<u8> {
        self.0.lock().map(|mut b| std::mem::take(&mut *b)).unwrap_or_default()
    }

    fn len(&self) -> usize {
        self.0.lock().map(|b| b.len()).unwrap_or_default()
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("archive buffer poisoned"))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

enum Writer {
    Tar(tar::Builder<Sink>),
    TarGz(Box<tar::Builder<GzEncoder<Sink>>>),
    Zip(Box<ZipWriter<StreamWriter<Sink>>>),
}

fn io_err(e: impl std::fmt::Display) -> QueryError {
    QueryError::Archive(e.to_string())
}

/// Unix 秒 → (年, 月, 日, 时, 分, 秒)，UTC（Howard Hinnant 的 civil_from_days）。
fn civil(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400) as u32;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day, rem / 3600, rem / 60 % 60, rem % 60)
}

/// zip 的时间只能表示 1980–2107 年，超出范围时取边界。
fn zip_time(secs: i64) -> zip::DateTime {
    let (y, mo, d, h, mi, s) = civil(secs);
    if y < 1980 {
        return zip::DateTime::default();
    }
    let y = y.min(2107) as u16;
    zip::DateTime::from_date_and_time(y, mo as u8, d as u8, h as u8, mi as u8, s as u8).unwrap_or_default()
}

fn file_mode(mode: EntryMode) -> u32 {
    match mode {
        EntryMode::Executable | EntryMode::Tree => 0o755,
        EntryMode::Symlink => 0o777,
        EntryMode::File => 0o644,
    }
}

impl Writer {
    fn new(format: ArchiveFormat, sink: Sink) -> Self {
        match format {
            ArchiveFormat::Tar => Writer::Tar(tar::Builder::new(sink)),
            ArchiveFormat::TarGz => Writer::TarGz(Box::new(tar::Builder::new(GzEncoder::new(
                sink,
                Compression::default(),
            )))),
            ArchiveFormat::Zip => Writer::Zip(Box::new(ZipWriter::new_stream(sink))),
        }
    }

    fn tar_header(mode: EntryMode, size: u64, mtime: i64) -> tar::Header {
        let mut h = tar::Header::new_gnu();
        h.set_mode(file_mode(mode));
        h.set_size(size);
        h.set_mtime(u64::try_from(mtime).unwrap_or(0));
        h.set_entry_type(match mode {
            EntryMode::Tree => tar::EntryType::Directory,
            EntryMode::Symlink => tar::EntryType::Symlink,
            EntryMode::File | EntryMode::Executable => tar::EntryType::Regular,
        });
        h
    }

    fn zip_options(mode: EntryMode, size: u64, mtime: i64) -> SimpleFileOptions {
        SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .unix_permissions(file_mode(mode))
            .last_modified_time(zip_time(mtime))
            .large_file(size >= u64::from(u32::MAX))
    }

    /// 写一个条目：目录 `data` 为空，符号链接 `data` 为链接目标。
    fn add(&mut self, path: &str, mode: EntryMode, data: &[u8], mtime: i64) -> Result<()> {
        fn tar_add<W: Write>(
            b: &mut tar::Builder<W>,
            path: &str,
            mode: EntryMode,
            data: &[u8],
            mtime: i64,
        ) -> Result<()> {
            match mode {
                EntryMode::Symlink => {
                    let mut h = Writer::tar_header(mode, 0, mtime);
                    let target = std::str::from_utf8(data).map_err(io_err)?;
                    b.append_link(&mut h, path, target).map_err(io_err)
                }
                EntryMode::Tree => {
                    let mut h = Writer::tar_header(mode, 0, mtime);
                    b.append_data(&mut h, format!("{path}/"), std::io::empty())
                        .map_err(io_err)
                }
                _ => {
                    let mut h = Writer::tar_header(mode, data.len() as u64, mtime);
                    b.append_data(&mut h, path, data).map_err(io_err)
                }
            }
        }
        match self {
            Writer::Tar(b) => tar_add(b, path, mode, data, mtime),
            Writer::TarGz(b) => tar_add(b, path, mode, data, mtime),
            Writer::Zip(z) => {
                let opts = Self::zip_options(mode, data.len() as u64, mtime);
                match mode {
                    EntryMode::Tree => z.add_directory(format!("{path}/"), opts).map_err(io_err),
                    EntryMode::Symlink => {
                        let target = std::str::from_utf8(data).map_err(io_err)?;
                        z.add_symlink(path, target, opts).map_err(io_err)
                    }
                    _ => {
                        z.start_file(path, opts).map_err(io_err)?;
                        z.write_all(data).map_err(io_err)
                    }
                }
            }
        }
    }

    fn finish(self) -> Result<()> {
        match self {
            Writer::Tar(b) => b.into_inner().map(drop).map_err(io_err),
            Writer::TarGz(b) => (*b).into_inner().map_err(io_err)?.finish().map(drop).map_err(io_err),
            Writer::Zip(z) => z.finish().map(drop).map_err(io_err),
        }
    }
}

/// 接收归档数据的回调；返回 false 表示不再接收（例如客户端已断开）。
pub type Emit = dyn FnMut(Bytes) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send;

struct Run<'a> {
    blobs: &'a dyn BlobStore,
    writer: Writer,
    sink: Sink,
    chunk: usize,
    mtime: i64,
    emit: &'a mut Emit,
    written: u64,
    stopped: bool,
}

impl Run<'_> {
    /// 缓冲中满一块的部分交给调用方（`all` 时连同不足一块的结尾）；调用方不再接收时停止。
    async fn drain(&mut self, all: bool) {
        if self.sink.len() < self.chunk && !all {
            return;
        }
        let buf = Bytes::from(self.sink.take());
        let mut off = 0;
        while off < buf.len() && !self.stopped {
            let end = (off + self.chunk).min(buf.len());
            if end - off < self.chunk && !all {
                // 不足一块的结尾放回缓冲，和后面的内容一起发送
                let _ = self.sink.clone().write_all(&buf[off..]);
                return;
            }
            self.written += (end - off) as u64;
            if !(self.emit)(buf.slice(off..end)).await {
                self.stopped = true;
            }
            off = end;
        }
    }

    fn walk<'s>(&'s mut self, tree: ObjectId, base: String) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 's>> {
        Box::pin(async move {
            let bytes = self.blobs.get(&tree).await?.ok_or(QueryError::MissingObject(tree))?;
            let t = Tree::decode(&bytes).map_err(|e| QueryError::Corrupt(tree, e.to_string()))?;
            for e in t.entries() {
                if self.stopped {
                    return Ok(());
                }
                let path = format!("{base}{}", e.name);
                if e.mode == EntryMode::Tree {
                    self.writer.add(&path, e.mode, &[], self.mtime)?;
                    self.walk(e.id, format!("{path}/")).await?;
                    continue;
                }
                let data = match self.blobs.get(&e.id).await {
                    Ok(Some(d)) => d.to_vec(),
                    Ok(None) => return Err(QueryError::MissingObject(e.id)),
                    Err(scion_store::Error::Redacted { redaction_id, .. }) => redacted_placeholder(&redaction_id),
                    Err(err) => return Err(err.into()),
                };
                // 被 redact 的符号链接：写成普通文件
                let mode = if e.mode == EntryMode::Symlink && std::str::from_utf8(&data).is_err() {
                    EntryMode::File
                } else {
                    e.mode
                };
                self.writer.add(&path, mode, &data, self.mtime)?;
                self.drain(false).await;
            }
            Ok(())
        })
    }
}

/// 把目录树 `root` 写成归档。`base` 为 `root` 在仓库中的路径（空串为根目录），归档中的路径为
/// `prefix + base + 相对路径`。每积累 `chunk` 字节调用一次 `emit`，`emit` 返回 false 时停止。
/// 返回交给 `emit` 的总字节数。
pub async fn write_archive<'a>(
    blobs: &'a dyn BlobStore,
    root: ObjectId,
    base: &str,
    opts: &ArchiveOptions,
    chunk: usize,
    emit: &'a mut Emit,
) -> Result<u64> {
    let sink = Sink::default();
    let mut run = Run {
        blobs,
        writer: Writer::new(opts.format, sink.clone()),
        sink,
        chunk: chunk.max(1),
        mtime: opts.mtime,
        emit,
        written: 0,
        stopped: false,
    };
    let start = if base.is_empty() {
        opts.prefix.clone()
    } else {
        format!("{}{base}/", opts.prefix)
    };
    run.walk(root, start).await?;
    if run.stopped {
        return Ok(run.written);
    }
    let Run { writer, .. } = &mut run;
    let writer = std::mem::replace(writer, Writer::Tar(tar::Builder::new(Sink::default())));
    writer.finish()?;
    run.drain(true).await;
    Ok(run.written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use scion_object::TreeEntry;
    use scion_store::MemStore;
    use std::io::Read;

    /// 指定的文件按 redact 处理，其余转给内存存储。
    struct Redacting(MemStore, ObjectId);

    #[async_trait]
    impl BlobStore for Redacting {
        async fn put(&self, id: ObjectId, data: Bytes) -> scion_store::Result<()> {
            self.0.put(id, data).await
        }
        async fn get(&self, id: &ObjectId) -> scion_store::Result<Option<Bytes>> {
            if *id == self.1 {
                return Err(scion_store::Error::Redacted {
                    id: *id,
                    redaction_id: "rd1".into(),
                });
            }
            self.0.get(id).await
        }
        async fn exists(&self, id: &ObjectId) -> scion_store::Result<bool> {
            self.0.exists(id).await
        }
    }

    async fn blob(s: &MemStore, name: &str, mode: EntryMode, content: &[u8]) -> TreeEntry {
        let id = ObjectId::of_blob(content);
        s.put(id, Bytes::copy_from_slice(content)).await.expect("put");
        TreeEntry {
            name: name.into(),
            mode,
            id,
            size: content.len() as u64,
        }
    }

    async fn tree(s: &MemStore, entries: Vec<TreeEntry>) -> TreeEntry {
        let bytes = Tree::new(entries).expect("tree").encode();
        let id = ObjectId::hash(scion_object::ObjectKind::Tree, &bytes);
        s.put(id, Bytes::from(bytes)).await.expect("put");
        TreeEntry {
            name: String::new(),
            mode: EntryMode::Tree,
            id,
            size: 0,
        }
    }

    fn named(mut e: TreeEntry, name: &str) -> TreeEntry {
        e.name = name.into();
        e
    }

    /// README、bin/run（可执行）、bin/link（符号链接）、empty/（空目录）、big.bin（跨多个块）
    async fn sample(s: &MemStore) -> ObjectId {
        let run = blob(s, "run", EntryMode::Executable, b"#!/bin/sh\n").await;
        let link = blob(s, "link", EntryMode::Symlink, b"run").await;
        let bin = named(tree(s, vec![run, link]).await, "bin");
        let empty = named(tree(s, vec![]).await, "empty");
        let readme = blob(s, "README", EntryMode::File, b"hello\n").await;
        let big = blob(s, "big.bin", EntryMode::File, &vec![7u8; 10_000]).await;
        tree(s, vec![readme, bin, empty, big]).await.id
    }

    async fn collect(
        blobs: &dyn BlobStore,
        root: ObjectId,
        base: &str,
        format: ArchiveFormat,
        prefix: &str,
        chunk: usize,
    ) -> (Vec<u8>, Vec<usize>) {
        let chunks = Arc::new(Mutex::new(Vec::<Bytes>::new()));
        let sink = chunks.clone();
        let mut emit = move |b: Bytes| -> Pin<Box<dyn Future<Output = bool> + Send>> {
            sink.lock().expect("lock").push(b);
            Box::pin(async { true })
        };
        let opts = ArchiveOptions {
            format,
            prefix: prefix.into(),
            mtime: 1_700_000_000,
        };
        let n = write_archive(blobs, root, base, &opts, chunk, &mut emit)
            .await
            .expect("archive");
        let chunks = chunks.lock().expect("lock").clone();
        let sizes: Vec<usize> = chunks.iter().map(Bytes::len).collect();
        let all: Vec<u8> = chunks.iter().flat_map(|c| c.to_vec()).collect();
        assert_eq!(n, all.len() as u64);
        (all, sizes)
    }

    /// (路径, 类型, 权限, 内容或链接目标)
    type Listed = Vec<(String, char, u32, Vec<u8>)>;

    fn list_tar(r: impl Read) -> Listed {
        let mut a = tar::Archive::new(r);
        a.entries()
            .expect("entries")
            .map(|e| {
                let mut e = e.expect("entry");
                let h = e.header().clone();
                let path = e.path().expect("path").to_string_lossy().into_owned();
                assert_eq!(h.mtime().expect("mtime"), 1_700_000_000);
                let (kind, data) = match h.entry_type() {
                    tar::EntryType::Directory => ('d', vec![]),
                    tar::EntryType::Symlink => (
                        'l',
                        e.link_name()
                            .expect("link")
                            .expect("target")
                            .to_string_lossy()
                            .as_bytes()
                            .to_vec(),
                    ),
                    _ => {
                        let mut d = Vec::new();
                        e.read_to_end(&mut d).expect("read");
                        ('f', d)
                    }
                };
                (path, kind, h.mode().expect("mode") & 0o777, data)
            })
            .collect()
    }

    fn list_zip(data: Vec<u8>) -> Listed {
        let mut z = zip::ZipArchive::new(std::io::Cursor::new(data)).expect("zip");
        (0..z.len())
            .map(|i| {
                let mut f = z.by_index(i).expect("file");
                let mode = f.unix_mode().expect("mode");
                let kind = if f.is_dir() {
                    'd'
                } else if f.is_symlink() {
                    'l'
                } else {
                    'f'
                };
                let mut d = Vec::new();
                f.read_to_end(&mut d).expect("read");
                (f.name().expect("name").into_owned(), kind, mode & 0o777, d)
            })
            .collect()
    }

    fn expected(prefix: &str) -> Listed {
        let p = |s: &str| format!("{prefix}{s}");
        vec![
            (p("README"), 'f', 0o644, b"hello\n".to_vec()),
            (p("big.bin"), 'f', 0o644, vec![7u8; 10_000]),
            (p("bin/"), 'd', 0o755, vec![]),
            (p("bin/link"), 'l', 0o777, b"run".to_vec()),
            (p("bin/run"), 'f', 0o755, b"#!/bin/sh\n".to_vec()),
            (p("empty/"), 'd', 0o755, vec![]),
        ]
    }

    #[tokio::test]
    async fn every_format_round_trips() {
        let s = MemStore::new();
        let root = sample(&s).await;
        let (tar, sizes) = collect(&s, root, "", ArchiveFormat::Tar, "repo-main/", 4096).await;
        assert!(sizes.iter().all(|n| *n <= 4096) && sizes.len() > 2, "{sizes:?}");
        assert_eq!(list_tar(tar.as_slice()), expected("repo-main/"));
        let (gz, _) = collect(&s, root, "", ArchiveFormat::TarGz, "", 1 << 20).await;
        assert_eq!(list_tar(flate2::read::GzDecoder::new(gz.as_slice())), expected(""));
        let (z, _) = collect(&s, root, "", ArchiveFormat::Zip, "x/", 1000).await;
        let mut got = list_zip(z);
        got.sort();
        let mut want = expected("x/");
        want.sort();
        assert_eq!(got, want);
    }

    #[tokio::test]
    async fn subtree_keeps_full_paths_and_redacted_files_get_a_placeholder() {
        let s = MemStore::new();
        let root = sample(&s).await;
        let bin = Tree::decode(&s.get(&root).await.expect("get").expect("root"))
            .expect("tree")
            .entries()
            .iter()
            .find(|e| e.name == "bin")
            .expect("bin")
            .id;
        let redacted = ObjectId::of_blob(b"#!/bin/sh\n");
        let r = Redacting(s, redacted);
        let (tar, _) = collect(&r, bin, "bin", ArchiveFormat::Tar, "p/", 512).await;
        let got = list_tar(tar.as_slice());
        let names: Vec<&str> = got.iter().map(|e| e.0.as_str()).collect();
        assert_eq!(names, ["p/bin/link", "p/bin/run"]);
        assert_eq!(got[1].3, redacted_placeholder("rd1"));
    }

    #[tokio::test]
    async fn stops_when_the_receiver_goes_away_and_reports_missing_objects() {
        let s = MemStore::new();
        let root = sample(&s).await;
        let calls = Arc::new(Mutex::new(0));
        let c = calls.clone();
        let mut emit = move |_b: Bytes| -> Pin<Box<dyn Future<Output = bool> + Send>> {
            *c.lock().expect("lock") += 1;
            Box::pin(async { false })
        };
        let opts = ArchiveOptions {
            format: ArchiveFormat::Tar,
            prefix: String::new(),
            mtime: 0,
        };
        let n = write_archive(&s, root, "", &opts, 1024, &mut emit)
            .await
            .expect("stopped");
        assert_eq!((*calls.lock().expect("lock"), n), (1, 1024));

        let mut sink = |_b: Bytes| -> Pin<Box<dyn Future<Output = bool> + Send>> { Box::pin(async { true }) };
        let gone = ObjectId::of_blob(b"hello\n");
        s.delete(&gone).await.expect("delete");
        assert_eq!(
            write_archive(&s, root, "", &opts, 1024, &mut sink).await,
            Err(QueryError::MissingObject(gone))
        );
        let junk = ObjectId::hash(scion_object::ObjectKind::Tree, b"junk");
        s.put(junk, Bytes::from_static(b"junk")).await.expect("put");
        assert!(matches!(
            write_archive(&s, junk, "", &opts, 1024, &mut sink).await,
            Err(QueryError::Corrupt(id, _)) if id == junk
        ));
        let missing = ObjectId::hash(scion_object::ObjectKind::Tree, b"missing");
        assert_eq!(
            write_archive(&s, missing, "", &opts, 1024, &mut sink).await,
            Err(QueryError::MissingObject(missing))
        );
    }

    #[test]
    fn civil_dates_and_zip_time_bounds() {
        assert_eq!(civil(0), (1970, 1, 1, 0, 0, 0));
        assert_eq!(civil(1_700_000_000), (2023, 11, 14, 22, 13, 20));
        assert_eq!(civil(951_782_400), (2000, 2, 29, 0, 0, 0));
        assert_eq!(civil(-1), (1969, 12, 31, 23, 59, 59));
        assert_eq!(zip_time(0), zip::DateTime::default());
        let t = zip_time(1_700_000_000);
        assert_eq!(
            (t.year(), t.month(), t.day(), t.hour(), t.minute(), t.second()),
            (2023, 11, 14, 22, 13, 20)
        );
        assert_eq!(zip_time(i64::from(u32::MAX) * 4).year(), 2107);
        assert_eq!(file_mode(EntryMode::Symlink), 0o777);
    }
}
