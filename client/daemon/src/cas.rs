//! daemon 的本地对象存储：磁盘上按内容寻址的文件（`objects/<摘要首字节>/<id>`），前面加一层内存缓存。
//!
//! - 写入直接创建最终文件（不经临时文件和改名，检出大量小文件时文件系统操作减半）；进程崩溃留下的
//!   半个文件在读取时校验哈希失败，被删除并视为未命中，磁盘损坏的文件同样处理；
//! - 命中时更新文件修改时间；占用超过容量时按修改时间从旧到新删除，降到容量的 90%。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use moka::sync::Cache;
use scion_object::ObjectId;

pub struct Cas {
    dir: PathBuf,
    capacity: u64,
    used: AtomicU64,
    memory: Cache<ObjectId, Bytes>,
    collecting: AtomicBool,
}

impl Cas {
    /// 打开（必要时创建）`dir`。`capacity` 为磁盘占用上限，`memory_bytes` 为内存缓存大小。
    pub fn open(dir: &Path, capacity: u64, memory_bytes: u64) -> std::io::Result<Arc<Self>> {
        // 事先建好 256 个子目录，写入时不再逐个检查
        for b in 0..=255u8 {
            std::fs::create_dir_all(dir.join("objects").join(hex::encode([b])))?;
        }
        let used = scan(&dir.join("objects"))?.iter().map(|(_, size, _)| size).sum();
        Ok(Arc::new(Self {
            dir: dir.to_path_buf(),
            capacity,
            used: AtomicU64::new(used),
            memory: Cache::builder()
                .max_capacity(memory_bytes)
                .weigher(|_k: &ObjectId, v: &Bytes| u32::try_from(v.len()).unwrap_or(u32::MAX))
                .build(),
            collecting: AtomicBool::new(false),
        }))
    }

    fn path(&self, id: &ObjectId) -> PathBuf {
        // 按摘要的第一个字节分 256 个子目录（id 的前两个字节是算法和类型，分布不均）
        let shard = hex::encode(&id.digest()[..1]);
        self.dir.join("objects").join(shard).join(hex::encode(id.to_bytes()))
    }

    /// 磁盘占用（字节）。
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn get(&self, id: &ObjectId) -> Option<Bytes> {
        if let Some(b) = self.memory.get(id) {
            return Some(b);
        }
        let path = self.path(id);
        let data = std::fs::read(&path).ok()?;
        if !id.verify(&data) {
            tracing::warn!(%id, path = %path.display(), "corrupt cached object removed");
            if std::fs::remove_file(&path).is_ok() {
                self.used.fetch_sub(data.len() as u64, Ordering::Relaxed);
            }
            return None;
        }
        // 更新修改时间：最近用过的对象最后被回收
        if let Ok(f) = std::fs::File::options().write(true).open(&path) {
            let _ = f.set_modified(SystemTime::now());
        }
        let data = Bytes::from(data);
        self.memory.insert(*id, data.clone());
        Some(data)
    }

    /// 删除（对象被 redact）：内存层与磁盘层。
    pub fn remove(&self, id: &ObjectId) {
        self.memory.invalidate(id);
        let path = self.path(id);
        if let Ok(meta) = std::fs::metadata(&path)
            && std::fs::remove_file(&path).is_ok()
        {
            self.used.fetch_sub(meta.len(), Ordering::Relaxed);
        }
    }

    pub fn contains(&self, id: &ObjectId) -> bool {
        self.memory.contains_key(id) || self.path(id).exists()
    }

    /// 写入（调用方已校验内容与 id 相符）。已存在时什么都不做。
    pub fn put(&self, id: ObjectId, data: &Bytes) -> std::io::Result<()> {
        self.remember(id, data);
        self.persist(id, data)
    }

    /// 只放进内存层（磁盘写入由 [`Cas::persist`] 稍后在后台完成）。
    pub fn remember(&self, id: ObjectId, data: &Bytes) {
        self.memory.insert(id, data.clone());
    }

    /// 写入磁盘层。
    pub fn persist(&self, id: ObjectId, data: &Bytes) -> std::io::Result<()> {
        use std::io::Write;
        let path = self.path(&id);
        let mut f = match std::fs::File::options().write(true).create_new(true).open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(e) => return Err(e),
        };
        if let Err(e) = f.write_all(data) {
            drop(f);
            let _ = std::fs::remove_file(&path);
            return Err(e);
        }
        let used = self.used.fetch_add(data.len() as u64, Ordering::Relaxed) + data.len() as u64;
        if used > self.capacity {
            self.collect();
        }
        Ok(())
    }

    /// 占用超过容量时回收：按修改时间删除最旧的文件，直到降到容量的 90%。同一时间只有一个回收在跑。
    pub fn collect(&self) {
        if self.collecting.swap(true, Ordering::AcqRel) {
            return;
        }
        let target = self.capacity / 10 * 9;
        match scan(&self.dir.join("objects")) {
            Ok(mut files) => {
                files.sort_by_key(|(_, _, mtime)| *mtime);
                let mut used: u64 = files.iter().map(|(_, size, _)| size).sum();
                for (path, size, _) in files {
                    if used <= target {
                        break;
                    }
                    if std::fs::remove_file(&path).is_ok() {
                        used -= size;
                        if let Some(id) = path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .and_then(|n| hex::decode(n).ok())
                            .and_then(|b| ObjectId::from_bytes(&b).ok())
                        {
                            self.memory.invalidate(&id);
                        }
                    }
                }
                self.used.store(used, Ordering::Relaxed);
            }
            Err(e) => tracing::warn!(error = %e, "scanning the object cache failed"),
        }
        self.collecting.store(false, Ordering::Release);
    }
}

/// 列出目录下的全部对象文件：(路径, 大小, 修改时间)。
fn scan(dir: &Path) -> std::io::Result<Vec<(PathBuf, u64, SystemTime)>> {
    let mut out = Vec::new();
    for shard in std::fs::read_dir(dir)? {
        let shard = shard?;
        if !shard.file_type()?.is_dir() {
            continue;
        }
        for f in std::fs::read_dir(shard.path())? {
            let f = f?;
            let meta = f.metadata()?;
            if meta.is_file() {
                out.push((f.path(), meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(i: u32, len: usize) -> (ObjectId, Bytes) {
        let mut v = vec![0u8; len];
        v[..4].copy_from_slice(&i.to_le_bytes());
        (ObjectId::of_blob(&v), Bytes::from(v))
    }

    #[test]
    fn put_get_and_survive_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cas = Cas::open(dir.path(), 1 << 20, 1 << 20).expect("open");
        let (id, data) = obj(1, 100);
        assert!(cas.get(&id).is_none());
        assert!(!cas.contains(&id));
        cas.put(id, &data).expect("put");
        cas.put(id, &data).expect("idempotent");
        assert_eq!(cas.used(), 100);
        assert_eq!(cas.get(&id), Some(data.clone()));
        assert!(cas.contains(&id));
        // 重新打开：从磁盘读到，占用重新统计
        let again = Cas::open(dir.path(), 1 << 20, 1 << 20).expect("reopen");
        assert_eq!(again.used(), 100);
        assert_eq!(again.get(&id), Some(data));
    }

    #[test]
    fn corrupt_files_are_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cas = Cas::open(dir.path(), 1 << 20, 0).expect("open");
        let (id, data) = obj(2, 64);
        cas.put(id, &data).expect("put");
        std::fs::write(cas.path(&id), b"garbage").expect("corrupt");
        let fresh = Cas::open(dir.path(), 1 << 20, 0).expect("reopen without memory");
        assert!(fresh.get(&id).is_none());
        assert!(!cas.path(&id).exists(), "removed");
    }

    #[test]
    fn remove_deletes_memory_and_disk_copies() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cas = Cas::open(dir.path(), 1 << 20, 1 << 20).expect("open");
        let data = Bytes::from_static(b"secret");
        let id = ObjectId::of_blob(&data);
        cas.put(id, &data).expect("put");
        assert!(cas.contains(&id) && cas.used() == 6);
        cas.remove(&id);
        assert!(!cas.contains(&id));
        assert_eq!((cas.get(&id), cas.used()), (None, 0));
        cas.remove(&id);
    }

    #[test]
    fn collect_removes_least_recently_used() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cas = Cas::open(dir.path(), 1000, 0).expect("open");
        let objs: Vec<_> = (0..9).map(|i| obj(i, 100)).collect();
        for (i, (id, data)) in objs.iter().enumerate() {
            cas.put(*id, data).expect("put");
            // 修改时间区分先后
            let t = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000 + i as u64);
            std::fs::File::options()
                .write(true)
                .open(cas.path(id))
                .expect("open")
                .set_modified(t)
                .expect("mtime");
        }
        assert_eq!(cas.used(), 900);
        // 读一次最旧的：它变成最近使用
        assert!(cas.get(&objs[0].0).is_some());
        let (id, data) = obj(100, 200);
        cas.put(id, &data).expect("over capacity");
        assert!(cas.used() <= 900, "{}", cas.used());
        assert!(cas.contains(&objs[0].0), "recently read object kept");
        assert!(!cas.path(&objs[1].0).exists(), "oldest removed");
        assert!(cas.contains(&id));
    }

    #[test]
    fn scan_skips_stray_files_and_open_reports_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("objects")).expect("dir");
        std::fs::write(dir.path().join("objects").join("stray"), b"x").expect("stray");
        assert!(scan(&dir.path().join("objects")).expect("scan").is_empty());
        let file = tempfile::NamedTempFile::new().expect("file");
        assert!(Cas::open(&file.path().join("sub"), 1, 1).is_err());
    }
}
