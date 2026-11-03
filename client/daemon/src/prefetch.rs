//! WatchRefs 预取：工作副本解析过的 (服务端, 仓库, ref)，daemon 订阅它的变化；每次前进时把变化了的
//! 目录树和不超过 `max_blob_bytes` 的文件取进本地缓存，之后 `scn sync` 直接命中。
//!
//! 只沿变化的路径下钻（子树 id 没变就跳过）。第一次看到某个 ref 时只记下根目录树，不预取整棵树
//! （检出本身会读取它们）。订阅空闲 `idle` 后（期间没有再解析过这个 ref）自动取消。
//!
//! 预取用最近一次解析这个 ref 的调用者的身份回源，只保存在内存里。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use scion_client::{Auth, Client};
use scion_object::{EntryMode, ObjectId, Tree};

use crate::Daemon;
use crate::upstream::Upstream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchOptions {
    /// 一次 ref 变化最多预取的对象数。
    pub max_objects: usize,
    /// 同时预取不超过这个大小的文件；0 只预取目录树。
    pub max_blob_bytes: u64,
    /// 订阅空闲多久后取消。
    pub idle: Duration,
}

impl Default for PrefetchOptions {
    fn default() -> Self {
        Self {
            max_objects: 20_000,
            max_blob_bytes: 256 << 10,
            idle: Duration::from_secs(30 * 60),
        }
    }
}

type Key = (String, String, String);

struct Watch {
    last_used: Instant,
    auth: Arc<Mutex<Auth>>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
pub struct Prefetcher {
    opts: PrefetchOptions,
    watches: Mutex<HashMap<Key, Watch>>,
}

impl Prefetcher {
    pub fn new(opts: PrefetchOptions) -> Self {
        Self {
            opts,
            watches: Mutex::new(HashMap::new()),
        }
    }

    /// 登记（或刷新）一个 ref 的订阅。
    pub fn watch(&self, daemon: &Arc<Daemon>, up: Arc<Upstream>, repo: &str, reference: &str, auth: Auth) {
        let Ok(mut watches) = self.watches.lock() else {
            return;
        };
        let key = (up.url.clone(), repo.to_owned(), reference.to_owned());
        if let Some(w) = watches.get_mut(&key)
            && !w.task.is_finished()
        {
            w.last_used = Instant::now();
            if let Ok(mut a) = w.auth.lock() {
                *a = auth;
            }
            return;
        }
        let auth = Arc::new(Mutex::new(auth));
        let task = tokio::spawn(follow(
            daemon.clone(),
            up,
            repo.to_owned(),
            reference.to_owned(),
            auth.clone(),
            self.opts,
        ));
        watches.insert(
            key,
            Watch {
                last_used: Instant::now(),
                auth,
                task,
            },
        );
    }

    /// 取消空闲的订阅，返回剩余的订阅数。
    pub fn reap(&self) -> usize {
        let Ok(mut watches) = self.watches.lock() else {
            return 0;
        };
        let idle = self.opts.idle;
        watches.retain(|_, w| {
            let keep = w.last_used.elapsed() < idle && !w.task.is_finished();
            if !keep {
                w.task.abort();
            }
            keep
        });
        watches.len()
    }

    pub fn len(&self) -> usize {
        self.watches.lock().map(|w| w.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 取消全部订阅（daemon 退出时）。
    pub fn stop(&self) {
        if let Ok(mut watches) = self.watches.lock() {
            for (_, w) in watches.drain() {
                w.task.abort();
            }
        }
    }
}

async fn follow(
    daemon: Arc<Daemon>,
    up: Arc<Upstream>,
    repo: String,
    reference: String,
    auth: Arc<Mutex<Auth>>,
    opts: PrefetchOptions,
) {
    let mut backoff = Duration::from_millis(500);
    let mut root: Option<ObjectId> = None;
    loop {
        let current_auth = auth.lock().map(|a| a.clone()).unwrap_or_default();
        let watch = match up.client().await {
            Ok(c) => {
                c.with_repo(repo.clone())
                    .with_auth(current_auth)
                    .watch_refs(&[&reference])
                    .await
            }
            Err(s) => Err(scion_client::Error::from(s)),
        };
        let mut w = match watch {
            Ok(w) => w,
            Err(e) if e.is_not_found() => return,
            Err(e) => {
                tracing::debug!(%repo, r#ref = %reference, error = %e, "prefetch subscription failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        backoff = Duration::from_millis(500);
        loop {
            let change = match w.next().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(%repo, r#ref = %reference, error = %e, "prefetch subscription ended");
                    break;
                }
            };
            let Some(new) = change.root_tree else {
                continue;
            };
            let old = root.replace(new);
            if let Some(old) = old.filter(|o| *o != new) {
                let client = w_client(&up, &repo, &auth).await;
                if let Some(client) = client {
                    match warm(&daemon, &client, &up.url, &repo, old, new, opts).await {
                        Ok(n) => tracing::debug!(%repo, r#ref = %reference, objects = n, "prefetched"),
                        Err(e) => tracing::debug!(%repo, r#ref = %reference, error = %e, "prefetch failed"),
                    }
                }
            }
        }
        tokio::time::sleep(backoff).await;
    }
}

async fn w_client(up: &Upstream, repo: &str, auth: &Mutex<Auth>) -> Option<Client> {
    let a = auth.lock().map(|a| a.clone()).unwrap_or_default();
    up.client().await.ok().map(|c| c.with_repo(repo).with_auth(a))
}

/// 预取从 `old` 到 `new` 变化的目录树与小文件，返回取到（或已在本地）的对象数。
pub(crate) async fn warm(
    daemon: &Daemon,
    client: &Client,
    upstream: &str,
    repo: &str,
    old: ObjectId,
    new: ObjectId,
    opts: PrefetchOptions,
) -> Result<usize, tonic::Status> {
    let (upstream, repo): (Arc<str>, Arc<str>) = (Arc::from(upstream), Arc::from(repo));
    let mut seen = 0;
    let mut level = vec![(Some(old), new)];
    while !level.is_empty() && seen < opts.max_objects {
        let mut want: Vec<ObjectId> = level.iter().map(|(_, n)| *n).collect();
        want.extend(level.iter().filter_map(|(o, _)| *o));
        want.sort_unstable_by_key(|id| id.to_bytes());
        want.dedup();
        let got = daemon.objects(client, &upstream, &repo, &want).await;
        seen += level.len();
        let tree = |id: &ObjectId| -> Result<Tree, tonic::Status> {
            match got.get(id) {
                Some(Ok(b)) => Tree::decode(b).map_err(|e| tonic::Status::data_loss(format!("tree {id}: {e}"))),
                Some(Err(e)) => Err(e.clone()),
                None => Err(tonic::Status::internal(format!("tree {id} not fetched"))),
            }
        };
        let mut next = Vec::new();
        let mut blobs = Vec::new();
        for (o, n) in &level {
            let new_tree = tree(n)?;
            let old_tree = o.as_ref().map(tree).transpose()?;
            for e in new_tree.entries() {
                let before = old_tree.as_ref().and_then(|t| t.get(&e.name));
                if before.is_some_and(|b| b.id == e.id) {
                    continue;
                }
                if e.mode == EntryMode::Tree {
                    next.push((before.filter(|b| b.mode == EntryMode::Tree).map(|b| b.id), e.id));
                } else if opts.max_blob_bytes > 0 && e.size <= opts.max_blob_bytes {
                    blobs.push(e.id);
                }
            }
        }
        blobs.truncate(opts.max_objects.saturating_sub(seen));
        if !blobs.is_empty() {
            blobs.sort_unstable_by_key(|id| id.to_bytes());
            blobs.dedup();
            for r in daemon.objects(client, &upstream, &repo, &blobs).await.into_values() {
                r?;
            }
            seen += blobs.len();
        }
        next.truncate(opts.max_objects.saturating_sub(seen));
        level = next;
    }
    Ok(seen)
}
