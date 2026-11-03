//! 订阅 ref 变化（WatchRefs）。连接断开后自动带上已知版本重新订阅，不会漏掉变化。

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use scion_object::ObjectId;
use scion_proto::v1::ref_service_client::RefServiceClient;
use scion_proto::v1::{WatchRefsRequest, WatchRefsResponse};
use tonic::Code;
use tonic::codec::Streaming;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::ops::id_from;

/// 一个 ref 的新状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefChange {
    pub name: String,
    pub changeset: ObjectId,
    /// 提交的根目录树；服务端不知道时为 `None`。
    pub root_tree: Option<ObjectId>,
    pub version: u64,
    /// 是否来自订阅（或重新订阅）时的快照，而不是实时变化。
    pub initial: bool,
}

const MIN_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// ref 变化的订阅。用 [`Client::watch_refs`] 创建，反复调用 [`RefWatch::next`] 取变化。
pub struct RefWatch {
    client: Client,
    repo: String,
    refs: Vec<String>,
    /// 每个 ref 已交给调用方的版本，重新订阅时发给服务端。
    known: HashMap<String, u64>,
    stream: Option<Streaming<WatchRefsResponse>>,
    pending: VecDeque<RefChange>,
    /// 连续重连失败的上限；`None` 不限制。
    max_reconnects: Option<u32>,
}

impl Client {
    /// 订阅 `refs`（为空表示仓库中的全部 ref）的变化。先返回当前状态（`initial = true`），之后是每次前进
    /// 后的新状态。同一 ref 短时间内多次前进时，可能只收到最新的状态。
    pub async fn watch_refs(&self, refs: &[&str]) -> Result<RefWatch> {
        let mut w = RefWatch {
            client: self.clone(),
            repo: self.repo()?,
            refs: refs.iter().map(|r| (*r).to_owned()).collect(),
            known: HashMap::new(),
            stream: None,
            pending: VecDeque::new(),
            max_reconnects: None,
        };
        w.connect().await?;
        Ok(w)
    }
}

/// 连接层面的错误（服务端重启、网络中断），重新订阅即可。
fn reconnectable(e: &Error) -> bool {
    e.is_retryable()
        || matches!(
            e,
            Error::Rpc {
                code: Code::Unknown | Code::Cancelled | Code::Internal,
                reason: None,
                ..
            }
        )
}

impl RefWatch {
    /// 连续重连失败超过 `n` 次时 [`RefWatch::next`] 返回错误（默认一直重试）。
    pub fn max_reconnects(mut self, n: u32) -> Self {
        self.max_reconnects = Some(n);
        self
    }

    /// 每个 ref 已收到的最新版本。
    pub fn known_versions(&self) -> &HashMap<String, u64> {
        &self.known
    }

    async fn connect(&mut self) -> Result<()> {
        let req = WatchRefsRequest {
            repo: self.repo.clone(),
            refs: self.refs.clone(),
            known_versions: self.known.clone(),
        };
        let stream = RefServiceClient::new(self.client.channel.clone())
            .watch_refs(self.client.request(req)?)
            .await?
            .into_inner();
        self.stream = Some(stream);
        Ok(())
    }

    /// 把一条响应里比已知版本新的事件放进队列。
    fn accept(&mut self, resp: WatchRefsResponse) -> Result<()> {
        for e in resp.events {
            let known = self.known.get(&e.name).copied().unwrap_or(0);
            if e.version <= known {
                continue;
            }
            let root_tree = if e.root_tree.is_empty() {
                None
            } else {
                Some(id_from(&e.root_tree, "root tree")?)
            };
            let change = RefChange {
                changeset: id_from(&e.changeset_id, "changeset id")?,
                root_tree,
                version: e.version,
                initial: resp.snapshot,
                name: e.name,
            };
            self.known.insert(change.name.clone(), change.version);
            self.pending.push_back(change);
        }
        Ok(())
    }

    /// 下一个变化。连接断开时自动重新订阅（指数退避）；仓库不存在等不可重试的错误直接返回。
    pub async fn next(&mut self) -> Result<RefChange> {
        let mut failures = 0u32;
        let mut backoff = MIN_BACKOFF;
        loop {
            if let Some(c) = self.pending.pop_front() {
                return Ok(c);
            }
            let item = match self.stream.as_mut() {
                Some(s) => s.message().await.map_err(Error::from),
                None => Ok(None),
            };
            let err = match item {
                Ok(Some(resp)) => {
                    self.accept(resp)?;
                    continue;
                }
                // 服务端结束了流（例如正常关闭）：重新订阅
                Ok(None) => None,
                Err(e) if reconnectable(&e) => Some(e),
                Err(e) => return Err(e),
            };
            self.stream = None;
            loop {
                if let Some(max) = self.max_reconnects
                    && failures >= max
                {
                    return Err(err.unwrap_or_else(|| Error::Protocol("ref watch stream ended".into())));
                }
                failures += 1;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                match self.connect().await {
                    Ok(()) => break,
                    Err(e) if reconnectable(&e) => continue,
                    Err(e) => return Err(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scion_proto::v1::RefEvent;
    use std::collections::HashMap;

    fn ev(name: &str, version: u64, root: bool) -> RefEvent {
        RefEvent {
            name: name.into(),
            changeset_id: Bytes::copy_from_slice(&ObjectId::of_blob(name.as_bytes()).to_bytes()),
            root_tree: if root {
                Bytes::copy_from_slice(&ObjectId::of_blob(b"root").to_bytes())
            } else {
                Bytes::new()
            },
            version,
        }
    }

    #[test]
    fn reconnectable_errors() {
        let rpc = |code| Error::Rpc {
            code,
            reason: None,
            message: String::new(),
            metadata: HashMap::new(),
        };
        assert!(reconnectable(&rpc(Code::Unavailable)));
        assert!(reconnectable(&rpc(Code::Unknown)));
        assert!(reconnectable(&rpc(Code::Internal)));
        assert!(!reconnectable(&rpc(Code::NotFound)));
        assert!(!reconnectable(&rpc(Code::PermissionDenied)));
    }

    #[tokio::test]
    async fn accept_skips_known_versions_and_parses_ids() {
        // 不需要连接：只测试事件处理
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let mut w = RefWatch {
            client: Client::for_tests(channel),
            repo: "r".into(),
            refs: vec![],
            known: HashMap::from([("a".to_string(), 2)]),
            stream: None,
            pending: VecDeque::new(),
            max_reconnects: Some(0),
        };
        w.accept(WatchRefsResponse {
            events: vec![ev("a", 2, true), ev("a", 3, true), ev("b", 1, false)],
            snapshot: true,
        })
        .expect("accept");
        let got: Vec<_> = w
            .pending
            .iter()
            .map(|c| (c.name.clone(), c.version, c.initial))
            .collect();
        assert_eq!(got, [("a".into(), 3, true), ("b".into(), 1, true)]);
        assert_eq!(w.pending[0].root_tree, Some(ObjectId::of_blob(b"root")));
        assert_eq!(w.pending[1].root_tree, None);
        assert_eq!(w.known_versions().get("a"), Some(&3));
        let mut bad = ev("c", 1, false);
        bad.changeset_id = Bytes::from_static(b"x");
        assert!(matches!(
            w.accept(WatchRefsResponse {
                events: vec![bad],
                snapshot: false
            }),
            Err(Error::Protocol(_))
        ));
        // 队列中的变化先返回；之后没有流且不允许重连：报错
        assert_eq!(w.next().await.expect("a").name, "a");
        assert_eq!(w.next().await.expect("b").name, "b");
        assert!(matches!(w.next().await, Err(Error::Protocol(_))));
    }
}
