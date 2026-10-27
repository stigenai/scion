//! 两个节点共享存储（集群模式）：非 leader 节点把合入转给 leader；转发来的请求不再转发；
//! leader 正常退出后租约立即释放，另一个节点接管。

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use scion_api::{App, ServerConfig, serve};
use scion_land::{ChangesetWriter, ClusterConfig};
use scion_object::{Changeset, EntryMode, FileChange, ObjectId, Signature};
use scion_proto::v1::land_service_client::LandServiceClient;
use scion_proto::v1::{GetLandStatusRequest, LandRequest, LandState};
use scion_store::{BlobStore, IndexedObject, MemStore, MetaStore};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tonic_types::StatusExt;

const REPO: &str = "mono";

struct Node {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Node {
    async fn start(store: Arc<MemStore>) -> Node {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let config = ServerConfig {
            cluster: Some(ClusterConfig::new(url.clone())),
            warm_record_cache: false,
            // 不靠轮询：转发合入的节点自己通知订阅者
            ref_poll_interval: std::time::Duration::from_secs(3600),
            ..ServerConfig::default()
        };
        let app = App {
            config: Arc::new(config),
            meta: store.clone(),
            blobs: store,
        };
        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(serve(listener, app, async {
            let _ = rx.await;
        }));
        Node {
            url,
            stop: Some(tx),
            task,
        }
    }

    async fn client(&self) -> LandServiceClient<Channel> {
        LandServiceClient::connect(self.url.clone()).await.expect("connect")
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        self.task.await.expect("join").expect("serve");
    }
}

async fn commit(store: &Arc<MemStore>, parent: Option<ObjectId>, path: &str, n: u8) -> ObjectId {
    let data = format!("{path}\n");
    let id = ObjectId::of_blob(data.as_bytes());
    store.put(id, Bytes::from(data.clone())).await.expect("blob");
    store
        .index_objects(
            REPO,
            &[IndexedObject {
                id,
                size: data.len() as u64,
            }],
        )
        .await
        .expect("index");
    let sig = Signature {
        name: "a".into(),
        email: "a@x".into(),
        time_ms: 0,
        tz_offset_minutes: 0,
    };
    let cs = Changeset::new(
        parent.into_iter().collect(),
        sig.clone(),
        sig,
        path,
        [n; 16],
        vec![FileChange::put(path, EntryMode::File, id, data.len() as u64)],
        BTreeMap::new(),
    )
    .expect("cs");
    ChangesetWriter::new(store.clone(), store.clone())
        .create(REPO, &cs)
        .await
        .expect("create")
        .id
}

fn land(id: ObjectId, request_id: &str) -> LandRequest {
    LandRequest {
        repo: REPO.into(),
        request_id: request_id.into(),
        r#ref: "main".into(),
        changeset_id: Bytes::copy_from_slice(&id.to_bytes()),
        fast_forward_only: false,
        dry_run: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_forward_lands_to_the_leader() {
    let store = Arc::new(MemStore::new());
    store.create_repo(REPO).await.expect("repo");
    let a = Node::start(store.clone()).await;
    let b = Node::start(store.clone()).await;
    let base = commit(&store, None, "base", 1).await;
    let x = commit(&store, Some(base), "x", 2).await;
    let y = commit(&store, Some(base), "y", 3).await;

    // a 先处理 main：成为 leader
    let r = a
        .client()
        .await
        .land(land(base, "r-base"))
        .await
        .expect("a")
        .into_inner();
    assert_eq!(r.ref_version, 1);
    let mut watch = scion_proto::v1::ref_service_client::RefServiceClient::connect(b.url.clone())
        .await
        .expect("connect")
        .watch_refs(scion_proto::v1::WatchRefsRequest {
            repo: REPO.into(),
            refs: vec!["main".into()],
            known_versions: [("main".to_string(), 1)].into(),
        })
        .await
        .expect("watch")
        .into_inner();
    assert!(watch.message().await.expect("snapshot").expect("msg").snapshot);
    // 发给 b 的请求被转给 a
    let r = b
        .client()
        .await
        .land(land(x, "r-x"))
        .await
        .expect("forwarded")
        .into_inner();
    assert_eq!(r.ref_version, 2);
    // b 上的订阅者立即收到（不等轮询）
    let ev = tokio::time::timeout(std::time::Duration::from_secs(5), watch.message())
        .await
        .expect("timely")
        .expect("ok")
        .expect("event");
    assert_eq!(ev.events[0].version, 2);
    drop(watch);
    // 任何节点都能查询结果
    let st = b
        .client()
        .await
        .get_land_status(GetLandStatusRequest {
            repo: REPO.into(),
            request_id: "r-x".into(),
        })
        .await
        .expect("status")
        .into_inner();
    assert_eq!(st.state, LandState::Landed as i32);

    // 转发来的请求不再转发：返回可重试的错误并告知 leader
    let mut req = tonic::Request::new(land(y, "r-y"));
    req.metadata_mut().insert(
        scion_api::FORWARDED_HEADER,
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let err = b.client().await.land(req).await.expect_err("no second hop");
    assert_eq!(err.code(), tonic::Code::Unavailable);
    let info = err.get_error_details().error_info().cloned().expect("error info");
    assert_eq!(
        (info.reason.as_str(), info.metadata["leader"].as_str()),
        ("LAND_CONTENTION", a.url.as_str())
    );

    // a 正常退出：租约释放，b 立即接管
    let a_url = a.url.clone();
    a.shutdown().await;
    let r = b
        .client()
        .await
        .land(land(y, "r-y"))
        .await
        .expect("b leads")
        .into_inner();
    assert_eq!(r.ref_version, 3);
    assert_ne!(a_url, b.url);
    b.shutdown().await;
}
