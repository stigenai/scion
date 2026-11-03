//! 服务端对外声明的限额与拓扑提示。

use scion_proto::v1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_batch_objects: u32,
    pub max_chunk_bytes: u64,
    pub max_changes_per_changeset: u32,
    pub max_inline_bytes: u64,
    /// UploadBlobs 单个对象上限。M1 在内存中整体校验后写入；更大的文件在分块存储加入后支持。
    pub max_upload_object_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_batch_objects: 1_000,
            max_chunk_bytes: 1 << 20,
            max_changes_per_changeset: 100_000,
            max_inline_bytes: 1 << 20,
            max_upload_object_bytes: 256 << 20,
        }
    }
}

impl From<&Limits> for v1::Limits {
    fn from(l: &Limits) -> Self {
        v1::Limits {
            max_batch_objects: l.max_batch_objects,
            max_chunk_bytes: l.max_chunk_bytes,
            max_changes_per_changeset: l.max_changes_per_changeset,
            max_inline_bytes: l.max_inline_bytes,
            max_upload_object_bytes: l.max_upload_object_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadEndpoint {
    pub url: String,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub limits: Limits,
    pub read_endpoints: Vec<ReadEndpoint>,
    /// 服务端实现版本，仅供展示。
    pub server_version: String,
    /// 进程内缓存的提交记录条数上限（记录不可变，缓存无需失效）。
    pub record_cache_capacity: u64,
    /// 启动后在后台把各仓库的提交记录预热进缓存。
    pub warm_record_cache: bool,
    /// 写请求结果（幂等表）的保留时间。
    pub idempotency_ttl: std::time::Duration,
    /// 清理过期幂等记录的间隔。
    pub janitor_interval: std::time::Duration,
    /// 合入批处理参数（每个 ref 一个批处理任务）。
    pub land: scion_land::CoordinatorConfig,
    /// 集群模式（多个节点共享存储）：每个 ref 由持有租约的节点处理，其它节点转发。`None` 为单节点。
    pub cluster: Option<scion_land::ClusterConfig>,
    /// 全局在途请求上限，超过时立即拒绝（RESOURCE_EXHAUSTED，可重试）。
    pub max_concurrent_requests: usize,
    /// 按仓库的写入速率限制（上传、创建提交、合入各算一次）；`None` 不限制。
    pub repo_write_rate: Option<WriteRate>,
    /// 按调用者（认证过的主体，匿名时为来源 IP）的请求速率限制，所有 RPC 各算一次；`None` 不限制。
    pub tenant_rate: Option<WriteRate>,
    /// 每个调用者的在途请求上限；`None` 不限制。
    pub max_concurrent_per_tenant: Option<usize>,
    /// The longest a server stream (WatchRefs, GetObjects, ReadFile, Diff, Archive, Grep) stays open.
    /// It then ends with UNAVAILABLE and the client reconnects. `None` sets no limit.
    pub max_stream_lifetime: Option<std::time::Duration>,
    /// Open server streams per caller (the key of `max_concurrent_per_tenant`); `None` sets no limit.
    pub max_streams_per_tenant: Option<usize>,
    /// 集群共享密钥：节点间转发合入时带上，收到的节点据此确认是本集群转发的，不再重复计入限流。
    /// 未设置时转发的请求照常计数（调用方自带的转发头也绕不过限流）。
    pub cluster_secret: Option<String>,
    /// 请求处理时间上限：取它与客户端 deadline（`grpc-timeout`）中较小的一个，超时返回
    /// DEADLINE_EXCEEDED。`None` 只按客户端的 deadline。流式响应只限制到开始返回为止。
    pub max_request_timeout: Option<std::time::Duration>,
    /// WatchRefs 轮询元数据库的间隔，即集群中其它节点的合入通知到订阅者的最大延迟
    /// （本节点的合入立即通知）。只轮询有订阅者的仓库。
    pub ref_poll_interval: std::time::Duration,
    /// L3 进程内的对象缓存与后端读并发上限。
    pub blob_cache: scion_store::BlobCacheConfig,
    /// 认证与鉴权（默认不要求认证）。
    pub auth: scion_auth::GuardConfig,
    /// 从元数据库重新加载访问控制策略的间隔（其它节点修改策略后的最大生效延迟）。
    pub policy_poll_interval: std::time::Duration,
    /// Purge 未指定宽限期时，从安排到物理删除对象字节的等待时间（期间可以撤销）。
    pub purge_grace: std::time::Duration,
    /// 删除的仓库保留多久（期间可以恢复），之后 janitor 清除数据、释放名字。不小于 2 小时。
    pub repo_delete_grace: std::time::Duration,
    /// ref 变化的推送目标与事件保留期。
    pub events: crate::events::EventsConfig,
    /// 合入前检查（外部 webhook）；`None` 不检查。
    pub pre_land: Option<crate::prehook::PreLandConfig>,
    /// Serve git's smart HTTP protocol on the same listener (the git gateway).
    pub git: bool,
    /// How often an open WatchRefs stream authenticates its caller again and checks read access,
    /// also when no ref changes. A revoked caller loses the stream at the next tick.
    pub stream_recheck_interval: std::time::Duration,
}

/// 令牌桶参数：每秒补充 `per_second` 个，最多积攒 `burst` 个。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRate {
    pub per_second: u32,
    pub burst: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            read_endpoints: Vec::new(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            record_cache_capacity: 1_000_000,
            warm_record_cache: true,
            idempotency_ttl: std::time::Duration::from_secs(24 * 3600),
            janitor_interval: std::time::Duration::from_secs(600),
            land: scion_land::CoordinatorConfig::default(),
            cluster: None,
            max_concurrent_requests: 10_000,
            repo_write_rate: None,
            tenant_rate: None,
            max_concurrent_per_tenant: None,
            max_stream_lifetime: Some(std::time::Duration::from_secs(3600)),
            max_streams_per_tenant: None,
            cluster_secret: None,
            max_request_timeout: Some(std::time::Duration::from_secs(60)),
            ref_poll_interval: std::time::Duration::from_secs(1),
            blob_cache: scion_store::BlobCacheConfig::default(),
            purge_grace: std::time::Duration::from_secs(24 * 3600),
            repo_delete_grace: std::time::Duration::from_secs(24 * 3600),
            pre_land: None,
            git: true,
            stream_recheck_interval: std::time::Duration::from_secs(30),
            events: crate::events::EventsConfig {
                retention: std::time::Duration::from_secs(7 * 24 * 3600),
                ..Default::default()
            },
            auth: scion_auth::GuardConfig::default(),
            policy_poll_interval: std::time::Duration::from_secs(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_default_chunk_not_larger_than_grpc_default_message() {
        // tonic 默认最大解码消息为 4 MiB，分块必须留出余量
        assert!(Limits::default().max_chunk_bytes < 4 << 20);
        assert!(Limits::default().max_batch_objects > 0);
    }

    #[test]
    fn limits_into_proto_copies_all_fields() {
        let l = Limits {
            max_batch_objects: 1,
            max_chunk_bytes: 2,
            max_changes_per_changeset: 3,
            max_inline_bytes: 4,
            max_upload_object_bytes: 5,
        };
        let p: v1::Limits = (&l).into();
        assert_eq!(
            (
                p.max_batch_objects,
                p.max_chunk_bytes,
                p.max_changes_per_changeset,
                p.max_inline_bytes,
                p.max_upload_object_bytes
            ),
            (1, 2, 3, 4, 5)
        );
    }

    #[test]
    fn server_config_default_version_is_crate_version() {
        let c = ServerConfig::default();
        assert_eq!(c.server_version, env!("CARGO_PKG_VERSION"));
        assert!(c.read_endpoints.is_empty());
        assert!(c.record_cache_capacity > 0);
    }
}
