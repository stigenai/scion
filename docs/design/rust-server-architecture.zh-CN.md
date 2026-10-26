# Scion 服务端架构设计（Rust）

[English](rust-server-architecture.md) | 简体中文

> 版本：Draft v0.1 · 2026-10-16
> 前置阅读：[`scion-research.md`](./scion-research.md)（Scion 的背景、原文要点和整体方案）
> 参考实现：CarterT27/scion（Apache 2.0）。**只借鉴思路，不拷贝代码**（2026-10-16 决定），本项目代码全部自行实现。

---

## 0. 设计目标

| # | 目标 | 衡量标准 |
|---|---|---|
| G1 | **协议与语言无关**，官方只提供 Rust 默认客户端 | 默认客户端 `scn` 只依赖公开协议（见 [`rust-client-architecture.md`](./rust-client-architecture.zh-CN.md) §0）；不需要实现规范编码也能完成 clone/commit/land，以后任何语言都可以按协议自行实现客户端 |
| G2 | 高可用 | 单 AZ 故障无感；RPO=0；元数据 RTO 不超过 60 秒；无状态层故障秒级恢复 |
| G3 | 高并发 | 单 ref 每秒 200 次以上 land；L2 缓存单节点 20 万对象读/秒以上；10 万合入/天、3,000 万 checkout/天 |
| G4 | 提交成本与仓库大小无关 | 创建 changeset 的成本 = O(改动路径数 × 目录深度) |
| G5 | 简单部署 | Lite 模式单进程零依赖；`docker compose up` 起 Standard 全栈；生产用 Helm；所有服务共用一个二进制，靠子命令区分角色；**不依赖任何商业许可的组件** |

非目标（v1）：Git 协议兼容、离线和多主写入、代码 review UI、CI 调度。

**架构图**（PNG 用于浏览，SVG 是可编辑的源文件）：

![Scion 独立部署架构](./images/arch-01-standalone.png)

![部署三档](./images/arch-02-deployment-tiers.png)

外部集成见 [`starhub-integration.md`](./starhub-integration.zh-CN.md)（图 3、图 4）。

---

## 1. 两条核心原则

### 1.1 协议优先（Protocol is the product）
- `proto/` 加上 `SPEC.md` 加上一致性测试套件，**才是系统真正的接口**。Rust 服务端只是其中一种实现。
- proto 用 **buf** 管理：每个 PR 都跑 `buf lint` 和 `buf breaking`，**禁止破坏性变更**；需要新语义就加字段，或者开 `v2` 包。
- 官方只维护 **Rust SDK 与 CLI**（`scion-client` / `scn`）。proto 以 buf module 形式发布，其它语言的使用方可以自行生成 stub。

### 1.2 瘦客户端（Thin client）
所有难做对的逻辑都放在服务端，客户端只需要会做三件事：

| 客户端必须会 | 难度 |
|---|---|
| 计算 blob ID = `BLAKE3(原始文件字节)` | 各语言都有现成的 BLAKE3 库，一行代码 |
| zstd 解压（服务端也可以按协商返回未压缩数据，见 §3.4） | 各语言都有库 |
| 调用 gRPC | 由 proto 生成 |

**客户端不需要**：编码 Tree/Changeset、计算 tree hash、做 rebase 或 merge、算 merge-base。服务端返回 tree 时，会**同时**给出规范编码的原始字节（供想校验的客户端用）和解码后的 proto 结构（供普通客户端直接用）。

---

## 2. 系统总览

```
            ┌──────────── 客户端 ─────────────────┐
            │ scn CLI / scn daemon / Rust SDK    │
            │ Agent / CI / Web(gRPC-Web, JSON)   │
            └───────────────┬────────────────────┘
                            │ gRPC(HTTP/2) | gRPC-Web | REST/JSON
                            │ TLS + Bearer Token
┌───────────────────────────▼───────────────────────────────┐
│ scion cache   (L2, 区域级)   同一个 ObjectService           │
│   foyer(内存+NVMe) · rendezvous 分片 · singleflight · 对冲  │
└───────────────────────────┬───────────────────────────────┘
                            │ 内部 gRPC + mTLS
┌───────────────────────────▼───────────────────────────────┐
│ scion server  (L3, 权威, 无状态)                            │
│  Gateway 层：tonic + tonic-web + axum(REST)                 │
│  中间件：认证 · 鉴权 · 限流 · 并发上限 · deadline · 幂等 · 追踪 │
│  服务：Object · Write · Land · Ref · Query · Auth · Admin   │
│  引擎：TreeBuilder · Merge · LandCoordinator · Derive       │
├────────────────────────────────────────────────────────────┤
│ scion deriver  (后台：跟随 cs_log 计算派生数据)              │
│ scion janitor  (后台：孤儿对象清理、redaction purge、快照)   │
├────────────────────────────────────────────────────────────┤
│ MetaStore 三档: SQLite(Lite) | PostgreSQL(Standard, 默认)    │
│                 | YugabyteDB(Distributed)  — 同一套 sqlx 代码  │
│ BlobStore: S3 兼容(AWS/MinIO/Ceph/OSS…)·GCS·Azure·本地 FS   │
│ EventBus : NATS JetStream (ref 变更、合入事件)               │
└────────────────────────────────────────────────────────────┘
```

**单一二进制**：`scion server | cache | deriver | janitor | import | admin`。部署时同一个镜像按角色启动，运维面最小。

**Lite 模式**：`scion server --lite --data-dir ~/.scion` 在**一个进程**里运行 server、deriver 和 janitor，元数据用 SQLite，对象存在本地文件系统。适用于开发、单机、边缘设备，以及嵌入 CSGLite 这类本地产品。

---

## 3. 协议设计（跨语言契约）

### 3.1 传输
| 方式 | 用途 | 实现 |
|---|---|---|
| **gRPC / HTTP2** | 主协议，所有 SDK 和 daemon 使用 | `tonic` |
| **gRPC-Web** | 浏览器 | `tonic-web` |
| **REST/JSON** | 脚本和 curl：读文件、log、land、ref 查询等常用子集 | `axum`，proto↔JSON 用 `pbjson` 生成 serde，字段与 proto JSON 映射一致 |

所有流式 RPC 都必须是**可恢复的**：请求携带 offset 或 cursor，断开后可以从断点继续。

### 3.2 服务划分（package `scion.v1`）

```proto
service ServerInfoService {
  rpc GetServerInfo(GetServerInfoRequest) returns (ServerInfo); // 协议版本、特性、限额、上游拓扑提示
}

service ObjectService {           // L1/L2/L3 三层都实现，只有它能被缓存
  rpc GetObjects(GetObjectsRequest) returns (stream ObjectChunk);   // 按 id 批量取，大对象分块流式返回
  rpc GetTrees(GetTreesRequest)     returns (stream TreeChunk);     // 从 root 递归取，支持 depth 和路径前缀过滤
  rpc HasObjects(HasObjectsRequest) returns (HasObjectsResponse);   // 即 FindMissing
}

service WriteService {
  rpc UploadBlobs(stream UploadBlobChunk) returns (UploadBlobsResponse); // 内容寻址，天然幂等
  rpc CreateChangeset(CreateChangesetRequest) returns (CreateChangesetResponse);
  rpc Rebase(RebaseRequest) returns (RebaseResponse);
  rpc Merge(MergeRequest)   returns (MergeResponse);
}

service LandService {
  rpc Land(LandRequest) returns (LandResponse);                  // 服务端 rebase + CAS（可能经批处理）
  rpc GetLandStatus(GetLandStatusRequest) returns (LandStatus);  // 用于超时后按 request_id 查询结果
}

service RefService {
  rpc ResolveRef(ResolveRefRequest) returns (ResolveRefResponse);     // 永远不缓存
  rpc ListRefs / CreateRef / UpdateRef / DeleteRef / ListRefLog
  rpc WatchRefs(WatchRefsRequest) returns (stream RefEvent);          // daemon 预取、CI 触发
}

service QueryService {            // 「源码管理即 API」：不需要本地仓库
  rpc ReadFile(ReadFileRequest) returns (stream ReadFileChunk);   // rev:path
  rpc ListDir / Stat / Log / Blame / Diff(stream) / IsAncestor / MergeBase / ResolveRevision
  rpc GetDerived(GetDerivedRequest) returns (GetDerivedResponse);  // 通用派生数据查询
}

service AuthService  { rpc StartDeviceLogin / PollDeviceLogin / WhoAmI / CheckAccess }
service AdminService { rpc Redact / Unredact / Purge / ListRedactions / ReloadPolicy }
```

**每个请求都带 `repo`**：从 v1 起就支持多仓库，不像参考实现那样只有单仓库。

### 3.3 能力协商：`GetServerInfo`
```proto
message ServerInfo {
  string protocol_version = 1;          // "1.3"
  repeated string features = 2;         // "land.batch", "objects.uncompressed", "query.blame", ...
  Limits limits = 3;                    // max_batch_objects, max_inline_bytes, chunk_size, max_changes_per_cs ...
  repeated Endpoint read_endpoints = 4; // 推荐使用的区域 cache 地址（客户端可以选择不用）
  HashAlgo hash = 5;                    // BLAKE3_256
}
```
规则：客户端**只依赖 `features` 里声明的能力**；服务端新增能力只加 feature 标记，从不改变已有语义。这样新旧版本的 `scn` 可以和新旧版本的服务端混合运行，滚动升级时不需要客户端和服务端同时更新。

### 3.4 对象传输格式
```proto
message ObjectChunk {
  bytes id = 1;              // 34 字节：algo(1) | kind(1) | digest(32)
  uint64 offset = 2;         // 大对象分块
  bytes data = 3;
  Encoding encoding = 4;     // ZSTD | IDENTITY  (请求里用 accept_encodings 协商)
  bool last = 5;
  uint64 total_size = 6;
}
message TreeChunk {
  bytes id = 1;
  bytes canonical = 2;       // 规范编码字节，可选（请求 want_canonical=true）
  repeated TreeEntry entries = 3;  // 已解码结构，普通客户端只用这个
}
```

### 3.5 统一错误模型
使用 `google.rpc.Status` 加 `ErrorInfo{reason, domain="scion.dev", metadata}`。重试语义写在规范里，`scion-client` 严格照此实现，其它语言的客户端也应遵循：

| reason | gRPC code | 可重试 | 附带的详情 |
|---|---|---|---|
| `MERGE_CONFLICT` | ABORTED | 否（需要人或 Agent 介入） | 冲突路径列表及三方 blob id |
| `LAND_CONTENTION` | UNAVAILABLE | 是 | `RetryInfo` |
| `ALREADY_LANDED` | ALREADY_EXISTS | — | 已合入的结果 changeset |
| `REF_VERSION_MISMATCH` | FAILED_PRECONDITION | 先重新读取再重试 | 当前版本 |
| `OBJECT_MISSING` | FAILED_PRECONDITION | 先上传再重试 | 缺失的 id 列表 |
| `REDACTED` | NOT_FOUND | 否 | redaction id |
| `RATE_LIMITED` | RESOURCE_EXHAUSTED | 是 | `RetryInfo` |

### 3.6 幂等与分页
- **所有写请求**都必须带 `request_id`（客户端生成的 UUIDv7）。服务端在 `idempotency` 表里保留 24 小时：同一个 id 直接返回首次结果，参数不同则报 `INVALID_ARGUMENT`。
- 列表类接口统一使用 `page_size` 加不透明的 `page_token`；超大结果改用流式返回。

### 3.7 认证
- 统一放在 gRPC metadata 的 `authorization: Bearer <token>` 中。
- token 来源：OIDC（企业 IdP 或 GitHub device flow）、service principal（Agent/CI 使用，短期 token 通过 workload identity 交换）、PAT（仅限开发）。
- 服务端签发自己的短期 JWT（15 分钟，ES256），cache 层**只验签，不查库**。
- 服务之间使用 mTLS（rustls；可选接入 SPIFFE）。

---

## 4. 数据模型

### 4.1 对象 ID
`ObjectId = algo(u8) | kind(u8) | digest[32]`，文本形式为 `b1…`/`t1…`/`c1…`/`l1…`（前缀字母表示 kind，数字表示哈希算法，后接 64 位小写十六进制）。
- Blob：digest = BLAKE3(原始字节)
- Tree/Changeset：digest = BLAKE3(规范编码)

### 4.2 规范编码
**已确定：自定义 varint 格式**（2026-10-16）。LEB128 varint 加长度前缀加严格排序的极简二进制格式，规范见仓库根目录 [`SPEC.md`](../../SPEC.zh-CN.md)，并提供 **golden 测试向量**（`testdata/vectors/*.json`：输入结构 → 编码字节 → id；另有非法输入向量 → 预期错误）。格式自行定义，不追求与参考实现互通。只有需要自行校验的客户端才实现它，其它客户端可以忽略。

### 4.3 Changeset 与 Tree 的关系（关键决策）
```
Changeset { parents, author, committer, time, message, change_id, delta: [FileChange], extensions }
```
- **changeset id 只对 delta 计算**，不包含 root_tree，这一点与 Anthropic 原文一致。
- root_tree 作为**强制、同步的派生数据**：`CreateChangeset` 在同一次请求中构建好 tree，写入 `cs_tree` 表后再返回。建 tree 的成本是 O(k × depth)，仍然只和改动大小有关。

这样做同时得到两个好处：返回的 changeset 立刻可以 checkout，和参考实现一样方便；将来要改 tree 格式（例如加 fsnode 统计）时，只需要对 tree 重新派生，**changeset id 不受影响**。

### 4.4 元数据存储：三档部署

| 档位 | 数据库 | 对象存储 | 高可用 | 场景 | 许可证 |
|---|---|---|---|---|---|
| **Lite** | **SQLite**（WAL 模式，sqlx 内置驱动） | 本地文件系统 | 无（单机） | 开发、单机、边缘设备、嵌入 CSGLite 等本地产品 | Public Domain |
| **Standard**（默认） | **PostgreSQL 15+**，用 Patroni 做高可用 | MinIO / S3 兼容 | 主备自动切换，同步复制时 RPO=0 | 常规私有化部署，和 starhub（PG 15）同一套运维栈 | PostgreSQL License |
| **Distributed** | **YugabyteDB**（YSQL，兼容 PG 协议） | MinIO / S3 / 云 OSS | 跨 3 AZ，Raft，RPO=0 | 多可用区、大规模、高写入 | Apache 2.0 |

> 不选 CockroachDB：v24.3（2024-11）起取消了免费的 Core 版。年营收超过 1,000 万美元的企业和政府用户必须购买商业许可，而且必须上报遥测，与私有化部署和内网隔离环境冲突。

**一套代码，方言抽象**：`scion-store-sql` 基于 sqlx 实现，三档共用表结构和绝大部分 SQL，差异集中在 `Dialect` trait 中处理：

| 差异点 | SQLite | PostgreSQL | YugabyteDB |
|---|---|---|---|
| ref CAS | `BEGIN IMMEDIATE` + `UPDATE … WHERE version=?` | `UPDATE … WHERE version=? RETURNING` | 同 PostgreSQL |
| 事务冲突时重试 | `SQLITE_BUSY`（加 busy_timeout） | `40001`、`40P01` | `40001`（需要显式重试）、`40P01` |
| 隔离级别 | 天然串行（单写者） | 关键事务用 SERIALIZABLE；ref CAS 用单行条件更新，不依赖隔离级别 | 同 PostgreSQL |
| `cs_log` 序号 | 单写者下直接 `max+1` | 每个仓库一个计数行，在事务中自增 | 同 PostgreSQL；也可以用带 cache 的 sequence |
| 热点打散 | 不需要 | B-tree 即可 | 表和索引按 `HASH(repo_id, …)` 分片 |
| 只读副本 | 不需要 | 不可变数据读备库（Patroni replica） | follower read（`yb_read_from_followers`，加上有界陈旧度） |
| 批量写入 | 多行 INSERT | `UNNEST` 批量 INSERT | 同 PostgreSQL |

**档位迁移**：`scion admin migrate-meta --from sqlite://… --to postgres://…`，PG 迁到 YugabyteDB 同样适用。对象按内容寻址，换档时**对象数据原样复用**（本地文件系统可以用 `scion admin blob-sync` 搬到 S3），只需要迁移元数据。迁移前需要短暂冻结写入，迁移后跑 `fsck` 校验。

**一致性测试**：`scion-store` 的 conformance 测试套件对三种数据库**全部运行**。PostgreSQL 和 SQLite 每个 PR 都跑，YugabyteDB 每晚跑。

### 4.4.1 元数据表
| 表 | 主键 | 说明 |
|---|---|---|
| `repos` | repo_id | 配置、限额 |
| `objects_index` | (repo_id, id) | 仓库能看到哪些对象：大小、redaction 标记。对象字节本身全局只存一份，这张表决定可见性 |
| `objects_inline` | (repo_id, id) | 小于 4 KiB 的对象直接内联存储（tree 大多很小） |
| `changesets` | (repo_id, cs_id) | 规范编码字节、gen、first_parent |
| `cs_log` | (repo_id, seq) | WAL，序号单调递增（每仓库一个 sequence） |
| `cs_tree` | (repo_id, cs_id) | root tree id |
| `refs` | (repo_id, name) | target, version；**唯一的可变状态** |
| `ref_log` | (repo_id, name, version) | 审计记录 |
| `landed` | (repo_id, ref, change_id) | 防止同一个 change 重复合入（M3 实现） |
| `derived` | (repo_id, kind, kind_version, cs_id) | 派生数据的值或指针 |
| `idempotency` | (repo_id, request_id) | 带 TTL（M3 实现，默认 24 小时） |
| `leases` | (scope) | land leader 和 deriver 分片的租约（M3 实现 land leader） |

热点规避：Distributed 档的表都以 `HASH(repo_id)` 作为分片键，避免同一个仓库的写入集中到单个 tablet；`cs_log` 的序号做法见上表。Standard 和 Lite 档不需要处理热点。

### 4.5 Blob 存储布局
`s3://bucket/{prefix}objects/{kind}/{hh}/{hex}`。**对象字节全局只存一份**，不按仓库分目录：相同内容（例如同一模型的量化版、微调版之间的公共分片）在所有仓库中只存一次。仓库能读到哪些对象由 `objects_index` 决定，`HasObjects` 和 `GetObjects` 都先查索引，因此一个仓库无法探测其它仓库有哪些对象。M1 存原始字节，zstd 压缩在后续里程碑加入（以 key 后缀区分，旧对象照常可读）。

写入顺序不变式：**blob 持久化 → changeset 与 cs_tree 持久化 → ref CAS**。读路径永远不会看到悬挂引用。

大文件（大于 8 MiB）用 FastCDC 分块，blob 本身变成 `ChunkList` 对象（kind=0x05），客户端读取时由服务端透明拼接，因此**对客户端不可见**。

### 4.6 对象存储兼容性（MinIO / S3 兼容存储）

**目标**：一套代码能运行在 AWS S3、**MinIO**、Ceph RGW、SeaweedFS，以及阿里云 OSS、华为 OBS、腾讯 COS 等 S3 兼容存储上；也可以直接复用 starhub 现有的 MinIO。

**原则：只使用 S3 核心 API 子集**
| 使用 | 不依赖 |
|---|---|
| PutObject、GetObject（含 Range）、HeadObject、DeleteObject(s)、ListObjectsV2、分块上传（Create/UploadPart/Complete/Abort/ListParts）、CopyObject/UploadPartCopy、预签名 GET/PUT（SigV4） | S3 Select、Object Lambda、事件通知、Object Lock（作为可选项）、**条件写入（If-None-Match）**、存储桶策略、厂商特有的扩展头 |

**为什么不需要条件写入**：对象内容寻址，同一个 key 的内容必然相同，重复写入天然幂等，并发写同一个对象也不会出错。唯一的可变状态 ref 放在 MetaStore 里，不放在对象存储中。

**配置项**（`[blobstore.s3]`，基于 `object_store` crate 的 `AmazonS3Builder`）：
```toml
[blobstore.s3]
endpoint          = "http://minio:9000"     # 留空则使用 AWS
region            = "us-east-1"             # MinIO 可以随便填
bucket            = "scion-objects"
prefix            = "prod/"                 # 允许和其它业务共用一个 bucket
addressing        = "path"                  # MinIO/Ceph 用 path；阿里云 OSS 等需要 virtual-hosted
allow_http        = true                    # 仅内网使用
access_key_id     = "${S3_ACCESS_KEY}"      # 也可以用 IRSA、实例角色或 STS 临时凭证
secret_access_key = "${S3_SECRET_KEY}"
ca_bundle         = "/etc/scion/ca.pem"     # 私有 CA
checksum          = "when_required"         # 不默认发送新版 CRC 校验头(部分兼容存储会拒绝)
multipart_part_size = "16MiB"
max_concurrency   = 256
request_timeout   = "30s"
sse               = "none"                  # none | sse-s3 | sse-kms(key_id) | sse-c
storage_class     = ""                      # 可选
```

**一致性要求**：新写入的对象必须能立即读到（read-after-write）。AWS S3（2020 年起）、MinIO、Ceph RGW 都满足。§4.5 的写入顺序不变式依赖这一点，所以启动时会做自检。

**启动自检**：执行 `scion admin blobstore-check`，server 启动时也会自动跑一遍。检查项：PUT 后立即 GET 和 HEAD、Range 读、超过 5 GiB 的分块上传与 UploadPartCopy（可选）、预签名 URL 访问、批量删除时的条数上限、ListObjectsV2 分页、时钟偏差（SigV4 对时间敏感）。检查结果写成 feature 标记，例如不支持 UploadPartCopy 时自动改走「读出再写入」的方式。

> **实现说明**（2026-10-17）：服务适配层在 `server/store-blob/src/provider.rs`。`SCION_S3_PROVIDER` 可选 `auto`（默认）、`minio`、`seaweedfs`、`aws`、`generic`。启动检查依次执行：按 `Server` 响应头识别服务（显式配置与识别结果冲突时报错）、该服务的健康接口（MinIO `/minio/health/ready`、SeaweedFS `/healthz`）、bucket 是否存在（不存在时提示该服务的创建方法）、读写自检。请求风格按服务决定默认值，只有 AWS 默认 virtual-hosted。CI 在 SeaweedFS 4.48 与 MinIO 2023.12.23（bitnami 镜像，与 starhub 部署一致）上各跑一遍完整的存储一致性测试。使用说明见 [`docs/guide/object-storage.md`](../guide/object-storage.zh-CN.md)。

**直连对象存储下载（减轻服务端带宽压力）**：
- 超过阈值（默认 64 MiB）的 blob，`GetObjects` 可以不返回数据流，而是返回 `redirect{presigned_url, expires}`。前提是请求里声明了 `accept_redirect`，并且服务端开启了 `objects.presigned` 特性。
- 客户端直接从 MinIO 或 S3 下载，**下载后仍然校验 BLAKE3**。
- 敏感对象（`cacheable=false` 的那类）永远不走这个方式。

**直连对象存储上传**：
1. 大 blob 先调用 `BeginUpload`，拿到暂存区 key `staging/<upload_id>` 和一组预签名的分块 PUT 地址。
2. 客户端直接把数据传到对象存储。
3. 客户端调用 `CommitUpload(expected_id)`。服务端流式读回数据并校验 hash，通过后用 CopyObject 放到正式的 CAS key 下，然后删除暂存数据。
4. 校验失败就删除暂存数据并返回错误。janitor 会清理超过 24 小时仍未提交的暂存数据。

**key 布局**：`{prefix}objects/{kind}/{hh}/{hex}`。用 hash 作前缀，可以把对象均匀分散到 MinIO 的各个 erasure set 和 S3 的各个分区；`staging/` 和 `snapshots/` 是独立的前缀，方便单独配置生命周期规则。

**兼容性测试矩阵（CI）**：
| 后端 | 方式 | 频率 |
|---|---|---|
| SeaweedFS（锁定版本，开启签名鉴权） | CI 中 `docker run`，与本地开发环境 `deploy/dev` 相同 | 每个 PR |
| 本地文件系统（`object_store::local`） | 单元测试和 Lite 档 | 每个 PR |
| Ceph RGW、RustFS、MinIO 存量版本 | 容器 | 每晚 |
| AWS S3、阿里云 OSS、华为 OBS | 真实账号，使用专门的测试 bucket | 每周和发布前 |

> **MinIO 开源版已停止维护**（2026-10 核实）：官方下载站对全部社区版程序返回 `410 Gone`，说明 MinIO Server、mc、KES 开源项目已归档，不再提供安全更新，也不再处理漏洞报告。因此：
> - 开发环境和 PR 级 CI 改用 **SeaweedFS**（Apache 2.0，仍在维护）作为默认 S3 兼容存储；
> - 我们**只依赖标准 S3 语义，不使用任何 MinIO 专有 API**（如 `mc admin`、MinIO 的事件通知），所以 starhub 现有的 MinIO 依然可以直接使用，以后迁到 SeaweedFS、Ceph、RustFS 或云厂商存储时不需要改代码；
> - 建议新部署优先选择仍在维护的 S3 兼容存储，存量 MinIO 部署评估迁移计划。

---

## 5. 服务端内部架构（Rust）

### 5.1 Workspace 结构与分层

顶层目录按**角色**划分，一眼可以看出哪些代码属于服务端、哪些属于客户端、哪些两边共用。目录名表示位置和职责，crate 名统一带 `scion-` 前缀（避免与 crates.io 上的 `object` 等同名库冲突）。

```
proto/scion/v1/        协议定义 (.proto)，语言无关
SPEC.md                对象模型与规范编码；testdata/vectors/ 为 golden 向量

common/                服务端和客户端共用，只能依赖 common
  protocol/            scion-proto        tonic/prost 生成代码，零业务逻辑
  object/              scion-object       ObjectId、规范编码、Tree/Changeset、校验（无 IO）

server/                服务端，可依赖 common 与更低层的 server crate
  store/               scion-store        trait MetaStore / BlobStore + 内存实现 + 后端一致性测试套件
  store-sql/           scion-store-sql    sqlx：SQLite / PostgreSQL / YugabyteDB 三档 + Dialect
  store-blob/          scion-store-blob   object_store：S3 兼容存储 / 本地文件系统
  merge/               scion-merge        diff3、三方合并、冲突描述（纯计算）
  tree/                scion-tree         增量 TreeBuilder
  derive/              scion-derive       派生框架 + 各 deriver
  land/                scion-land         LandCoordinator（批处理 leader）、rebase 流水线
  query/               scion-query        log / blame / diff / merge-base
  authz/  auth/        scion-authz / scion-auth   策略引擎；OIDC / JWT / device flow
  cache/               scion-cache        分层缓存核心：foyer + singleflight + rendezvous
  obs/                 scion-obs          tracing / OTel / metrics
  api/                 scion-api          tonic 服务实现 + axum REST + tower 中间件
  scion/               scion              服务端单二进制，子命令分角色

client/                客户端，只能依赖 common 与 client，禁止依赖 server
  sdk/                 scion-client       Rust SDK
  wc/                  scion-wc           工作副本
  daemon/              scion-daemon       节点 L1 缓存
  vfs/                 scion-vfs          虚拟化 checkout（M7 之后）
  gitimport/           scion-gitimport    git 历史导入（只走公开协议，scn import git）
  cli/                 scion-cli          二进制 scn

testing/               scion-testkit（假后端、故障注入、git 差分对照、lincheck）
                       scion-conformance（黑盒协议一致性套件，可指向任何服务端地址）
xtask/                 仓库维护任务：cargo xtask layers（依赖方向检查）
```
客户端部分的设计见 [`rust-client-architecture.md`](./rust-client-architecture.zh-CN.md)。目录按里程碑逐步创建，M0 已有 `common/protocol`、`common/object`、`server/store`、`server/api`、`server/scion`、`client/sdk`、`client/cli`。

**分层规则**（`cargo xtask layers` 在 CI 中检查，只检查普通依赖，dev-dependencies 不受限）：
- 区域规则：`common` 只依赖 `common`；`client` 只依赖 `common` 和 `client`；`server` 只依赖 `common` 和 `server`。区域由 crate 所在的顶层目录自动判定。
- 服务端内部按层：`store` ← `store-sql/store-blob` ← `tree` ← `merge/derive/query` ← `land` ← `api` ← `scion`，只能依赖更低层；新增服务端 crate 必须在 xtask 中登记层级，否则检查失败。`authz` 只在 `api` 层生效。

### 5.2 运行时与关键依赖
| 用途 | crate |
|---|---|
| 异步运行时 | `tokio` |
| gRPC | `tonic`, `prost`, `tonic-web`, `tonic-health`, `tonic-reflection` |
| REST | `axum`, `pbjson` |
| 中间件 | `tower`（timeout、concurrency limit、load shed）、`governor`（令牌桶） |
| SQL | `sqlx`（sqlite 和 postgres 两个驱动；YugabyteDB 走 postgres 驱动；事务冲突的重试按 Dialect 统一处理） |
| 对象存储 | `object_store` |
| 缓存 | `foyer`（内存加磁盘混合，L2 使用）、`moka`（进程内小缓存） |
| 哈希/压缩/分块 | `blake3`, `zstd`, `fastcdc` |
| TLS/JWT | `rustls`, `jsonwebtoken` |
| 可观测 | `tracing`, `opentelemetry-otlp`, `metrics` + Prometheus exporter |
| 测试 | `proptest`, `insta`, `testcontainers`, `loom`（并发原语） |

### 5.3 请求处理管线（tower 中间件，从外到内）
```
TLS → 请求 id 和 trace 上下文 → 认证(验签 JWT) → 按租户/仓库限流(governor)
    → 全局并发上限 + 过载丢弃(load shed) → deadline 传递(取客户端值与上限的较小者)
    → 幂等检查(写请求) → 鉴权(仓库/路径/ref) → handler
```

---

## 6. 写路径

### 6.1 CreateChangeset
```
1. 校验：parents 存在；delta 中每个 blob 已存在(否则返回 OBJECT_MISSING 及缺失列表)；路径合法；写权限；限额
2. 读 parent 的 root tree → TreeBuilder 只重写 delta 涉及路径上的节点 (O(k·depth))
3. 新 tree 节点：小于 4K 的写 objects_inline，否则写 S3
4. 一个 SQL 事务内：changesets + cs_tree + cs_log
5. 返回 {cs_id, root_tree}
```
**更简单的客户端入口**：`CreateChangeset` 也可以直接接受 `inline_content`（小于 1 MiB 的文件内容），服务端负责算 hash 并存储。Agent 改几个文件时，一次 RPC 就能完成提交。

### 6.2 Land 与 LandCoordinator（高并发核心）
单行 CAS 本身是正确的，但在热点 ref 上会出现**重试风暴**，而且每次重试都要重新 rebase。解决办法是**每个 ref 一个 leader 加批处理**：

```
Land 请求 → 任意 server → 查 leases 表找到 (repo, ref) 的 leader
        → 本机是 leader 就直接入队，否则通过内部 gRPC 转给 leader

LandCoordinator (每个热点 ref 一个 tokio 任务):
  在内存中维护 head (每次 CAS 成功后更新；租约刚拿到时从库里重新读取)
  loop:
    batch = 收集请求，直到 (5ms 或 64 个，先到为准)
    onto = head
    for req in batch:                    # 顺序 rebase，彼此冲突时各自报错，不影响整批
      r = rebase(req.stack, onto)        # 冲突检测只看 base..onto 期间与本次 delta 的路径交集
      ok  → onto = r.tip; accepted += r
      err → 回复该请求 MERGE_CONFLICT
    一个事务: CAS(ref, expect=head_version, new=onto) + ref_log + landed 行
    成功 → head = onto；逐个回复
    失败(版本不对，说明租约被抢或有外部写入) → 重新读 head，整批重做
```

- **批内冲突**：第 i 个请求和前面已接受的请求冲突时，只有它自己失败，不影响其它请求。
- **租约**：TTL 5 秒，每 2 秒续约；CAS 时检查版本号，作为 fencing token，即使 leader 的租约过期，也不会产生错误写入。
- **冷 ref**（低频的 release 分支等）直接走 CAS 和抖动重试，不启用 leader。是否启用 leader 由 ref 的 QPS 自动决定。
- **客户端超时**：用相同的 `request_id` 重试，或者调用 `GetLandStatus` 查询，保证不会重复合入。
- 预期性能：一次 CAS 往返约 10 毫秒，每批平均 20 到 50 个请求，**单 ref 每秒可以合入几百到上千次**。

### 6.3 rebase 与冲突检测
- 候选冲突路径 = `paths(delta)` ∩ `changed_paths(base..onto)`。后者由派生数据 `path_history` 加 changeset 跳表快速得到，**不需要遍历整棵树**。
- 交集为空：直接把 delta 应用到 onto 上（最常见的情况）。
- 交集非空：对每个路径做三方合并。文本文件用 diff3；二进制文件、模式变化、删除与修改相冲突的情况都直接报冲突。
- 冲突详情会把 base、ours、theirs 三个 blob id 都返回给客户端，Agent 可以据此自动解决冲突，再重新提交。

---

## 7. 读路径与缓存层

### 7.1 三层同构
`ObjectService` 在 daemon（L1，客户端侧，可以用任何语言实现）、`scion cache`（L2）、`scion server`（L3）都提供，**消息定义完全相同**。因为对象内容寻址且不可变，各层可以**永久缓存，不需要做失效**。ref 永远不缓存。

### 7.2 L2 区域缓存 `scion cache`
- **存储**：foyer 混合缓存，内存层几 GB 到几十 GB，NVMe 层几百 GB 到几 TB。
- **分片**：以 cache 节点列表做 rendezvous hashing，按对象 id 路由。节点收到不属于自己的 id 时，先在节点之间转发一跳，再决定是否回源，从而避免每个节点都向上游回源。
- **合并回源**：用 singleflight 合并同一 id 的并发 miss；`GetTrees` 递归请求会被拆成多个 id 的批量回源。
- **请求对冲**：上游请求超过 p95 延迟仍未返回时，向另一个上游副本再发一份。
- **故障隔离**：每个上游一个熔断器；回源并发设上限；**上游异常时绝不让流量绕过 L2 直连源站**；区域整体故障时客户端会收到 UNAVAILABLE 和 RetryInfo。
- **预热**：订阅 `WatchRefs`，main 每次前进时预取新的 tree 节点（只取变化的路径）。
- 本层**没有任何持久状态**：节点挂掉只损失命中率，补一个新节点即可。

### 7.3 L3 server 读路径
小对象查 `objects_inline`，大对象查 S3。进程内还有一个 moka 缓存，存放 tree 和元数据。所有 S3 读都经过 singleflight 和并发上限。

---

## 8. 派生数据框架

```rust
#[async_trait]
pub trait Deriver: Send + Sync + 'static {
    const KIND: DerivedKind;       // Tree, Gen, SkipList, PathHistory, Blame, PathIndex, ...
    const VERSION: u32;            // 格式升级时递增；新旧版本可以并存、后台回填
    type Value: Encode + Decode;
    fn deps(&self) -> &'static [DerivedKind];
    async fn derive(&self, ctx: &DeriveCtx, cs: &Changeset,
                    parents: &[Self::Value]) -> Result<Self::Value>;
}
```
- **lazy**：查询时发现缺失，就沿 first-parent 链回溯到最近一个已计算的祖先，再按拓扑顺序逐个计算回来。同一个 (kind, cs) 的并发计算通过 singleflight 合并，同时加上 `leases` 表防止多个节点重复计算。
- **eager**：`scion deriver` 跟随 `cs_log` 推进；工作按 (repo, kind) 分片，用租约分配给各个 worker。
- **首批 deriver**：`Gen`（拓扑深度）、`SkipList`（祖先跳表，用于 merge-base 和 is-ancestor）、`PathHistory`（每个路径上一次被修改的位置，用于 log、blame 和冲突检测）、`Blame`、`PathIndex`（路径前缀搜索）、`GitSha`（Git 互操作）。
- 派生数据的值统一走 `GetDerived` 和具体的 Query RPC 返回，**派生格式本身永远不暴露给客户端**。

---

## 9. 权限与安全

- **策略**：`acl.toml` 放在仓库之外（存在 MetaStore 中，`ReloadPolicy` 热加载）。按路径前缀和 ref 模式，给 principal 或 group 分配 read/write/land/admin 权限。
- **读权限的执行点**：在 `GetTrees` 和 `ListDir` 返回 tree 时，过滤掉无权访问的子目录条目（客户端只会看到一个 `inaccessible` 标记）。拿不到子树 hash，就无法请求其中的内容。
- **cache 层的模型**：256 位的 hash 视为 capability。L2 只确认 token 有效，不做路径鉴权。对于**敏感前缀**（策略中标记为 `sensitive=true` 的路径），对应 blob 的 key 带有 ACL 域标签，L2 对这类对象一律回源 L3 鉴权，并且不缓存。
- **Redaction**：在 `objects_index` 上打标记，从此读取一律返回 `REDACTED`，ID 和历史都不变；`janitor` 负责在宽限期后从 S3 物理删除。这个思路借鉴自参考实现。
- **审计**：所有写操作、ref 变更和管理操作都写入 `ref_log`，并作为审计事件发到 NATS。

---

## 10. 高可用与容量

| 组件 | 部署 | 故障影响 |
|---|---|---|
| server | 每个 AZ 不少于 2 个，HPA 按 CPU 和在途请求数扩缩 | 无（无状态） |
| cache | 每区域 N 个，扩缩容时 rendezvous 只迁移 1/N 的数据 | 只损失命中率 |
| deriver / janitor | 不少于 2 个，按租约分片 | 派生数据暂时滞后，lazy 计算兜底 |
| LandCoordinator | 运行在 server 内部，按租约分配 | 约 5 秒内 leader 切换，期间 land 延迟升高 |
| 元数据库：Standard 档 | PostgreSQL + Patroni（1 主 2 备，同步复制） | 主库故障后几十秒内自动切换，期间写入不可用，读不受影响 |
| 元数据库：Distributed 档 | YugabyteDB 跨 3 AZ，RF=3 或 5 | 损失少数副本时无感 |
| 元数据库：Lite 档 | SQLite 单文件 | 不提供高可用；靠定期备份（`VACUUM INTO` 或 Litestream 一类工具） |
| S3 | 托管服务，开启跨区复制 | — |
| NATS | 3 节点 JetStream | 只影响事件推送，客户端会降级为轮询 ref |

**多区域**：写请求集中到主区域（ref CAS 需要强一致）。读请求走本区域的 L2，再回源到最近的 L3。L3 读不可变数据时走就近副本（YugabyteDB 用 follower read，PostgreSQL 读区域只读副本）；只有读 ref 时才走主库或 leader。多区域部署需要 Distributed 档。

**粗略容量估算**（3,000 万 checkout/天，10 万合入/天）：
- L2：峰值约 30 万对象/秒，约 4 到 8 个大内存加 NVMe 节点。
- L3：承接约 1% 的回源，约 3 到 6 个节点。
- 元数据库：写入压力主要来自 changeset 和派生数据，约每秒 1 到 3 千行。单个 PostgreSQL 主库可以承受，这一规模用 Standard 档即可；需要多 AZ 写入或更高规模时，换 5 节点的 YugabyteDB。

---

## 11. 可观测性与运维

- **追踪**：trace 从客户端（SDK 自动注入 `traceparent`）贯穿 L2、L3、元数据库和 S3。
- **核心 SLI**：每层命中率、`GetObjects`/`GetTrees` 的 p50/p99/p999、land 端到端延迟、批大小分布、CAS 失败率、派生滞后（`cs_log` 头部 seq 减去已派生 seq）、各服务的 QPS 和错误率。
- **SLO 示例**：`ResolveRef` p99 小于 20 毫秒；L2 命中时 `GetObjects` p99 小于 10 毫秒；land p99 小于 500 毫秒（无冲突时）。
- **运维命令**：`scion admin fsck`（校验对象完整性和引用闭包）、`scion admin rederive --kind blame --version 2`、`scion admin lease ls`、`scion admin drain <node>`。

---

## 12. 测试与一致性保障

| 层级 | 内容 |
|---|---|
| 单元 / 性质测试 | 规范编码往返和拒绝非法输入（proptest）、TreeBuilder 与「全量重建」结果一致、diff3 对照 |
| 差分测试 | 导入真实 Git 仓库后，对比 Scion 和 git 的 log、blame、diff、merge-base 输出 |
| 并发正确性 | land 的线性一致性检查（lincheck），覆盖并发 land、租约抢占和 leader 被杀的场景 |
| 故障注入 | 在 `land.after_rebase`、`after_commit`、S3 写入后崩溃等埋点注入故障（借鉴参考实现） |
| **协议一致性** | `scion-conformance` 黑盒套件：传入任意服务端地址就能运行；测试场景用 YAML 描述，由 `scion-client` 执行，场景文件与语言无关 |
| 压测 | 持续施压的 landing 压测（泊松到达）；用 ghz 压 `GetObjects`；目标见 §0 |
| 混沌 | 杀 L2、PG 主备切换（Patroni）、YugabyteDB leader 迁移、AZ 断网、S3 延迟注入 |

### 12.1 函数级测试要求（强制，服务端和客户端通用）

每个函数都要有测试，这是合并的硬性门槛，代码评审和 CI 都按这一节检查。

**覆盖范围**
1. **每个含逻辑的函数**（不论 `pub` 还是私有）都要有单元测试，放在同文件的 `#[cfg(test)] mod tests` 中。只有纯转发、纯字段访问、`derive` 生成的代码可以豁免。
2. 每个函数的测试至少覆盖三类情况：**正常路径**、**边界条件**（空输入、零、最大值、单元素、恰好等于限额）、**每一个错误分支**。错误类型的每个变体至少有一个测试能触发它。
3. **公开 API**（crate 对外导出的类型和函数）除单元测试外，还要有 doc test 或 `tests/` 下的集成测试，展示正确用法。
4. **编解码、排序、哈希等确定性逻辑**必须加性质测试（`proptest`）：往返一致、非法输入必被拒绝、输出唯一。
5. **修 bug 必须先加能复现它的回归测试**，测试名或注释里写明问题编号。
6. 涉及并发的函数（锁、租约、批处理、singleflight）要有并发测试：`tokio::test(flavor = "multi_thread")` 加多任务竞争；底层同步原语用 `loom`。

**写法约定**
- 测试命名：`<函数名>_<场景>_<预期>`，如 `decode_tree_unsorted_entries_rejected`。
- 测试必须确定、可重复：不依赖网络、真实时间和执行顺序；时间和随机数通过注入的 `Clock`、种子控制。需要外部服务（PG、S3）的测试放在 `tests/` 下，用 `#[ignore]` 或 feature 隔离，在 CI 的集成阶段统一跑。
- 只断言行为，不断言实现细节；错误用 `matches!` 断言具体变体，不比较错误文案。
- 单个单元测试应在 100 毫秒内完成。

**CI 门槛**（`cargo llvm-cov` 统计）
| 范围 | 行覆盖率 | 分支覆盖率 |
|---|---|---|
| 核心 crate：`scion-object`、`scion-store*`、`scion-tree`、`scion-merge`、`scion-land`、`scion-wc` | ≥ 90% | ≥ 80% |
| 其余 crate | ≥ 80% | — |
| 每个 PR 新增或修改的代码（diff coverage） | ≥ 90% | — |

低于门槛时 CI 失败。确实无法测试的代码（例如 `main` 中的进程启动胶水）用 `#[cfg_attr(coverage_nightly, coverage(off))]` 显式标注，并在评审中说明理由。

---

## 13. 客户端接入示意（Rust SDK）

```rust
let c = scion_client::Client::builder()
    .endpoint("https://scion.example.com").repo("mono")
    .auth(Auth::from_env()).build().await?;

let head = c.resolve_ref("main").await?;
let cs = c.changeset(&head)
    .put("svc/a.rs", new_bytes)          // 小文件内联，大文件自动先上传
    .message("fix").create().await?;
match c.land("main", &cs).await {        // request_id 自动生成，可安全重试
    Ok(r) => println!("landed {}", r.tip),
    Err(e) if e.is_merge_conflict() => { /* e.conflicts(): 三方 blob id */ }
    Err(e) => return Err(e.into()),
}
```
SDK、工作副本、daemon 和 CLI 的完整设计见 [`rust-client-architecture.md`](./rust-client-architecture.zh-CN.md)。

---

## 14. 里程碑

| 里程碑 | 周期 | 内容 | 验收 |
|---|---|---|---|
| **M0 协议与骨架** | 2 周 | proto v1 + buf CI、`scion-object`、golden 测试向量、内存版 store、单二进制骨架、`GetServerInfo`、覆盖率门槛接入 CI | 编码向量测试通过；`scion-client` 能连上并完成能力协商；满足 §12.1 |
| **M1 最小闭环** | 4 周 | sqlx（SQLite + PostgreSQL）+ object_store（本地文件系统 + S3 兼容存储，CI 用 SeaweedFS）、Lite 模式、Upload/CreateChangeset/TreeBuilder、直接 CAS 的 Land、ReadFile/ListDir/GetTrees、git 导入 | `scn` 完成 clone(sparse)/commit/land；差分测试通过 |

> **M1 实现说明**（2026-10-16）：
> - **git 导入放在客户端**（`client/gitimport`，命令 `scn import git <path> --repo NAME`），只通过公开协议写入，与普通用户走同一条校验路径；服务端不需要额外的写入通道。结果确定：`change_id` 由 git sha 推导，git sha 存入扩展字段 `git.sha1`，重复导入得到相同的提交 id；增量导入沿第一父提交链逐个快进。差分测试逐个提交比对 git 与 Scion 的文件列表、mode、内容和元数据。大仓库的批量导入（starhub 迁移，§16）后续改为服务端直写。
> - **合入只支持快进**；工作副本的 `scn sync` 在客户端做文件级 rebase，冲突以退出码 3 报告。M3 的服务端 rebase 上线后，`Land` 直接处理落后的提交。

> **M2 实现说明**（2026-10-16）：
> - **PathHistory 索引**：创建提交时记录 delta 中的每个路径（`path_changes` 表），按路径过滤的 log 与 blame 从索引取候选提交，不逐个解码提交对象。
> - **第一父提交链跳表**：提交记录带 `fp_depth` 与 `fp_jump`（Myers skew-binary 单指针跳表），O(log n) 找到链上任意深度的祖先；`is_ancestor` 先走这条快速路径，blame 用它判断候选是否在链上。
> - **提交记录缓存**：记录不可变，服务端用 `CachedMeta` 在进程内缓存并在启动时后台预热（10 万条约 0.5 秒）；遍历历史时按代数批量预取，缓存未命中也不会逐条查库。
> - **blame 缓存**：派生数据只存最终版本和每 32 个版本一个检查点，再次 blame 只算增量；沿第一父提交链追溯，经由合并进来的改动归到合并提交。
> - **blame 缓存查询批量化**：最新版本单独查一次（重复查询的常见情况），其余版本每 64 个一次 `get_derived_many`，避免远端数据库上逐版本往返。
> - **GitSha 索引**：导入的提交带扩展字段 `git.sha1`，创建提交时写入派生数据 `git-sha1`；`ResolveRevision` 依次按提交 id、ref、git sha 解析，`GetDerived` 可查询 git sha 与代数。
> - **查询 RPC**：Log、IsAncestor、MergeBase、Diff（服务端流，每批 500 个条目）、Blame、ResolveRevision、Stat、GetDerived，均在 `GetServerInfo` 中以特性声明。
> - **从 M1 升级**：M1 只在 `first_parent` 列记录父提交。迁移 0008 把它拷入 `parents`，并把这些仓库的跳表标记为未知。随后运行 `scion admin reindex [--repo NAME]`（在启动服务端之前运行；服务端会缓存提交记录），它以提交对象为准修正谱系（全部父提交、包括合并提交的其余父提交，以及跳表，错误的值会被覆盖），并补建 PathHistory 与 GitSha。可重复执行。
> - **基准**（`server/query/tests/bench.rs`，10 万提交、对象在内存）：SQLite 与 PostgreSQL 元数据下，log、diff、ancestry、merge-base、blame 的 p99 均低于 20 ms（PostgreSQL：冷 blame p99 13.6 ms，log --path p99 15.6 ms）。对象放在 S3 时 blame / diff 的读对象开销由 M4 的缓存层解决。
> - **推迟到后续里程碑**：通用 Deriver 框架与独立的 deriver 进程（M3/M4）；PathIndex。blame 只沿第一父提交链追溯，按路径过滤的 log 不跟踪重命名。
| **M2 派生与查询** | 3 周 | 派生框架、Gen/SkipList/PathHistory/Blame、Log/Blame/Diff/MergeBase、deriver 进程 | 在 10 万提交的仓库上，查询 p99 小于 100 毫秒 |
| **M3 高并发** | 3 周 | LandCoordinator 批处理和租约、幂等表、限流与过载丢弃、lincheck | 单 ref 每秒 200 次以上 land；leader 被杀时没有重复或丢失的合入 |

> **M3 实现说明**（2026-10-17）：
> - **服务端 rebase**（`server/merge`）：只比较 delta 涉及的路径在 base 与 onto 中的条目，未变化的直接应用，代价与仓库大小和中间提交数无关；双方都改的文件按行 diff3（diffy），可执行位按三方规则合并；二进制、符号链接、超过 8 MiB、修改/删除、双方新增、文件/目录冲突都报 MERGE_CONFLICT，详情放在 ErrorInfo metadata（`path.N`、`kind.N`、`base.N`、`ours.N`、`theirs.N`）。比 §6.3 的「path_history 求交集」更直接，结果相同。
> - **Land 默认 rebase**：从提交沿第一父往回到 ref 历史中的提交为止是待合入的栈，快进或逐个 rebase；`fast_forward_only` 保留只快进的语义（git 导入使用）。rebase 后的提交保留作者、说明与 change id，去掉 `git.sha1`。
> - **幂等与去重**：`land_commit` 在一个事务里更新 ref、写 `landed`（同一 ref 上每个 change id 只合入一次）和 `idempotency`（同一 request id 返回第一次成功的结果，参数不同则 INVALID_ARGUMENT，保留 24 小时）。只记录成功的结果：失败的请求什么都没写，重试会重新执行。`GetLandStatus` 按 request id 查询。SDK 在可重试错误下用同一个 request id 重试。
> - **LandCoordinator**（`server/land`）：每个 ref 一个任务，最多 64 个请求或 5 ms 一批，按顺序 rebase，冲突的请求单独失败；批内的 rebase 结果先放在内存（写后缓冲），后续请求直接在上面继续，整批最后批量写入（对象并发写，`index_paths_many` / `put_changesets` 多行插入），一次 `land_commit` 提交。
> - **集群**：`scion server --advertise-url URL` 开启。每个 ref 的任务持有租约（`leases` 表，TTL 5 秒，2 秒续约），其它节点收到 `NotLeader` 后通过 gRPC 转发（`x-scion-forwarded`，不再二次转发）。正常退出时释放租约，其它节点立即接管；进程被杀时等租约到期。ref 版本号是 fencing token，两个节点短暂同时认为自己是 leader 也不会写坏 ref。
> - **过载保护**：全局在途请求上限（超过立即拒绝，RESOURCE_EXHAUSTED / RATE_LIMITED，带 `retry_after_ms`），按仓库的写入令牌桶（`--repo-write-rate`）。
> - **验收**：`server/land/tests/bench.rs`（2000 个请求、200 个并发客户端、同一个 ref、对象在内存）：PostgreSQL 885 次/秒（p99 379 ms），SQLite 1921 次/秒（p99 173 ms）。`server/land/tests/lincheck.rs` 检查不重复、不丢失、不凭空出现和实时顺序，覆盖单节点并发冲突、两节点转发、合入进行中杀掉 leader（SQLite 元数据，客户端用同一个 request id 重试）。
> - **推迟**：冷 ref 不启用 leader 的自动判定（目前所有 ref 都走批处理任务，空闲退出）；按租户限流与 deadline 传递在 M5 与认证一起做。
| **M4 缓存层** | 3 周 | `scion cache`(foyer + rendezvous + singleflight + 对冲)、WatchRefs、预热 | L2 命中率不低于 99%；单节点 20 万对象读/秒以上 |
> **M4 实现说明**（2026-10-17）：
> - **WatchRefs**（`RefService.WatchRefs`）：先发快照，之后每次前进发送新状态；只保证送达每个 ref 的最新状态，断开后带上 `known_versions` 重新订阅即可续上。本节点合入后立即推送；集群中其它节点的合入由按仓库的轮询发现（只轮询有订阅者的仓库，`--ref-poll-interval`，默认 1 秒），未使用 NATS。停机时主动结束订阅流，避免优雅停机被长连接挡住。
> - **`scion cache`**（`server/cache`）：foyer 混合缓存（内存层淘汰写入磁盘层），rendezvous 哈希分片（BLAKE3 打分，节点之间只转发一跳，负责节点不可达时自己回源），single-flight 放在新的共用 crate `common/flight`（领取与完成分开，一批未命中合成一次回源请求）。上游池：p95 对冲、按上游熔断（连续 5 次失败冷却 5 秒）、业务错误不换上游、回源结果逐个校验哈希。预热订阅上游的 WatchRefs，只沿变化的子树下钻。**按仓库的可见性与 L3 一致**（评审指出命中与 single-flight 原先只看对象 id）：字节按 id 去重，single-flight 按 (仓库, id) 合并，应答前确认 id 在请求的仓库可见，回源成功的 (仓库, id) 记入可见集，命中而未知的批量 `HasObjects` 确认，不可见返回与 L3 相同的 OBJECT_MISSING。daemon 同样处理。
> - **L3 读路径**（§7.3）：`CachedBlobs` 包在 BlobStore 外，小对象（≤1 MiB）进按字节计权的 moka 缓存，同一对象的并发读只读一次后端，后端并发上限 256。`--read-endpoint URL@REGION` 在 ServerInfo 中推荐缓存。
> - **`scn daemon`**（`client/daemon`）：Unix socket（0600）。`GetObjects` 由本地 CAS（磁盘文件 + 内存层，读取校验哈希，按修改时间回收）应答，未命中按调用者身份经 SDK 回源（会使用服务端推荐的区域缓存）；`RefService` 转发并在 `ResolveRef` 时登记预取；其余 RPC 在 HTTP/2 层原样转发（axum fallback），daemon 不需要理解它们。SDK 经 daemon 时每个请求带 `x-scion-upstream`，一个 daemon 可服务多个服务端。`scn` 默认经 daemon，按需拉起，失败时直连，`SCION_NO_DAEMON=1` 关闭。
> - **SDK**：`get_objects` 批量读取（工作副本的检出与同步改用它，检出时 8 批并发下载、后台线程写文件）；按区域选择只读端点，连接时不可达则直连服务端。
> - **修复**：WriteService 的请求大小上限原先等于上传分块（约 1.1 MB），10 万个改动的 `CreateChangeset` 会被拒绝；现在按 `max_changes_per_changeset` 留足空间（每条 256 字节，最多 256 MiB）。
> - **验收**：`server/cache/tests/bench.rs`（Apple M5、对象在内存、客户端与缓存同机）：200 个客户端冷启动检出同一组 2 万个对象，命中率 99.5%（理论上限），每个对象只回源一次；热缓存单节点每秒 153 万个对象（gRPC，每批 64 个），每次 1 个对象时每秒 16.4 万次请求。`client/daemon/tests/bench.rs`：10 万个文件经热 daemon 检出 5.3 秒，与本机直接创建这些文件的 5.4 秒相同（客户端目标 5 秒受本机文件系统限制，多线程创建文件反而更慢）。
> - **推迟**：缓存层的敏感前缀隔离与 L2 的 token 校验随 M5 的 ACL 一起做；daemon 的 `/metrics` 与 systemd / launchd 托管放到 M5；`GetTrees` 递归 RPC 未实现（预热与检出用按层批量的 `GetObjects`）。
> **M5a 认证与鉴权实现说明**（2026-10-17）：M5 拆成多个 PR，这是第一个。
> - **只用标准协议**：自签访问令牌（ES256 JWT，15 分钟）；不透明令牌（PAT / 刷新令牌，只存 SHA-256）；外部 JWT（JWKS 或 HS256 共享密钥，claims 映射可配）；RFC 7662 内省；RFC 8628 device flow；OPA 兼容的鉴权回调。starhub-server（HS256、`current_user`、rebac）只是一份配置，见 `docs/guide/auth.md`。
> - **ACL**：TOML 规则按主体、仓库通配、动作（read < write < land < admin）、ref、路径前缀授予，存在元数据库中，各节点 30 秒内重新加载。按 id 读对象需要整个仓库的读权限（§9 中 GetTrees 过滤目录条目的做法推迟，`GetTrees` 尚未实现）；`ListDir` / `Diff` 为只限部分路径的读者过滤条目；创建提交逐个检查改动路径；合入检查目标 ref。
> - **缓存层**：与 §9「L2 只验签」不同，L2 和 daemon 不解析令牌，用调用者的令牌向 L3 `CheckAccess(repo, read)`，结果按（令牌, 仓库）缓存 60 秒。这样对 PAT、外部令牌和鉴权回调都适用，吊销最多延迟 60 秒。daemon 在服务端不可达时，对 24 小时内确认过的身份放行本地读取。敏感前缀不缓存（§9）尚未实现。
> - **客户端**：SDK `Auth::Exchange` 换取访问令牌并提前 2 分钟续期；`scn login`（device flow 或 `--token`）、`whoami`、`logout`，凭据存 macOS 钥匙串或 0600 文件。

> **M5b redaction 与运维命令实现说明**（2026-10-18）：
> - **数据模型**：没有在 `objects_index` 上加列，而是新表 `redactions`，按 (仓库, 对象) 一条记录（原因、操作人、时间、是否有效、安排的删除时间、删除时间）。撤销只把记录标为无效，不删除。每次修改分配一个全局递增的 `seq`，取自单行计数表 `redaction_seq`：写入事务先更新这一行，行锁让写入按 seq 顺序提交，按 seq 增量同步不会漏掉晚提交的小序号。不用 SEQUENCE，因为序列值的分配顺序与提交顺序无关。
> - **只能 redact blob**：目录树和提交对象是历史结构，redact 它们会让检出和查询失效。
> - **读路径**：`GetObjects` 在确认可见之后批量查一次 redaction，任何一个对象被 redact 就整个请求返回 `REDACTED`，与 OBJECT_MISSING 一样要么完整成功、要么一开始就报错。`ReadFile`、`Blame` 和合入时的三方合并经由 `RedactedBlobs` 读取，逐个检查 blob。rebase 需要合并 redact 过的版本时合入失败，内容不会进入新提交。
> - **缓存层**：新 RPC `ObjectService.ListRedactedObjects(repo, after_seq)`，需要仓库中任意读权限。L2 和 daemon 按仓库保存 redact 过的对象，最多每 10 秒用调用者的令牌增量同步一次。命中 redaction 时返回与 L3 相同的 REDACTED，并删除本地字节（字节跨仓库共享，其它仓库的读取会重新回源）。同步失败时拒绝这次读取（`UNAVAILABLE`，令牌被拒等业务错误原样返回），不交出本地副本，否则上游不可用期间「最多 10 秒」的上限不成立；只有旧服务端的 `UNIMPLEMENTED` 按没有 redaction 处理。
> - **purge**：`Purge` 安排在宽限期（`--purge-grace`，默认 24 小时）后删除，期间可以撤销。janitor 每轮执行到期的删除，只有对象索引中有该对象的每个仓库都已 redact 才删除字节，否则保持待删除并记录日志。判断“哪些仓库有这个对象”用 `object_repos`：按仓库逐个用主键探测，不为 `objects_index` 增加按 id 的二级索引（写入热路径上不值得多维护一个）。`BlobStore` 新增 `delete`。
> - **运维命令**：`scion admin redact / unredact / purge / redactions`，直接读写存储；`AdminService` 提供同名的远程接口，权限为所涉及仓库的 admin，`all_repos` 和不限仓库的列表需要系统管理。`scion admin fsck` 校验 ref → 提交 → 目录树 → 文件的引用闭包、对象索引（含大小）与对象存储，`--verify` 读出并校验哈希。`scion admin lease ls` 列出租约。
> - **限制**：检出包含被 redact 文件的版本会失败（没有占位文件）；blame 追溯到被 redact 的版本时返回 REDACTED；purge 之后重新上传的相同内容不会自动删除。`rederive` 和 `drain` 推迟。
> **M5c 可观测性与限流实现说明**（2026-10-18）：
> - **新 crate `scion-obs`**（服务端最底层，store 以上都可以用）：
>   - 指标用 `prometheus-client`。进程内一份注册表，各层直接更新；区域缓存已有的统计在抓取时读取，不改成指标类型。`/metrics` 在独立端口（`--metrics-listen`），不经认证与限流；
>   - 追踪用 `tracing-opentelemetry` 加 OTLP gRPC 导出，按标准环境变量配置。
> - **中间件顺序**（从外到内）：追踪 → 指标 → 全局过载丢弃 → 认证 → 按租户限流 → deadline → 各服务，与 §5.3 一致。
>   - 过载丢弃放在认证之前：过载时不再花 CPU 验签；
>   - 按租户限流要用认证出的身份，所以在认证之后。
> - **指标的标签**：指标层在认证之外，路径由调用方任意给出。只给协议中的方法建时间序列，方法列表由 `scion-proto` 的 build.rs 从 proto 生成（`METHODS`）；其它路径归入 `unknown`。
> - **指标的状态码**：可能在响应头里（只有 trailers 的错误响应），也可能在流结束时的 trailers 里，所以包装响应体，结束时才记录。流式 RPC 的延迟因此是整个流的时长。
> - **按租户限流**：没有使用 `governor`，沿用已有的令牌桶。
>   - 租户：认证过的主体；匿名请求按来源 IP。
>   - 令牌桶和在途名额放在 moka 中，空闲 10 分钟淘汰、最多 100 万个 key：按 IP 计数时不会无限增长；空闲期间令牌早已补满，淘汰不改变限流结果。
>   - 集群内转发的请求不再计（与按仓库的写入限流一致）。`x-scion-forwarded` 是普通 metadata，调用方也能带上，所以只有值等于集群共享密钥（`--cluster-secret`，常数时间比较）时才算本集群转发的；没有配置密钥时一律计数。按仓库的写入限流同样改为这个判断。带了这个头的请求仍然不再转发（避免环路），伪造只会让调用方自己的请求失败。
> - **deadline**：
>   - 取客户端 `grpc-timeout` 与 `--max-request-timeout`（默认 60 秒）中较小的一个，超时返回 DEADLINE_EXCEEDED；
>   - 截止时间放进请求 extensions。转发给 leader 的合入和区域缓存的回源用剩余时间设置 `grpc-timeout`；区域缓存回源还会注入 `traceparent`；
>   - tonic 自身也按 `grpc-timeout` 计时并先触发（返回 CANCELLED），所以服务端上限之外，本层主要负责传递和计数。
> - **推迟**：
>   - SDK 和 `scn` 自动注入 `traceparent`；
>   - daemon 的 `/metrics`：客户端 crate 不能依赖服务端的 `scion-obs`；
>   - 派生滞后指标：还没有 deriver；
>   - 元数据库与 S3 调用的子 span。

> **仓库与分支管理实现说明**（2026-10-18）：这是接入 starhub 的最小闭环第一步，插在 M5c 和 M5d 之间。
> - **`RepoService`**：Create / Get / List / Update / Delete / Restore / Fork。配额推迟。
>   - 权限：创建、修改、删除、恢复需要该仓库名上的 admin；fork 需要来源的 read 加目标的 admin；ListRepos 逐个检查 read，只返回能读的仓库。
> - **软删除**：`repos` 表加 `deleted_at_ms`，按名字查仓库只找未删除的。宽限期（`--repo-delete-grace`，默认 24 小时，下限 2 小时）后 janitor 分批清除。
>   - 清除方式：按每张表的主键每次删一批（`(pk) IN (SELECT pk … LIMIT n)`，SQLite、PostgreSQL 通用，不依赖 ctid），最后删 `repos` 行，名字才释放。
>   - 宽限期下限的原因：各层缓存按仓库名索引（提交记录缓存、L2 与 daemon 的可见集和 redaction 状态），都加了 1 小时的 TTL。名字释放前它们一定已过期，所以不需要跨节点失效。
>   - `object_repos` 不计入已删除的仓库。
> - **fork**：一个事务内用 `INSERT … SELECT` 复制对象索引、提交、派生数据、路径历史和 ref；有效的 redaction 用 `ROW_NUMBER()` 分配新的 seq。不复制合入记录和请求结果。超大仓库的异步 fork 推迟。
> - **ref 写操作**：`CreateRef` / `UpdateRef`（可以非快进，即强制回退）/ `DeleteRef`，都是单个 ref 的 CAS，需要该 ref 的 land 权限。默认分支不能删除。
>   - 删除后重新创建的 ref 版本从 1 开始；WatchRefs 不推送删除。
> - **部署**：`Dockerfile`（多阶段构建，BuildKit 缓存，非 root 运行，基础镜像可换成镜像站）；`deploy/compose`（现为 `deploy/prd` 和 `deploy/dev`；PostgreSQL + SeaweedFS + server，可选 cache profile）。

> **ref 变化事件实现说明**（2026-10-18）：starhub 接入 S1 的事件部分。
> - **事件表**：`ref_events` 在 `land_commit` / `delete_ref` / `fork_repo` 的同一事务里写入。seq 取自单行计数器 `ref_event_seq`，与 `redaction_seq` 相同：行锁让 seq 顺序等于提交顺序，按 seq 增量读取不会漏掉晚提交的小序号。
>   - 代价：所有 ref 写入在这一行上串行。合入是按批提交的，PostgreSQL 基准与 main 没有可测出的差异（同一台机器上 762–847 对 763–788 次/秒）。
>   - 事件记录仓库名而不是 repo_id，仓库清除后在保留期内仍可读。
>   - 变化前的提交取自 `ref_log` 的上一版本，删除时取 `DELETE … RETURNING`。
> - **拉取**：`EventService.ListRefEvents(after_seq)`。`ref_event_bounds` 返回（最早保留的 seq，计数器的值），即使事件已全部清理也能判断消费方落后的部分是否已丢失（`truncated`）。
> - **推送**：webhook（HMAC-SHA256，GitHub 格式）与 NATS JetStream（`Nats-Msg-Id` 去重，等待确认）。
>   - 每个目标一个租约（`events/<目标>`），进度存在 settings 中，按 seq 顺序、至少送达一次，失败时退避重试。
>   - 新目标从服务端开始监听前的位置开始，不重放历史。
>   - 内容中的提交用 `scion_query::new_commits` 计算（从 new 可达、从 old 不可达），路径级改动取自提交的 delta。
>   - 与原设计的差别：subject 不含仓库名（仓库名可以包含 `.`）。
> - **推迟**：仓库级事件（创建、删除、fork 本身）、webhook 按仓库或按事件类型过滤、多个 webhook。

> **合入前检查实现说明**（2026-10-18）：starhub 接入 S1 的推送前检查。
> - **检查时机**：`Land`、`CreateRef`、`UpdateRef` 在授权和限流之后、进入合入队列之前，同步调用外部 webhook。
>   - 检查的是客户端提交的版本（服务端 rebase 不改变每个提交的 delta），所以放在协调器之外，不拖慢批处理。
>   - 本集群转发来的合入（带正确的 `--cluster-secret`）不再检查。
> - **检查的范围**：`new_commits(base, tip)`，base 为 ref 当前提交；新建的 ref 以默认分支为基准。没有新提交时不调用。
> - **结果**：2xx 允许（可以用 `{"allowed": false}` 拒绝）；4xx（408、429 除外）拒绝；其它情况视为不可用，默认拒绝（fail-closed）。
>   - 配置了检查却无法创建 HTTP 客户端时一律拒绝，不会悄悄跳过检查。
>   - 拒绝返回新的 `PRE_LAND_REJECTED`。

| **M5 生产化** | 4 周 | YugabyteDB 档与 `migrate-meta` 工具、ACL/OIDC/JWT、redaction、OTel、Helm、混沌测试、多区域 follower read、`conformance` 发布 | 全部 SLO 达标；单 AZ 故障演练通过 |

---

## 15. 待决策项

1. ~~元数据库~~：已确定为三档，SQLite / PostgreSQL / YugabyteDB，共用一套 sqlx 代码，不使用 CockroachDB（2026-10-16，见 §4.4）。
2. ~~规范编码~~：已确定为自定义 varint 格式，配齐 golden 测试向量，规范见 `SPEC.md`（2026-10-16，见 §4.2）。
3. ~~是否复用参考实现的代码~~：只借鉴思路（规范组织方式、故障注入埋点、redaction 等），不拷贝代码，全部自行实现（2026-10-16）。
4. ~~客户端语言~~：已确定为 Rust，只提供默认客户端，不做 Go 版本（2026-10-16）。

---

## 16. 外部系统集成用的扩展点

为了接入 starhub-server 并与 Gitaly 并行运行，需要新增以下**通用**能力（独立部署时同样可用），完整设计见 [`starhub-integration.md`](./starhub-integration.zh-CN.md) §5：

- `RepoAdminService`：Create/Delete/Get/Update/Fork/List，面向大量小仓库，每个仓库的常驻开销都要按需懒加载。
- 外部身份（可信 issuer 的 JWKS），可选外部鉴权回调（带 TTL 缓存）。
- pre-land webhook：同步调用，可配置超时和故障时的处理（fail-open 或 fail-closed）。
- `scion.<repo>.ref.updated` 事件（包含路径级 delta），发到 NATS，或通过 webhook 推送。
- 派生数据：`GitSha`、`LastCommitPerEntry`、`LfsPointerIndex`、`TreeStats`、`Sha256`。
  > **GitSha 实现说明**（2026-10-18）：所有提交都有 git 兼容 sha。导入的提交用扩展字段 `git.sha1`；其余提交按需逐层计算 blob/tree/commit（commit 对象多一行 `scion-id` 头保证单射），结果缓存在派生数据 `git-commit`、`git-sha1`（反向）、`git-tree`、`git-blob` 中，未变化的子树直接命中。`GetGitShas` 批量映射，事件与 pre-land 请求带 `old_git_sha`/`new_git_sha`/`git_sha`。S3 的 git-gateway 复用同一套对象格式。详见 [`docs/guide/git-sha.md`](../guide/git-sha.zh-CN.md)。
  > **Archive 实现说明**（2026-10-18）：`QueryService.Archive` 按目录树深度优先写 tar / tar.gz / zip（zip 用不需要 Seek 的流式写入），每满 `max_chunk_bytes` 发送一块，内存与单个文件相当；条目时间取提交时间，redact 的文件写占位说明。权限要求子树全部可读。详见 [`docs/guide/archive.md`](../guide/archive.zh-CN.md)。
  > **LastCommitPerEntry / TreeStats 实现说明**（2026-10-18）：`ListLastCommits` 沿第一父提交链（与 Blame 同语义），按（提交, 目录）缓存；第一父提交有缓存时按本提交的 delta 增量更新，否则用 PathHistory 索引逐条目找链上最近的候选，索引不可用时扫描 delta（上限 2000 个提交）。`GetTreeStats` 按目录树 id 缓存文件数、子目录数与字节数。详见 [`docs/guide/browse.md`](../guide/browse.zh-CN.md)。
- `QueryService.Archive`、`ImportService`（从 git 增量导入）。
- `scion git-gateway` 角色：实现 Gitaly 的 SmartHTTP/SSH 服务子集，让 git CLI 也能访问 Scion 仓库。
- 对象存储：与 starhub 共用 MinIO 时，使用独立的 bucket 或 `prefix`（见 §4.6）。
