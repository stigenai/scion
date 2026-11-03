# 缓存：区域缓存与本机 daemon

[English](cache.md) | 简体中文

Scion 的读取分三层，三层提供完全相同的 `ObjectService`：

```
scn / SDK ──► scn daemon（L1，每台机器）──► scion cache（L2，每个区域）──► scion server（L3，权威）
```

对象按内容寻址、不可变，每一层都可以永久缓存，不需要失效；ref 永远不缓存。缓存层没有持久状态：挂掉只损失命中率。

本文说明怎样部署区域缓存、客户端怎样选择读取路径，以及 daemon 的行为。

## 区域缓存 `scion cache`

### 启动

```sh
scion cache --listen 0.0.0.0:7090 \
  --upstream http://10.0.0.11:7080 --upstream http://10.0.0.12:7080 \
  --memory 8GiB --disk-dir /nvme/scion-cache --disk-size 500GiB \
  --warm mono:main
```

然后让服务端把它推荐给客户端：

```sh
scion server ... --read-endpoint http://cache.cn-beijing.internal:7090@cn-beijing
```

客户端连接服务端后从 `ServerInfo.read_endpoints` 里选一个缓存读取对象：优先区域匹配的（SDK 的 `ReadEndpoints::Region`），否则取第一个。连接时缓存不可达，就直接从服务端读取。写入、合入、ref 和查询始终发给服务端。

### 参数

| 参数 | 环境变量 | 默认 | 说明 |
|---|---|---|---|
| `--upstream URL` | `SCION_CACHE_UPSTREAMS` | 必填 | 上游服务端，可重复指定（同一集群的多个节点，互为副本） |
| `--memory SIZE` | `SCION_CACHE_MEMORY` | 1GiB | 内存层容量 |
| `--disk-dir DIR` | `SCION_CACHE_DISK_DIR` | 不启用 | 磁盘层目录，建议放在 NVMe 上；内存层淘汰的对象写入这里 |
| `--disk-size SIZE` | `SCION_CACHE_DISK_SIZE` | 64GiB | 磁盘层容量 |
| `--max-object SIZE` | | 8MiB | 超过这个大小的对象照常转发但不缓存（最大 16MiB） |
| `--advertise-url URL` / `--peer URL` | `SCION_CACHE_ADVERTISE_URL` / `SCION_CACHE_PEERS` | 单节点 | 多节点分片，见下文 |
| `--warm REPO[:REF,…]` | `SCION_CACHE_WARM` | 不预热 | 预热的仓库与 ref，可重复指定 |
| `--warm-blob-max SIZE` | | 0 | 预热时连同不超过这个大小的文件一起预取；0 只预取目录树 |
| `--no-hedge` | | 开启对冲 | 关闭对冲 |
| `--max-stream-lifetime DURATION` | `SCION_MAX_STREAM_LIFETIME` | 1h | GetObjects 流最长保持多久，到期以 UNAVAILABLE 结束，客户端重连。缓存还会在该时间加最多 30s 宽限后关闭连接，不再读取的客户端无法一直占用。`0s` 不限制 |
| `--tenant-streams N` | `SCION_TENANT_STREAMS` | 不限制 | 每个调用者同时打开的 GetObjects 流数。调用者按 `authorization` 头中 Bearer token 的哈希区分（非 Bearer 头按对端 IP 区分），同一用户的两个 token 各有一份额度。超出时返回 RESOURCE_EXHAUSTED |

### 读取路径

1. 本地命中（内存层，其次磁盘层）直接返回。
2. 未命中：同一对象的并发请求只回源一次（single-flight），一个请求里的多个未命中合成一次回源请求。
3. 多节点时先交给负责这个对象的节点，见下一节。
4. 回源到上游服务端：
   - **对冲**：请求超过最近 p95 延迟仍未返回时，向另一个上游再发一份，取先返回的结果。
   - **熔断**：连续失败 5 次的上游冷却 5 秒，期间不再向它发请求。
   - **故障转移**：不可用、超时、数据损坏时换下一个上游重试；对象不存在这类业务错误原样返回。
   - **校验**：回源得到的每个对象都校验哈希。

### 多节点分片

一个区域部署多个缓存节点时，每个节点声明自己的地址和其它节点：

```sh
# 节点 1
scion cache --upstream http://l3:7080 --advertise-url http://10.0.1.1:7090 \
  --peer http://10.0.1.2:7090 --peer http://10.0.1.3:7090
```

对象按 rendezvous 哈希分配给节点。节点收到不归自己负责的对象时，先转给负责的节点（只转一跳），那个节点再决定是否回源。因此整个区域对同一个对象只回源一次。增减节点时只有约 1/N 的对象改变归属。负责的节点不可达时自己回源。

客户端连接任何一个节点都可以，前面放负载均衡即可。

### 预热

`--warm mono:main` 订阅服务端的 WatchRefs：

- 订阅时先预取 ref 当前的整棵目录树（受数量上限限制）。
- 之后 ref 每次前进，只沿变化的路径预取新的目录树，子树 id 没变的目录整个跳过，代价与改动大小成正比。

服务端在本节点合入后立即推送；集群中其它节点的合入通过轮询元数据库发现，间隔由服务端的 `--ref-poll-interval` 控制（默认 1 秒）。

### 安全模型

缓存不持有凭证：调用者的 `authorization` 原样转给上游。

可见性与服务端一致：对象只对索引中有它的仓库可见。
- 字节按对象 id 缓存，不同仓库的相同内容只存一份。
- 应答前确认对象在本次请求的仓库里可见：回源成功的 (仓库, 对象) 记入可见集；本地命中而可见性未知的，先向服务端批量 `HasObjects` 确认。
- 不可见时返回与服务端相同的 `OBJECT_MISSING`。
- 同一对象的并发未命中按 (仓库, 对象) 合并，一个仓库的回源结果不会交给另一个仓库的请求。

按用户的权限同样向服务端确认（`CheckAccess`），见认证文档的[缓存与 daemon](auth.zh-CN.md#缓存与-daemon)。

## 本机 daemon `scn daemon`

`scn` 默认经本机 daemon 连接服务端。daemon 没在运行时，第一次需要连接的命令会在后台拉起它。拉起失败时直接连服务端。

- **对象读取**：daemon 先查本机缓存（磁盘加内存，读取时校验哈希）。未命中时按调用者的身份回源（经服务端推荐的区域缓存），同一仓库对同一对象的并发未命中只回源一次。与区域缓存一样，只把对象交给能看到它的仓库。
- **其它请求**：在 HTTP/2 层原样转发给服务端，daemon 不需要理解它们。
- **预取**：工作副本解析过的 ref（`ResolveRef`）之后每次前进，daemon 在后台取进变化的目录树和不超过 256 KiB 的文件，之后 `scn sync` 直接命中。订阅空闲 30 分钟后取消。
- **多个服务端**：一个 daemon 可以同时服务多个服务端，每个请求带着真正的服务端地址。
- **凭证**：daemon 不持有凭证，每个请求带着调用者自己的 token。
- **退出**：空闲 30 分钟后自动退出。

| 命令 / 环境变量 | 说明 |
|---|---|
| `scn daemon run [--max-disk BYTES] [--idle-timeout SECS]` | 前台运行（`--idle-timeout 0` 不自动退出） |
| `scn daemon status` / `scn daemon stop` | 查看 / 停止 |
| `scn doctor` | `objects:` 一行显示对象经由 daemon、区域缓存还是服务端读取 |
| `SCION_NO_DAEMON=1` | 不使用 daemon，直接连接（CI 容器里通常这么设置） |
| `SCION_DAEMON_SOCKET` / `SCION_DAEMON_CACHE_DIR` | 自定义 socket 与缓存目录 |

默认位置：

| 平台 | socket | 对象缓存 |
|---|---|---|
| Linux | `$XDG_RUNTIME_DIR/scion/daemon.sock` | `$XDG_CACHE_HOME/scion/objects`（默认 `~/.cache/scion/objects`） |
| macOS | `~/Library/Caches/scion/daemon.sock` | `~/Library/Caches/scion/objects` |

socket 只有当前用户能连接（权限 0600）。磁盘缓存默认上限 20 GiB，超过时按最近使用时间回收。daemon 目前只支持 Unix（Linux、macOS）。

## 性能参考

见[组件基准](../performance.md)，数据来自 `server/cache/tests/bench.rs` 与 `client/daemon/tests/bench.rs`：

- 200 个客户端冷启动检出同一组对象，命中率 99.5%，每个对象只从服务端取一次。
- 热缓存单节点每秒 153 万个对象（gRPC，每批 64 个）。
- 经 daemon 热检出 10 万个文件，耗时与直接创建这些文件相同。
