# Scion 默认客户端架构设计（Rust）

[English](rust-client-architecture.md) | 简体中文

> 版本：Draft v0.1 · 2026-10-16
> 配套文档：[`rust-server-architecture.md`](./rust-server-architecture.zh-CN.md)（协议、错误模型、认证都以它为准）

---

## 0. 定位与铁律

默认客户端是项目的「官方客户端」，包含 CLI `scn`、节点 daemon 和 Rust SDK。

**铁律：客户端只能依赖公开协议。**
- 客户端 crate 都放在 `client/` 目录下，**只允许依赖** `common/` 下的 `scion-proto`（生成代码）和 `scion-object`（纯计算：ObjectId、规范编码，不做 IO），以及 `client/` 内的其它 crate。
- **禁止**依赖任何服务端 crate（store、land、derive、api……）。这条规则由 CI 中的依赖分层检查强制执行。
- 只有测试代码（`dev-dependencies`）可以在进程内拉起服务端，用来做端到端测试。

这样能保证：默认客户端能做到的，任何人照着协议也一定能做到。官方**只维护这一个 Rust 客户端**，不提供 Go 等其它语言版本，但协议本身保持语言无关。

---

## 1. 组件

```
┌──────────────────────── 用户 / Agent / CI ────────────────────────┐
│  scn (CLI)               Rust 程序 / Agent 框架                    │
│   └─ scion-wc (工作副本)    └─ scion-client (SDK)                  │
│        └──────────────┬─────────┘                                 │
│                 scion-client (SDK: 传输·认证·重试·上传·拉取·land)    │
└───────────────────────┬───────────────────────────────────────────┘
                        │ 优先: Unix socket          否则: 直接连接
                        ▼                                  │
              scn daemon (每节点 L1)                        │
               本地 CAS 缓存 · 读请求就地应答                │
               其余请求原样转发                              │
                        │                                  │
                        ▼                                  ▼
                scion cache (L2, 区域) ──────▶ scion server (L3)
```

| 目录 | crate / 二进制 | 职责 |
|---|---|---|
| `client/sdk` | `scion-client` | **SDK**：连接管理、端点发现、认证与刷新、统一重试、批量上传和下载、land 辅助、`--json` 输出模型 |
| `client/wc` | `scion-wc` | **工作副本**：sparse 配置、checkout/sync、stat 缓存、status、commit 时收集改动、冲突状态、操作日志 |
| `client/daemon` | `scion-daemon` | **L1 缓存**：实现 `ObjectService`，持有本地 CAS；其余 RPC 透明转发；预取 |
| `client/cli` | `scion-cli` → 二进制 `scn` | 命令行；`scn daemon` 是子命令，所以客户端始终只有**一个二进制** |
| `client/vfs` | `scion-vfs`（M7 之后） | 虚拟化 checkout：Linux 用 FUSE，macOS 用 NFSv3 |

客户端二进制 `scn` 与服务端二进制 `scion` **分开发布**，客户端不会链接 sqlx、object_store 等服务端依赖，体积更小，也更容易审计。

---

## 2. 连接与端点发现

优先级从高到低：
1. 位置参数 URL 中的端点
2. 命令行参数 `--endpoint`
3. 环境变量 `SCION_ENDPOINT`
4. 如果命令使用工作副本，则使用 `.scion/state.toml` 中的端点
5. 已保存的登录端点
6. `http://127.0.0.1:7080`

`scn clone` 和 `scn import git` 跳过第 4 项。

**读写分流**（由 `scion-client` 自动完成）：
- 存在 daemon socket（`$XDG_RUNTIME_DIR/scion/daemon.sock`，macOS 上是 `~/Library/Caches/scion/daemon.sock`）时，**所有请求都发给 daemon**，由它决定就地应答还是转发。
- 没有 daemon 时：`ObjectService` 的读请求发到 `ServerInfo.read_endpoints` 里最近的 L2，其它请求发到 server。
- CLI 第一次需要读对象时，如果 daemon 未运行，会**自动在后台拉起**它（可用 `SCION_NO_DAEMON=1` 关闭，CI 容器里通常这么做）。

连接层：基于 tonic `Channel`，开启 HTTP/2 keepalive，启动时调用一次 `GetServerInfo` 并缓存 5 分钟，之后根据其中的 `features` 决定启用哪些能力。

---

## 3. 认证

| 场景 | 方式 |
|---|---|
| 开发者 | `scn login` 走 OIDC device flow；token 存入系统钥匙串（`keyring` crate），不可用时退回到权限为 0600 的文件 |
| Agent / CI | `SCION_TOKEN` 环境变量，或 workload identity 文件（`SCION_TOKEN_FILE`）通过 `AuthService` 换取短期 JWT |
| 刷新 | SDK 在 token 过期前 2 分钟自动刷新；收到 `UNAUTHENTICATED` 时刷新一次再重试一次 |

**daemon 不持有用户凭证**：每个请求带着调用者自己的 Bearer token，由 daemon 原样转发。多个用户共用一个 daemon 时，彼此的身份也不会混淆。

---

## 4. SDK（`scion-client`）

### 4.1 API 形态
```rust
let c = scion_client::Client::builder()
    .endpoint("https://scion.example.com")
    .repo("mono")
    .auth(Auth::from_env())
    .build().await?;

// 只用 API：读文件、改文件、合入（Agent 最常用的路径）
let head = c.resolve_ref("main").await?;
let src  = c.read_file(&head, "svc/a.rs").await?;              // Bytes，大文件自动分块拼接
let cs   = c.changeset(&head)
            .put("svc/a.rs", patched)                           // 小于 1 MiB 内联，否则自动先上传
            .delete("svc/old.rs")
            .message("fix: …")
            .create().await?;
match c.land("main", &cs).await {                              // 自动生成并持久化 request_id
    Ok(r) => println!("landed {}", r.tip),
    Err(e) if e.is_merge_conflict() => { let cf = e.conflicts(); /* 三方 blob id */ }
    Err(e) => return Err(e.into()),
}

// 流式与批量
let stream = c.get_trees(&root, TreeQuery::prefixes(["svc/", "lib/"]).depth(Unlimited));
c.fetch_blobs(ids).concurrency(64).for_each(|blob| …).await?;
```

### 4.2 内建的统一行为（严格按服务端文档 §3.5）
- **重试**：仅对表中标记为可重试的错误重试；遵守 `RetryInfo`；指数退避加全抖动；有总时长上限。
- **幂等**：写请求的 `request_id` 默认自动生成（UUIDv7）。工作副本场景下，id 在发出请求**之前**就写入操作日志，崩溃后可以用同一个 id 续做（见 §6）。
- **上传**：`HasObjects` 分批查询（每批 1,000 个）→ 只上传缺失的对象 → 小对象走批量接口，大对象分块流式上传；并发上限可配置。
- **下载**：`GetObjects` 按 64 个 id 一批发起多个流；**收到数据后先校验 BLAKE3**，不一致就报错并重新拉取（不信任任何缓存层）。
- **可观测**：自动注入 `traceparent`；SDK 暴露请求计数和耗时指标（可选开启 `metrics` feature）。

### 4.3 一致性验收
`scion-client` 负责执行服务端文档 §12 中的一致性测试 YAML 场景，既用来验收服务端，也用来验收 SDK 自身。

---

## 5. 工作副本（`scion-wc`）

### 5.1 目录布局
```
<root>/
  .scion/
    state.toml        # repo, endpoint, base 提交, 跟踪的 ref, sparse 配置, 本地草稿栈
    dirstate.redb     # stat 缓存: path → (size, mtime_ns, ctime_ns, ino, mode, blob_id)
    oplog/            # 操作日志: 进行中的 checkout/commit/land，用于崩溃恢复
    conflicts/        # 冲突记录: path → {base, ours, theirs} blob id
    lock              # 操作锁 (flock)，防止并发 scn 命令互相破坏
  <只物化 sparse 中列出的目录>
```
- `dirstate` 使用 `redb`（纯 Rust、事务型的嵌入式 KV），对百万级路径也能做到增量更新，不需要每次全量重写。
- **sparse 配置**：一组目录前缀（加可选的排除规则）。`.scionignore` 使用 gitignore 语法，由 `ignore` crate 解析。

> **实现说明（稀疏检出）**：范围为根目录下的文件加若干目录（cone 语义，但不含上级目录中的文件），保存在 `.scion/state.json` 的 `sparse` 字段（None 为全部检出）；排除规则暂未实现。
> - 遍历目录树时剪枝：只进入范围目录本身、其上级和其下级，范围外的目录树和文件内容不请求。服务端不需要改动；按前缀过滤的 `GetTrees` 只是减少往返的优化，留到以后。
> - sync、land、客户端 rebase 和调整范围时，比较的双方都按同一范围过滤：dirstate 只有范围内的文件，和完整目录树比较会把范围外的文件当作删除。
> - 范围外出现的未跟踪文件一律拒绝（status/commit 报错），因为服务端上同一路径可能有没检出的内容，提交会悄悄覆盖它。
> - `scn sparse add|rm|set|disable` 要求没有未提交的改动，按当前快照（有未合入的提交时为它）检出或删除差异部分。旧范围外的未跟踪文件按新范围判断：仍在范围外的报错；新范围包含的，与快照版本相同则接管、不同则拒绝（不覆盖），快照中没有的保留为新增文件。

### 5.2 checkout / sync
```
1. 解析目标 ref → 提交 → root tree
2. 并行 GetTrees(prefix 过滤)，只拉取 sparse 范围内的 tree
3. 新旧 tree 做差异 (只比较 hash 不同的子树)，得到 增/删/改 清单
4. 写操作日志: {op: checkout, from, to, plan_hash}
5. 批量拉取缺失的 blob (先查 daemon/本地)，并行写文件:
     先写到临时文件 → fsync(可配置) → rename 原子替换；处理权限位、符号链接、目录增删
     打开目录时复用父目录 fd，减少路径查找开销
6. 更新 dirstate 和 state.toml(base=to)，清除操作日志
```
- **成本只与差异大小有关**：`sync` 到新的 main 时只改动有变化的路径。
- **本地有未提交改动**：`sync` 会先在服务端 `Rebase` 本地的草稿提交，再 checkout；未提交的脏文件如果和更新范围冲突就拒绝执行，并提示先 commit（不在本地做文件级合并，保持客户端简单）。

### 5.3 status
1. 用并行遍历（`jwalk` / `ignore`）扫描 sparse 目录。
2. 和 `dirstate` 比较 stat 信息：完全一致就跳过；不一致的文件才用 BLAKE3 重新计算 hash（大文件用 mmap，多文件用 rayon 并行）。
3. 新文件、删除的文件、修改的文件，分别对照 base tree 得出结果。
4. **处理「同一秒内修改」的问题**：mtime 与写入 dirstate 的时间落在同一个时间窗口内的条目标记为「不确定」，下次必定重新计算 hash，避免漏掉改动。

M6 起可选**文件监听模式**：daemon 用 `notify` crate 持续记录脏路径，此时 `status` 的成本只与脏文件数量有关。

### 5.4 commit / 草稿栈
- `scn commit`：收集改动 → 上传缺失的 blob → `CreateChangeset(parent = 栈顶草稿或 base)`，得到新的草稿 id，追加到 `state.toml` 的草稿栈中。
- **草稿保存在服务端**：客户端只记录 id，换一台机器 `scn checkout <draft-id>` 就能继续工作。
- `scn amend` 和 `scn split` 都是基于服务端 `CreateChangeset` 和 `Rebase` 的组合操作，客户端不需要任何历史逻辑。

### 5.5 land 与冲突处理
- `scn land`：先把 `request_id` 写入操作日志，再调用 `Land(stack = 草稿栈)`。
  - 成功：清空草稿栈，`base` 指向新的 tip；默认自动 sync。
  - 超时或崩溃：下次执行任意 `scn` 命令时，先检查操作日志，用同一个 `request_id` 调 `GetLandStatus` 收尾。**保证不会重复合入，也不会丢掉已经成功的合入。**
  - `MERGE_CONFLICT`：把三方 blob 记录到 `.scion/conflicts/`，在工作区写入带冲突标记的文件；`scn resolve <path>` 标记为已解决后再次 `scn land` 即可。`--json` 输出中包含完整的冲突结构，Agent 可以直接取三方内容自行解决。

---

## 6. 崩溃一致性

| 操作 | 操作日志记录 | 恢复方式 |
|---|---|---|
| checkout/sync | from、to、计划的 hash | 重新执行（操作是幂等的：已经是目标内容的文件会被跳过） |
| commit | 已上传的对象、request_id | 用同一个 request_id 重发 `CreateChangeset` |
| land | request_id、草稿栈 | 调 `GetLandStatus` 查询结果，或用同一个 id 重发 |

所有会修改工作区的命令都必须先拿到 `.scion/lock`。

---

## 7. 节点 daemon（`scn daemon`）

### 7.1 职责
- 在 Unix socket 上实现**完整的 `ObjectService`**，用本地 CAS 缓存就地应答；未命中时向上游（L2）回源。
- 其余 RPC（Write、Land、Ref、Query、Auth）**原样转发**，基于 tonic/hyper 实现透明代理，不做解析，也不保留状态。
- 订阅 `WatchRefs`：main 前进时，按本机工作副本登记的 sparse 前缀预取新的 tree 和 blob。

### 7.2 本地缓存
- 存储：`foyer` 的磁盘缓存，或者按 `objects/<hh>/<hex>` 分片的目录。总容量有上限（默认取 50 GB 和磁盘 20% 中较小的一个），按 LRU 淘汰。
- **写入缓存前校验 hash**，读取时可配置抽样校验。
- 同一对象的并发未命中由 singleflight 合并，只回源一次。
- **缓存策略**：服务端会在 `ObjectChunk` 上标记 `cacheable=false`（敏感前缀的对象），daemon 对这类对象只转发，不落盘。
- **多用户**：socket 权限设为 0660，归属 `scion` 组；对象内容寻址，读取时请求者必须已知 hash（capability 模型）。
- 启动快照：可以从 `SCION_SNAPSHOT_URL` 拉取周期性生成的热对象包来预热，CI 节点冷启动时很有用。

### 7.3 生命周期
CLI 会按需拉起 daemon；也可以由 systemd/launchd 托管（`scn daemon install`）。空闲 30 分钟且没有工作副本登记时自动退出（可配置）。`/metrics` 暴露命中率、缓存大小、回源延迟等指标。

---

## 8. CLI（`scn`）

### 8.1 命令
```
scn login | logout | whoami
scn clone <repo> [dir] [--sparse a/,b/] [--ref main]
scn sparse add|rm|ls <prefix>
scn status | diff [--staged-against <rev>] | log [path] | blame <file>
scn commit -m <msg> | amend | split
scn land [--ref main] [--no-sync]
scn sync | checkout <rev>
scn resolve <path> | conflicts
scn cat <rev>:<path> | ls <rev>:<dir> | show <rev>     # 不需要工作副本
scn daemon [run|install|status|stop]
scn doctor                                             # 检查连接、认证、daemon、缓存、时钟偏差
```
- log、blame、diff（针对历史版本）**全部调用服务端 Query RPC**，客户端没有任何历史数据。

### 8.2 面向 Agent 的设计
- 所有命令支持 `--json`。输出带 `"schema": "scion.cli.status/v1"` 字段，schema 文件放在 `schemas/` 目录，变更时遵循和 proto 相同的兼容规则。
- 退出码稳定：`0` 成功、`1` 一般错误、`2` 用法错误、`3` 合并冲突、`4` 可重试错误（已用尽重试次数）、`5` 认证失败。
- 设置 `SCION_TOKEN` 时完全不交互；默认不分页，也不输出颜色（TTY 下除外）。

### 8.3 实现
`clap`（derive）、`tokio`、`indicatif`（只在 TTY 下显示进度）、`anyhow`/`thiserror`，错误信息附带下一步提示（例如「运行 `scn conflicts` 查看冲突」）。

---

## 9. 发布与分发

| 渠道 | 形式 |
|---|---|
| Linux | 静态链接的 musl 二进制（x86_64/aarch64）、deb/rpm |
| macOS | universal 二进制、Homebrew tap |
| CI | `ghcr.io/<org>/scn` 镜像；也可以在 K8s 上以 DaemonSet 方式运行 daemon，Pod 通过 hostPath 挂载 socket |
| Rust 开发者 | `cargo install scion-cli`；SDK 以 `scion-client` 发布到 crates.io |
| 安装脚本 | `curl … \| sh`，脚本会校验签名（cosign） |

---

## 10. 测试

| 层级 | 内容 |
|---|---|
| 单元 | dirstate 的「同一秒内修改」问题、sparse 规则匹配、tree 差异计算、原子写入 |
| 端到端 | 在 dev-dependencies 中进程内拉起服务端（内存后端），跑完整的 clone → commit → land → sync 流程 |
| 一致性 | Rust SDK 执行服务端文档 §12 的 YAML 场景 |
| 崩溃恢复 | 在操作日志的每个阶段注入崩溃（kill -9 模拟），验证恢复后的状态正确 |
| 差分 | 对同一个仓库执行相同操作，比较 `scn status/diff` 与 `git status/diff` 的输出 |
| 性能 | 10 万文件的 sparse checkout、status（无改动和 1% 改动两种情况）、daemon 命中率 |

**函数级测试要求**：与服务端相同，严格执行服务端文档 §12.1（每个含逻辑的函数都要有单元测试，覆盖正常、边界和全部错误分支；`scion-wc` 属于核心 crate，行覆盖率不低于 90%）。

**性能目标**：在 10 万文件规模下，无改动的 `scn status` 小于 300 毫秒，命中 daemon 时 sparse clone 小于 5 秒。

---

## 11. 里程碑（与服务端合并排期）

| 里程碑 | 客户端交付 |
|---|---|
| M0 | `scion-client` 骨架、`GetServerInfo`、golden 向量测试、连接与认证（静态 token） |
| M1 | `scn clone/sparse/status/commit/land/sync/cat/ls`（直连，不经 daemon）、dirstate、操作日志 |
| M2 | `scn log/blame/diff/show`、`--json` schema v1 |
| M3 | land 崩溃恢复（`GetLandStatus`）、冲突处理流程、一致性 YAML 执行器 |
| M4 | `scn daemon`（L1 缓存、透明转发、WatchRefs 预取）、自动拉起 |
| M5 | device flow 登录、钥匙串、`scn doctor`、各平台发布、DaemonSet 部署方式 |
| M6 | 文件监听模式的 status |
| M7 | `scion-vfs`：Linux FUSE（`fuser` crate）、macOS NFSv3 loopback |
