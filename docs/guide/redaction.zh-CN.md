# Redaction 与数据校验

[English](redaction.md) | 简体中文

## Redaction：清除泄露的内容

密码、密钥等敏感内容被提交进仓库后，改写历史代价很大：提交 id 全部改变，所有工作副本都要重新同步。Scion 的做法是 **redact**：

- 对象 id 和历史都不变；
- 读取被 redact 的文件内容时返回 `REDACTED`（gRPC `NOT_FOUND`，`ErrorInfo.reason = REDACTED`，metadata 带 `ids` 和 `redaction_ids`）；
- 需要彻底删除时再 **purge**，在宽限期后物理删除对象字节。

只有文件内容（blob）可以 redact。目录树和提交对象是历史结构，不能 redact。

### 操作

```sh
# 按 REV:PATH 或 blob id 指定，可以一次指定多个
scion admin redact --repo mono --path main:config/prod.env --reason "leaked database password"
scion admin redact --repo mono b1a3f0… --reason "..."

# 同一个对象可能也在 fork 里：一并 redact 所有含它的仓库
scion admin redact --repo mono --path main:config/prod.env --all-repos --reason "..."

scion admin redactions [--repo mono] [--id rd_…]   # 列出
scion admin unredact rd_…                          # 撤销（purge 执行之前）
scion admin purge rd_… [--grace 24h]               # 安排物理删除，宽限期内可以撤销
scion admin purge rd_… --now                       # 立即删除，不可撤销
```

这些命令直接读写存储，存储参数与 `scion server` 相同（`--meta postgres://…`、`--lite --data-dir DIR` 等）。也可以通过 `AdminService` 远程调用 `Redact`、`Unredact`、`Purge`、`ListRedactions`：

| 操作 | 需要的权限 |
|---|---|
| Redact 一个仓库 | 该仓库的 admin |
| Redact 所有仓库（`all_repos`） | 系统管理 |
| Unredact、Purge | 涉及的每个仓库的 admin |
| 列出某个仓库 | 该仓库的 admin |
| 不限仓库地列出 | 系统管理 |

每条记录保存原因、操作人和时间。服务端日志以 warn 级别记录每次操作。

### 生效范围

| 位置 | 行为 |
|---|---|
| 服务端 | `GetObjects`、`ReadFile`、`Blame` 立即返回 `REDACTED`；`ListDir`、`Diff`、`Log` 照常（只涉及路径和 id） |
| 合入 | rebase 需要三方合并 redact 过的版本时，合入失败并返回 `REDACTED`，内容不会被合并进新提交 |
| 区域缓存 `scion cache` | 每个仓库最多每 10 秒用调用者的令牌向服务端同步一次（`ObjectService.ListRedactedObjects`），之后返回 `REDACTED` 并删除本地副本 |
| 本机 daemon | 同上，按（服务端, 仓库）同步 |
| 同步失败 | 上游不可用、超时或离线时，缓存层**拒绝**读取（`UNAVAILABLE`），不返回本地副本；令牌被拒等错误原样返回。服务端对 `ListRedactedObjects` 返回 `UNIMPLEMENTED` 时同样拒绝读取（`UNAVAILABLE`） |
| 撤销 | 同样最多 10 秒后在缓存层生效 |

redaction 按仓库生效：同一个对象在别的仓库里没有被 redact 时，那个仓库照常可以读取。

### Purge 的条件

对象字节在所有仓库之间只存一份。只有对象索引中有它的**每个**仓库都已 redact，服务端才会删除字节。否则保持待删除，每轮检查时记一条 warn 日志，列出还能读取它的仓库，可以用 `--all-repos` 补上。

- **执行者**：服务端的 janitor 定期执行到期的删除（默认每 10 分钟）。删除是幂等的，集群中多个节点同时执行也没有问题。
- **宽限期**：`scion server --purge-grace` 设置 `AdminService.Purge` 未指定时的默认值，默认 24 小时。
- **删除后**：不能再撤销。缓存层的副本在 redact 时已经删除。

### 限制

- **检出**：检出包含被 redact 文件的版本会失败。先提交一个删除或替换该文件的修改，再 redact 历史中的版本。
- **Blame**：追溯到被 redact 的历史版本时，blame 返回 `REDACTED`。
- **重新上传**：purge 之后有人又上传了相同内容，字节会重新写入。仓库里的 redaction 记录仍在，读取照样返回 `REDACTED`，但字节要等下一次 redact 并 purge 才会删除。

## `scion admin fsck`：数据校验

只读地检查每个仓库：

- 每个 ref 指向的提交有记录；
- 每条提交记录的父提交有记录，提交对象存在、哈希正确、能解码，且父提交与记录一致；
- 从每个提交的根目录树出发的引用闭包：
  - 目录树存在、哈希正确、能解码；
  - 文件在仓库的对象索引里，索引中的大小与目录项一致，字节存在；
  - 被 redact 的文件只检查索引，已经 purge 的允许字节不存在。

```sh
scion admin fsck --meta postgres://… --blobs s3://bucket
scion admin fsck --repo mono --verify   # --verify 读出每个文件并校验哈希（默认只确认存在）
```

最多列出 1000 个问题，最后一行给出问题总数。发现问题时退出码非 0，可以用于定时巡检。在元数据迁移（`migrate-meta`）之后也应运行一次。

## `scion admin lease ls`：租约

列出合入 leader 等租约：持有者、换手次数（token）和剩余时间。用于排查集群中某个 ref 由哪个节点处理。

```sh
scion admin lease ls --meta postgres://…
land/mono/main  holder=http://10.0.0.5:7080  token=3  expires in 3812 ms
```
