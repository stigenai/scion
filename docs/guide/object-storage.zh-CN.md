# 对象存储：SeaweedFS、MinIO 与其他 S3 兼容服务

[English](object-storage.md) | 简体中文

Scion 服务端把文件内容、目录树和提交对象存放在对象存储中（`--blobs`）。可选后端：

| `--blobs` | 用途 |
|---|---|
| `mem` | 测试，进程退出即丢失 |
| 目录路径（或 `--lite --data-dir`） | 单机 Lite 档 |
| `s3://bucket[/prefix]` | 生产：SeaweedFS、MinIO、AWS S3，或其他兼容 S3 的服务 |

数据读写只用 S3 核心子集（PUT / GET（含 Range）/ HEAD / LIST），不依赖条件写或版本控制。SeaweedFS 和 MinIO 每次 CI 都会跑完整的存储一致性测试。

## 压缩

服务端把对象用 zstd（级别 3）压缩后存储，前提是能省下至少 10%。已经压缩过的内容（模型权重、压缩包、图片等）原样存储。

- **透明**：对象 id、元数据中的大小，以及客户端看到的一切，都是原始字节的。
- **旧对象**：压缩之前写入的对象，或关闭压缩时写入的对象，读出来不变；之后再上传同一个对象时保留已有的副本。
- **效果**：导入 starhub-server（6,312 个提交）时，对象字节从 1,007 MB 降到 227 MB；同一段历史的 git pack 是 392 MB。
- **关闭**：`SCION_BLOB_COMPRESSION=off` 让新对象不压缩存储，例如 CPU 紧张而存储便宜时。两种形式都能读取。

## Pack

一起写入的对象存在一起：一个提交新产生的目录树和它的 changeset，以及每批上传（最多 16 MB 或 4,096 个对象），写进 `packs/` 下的一个 *pack*，而不是每个对象各存一份。元数据库记录每个打包对象的位置（`pack_locations`），读取时只取它的字节范围。

- **原因**：大多数对象很小（starhub-server 的目录树和提交大约 1 KB）。在 MinIO 上，每个存储对象都是一个目录加一个 `xl.meta` 文件，各至少占一个磁盘块，而且每次写入都是一次请求。导入 starhub-server 时，原来存 67,636 个对象、占 MinIO 磁盘 546 MB；打包后存 10,065 个、占 282 MB，导入时间从 154 秒降到约 95 秒。
- **读取**：第一次读一个 pack 时，同时从末尾取回它的索引（小于 64 KB 时取回整个 pack），同一个提交的其它对象就不用再查。
- **不打包的对象**：超过 1 MB 的对象，以及单独写入的对象。打包功能之前写入的对象仍从原处读取。
- **开销**：元数据库里每个打包对象一行（starhub-server 占 PostgreSQL 13 MB），以及范围读取（带 `Range` 的 `GET`），所有 S3 兼容服务都支持。
- **Redaction**：purge 一个打包对象时，会重写它所在的 pack，把它去掉。
- **关闭**：`SCION_BLOB_PACKS=off` 让新对象都不打包。已经打包的对象仍然可读。

## Repack

`scion admin repack` 把所有可达对象重写进少数几个大 pack，每个文件和目录的旧版本存成相对下一个更新版本的 delta，然后删除被取代的 pack 和散落对象。它相当于 Scion 的 `git gc`，`scion server` 会自动运行它（见下文）；服务端运行期间也可以安全执行。

```sh
scion admin repack --meta postgres://… --blobs s3://scion-objects   # --dry-run 只统计
```

- **自动运行**：服务端每隔 `--repack-check-interval`（15 分钟）统计一次上次 repack 之后写进对象存储的数据量（比每次 repack 留下的标记 `repack/last` 更新的 pack 和散落对象），满足以下任一条件就 repack：
  - 达到 `--repack-after`（256 MiB），例如大批导入之后；
  - 距上次 repack 已过 `--repack-every`（24 小时），且至少写入了 1 MiB。

  这些 repack 是增量的：只打包上次 repack 之后写入的数据（比上次 repack 更新的 pack 和散落对象，不含它自己写出的 pack），在新对象之间配对版本，所以开销取决于写入量，而不是存储的总量。距上次全量 repack 超过 `--repack-full-every`（7 天）后，下一次就是全量：重写全部可达对象，同时把文件的旧版本存成相对新版本的 delta，并压缩元数据。设为 `0s` 时每次都是全量。

  多个节点时同一时间只有一个节点 repack（靠元数据库中的租约）。`--repack-after 0 --repack-every 0s` 关闭自动运行。日志里会写明为什么运行、是哪一种、做了什么。
- **手动运行**：`scion admin repack --incremental` 做一次增量 repack；不加时做全量。
- **Delta**：旧版本用 zstd 压缩，以较新的版本作参照（即 `zstd --patch-from` 的做法）。读得最多的最新版本保持完整。delta 链最长 50，与 git 相同；读取时沿链展开，并缓存用到的基版本。同一个 pack 中的基版本用偏移和长度指明（几个字节，类似 git 的 `OFS_DELTA`），其它基版本用 id 指明；含有前一种 delta 的 pack，需要不低于写入它的 repack 版本的服务端才能读取。
- **效果**：starhub-server（6,338 个提交，PostgreSQL + MinIO）的 67,847 个对象写进 1 个 27 MB 的 pack，其中 51,262 个是 delta，耗时 24 秒。MinIO 从 283 MB 降到 86 MB（其余是三个超过 8 MB 的已提交二进制文件，不打包）；Gitaly 中同一段历史的仓库是 82 MB。
- **删除被取代的数据**：其中已经没有任何对象的 pack，且早于 `--prune-after`（默认 1 小时，避免碰到正在写入的 pack）；以及已经打包的散落对象。
- **读取**：大 pack 按 1 MiB 的块读取，最多缓存 128 MiB，所以一个提交的对象和一条 delta 链上的各个版本大多从内存读取。
- **元数据**：repack 会重写所有对象的 pack 位置，之后在 PostgreSQL 上压缩元数据。pack 位置表小于 256 MB 时用 `VACUUM FULL` 原地重写，最多锁表约一秒，200 毫秒内拿不到锁就跳过；更大的表只做 vacuum，之后的写入复用空出的空间。对象 id 和路径上的索引用 `REINDEX CONCURRENTLY` 重建，不阻塞读写。starhub-server 的数据库因此从 68 MB 降到 54 MB。
- **Redaction**：purge 一个被其它对象当作基版本的对象时，先把那些对象重写成完整版本。

## 连接参数

| 环境变量 | 说明 | 默认 |
|---|---|---|
| `SCION_S3_ENDPOINT` | 服务地址，如 `http://minio:9000`。访问 AWS S3 时留空 | 无 |
| `SCION_S3_PROVIDER` | `auto`、`minio`、`seaweedfs`、`aws`、`generic` | `auto` |
| `SCION_S3_ACCESS_KEY_ID` / `SCION_S3_SECRET_ACCESS_KEY` | 访问密钥；未设置时读 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | 无 |
| `SCION_S3_REGION` | 区域；未设置时读 `AWS_REGION` | `us-east-1` |
| `SCION_S3_VIRTUAL_HOSTED` | `true` 时用 `bucket.host` 风格 | 按服务决定，只有 AWS 默认为 `true` |
| `SCION_S3_MAX_RETRIES` | 单个请求的最大重试次数 | 10 |

`s3://` 后面的 `/prefix` 用来让多个部署共用一个 bucket。

## 服务适配

启动时服务端会先检查对象存储，任何一步失败都会拒绝启动，并说明原因：

1. **识别服务**：读 endpoint 根路径返回的 `Server` 响应头，`MinIO` 识别为 MinIO，`SeaweedFS …` 识别为 SeaweedFS，`AmazonS3` 识别为 AWS。
   - `SCION_S3_PROVIDER=auto` 时直接采用识别结果。
   - 显式配置的服务与识别结果不一致时报错，多半是 endpoint 写错了。
   - 前面有反向代理、识别不出时，以配置为准。
2. **健康检查**：MinIO 检查 `/minio/health/ready`，SeaweedFS 检查 `/healthz`。服务还没就绪时，报错中会带上返回的状态码。
3. **bucket 检查**：bucket 不存在时，报错中给出该服务的创建方法。
4. **读写自检**：写入、读回并校验一个探测对象。用于发现密钥错误、权限不足等问题。

启动日志会记录识别结果，例如：

```
object store ready backend=s3 (minio, Server: MinIO) bucket=scion-objects
```

| | SeaweedFS | MinIO |
|---|---|---|
| 识别依据（`Server` 响应头） | `SeaweedFS <版本>` | `MinIO` |
| 健康检查 | `/healthz` | `/minio/health/ready` |
| 创建 bucket | `echo 's3.bucket.create -name B' \| weed shell` | `mc mb <alias>/B`，或容器环境变量 `MINIO_DEFAULT_BUCKETS=B` |
| 请求风格 | path | path |
| 已测试版本 | 4.48 | 2023.12.23（bitnami 镜像，与 starhub / CSGHub 部署一致） |

## SeaweedFS

开发环境默认使用 SeaweedFS。MinIO 开源版已归档、停止维护，SeaweedFS 是仍在维护的 Apache 2.0 项目。

```sh
docker compose -f deploy/dev/docker-compose.yml up -d s3 s3-init
SCION_S3_ENDPOINT=http://localhost:8333 \
SCION_S3_ACCESS_KEY_ID=scion SCION_S3_SECRET_ACCESS_KEY=scion-dev-secret \
SCION_S3_PROVIDER=seaweedfs \
scion server --meta postgres://scion:scion-dev-secret@localhost:5432/scion --blobs s3://scion-objects
```

## MinIO

接入 starhub / CSGHub 时，可以直接使用它们现有的 MinIO：

```sh
docker compose -f deploy/dev/docker-compose.yml --profile minio up -d minio
SCION_S3_ENDPOINT=http://localhost:9000 \
SCION_S3_ACCESS_KEY_ID=scion SCION_S3_SECRET_ACCESS_KEY=scion-dev-secret \
SCION_S3_PROVIDER=minio \
scion server --meta postgres://scion:scion-dev-secret@localhost:5432/scion --blobs s3://scion-objects/scion
```

与其他服务共用 MinIO 时，建议给 Scion 单独的 bucket 或前缀，并只授予该范围的读写权限。

## 代理

请求遵循系统代理设置（`HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY`）。对象存储在内网或本机时，记得把它的地址加入 `NO_PROXY`，例如 `NO_PROXY=127.0.0.1,localhost,minio`。

## 测试

对象存储测试通过环境变量指向真实服务，未设置时跳过：

```sh
SCION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 SCION_TEST_S3_BUCKET=scion-test \
SCION_TEST_S3_ACCESS_KEY=scion SCION_TEST_S3_SECRET_KEY=scion-dev-secret \
SCION_TEST_S3_PROVIDER=minio \
cargo test -p scion-store-blob -p scion
```

`SCION_TEST_S3_PROVIDER` 用来校验识别结果，可以不设置。
