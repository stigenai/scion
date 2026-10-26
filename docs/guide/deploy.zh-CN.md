# 部署

[English](deploy.md) | 简体中文

## Release 二进制

### 安装脚本

[`install.sh`](../../install.sh) 在 Linux 或 macOS 上安装最新的 release：按平台选择安装包，按 `SHA256SUMS` 校验，把 `scn`（加 `--server` 时还有 `scion`）放进 `/usr/local/bin`；不需要 sudo 就能装进用户自己 `PATH` 里的目录时，就装到那里。

```sh
curl -fsSL https://raw.githubusercontent.com/OpenCSGs/scion/main/install.sh | sh                       # 客户端
curl -fsSL https://raw.githubusercontent.com/OpenCSGs/scion/main/install.sh | sh -s -- --server        # 加上服务端，作为系统服务运行
```

加 `--server` 时，服务端以 Lite 模式作为系统服务运行：

| | Linux | macOS |
|---|---|---|
| 服务 | systemd 单元 `scion`，以系统用户 `scion` 运行 | launchd agent `com.opencsg.scion`，以当前用户运行 |
| 数据 | `/var/lib/scion` | `~/Library/Application Support/scion` |
| 日志 | `journalctl -u scion` | `~/Library/Logs/scion.log` |
| 停止 | `sudo systemctl stop scion` | `launchctl bootout gui/$(id -u)/com.opencsg.scion` |

| 选项 | 默认值 | 说明 |
|---|---|---|
| `--listen ADDR` | `127.0.0.1:7080` | 监听回环地址以外时，服务要求认证；安装脚本写入策略（admins 组管理一切，任何已登录身份可以读写、合入），并打印第一个管理员令牌。重复执行时保留已有的策略 |
| `--data-dir DIR` | 见上表 | |
| `--no-service` | | 只安装二进制，并打印启动服务端的命令 |
| `--version vX.Y.Z` | 最新版 | 也可以用 `SCION_VERSION` |
| `--install-dir DIR` | 见上文 | 也可以用 `SCION_INSTALL_DIR` |

GitHub 访问慢或访问不了时，`SCION_DOWNLOAD_URL` 把 `https://github.com/OpenCSGs/scion/releases/download` 换成镜像地址，目录结构同为 `<tag>/<文件>`。没有 systemd 时（例如在容器里），安装脚本只安装二进制，并打印启动服务端的命令。

### 手动安装

每个 [GitHub release](https://github.com/OpenCSGs/scion/releases) 都为每个平台提供两个包：服务端 `scion-server-<版本>-<平台>.tar.gz`（`scion`：服务端、区域缓存和管理命令）和客户端 `scn-<版本>-<平台>.tar.gz`。平台有 `x86_64-unknown-linux-gnu`、`aarch64-unknown-linux-gnu`（需要 glibc 2.35 及以上）、`aarch64-apple-darwin` 和 `x86_64-apple-darwin`；`SHA256SUMS` 列出校验和。

```sh
V=v0.1.1 T=x86_64-unknown-linux-gnu
for p in scion-server scn; do
  curl -fsSLO https://github.com/OpenCSGs/scion/releases/download/$V/$p-$V-$T.tar.gz && tar -xzf $p-$V-$T.tar.gz
done
curl -fsSLO https://github.com/OpenCSGs/scion/releases/download/$V/SHA256SUMS && sha256sum --check --ignore-missing SHA256SUMS
sudo install scion-server-$V-$T/scion scn-$V-$T/scn /usr/local/bin/

# 只给本机用：监听回环地址，不需要令牌
scion server --lite --data-dir ~/.scion-data

# 给团队用：其他人能连上，所以必须开启认证
scion server --lite --data-dir ~/.scion-data --listen 0.0.0.0:7080 --auth-required
scion admin token create --lite --data-dir ~/.scion-data --subject admin --group admins
```

macOS 上用 `shasum -a 256 --check --ignore-missing SHA256SUMS` 校验。二进制没有签名：用浏览器下载的文件要先执行 `xattr -d com.apple.quarantine scion scn` 才能运行。需要常驻运行时，和其它服务一样交给 systemd 或 launchd 管理。

## Docker 镜像

仓库根目录的 `Dockerfile` 构建一个包含 `scion`（服务端、区域缓存、管理命令）和 `scn`（客户端）的镜像，以非 root 用户（uid 10001）运行。

预构建的镜像是 `opencsg-registry.cn-beijing.cr.aliyuncs.com/opencsghq/scion`（标签 `latest` 和版本号，例如 `0.1.1`），包含 `linux/amd64` 与 `linux/arm64` 两种架构，Linux 服务器和 Mac（Docker Desktop，Apple 芯片与 Intel）都能直接运行。每个发布标签都由发布流程自动推送。自己构建多架构镜像时，编译阶段在构建机的架构上交叉编译，不需要 QEMU 模拟：

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/scion:latest --push .
# 访问不了 deb.debian.org 时指定 Debian 镜像源
docker buildx build --platform linux/amd64,linux/arm64 --build-arg DEBIAN_MIRROR=mirrors.ustc.edu.cn ...
```

```sh
docker build -t scion .
# 拉不到 Docker Hub 时换成镜像站
docker build -t scion \
  --build-arg RUST_IMAGE=docker.m.daocloud.io/library/rust:1.99-slim-bookworm \
  --build-arg RUNTIME_IMAGE=docker.m.daocloud.io/library/debian:bookworm-slim .
```

默认命令是 Lite 档：SQLite 加本地目录，数据放在卷 `/data`，并且**要求认证**（`--auth-required`）。第一次启动后，在容器里签发第一个令牌：

```sh
docker run -d --name scion -p 7080:7080 -v scion-data:/data scion
# 第一个令牌（只显示这一次）
docker exec scion scion admin token create --lite --subject admin --group admins

scn login http://localhost:7080 --token scn_pat_…
scn repo create http://localhost:7080/mono
```

保存策略之前，生效的是[默认策略](auth.zh-CN.md#访问控制策略)：admins 组管理一切，任何已登录的身份都可以读写、合入。

服务端监听在回环地址以外、又没有开认证时拒绝启动，因为能连上的人都能读写所有仓库。确实要匿名运行（例如只在内网测试）时加 `--allow-anonymous`（`SCION_ALLOW_ANONYMOUS=true`）：

```sh
docker run -d -p 7080:7080 -v scion-data:/data scion --log-format json server --listen 0.0.0.0:7080 \
  --lite --data-dir /data --allow-anonymous --repo mono      # 匿名，启动时预建仓库
```

| 端口 | 用途 |
|---|---|
| 7080 | `scion server` 的 gRPC |
| 7090 | `scion cache` 的 gRPC |
| 9464 | `--metrics-listen` 的 Prometheus 指标 |

所有参数都有对应的环境变量（`SCION_META`、`SCION_BLOBS`、`SCION_S3_*`、`SCION_AUTH_*`、`SCION_METRICS_LISTEN` 等），见 `scion server --help`。

## docker compose

单机部署有两份 compose 文件。两者都包含 PostgreSQL（Standard 档的元数据库）、SeaweedFS（S3 兼容对象存储，启动时创建 bucket `scion-objects`）和 `scion server`；加 `--profile cache` 还会在 7090 启动区域缓存 `scion cache`。

| | [`deploy/prd`](../../deploy/prd/docker-compose.yml) | [`deploy/dev`](../../deploy/dev/docker-compose.yml) |
|---|---|---|
| 用途 | 单机生产部署 | 开发和试用 |
| 认证 | 必须认证：`SCION_AUTH_REQUIRED=true` | 不认证：`SCION_ALLOW_ANONYMOUS=true` |
| 密钥 | 来自 `.env`，缺少时 compose 拒绝启动 | 固定的开发密钥 |
| 重启 | `restart: unless-stopped` | 不自动重启 |
| 端口 | 7080、9464（启用缓存时还有 7090、9465） | 另外开放 PostgreSQL 5432 和 S3 8333，供 `cargo run` 和测试使用 |
| 默认启动的服务 | 全部 | 只有 PostgreSQL 和 S3，服务端要加 `--profile server` |

### 生产：`deploy/prd`

```sh
cd deploy/prd
printf 'SCION_DB_PASSWORD=%s\nSCION_S3_SECRET=%s\n' "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" > .env
chmod 600 .env
# 国内用阿里云上的 scion 镜像，其余镜像走镜像站
printf 'SCION_IMAGE=opencsg-registry.cn-beijing.cr.aliyuncs.com/opencsghq/scion:latest\nREGISTRY=docker.m.daocloud.io\n' >> .env
docker compose up -d                       # 加 --profile cache 同时启动区域缓存
docker compose exec scion scion admin token create --subject admin --group admins   # 第一个管理员

scn login http://服务器:7080 scn_pat_…
scn repo create http://服务器:7080/demo
```

- **镜像**：默认 `opencsghq/scion:latest`；在 `.env` 里设置 `SCION_IMAGE=…:0.1.11` 可以固定版本。
- **登录与策略**：令牌可以直接使用。需要浏览器登录（不带令牌的 `scn login`）、外部签发方、组织或访问策略时，把签名密钥和 `auth.toml` 挂载到 `scion` 服务，并设置 `SCION_JWT_KEY_FILE` 和 `SCION_AUTH_CONFIG`，见 [auth.md](auth.zh-CN.md)。这些文件要能被镜像里的用户（uid 10001）读取。
- **指标**：9464 不需要认证，不要暴露到公网。
- **数据**：在 `pg-data` 和 `s3-data` 两个卷里；PostgreSQL 用 `docker compose exec postgres pg_dump -U scion scion` 备份。
- **超出单机时**：改用正式的 S3 或 MinIO 服务（见 [object-storage.md](object-storage.zh-CN.md)），多节点设置 `--advertise-url` 和 `--cluster-secret`（见 [cluster.md](cluster.zh-CN.md)）。

### 开发和试用：`deploy/dev`

```sh
docker compose -f deploy/dev/docker-compose.yml up -d                    # 只启动 PostgreSQL + S3，供 cargo run 和测试使用
# 同时启动服务端；国内用阿里云上的 scion 镜像，其余镜像走镜像站
SCION_IMAGE=opencsg-registry.cn-beijing.cr.aliyuncs.com/opencsghq/scion:latest REGISTRY=docker.m.daocloud.io \
  docker compose -f deploy/dev/docker-compose.yml --profile server up -d
# 能访问 Docker Hub 时，直接用 opencsghq/scion:latest
docker compose -f deploy/dev/docker-compose.yml --profile server up -d
# 用本地源码构建的服务端
docker build -t scion:dev . && SCION_IMAGE=scion:dev docker compose -f deploy/dev/docker-compose.yml --profile server up -d
# 同时启动区域缓存（7090）
docker compose -f deploy/dev/docker-compose.yml --profile server --profile cache up -d

scn doctor http://localhost:7080
scn repo create http://localhost:7080/demo
scn clone http://localhost:7080/demo
curl -s localhost:9464/metrics | head
```

## 健康检查

- **gRPC 标准健康检查**：`grpc.health.v1.Health` 不需要认证，可以用于 Kubernetes 的 gRPC probe。
- **compose 中的检查**：用镜像自带的 `scn doctor --endpoint http://127.0.0.1:7080`，它会完成一次能力协商。
