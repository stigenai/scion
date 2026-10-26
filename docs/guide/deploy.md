# Deployment

English | [简体中文](deploy.zh-CN.md)

## Release binaries

### The installer

[`install.sh`](../../install.sh) installs the latest release on Linux or macOS: it picks the platform's packages, checks them against `SHA256SUMS`, and puts `scn` (and with `--server`, `scion`) in `/usr/local/bin`, or in a directory of the user's on `PATH` when that needs no sudo.

```sh
curl -fsSL https://raw.githubusercontent.com/OpenCSGs/scion/main/install.sh | sh                       # the client
curl -fsSL https://raw.githubusercontent.com/OpenCSGs/scion/main/install.sh | sh -s -- --server        # and the server as a service
```

With `--server`, the server runs in Lite mode as a service:

| | Linux | macOS |
|---|---|---|
| Service | systemd unit `scion`, as the system user `scion` | launchd agent `com.opencsg.scion`, as you |
| Data | `/var/lib/scion` | `~/Library/Application Support/scion` |
| Logs | `journalctl -u scion` | `~/Library/Logs/scion.log` |
| Stop | `sudo systemctl stop scion` | `launchctl bootout gui/$(id -u)/com.opencsg.scion` |

| Option | Default | What |
|---|---|---|
| `--listen ADDR` | `127.0.0.1:7080` | Beyond loopback, the service requires authentication, and the installer writes a policy (admins manage everything; anyone signed in can read, write and land) and prints the first administrator's token. Run again, it keeps an existing policy |
| `--data-dir DIR` | as above | |
| `--no-service` | | Install the binaries only, and print how to start the server |
| `--version vX.Y.Z` | the latest | Also `SCION_VERSION` |
| `--install-dir DIR` | as above | Also `SCION_INSTALL_DIR` |

`SCION_DOWNLOAD_URL` replaces `https://github.com/OpenCSGs/scion/releases/download` with a mirror holding the same `<tag>/<file>` layout, where GitHub is slow or unreachable. Without systemd (in a container, say), the installer installs the binaries and prints the command to start the server.

### By hand

Each [GitHub release](https://github.com/OpenCSGs/scion/releases) has two packages per platform: the server, `scion-server-<version>-<target>.tar.gz` (`scion`: server, regional cache and admin commands), and the client, `scn-<version>-<target>.tar.gz`. The targets are `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` (glibc 2.35 or newer) and `aarch64-apple-darwin` and `x86_64-apple-darwin`; `SHA256SUMS` lists the checksums.

```sh
V=v0.1.1 T=x86_64-unknown-linux-gnu
for p in scion-server scn; do
  curl -fsSLO https://github.com/OpenCSGs/scion/releases/download/$V/$p-$V-$T.tar.gz && tar -xzf $p-$V-$T.tar.gz
done
curl -fsSLO https://github.com/OpenCSGs/scion/releases/download/$V/SHA256SUMS && sha256sum --check --ignore-missing SHA256SUMS
sudo install scion-server-$V-$T/scion scn-$V-$T/scn /usr/local/bin/

# Just this machine: loopback, no token needed
scion server --lite --data-dir ~/.scion-data

# For a team: reachable by others, so authentication is required
scion server --lite --data-dir ~/.scion-data --listen 0.0.0.0:7080 --auth-required
scion admin token create --lite --data-dir ~/.scion-data --subject admin --group admins
```

On macOS, use `shasum -a 256 --check --ignore-missing SHA256SUMS`. The binaries are not signed: a file downloaded in a browser needs `xattr -d com.apple.quarantine scion scn` before it runs. To keep the server running, run it under systemd or launchd like any other service.

## Docker image

The `Dockerfile` at the repository root builds an image with `scion` (the server, the regional cache and the admin commands) and `scn` (the client). It runs as an unprivileged user (uid 10001).

The prebuilt image is `opencsghq/scion` on Docker Hub (tags `latest` and a version such as `0.1.1`), for `linux/amd64` and `linux/arm64`: it runs on Linux servers and in Docker Desktop on Macs, Apple silicon and Intel alike. The release workflow pushes it for every release tag. A multi-arch build of your own cross-compiles on the build machine's architecture, without QEMU emulation:

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/scion:latest --push .
# When deb.debian.org is unreachable, name a Debian mirror
docker buildx build --platform linux/amd64,linux/arm64 --build-arg DEBIAN_MIRROR=mirrors.ustc.edu.cn ...
```

```sh
docker build -t scion .
# When Docker Hub is unreachable, use a mirror
docker build -t scion \
  --build-arg RUST_IMAGE=docker.m.daocloud.io/library/rust:1.99-slim-bookworm \
  --build-arg RUNTIME_IMAGE=docker.m.daocloud.io/library/debian:bookworm-slim .
```

The default command runs the Lite tier, SQLite and a local directory, with its data in the volume `/data`, and **requires authentication** (`--auth-required`). After the first start, issue the first token inside the container:

```sh
docker run -d --name scion -p 7080:7080 -v scion-data:/data scion
# The first token (shown only this once)
docker exec scion scion admin token create --lite --subject admin --group admins

scn login http://localhost:7080 --token scn_pat_…
scn repo create http://localhost:7080/mono
```

Until a policy is saved, the [default policy](auth.md#access-control-policy) applies: the `admins` group manages everything and anyone signed in can read, write and land.

The server refuses to start when it listens beyond loopback without authentication, since anyone who can reach it could then read and write every repository. To run anonymously anyway (for example, for a test on an internal network), add `--allow-anonymous` (`SCION_ALLOW_ANONYMOUS=true`):

```sh
docker run -d -p 7080:7080 -v scion-data:/data scion --log-format json server --listen 0.0.0.0:7080 \
  --lite --data-dir /data --allow-anonymous --repo mono      # anonymous, creating a repository at start
```

| Port | Use |
|---|---|
| 7080 | gRPC of `scion server` |
| 7090 | gRPC of `scion cache` |
| 9464 | Prometheus metrics of `--metrics-listen` |

Every option has an environment variable (`SCION_META`, `SCION_BLOBS`, `SCION_S3_*`, `SCION_AUTH_*`, `SCION_METRICS_LISTEN` and so on); see `scion server --help`.

## docker compose

Two compose files cover one machine. Both run PostgreSQL (the metadata database of the Standard tier), SeaweedFS (S3-compatible object storage that creates the bucket `scion-objects` at start), `scion server` and, with `--profile cache`, the regional cache `scion cache` on 7090.

| | [`deploy/prd`](../../deploy/prd/docker-compose.yml) | [`deploy/dev`](../../deploy/dev/docker-compose.yml) |
|---|---|---|
| For | Production on one machine | Development and trials |
| Authentication | Required: `SCION_AUTH_REQUIRED=true` | None: `SCION_ALLOW_ANONYMOUS=true` |
| Secrets | From `.env`; compose refuses to start without them | Fixed development secrets |
| Restarts | `restart: unless-stopped` | None |
| Ports | 7080, 9464 (and 7090, 9465 with the cache) | Also PostgreSQL 5432 and S3 8333, for `cargo run` and the tests |
| Default services | All | Only PostgreSQL and S3; the server needs `--profile server` |

### Production: `deploy/prd`

```sh
cd deploy/prd
printf 'SCION_DB_PASSWORD=%s\nSCION_S3_SECRET=%s\n' "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" > .env
chmod 600 .env
docker compose up -d                       # add --profile cache for the regional cache
docker compose exec scion scion admin token create --subject admin --group admins   # the first administrator

scn login http://SERVER:7080 scn_pat_…
scn repo create http://SERVER:7080/demo
```

- **Image:** `opencsghq/scion:latest`; pin a release with `SCION_IMAGE=opencsghq/scion:0.1.11` in `.env`, and add `REGISTRY=docker.m.daocloud.io` when Docker Hub is out of reach.
- **Sign-in and policies:** tokens work as they are. For browser login (`scn login` without a token), external issuers, organizations or a policy, mount a signing key and an `auth.toml` into the `scion` service and set `SCION_JWT_KEY_FILE` and `SCION_AUTH_CONFIG`; see [auth.md](auth.md). The files must be readable by the image's user (uid 10001).
- **Metrics:** 9464 needs no authentication; keep it off public networks.
- **Data:** in the volumes `pg-data` and `s3-data`; back up PostgreSQL with `docker compose exec postgres pg_dump -U scion scion`.
- **Beyond one machine:** use a real S3 or MinIO service ([object-storage.md](object-storage.md)) and several nodes with `--advertise-url` and `--cluster-secret` ([cluster.md](cluster.md)).

### Development and trials: `deploy/dev`

```sh
docker compose -f deploy/dev/docker-compose.yml up -d                    # PostgreSQL + S3 for cargo run and the tests
docker compose -f deploy/dev/docker-compose.yml --profile server up -d   # the server too (opencsghq/scion:latest)
# The server built from this checkout instead
docker build -t scion:dev . && SCION_IMAGE=scion:dev docker compose -f deploy/dev/docker-compose.yml --profile server up -d
# Through a registry mirror; with the regional cache as well (7090)
REGISTRY=docker.m.daocloud.io docker compose -f deploy/dev/docker-compose.yml --profile server --profile cache up -d

scn doctor http://localhost:7080
scn repo create http://localhost:7080/demo
scn clone http://localhost:7080/demo
curl -s localhost:9464/metrics | head
```

## Health checks

- **The standard gRPC health check:** `grpc.health.v1.Health` needs no authentication and suits Kubernetes' gRPC probes.
- **The check in compose:** the image's own `scn doctor --endpoint http://127.0.0.1:7080`, which completes a capability negotiation.
