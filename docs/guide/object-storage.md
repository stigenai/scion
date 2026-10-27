# Object storage: SeaweedFS, MinIO and other S3-compatible services

English | [简体中文](object-storage.zh-CN.md)

The Scion server keeps file contents, trees and commit objects in an object store (`--blobs`). The backends:

| `--blobs` | Use |
|---|---|
| `mem` | Tests; lost when the process exits |
| A directory path (or `--lite --data-dir`) | The single-node Lite tier |
| `s3://bucket[/prefix]` | Production: SeaweedFS, MinIO, AWS S3 or any other S3-compatible service |

Reads and writes use only S3's core subset (PUT, GET with Range, HEAD, LIST), without conditional writes or versioning. Every CI run passes the full storage conformance tests on SeaweedFS and MinIO.

## Compression

The server stores objects zstd-compressed (level 3) when that saves at least 10%. Content that is already compressed, such as model weights, archives or images, is stored as it is.

- **Transparent:** object ids, sizes in the metadata and everything clients see are those of the original bytes.
- **Old objects:** objects written before compression, or with compression turned off, read back unchanged, and a later upload of the same object keeps the existing copy.
- **Effect:** importing starhub-server (6,312 commits) stores 227 MB of object bytes instead of 1,007 MB; git's pack for the same history is 392 MB.
- **Turning it off:** `SCION_BLOB_COMPRESSION=off` stores new objects uncompressed, for example when CPU is scarce and storage is cheap. Reads handle both forms either way.

## Packs

Objects written together are stored together: a commit's new trees and its changeset, and each batch of an upload (up to 16 MB or 4,096 objects), go into one *pack* under `packs/` instead of one stored object each. The metadata store records where each packed object is (`pack_locations`), and reads fetch just its byte range.

- **Why:** most objects are small (the trees and commits of starhub-server are around 1 KB). On MinIO every stored object is a directory plus an `xl.meta` file of at least a disk block each, and every write is a request. Importing starhub-server stored 67,636 objects taking 546 MB of MinIO disk; with packs it stores 10,065 taking 282 MB, and the import takes about 95 s instead of 154 s.
- **Reads:** the first read from a pack also fetches the pack's index from its end (and the whole pack when it is under 64 KB), so the other objects of the same commit need no further lookups.
- **What stays loose:** objects over 1 MB, and an object written on its own. Objects written before packs keep being read where they are.
- **Cost:** a row per packed object in the metadata store (13 MB of PostgreSQL for starhub-server), and range reads (`GET` with `Range`), which every S3-compatible service supports.
- **Redaction:** purging a packed object rewrites its pack without it.
- **Turning it off:** `SCION_BLOB_PACKS=off` writes every new object loose. Packed objects stay readable.

## Repacking

`scion admin repack` rewrites all reachable objects into a few large packs, storing older versions of each file and directory as deltas against the next newer version, then removes the packs and loose objects this superseded. It is Scion's `git gc`, and `scion server` runs it on its own (below); it is safe while the server runs.

```sh
scion admin repack --meta postgres://… --blobs s3://scion-objects   # --dry-run to only count
```

- **On its own:** every `--repack-check-interval` (15 minutes) the server adds up what was written to the object store since the last repack (packs and loose objects newer than the marker each repack leaves at `repack/last`), and repacks:
  - when that is at least `--repack-after` (256 MiB), as after a large import;
  - when `--repack-every` (24 hours) has passed since the last repack and at least 1 MiB was written.

  These repacks are incremental: they pack only what was written since the last repack (found from the packs and loose objects newer than it, leaving out the packs it wrote), pairing versions among the new objects, so their cost follows the writes, not the size of the store. Once `--repack-full-every` (7 days) has passed since the last full repack, the next one is full: it rewrites everything reachable, which also stores the older versions of files as deltas against the newer ones and compacts the metadata. `0s` makes every repack full.

  With several nodes, one repacks at a time (a lease in the metadata store). `--repack-after 0 --repack-every 0s` turns it off. The log shows why it ran, which kind it was, and what it did.
- **By hand:** `scion admin repack --incremental` makes an incremental repack; without it, a full one.
- **Deltas:** an older version is stored as zstd with the newer version as a reference (what `zstd --patch-from` does). The newest versions, which are read most, stay whole. Chains are at most 50 long, as in git; reads follow them and cache the bases they use. A delta names a base in the same pack by its offset and length (a few bytes, like git's `OFS_DELTA`) and any other base by its id; packs with deltas named this way need a server at least as new as the repack that wrote them.
- **Effect** on starhub-server (6,338 commits, PostgreSQL + MinIO): 67,847 objects in 1 pack of 27 MB, 51,262 of them deltas, in 24 s. MinIO goes from 283 MB to 86 MB (the rest is three committed binaries over 8 MB, which stay loose); Gitaly's repository for the same history is 82 MB.
- **Removing what it superseded:** packs none of whose objects live there any more, once older than `--prune-after` (default 1 hour, so that packs being written are not touched), and loose objects that are also packed.
- **Reads** fetch large packs in 1 MiB blocks and keep 128 MiB of them, so a commit's objects and the versions of a delta chain mostly come from memory.
- **Metadata:** the repack rewrites every pack location. On PostgreSQL it then compacts the metadata. It rewrites the pack locations table in place with `VACUUM FULL` when the table is under 256 MB, which locks it for about a second at most and is skipped if the lock is not granted within 200 ms; larger tables are only vacuumed, so later writes reuse their space. It also rebuilds the indexes over object ids and paths with `REINDEX CONCURRENTLY`, which does not block reads or writes. For starhub-server this takes the database from 68 MB to 54 MB.
- **Redaction:** purging an object that others are stored against first rewrites those others whole.

## Connection settings

| Environment variable | What | Default |
|---|---|---|
| `SCION_S3_ENDPOINT` | The service's address, such as `http://minio:9000`; empty for AWS S3 | none |
| `SCION_S3_PROVIDER` | `auto`, `minio`, `seaweedfs`, `aws` or `generic` | `auto` |
| `SCION_S3_ACCESS_KEY_ID` / `SCION_S3_SECRET_ACCESS_KEY` | Access keys; `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` when unset | none |
| `SCION_S3_REGION` | The region; `AWS_REGION` when unset | `us-east-1` |
| `SCION_S3_VIRTUAL_HOSTED` | `true` for `bucket.host` style | by service; `true` only for AWS |
| `SCION_S3_MAX_RETRIES` | Retries per request at most | 10 |

The `/prefix` after `s3://` lets several deployments share one bucket.

## Adapting to the service

At start, the server checks the object store and refuses to start, saying why, if any step fails:

1. **Identify the service** from the `Server` header of the endpoint's root: `MinIO` is MinIO, `SeaweedFS …` is SeaweedFS and `AmazonS3` is AWS.
   - With `SCION_S3_PROVIDER=auto`, the result is used as it is.
   - An explicitly configured service that differs from the result is an error, most likely a wrong endpoint.
   - Behind a reverse proxy that hides the header, the configuration wins.
2. **Health check:** `/minio/health/ready` for MinIO, `/healthz` for SeaweedFS. When the service is not ready yet, the error includes the status it returned.
3. **Bucket check:** when the bucket does not exist, the error says how to create it on that service.
4. **Read-write self-test:** writes, reads back and checks a probe object, to catch wrong keys, missing permissions and the like.

The startup log records what was identified, for example:

```
object store ready backend=s3 (minio, Server: MinIO) bucket=scion-objects
```

| | SeaweedFS | MinIO |
|---|---|---|
| Identified by (`Server` header) | `SeaweedFS <version>` | `MinIO` |
| Health check | `/healthz` | `/minio/health/ready` |
| Creating a bucket | `echo 's3.bucket.create -name B' \| weed shell` | `mc mb <alias>/B`, or the container environment variable `MINIO_DEFAULT_BUCKETS=B` |
| Request style | path | path |
| Tested version | 4.48 | 2023.12.23 (the bitnami image, as in starhub and CSGHub deployments) |

## SeaweedFS

The development environment uses SeaweedFS by default: MinIO's open-source edition is archived and no longer maintained, while SeaweedFS is a maintained Apache 2.0 project.

```sh
docker compose -f deploy/dev/docker-compose.yml up -d s3 s3-init
SCION_S3_ENDPOINT=http://localhost:8333 \
SCION_S3_ACCESS_KEY_ID=scion SCION_S3_SECRET_ACCESS_KEY=scion-dev-secret \
SCION_S3_PROVIDER=seaweedfs \
scion server --meta postgres://scion:scion-dev-secret@localhost:5432/scion --blobs s3://scion-objects
```

## MinIO

With starhub or CSGHub, Scion can use their existing MinIO:

```sh
docker compose -f deploy/dev/docker-compose.yml --profile minio up -d minio
SCION_S3_ENDPOINT=http://localhost:9000 \
SCION_S3_ACCESS_KEY_ID=scion SCION_S3_SECRET_ACCESS_KEY=scion-dev-secret \
SCION_S3_PROVIDER=minio \
scion server --meta postgres://scion:scion-dev-secret@localhost:5432/scion --blobs s3://scion-objects/scion
```

When sharing MinIO with other services, give Scion its own bucket or prefix, and grant it read and write access to that only.

## Proxies

Requests follow the system proxy settings (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`). When the object store is on an internal network or this machine, add its address to `NO_PROXY`, for example `NO_PROXY=127.0.0.1,localhost,minio`.

## Tests

The object storage tests point at a real service through environment variables, and are skipped when they are unset:

```sh
SCION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 SCION_TEST_S3_BUCKET=scion-test \
SCION_TEST_S3_ACCESS_KEY=scion SCION_TEST_S3_SECRET_KEY=scion-dev-secret \
SCION_TEST_S3_PROVIDER=minio \
cargo test -p scion-store-blob -p scion
```

`SCION_TEST_S3_PROVIDER` checks what was identified and may be left unset.
