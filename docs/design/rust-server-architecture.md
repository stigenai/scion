# Scion server architecture (Rust)

English | [简体中文](rust-server-architecture.zh-CN.md)

> Version: Draft v0.1 · 2026-10-16
> Read first: [`scion-research.md`](./scion-research.md) (Scion's background, the original article's key points and the overall plan)
> Reference implementation: CarterT27/scion (Apache 2.0). **Ideas only, no copied code** (decided 2026-10-16): all code in this project is written here.

---

## 0. Design goals

| # | Goal | Measure |
|---|---|---|
| G1 | **A language-neutral protocol**, with only a default Rust client from the project | The default client `scn` depends only on the public protocol (see §0 of [`rust-client-architecture.md`](./rust-client-architecture.md)); clone/commit/land need no canonical encoding, so clients in any language can be written from the protocol later |
| G2 | High availability | A single-AZ failure goes unnoticed; RPO=0; metadata RTO at most 60 seconds; stateless layers recover in seconds |
| G3 | High concurrency | 200+ lands per second on one ref; 200,000+ object reads per second per L2 cache node; 100,000 lands and 30 million checkouts a day |
| G4 | Commit cost independent of repository size | Creating a changeset costs O(changed paths × directory depth) |
| G5 | Simple deployment | Lite mode in one process with no dependencies; `docker compose up` for the full Standard stack; Helm in production; one binary for every service, with roles chosen by subcommand; **no commercially licensed components** |

Non-goals (v1): git protocol compatibility, offline and multi-master writes, a code review UI, CI scheduling.

**Architecture diagrams** (PNGs to view, SVGs as the editable sources):

![Scion standalone architecture](./images/arch-01-standalone.en.png)

![The three deployment tiers](./images/arch-02-deployment-tiers.png)

External integrations are in [`starhub-integration.md`](./starhub-integration.md) (figures 3 and 4).

---

## 1. Two core principles

### 1.1 The protocol is the product
- `proto/` plus `SPEC.md` plus the conformance suite **are the system's real interface**. The Rust server is one implementation of it.
- The protos are managed with **buf**: every PR runs `buf lint` and `buf breaking`, and **breaking changes are forbidden**; new semantics get new fields or a `v2` package.
- The project maintains only **the Rust SDK and CLI** (`scion-client` / `scn`). The protos are published as a buf module, so users in other languages can generate their own stubs.

### 1.2 Thin clients
Everything hard to get right lives on the server; a client needs to do only three things:

| A client must | Difficulty |
|---|---|
| Compute blob ids = `BLAKE3(raw file bytes)` | Every language has a BLAKE3 library: one line |
| Decompress zstd (the server can also return uncompressed data by negotiation, §3.4) | Every language has a library |
| Call gRPC | Generated from the protos |

**Clients do not need to:** encode trees or changesets, compute tree hashes, rebase or merge, or find merge bases. When the server returns a tree, it gives **both** the canonical bytes (for clients that want to verify) and the decoded proto structure (for ordinary clients to use as it is).

---

## 2. System overview

```
            ┌──────────── Clients ────────────────┐
            │ scn CLI / scn daemon / Rust SDK    │
            │ agents / CI / web (gRPC-Web, JSON) │
            └───────────────┬────────────────────┘
                            │ gRPC (HTTP/2) | gRPC-Web | REST/JSON
                            │ TLS + bearer token
┌───────────────────────────▼───────────────────────────────┐
│ scion cache   (L2, regional)   the same ObjectService       │
│   foyer (memory + NVMe) · rendezvous sharding · singleflight · hedging │
└───────────────────────────┬───────────────────────────────┘
                            │ internal gRPC + mTLS
┌───────────────────────────▼───────────────────────────────┐
│ scion server  (L3, authoritative, stateless)               │
│  Gateway: tonic + tonic-web + axum (REST)                   │
│  Middleware: authn · authz · rate limits · concurrency cap · deadlines · idempotency · tracing │
│  Services: Object · Write · Land · Ref · Query · Auth · Admin │
│  Engines: TreeBuilder · Merge · LandCoordinator · Derive    │
├────────────────────────────────────────────────────────────┤
│ scion deriver  (background: derived data following cs_log) │
│ scion janitor  (background: orphan cleanup, redaction purges, snapshots) │
├────────────────────────────────────────────────────────────┤
│ MetaStore in three tiers: SQLite (Lite) | PostgreSQL (Standard, default) │
│                 | YugabyteDB (Distributed) — the same sqlx code │
│ BlobStore: S3-compatible (AWS/MinIO/Ceph/OSS…) · GCS · Azure · local FS │
│ EventBus : NATS JetStream (ref changes, land events)        │
└────────────────────────────────────────────────────────────┘
```

**One binary:** `scion server | cache | deriver | janitor | import | admin`. One image starts in each role, for the smallest operational surface.

**Lite mode:** `scion server --lite --data-dir ~/.scion` runs server, deriver and janitor **in one process**, with SQLite metadata and objects on the local file system. For development, single machines, edge devices and embedding in local products such as CSGLite.

---

## 3. The protocol (a cross-language contract)

### 3.1 Transports
| Transport | Use | Implementation |
|---|---|---|
| **gRPC / HTTP2** | The main protocol, used by every SDK and the daemon | `tonic` |
| **gRPC-Web** | Browsers | `tonic-web` |
| **REST/JSON** | Scripts and curl: a common subset of reading files, log, land, refs | `axum`, with serde generated by `pbjson` for proto↔JSON, fields as in the proto JSON mapping |

Every streaming RPC must be **resumable**: requests carry an offset or cursor, so a broken stream continues where it stopped.

### 3.2 Services (package `scion.v1`)

```proto
service ServerInfoService {
  rpc GetServerInfo(GetServerInfoRequest) returns (ServerInfo); // protocol version, features, limits, upstream topology hints
}

service ObjectService {           // implemented by L1, L2 and L3; the only cacheable service
  rpc GetObjects(GetObjectsRequest) returns (stream ObjectChunk);   // by id in batches; large objects streamed in chunks
  rpc GetTrees(GetTreesRequest)     returns (stream TreeChunk);     // recursive from a root, with depth and path prefix filters
  rpc HasObjects(HasObjectsRequest) returns (HasObjectsResponse);   // FindMissing
}

service WriteService {
  rpc UploadBlobs(stream UploadBlobChunk) returns (UploadBlobsResponse); // content-addressed, so idempotent
  rpc CreateChangeset(CreateChangesetRequest) returns (CreateChangesetResponse);
  rpc Rebase(RebaseRequest) returns (RebaseResponse);
  rpc Merge(MergeRequest)   returns (MergeResponse);
}

service LandService {
  rpc Land(LandRequest) returns (LandResponse);                  // server-side rebase + CAS (possibly batched)
  rpc GetLandStatus(GetLandStatusRequest) returns (LandStatus);  // look a result up by request_id after a timeout
}

service RefService {
  rpc ResolveRef(ResolveRefRequest) returns (ResolveRefResponse);     // never cached
  rpc ListRefs / CreateRef / UpdateRef / DeleteRef / GetRefLog
  rpc WatchRefs(WatchRefsRequest) returns (stream RefEvent);          // daemon prefetching, CI triggers
}

service QueryService {            // "source control as an API": no local repository needed
  rpc ReadFile(ReadFileRequest) returns (stream ReadFileChunk);   // rev:path
  rpc ListDir / Stat / Log / Blame / Diff(stream) / IsAncestor / MergeBase / ResolveRevision
  rpc GetDerived(GetDerivedRequest) returns (GetDerivedResponse);  // generic derived data queries
}

service AuthService  { rpc StartDeviceLogin / PollDeviceLogin / WhoAmI / CheckAccess }
service AdminService { rpc Redact / Unredact / Purge / ListRedactions / ReloadPolicy }
```

**Every request names its `repo`:** several repositories from v1, unlike the reference implementation's single repository.

### 3.3 Capability negotiation: `GetServerInfo`
```proto
message ServerInfo {
  string protocol_version = 1;          // "1.3"
  repeated string features = 2;         // "land.batch", "objects.uncompressed", "query.blame", ...
  Limits limits = 3;                    // max_batch_objects, max_inline_bytes, chunk_size, max_changes_per_cs ...
  repeated Endpoint read_endpoints = 4; // regional cache addresses to use (clients may decline)
  HashAlgo hash = 5;                    // BLAKE3_256
}
```
The rule: clients **depend only on capabilities declared in `features`**; new server capabilities only add feature flags and never change existing semantics. Old and new `scn` versions then mix with old and new servers, and rolling upgrades need no lockstep between client and server.

### 3.4 Object transfer format
```proto
message ObjectChunk {
  bytes id = 1;              // 34 bytes: algo(1) | kind(1) | digest(32)
  uint64 offset = 2;         // chunks of large objects
  bytes data = 3;
  Encoding encoding = 4;     // ZSTD | IDENTITY  (negotiated with accept_encodings in the request)
  bool last = 5;
  uint64 total_size = 6;
}
message TreeChunk {
  bytes id = 1;
  bytes canonical = 2;       // canonical bytes, optional (request want_canonical=true)
  repeated TreeEntry entries = 3;  // the decoded structure, all ordinary clients use
}
```

### 3.5 The error model
`google.rpc.Status` with `ErrorInfo{reason, domain="scion.dev", metadata}`. Retry semantics are part of the specification; `scion-client` implements them strictly, and clients in other languages should too:

| reason | gRPC code | Retryable | Details |
|---|---|---|---|
| `MERGE_CONFLICT` | ABORTED | No (a person or agent must step in) | Conflicting paths and the three blob ids |
| `LAND_CONTENTION` | UNAVAILABLE | Yes | `RetryInfo` |
| `ALREADY_LANDED` | ALREADY_EXISTS | — | The landed changeset |
| `REF_VERSION_MISMATCH` | FAILED_PRECONDITION | After reading again | The current version |
| `OBJECT_MISSING` | FAILED_PRECONDITION | After uploading | The missing ids |
| `REDACTED` | NOT_FOUND | No | The redaction id |
| `RATE_LIMITED` | RESOURCE_EXHAUSTED | Yes | `RetryInfo` |

### 3.6 Idempotency and paging
- **Every write request** carries a `request_id` (a client-generated UUIDv7). The server keeps it in the `idempotency` table for 24 hours: the same id returns the first result, and different parameters are `INVALID_ARGUMENT`.
- Listings use `page_size` and an opaque `page_token`; very large results stream instead.

### 3.7 Authentication
- Always in the gRPC metadata `authorization: Bearer <token>`.
- Token sources: OIDC (an enterprise IdP or the GitHub device flow), service principals (agents and CI, exchanging workload identities for short-lived tokens), PATs (development only).
- The server issues its own short-lived JWTs (15 minutes, ES256), which cache layers **verify without database lookups**.
- Services talk to each other over mTLS (rustls; SPIFFE optional).

---

## 4. Data model

### 4.1 Object ids
`ObjectId = algo(u8) | kind(u8) | digest[32]`, in text as `b1…`/`t1…`/`c1…`/`l1…` (the letter is the kind, the digit the hash algorithm, followed by 64 lowercase hex digits).
- Blob: digest = BLAKE3(raw bytes)
- Tree/Changeset: digest = BLAKE3(canonical encoding)

### 4.2 Canonical encoding
**Decided: a custom varint format** (2026-10-16). A minimal binary format of LEB128 varints, length prefixes and strict ordering, specified in [`SPEC.md`](../../SPEC.md) at the repository root, with **golden test vectors** (`testdata/vectors/*.json`: input structures → encoded bytes → ids, and invalid inputs → expected errors). The format is our own, with no attempt at interoperating with the reference implementation. Only clients that verify on their own implement it; the others can ignore it.

### 4.3 Changesets and trees (a key decision)
```
Changeset { parents, author, committer, time, message, change_id, delta: [FileChange], extensions }
```
- **A changeset's id covers only the delta**, not root_tree, as in Anthropic's article.
- root_tree is **mandatory, synchronous derived data**: `CreateChangeset` builds the tree in the same request and writes it to the `cs_tree` table before returning. Building it costs O(k × depth), still proportional to the change.

This gets both benefits: a returned changeset can be checked out at once, as conveniently as in the reference implementation, and a future change of tree format (adding fsnode statistics, say) only rederives trees, **leaving changeset ids alone**.

### 4.4 Metadata storage: three tiers

| Tier | Database | Object storage | High availability | Use | License |
|---|---|---|---|---|---|
| **Lite** | **SQLite** (WAL mode, sqlx's built-in driver) | Local file system | None (single node) | Development, single machines, edge devices, embedding in local products such as CSGLite | Public Domain |
| **Standard** (default) | **PostgreSQL 15+**, with Patroni for high availability | MinIO / S3-compatible | Automatic failover; RPO=0 with synchronous replication | Ordinary private deployments, on the same operations stack as starhub (PG 15) | PostgreSQL License |
| **Distributed** | **YugabyteDB** (YSQL, PG wire-compatible) | MinIO / S3 / cloud OSS | Across 3 AZs, Raft, RPO=0 | Several availability zones, large scale, heavy writes | Apache 2.0 |

> Not CockroachDB: from v24.3 (2024-11) the free Core edition is gone. Companies with over US$10 million of annual revenue, and government users, must buy a commercial license, and telemetry must be reported, which conflicts with private and air-gapped deployments.

**One codebase, dialects abstracted:** `scion-store-sql` is built on sqlx; the three tiers share the schema and almost all SQL, and the differences live in a `Dialect` trait:

| Difference | SQLite | PostgreSQL | YugabyteDB |
|---|---|---|---|
| Ref CAS | `BEGIN IMMEDIATE` + `UPDATE … WHERE version=?` | `UPDATE … WHERE version=? RETURNING` | As PostgreSQL |
| Retrying transaction conflicts | `SQLITE_BUSY` (with busy_timeout) | `40001`, `40P01` | `40001` (explicit retries needed), `40P01` |
| Isolation | Serial by nature (one writer) | SERIALIZABLE for key transactions; ref CAS as a single-row conditional update, independent of isolation | As PostgreSQL |
| `cs_log` sequence numbers | `max+1` under one writer | A counter row per repository, incremented in the transaction | As PostgreSQL; or a sequence with a cache |
| Spreading hot spots | Not needed | B-trees are enough | Tables and indexes sharded by `HASH(repo_id, …)` |
| Read replicas | Not needed | Immutable data from standbys (Patroni replicas) | Follower reads (`yb_read_from_followers`, with bounded staleness) |
| Bulk writes | Multi-row INSERT | `UNNEST` bulk INSERT | As PostgreSQL |

**Moving between tiers:** `scion admin migrate-meta --from sqlite://… --to postgres://…`, and likewise from PG to YugabyteDB. Objects are content-addressed, so a tier change **reuses the object data as it is** (`scion admin blob-sync` moves a local file system to S3) and migrates only metadata. Writes freeze briefly during the migration, and `fsck` checks the result.

**Conformance tests:** the conformance suite of `scion-store` **runs against all three databases**: PostgreSQL and SQLite on every PR, YugabyteDB nightly.

### 4.4.1 Metadata tables
| Table | Primary key | What |
|---|---|---|
| `repos` | repo_id | Configuration, limits |
| `objects_index` | (repo_id, id) | Which objects a repository can see: size, redaction flag. Object bytes are stored once globally; this table decides visibility |
| `objects_inline` | (repo_id, id) | Objects under 4 KiB stored inline (most trees are small) |
| `changesets` | (repo_id, cs_id) | Canonical bytes, gen, first_parent |
| `cs_log` | (repo_id, seq) | WAL with monotonically increasing sequence numbers (a sequence per repository) |
| `cs_tree` | (repo_id, cs_id) | The root tree id |
| `refs` | (repo_id, name) | target, version; **the only mutable state** |
| `ref_log` | (repo_id, name, version) | Audit records |
| `landed` | (repo_id, ref, change_id) | Keeps a change from landing twice (M3) |
| `derived` | (repo_id, kind, kind_version, cs_id) | Derived values or pointers |
| `idempotency` | (repo_id, request_id) | With a TTL (M3, 24 hours by default) |
| `leases` | (scope) | Leases of land leaders and deriver shards (land leaders in M3) |

Avoiding hot spots: in the Distributed tier, tables shard by `HASH(repo_id)`, so one repository's writes do not pile onto one tablet; `cs_log` numbering is as in the table above. The Standard and Lite tiers need no hot-spot handling.

### 4.5 Blob storage layout
`s3://bucket/{prefix}objects/{kind}/{hh}/{hex}`. **Object bytes are stored once globally**, not per repository: the same content (such as the chunks quantized and fine-tuned versions of one model share) is stored once across all repositories. What a repository can read is decided by `objects_index`; `HasObjects` and `GetObjects` consult the index first, so one repository cannot probe which objects another has. M1 stores raw bytes; zstd compression comes in a later milestone (told apart by key suffix, with old objects readable as before).

The write ordering invariant: **blobs persisted → changeset and cs_tree persisted → ref CAS**. Reads never see a dangling reference.

Large files (over 8 MiB) are chunked with FastCDC, and the blob becomes a `ChunkList` object (kind=0x05) that the server reassembles transparently on reads, so it is **invisible to clients**.

### 4.6 Object storage compatibility (MinIO / S3-compatible storage)

**Goal:** one codebase on AWS S3, **MinIO**, Ceph RGW, SeaweedFS and S3-compatible services such as Alibaba Cloud OSS, Huawei OBS and Tencent COS; starhub's existing MinIO reusable as it is.

**Principle: only S3's core API subset**
| Used | Not relied on |
|---|---|
| PutObject, GetObject (with Range), HeadObject, DeleteObject(s), ListObjectsV2, multipart uploads (Create/UploadPart/Complete/Abort/ListParts), CopyObject/UploadPartCopy, presigned GET/PUT (SigV4) | S3 Select, Object Lambda, event notifications, Object Lock (optional), **conditional writes (If-None-Match)**, bucket policies, vendor-specific headers |

**Why conditional writes are not needed:** objects are content-addressed, so one key always has the same content, repeated writes are idempotent, and concurrent writes of one object cannot go wrong. The only mutable state, refs, lives in the MetaStore, not in object storage.

**Settings** (`[blobstore.s3]`, on the `object_store` crate's `AmazonS3Builder`):
```toml
[blobstore.s3]
endpoint          = "http://minio:9000"     # empty for AWS
region            = "us-east-1"             # anything for MinIO
bucket            = "scion-objects"
prefix            = "prod/"                 # lets other services share the bucket
addressing        = "path"                  # path for MinIO/Ceph; virtual-hosted for Alibaba Cloud OSS and others
allow_http        = true                    # internal networks only
access_key_id     = "${S3_ACCESS_KEY}"      # or IRSA, instance roles, STS temporary credentials
secret_access_key = "${S3_SECRET_KEY}"
ca_bundle         = "/etc/scion/ca.pem"     # private CA
checksum          = "when_required"         # no new-style CRC headers by default (some compatible stores reject them)
multipart_part_size = "16MiB"
max_concurrency   = 256
request_timeout   = "30s"
sse               = "none"                  # none | sse-s3 | sse-kms(key_id) | sse-c
storage_class     = ""                      # optional
```

**Consistency requirement:** new objects must be readable at once (read-after-write). AWS S3 (since 2020), MinIO and Ceph RGW all are. The write ordering invariant of §4.5 depends on it, hence the self-test at start.

**Self-test at start:** `scion admin blobstore-check`, which the server also runs at start. It checks GET and HEAD right after PUT, Range reads, multipart uploads over 5 GiB and UploadPartCopy (optional), presigned URL access, the batch delete limit, ListObjectsV2 paging and clock skew (SigV4 is time-sensitive). The results become feature flags: without UploadPartCopy, for example, it falls back to reading and writing again.

> **Implementation note** (2026-10-17): the service adaptation layer is `server/store-blob/src/provider.rs`. `SCION_S3_PROVIDER` is `auto` (default), `minio`, `seaweedfs`, `aws` or `generic`. The checks at start, in order: identify the service by its `Server` header (an explicit setting that conflicts with the result is an error), the service's health endpoint (MinIO `/minio/health/ready`, SeaweedFS `/healthz`), whether the bucket exists (if not, how to create it on that service), and a read-write self-test. The request style defaults by service, virtual-hosted only for AWS. CI runs the full storage conformance tests on SeaweedFS 4.48 and on MinIO 2023.12.23 (the bitnami image, as in starhub's deployment). Usage is in [`docs/guide/object-storage.md`](../guide/object-storage.md).

**Downloads straight from object storage (sparing server bandwidth):**
- For blobs over a threshold (64 MiB by default), `GetObjects` can return `redirect{presigned_url, expires}` instead of a data stream, when the request declares `accept_redirect` and the server has the `objects.presigned` feature on.
- Clients download from MinIO or S3 directly and **still check BLAKE3 afterwards**.
- Sensitive objects (those with `cacheable=false`) never go this way.

**Uploads straight to object storage:**
1. For a large blob, `BeginUpload` returns a staging key `staging/<upload_id>` and a set of presigned part PUT URLs.
2. The client uploads straight to object storage.
3. The client calls `CommitUpload(expected_id)`. The server streams the data back and checks its hash, then CopyObjects it to its CAS key and deletes the staging data.
4. When the check fails, the staging data is deleted and an error returned. The janitor cleans up staging data uncommitted after 24 hours.

**Key layout:** `{prefix}objects/{kind}/{hh}/{hex}`. A hash prefix spreads objects evenly over MinIO's erasure sets and S3's partitions; `staging/` and `snapshots/` are separate prefixes, so lifecycle rules can be set on them alone.

**Compatibility test matrix (CI):**
| Backend | How | How often |
|---|---|---|
| SeaweedFS (pinned version, signature auth on) | `docker run` in CI, as in the local development environment `deploy/dev` | Every PR |
| Local file system (`object_store::local`) | Unit tests and the Lite tier | Every PR |
| Ceph RGW, RustFS, older MinIO versions | Containers | Nightly |
| AWS S3, Alibaba Cloud OSS, Huawei OBS | Real accounts with dedicated test buckets | Weekly and before releases |

> **MinIO's open-source edition is no longer maintained** (checked 2026-10): the official download site returns `410 Gone` for every community build, meaning the MinIO Server, mc and KES open-source projects are archived, with no more security updates and no handling of vulnerability reports. So:
> - the development environment and PR-level CI use **SeaweedFS** (Apache 2.0, still maintained) as the default S3-compatible store;
> - we **rely only on standard S3 semantics and no MinIO-specific APIs** (such as `mc admin` or MinIO's event notifications), so starhub's existing MinIO still works as it is, and moving to SeaweedFS, Ceph, RustFS or a cloud provider later needs no code changes;
> - new deployments should prefer an S3-compatible store that is still maintained, and existing MinIO deployments should plan a migration.

---

## 5. Server internals (Rust)

### 5.1 Workspace layout and layering

Top-level directories follow **roles**, so it is clear at a glance which code is the server's, which the client's and which both share. Directory names say where and what; crate names all carry the `scion-` prefix (avoiding clashes with crates.io libraries such as `object`).

```
proto/scion/v1/        protocol definitions (.proto), language-neutral
SPEC.md                object model and canonical encoding; golden vectors in testdata/vectors/

common/                shared by server and client; may depend only on common
  protocol/            scion-proto        tonic/prost generated code, no business logic
  object/              scion-object       ObjectId, canonical encoding, Tree/Changeset, validation (no IO)

server/                the server; may depend on common and lower server crates
  store/               scion-store        traits MetaStore / BlobStore + in-memory implementation + backend conformance suite
  store-sql/           scion-store-sql    sqlx: SQLite / PostgreSQL / YugabyteDB tiers + Dialect
  store-blob/          scion-store-blob   object_store: S3-compatible storage / local file system
  merge/               scion-merge        diff3, three-way merges, conflict descriptions (pure computation)
  tree/                scion-tree         incremental TreeBuilder
  derive/              scion-derive       the derivation framework + derivers
  land/                scion-land         LandCoordinator (batching leader), the rebase pipeline
  query/               scion-query        log / blame / diff / merge-base
  authz/  auth/        scion-authz / scion-auth   policy engine; OIDC / JWT / device flow
  cache/               scion-cache        tiered cache core: foyer + singleflight + rendezvous
  obs/                 scion-obs          tracing / OTel / metrics
  api/                 scion-api          tonic services + axum REST + tower middleware
  scion/               scion              the single server binary, roles by subcommand

client/                the client; may depend only on common and client, never server
  sdk/                 scion-client       Rust SDK
  wc/                  scion-wc           working copy
  daemon/              scion-daemon       node L1 cache
  vfs/                 scion-vfs          virtualized checkouts (after M7)
  gitimport/           scion-gitimport    git history import (public protocol only, scn import git)
  cli/                 scion-cli          the scn binary

testing/               scion-testkit (fake backends, fault injection, git differential checks, lincheck)
                       scion-conformance (black-box protocol conformance suite, pointable at any server)
xtask/                 repository maintenance: cargo xtask layers (dependency direction checks)
```
The client side is designed in [`rust-client-architecture.md`](./rust-client-architecture.md). Directories appear milestone by milestone; M0 has `common/protocol`, `common/object`, `server/store`, `server/api`, `server/scion`, `client/sdk` and `client/cli`.

**Layering rules** (checked in CI by `cargo xtask layers`, on normal dependencies only; dev-dependencies are free):
- Zone rules: `common` depends only on `common`; `client` only on `common` and `client`; `server` only on `common` and `server`. A crate's zone is its top-level directory.
- Inside the server, by layer: `store` ← `store-sql/store-blob` ← `tree` ← `merge/derive/query` ← `land` ← `api` ← `scion`, each depending only on lower layers; a new server crate must register its layer in xtask or the check fails. `authz` applies only at the `api` layer.

### 5.2 Runtime and key dependencies
| Use | Crates |
|---|---|
| Async runtime | `tokio` |
| gRPC | `tonic`, `prost`, `tonic-web`, `tonic-health`, `tonic-reflection` |
| REST | `axum`, `pbjson` |
| Middleware | `tower` (timeout, concurrency limit, load shed), `governor` (token buckets) |
| SQL | `sqlx` (the sqlite and postgres drivers; YugabyteDB through the postgres driver; transaction conflict retries handled uniformly by Dialect) |
| Object storage | `object_store` |
| Caches | `foyer` (hybrid memory + disk, for L2), `moka` (small in-process caches) |
| Hashing / compression / chunking | `blake3`, `zstd`, `fastcdc` |
| TLS/JWT | `rustls`, `jsonwebtoken` |
| Observability | `tracing`, `opentelemetry-otlp`, `metrics` + a Prometheus exporter |
| Tests | `proptest`, `insta`, `testcontainers`, `loom` (concurrency primitives) |

### 5.3 The request pipeline (tower middleware, outside in)
```
TLS → request id and trace context → authentication (JWT verification) → per-tenant/repository rate limits (governor)
    → global concurrency cap + load shedding → deadline propagation (the smaller of the client's and the cap)
    → idempotency check (writes) → authorization (repository/path/ref) → handler
```

---

## 6. The write path

### 6.1 CreateChangeset
```
1. Validate: parents exist; every blob in the delta exists (otherwise OBJECT_MISSING with the missing list); paths are valid; write permission; limits
2. Read the parent's root tree → TreeBuilder rewrites only the nodes on the delta's paths (O(k·depth))
3. New tree nodes: under 4K into objects_inline, otherwise to S3
4. In one SQL transaction: changesets + cs_tree + cs_log
5. Return {cs_id, root_tree}
```
**A simpler entry for clients:** `CreateChangeset` also accepts `inline_content` (file contents under 1 MiB), hashed and stored by the server, so an agent changing a few files commits in one RPC.

### 6.2 Land and the LandCoordinator (the core of high concurrency)
A single-row CAS is correct on its own, but on a hot ref it causes **retry storms**, and every retry rebases again. The answer is **a leader per ref, with batching**:

```
Land request → any server → look up the leader of (repo, ref) in the leases table
        → enqueue locally if this node is the leader, otherwise forward to the leader over internal gRPC

LandCoordinator (one tokio task per hot ref):
  keeps head in memory (updated after each successful CAS; reread from the database when the lease is taken)
  loop:
    batch = collect requests until (5ms or 64, whichever first)
    onto = head
    for req in batch:                    # rebase in order; conflicts fail individually, not the whole batch
      r = rebase(req.stack, onto)        # conflict detection looks only at paths shared by base..onto and this delta
      ok  → onto = r.tip; accepted += r
      err → answer that request MERGE_CONFLICT
    one transaction: CAS(ref, expect=head_version, new=onto) + ref_log + landed rows
    success → head = onto; answer each
    failure (wrong version: the lease was taken or something wrote from outside) → reread head, redo the batch
```

- **Conflicts within a batch:** when request i conflicts with requests accepted before it, only it fails.
- **Leases:** TTL 5 seconds, renewed every 2; the CAS checks the version, which is the fencing token, so even a leader whose lease expired cannot write wrongly.
- **Cold refs** (rarely landed release branches and the like) use CAS with jittered retries, without a leader. Whether a ref gets a leader is decided automatically by its QPS.
- **Client timeouts:** retry with the same `request_id`, or call `GetLandStatus`; nothing lands twice.
- Expected performance: a CAS round trip is about 10 ms and a batch averages 20 to 50 requests, so **one ref takes hundreds to thousands of lands per second**.

### 6.3 Rebase and conflict detection
- Candidate conflict paths = `paths(delta)` ∩ `changed_paths(base..onto)`. The latter comes quickly from the derived data `path_history` and the changeset skip list, **without walking the whole tree**.
- An empty intersection: apply the delta to onto directly (the most common case).
- A non-empty one: a three-way merge per path. Text files with diff3; binary files, mode changes and modify/delete conflicts are reported as conflicts.
- Conflict details return the base, ours and theirs blob ids, so agents can resolve them automatically and submit again.

---

## 7. The read path and cache layers

### 7.1 Three layers, one interface
`ObjectService` is served by the daemon (L1, client side, implementable in any language), `scion cache` (L2) and `scion server` (L3), **with identical messages**. Objects are content-addressed and immutable, so every layer **caches them forever without invalidation**. Refs are never cached.

### 7.2 The L2 regional cache, `scion cache`
- **Storage:** a foyer hybrid cache, with a memory tier of gigabytes to tens of gigabytes and an NVMe tier of hundreds of gigabytes to terabytes.
- **Sharding:** rendezvous hashing over the cache nodes, routing by object id. A node asked for an id it does not own forwards one hop to its owner before deciding to fetch upstream, so not every node fetches from upstream.
- **Merged fetches:** singleflight merges concurrent misses of one id; recursive `GetTrees` requests become batched fetches of many ids.
- **Hedged requests:** when an upstream request has not returned after the p95 latency, the same request goes to another upstream replica.
- **Fault isolation:** a circuit breaker per upstream; a cap on upstream concurrency; **when upstream misbehaves, traffic never bypasses L2 to the origin**; when a whole region fails, clients get UNAVAILABLE with RetryInfo.
- **Warming:** subscribe to `WatchRefs` and prefetch the new tree nodes each time main moves (only along changed paths).
- This layer has **no durable state:** losing a node costs only hit rate, and a new one replaces it.

### 7.3 The L3 server read path
Small objects from `objects_inline`, large ones from S3, with an in-process moka cache for trees and metadata. Every S3 read goes through singleflight and a concurrency cap.

---

## 8. The derivation framework

```rust
#[async_trait]
pub trait Deriver: Send + Sync + 'static {
    const KIND: DerivedKind;       // Tree, Gen, SkipList, PathHistory, Blame, PathIndex, ...
    const VERSION: u32;            // bumped when the format changes; versions coexist and backfill in the background
    type Value: Encode + Decode;
    fn deps(&self) -> &'static [DerivedKind];
    async fn derive(&self, ctx: &DeriveCtx, cs: &Changeset,
                    parents: &[Self::Value]) -> Result<Self::Value>;
}
```
- **Lazy:** when a query finds a value missing, it goes back along the first-parent chain to the nearest computed ancestor and computes forward in topological order. Concurrent computations of one (kind, cs) merge through singleflight, and the `leases` table keeps several nodes from computing the same thing.
- **Eager:** `scion deriver` follows `cs_log`; work is sharded by (repo, kind) and assigned to workers by lease.
- **The first derivers:** `Gen` (topological depth), `SkipList` (an ancestor skip list for merge-base and is-ancestor), `PathHistory` (where each path last changed, for log, blame and conflict detection), `Blame`, `PathIndex` (path prefix search), `GitSha` (git interoperability).
- Derived values reach clients only through `GetDerived` and the specific query RPCs; **derived formats themselves are never exposed**.

---

## 9. Permissions and security

- **Policy:** `acl.toml` lives outside the repository (in the MetaStore, hot-reloaded by `ReloadPolicy`), granting read/write/land/admin to principals or groups by path prefix and ref pattern.
- **Where read permission is enforced:** `GetTrees` and `ListDir` filter out subdirectory entries the caller cannot access (the client sees only an `inaccessible` marker). Without a subtree's hash, its contents cannot be requested.
- **The cache layers' model:** a 256-bit hash is a capability. L2 only checks the token is valid, without path authorization. For **sensitive prefixes** (paths marked `sensitive=true` in the policy), the blobs' keys carry an ACL domain tag, and L2 always sends those objects to L3 for authorization and never caches them.
- **Redaction:** a flag on `objects_index`, after which every read returns `REDACTED` with ids and history unchanged; the `janitor` deletes the bytes from S3 after a grace period. The idea comes from the reference implementation.
- **Audit:** every write, ref change and admin operation goes into `ref_log` and out to NATS as an audit event.

---

## 10. High availability and capacity

| Component | Deployment | Impact of a failure |
|---|---|---|
| server | At least 2 per AZ, scaled by HPA on CPU and requests in flight | None (stateless) |
| cache | N per region; scaling moves only 1/N of the data with rendezvous hashing | Only hit rate |
| deriver / janitor | At least 2, sharded by lease | Derived data lags for a while, with lazy computation as the fallback |
| LandCoordinator | Inside the server, assigned by lease | The leader changes within about 5 seconds, with higher land latency meanwhile |
| Metadata, Standard tier | PostgreSQL + Patroni (1 primary, 2 standbys, synchronous replication) | Automatic failover within tens of seconds after a primary failure; writes unavailable meanwhile, reads unaffected |
| Metadata, Distributed tier | YugabyteDB across 3 AZs, RF=3 or 5 | Losing a minority of replicas goes unnoticed |
| Metadata, Lite tier | One SQLite file | No high availability; periodic backups (`VACUUM INTO` or a tool like Litestream) |
| S3 | A managed service with cross-region replication | — |
| NATS | 3-node JetStream | Only event pushes; clients fall back to polling refs |

**Several regions:** writes go to the primary region (ref CAS needs strong consistency). Reads go to the local L2, then the nearest L3. L3 reads immutable data from the nearest replica (follower reads with YugabyteDB, regional read replicas with PostgreSQL); only ref reads go to the primary or leader. Several regions need the Distributed tier.

**Rough capacity** (30 million checkouts and 100,000 lands a day):
- L2: about 300,000 objects per second at peak, on about 4 to 8 nodes with large memory and NVMe.
- L3: about 1% of reads come through, on about 3 to 6 nodes.
- Metadata: writes come mostly from changesets and derived data, about 1,000 to 3,000 rows per second. One PostgreSQL primary handles that, so the Standard tier suffices at this scale; for writes across several AZs or larger scale, a 5-node YugabyteDB.

---

## 11. Observability and operations

- **Tracing:** a trace runs from the client (the SDK injects `traceparent`) through L2, L3, the metadata database and S3.
- **Core SLIs:** hit rate per layer, p50/p99/p999 of `GetObjects`/`GetTrees`, end-to-end land latency, the batch size distribution, the CAS failure rate, derivation lag (the head seq of `cs_log` minus the derived seq), QPS and error rate per service.
- **Example SLOs:** `ResolveRef` p99 under 20 ms; `GetObjects` p99 under 10 ms on an L2 hit; land p99 under 500 ms (without conflicts).
- **Operations commands:** `scion admin fsck` (object integrity and reference closure), `scion admin rederive --kind blame --version 2`, `scion admin lease ls`, `scion admin drain <node>`.

---

## 12. Tests and conformance

| Level | Contents |
|---|---|
| Unit / property tests | Canonical encoding round trips and rejection of invalid input (proptest), TreeBuilder against full rebuilds, diff3 against a reference |
| Differential tests | After importing real git repositories, Scion's log, blame, diff and merge-base against git's |
| Concurrency correctness | Linearizability checks of land (lincheck), covering concurrent lands, lease takeovers and killed leaders |
| Fault injection | Crashes at `land.after_rebase`, `after_commit`, after S3 writes and other points (after the reference implementation) |
| **Protocol conformance** | The `scion-conformance` black-box suite: runs against any server address; scenarios in YAML, run by `scion-client`, language-neutral |
| Load tests | Sustained landing load (Poisson arrivals); ghz against `GetObjects`; targets in §0 |
| Chaos | Killing L2, PG failovers (Patroni), YugabyteDB leader moves, AZ partitions, S3 latency injection |

### 12.1 Function-level test requirements (mandatory, server and client alike)

Every function has tests. This is a hard gate for merging, checked in review and in CI by this section.

**Coverage**
1. **Every function with logic** (`pub` or private) has unit tests in the same file's `#[cfg(test)] mod tests`. Only pure forwarding, plain field access and `derive`-generated code are exempt.
2. Each function's tests cover at least three kinds of case: **the normal path**, **edge cases** (empty input, zero, the maximum, a single element, exactly at a limit) and **every error branch**. Each variant of an error type is triggered by at least one test.
3. **Public APIs** (types and functions a crate exports) also have doc tests or integration tests under `tests/` that show correct usage.
4. **Deterministic logic such as encoding, sorting and hashing** has property tests (`proptest`): round trips agree, invalid input is always rejected, outputs are unique.
5. **A bug fix starts with a regression test that reproduces it**, naming the issue in the test name or a comment.
6. Concurrent functions (locks, leases, batching, singleflight) have concurrency tests: `tokio::test(flavor = "multi_thread")` with competing tasks; `loom` for low-level synchronization primitives.

**Conventions**
- Test names: `<function>_<case>_<expected>`, such as `decode_tree_unsorted_entries_rejected`.
- Tests are deterministic and repeatable: no network, real time or execution order; time and randomness through an injected `Clock` and seeds. Tests needing external services (PG, S3) live under `tests/`, behind `#[ignore]` or a feature, and run in CI's integration stage.
- Assert behavior, not implementation details; assert errors by variant with `matches!`, not by message text.
- A unit test finishes within 100 ms.

**CI gates** (measured by `cargo llvm-cov`)
| Scope | Line coverage | Branch coverage |
|---|---|---|
| Core crates: `scion-object`, `scion-store*`, `scion-tree`, `scion-merge`, `scion-land`, `scion-wc` | ≥ 90% | ≥ 80% |
| Other crates | ≥ 80% | — |
| Code added or changed by each PR (diff coverage) | ≥ 90% | — |

CI fails below a gate. Code that really cannot be tested (process startup glue in `main`, say) is marked explicitly with `#[cfg_attr(coverage_nightly, coverage(off))]`, with the reason given in review.

---

## 13. A client sketch (Rust SDK)

```rust
let c = scion_client::Client::builder()
    .endpoint("https://scion.example.com").repo("mono")
    .auth(Auth::from_env()).build().await?;

let head = c.resolve_ref("main").await?;
let cs = c.changeset(&head)
    .put("svc/a.rs", new_bytes)          // small files inline, large ones uploaded first
    .message("fix").create().await?;
match c.land("main", &cs).await {        // request_id generated; safe to retry
    Ok(r) => println!("landed {}", r.tip),
    Err(e) if e.is_merge_conflict() => { /* e.conflicts(): the three blob ids */ }
    Err(e) => return Err(e.into()),
}
```
The SDK, working copy, daemon and CLI are designed in full in [`rust-client-architecture.md`](./rust-client-architecture.md).

---

## 14. Milestones

| Milestone | Duration | Contents | Acceptance |
|---|---|---|---|
| **M0 Protocol and skeleton** | 2 weeks | proto v1 + buf CI, `scion-object`, golden test vectors, in-memory store, the single-binary skeleton, `GetServerInfo`, coverage gates in CI | Encoding vector tests pass; `scion-client` connects and negotiates capabilities; §12.1 met |
| **M1 Minimal loop** | 4 weeks | sqlx (SQLite + PostgreSQL) + object_store (local file system + S3-compatible storage, SeaweedFS in CI), Lite mode, Upload/CreateChangeset/TreeBuilder, Land by direct CAS, ReadFile/ListDir/GetTrees, git import | `scn` does clone (sparse)/commit/land; differential tests pass |

> **M1 implementation notes** (2026-10-16):
> - **git import lives in the client** (`client/gitimport`, the command `scn import git <path> --repo NAME`), writing only through the public protocol, along the same validation path as any user; the server needs no extra write channel. The result is deterministic: `change_id` derives from the git SHA, the git SHA goes into the extension field `git.sha1`, and repeated imports give the same commit ids; incremental imports fast-forward commit by commit along the first-parent chain. Differential tests compare git and Scion commit by commit: file lists, modes, contents and metadata. Bulk imports of large repositories (the starhub migration, §16) will write on the server directly later.
> - **Lands only fast-forward;** the working copy's `scn sync` rebases at file level on the client, reporting conflicts with exit code 3. Once M3's server-side rebase ships, `Land` handles commits that fell behind.

> **M2 implementation notes** (2026-10-16):
> - **The PathHistory index:** creating a commit records every path in its delta (the `path_changes` table), so log and blame filtered by path take candidate commits from the index instead of decoding commit objects one by one.
> - **A first-parent skip list:** commit records carry `fp_depth` and `fp_jump` (a Myers skew-binary single-pointer skip list), finding an ancestor at any depth of the chain in O(log n); `is_ancestor` takes this fast path first, and blame uses it to tell whether a candidate is on the chain.
> - **The commit record cache:** records are immutable, so the server caches them in process with `CachedMeta` and warms it in the background at start (about 0.5 s for 100,000); history walks prefetch by generation in batches, so even cache misses do not query one row at a time.
> - **The blame cache:** derived data holds only the final version and a checkpoint every 32 versions, so blaming again computes only the increment; it follows the first-parent chain, and changes brought in by merges count for the merge commit.
> - **Batched blame cache lookups:** the latest version is looked up alone (the common case of repeated queries), the others 64 at a time with `get_derived_many`, avoiding a round trip per version to a remote database.
> - **The GitSha index:** imported commits carry the extension field `git.sha1`, written to the derived data `git-sha1` when the commit is created; `ResolveRevision` tries commit ids, refs and git SHAs in turn, and `GetDerived` returns git SHAs and generations.
> - **Query RPCs:** Log, IsAncestor, MergeBase, Diff (server-streamed, 500 entries per batch), Blame, ResolveRevision, Stat and GetDerived, each declared as a feature in `GetServerInfo`.
> - **Upgrading from M1:** M1 recorded only the `first_parent` column. Migration 0008 copies it into `parents` and marks those repositories' skip lists unknown. Then `scion admin reindex [--repo NAME]` (before starting the server, which caches commit records) corrects lineage from the commit objects (all parents, merges' other parents included, and skip lists, overwriting wrong values) and backfills PathHistory and GitSha. It can be run again.
> - **Benchmarks** (`server/query/tests/bench.rs`, 100,000 commits, objects in memory): with SQLite and PostgreSQL metadata, the p99 of log, diff, ancestry, merge-base and blame are all under 20 ms (PostgreSQL: cold blame p99 13.6 ms, log --path p99 15.6 ms). With objects on S3, the object reads of blame and diff are left to M4's cache layers.
> - **Left for later milestones:** the generic Deriver framework and a separate deriver process (M3/M4); PathIndex. Blame follows only the first-parent chain, and log filtered by path does not follow renames.

| Milestone | Duration | Contents | Acceptance |
|---|---|---|---|
| **M2 Derivation and queries** | 3 weeks | The derivation framework, Gen/SkipList/PathHistory/Blame, Log/Blame/Diff/MergeBase, the deriver process | Query p99 under 100 ms on a repository of 100,000 commits |
| **M3 High concurrency** | 3 weeks | LandCoordinator batching and leases, the idempotency table, rate limits and load shedding, lincheck | 200+ lands per second on one ref; no duplicated or lost lands when the leader is killed |

> **M3 implementation notes** (2026-10-17):
> - **Server-side rebase** (`server/merge`): compares only the delta's paths in base and onto, applying unchanged ones directly, at a cost independent of the repository's size and of the commits in between; files changed on both sides get a line-level diff3 (diffy), and the executable bit merges by three-way rules; binary files, symbolic links, files over 8 MiB, modify/delete, add/add and file/directory conflicts are MERGE_CONFLICT, with details in the ErrorInfo metadata (`path.N`, `kind.N`, `base.N`, `ours.N`, `theirs.N`). More direct than §6.3's "intersect path_history", with the same results.
> - **Land rebases by default:** the stack to land runs from the commit back along first parents to a commit in the ref's history, fast-forwarded or rebased one by one; `fast_forward_only` keeps the fast-forward-only semantics (used by the git import). Rebased commits keep author, message and change id, and drop `git.sha1`.
> - **Idempotency and deduplication:** `land_commit` updates the ref, writes `landed` (each change id lands once per ref) and `idempotency` (the same request id returns its first success, different parameters are INVALID_ARGUMENT, kept 24 hours) in one transaction. Only successes are recorded: a failed request wrote nothing, and its retry runs again. `GetLandStatus` looks up by request id. The SDK retries retryable errors with the same request id.
> - **LandCoordinator** (`server/land`): one task per ref, batches of at most 64 requests or 5 ms, rebased in order, conflicting requests failing alone; a batch's rebase results stay in memory first (a write-behind buffer) for later requests to build on, and the whole batch is written at the end in bulk (objects written concurrently, multi-row inserts with `index_paths_many` / `put_changesets`) and committed with one `land_commit`.
> - **Clusters:** turned on with `scion server --advertise-url URL`. Each ref's task holds a lease (the `leases` table, TTL 5 seconds, renewed every 2), and other nodes that get `NotLeader` forward over gRPC (`x-scion-forwarded`, never forwarded again). A normal exit releases leases so others take over at once; a killed process's leases expire. The ref version is the fencing token, so two nodes briefly both believing they lead cannot corrupt the ref.
> - **Overload protection:** a global cap on requests in flight (refused at once beyond it, RESOURCE_EXHAUSTED / RATE_LIMITED with `retry_after_ms`), and a per-repository write token bucket (`--repo-write-rate`).
> - **Acceptance:** `server/land/tests/bench.rs` (2,000 requests, 200 concurrent clients, one ref, objects in memory): PostgreSQL 885/s (p99 379 ms), SQLite 1,921/s (p99 173 ms). `server/land/tests/lincheck.rs` checks no duplicates, no losses, nothing from thin air and real-time order, covering concurrent conflicts on one node, forwarding between two nodes and killing the leader during a land (SQLite metadata, clients retrying with the same request id).
> - **Left for later:** automatically keeping cold refs off leaders (every ref goes through a batching task for now, which exits when idle); per-tenant rate limits and deadline propagation in M5, with authentication.

| Milestone | Duration | Contents | Acceptance |
|---|---|---|---|
| **M4 Cache layers** | 3 weeks | `scion cache` (foyer + rendezvous + singleflight + hedging), WatchRefs, warming | L2 hit rate at least 99%; 200,000+ object reads per second per node |

> **M4 implementation notes** (2026-10-17):
> - **WatchRefs** (`RefService.WatchRefs`): a snapshot first, then the new state at each move; only the latest state of each ref is guaranteed, and resubscribing with `known_versions` resumes after a disconnect. Lands on this node are pushed at once; lands on other nodes of a cluster are found by per-repository polling (only repositories with subscribers, every `--ref-poll-interval`, 1 second by default), without NATS. Subscription streams are ended on shutdown, so long connections do not hold up a graceful stop.
> - **`scion cache`** (`server/cache`): a foyer hybrid cache (memory evictions go to disk), rendezvous sharding (BLAKE3 scores, one hop between nodes, fetching upstream itself when the owner is unreachable), and single-flight in the new shared crate `common/flight` (claiming and completing separated, a batch of misses merged into one upstream request). The upstream pool: p95 hedging, per-upstream circuit breaking (5 failures in a row cool down for 5 seconds), application errors not switching upstreams, every fetched object's hash checked. Warming subscribes to upstream WatchRefs and descends only into changed subtrees. **Per-repository visibility matches L3** (review pointed out that hits and single-flight looked only at object ids): bytes are deduplicated by id, single-flight merges by (repository, id), the id is confirmed visible in the request's repository before answering, successful fetches record (repository, id) in a visible set, hits of unknown visibility are confirmed in a batched `HasObjects`, and invisible ids return the same OBJECT_MISSING as L3. The daemon does the same.
> - **The L3 read path** (§7.3): `CachedBlobs` wraps the BlobStore; small objects (≤1 MiB) go into a moka cache weighted by bytes, concurrent reads of one object read the backend once, and backend concurrency is capped at 256. `--read-endpoint URL@REGION` recommends caches in ServerInfo.
> - **`scn daemon`** (`client/daemon`): a Unix socket (0600). `GetObjects` is answered from a local CAS (disk files + a memory tier, hashes checked on read, evicted by modification time), and misses are fetched as the caller through the SDK (which uses the regional cache the server recommends); `RefService` is forwarded, registering prefetches on `ResolveRef`; other RPCs are forwarded as they are at the HTTP/2 level (an axum fallback), so the daemon needs no knowledge of them. Through the daemon, the SDK sends `x-scion-upstream` with each request, so one daemon serves several servers. `scn` goes through the daemon by default, starting it on demand and connecting directly when that fails; `SCION_NO_DAEMON=1` turns it off.
> - **SDK:** batched `get_objects` (used by the working copy's checkouts and syncs, with 8 batches downloading concurrently and files written on background threads during checkouts); read endpoints chosen by region, connecting directly to the server when unreachable.
> - **Fix:** WriteService's request size limit used to equal the upload chunk (about 1.1 MB), rejecting a `CreateChangeset` of 100,000 changes; it now leaves room by `max_changes_per_changeset` (256 bytes each, at most 256 MiB).
> - **Acceptance:** `server/cache/tests/bench.rs` (Apple M5, objects in memory, clients and cache on one machine): 200 clients checking out the same 20,000 objects from cold, a 99.5% hit rate (the theoretical maximum), each object fetched upstream once; 1.53 million objects per second from one warm node (gRPC, batches of 64), and 164,000 requests per second of 1 object each. `client/daemon/tests/bench.rs`: 100,000 files checked out through a warm daemon in 5.3 s, the same as creating those files locally (5.4 s; the client target of 5 s is bounded by the local file system, and creating files on several threads is slower).
> - **Left for later:** isolating sensitive prefixes in cache layers and L2 token checks, with M5's ACLs; the daemon's `/metrics` and systemd / launchd hosting in M5; the recursive `GetTrees` RPC is not implemented (warming and checkouts use level-by-level batched `GetObjects`).

> **M5a authentication and authorization notes** (2026-10-17): M5 is split into several PRs, and this is the first.
> - **Standard protocols only:** self-issued access tokens (ES256 JWT, 15 minutes); opaque tokens (PATs and refresh tokens, only their SHA-256 stored); external JWTs (JWKS or an HS256 shared secret, with configurable claim mapping); RFC 7662 introspection; the RFC 8628 device flow; an OPA-compatible authorization callback. starhub-server (HS256, `current_user`, rebac) is just a configuration; see `docs/guide/auth.md`.
> - **ACL:** TOML rules grant by subject, repository wildcard, action (read < write < land < admin), ref and path prefix, kept in the metadata database and reloaded by every node within 30 seconds. Reading objects by id needs read on the whole repository (§9's filtering of directory entries in GetTrees is deferred, as `GetTrees` is not implemented); `ListDir` / `Diff` filter entries for readers limited to some paths; creating a commit checks each changed path; landing checks the target ref.
> - **Cache layers:** unlike §9's "L2 only verifies signatures", L2 and the daemon do not parse tokens: they call `CheckAccess(repo, read)` on L3 with the caller's token and cache the result per (token, repository) for 60 seconds. This works for PATs, external tokens and the authorization callback alike, with revocations at most 60 seconds late. When the server is unreachable, the daemon lets identities confirmed within 24 hours read locally. Keeping sensitive prefixes out of caches (§9) is not implemented yet.
> - **Client:** the SDK's `Auth::Exchange` exchanges for access tokens and renews 2 minutes early; `scn login` (device flow or `--token`), `whoami` and `logout`, with credentials in the macOS Keychain or a 0600 file.

> **M5b redaction and operations notes** (2026-10-18):
> - **Data model:** no column on `objects_index`, but a new table, `redactions`, with a row per (repository, object) (reason, operator, time, whether active, scheduled deletion time, deletion time). Undoing only marks the row inactive. Each change gets a globally increasing `seq` from the single-row counter table `redaction_seq`: writing transactions update that row first, and its row lock commits writes in seq order, so syncing incrementally by seq never misses a small seq committed late. Not a SEQUENCE, since sequence values are allocated in an order unrelated to commit order.
> - **Only blobs can be redacted:** trees and commit objects are the structure of history, and redacting them would break checkouts and queries.
> - **The read path:** `GetObjects` checks redactions in one batch after visibility, and returns `REDACTED` for the whole request if any object is redacted, succeeding fully or failing up front as with OBJECT_MISSING. `ReadFile`, `Blame` and the three-way merges of lands read through `RedactedBlobs`, checking blob by blob. When a rebase needs to merge a redacted version, the land fails and the content never enters a new commit.
> - **Cache layers:** a new RPC, `ObjectService.ListRedactedObjects(repo, after_seq)`, needing any read in the repository. L2 and the daemon keep the redacted objects per repository, syncing incrementally at most every 10 seconds with the caller's token. A redacted hit returns the same REDACTED as L3 and deletes the local bytes (bytes are shared across repositories, so other repositories' reads fetch upstream again). When a sync fails, the read is refused (`UNAVAILABLE`; application errors such as a rejected token are passed on) rather than served from the local copy, since otherwise the "at most 10 seconds" bound would not hold while upstream is unavailable; only an old server's `UNIMPLEMENTED` counts as no redactions.
> - **Purges:** `Purge` schedules a deletion after a grace period (`--purge-grace`, 24 hours by default), undoable meanwhile. Each janitor round runs due deletions, deleting bytes only when every repository whose object index has the object has redacted it, and otherwise keeping it pending and logging. "Which repositories have this object" is `object_repos`: a primary-key probe per repository, rather than a secondary index by id on `objects_index` (not worth maintaining on the write hot path). `BlobStore` gains `delete`.
> - **Operations commands:** `scion admin redact / unredact / purge / redactions`, reading and writing the stores directly; `AdminService` offers the same remotely, needing admin on the repositories involved, with `all_repos` and unscoped listings needing system administration. `scion admin fsck` checks the reference closure ref → commit → tree → file, the object index (sizes included) and object storage, and `--verify` reads and hashes. `scion admin lease ls` lists leases.
> - **Limits:** checking out a revision with a redacted file fails (no placeholder file); blame returns REDACTED when it reaches a redacted version; the same content uploaded again after a purge is not deleted automatically. `rederive` and `drain` are deferred.

> **M5c observability and rate limiting notes** (2026-10-18):
> - **A new crate, `scion-obs`** (the lowest server layer, usable from store up):
>   - metrics with `prometheus-client`: one registry per process, updated by each layer directly; the regional cache's existing statistics are read at scrape time rather than turned into metric types. `/metrics` is on its own port (`--metrics-listen`), outside authentication and rate limits;
>   - tracing with `tracing-opentelemetry` and OTLP gRPC export, configured by the standard environment variables.
> - **Middleware order** (outside in): tracing → metrics → global load shedding → authentication → per-tenant rate limits → deadlines → services, as in §5.3.
>   - Load shedding comes before authentication, so overload spends no CPU on signatures.
>   - Per-tenant limits need the authenticated identity, so they come after authentication.
> - **Metric labels:** the metrics layer is outside authentication, and paths are whatever callers send, so only the protocol's methods get time series; the method list (`METHODS`) is generated from the protos by `scion-proto`'s build.rs, and other paths count as `unknown`.
> - **Metric status codes:** may be in the response headers (trailers-only error responses) or in the trailers at the end of a stream, so the response body is wrapped and recorded when it ends. Streaming RPCs' latency is therefore the whole stream's.
> - **Per-tenant rate limits:** not `governor`, but the existing token buckets.
>   - Tenants: the authenticated principal; anonymous requests by source IP.
>   - Token buckets and in-flight slots live in moka, evicted after 10 idle minutes, at most a million keys: counting by IP cannot grow without bound, and idle buckets are full anyway, so eviction does not change the outcome.
>   - Requests forwarded within the cluster are not counted again (as with per-repository write limits). `x-scion-forwarded` is ordinary metadata that callers could send too, so a request counts as forwarded only when the value equals the cluster's shared secret (`--cluster-secret`, compared in constant time); without a secret configured, everything counts. Per-repository write limits use the same check now. Requests with the header are still never forwarded again (avoiding loops), so forging it only fails the caller's own requests.
> - **Deadlines:**
>   - the smaller of the client's `grpc-timeout` and `--max-request-timeout` (60 seconds by default), returning DEADLINE_EXCEEDED beyond it;
>   - the deadline goes into the request extensions. Lands forwarded to the leader and the regional cache's upstream requests set `grpc-timeout` from the time left; the regional cache's upstream requests also inject `traceparent`;
>   - tonic itself times `grpc-timeout` and fires first (returning CANCELLED), so beyond the server's cap this layer mostly propagates and counts.
> - **Left for later:**
>   - the SDK and `scn` injecting `traceparent`;
>   - the daemon's `/metrics`: client crates cannot depend on the server's `scion-obs`;
>   - a derivation lag metric: there is no deriver yet;
>   - child spans for metadata database and S3 calls.

> **Repository and branch management notes** (2026-10-18): the first step of the minimal loop for starhub, between M5c and M5d.
> - **`RepoService`:** Create / Get / List / Update / Delete / Restore / Fork. Quotas are deferred.
>   - Permissions: admin on the repository's name to create, change, delete and restore; read on the source and admin on the target to fork; ListRepos checks read repository by repository and returns only readable ones.
> - **Soft deletion:** `repos` gains `deleted_at_ms`, and lookups by name find only repositories not deleted. After the grace period (`--repo-delete-grace`, 24 hours by default, at least 2), the janitor purges in batches.
>   - How: a batch at a time by each table's primary key (`(pk) IN (SELECT pk … LIMIT n)`, for SQLite and PostgreSQL alike, without ctid), deleting the `repos` row last, which frees the name.
>   - Why the grace period has a minimum: caches at every layer are keyed by repository name (the commit record cache, the visible sets and redaction state of L2 and the daemon), all with a 1-hour TTL. They have expired before the name is freed, so no cross-node invalidation is needed.
>   - `object_repos` leaves deleted repositories out.
> - **Forks:** one transaction copies the object index, commits, derived data, path history and refs with `INSERT … SELECT`; active redactions get new seqs through `ROW_NUMBER()`. Land records and request results are not copied. Asynchronous forks of very large repositories are deferred.
> - **Ref writes:** `CreateRef` / `UpdateRef` (non-fast-forward allowed, that is, a forced move back) / `DeleteRef`, each a CAS of one ref, needing land on that ref. The default branch cannot be deleted.
>   - A ref recreated after deletion starts at version 1 again; WatchRefs does not push deletions.
> - **Deployment:** a `Dockerfile` (multi-stage build, BuildKit caches, running as non-root, base images replaceable by mirrors) and `deploy/compose` (now `deploy/prd` and `deploy/dev`; PostgreSQL + SeaweedFS + server, with an optional cache profile).

> **Ref event notes** (2026-10-18): the event part of starhub's S1.
> - **The event table:** `ref_events` is written in the same transaction as `land_commit` / `delete_ref` / `fork_repo`. Its seq comes from the single-row counter `ref_event_seq`, as with `redaction_seq`: the row lock makes seq order the commit order, so reading incrementally by seq never misses a small seq committed late.
>   - The cost: every ref write serializes on that row. Lands commit in batches, and the PostgreSQL benchmark shows no measurable difference from main (762–847 against 763–788 per second on one machine).
>   - Events record the repository name rather than repo_id, so they stay readable for the retention period after a purge.
>   - The commit before a change comes from the previous version in `ref_log`, or from `DELETE … RETURNING` for deletions.
> - **Pulling:** `EventService.ListRefEvents(after_seq)`. `ref_event_bounds` returns (the earliest seq kept, the counter's value), so even with every event cleaned up it can tell whether part of what a consumer missed is gone (`truncated`).
> - **Pushing:** webhooks (HMAC-SHA256 in GitHub's format) and NATS JetStream (`Nats-Msg-Id` deduplication, waiting for acknowledgment).
>   - A lease per target (`events/<target>`), progress kept in settings, in seq order, at least once, retried with backoff on failure.
>   - A new target starts from where the server was before it began listening, without replaying history.
>   - The commits in an event come from `scion_query::new_commits` (reachable from new and not from old), and path-level changes from the commits' deltas.
>   - Unlike the original design, subjects do not contain the repository name (which may contain `.`).
> - **Left for later:** repository-level events (creation, deletion, forks themselves), webhook filters by repository or event type, several webhooks.

> **Pre-land check notes** (2026-10-18): the pre-push checks of starhub's S1.
> - **When:** `Land`, `CreateRef` and `UpdateRef` call the external webhook synchronously, after authorization and rate limits and before entering the land queue.
>   - What is checked is the version the client submitted (a server-side rebase does not change each commit's delta), so the check sits outside the coordinator and does not slow batching.
>   - Lands forwarded within the cluster (with the right `--cluster-secret`) are not checked again.
> - **What:** `new_commits(base, tip)`, with base the ref's current commit; a new ref compares with the default branch. Without new commits, nothing is called.
> - **Outcomes:** 2xx allows (`{"allowed": false}` denies); 4xx (but 408 and 429) denies; anything else counts as unavailable and denies by default (fail-closed).
>   - When a check is configured but its HTTP client cannot be created, everything is denied rather than silently skipping the check.
>   - Denials return the new `PRE_LAND_REJECTED`.

| Milestone | Duration | Contents | Acceptance |
|---|---|---|---|
| **M5 Production** | 4 weeks | The YugabyteDB tier and the `migrate-meta` tool, ACL/OIDC/JWT, redaction, OTel, Helm, chaos tests, multi-region follower reads, publishing `conformance` | Every SLO met; a single-AZ failure drill passes |

---

## 15. Open decisions

1. ~~Metadata database~~: decided, three tiers, SQLite / PostgreSQL / YugabyteDB, sharing one sqlx codebase, without CockroachDB (2026-10-16, see §4.4).
2. ~~Canonical encoding~~: decided, a custom varint format with golden test vectors, specified in `SPEC.md` (2026-10-16, see §4.2).
3. ~~Reusing the reference implementation's code~~: ideas only (how the spec is organized, fault injection points, redaction and so on), no copied code, everything written here (2026-10-16).
4. ~~Client language~~: decided, Rust, with only the default client and no Go version (2026-10-16).

---

## 16. Extension points for external systems

To connect starhub-server and run alongside Gitaly, these **general** capabilities are added (usable by standalone deployments too); the full design is in §5 of [`starhub-integration.md`](./starhub-integration.md):

- `RepoAdminService`: Create/Delete/Get/Update/Fork/List, for many small repositories, with every per-repository resident cost loaded lazily.
- External identities (JWKS of trusted issuers), and an optional external authorization callback (with a TTL cache).
- The pre-land webhook: called synchronously, with a configurable timeout and failure handling (fail-open or fail-closed).
- `scion.<repo>.ref.updated` events (with path-level deltas), on NATS or pushed as webhooks.
- Derived data: `GitSha`, `LastCommitPerEntry`, `LfsPointerIndex`, `TreeStats`, `Sha256`.
  > **GitSha notes** (2026-10-18): every commit has a git-compatible SHA. Imported commits use the extension field `git.sha1`; others compute blob/tree/commit level by level on demand (the commit object carries an extra `scion-id` header so the mapping is injective), cached in the derived data `git-commit`, `git-sha1` (the reverse), `git-tree` and `git-blob`, with unchanged subtrees hitting the cache. `GetGitShas` maps in batches, and events and pre-land requests carry `old_git_sha`/`new_git_sha`/`git_sha`. S3's git gateway reuses the same object formats. See [`docs/guide/git-sha.md`](../guide/git-sha.md).
  > **Archive notes** (2026-10-18): `QueryService.Archive` writes tar / tar.gz / zip depth first through the tree (zip with a streaming writer that needs no Seek), sending a chunk each time `max_chunk_bytes` fills, with memory about one file's size; entries have the commit's time, and redacted files get a placeholder note. It needs read access to the whole subtree. See [`docs/guide/archive.md`](../guide/archive.md).
  > **LastCommitPerEntry / TreeStats notes** (2026-10-18): `ListLastCommits` follows the first-parent chain (with Blame's semantics), cached per (commit, directory); with the first parent cached, it updates incrementally from this commit's delta, otherwise finds each entry's nearest candidate on the chain through the PathHistory index, scanning deltas (up to 2,000 commits) when the index is unavailable. `GetTreeStats` caches file counts, subdirectory counts and bytes by tree id. See [`docs/guide/browse.md`](../guide/browse.md).
- `QueryService.Archive`, `ImportService` (incremental imports from git).
- The `scion git-gateway` role: a subset of Gitaly's SmartHTTP/SSH services, so the git CLI can reach Scion repositories too.
- Object storage: when sharing MinIO with starhub, a separate bucket or `prefix` (see §4.6).
