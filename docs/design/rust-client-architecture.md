# Scion's default client architecture (Rust)

English | [简体中文](rust-client-architecture.zh-CN.md)

> Version: Draft v0.1 · 2026-10-16
> Companion document: [`rust-server-architecture.md`](./rust-server-architecture.md), which is authoritative for the protocol, the error model and authentication.

---

## 0. Position and the iron rule

The default client is the project's official client: the CLI `scn`, the node daemon and the Rust SDK.

**The iron rule: the client depends only on the public protocol.**
- Client crates live under `client/` and **may depend only** on `scion-proto` (generated code) and `scion-object` (pure computation: ObjectId and canonical encoding, no IO) under `common/`, and on other crates in `client/`.
- They **must not** depend on any server crate (store, land, derive, api…). A dependency layering check in CI enforces this.
- Only test code (`dev-dependencies`) may start a server in process, for end-to-end tests.

This guarantees that anything the default client can do, anyone can do by following the protocol. The project **maintains only this one Rust client**, with no Go or other language versions, but the protocol itself stays language-neutral.

---

## 1. Components

```
┌──────────────────────── Users / agents / CI ──────────────────────┐
│  scn (CLI)               Rust programs / agent frameworks          │
│   └─ scion-wc (working copy) └─ scion-client (SDK)                 │
│        └──────────────┬─────────┘                                 │
│      scion-client (SDK: transport · auth · retries · upload · fetch · land) │
└───────────────────────┬───────────────────────────────────────────┘
                        │ preferred: Unix socket     otherwise: direct
                        ▼                                  │
              scn daemon (L1 per node)                      │
               local CAS cache · answers reads itself       │
               forwards everything else as it is            │
                        │                                  │
                        ▼                                  ▼
                scion cache (L2, regional) ──────▶ scion server (L3)
```

| Directory | Crate / binary | Responsibility |
|---|---|---|
| `client/sdk` | `scion-client` | **SDK:** connections, endpoint discovery, authentication and refresh, uniform retries, batched uploads and downloads, land helpers, the `--json` output model |
| `client/wc` | `scion-wc` | **Working copy:** sparse configuration, checkout/sync, the stat cache, status, collecting changes for a commit, conflict state, the operation log |
| `client/daemon` | `scion-daemon` | **L1 cache:** implements `ObjectService` over a local CAS; forwards other RPCs transparently; prefetching |
| `client/cli` | `scion-cli` → the binary `scn` | The command line; `scn daemon` is a subcommand, so the client is always **one binary** |
| `client/vfs` | `scion-vfs` (after M7) | Virtualized checkouts: FUSE on Linux, NFSv3 on macOS |

The client binary `scn` and the server binary `scion` **ship separately**: the client links none of the server's dependencies such as sqlx or object_store, so it is smaller and easier to audit.

---

## 2. Connections and endpoint discovery

The CLI uses the first available endpoint in the order below.

1. The endpoint in a positional URL.
2. The `--endpoint` option.
3. The `SCION_ENDPOINT` variable.
4. The endpoint in `.scion/state.toml` if the command uses a working copy.
5. The endpoint from the saved login.
6. `http://127.0.0.1:7080`.

`scn clone` and `scn import git` skip step 4.

**Splitting reads from writes** (done by `scion-client`):
- When a daemon socket exists (`$XDG_RUNTIME_DIR/scion/daemon.sock`, or `~/Library/Caches/scion/daemon.sock` on macOS), **every request goes to the daemon**, which answers it or forwards it.
- Without a daemon, `ObjectService` reads go to the nearest L2 in `ServerInfo.read_endpoints`, and everything else to the server.
- The first time the CLI needs to read objects and no daemon is running, it **starts one in the background** (`SCION_NO_DAEMON=1` turns this off, as CI containers usually do).

The connection layer is a tonic `Channel` with HTTP/2 keepalive. It calls `GetServerInfo` once at start, caches it for 5 minutes, and enables capabilities according to its `features`.

---

## 3. Authentication

| Case | How |
|---|---|
| Developers | `scn login` through the OIDC device flow; tokens go into the system keychain (the `keyring` crate), or a file with mode 0600 when that is unavailable |
| Agents / CI | The environment variable `SCION_TOKEN`, or a workload identity file (`SCION_TOKEN_FILE`) exchanged through `AuthService` for a short-lived JWT |
| Refresh | The SDK refreshes 2 minutes before a token expires; on `UNAUTHENTICATED`, it refreshes once and retries once |

**The daemon holds no user credentials:** each request carries the caller's own bearer token, which the daemon forwards as it is, so users sharing one daemon never get each other's identities.

---

## 4. SDK (`scion-client`)

### 4.1 The API
```rust
let c = scion_client::Client::builder()
    .endpoint("https://scion.example.com")
    .repo("mono")
    .auth(Auth::from_env())
    .build().await?;

// API only: read a file, change files, land (the path agents use most)
let head = c.resolve_ref("main").await?;
let src  = c.read_file(&head, "svc/a.rs").await?;              // Bytes; large files are reassembled from chunks
let cs   = c.changeset(&head)
            .put("svc/a.rs", patched)                           // inline under 1 MiB, uploaded first otherwise
            .delete("svc/old.rs")
            .message("fix: …")
            .create().await?;
match c.land("main", &cs).await {                              // generates and persists a request_id
    Ok(r) => println!("landed {}", r.tip),
    Err(e) if e.is_merge_conflict() => { let cf = e.conflicts(); /* the three blob ids */ }
    Err(e) => return Err(e.into()),
}

// Streams and batches
let stream = c.get_trees(&root, TreeQuery::prefixes(["svc/", "lib/"]).depth(Unlimited));
c.fetch_blobs(ids).concurrency(64).for_each(|blob| …).await?;
```

### 4.2 Uniform built-in behavior (strictly per §3.5 of the server document)
- **Retries:** only for errors the table marks retryable; honoring `RetryInfo`; exponential backoff with full jitter; a cap on the total time.
- **Idempotency:** write requests get a `request_id` (UUIDv7) by default. In a working copy, the id goes into the operation log **before** the request is sent, so after a crash the same id finishes the job (see §6).
- **Uploads:** `HasObjects` in batches (1,000 per batch) → upload only what is missing → small objects through the batch API, large ones in streamed chunks; the concurrency limit is configurable.
- **Downloads:** `GetObjects` in several streams of 64 ids each; **every object's BLAKE3 is checked on arrival**, and a mismatch is an error and a refetch (no cache layer is trusted).
- **Observability:** `traceparent` is injected; the SDK exposes request counts and latencies (with the optional `metrics` feature).

### 4.3 Conformance
`scion-client` runs the conformance YAML scenarios of §12 of the server document, which check the server and the SDK itself.

---

## 5. The working copy (`scion-wc`)

### 5.1 Layout
```
<root>/
  .scion/
    state.toml        # repo, endpoint, base commit, tracked ref, sparse configuration, local draft stack
    dirstate.redb     # stat cache: path → (size, mtime_ns, ctime_ns, ino, mode, blob_id)
    oplog/            # operation log: checkouts/commits/lands in progress, for crash recovery
    conflicts/        # conflict records: path → {base, ours, theirs} blob ids
    lock              # operation lock (flock), so that concurrent scn commands cannot corrupt each other
  <only the directories listed in sparse are materialized>
```
- `dirstate` uses `redb` (a pure-Rust, transactional embedded KV store), which updates incrementally even with millions of paths instead of rewriting everything each time.
- **Sparse configuration:** a list of directory prefixes (with optional exclusions). `.scionignore` uses gitignore syntax, parsed by the `ignore` crate.

> **Implementation note (sparse checkout):** the scope is the files at the root plus some directories (cone semantics, without the files of parent directories), kept in the `sparse` field of `.scion/state.json` (None for a full checkout); exclusions are not implemented yet.
> - The tree walk prunes: it enters only scope directories, their parents and their children, and never requests trees or file contents outside the scope. The server needs no changes; a prefix-filtered `GetTrees` would only save round trips and is left for later.
> - sync, land, the client's rebase and scope changes filter both sides of every comparison by the same scope: dirstate holds only files in scope, and comparing it with a full tree would take the files outside it for deletions.
> - Untracked files outside the scope are always refused (status and commit fail), since the server may have content at the same path that was not checked out, and a commit would silently overwrite it.
> - `scn sparse add|rm|set|disable` require no uncommitted changes and check out or delete the difference at the current snapshot (the pending commit, if any). Untracked files outside the old scope are judged by the new one: still outside, an error; inside, taken over when they match the snapshot's version and refused (never overwritten) when they differ; not in the snapshot, kept as new files.

### 5.2 checkout / sync
```
1. Resolve the target ref → commit → root tree
2. Parallel GetTrees (prefix-filtered), fetching only the trees in the sparse scope
3. Diff the old and new trees (comparing only subtrees whose hashes differ) into additions, deletions and changes
4. Write the operation log: {op: checkout, from, to, plan_hash}
5. Fetch missing blobs in batches (daemon/local first) and write files in parallel:
     write to a temporary file → fsync (configurable) → rename to replace atomically; handle modes, symbolic links, directories added and removed
     reuse the parent directory's fd when opening directories, to save path lookups
6. Update dirstate and state.toml (base=to), clear the operation log
```
- **Cost depends only on the difference:** a `sync` to a new main touches only the changed paths.
- **Uncommitted local changes:** `sync` first `Rebase`s the local draft commits on the server, then checks out; dirty uncommitted files that conflict with the update are refused, with a hint to commit first (no file-level merging locally, to keep the client simple).

### 5.3 status
1. Scan the sparse directories with a parallel walk (`jwalk` / `ignore`).
2. Compare stat information with `dirstate`: entries that match exactly are skipped; only mismatches are rehashed with BLAKE3 (mmap for large files, rayon across files).
3. New, deleted and modified files are each resolved against the base tree.
4. **The same-second modification problem:** entries whose mtime falls in the same time window as the dirstate write are marked "uncertain" and always rehashed next time, so no change is missed.

From M6, an optional **file-watching mode**: the daemon records dirty paths with the `notify` crate, so `status` costs only as much as the dirty files.

### 5.4 commit / draft stack
- `scn commit`: collect changes → upload missing blobs → `CreateChangeset(parent = the top draft or base)`, which gives a new draft id pushed onto the draft stack in `state.toml`.
- **Drafts live on the server:** the client records only ids, so `scn checkout <draft-id>` on another machine carries on.
- `scn amend` and `scn split` are compositions of the server's `CreateChangeset` and `Rebase`; the client needs no history logic.

### 5.5 Landing and conflicts
- `scn land`: write the `request_id` to the operation log, then call `Land(stack = the draft stack)`.
  - Success: clear the draft stack, point `base` at the new tip; sync automatically by default.
  - Timeout or crash: the next `scn` command checks the operation log first and finishes with `GetLandStatus` under the same `request_id`. **Nothing lands twice, and no successful land is lost.**
  - `MERGE_CONFLICT`: record the three blobs in `.scion/conflicts/` and write files with conflict markers into the working tree; after `scn resolve <path>` marks them resolved, `scn land` again. The `--json` output has the full conflict structure, so agents can take the three versions and resolve them themselves.

---

## 6. Crash consistency

| Operation | Operation log records | Recovery |
|---|---|---|
| checkout/sync | from, to, the plan's hash | Run again (idempotent: files that already have the target content are skipped) |
| commit | Uploaded objects, the request_id | Resend `CreateChangeset` with the same request_id |
| land | The request_id, the draft stack | Look the result up with `GetLandStatus`, or resend with the same id |

Every command that changes the working tree must hold `.scion/lock` first.

---

## 7. The node daemon (`scn daemon`)

### 7.1 Responsibilities
- Implement **the whole `ObjectService`** on a Unix socket, answering from a local CAS cache and fetching from the upstream (L2) on misses.
- Forward other RPCs (Write, Land, Ref, Query, Auth) **as they are**, through a transparent tonic/hyper proxy that neither parses them nor keeps state.
- Subscribe to `WatchRefs`: when main moves, prefetch the new trees and blobs under the sparse prefixes the machine's working copies registered.

### 7.2 The local cache
- Storage: `foyer`'s disk cache, or a directory sharded as `objects/<hh>/<hex>`. Total size is capped (by default, the smaller of 50 GB and 20% of the disk), with LRU eviction.
- **Hashes are checked before caching**; reads can be sample-checked.
- Concurrent misses of one object are merged by singleflight and fetched once.
- **Caching policy:** the server marks `cacheable=false` on `ObjectChunk`s (objects under sensitive prefixes), which the daemon only forwards and never writes to disk.
- **Several users:** the socket has mode 0660 and belongs to the `scion` group; objects are content-addressed, so a requester must already know the hash (a capability model).
- Snapshots at start: a periodically generated bundle of hot objects can be fetched from `SCION_SNAPSHOT_URL` to warm up, which helps CI nodes starting cold.

### 7.3 Lifecycle
The CLI starts the daemon on demand; systemd or launchd can host it too (`scn daemon install`). It exits after 30 idle minutes with no working copy registered (configurable). `/metrics` exposes the hit rate, cache size, upstream latency and so on.

---

## 8. The CLI (`scn`)

### 8.1 Commands
```
scn login | logout | whoami
scn clone <repo> [dir] [--sparse a/,b/] [--ref main]
scn sparse add|rm|ls <prefix>
scn status | diff [--staged-against <rev>] | log [path] | blame <file>
scn commit -m <msg> | amend | split
scn land [--ref main] [--no-sync]
scn sync | checkout <rev>
scn resolve <path> | conflicts
scn cat <rev>:<path> | ls <rev>:<dir> | show <rev>     # no working copy needed
scn daemon [run|install|status|stop]
scn doctor                                             # checks connection, authentication, daemon, cache, clock skew
```
- log, blame and diff (of history) **all call the server's Query RPCs**; the client has no history data at all.

### 8.2 Designed for agents
- Every command takes `--json`. The output carries a field `"schema": "scion.cli.status/v1"`; the schema files live in `schemas/` and change under the same compatibility rules as the protos.
- Stable exit codes: `0` success, `1` a general error, `2` a usage error, `3` a merge conflict, `4` a retryable error (retries exhausted), `5` an authentication failure.
- With `SCION_TOKEN` set, nothing is interactive; no paging and no color by default (except on a TTY).

### 8.3 Implementation
`clap` (derive), `tokio`, `indicatif` (progress on a TTY only), `anyhow`/`thiserror`; error messages carry a next step ("run `scn conflicts` to see the conflicts", for example).

---

## 9. Releases and distribution

| Channel | Form |
|---|---|
| Linux | Static musl binaries (x86_64/aarch64), deb/rpm |
| macOS | Universal binaries, a Homebrew tap |
| CI | The `ghcr.io/<org>/scn` image; the daemon can also run as a Kubernetes DaemonSet, with pods mounting the socket through hostPath |
| Rust developers | `cargo install scion-cli`; the SDK on crates.io as `scion-client` |
| Install script | `curl … \| sh`, which checks the signature (cosign) |

---

## 10. Tests

| Level | Contents |
|---|---|
| Unit | dirstate's same-second problem, sparse rule matching, tree diffs, atomic writes |
| End to end | A server started in process from dev-dependencies (memory backend), running clone → commit → land → sync in full |
| Conformance | The Rust SDK runs the YAML scenarios of §12 of the server document |
| Crash recovery | Crashes injected at each stage of the operation log (simulating kill -9), checking the state after recovery |
| Differential | The same operations on the same repository, comparing the output of `scn status/diff` with `git status/diff` |
| Performance | Sparse checkouts of 100,000 files, status (unchanged and with 1% changed), the daemon's hit rate |

**Function-level tests:** as on the server, §12.1 of the server document applies strictly (every function with logic has unit tests covering the normal case, the edge cases and every error branch; `scion-wc` is a core crate, with line coverage of at least 90%).

**Performance targets:** at 100,000 files, an unchanged `scn status` under 300 ms, and a sparse clone hitting the daemon under 5 s.

---

## 11. Milestones (scheduled together with the server)

| Milestone | Client deliverables |
|---|---|
| M0 | The `scion-client` skeleton, `GetServerInfo`, golden vector tests, connections and authentication (static tokens) |
| M1 | `scn clone/sparse/status/commit/land/sync/cat/ls` (direct, without the daemon), dirstate, the operation log |
| M2 | `scn log/blame/diff/show`, `--json` schema v1 |
| M3 | Land crash recovery (`GetLandStatus`), the conflict workflow, the conformance YAML runner |
| M4 | `scn daemon` (L1 cache, transparent forwarding, WatchRefs prefetching), starting on demand |
| M5 | Device-flow login, the keychain, `scn doctor`, releases for each platform, DaemonSet deployment |
| M6 | Status in file-watching mode |
| M7 | `scion-vfs`: Linux FUSE (the `fuser` crate), macOS NFSv3 loopback |
