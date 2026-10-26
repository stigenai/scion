# Roadmap: large planned tasks we are not doing yet

This document collects tasks that are **large but not needed right now**, so they don't get lost across design docs and PR threads.

- **What each entry records**: what the task is, why it waits, when to start it, and the design points we already know.
- **When work starts**: move the entry into the relevant design doc and open a PR. When it lands, delete the entry here or mark it done.

The current focus is the **local development loop**: ignore files, undoing and amending commits, switching and merging branches, and resolving conflicts locally. That work is not tracked here; see Status in the README.

Legend: 🔵 has an initial design, ⚪ idea only.

---

## 1. Client

### 1.1 Virtualized checkout (`scion-vfs`) 🔵
- **What**: mount the working copy as a virtual file system.
  - Directory listings and file metadata appear immediately.
  - File contents download on first read, so `cat`, `sed` or an editor fetch them automatically.
- **Platforms**:
  - Linux: FUSE (`fuser` crate).
  - macOS: an NFSv3 loopback server, as EdenFS does, which needs no kernel extension. FSKit is worth evaluating later.
  - Windows: ProjFS.
- **Today**: `scn clone` downloads every file in the checkout scope. Files outside the scope can be fetched with sparse checkout (`scn sparse add`) or read with `scn cat`.
- **Why it waits**:
  - It is the largest single item: one implementation per platform, plus cache coherence, turning files into real files on write, and crash recovery.
  - Sparse checkout already covers the main use case of only needing part of a repository.
- **Start when**:
  - a monorepo is too large even for sparse checkout (millions of files, and developers can't predict which directories they need); or
  - agents need to open any revision in seconds.
- **Design points**:
  - Reads go through the daemon's L1 cache and `GetObjects`.
  - Writes turn a file into a real local file (copy-on-write). The dirstate tracks only those files, so `status` compares only them.
  - It shares its scope definition with sparse checkout.
  - See M7 in the [client design](rust-client-architecture.md).

### 1.2 File-watcher status 🔵
- **What**: the daemon watches the working copy (inotify / fanotify on Linux, FSEvents on macOS), and `scn status` reads the change list instead of walking the tree.
- **Why it waits**: the stat cache is fast enough up to tens of thousands of files. `status` on this repository takes no measurable time.
- **Start when**: a checkout scope has more than about 100,000 files, or `status` takes more than 200 ms.
- **Design points**:
  - If the watcher loses events (queue overflow, daemon restart), fall back to a full scan.
  - See M6 in the client design.

### 1.3 Daemon service install and metrics ⚪
- **What**:
  - `scn daemon install` registers the daemon as a systemd user service or a launchd agent.
  - The daemon serves `/metrics`: hit rate, cache size, upstream latency.
- **Start when**: per-platform packages are built (see 2.2).

### 1.4 traceparent from the SDK ⚪
- **What**: the SDK and CLI send a W3C `traceparent`, so client operations join the server-side traces.
- **Start when**: we need to debug latency between clients and the server.

---

## 2. Server

### 2.1 M5d: YugabyteDB and moving between tiers 🔵
- **What**:
  - Run the Distributed tier (YugabyteDB) on the same sqlx code, with transaction-conflict retries handled per dialect.
  - `scion admin migrate-meta --from … --to …`: SQLite → PostgreSQL → YugabyteDB.
  - `scion admin blob-sync`: copy objects from a local file system to S3.
  - A nightly conformance run on YugabyteDB.
- **Why it waits**: one PostgreSQL primary handles today's write load (see the capacity section of the server design).
- **Start when**: we need writes across availability zones, a single PostgreSQL primary nears its write limit, or the starhub deployment grows large.

### 2.2 M5e: production deployment and validation 🔵
- **What**:
  - A Helm chart.
  - Release builds: server images and `scn` packages for each platform.
  - A conformance suite that runs across implementations.
  - Chaos tests: kill L2, PostgreSQL failover, YugabyteDB leader moves, availability-zone partitions, injected S3 latency.
  - An availability-zone failure drill.
- **Start when**: before the first production deployment.

### 2.3 Deriver framework and a separate deriver process 🔵
- **What**:
  - Bring the derived data under one framework that declares dependencies, invalidates by version and backfills in batches. Today it is spread across the land path and the query path: generation, skip list, PathHistory, blame, GitSha, LastCommitPerEntry, TreeStats.
  - A separate process follows the land log and computes ahead of time.
- **Why it waits**: computing on demand with caching is fast enough today (see Performance in the README).
- **Start when**: more kinds of derived data arrive (LfsPointerIndex, Sha256, PathIndex), or cold-query latency becomes a problem.

### 2.4 More derived data and queries ⚪
- **`PathIndex`**: path-prefix search, for "go to file".
- **`LfsPointerIndex`**: the LFS pointer files in a repository, for starhub's `GetRepoLfsPointers`.
- **`Sha256`**: the SHA-256 of each blob, for the LFS and HF protocols.
- **`GetTrees`**: return several levels of a tree in one call, for recursive listings.
- **Merges and renames in blame**: blame follows only the first-parent chain and stops at renames. Path log follows renames with `--follow`.
- **A path cursor for ListLastCommits**: when one generation fills a whole page of index rows, it currently falls back to a scan. The result is correct, just slower.

### 2.5 Git compatibility (`git-gateway`): in progress
- Moved to the [git gateway design](git-gateway.md), which starts with stage G1 (fetch).

### 2.6 Server-side import (`ImportService`) ⚪
- **What**: incremental import from a git remote or a local path, run by the server, for mirrors and bulk migration. Import currently runs in the client (`scn import git`).
- **Start when**: starhub phase S2 (migrating existing repositories).

### 2.7 Native large files ⚪
- **What**:
  - Store large files as Scion-native chunked blobs: fetched on demand, deduplicated globally.
  - Add an `ExternalBlob` type that points at LFS objects already in object storage, so migration doesn't copy the data.
- **Start when**: starhub phase S4.

### 2.8 More repository management ⚪
- **Repository quotas**: size and file count, based on TreeStats.
- **Repository-level events**: creates, deletes and forks also go into the event feed.
- **Async fork**: fork very large repositories as a background task.

### 2.9 Storage follow-ups ⚪
Packs, deltas and `scion admin repack` are done ([object storage](../guide/object-storage.md#packs)): starhub-server takes 86 MB of MinIO against Gitaly's 82 MB, plus 54 MB of metadata in PostgreSQL. What is left:
- **Repack on its own**: a server-side schedule (after large imports, or nightly) instead of an admin command.
- **Better delta bases**: also try other candidates (renamed files, files of the same name and similar size), as git does. Trees take 6.1 MB against git's 3.6 MB.
- **Smaller pack format**: deltas name their base by its full id (38 bytes each, 2 MB on starhub-server), and the trailer stores offsets that follow from the lengths (0.8 MB).
- **Smaller metadata**: `path_changes` repeats each path in every row and in its index; ids for paths would save about 3 MB on starhub-server, at the cost of one more insert per write.
- **Pack locations on large installs**: past 256 MB the table is only vacuumed after a repack, so it stays at twice its size; partition it by pack generation, or rewrite it without locking.
- **Incremental repack**: only the objects written since the last one, appended as new packs.

### 2.10 Caching and multiple regions ⚪
- **Cold-ref detection**: stop warming and prefetching refs nobody reads.
- **Isolating sensitive prefixes**: keep content under path-level permissions out of shared caches.
- **Multi-region deployment**: writes go to the primary region and reads go to regional caches. This needs the Distributed tier.

---

## 3. Integrations

### 3.1 starhub-server 🔵
- **Scion side (S1)**: done. This covers repository management, external identity and authorization, the pre-land check, events, GitSha, LastCommitPerEntry, TreeStats and Archive.
- **starhub side, not started**:
  - a `storage_backend` field and `RoutingGitServer`;
  - the Go adapter;
  - the NATS consumer;
  - the internal `pre_land`, `introspect` and `authz` endpoints;
  - joint end-to-end tests.
- The phased plan is in the [starhub integration design](starhub-integration.md).

### 3.2 CSGLite ⚪
- **What**: CSGLite embeds the Lite tier (SQLite plus local objects).
- See section 8 of the [starhub integration design](starhub-integration.md).
