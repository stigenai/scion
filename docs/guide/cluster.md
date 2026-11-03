# Clusters and landing

English | [简体中文](cluster.zh-CN.md)

The Scion server is stateless: several `scion server` nodes that share one metadata database (PostgreSQL or YugabyteDB) and one object store (S3-compatible) form a cluster behind a load balancer. This guide describes how landing works in a cluster, how to configure it and what happens when a node fails.

## What a land does

`Land` lands a commit on a ref (`main`, say):

- When the commit follows the ref's current commit, the ref fast-forwards.
- When the ref has moved, the server rebases the stack of commits being landed onto the latest ref (a three-way merge). Changes that do not overlap land directly; changes to different parts of the same file merge automatically; changes to the same lines return `MERGE_CONFLICT`, with the conflicting paths and the ids of the base, ours and theirs versions.
- With `fast_forward_only=true`, only fast-forwards are allowed (`scn import git` uses this).
- One land takes at most 256 commits (fast-forwards, new refs and rebases alike): the server records the change id of every commit that enters the ref to deduplicate, and beyond the limit returns `FAILED_PRECONDITION`, so clients should land in segments. `scn import git` fast-forwards 256 at a time.
- **Exactly once:** every request carries a request id. After a network failure or a timeout, retrying with the same request id returns the result of the first success (results are kept for 24 hours). Each change id also lands at most once on a ref, even if the client resubmits with a new request id. `GetLandStatus` looks a result up by request id. The Rust SDK retries retryable errors with the same request id on its own.

Concurrent lands on one ref are grouped into batches by the LandCoordinator (at most 64 requests or 5 ms), rebased in order and committed at once. A conflicting request fails alone and does not affect the others in its batch.

## Turning on cluster mode

Each node declares, with `--advertise-url`, the address the other nodes can reach it at:

```sh
# Node 1
scion server --listen 0.0.0.0:7080 --advertise-url http://10.0.0.11:7080 \
  --meta postgres://scion:…@db:5432/scion --blobs s3://scion-objects/prod

# Node 2
scion server --listen 0.0.0.0:7080 --advertise-url http://10.0.0.12:7080 \
  --meta postgres://scion:…@db:5432/scion --blobs s3://scion-objects/prod
```

The environment variable `SCION_ADVERTISE_URL` works too. Lite mode (`--lite`) is single-node and cannot form a cluster.

Give every node the same `--cluster-secret` as well (`SCION_CLUSTER_SECRET`, printable ASCII).
- **What it does:** nodes forwarding a land include it, so the receiving node knows the request was forwarded within the cluster and does not count it against the rate limits again.
- **Without it:** a forwarded land counts once on each of the two nodes. Callers who add the forwarding header themselves cannot get around the limits either.

In cluster mode, one node (the leader) handles the lands of each ref:

- The first node to receive a land for a ref takes a lease on the ref and becomes its leader. Leases last 5 seconds and are renewed every 2.
- Other nodes forward that ref's lands to the leader (directly between nodes; a forwarded request is never forwarded again).
- After a ref has been idle for 30 seconds, the leader releases its lease, and the next land elects a leader again.

Leases only keep nodes from contending for the same ref. Correctness comes from conditional updates of the ref: the ref's version is the fencing token, so even if two nodes briefly both believe they are the leader, the ref cannot be corrupted.

## Node failures

| Situation | Outcome |
|---|---|
| The leader exits normally (SIGTERM) | It finishes the batch in hand and releases its leases; another node takes over at once. |
| The leader is killed or cut off | Its lease expires within 5 seconds and another node takes over. Meanwhile lands on that ref return retryable errors (`UNAVAILABLE` / `LAND_CONTENTION`). |
| The client's connection drops while it waits for a result | Retry with the same request id: a land that went through returns its original result, one that did not is processed again, with nothing duplicated or lost. |

Linearizability tests cover this (`server/land/tests/lincheck.rs`), including killing the leader while lands are in progress.

## Overload protection

| Option | Environment variable | Default | What |
|---|---|---|---|
| `--max-concurrent-requests N` | `SCION_MAX_CONCURRENT_REQUESTS` | 10000 | Requests in flight per node. Beyond it, requests are refused at once (`RESOURCE_EXHAUSTED` / `RATE_LIMITED`, with `retry_after_ms`), not queued. Health checks are exempt. |
| `--repo-write-rate RATE[/BURST]` | `SCION_REPO_WRITE_RATE` | unlimited | Writes per node and repository (an upload, a commit creation and a land count once each); beyond it, `RATE_LIMITED`. |
| `--tenant-rate RATE[/BURST]` | `SCION_TENANT_RATE` | unlimited | Requests per node and caller; every RPC counts once. The caller is the authenticated principal, or the source IP for anonymous requests. |
| `--tenant-concurrency N` | `SCION_TENANT_CONCURRENCY` | unlimited | Requests in flight per node and caller, so that one caller cannot take every slot. |
| `--max-request-timeout DURATION` | `SCION_MAX_REQUEST_TIMEOUT` | 60s | The longest a request may take, or the client's deadline (`grpc-timeout`) if shorter; beyond it, `DEADLINE_EXCEEDED`. A server stream is limited only until it starts; `--max-stream-lifetime` limits how long it stays open. `0s` leaves only the client's deadline. |
| `--tenant-streams N` | `SCION_TENANT_STREAMS` | unlimited | Open server streams (`WatchRefs`, `GetObjects`, `ReadFile`, `Diff`, `Archive`, `Grep`) per node and caller. The caller is the same as for `--tenant-rate`. Beyond it, the new stream gets `RESOURCE_EXHAUSTED` / `RATE_LIMITED`. A call that fails authorization takes no slot. |
| `--max-stream-lifetime DURATION` | `SCION_MAX_STREAM_LIFETIME` | 1h | The longest a server stream stays open. It then ends with `UNAVAILABLE` ("stream lifetime of Ns reached; reconnect"), which the client SDK retries: `scn watch` reconnects and resumes by version with no lost event. The server also closes each connection after this time plus a grace of at most 30s. Thus a client that stops reading cannot hold a stream open. `0s` sets no limit. |

Each ref's land queue also holds at most 1,024 requests; when it is full, lands return `RATE_LIMITED` too.

- **Per-caller limits:** health checks do not count, nor do requests forwarded with the right `--cluster-secret`, which the first node already counted.
- **Deadlines carry over:** a land forwarded to the leader keeps the remaining deadline, and so do the regional cache's requests to the server.
- **Refused requests** count in the metric `scion_rejected_requests_total{reason}`; see [observability.md](observability.md).

## Performance

Lands on one ref, 2,000 requests from 200 concurrent clients, each changing a different file (`server/land/tests/bench.rs`, objects in memory):

| Metadata database | Throughput | p99 latency |
|---|---|---|
| PostgreSQL 16 | 885/s | 379 ms |
| SQLite | 1,921/s | 173 ms |
