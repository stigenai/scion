# Caching: the regional cache and the local daemon

English | [简体中文](cache.zh-CN.md)

Scion reads go through three layers, and all three serve exactly the same `ObjectService`:

```
scn / SDK ──► scn daemon (L1, per machine) ──► scion cache (L2, per region) ──► scion server (L3, authoritative)
```

Objects are content-addressed and immutable, so every layer can cache them forever without invalidation; refs are never cached. Cache layers keep no durable state: losing one costs only hit rate.

This guide covers deploying a regional cache, how clients pick their read path and how the daemon behaves.

## The regional cache, `scion cache`

### Starting it

```sh
scion cache --listen 0.0.0.0:7090 \
  --upstream http://10.0.0.11:7080 --upstream http://10.0.0.12:7080 \
  --memory 8GiB --disk-dir /nvme/scion-cache --disk-size 500GiB \
  --warm mono:main
```

Then have the server recommend it to clients:

```sh
scion server ... --read-endpoint http://cache.cn-beijing.internal:7090@cn-beijing
```

After connecting to the server, a client picks a cache from `ServerInfo.read_endpoints` to read objects from: one in its region if there is one (the SDK's `ReadEndpoints::Region`), otherwise the first. If the cache cannot be reached when connecting, the client reads from the server. Writes, lands, refs and queries always go to the server.

### Options

| Option | Environment variable | Default | What |
|---|---|---|---|
| `--upstream URL` | `SCION_CACHE_UPSTREAMS` | required | Upstream servers; repeatable (nodes of one cluster, replicas of each other) |
| `--memory SIZE` | `SCION_CACHE_MEMORY` | 1GiB | Size of the memory tier |
| `--disk-dir DIR` | `SCION_CACHE_DISK_DIR` | off | Directory of the disk tier, preferably on NVMe; objects evicted from memory go here |
| `--disk-size SIZE` | `SCION_CACHE_DISK_SIZE` | 64GiB | Size of the disk tier |
| `--max-object SIZE` | | 8MiB | Larger objects are forwarded as usual but not cached (at most 16MiB) |
| `--advertise-url URL` / `--peer URL` | `SCION_CACHE_ADVERTISE_URL` / `SCION_CACHE_PEERS` | single node | Sharding across nodes; see below |
| `--warm REPO[:REF,…]` | `SCION_CACHE_WARM` | no warming | Repositories and refs to warm; repeatable |
| `--warm-blob-max SIZE` | | 0 | Also prefetch files up to this size when warming; 0 prefetches trees only |
| `--no-hedge` | | hedging on | Turn hedging off |
| `--max-stream-lifetime DURATION` | `SCION_MAX_STREAM_LIFETIME` | 1h | The longest a GetObjects stream stays open. It then ends with UNAVAILABLE and the client reconnects. The cache also closes a connection at this time plus a grace of 30s at most, so a client that stops reading cannot hold it. `0s` sets no limit |
| `--tenant-streams N` | `SCION_TENANT_STREAMS` | no limit | Open GetObjects streams for each caller. A caller is a hash of the Bearer token in the `authorization` header, so two tokens of one user have two budgets. A header that is not a Bearer one counts as the peer IP address. The next stream gets RESOURCE_EXHAUSTED |

### The read path

1. Local hits (memory first, then disk) return at once.
2. Misses: concurrent requests for one object fetch it once (single-flight), and the misses of one request are fetched in one upstream request.
3. With several nodes, the object's owner node is asked first; see the next section.
4. Fetching from the upstream servers:
   - **Hedging:** when a request has not returned after the recent p95 latency, the same request goes to another upstream, and the first answer wins.
   - **Circuit breaking:** an upstream that failed 5 times in a row cools down for 5 seconds without requests.
   - **Failover:** when an upstream is unavailable, times out or returns corrupt data, the next one is tried; application errors such as a missing object are passed on as they are.
   - **Verification:** every fetched object's hash is checked.

### Sharding across nodes

With several cache nodes in a region, each declares its own address and the others':

```sh
# Node 1
scion cache --upstream http://l3:7080 --advertise-url http://10.0.1.1:7090 \
  --peer http://10.0.1.2:7090 --peer http://10.0.1.3:7090
```

Objects are assigned to nodes by rendezvous hashing. A node asked for an object it does not own passes the request to the owner (one hop only), which decides whether to fetch it upstream, so the whole region fetches each object once. Adding or removing a node moves about 1/N of the objects. When the owner is unreachable, a node fetches upstream itself.

Clients can connect to any node, so a load balancer in front is enough.

### Warming

`--warm mono:main` subscribes to the server's WatchRefs:

- On subscribing, it prefetches the ref's whole current tree (within a limit on the number of objects).
- After that, each time the ref moves, it prefetches only the new trees along the changed paths, skipping directories whose subtree id did not change, at a cost proportional to the change.

The server pushes lands on its own node at once; lands on other nodes of a cluster are found by polling the metadata database, every `--ref-poll-interval` of the server (1 second by default).

### Security model

The cache holds no credentials: the caller's `authorization` goes upstream as it is.

Visibility is the server's: an object is visible only in repositories whose index has it.
- Bytes are cached by object id, so the same content in different repositories is stored once.
- Before answering, the cache confirms the object is visible in the request's repository: a successful fetch records (repository, object) in a visible set, and local hits of unknown visibility are first confirmed with the server in a batched `HasObjects`.
- Invisible objects return the same `OBJECT_MISSING` as the server.
- Concurrent misses of one object are merged per (repository, object), so one repository's fetch never answers another repository's request.

Per-user permissions are checked with the server as well (`CheckAccess`); see [caches and the daemon](auth.md#caches-and-the-daemon) in the authentication guide.

## The local daemon, `scn daemon`

`scn` connects to the server through a local daemon by default. When no daemon is running, the first command that needs a connection starts one in the background, and connects directly if that fails.

- **Object reads:** the daemon checks its local cache first (disk plus memory, hashes checked on read). Misses are fetched as the caller (through the regional cache the server recommends), and concurrent misses of one object in one repository are fetched once. As with the regional cache, an object goes only to repositories that can see it.
- **Other requests:** forwarded to the server as they are at the HTTP/2 level, so the daemon needs no knowledge of them.
- **Prefetching:** each time a ref the working copy resolved (`ResolveRef`) moves, the daemon fetches the changed trees and files up to 256 KiB in the background, so a later `scn sync` hits. A subscription idle for 30 minutes is dropped.
- **Several servers:** one daemon can serve several servers at once; each request carries its real server address.
- **Credentials:** the daemon holds none; each request carries the caller's own token.
- **Exit:** after 30 idle minutes.

| Command / environment variable | What |
|---|---|
| `scn daemon run [--max-disk BYTES] [--idle-timeout SECS]` | Run in the foreground (`--idle-timeout 0` never exits on its own) |
| `scn daemon status` / `scn daemon stop` | Show / stop |
| `scn doctor` | The `objects:` line says whether objects come through the daemon, the regional cache or the server |
| `SCION_NO_DAEMON=1` | Connect directly, without the daemon (usual in CI containers) |
| `SCION_DAEMON_SOCKET` / `SCION_DAEMON_CACHE_DIR` | Another socket and cache directory |

Default locations:

| Platform | Socket | Object cache |
|---|---|---|
| Linux | `$XDG_RUNTIME_DIR/scion/daemon.sock` | `$XDG_CACHE_HOME/scion/objects` (`~/.cache/scion/objects` by default) |
| macOS | `~/Library/Caches/scion/daemon.sock` | `~/Library/Caches/scion/objects` |

Only the current user can connect to the socket (mode 0600). The disk cache holds 20 GiB by default and evicts the least recently used objects beyond that. The daemon supports only Unix (Linux and macOS) for now.

## Performance

See the [component benchmarks](../performance.md), from `server/cache/tests/bench.rs` and `client/daemon/tests/bench.rs`:

- 200 clients checking out the same objects from cold: a 99.5% hit rate, each object fetched from the server once.
- 1.53 million objects per second from one warm cache node (gRPC, batches of 64).
- A warm checkout of 100,000 files through the daemon takes as long as creating those files.
