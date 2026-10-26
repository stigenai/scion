# Git gateway: git clone, fetch and push on Scion repositories

English | [简体中文](git-gateway.zh-CN.md)

Status: G1 (fetch) and G2 (push) are implemented; G3 (efficiency) is done but for caching packs of busy refs; G4 (starhub) is next. This was item 2.5 of the [roadmap](roadmap.md). Using it: [git clients](../guide/git-clients.md).

Scion's own client is `scn`, but many tools speak only git: CI systems, IDEs, package managers, `git submodule`, starhub's users. The gateway lets the git CLI clone, fetch and push a Scion repository at the URL `scn` already uses:

```sh
git clone http://scion.example.com:7080/agentlab/app
```

## 1. Goals and limits

**Goals:**
- **Clone and fetch** with any git 2.x client, over smart HTTP (protocol v0 and v2).
- **Push**: fast-forward pushes to branches, creating and deleting branches and tags.
- **Stable SHAs:** a commit has one git SHA, the one `GetGitShas` already returns ([git-sha guide](../guide/git-sha.md)). A commit pushed through git keeps the SHA it was pushed with, and so do commits imported from git.
- **The same permissions** as `scn`: the same tokens, the same authorization chain and the same pre-land checks.
- **No cost to `scn`:** the gateway reads the same storage and derived data. `scn`'s read and write paths do not change.

**Not in scope (or later, see §9):**
- SSH (starhub already runs gitlab-shell; the gateway serves it in G4);
- submodules (gitlinks) in Scion trees: pushes that add them are refused;
- `git push --atomic`, since Scion updates one ref at a time;
- server-side rebases of git pushes. A push that is not a fast-forward is refused, as git users expect, and `git pull --rebase` resolves it on the client.

## 2. Where it runs

- **In `scion server`, on the same listener as gRPC.** gRPC requests are HTTP/2 with `content-type: application/grpc`; git requests are HTTP/1.1 (or HTTP/2) to `/<repo>/info/refs`, `/<repo>/git-upload-pack` and `/<repo>/git-receive-pack`. One router sends each request to the right side, so a repository has one URL for both clients.
  - A `.git` suffix on the repository name is accepted and ignored.
  - `--no-git` (or `[git] enabled = false`) turns the gateway off. It is on by default.
- **Code in its own crate, `server/git` (`scion-git`).** It depends on `scion-store`, `scion-query` and `scion-land`, not on the gRPC services, so a later stage can run it as a separate role (`scion git-gateway`) next to `scion cache` without a rewrite.
- **Direct storage access.** The gateway reads the `MetaStore` and `BlobStore` (with the record and object caches) and lands through the same `Coordinator` as `LandService`. Going through gRPC would cost a round trip per tree.

## 3. Mapping refs and objects

**Refs:**

| Scion | git |
|---|---|
| a branch `main` | `refs/heads/main` |
| a tag `tags/v1` | `refs/tags/v1` |
| the default branch | `HEAD`, a symbolic ref (`symref=HEAD:refs/heads/main`) |

Other git refs (`refs/notes/*`, `refs/pull/*` and so on) cannot be pushed.

**Objects:** every Scion object has one git object, built from the `GitSha` derived data:

| Scion | git object |
|---|---|
| a file | `blob`: its content |
| a directory | `tree`: entries in git's order, modes `100644`, `100755`, `120000`, `40000` |
| a commit | `commit`: see below |

**Commit objects.** A commit's git object is, in this order:
1. **the raw object, if the commit has one.** The new extension `git.commit` holds the git commit object exactly as git wrote it: signatures (`gpgsig`), `encoding`, `mergetag` and other headers included. Pushes and new imports record it (§5, §6).
2. **otherwise, for a commit with `git.sha1`** (imported before `git.commit` existed): the object rebuilt from the commit's fields without the `scion-id` header. If its SHA is the recorded `git.sha1`, it is the commit's object. If not (it had a signature, or a submodule the import skipped), the gateway cannot serve the commit and says so (§7).
3. **otherwise** (a commit made with `scn`): the computed object with the `scion-id` header, as `GetGitShas` computes it today.

In each case, the gateway checks that the object's SHA is the commit's git SHA, and that its `tree` and `parent` lines name the git SHAs of the commit's root tree and parents. A commit that fails the check is never served under a SHA that does not match its content.

**Lookups the other way.** `git-sha1` already maps a commit SHA back to a Scion commit. Two new derived kinds map tree and blob SHAs back (`git-tree-id`, `git-blob-id`), filled at the same time as `git-tree` and `git-blob`. A push uses them to find the objects it builds on.

## 4. Fetch (`git-upload-pack`)

**Advertising refs.**
- **v2** (git 2.26 and newer by default): `ls-refs` with `ref-prefix`, `symrefs` and `peel`; the `fetch` command.
- **v0/v1:** the ref list with the capabilities `multi_ack_detailed`, `side-band-64k`, `thin-pack`, `ofs-delta`, `no-done`, `shallow`, `deepen-since`, `symref`, `agent`; v2 offers `fetch=shallow`.
- Each ref's SHA comes from `GetGitShas`; a commit not derived yet is derived first.

**Negotiation.**
- The client names what it **wants** (tips) and what it **has**.
- The gateway reports a `have` as common when `git-sha1` maps it to a commit in the repository, which is enough to stop the client walking further back.

**Which objects to send.**
- **Commits:** the ancestors of the wants that are not ancestors of the common haves, from the commit graph (generation numbers bound the walk).
- **Trees and blobs:** walked from each new commit's root tree, skipping every tree and blob already sent and every one reachable from the common haves.
  - Scion trees are content-addressed, as git's are, so an unchanged subtree has the same id in both commits and is skipped as a whole.
  - For each common commit, its root tree's subtrees are added to the skip set lazily: only the paths that new commits change are compared.

**Shallow fetches** (`--depth`, `--shallow-since`, `--unshallow`).
- The boundary is found by walking from the wants level by level: with `deepen N`, commits at depth N keep no parents; with `deepen-since T`, a commit keeps its parents only if they were all committed at T or later.
- Boundary commits with parents go to the client as `shallow` lines (v2: a `shallow-info` section before the pack; v0: before the acknowledgments, in every response).
- The client's own shallow commits have no parents on its side whatever its haves say, so the walk stops at them. When the new depth reaches past one, it is an `unshallow` line and its parents are wanted.
- `deepen-relative` (`git fetch --deepen`) and `deepen-not` (`--shallow-exclude`) are refused with a message.

**Partial clones** (`--filter=blob:none`, `--filter=blob:limit=N`).
- The tree walk leaves out every file, or the files over N bytes; the trees all go.
- git then asks for the files it needs by SHA (`allow-reachable-sha1-in-want` in v0). A want that is not a commit is looked up through `git-tree-id` and `git-blob-id`; for data derived before those mappings, the refs' trees are indexed once (a marker records it) and the lookup retried.
- Other filters (`tree:N`, `sparse:oid`) are refused.

**The pack.**
- Format: pack version 2, written as a stream with no temporary file; its trailer is the SHA-1 of the pack.
- **Deltas only along paths.** A tree or file that a commit changed is sent as a delta against the object at the same path in a parent, when the delta is clearly smaller; no other candidates are searched, so finding deltas costs one comparison per changed object.
  - The base is named by offset (`ofs-delta`) when it is earlier in the pack, by SHA (`ref-delta`) when it comes later or, with `thin-pack`, when the client has it: an incremental fetch sends changed files as deltas against the client's copies.
  - Trees and files go oldest first, so bases mostly come first. Recently written contents (64 MiB) are kept for deltas; other bases are read again.
  - The delta indexes the base by 16-byte blocks and extends each match both ways. Files over 32 MiB are sent whole.
  - Measured on this repository (254 commits): a clone's pack is 10.0 MB against git's 9.1 MB (whole objects: 22 MB), and the clone is faster than without deltas (3.6 s against 5.9 s), since fewer bytes are compressed and sent.
- Output goes through side band 1, with progress on side band 2 (`Counting objects`, `Writing objects`).
- **Memory stays bounded.** Objects are streamed as they are read. The sets of SHAs already sent hold 20 bytes per object.

## 5. Push (`git-receive-pack`)

**Advertising.**
- The ref list and the capabilities `report-status`, `report-status-v2`, `delete-refs`, `ofs-delta`, `side-band-64k`, `quiet`, `agent`.
- Not `atomic`: Scion updates one ref at a time, so `git push --atomic` is refused by git itself. Not `push-options` either.
- Also `no-thin`: the client then sends a self-contained pack. Its deltas can name only objects in the same pack, so the gateway never has to rebuild an existing object to apply a delta.

**Receiving the pack.**
- Spooled to a temporary file as it arrives (up to 16 GiB per push), then read from it on a blocking thread. Resolved objects stay in memory up to 256 MiB and go to a second temporary file beyond that; a 1 GB push peaked at 555 MB of server memory, a 400 MB one at 457 MB:
  - whole objects and `ofs-delta`/`ref-delta` entries whose bases are in the pack;
  - each object's SHA computed, and the pack's trailer checked;
  - each file at most `max_upload_object_bytes`, the same limit as `scn` uploads.
- Files are stored before the commit that needs them, with the same dedup as `scn` uploads: a file Scion already has is not written again.

**Turning commits into Scion commits.**
- For each pushed commit, parents first:
  - **Parents** are Scion commits: found through `git-sha1`, or made earlier in the same push.
  - **The tree** is converted from git's: each entry comes from the pack, or is found through `git-tree-id`/`git-blob-id` (a renamed directory, for example, is not sent again). For trees derived before these mappings existed, the first parent's tree is indexed once on a miss.
  - **The delta** against the first parent comes from comparing the trees, skipping every subtree with the same SHA on both sides.
  - **Refused** (the push stops with a message):
    - a gitlink;
    - a mode other than the four Scion has (an old `100664` mode is read as `100644`, as git does, but that changes the tree's SHA, so it is refused too);
    - a path that is not UTF-8;
    - a path Scion's rules reject.
  - **The Scion commit** gets:
    - author, committer and message from the git commit, and its `change_id` from the git SHA, as imports do;
    - the extensions `git.sha1` (the SHA) and, when the fields cannot rebuild it, `git.commit` (the raw object).
  - **Annotated tags:** the ref `tags/NAME` points at the commit, and the tag object goes into the tag's annotation (`TagAnnotation.git_object`, kept byte for byte; the server checks that it names the commit). Fetches advertise the tag object's SHA with the peeled commit (`^{}`, or `peeled:` with v2's `peel`) and send the object when it is wanted. Tags of trees, blobs or tags are refused.
- **Checks.**
  - Before creating the commit, the gateway checks that the Scion tree's git SHA equals the commit's `tree` line. A git tree that does not round-trip is refused, so the SHA a git user sees is always the SHA of the content.
  - Only the server may set `git.sha1` and `git.commit` for a non-admin writer, and only after these checks. A non-admin client still cannot set them through `CreateChangeset`.

**Updating refs.**
- Each command (`old new ref`) becomes:
  - a branch: a `Land` with `fast_forward_only`, and the old value as the expected one;
  - a new branch or tag: a `CreateRef`;
  - `0000…` as the new value: a `DeleteRef`;
  - `--force` to a commit that is not a descendant: an `UpdateRef` with the old value. It needs the same `land` permission on the ref as moving a branch with `scn`, so a policy that protects a branch refuses it too.
- **Authorization:**
  - `read` on the repository to fetch;
  - `write` to send the pack;
  - `land` on each ref to update it, with the same per-ref policy (protected branches, for example).
  - Pre-land checks run as for `scn land` and can refuse a ref with their message.
- **Each ref on its own:** each ref is reported as `ok` or `ng <reason>`. A command whose old value is not the ref's current one gets `ng … fetch first`.
- **Events:** a push makes the same `ref.updated` events as a land, with the git SHAs in them.

## 6. Importing keeps the raw commit too

`scn import git` (and the later `ImportService`) records `git.commit` alongside `git.sha1` when rule 2 of §3 would not rebuild the commit: signed commits, `encoding` headers, messages that are not UTF-8. They then come out of the gateway byte for byte, while the other commits stay as small as before. Commits already imported without it are served as §3 describes.

## 7. Errors a git user sees

A fetch is planned, and its commits built, before the server answers, so that an error goes to the client as an `ERR` packet in place of the answer: git prints just `fatal: remote error: …`. An error found later, while the pack is being sent, comes on side band 3 as `remote: …`, followed by git's own complaint about the cut-off pack. Pushes report errors as `ng` lines.

| Case | Message |
|---|---|
| no access | HTTP 401 with `WWW-Authenticate: Basic`, so git asks for credentials; 403 when the token cannot read the repository |
| not a fast-forward | `ng refs/heads/main non-fast-forward` (git's own hint follows) |
| a commit that cannot be served (§3) | `git commit 3f2a… was imported without its raw git object, …; import the git repository again into a new Scion repository with scn 0.1.11 or later (this one keeps serving the old import), or clone it with scn (Scion commit C1…)` |
| a gitlink or an unsupported mode in a push | `path vendor/lib is a submodule; Scion repositories cannot hold submodules` |
| a pre-land check refused | the check's message |

## 8. Authentication

- HTTP Basic: any user name, with a Scion token (PAT, access token or a token from an external issuer) as the password. `Authorization: Bearer <token>` also works.
- Git's credential helpers store the token like any other HTTPS password.
- Anonymous access follows the server's settings: a public repository can be cloned without credentials when anonymous reads are allowed.

## 9. Stages

| Stage | What | Done when |
|---|---|---|
| **G1 Fetch** (done) | The router on the gRPC listener; refs; `upload-pack` v0 and v2; packs without deltas; Basic auth; commit objects as §3 | `git clone`, `git fetch` and `git pull` work against the Mac mini, and `git fsck --strict` passes on the result |
| **G2 Push** (done) | The `git-tree-id`/`git-blob-id` lookups; `receive-pack`: parsing packs, turning git commits into Scion commits, fast-forward lands, creating and deleting refs, `git.commit` (`scn import git` records it already) | `git push` of new commits, branches and tags; signed commits keep their SHAs; non-fast-forwards are refused |
| **G3 Efficiency** (done but the pack cache) | Deltas in packs; shallow clones (`--depth`); partial clones (`--filter=blob:none`); caching the last pack of a busy ref | A clone of starhub-server moves no more than 1.5 times git's bytes |
| **G4 starhub** | Gitaly's `SmartHTTPService` and `SSHService` over gRPC (with sidechannel), for gitlab-workhorse and gitlab-shell; native SSH stays out unless asked for | starhub's git routes work on a Scion repository |

## 10. Testing

- **Protocol tests:**
  - pkt-line, capabilities and pack writing and reading, with fixed vectors;
  - packs that git itself wrote (`git pack-objects`) are parsed;
  - packs the gateway writes are checked with `git index-pack --strict`.
- **End-to-end tests with the real git CLI:**
  - `server/api/tests/git.rs` starts a server, then runs `git clone`, `fetch`, `pull`, `push` and `push --force`;
  - each test checks that `git log` matches `scn log` and that `GetGitShas` returns the SHAs git sees;
  - the tests skip themselves when git is not installed.
- **Round trip:** importing a git repository (with signed commits and merges), cloning it through the gateway and comparing every object with the original repository gives no differences.
- **The Mac mini** before every release, with a git suite next to the `scn` ones.
