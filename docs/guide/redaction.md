# Redaction and fsck

English | [简体中文](redaction.zh-CN.md)

## Redaction: removing leaked content

When a password, a key or other secret has been committed, rewriting history is expensive: every commit id changes and every working copy has to resync. Scion **redacts** instead:

- object ids and history stay the same;
- reading a redacted file returns `REDACTED` (gRPC `NOT_FOUND` with `ErrorInfo.reason = REDACTED`, and `ids` and `redaction_ids` in the metadata);
- to delete it for good, **purge** it, which removes the object bytes after a grace period.

Only file contents (blobs) can be redacted. Trees and commit objects are the structure of history and cannot.

### Commands

```sh
# By REV:PATH or blob id, several at once if needed
scion admin redact --repo mono --path main:config/prod.env --reason "leaked database password"
scion admin redact --repo mono b1a3f0… --reason "..."

# The same object may be in forks too: redact it in every repository that has it
scion admin redact --repo mono --path main:config/prod.env --all-repos --reason "..."

scion admin redactions [--repo mono] [--id rd_…]   # list
scion admin unredact rd_…                          # undo (before a purge runs)
scion admin purge rd_… [--grace 24h]               # schedule deletion; undoable during the grace period
scion admin purge rd_… --now                       # delete now, for good
```

These commands read and write the stores directly, with the same storage options as `scion server` (`--meta postgres://…`, `--lite --data-dir DIR` and so on). `AdminService` also offers `Redact`, `Unredact`, `Purge` and `ListRedactions` remotely:

| Operation | Permission needed |
|---|---|
| Redact in one repository | admin of that repository |
| Redact in every repository (`all_repos`) | system admin |
| Unredact, purge | admin of every repository involved |
| List one repository's | admin of that repository |
| List across repositories | system admin |

Every record keeps the reason, who did it and when. The server logs each operation at warn level.

### Where it takes effect

| Where | Behavior |
|---|---|
| The server | `GetObjects`, `ReadFile` and `Blame` return `REDACTED` at once; `ListDir`, `Diff` and `Log` work as before (they involve only paths and ids) |
| Landing | When a rebase needs a three-way merge of a redacted version, the land fails with `REDACTED`; the content is never merged into a new commit |
| The regional cache, `scion cache` | Syncs with the server at most every 10 seconds per repository, with the caller's token (`ObjectService.ListRedactedObjects`), then returns `REDACTED` and deletes its local copy |
| The local daemon | The same, per server and repository |
| When a sync fails | With the upstream unavailable, timing out or offline, cache layers **refuse** to read (`UNAVAILABLE`) rather than serve their local copy; errors such as a rejected token are passed on as they are. If the server answers `UNIMPLEMENTED` to `ListRedactedObjects`, they also refuse the read (`UNAVAILABLE`) |
| Undoing | Takes effect in cache layers within 10 seconds as well |

Redactions are per repository: the same object, if not redacted in another repository, can still be read there.

### When a purge deletes

Object bytes are stored once for all repositories. The server deletes them only when **every** repository whose object index has the object has redacted it. Otherwise the deletion stays pending, and each check logs a warning naming the repositories that can still read it; `--all-repos` covers them.

- **Who runs it:** the server's janitor runs due deletions periodically (every 10 minutes by default). Deletion is idempotent, so several nodes of a cluster running it at once is fine.
- **Grace period:** `scion server --purge-grace` sets the default when `AdminService.Purge` gives none, 24 hours by default.
- **After deletion:** it cannot be undone. Cache layers already deleted their copies at redaction.

### Limits

- **Checkouts:** checking out a revision with a redacted file fails. Commit a change that deletes or replaces the file first, then redact the versions in history.
- **Blame:** blame returns `REDACTED` when it reaches a redacted version in history.
- **Uploading again:** if someone uploads the same content after a purge, the bytes are written again. The repository's redaction record remains and reads still return `REDACTED`, but the bytes are deleted only by the next redaction and purge.

## `scion admin fsck`: checking the data

Checks every repository, read-only:

- each ref points at a commit that has a record;
- each commit record's parents have records, and its commit object exists, hashes correctly, decodes and has the same parents as the record;
- the closure of references from each commit's root tree:
  - trees exist, hash correctly and decode;
  - files are in the repository's object index, the index's size matches the tree entry, and the bytes exist;
  - redacted files are checked only in the index, and purged ones may have no bytes.

```sh
scion admin fsck --meta postgres://… --blobs s3://bucket
scion admin fsck --repo mono --verify   # --verify reads and hashes every file (by default it only checks they exist)
```

It lists up to 1,000 problems and ends with their total. It exits non-zero when it finds any, so it suits scheduled checks. Run it after a metadata migration (`migrate-meta`) too.

## `scion admin lease ls`: leases

Lists leases such as land leaders': the holder, how many times it changed hands (token) and the time left. Useful to find which node handles a ref in a cluster.

```sh
scion admin lease ls --meta postgres://…
land/mono/main  holder=http://10.0.0.5:7080  token=3  expires in 3812 ms
```
