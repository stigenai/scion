# Plan: bulk push and clone

Spec: [spec.md](spec.md). Base branch for all PRs: `ent/qualification-patches`.

## Approach

### Push

The land coordinator builds many changesets in memory. It writes them in one batch.

- `Pending` (`server/land/src/pending.rs`) holds records and objects that are not stored yet.
- `ChangesetWriter::build` (`server/land/src/create.rs`) makes a `Built` without a write.
- `ChangesetWriter::persist(&[Built])` writes a batch in the correct order.

The push reuses these parts:

1. `Converter::order` resolves all pack commits with one `get_derived_many(GITSHA_KIND)`.
2. The converter collects one window of commits. For each commit, it calculates the delta against the parent tree. The parent tree comes from the window (in memory) or from the store.
3. Add `PushBackend::create_many(p, repo, &[Changeset]) -> Vec<ChangesetRecord>`. It checks paths for each changeset and admits the window one time. It builds each changeset with a `Pending` that holds the window's records and trees. Then it calls `persist` one time.
4. The converter keeps an overlay of the window's derived values (tree SHA maps, file SHA maps, commit maps). `tree_sha`, `lookup` and `record` read the overlay first. After `create_many`, one `put_derived_many` call for each kind writes the overlay.
5. `put_packed` uploads packs with bounded concurrency (4).

### Clone

`server/git/src/upload.rs` and `server/git/src/objects.rs` walk the planned trees level by level. Each level does one batched derived read and one batched pack-location read. File reads run 16 wide. They share the location lookups.

## Files to change

| Area | Files |
|---|---|
| Push order and overlay | `server/git/src/receive.rs`, `server/git/src/objects.rs`, `server/query/src/gitsha.rs` |
| Batch create | `server/git/src/lib.rs` (`PushBackend`), `server/api/src/service/write.rs` (`create_many`), `server/land/src/create.rs` (`create_many` on `ChangesetWriter`) |
| Commit-to-git-SHA map at push | `server/land/src/create.rs`, `server/query/src/gitsha.rs` (`index_git_sha`) |
| Parallel pack upload | `server/store-blob/src/lib.rs` (`put_packed`) |
| Clone plan | `server/git/src/upload.rs`, `server/git/src/objects.rs` |
| Pack locations | `server/store-blob/src/lib.rs` (read path near line 549) |

## Test strategy

- Each task adds a test that counts store calls through a counting `MetaStore` wrapper. The wrapper works like `Hooked` in `server/land/src/tests.rs`. A regression to calls for each commit then fails a unit test.
- Crash tests: inject a failure after each write step of a window (bytes, index, paths, records, maps). Then push again. Check that the result equals a clean push.
- Postgres store testsuite: `SCION_TEST_PG_URL` (see `server/store-sql/src/postgres.rs` tests).
- End to end: ent `make perf` and `make smoke` on the compose stack (owner or ent rig).

## Risks

- Memory: one window holds up to 64 MiB of file bytes plus trees. The pack is fully in memory (`Unpacked`). Therefore, the extra cost has an upper limit.
- Ref CAS, land checks and ref events do not change; they run after the conversion, as today.
- Generation and `fp_skip` values come from `Pending` records. They are deterministic. A retry therefore gives the same ids.
- All inserts are `ON CONFLICT DO NOTHING`. Packs are content-named. A retry after a partial window is therefore idempotent.

## Tasks (one PR each)

| # | Task | Depends on |
|---|---|---|
| T1 | Batch `known()` in `Converter::order` | - |
| T2 | `ChangesetWriter::create_many` and `PushBackend::create_many` | - |
| T3 | Converter window loop with in-memory overlay and batched derived writes | T1, T2 |
| T4 | Write the commit-to-git-SHA map at push | - |
| T5 | Parallel pack upload in `put_packed` | - |
| T6 | Clone: batched derived reads per tree level | - |
| T7 | Clone: read delta-base SHAs only for ref-deltas | - |
| T8 | Clone: batched pack locations and parallel pack fetch | - |
| T9 | Clone: batched commit record loads | - |
| T10 | Push crash and retry tests for each write step | T3 |
