# Bulk push and clone

## Problem

ent mirrors company monorepos into Scion with `git push`. Engineers and agents clone them back. Postgres and S3 round trips limit both paths, not the CPU:

- A push converts the pushed commits one at a time (`server/git/src/receive.rs`, `Converter::convert`). After PR #19, each commit continues to make about 56 Postgres statements, about 17 write transactions and 2 S3 PUTs. Each write transaction waits for a WAL flush.
- A clone walks trees one at a time (`server/git/src/upload.rs`). A warm clone makes 6.5 statements for each commit. A cold clone (on a different node) makes 13.9 statements for each commit. It also makes one serial S3 GET for each pack that it reads.

Before PR #19, prod took 501 s to push spindle, and 321 s to clone it. Spindle is a 2,125-commit, 15.9 MB repo. The next repo is infra-blocks: 12,493 commits, 115,091 objects and a 68 MB pack. At that rate, the push takes an estimated 45-50 min.

## User stories

### US-1: Mirror a monorepo in seconds

Given an empty Scion repo and a git repo with 2,125 commits,
when the mirror job runs `git push` of `main`,
then the push completes and the Postgres statement count for the push is O(windows), not O(commits).

### US-2: A failed push leaves a repo that a retry can complete

Given a push that fails after some windows are written (crash, timeout, pool error),
when the client pushes again,
then the push completes, and `git clone` gives the same commits and trees as the source.

### US-3: Clone a monorepo in seconds

Given a Scion repo with 2,125 commits on another node (cold cache),
when a client runs `git clone`,
then the Postgres statement count is O(1) for each tree level, not O(trees). Pack reads run in parallel.

## Requirements

- **FR-001** The push converts the pushed commits in windows. A window closes at the first of these limits: 500 commits or 64 MiB of new file bytes. It also closes at the end of the push.
- **FR-002** For each window, the push makes a set number of store calls. This number does not depend on the commit count. The calls are:
  - one blob store call (packs of up to 16 MiB, uploaded in parallel);
  - one object index write;
  - one path index write;
  - one changeset record write;
  - one write for each derived kind (git SHA maps for commits, trees and files).
- **FR-003** The write order in each window is: object bytes, then object index, then path index, then changeset records, then git SHA maps. The push must not write a git SHA map entry before it writes the changeset record.
- **FR-004** The push finds which pushed commits the repo has. It uses one batched derived read for all commits in the pack. It does not use one read for each commit.
- **FR-005** In a window, the converter reads parent records, tree SHAs and file SHAs of the same-window commits from memory. It does not read them from the store.
- **FR-006** The tree SHA check stays. For each converted commit, the git SHA of the stored Scion tree must equal the pushed tree SHA. The converter calculates the git SHA from the in-memory window data.
- **FR-007** The push writes the commit-to-git-SHA map and the git-SHA-to-commit map. The first clone after a push does not write to Postgres.
- **FR-008** Path authorization (`authorize_paths`) continues to run for each commit. The rate limiter admits each window one time.
- **FR-009** The clone plan reads the derived git SHAs for all trees and files of one tree level in one batched call.
- **FR-010** The clone reads a delta base's git SHA only when it writes a ref-delta. A clone that writes only ofs-deltas reads none.
- **FR-011** The clone reads the pack locations of the planned objects in batched calls. It fetches packs in parallel, with bounded concurrency. Concurrent reads of objects in the same pack share one location lookup and one trailer read.
- **FR-012** The clone loads the commit records it needs in batched calls (`changesets_below` or a batched `get_changesets`).
- **FR-013** All existing tests must pass. The store testsuite contracts (`RepoNotFound`, idempotent writes) must stay.

## Out of scope

- Store or stream a pre-built git pack for each repo (the "boundary" design). It needs redaction invalidation.
- Shallow and filtered fetch changes.
- `synchronous_commit=off` for derived data.
- Changes to the scn protocol, the scion-cache service and native (non-git) commits.

## Success criteria

Use ent `make perf` on the local compose stack and the Postgres statement log to measure these criteria:

- **SC-1** A 200-commit synthetic push (3 files for each commit, new seed) makes at most 60 Postgres statements in total. It does not make about 11,000.
- **SC-2** A clone of the same repo on the other node makes at most 100 Postgres statements.
- **SC-3** A push of spindle (2,294 commits) on the local stack completes in less than 30 s. At this time, it takes 124 s. The clone heads and trees must agree with the source. `make perf` checks this.
- **SC-4** After deploy (owner step), the prod push of spindle completes in less than 60 s and the clone in less than 60 s.

## Clarifications

- 2026-11-15: The owner chose a new factory rig `scion` for this work. The owner also chose one epic for push and clone.
- 2026-11-15: The investigation data comes from ent beads ent-78i.9 and ent-78i.11. The data includes statement logs, the `synchronous_commit` test and cold-clone counts.
- 2026-11-15: The window limits (500 commits, 64 MiB) are defaults. A config value is not needed now.
- 2026-11-16: The owner let Claude choose the rate limit. Decision: the limiter admits each window one time (FR-008). A window is a bounded unit of work (500 commits or 64 MiB). If each commit counts, a large mirror push can fail partway through.

## Open questions

- None.
