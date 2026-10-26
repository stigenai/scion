# Importing from git

English | [简体中文](git-import.zh-CN.md)

Import an existing git repository into Scion with all its history. Each git commit becomes one Scion commit, with author, committer, times, time zones and messages kept as they are.

- [Before you start](#before-you-start)
- [Quick start](#quick-start)
- [Command reference](#command-reference)
- [What is imported](#what-is-imported)
- [Incremental imports](#incremental-imports)
- [Checking the result](#checking-the-result)
- [Limits](#limits)
- [Troubleshooting](#troubleshooting)

## Before you start

1. **git on this machine.** The importer reads the repository through the git command line (`rev-list`, `diff-tree`, `cat-file`); any recent git works.
2. **The target repository on the server.** Create it with `scn repo create mono`, or with `--repo` when starting the server (repeating it is not an error):

   ```sh
   scion server --lite --data-dir ./.scion-data --repo mono
   ```

3. **`scn` installed:** `cargo install --path client/cli`.
4. **`admin` on the target repository when the server requires authentication.** Imported commits carry their original git SHA (the extension field `git.sha1`), which the server takes as the commit's SHA, so only repository admins may write it; other writers get `PERMISSION_DENIED`. Otherwise any writer could point any git SHA at their own commit.

The import writes only through the public protocol, with the same server-side checks as any user's commit, so it works against any Scion server (Lite mode on this machine, or a cluster on PostgreSQL and S3).

## Quick start

```sh
scn import git ~/src/my-repo --repo mono
# Importing commits: 100% (1287/1287), done.
# imported 1287 commits into mono (main at c1a3f09b2c41; 1287 new, 0 already there)
```

Then clone and look around:

```sh
scn clone mono && cd mono
scn ls main:
scn cat main:README.md
```

## Command reference

```text
scn import git <PATH> --repo <NAME> [--git-ref <REF>] [--ref <REF>] [--endpoint <URL>] [--json]
```

| Option | Default | What |
|---|---|---|
| `<PATH>` | — | The git repository (a working tree or a bare repository) |
| `--repo` | — | The target Scion repository, required; or the environment variable `SCION_REPO` |
| `--git-ref` | `main` | The git reference to import: a branch, a tag or a commit SHA |
| `--ref` | `main` | The Scion ref to update after the import |
| `--endpoint` | `http://127.0.0.1:7080` | The server's address; or the environment variable `SCION_ENDPOINT` |
| `--json` | — | JSON output, schema `scion.cli.import/v1` |

`--json` output looks like this:

```json
{"schema":"scion.cli.import/v1","repo":"mono","ref":"main","commits":1287,"skipped_gitlinks":0,"tip":"c1a3f09b2c41…"}
```

Exit codes: `0` success; `1` a general error (not a git repository, a missing git reference, diverged history and so on); `2` a usage error (such as a missing `--repo`); `4` a retryable network error; `5` an authentication failure.

To import several branches, run it once per branch, mapping each with `--git-ref` and `--ref`:

```sh
scn import git ~/src/my-repo --repo mono --git-ref main --ref main
scn import git ~/src/my-repo --repo mono --git-ref release/1.x --ref release/1.x
```

Commits the branches share are not uploaded twice, and get the same Scion commit ids.

**Re-imports and shared history:** commits the server already has are skipped without reading them from git, so importing another branch that shares history with an imported one, re-importing, or importing a tag takes about a second; the summary says how many commits were new. (Importing starhub-server's second branch went from 39 s to 0.5 s.)

Because of this, a re-import does not upgrade commits imported by an older scn. Before 0.1.11, signed commits and commits with unusual headers were imported without their raw git object, so [git clients](git-clients.md) cannot fetch them; to serve such a repository over git, import it again into a new Scion repository.

**Tags:** import a git tag into `tags/NAME` to create a Scion tag at the same commit, for example `scn import git ~/src/my-repo --repo mono --git-ref v1.0 --ref tags/v1.0`. The tag is created in one step (tags never move); importing it again is a no-op, and a tag that already points elsewhere is reported instead of moved. An annotated tag keeps its tag object, byte for byte: its message and tagger show in `scn show v1.0`, and the git gateway serves it with the same SHA.

## What is imported

| git | Scion | Notes |
|---|---|---|
| Commits | Commits | In topological order: parents always before children |
| Parents (several for a merge) | `parents` | In the same order; the first is the first parent |
| Changes against the first parent | `delta` | A root commit's against the empty tree |
| Author / committer | `author` / `committer` | Name, email, time (to the second) and time zone kept |
| Message | `message` | Kept as it is; non-UTF-8 bytes are converted lossily |
| Commit SHA | The extension field `git.sha1` | 20 bytes, to find the git commit of a Scion commit |
| Signatures, `encoding` and other headers | The extension field `git.commit` | The raw commit object, only for commits the fields above cannot rebuild, so [git clients](git-clients.md) get them back with the same SHA |
| — | `change_id` | The first 16 bytes of the git SHA |
| Files `100644` (and the old `100664`) | Files | |
| Executables `100755` | Executables | |
| Symbolic links `120000` | Symbolic links | The target as content |
| Submodules `160000` | — | Skipped, and counted in `skipped_gitlinks` |

**The result is deterministic:** the same git history gives exactly the same Scion commit ids however many times and into whichever server it is imported. File contents already uploaded are not uploaded again, so repeating an import is cheap.

## Incremental imports

When the git repository has new commits, run the same command again:

```sh
scn import git ~/src/my-repo --repo mono
```

The importer reuses the commits already there, imports only the new ones, and fast-forwards the Scion ref from where it is, commit by commit along the first-parent chain, to the latest.

When the git history was rewritten (a `rebase`, `commit --amend` or `reset` and a force push), the Scion ref's commit is no longer in the new history; the import fails with `not in the imported first-parent history` and leaves the ref alone. Import into a new ref then:

```sh
scn import git ~/src/my-repo --repo mono --ref main-rewritten
```

## Checking the result

- Look at files: `scn ls main:src`, `scn cat main:src/lib.rs`.
- Read history by commit: `scn cat <commit id>:path`. Commit ids come from `tip` in the output of `scn import git --json`, or from the `base` field of `scn status --json` in a working copy.
- Against git: each Scion commit's extension field `git.sha1` records its git commit, for programs to look up through the SDK (decode the commit object with `scion_object::Changeset::decode`).

Differential tests check the importer (`client/gitimport/tests/differential.rs`). They build a history with real git, covering renames, deletions, executable bits, symbolic links, binary files, non-ASCII paths, merges, directories and files swapping places, submodules and empty commits, then compare every imported commit's file list, modes, contents, times, time zones and parents with git's, which must match exactly.

## Limits

| Limit | What |
|---|---|
| Submodules | Skipped: submodule pointers are not imported |
| Commit signatures | GPG and SSH signatures (`gpgsig`) are not imported; Scion commit ids depend on content, not signatures |
| Tags | One per import, with `--git-ref <tag> --ref tags/<name>` (see above); annotated tags keep their objects, tags of trees or blobs are refused |
| Renames | Recorded as a deletion and an addition; unchanged contents are not uploaded again |
| File size | Bounded by the server's upload limit, 256 MiB by default; larger files fail the import. Chunked storage will allow larger files in a later version |
| Git LFS | The LFS pointer files themselves are imported; LFS objects are not downloaded |
| File names | Must be valid UTF-8; otherwise the import fails and names the path |
| Speed | One `git diff-tree` and one server request per commit, which suits small and medium repositories; bulk migrations of very large ones (such as for starhub) will write on the server directly |

## Troubleshooting

**`repository "mono" not found`**
The server has no such repository yet. Create it with `scn repo create mono`, or start the server with `--repo mono`.

**`cannot connect to http://127.0.0.1:7080`**
The server is not running, or the address is wrong. Pass it with `--endpoint` or the environment variable `SCION_ENDPOINT`. With an HTTP proxy set (`http_proxy`), set `NO_PROXY=127.0.0.1,localhost` to reach a local server.

**`not a git repository`** or **`git rev-list … failed`**
`<PATH>` is not a git repository, or the reference given by `--git-ref` does not exist. `git -C <PATH> branch -a` lists the branches.

**What if an import fails midway?**
Run it again. Imported commits and uploaded contents are reused, and the Scion ref is updated only after every commit was imported.
