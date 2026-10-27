# Coming from git

English | [简体中文](coming-from-git.zh-CN.md)

`scn` keeps git's spelling for arguments (`HEAD~1`, `A..B`, `-- PATH`, `-am`, `branch -d`). It uses its own command names only where Scion's model really differs from git's. This page maps one to the other; [git commands in scn](git-commands.md) is the short version.

## What is different in the model

- **No staging area.** `scn commit` takes every local change, or with paths (`scn commit -m MSG src/`) the changes in them. New, deleted and renamed files are found by scanning the working tree; there is no `add`, `rm` or `mv`.
- **No local history.** A working copy holds one snapshot. History lives on the server, so `log`, `blame`, `show` and `diff REV` are server queries, and `cat`, `ls` and `archive` work without a working copy.
- **Committing is not publishing.**
  - `scn commit` creates a commit on the server that is not on any branch yet.
  - `scn land` publishes it, like `git push`. If the branch moved, the server rebases the commit, and it lands exactly once.
- **Pending commits stack.** Commits made with `scn commit` and not landed yet pile up on one another, and `scn land` lands them all, in order. There are no local branches: a branch lives on the server (`scn switch -c NAME`).
- **One remote.** A working copy belongs to one server and one repository, its `origin`; `scn remote set-url` points it at a new address.

## Command map

| git | scn | Notes |
|---|---|---|
| `git init` | `scn repo create NAME` | Repositories live on the server |
| `git clone URL [DIR]` | `scn clone URL [DIR] [-b B]`, or `scn --endpoint SERVER clone REPO` | `URL` is `http(s)://host[:port]/REPO`. Downloads the snapshot, not the history; inside the working copy, commands need no address |
| `git clone --sparse` + `git sparse-checkout set` | `scn clone REPO --sparse src --sparse docs`, `scn sparse set/add/rm/disable` | Directories; `--sparse a,b` works too |
| `git worktree add PATH [BRANCH]`, `-b NEW`, `--detach`, `list`, `remove` | `scn workspace add PATH [BRANCH]`, `-b NEW`, `--detach`, `list`, `remove` | As in git, without a branch it is the branch named after PATH, made at HEAD if missing. The new working copy takes the files this one has unchanged by copying them (a copy-on-write clone on APFS, Btrfs and XFS), and fetches only the rest; it has the same sparse directories. Any of them lists the set; `remove` refuses one with local changes or pending commits unless `--force` |
| `git status` | `scn status` | git's long format; `-s` gives `A ` / `M ` / `D ` (every change is staged) and `UU` for conflicts. Like git against `origin/main`, it says how many commits the branch on the server has that the working copy lacks (`-sb`: `## main [ahead 1, behind 2]`); that asks the server, which `--no-ahead-behind` skips |
| `git add`, `git rm`, `git mv` | not needed | Just change the files |
| `git commit -m MSG`, `-am MSG` | `scn commit -m MSG`, `-am MSG` | `-a` is accepted and changes nothing. The author comes from `--author`, then `scn config`, then git's `user.name` / `user.email` |
| `git commit -m MSG PATH...` | `scn commit -m MSG PATH...` | Only the changes at or under the paths (relative to the current directory); the rest stay local changes. Every path needs a change. Not with `-a` or `--amend`, nor when finishing a conflict |
| `git commit --amend` | `scn commit --amend` | Only the newest pending commit |
| `git reset --soft HEAD~1` | `scn uncommit` | Drops the newest pending commit and keeps its changes |
| `git restore [-s REV] PATH`, `git checkout [REV] -- PATH` | `scn restore [-s REV] PATH` | `.` for everything; added files are kept (`scn clean` removes them); `--source REV` takes the files from REV as local changes |
| `git clean -n` / `-f` / `-fd` / `-fdx` [PATH...] | `scn clean -n` / `-f` / `-fd` / `-fdx` [PATH...] | Removes untracked files, and with `-x` ignored ones too (build output); needs `-f`, or `-n` to only list them. Untracked directories go whole (`-d` is accepted and changes nothing); `.scion/`, `.git/` and nested repositories or working copies stay |
| `git stash [push] [-m MSG]`, `list`, `show [-p]`, `pop`, `apply`, `drop` [`stash@{N}`] | the same | Saves every local change, new files included (git leaves those without `-u`), as a commit on the server on no branch, and puts the tree back to its commit. `pop` / `apply` merge an entry back three-way, so it also applies after `scn sync` or on another branch; they need a clean tree, and on conflicts the files get markers and the entry is kept. The list belongs to the working copy |
| `git remote -v`, `git remote set-url origin URL` | `scn remote -v`, `scn remote set-url URL` | The server's new address, or another repository with the same commits (a fork); checked before it is saved |
| `git push` | `scn land` | The server rebases when the branch moved |
| `git pull --rebase` | `scn sync` | Moves to the branch tip and rebases the pending commits one by one; no separate `fetch` |
| `git rebase --abort` | `scn sync --abort` | Gives up a sync that stopped on conflicts: the working copy goes back to its commit and pending commits from before the sync |
| `git fetch` + `git log origin/main` | `scn log main` (or `origin/main`), `scn diff HEAD main` | Commands read the server, so there is nothing to fetch; the working copy stays as it is |
| `git branch` | `scn branch` (or `scn branch ls`) | |
| `git branch NAME [REV]` | `scn branch NAME [REV]` (or `scn branch create`) | Branches are server refs. A branch named `ls`, `create` or `rm` needs the `create` form |
| `git branch -d NAME` / `-D` | `scn branch -d NAME` / `-D` (or `scn branch rm`) | |
| `git tag`, `git tag NAME [REV]`, `git tag -a NAME -m MSG [REV]`, `git tag -n`, `git tag -d NAME` | the same | Stored as refs `tags/NAME` that the server never moves. `-m` (with or without `-a`) makes an annotated tag: a message, and the configured author as the tagger; `scn show NAME` shows them, `scn tag -n` lists the first lines. No signed tags (`-s`). A tag name works as a revision (`scn log v1.0`) unless a branch has the same name |
| `git switch B`, `git switch -c B` | `scn switch B`, `scn switch -c B` | Refuses with a pending commit or modified files; added files are carried over |
| `git switch --detach REV`, `git checkout REV` | `scn switch --detach REV` | The files of any revision (`v1.0`, `main~3`, an id) to look at, build or test, on no branch: `scn status` says `HEAD detached at …`, and commit, land, sync, merge, revert and cherry-pick refuse. `scn switch BRANCH` goes back; `scn switch -c NAME` starts a branch there |
| `git merge B` | `scn merge B` | Makes a pending merge commit; `scn land` publishes it |
| `git merge --abort` | `scn merge --abort` | |
| `git revert REV` | `scn revert REV` | Makes a pending commit that undoes REV (`Revert "…"`, `This reverts commit …`); later changes to the same lines are kept, overlaps conflict as in a merge. A merge commit needs `-m 1`. `scn revert --abort` gives up |
| `git cherry-pick REV` | `scn cherry-pick REV` | Applies REV's changes here as a pending commit with its message and author (`--author` to change it, `-x` to note the original); overlaps conflict as in a merge. `scn cherry-pick --abort` gives up |
| `git add` after fixing a conflict | `scn resolve PATH`, `--ours` / `--theirs` | Refuses leftover markers unless `--force`; no paths lists conflicts |
| `git rebase --continue`, `git merge --continue`, `git revert --continue`, `git cherry-pick --continue` | `scn commit` | `-m` is optional; it keeps the original message |
| `git log [REV] [-n N] [--oneline] [--follow] [-- PATH]` | `scn log [REV] [-n N] [--oneline] [--follow] [-- PATH]` | git's format (default 20 commits); `--path PATH` also works. `--follow` continues a file's history across renames and copies, as git does: the server finds the file it came from, a deleted one with the same content or at least 50% similar (git's default `-M50%`), else one it was copied from; it lists no merges, as git does, and stops where you cannot read the old path. With a path, a merge that took the file as it was on one of the merged branches is left out, as in git (that branch's commits show the change); commits come newest generation first rather than by commit date. As in git, it starts at the working copy's commit (HEAD); `scn log main` shows the branch on the server, including what others landed since your last `scn sync`. A commit the server rebased also shows `CommitDate:` and `Rebased-from:`, the commit as it was made; commit dates follow the order commits landed in, author dates when they were written |
| `git show [REV] [-- PATH]` | `scn show [REV] [--name-only | --name-status | --stat] [-- PATH]` | |
| `git diff`, `git diff A B`, `A..B`, `A...B` | `scn diff`, `scn diff A B`, `A..B`, `A...B` | `--name-only`, `--name-status`, `--stat`, `-- PATH` |
| `git blame [REV] -- PATH` | `scn blame [REV:]PATH` | A bare `PATH` is relative to the current directory, as for `log`, `show` and `diff`; `REV:PATH` starts at the root, as in git. Lines a merge brought in keep the blame of the branch they came from, and a renamed file keeps the blame it had under its old name, as in git (copies are not followed, as without `-C`) |
| `git grep [-n] [-l] [-i] [-F] PATTERN [REV] [-- PATH...]` | the same, plus `-m N` | Without REV in a working copy it searches the working tree, as git does: HEAD on the server (so it also covers what a sparse checkout leaves out), with files changed locally searched on your machine in their place, including new ones; deleted files are not searched. With REV, or outside a working copy, it runs on the server against that commit and needs no checkout. Rust regex syntax (close to `-E`). Binary files, files over 1 MiB, links and redacted files are left out; it stops after 1000 matching lines unless `-m` says otherwise (at most 10000). Exits 1 when nothing matches |
| `git reflog [BRANCH]` | `scn reflog [BRANCH] [-n N]` | The server keeps it, for every branch and tag, so it covers everyone's lands, not one clone's: `ID main@{N}: TIME created / fast-forward / forced by WHO: SUBJECT`, newest first. WHO is the account whose request moved it; when the server landed several requests in one move it names them all (`by alice, bob`). Moves from before servers recorded this name no one. Deleting a branch deletes its log |
| `git show REV:PATH` | `scn cat REV:PATH` | No working copy needed |
| `git ls-tree REV DIR` | `scn ls REV:DIR`, or `scn ls REV` for the root | A bare argument that is not a revision is a directory; `:DIR` forces a directory |
| `git archive REV` | `scn archive REV`, `scn archive [REV:][DIR] --format tar.gz\|zip\|tar [--prefix P] [-o FILE]` | |
| `git config user.name` / `user.email` | `scn config set user.name` / `user.email` | The only settings so far |
| `.gitignore` | `.gitignore` and `.scionignore` | Same syntax; `.scionignore` wins in the same directory |
| `git fast-import` | `scn import git PATH --repo R` | Keeps the original git SHAs |

## Transitional git spellings

To ease the move, these git commands also work. Each runs the scn command on the right and prints a hint on stderr naming it, for example:

```
hint: `scn push` is a transitional git-style spelling; use `scn land` instead
```

They are not listed in `scn --help` and may be removed later, so scripts should use the scn names.

| You type | scn runs |
|---|---|
| `scn push` | `scn land` |
| `scn pull [--rebase]` | `scn sync` |
| `scn checkout BRANCH`, `scn checkout -b NAME` | `scn switch BRANCH`, `scn switch -c NAME` |
| `scn checkout REV` (no such branch) | `scn switch --detach REV` |
| `scn checkout -- PATH`, `scn checkout PATH` (an existing path) | `scn restore PATH` |
| `scn reset --soft [HEAD~1]` | `scn uncommit` |
| `scn reset --hard [HEAD]` | `scn restore .` |
| `scn rebase --continue`, `scn merge --continue`, `scn revert --continue`, `scn cherry-pick --continue` | `scn commit` |
| `scn rebase --abort` | `scn sync --abort` |
| `scn add ...` | nothing: there is no staging area |
| `scn rm [-r] PATH...`, `scn mv FROM TO` | deletes / moves the files; the next `scn commit` picks it up |

Other forms, like `reset --hard HEAD~3` or a bare `rebase`, fail with a message that explains the scn way.

## Revisions

| Form | Meaning |
|---|---|
| `main`, `feature/x` | A branch |
| `v1.0`, `tags/v1.0` | A tag |
| `c1…` | A Scion commit id |
| 40 hex digits | A git SHA. Every commit has one; see [git-sha.md](git-sha.md) |
| `HEAD`, `@` | The working copy's current commit: the pending one if any, else the synced one |
| `REV~N` | The N-th first-parent ancestor (`~` alone means `~1`) |
| `REV^`, `REV^N` | The first or N-th parent (`^0` is the commit itself) |

Suffixes combine as in git, for example `main~2^2`. A branch whose name literally contains `~` or `^` takes precedence over the suffix reading.

## Not available yet

- **Working copy**: commits on a detached working copy, interactive rebase, committing part of a file (`add -p`).
- **Queries**: `bisect`, `REF@{N}` revisions (use the id `scn reflog` prints).
- **Collaboration**: `remote add`, `fetch`. The git CLI itself can clone, fetch and push a Scion repository (see [git clients](git-clients.md)).
