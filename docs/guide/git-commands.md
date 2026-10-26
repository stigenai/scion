# git commands in scn

English | [简体中文](git-commands.zh-CN.md)

A quick reference: which git commands you can type in `scn` as they are, which work as transitional spellings, and which are spelled differently. [Coming from git](coming-from-git.md) explains why the models differ.

## Typed the same as in git

| git | scn | Notes |
|---|---|---|
| `git clone URL [DIR] [-b B]` | the same | `URL` is `http(s)://host[:port]/REPO`; `-b`, `--branch` and `--ref` all name the branch. Inside the clone, no command needs the address again |
| `git status [-s] [-b] [--porcelain]` | `scn status` | git's long format; `-s` gives `A ` / `M ` / `D ` (every change is staged) and `UU` for conflicts |
| `git config user.name NAME`, `git config --list`, `--unset` | the same | Only `user.name` and `user.email`; settings are per user (`--global` is accepted) |
| `git commit -m MSG`, `-am MSG` | `scn commit -m MSG`, `-am MSG` | Takes every local change (`-a` is accepted and changes nothing); several `-m` become paragraphs |
| `git commit -m MSG PATH...` | `scn commit -m MSG PATH...` | Only the changes at or under the paths; the rest stay local changes |
| `git commit --amend` | `scn commit --amend` | |
| `git restore [-s REV] PATH`, `git checkout REV -- PATH` | `scn restore [-s REV] PATH` | `--source` takes the files from REV as local changes |
| `git clean -n`, `-f`, `-fd`, `-fdx` [PATH...] | the same | Untracked files; `-x` ignored ones too; nested repositories stay |
| `git stash [push] [-m MSG]`, `list`, `show [-p]`, `pop`, `apply`, `drop` [`stash@{N}`] | the same | Every local change, new files included; `pop` and `apply` need a clean tree |
| `git switch B`, `git switch -c B` | `scn switch B`, `scn switch -c B` | |
| `git switch --detach REV`, `git checkout REV` | `scn switch --detach REV` | On no branch: nothing can be committed there |
| `git merge B`, `git merge --abort` | `scn merge B`, `scn merge --abort` | Then `scn land` to publish |
| `git revert REV`, `-m N`, `--abort` | the same | One commit at a time, as a pending commit; then `scn land` |
| `git cherry-pick REV`, `-x`, `-m N`, `--abort` | the same | One commit at a time; keeps its message and author |
| `git branch [-v] [-a]`, `git branch NAME [REV]`, `git branch -d NAME`, `--show-current` | the same | Branches live on the server, so `-a` lists the same ones |
| `git tag [-l]`, `git tag NAME [REV]`, `git tag -d NAME` | the same | Lightweight tags |
| `git log [REV] [-n N \| -N] [--oneline] [-- PATH]` | the same | |
| `git show [REV] [-- PATH]` | the same | |
| `git diff [--cached]`, `git diff A B`, `A..B`, `A...B` | the same | `--name-only`, `--name-status`, `--stat`, `-- PATH`; with no staging area, `--cached` / `--staged` show the same local changes |
| `git blame [-L N,M] PATH` | `scn blame [-L N,M] PATH` | `scn blame REV:PATH` for a revision |
| `git grep [-n] [-l] [-i] [-F] PATTERN [REV] [-- PATH...]` | the same | Searches the commit (HEAD by default) on the server, not local changes |
| `git reflog [BRANCH]` | `scn reflog [BRANCH] [-n N]` | How the branch moved on the server: created, fast-forward or forced, with times |
| `git remote -v`, `git remote get-url origin`, `git remote set-url origin URL` | the same | One remote, `origin`; `set-url` checks the new URL has the working copy's commits |
| `git archive REV` | `scn archive REV` | `--format tar.gz\|zip\|tar`, `--prefix`, `-o FILE` |
| Revisions: `HEAD`, `@`, `HEAD~1`, `main^2`, `A..B`, 40-digit SHAs | the same | Every commit has a git SHA |
| `.gitignore` | `.gitignore` | `.scionignore` too |

## Transitional spellings

These work too, and print a hint naming the scn command they ran. They are not in `scn --help` and may go away later, so scripts should use the scn names.

| You type | scn runs |
|---|---|
| `scn push` | `scn land` |
| `scn pull`, `scn pull --rebase` | `scn sync` |
| `scn checkout BRANCH`, `scn checkout -b NAME` | `scn switch BRANCH`, `scn switch -c NAME` |
| `scn checkout REV` (no such branch) | `scn switch --detach REV` |
| `scn checkout -- PATH`, `scn checkout PATH` | `scn restore PATH` |
| `scn reset --soft HEAD~1` | `scn uncommit` |
| `scn reset --hard` | `scn restore .` |
| `scn rebase --continue`, `scn merge --continue`, `scn revert --continue`, `scn cherry-pick --continue` | `scn commit` |
| `scn rebase --abort` | `scn sync --abort` |
| `scn add …` | nothing: there is no staging area, and `scn commit` takes every change |
| `scn rm [-r] PATH…`, `scn mv FROM TO` | deletes or moves the files; the next `scn commit` takes it |

## Spelled differently

| git | scn | Why |
|---|---|---|
| `git init` | `scn repo create NAME` or `scn repo create URL` | Repositories live on the server |
| `git push [-u] [origin [BRANCH]]` | `scn land` | The server rebases when the branch moved, and lands exactly once; `--force` is refused (landing never rewrites a branch) |
| `git pull [--rebase \| --ff-only] [origin [BRANCH]]` | `scn sync` | No separate `fetch`: there is no local history |
| `git reset --soft HEAD~1` | `scn uncommit` | |
| `git add` after a conflict | `scn resolve PATH` | `--ours` / `--theirs` to take one side |
| `git show REV:PATH` | `scn cat REV:PATH` | No working copy needed |
| `git ls-tree REV DIR` | `scn ls REV:DIR` | |
| `git sparse-checkout set DIRS` | `scn clone … --sparse DIRS`, `scn sparse set/add/rm/disable` | |
| `git config user.name` / `user.email` | `scn config set user.name` / `user.email` | |
| `git fast-import`, migrating a repository | `scn import git PATH --repo REPO` | Keeps the original SHAs |

Commands that read history also work outside a working copy with `--repo URL`, for example `scn log --repo http://host/org/app`.

## Not available yet

Interactive rebase, `add -p`, `bisect`, `REF@{N}` revisions, annotated tags, `remote add` and `fetch`. `scn init`, `fetch` and `remote add` say what to use instead. The git CLI itself can clone, fetch and push a Scion repository through the [git gateway](git-clients.md).
