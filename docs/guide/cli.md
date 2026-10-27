# A tour of scion and scn

English | [简体中文](cli.zh-CN.md)

Starting a server, then the commands of an everyday workflow. [Coming from git](coming-from-git.md) maps git commands to their scn equivalents; every command has `--help`, and `--json` for scripts.

```sh
# Option 1: docker compose with PostgreSQL + S3-compatible storage (see deploy.md)
docker compose -f deploy/dev/docker-compose.yml --profile server up -d   # anonymous; production: deploy/prd

# Option 2: a server in Lite mode (SQLite + a local directory, persisted), from source
cargo run -p scion -- server --lite --data-dir ./.scion-data

# Other backends: --meta mem|sqlite://PATH|postgres://…  --blobs mem|DIR|s3://bucket/prefix
# S3 settings: SCION_S3_ENDPOINT, SCION_S3_ACCESS_KEY_ID, SCION_S3_SECRET_ACCESS_KEY, SCION_S3_REGION,
# SCION_S3_PROVIDER=auto|minio|seaweedfs|aws|generic (SeaweedFS and MinIO are tested in CI; see object-storage.md)

# In another terminal: install scn and check the connection
cargo install --path client/cli
scn doctor
scn config set user.name "Ada Lovelace"       # default commit author (falls back to git's user.name / user.email)
scn config set user.email ada@example.com
scn repo create mono                # repositories and branches: scn repo / scn branch (see repos.md)

# Everyday workflow
scn clone mono && cd mono          # check out (ref main by default; empty repositories work too)
# or name the server and repository in one URL, as with git; inside the working copy no address is needed:
# scn clone http://127.0.0.1:7080/mono
echo 'hello' > README.md
echo 'target/' > .gitignore         # .gitignore and .scionignore are honored (gitignore syntax)
scn status                          # git's long format, and how far main on the server has moved; `scn status -s`: A  .gitignore, A  README.md
scn commit -m "add readme"          # or --author "Ada <ada@example.com>"
scn restore src/main.rs             # discard local changes to tracked files (`.` for all); added files are kept
scn clean -n                        # list untracked files; -f removes them, -fx ignored ones too
scn stash                           # put local changes aside (scn stash pop brings them back, also after a sync)
scn commit -m "add tests"           # another commit on top: pending commits stack, and `scn land` lands them all
scn commit -m "docs only" docs/     # only the changes under docs/; the others stay local
scn commit --amend                  # fold more local changes into the newest pending commit (keeps message and author)
scn uncommit                        # drop the newest pending commit, keep its changes as local changes
scn land                            # land on main; if main moved, the server rebases (three-way merge)
scn sync                            # pull others' changes; rebases the pending commits one by one (three-way, line level)
scn tag v1.0                        # tag the current commit; `scn tag` lists, `scn log v1.0` uses it
scn tag -a v1.1 -m "Release 1.1"    # an annotated tag, with a message; `scn show v1.1` shows it
scn switch -c feature               # create a branch at the current snapshot and switch to it; `scn switch main` back
scn switch --detach v1.0            # the files of v1.0 on no branch, to build or test; `scn switch main` back
scn merge feature                   # merge a branch as a pending merge commit (three-way, line level); then `scn land`
scn revert c1a2b3c4d5e6             # a pending commit that undoes a landed one (-m 1 for a merge); then `scn land`
scn cherry-pick c1a2b3c4d5e6        # apply another branch's commit here, keeping its message and author (-x notes it)
# On conflicts (scn sync, merge, revert or cherry-pick) files get <<<<<<< / >>>>>>> markers and `scn status` lists them under Unmerged paths (`-s`: UU):
scn resolve src/main.rs             # after editing out the markers; or --ours / --theirs to take one side
scn commit                          # finishes it, reusing the original message; `scn merge --abort` (or revert / cherry-pick --abort) gives up, as `scn sync --abort` does for a sync
scn cat main:README.md              # read files without a working copy (add --repo outside one)
scn ls main:                        # list a directory; every command supports --json
scn log -n 10 -- src                # history from HEAD, newest first, optionally limited to a path (git-style: HEAD~1, A..B)
scn log main                        # the branch on the server, with what others landed since your last sync
scn log --follow -- docs/guide.md   # one file's history across renames, as in git
scn reflog main                     # how the branch moved: created, fast-forward (lands) or forced, when and by whom
scn grep -n TODO -- src             # search the working tree, as git grep; with REV, that commit on the server
scn watch                           # follow the branches on the server: one line per commit as it lands (--json for agents)
scn diff                            # uncommitted local changes (unified diff)
scn diff main                       # what the tip of main changed; `scn diff A B` compares two revisions
scn blame src/lib.rs                # the commit that last changed each line
scn clone mono part --sparse src/   # sparse checkout: only src/ and files at the root; `scn sparse add docs/` later
scn archive -o mono.tar.gz          # download a snapshot as tar.gz / zip / tar (see archive.md)

# scn talks to the server through a local daemon (scn daemon), started on demand; SCION_NO_DAEMON=1 opts out
scn daemon status

# A regional cache in front of one or more servers; advertise it with scion server --read-endpoint
# (see cache.md)
cargo run -p scion -- cache --upstream http://127.0.0.1:7080 --listen 127.0.0.1:7090 --warm mono:main

# Import an existing git repository with its history, authors and timestamps
# (see git-import.md)
scn import git ~/src/my-repo --repo mono

# After upgrading a server that already has data, before starting it: repair lineage and backfill derived indexes (idempotent)
cargo run -p scion -- admin reindex --lite --data-dir ./.scion-data
```

## Server addresses

A repository can be named in two ways:

- **A URL, as with git:** `http(s)://host[:port]/REPO`, such as `scn clone http://scion.example.com:7080/org/app`. It works wherever a repository is named: `clone`, `--repo` (`scn cat README.md --repo http://host/org/app`), and `repo create / info / set / rm / restore / fork / ls`. Commands about the server alone take its address: `scn login URL`, `scn logout URL`, `scn whoami URL`, `scn doctor URL`.
- **A name and a server:** `scn --endpoint http://host:7080 clone org/app`, or `SCION_ENDPOINT` set once.

A working copy remembers the server it was cloned from, so inside it `status`, `commit`, `land`, `sync`, `log` and the rest need no address. Where several are given, the address in a URL comes first, then `--endpoint` / `SCION_ENDPOINT`, then the working copy's, then `http://127.0.0.1:7080`.

When the server moves (a new IP or host name), point the working copy at it with `scn remote set-url http://NEW-HOST:7080/REPO`; `scn remote -v` shows the current one. The new address must have the commits the working copy is on, so a fork or a migrated copy of the repository works too.

## Output

Text output follows git: a command prints what git prints for the same step, and a command git keeps quiet prints nothing. Progress and notes such as `Cloning into 'mono'...` go to standard error. When standard error is a terminal, `clone`, `import git` and `archive` show git-style progress (`Importing commits:  45% (2852/6338)` for an import, `Receiving archive: 12.40 MiB | 8.10 MiB/s` for an archive):

```text
Cloning into 'mono'...
Listing files: 18342, done.
Receiving objects: 100% (18342/18342), 212.40 MiB | 38.12 MiB/s, done.
```

`-q` / `--quiet` drops progress and notes, and, as git's `-q`, the report of what a command did (`[main c1…] subject` after a commit, `Fast-forward` after a sync); what a command shows (`log`, `status`, `diff`, lists) and errors are still printed. `--json` prints one JSON object with a `schema` field instead, and no progress.

## What a checkout will not write

A repository's paths come from the server, so a hostile repository must not be able to change anything outside the working copy. `scn` refuses to write or remove, and stops with `refusing to write <path>`:

- paths with a `.scion` or `.git` component, in any case (`.SCION`, `.Git`): the working copy's own metadata, and git's;
- anything but plain relative paths (no `..`, empty components or backslashes);
- paths that lead through a symbolic link, such as a link left by an earlier version where the new version has a directory, or a link `A` next to `a/...` on a case-insensitive file system (macOS, Windows).

Symbolic links themselves are checked out as links, as git does; only writes through them are refused.
