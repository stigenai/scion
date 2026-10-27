# Agents

English | [简体中文](agents.zh-CN.md)

Scion is built for agents that change code: many of them at once, each short-lived, often needing only part of a repository. This page is about the tools for them.

## MCP: the repository without a checkout

`scn mcp` serves a repository to an agent over the [Model Context Protocol](https://modelcontextprotocol.io) on standard input and output. The agent reads, searches and walks history at any revision on the server, and makes commits there from file contents, without cloning or a working tree.

```sh
# Claude Code
claude mcp add scion -- scn mcp --repo http://scion.example.com:7080/org/app
```

Inside a working copy, `scn mcp` serves that working copy's repository and lands on its branch; elsewhere, `--repo` names the repository. It signs in as `scn` does (`scn login`, or `SCION_TOKEN`), and commits carry the author `scn` is configured with (`SCION_AUTHOR` or `user.name` / `user.email`).

| Tool | What it does |
|---|---|
| `read_file` | A file's content at a revision (cut at 256 KiB, with `truncated`) |
| `list_dir` | A directory's entries |
| `grep` | Lines matching a regular expression, optionally under some paths |
| `log` | Commits newest first, optionally for one path (`follow` across renames) |
| `blame` | The commit that last changed each line |
| `diff` | The changes between two revisions, with patches |
| `show` | A commit's message, author and changes |
| `commit` | A commit on top of a revision from whole file contents, deletions and `edits`; with `land`, also on the branch |
| `edit_file` | Replaces `old_string` with `new_string` in one file and commits it (landed unless `land` is false) |
| `land` | Puts a commit on a branch; with `dry_run`, only says what it would do (fast-forward, rebase or the conflicts) |

- **Edits are applied by the server.** `edit_file` and `commit`'s `edits` replace text in the file as the base commit has it: the agent sends only the text it changes, not the file, and needs no copy of it. `old_string` must appear exactly once unless `replace_all`; otherwise the call fails and says how many times it appears. The SDK has the same as `ChangesetBuilder::edit`.
- **Revisions** are branch and tag names, commit ids, git SHAs and `HEAD~N`; without one, the branch.
- **Results** are the JSON the same `scn` command prints with `--json`; a failed call returns `scn`'s error JSON (`reason`, `retryable`, `hint`) with `isError`.
- **Agents at the same time.** Each commit names the commit it was made on (`base`, by default the branch's tip when it is made). Landing rebases it onto whatever other agents landed meanwhile, on the server, without the agent fetching anything. When two agents changed the same lines, the second land is refused with the conflict, and that agent reads the new tip and tries again.

## Workspaces: a working copy per agent in seconds

Agents that need files on disk (to build and run tests) each get their own working copy with `scn workspace add`, git's `git worktree add`:

```sh
scn workspace add ../agent-1            # on branch agent-1, made at HEAD
scn workspace add ../agent-2 main       # on main
scn workspace list
scn workspace remove ../agent-1
```

The new working copy takes every file this one has unchanged by copying it, which on APFS (macOS), Btrfs and XFS is a copy-on-write clone that costs neither time nor disk space, and fetches only the files that differ. It is ready in about the time it takes to list the files. Each is a full working copy with its own branch and pending commits; they land on the server like any other, so two of them on one branch are rebased onto each other there.

## Undo: go back after a mistake

Every command that changes a working copy (commit, restore, clean, sync, switch, merge, stash, land…) is logged first with what the working copy was, local changes included. An agent that went wrong goes back without knowing which command did it:

```sh
scn undo               # before the last command; again: redo
scn op log             # the last 100, newest first
scn op restore 12      # before operation 12
```

Going back makes the working tree that commit's files plus the local changes it had, and the state (branch, pending commits, a conflict in progress) what it was. Undo is itself logged. What a land put on the server stays there; `scn revert` undoes it on the branch.

## Marking an agent's commits

`SCION_TRAILERS` adds git trailers to every commit `scn` makes in that environment, the MCP server's included, and `scn commit --trailer` (or the commit tool's `trailers`) adds them one commit at a time. Give each agent run its own, and its commits can be found, reviewed or reverted together later:

```sh
export SCION_TRAILERS='Agent: claude; Agent-Session: 2026-10-27-a'
scn log --grep 'Agent-Session: 2026-10-27-a' --oneline
scn log --author 'bot@agents' -n 50
```

`--grep` and `--author` filter on the server, so they work on the branch's whole history without a checkout; the MCP log tool takes `grep` and `author` too.
