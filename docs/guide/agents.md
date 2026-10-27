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
| `commit` | A commit from whole file contents and deletions on top of a revision; with `land`, also on the branch |
| `land` | Puts a commit on a branch |

- **Revisions** are branch and tag names, commit ids, git SHAs and `HEAD~N`; without one, the branch.
- **Results** are the JSON the same `scn` command prints with `--json`; a failed call returns `scn`'s error JSON (`reason`, `retryable`, `hint`) with `isError`.
- **Agents at the same time.** Each commit names the commit it was made on (`base`, by default the branch's tip when it is made). Landing rebases it onto whatever other agents landed meanwhile, on the server, without the agent fetching anything. When two agents changed the same lines, the second land is refused with the conflict, and that agent reads the new tip and tries again.
