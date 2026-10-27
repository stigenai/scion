# Agent

[English](agents.md) | 简体中文

Scion 是为修改代码的 agent 设计的：它们往往同时有很多个，每个存在的时间都不长，通常也只需要仓库的一部分。这一页介绍面向 agent 的工具。

## MCP：不用检出就能用仓库

`scn mcp` 通过标准输入输出，用 [Model Context Protocol](https://modelcontextprotocol.io) 把一个仓库提供给 agent。agent 可以在服务端读取任意版本、搜索、查看历史，也可以直接用文件内容在服务端生成提交，不需要 clone，也不需要工作区。

```sh
# Claude Code
claude mcp add scion -- scn mcp --repo http://scion.example.com:7080/org/app
```

在工作副本里运行时，`scn mcp` 提供这个工作副本的仓库，并合入到它所在的分支；在别处运行时用 `--repo` 指定仓库。登录方式和 `scn` 相同（`scn login` 或 `SCION_TOKEN`），提交的作者是 `scn` 配置的作者（`SCION_AUTHOR` 或 `user.name` / `user.email`）。

| 工具 | 作用 |
|---|---|
| `read_file` | 某个版本中一个文件的内容（超过 256 KiB 的部分截掉，并标记 `truncated`） |
| `list_dir` | 一个目录的条目 |
| `grep` | 匹配正则表达式的行，可以限定在某些路径下 |
| `log` | 从新到旧的提交，可以只看某个路径（`follow` 跨越重命名） |
| `blame` | 每一行最后被改动的提交 |
| `diff` | 两个版本之间的改动，附带补丁 |
| `show` | 一个提交的说明、作者和改动 |
| `commit` | 在某个版本之上，用完整的文件内容和要删除的路径生成提交；加 `land` 时同时合入分支 |
| `land` | 把一个提交合入分支 |

- **版本**可以是分支名、标签名、提交 id、git SHA 和 `HEAD~N`；不给时就是当前分支。
- **结果**就是同一条 `scn` 命令加 `--json` 时输出的 JSON；调用失败时返回 `scn` 的错误 JSON（`reason`、`retryable`、`hint`），并标记 `isError`。
- **多个 agent 同时工作。** 每个提交都记录它基于哪个提交（`base`，默认是生成提交时分支的最新提交）。合入时，服务端把它 rebase 到其他 agent 在这期间合入的内容之上，agent 不需要拉取任何东西。两个 agent 改了同样的行时，后合入的一方会被拒绝并收到冲突信息，它读取新的最新提交后再试一次即可。
