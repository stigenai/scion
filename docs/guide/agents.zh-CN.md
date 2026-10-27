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
| `commit` | 在某个版本之上，用完整的文件内容、要删除的路径和 `edits` 生成提交；加 `land` 时同时合入分支 |
| `edit_file` | 把一个文件里的 `old_string` 替换成 `new_string` 并提交（默认同时合入，`land` 为 false 时不合入） |
| `land` | 把一个提交合入分支 |

- **编辑由服务端完成。** `edit_file` 和 `commit` 的 `edits` 在基础提交里的那个文件上做文本替换：agent 只需要发送要改的文字，不用发送整个文件，本地也不需要有这个文件。不加 `replace_all` 时 `old_string` 必须恰好出现一次，否则调用失败，并说明它出现了几次。SDK 里对应的是 `ChangesetBuilder::edit`。
- **版本**可以是分支名、标签名、提交 id、git SHA 和 `HEAD~N`；不给时就是当前分支。
- **结果**就是同一条 `scn` 命令加 `--json` 时输出的 JSON；调用失败时返回 `scn` 的错误 JSON（`reason`、`retryable`、`hint`），并标记 `isError`。
- **多个 agent 同时工作。** 每个提交都记录它基于哪个提交（`base`，默认是生成提交时分支的最新提交）。合入时，服务端把它 rebase 到其他 agent 在这期间合入的内容之上，agent 不需要拉取任何东西。两个 agent 改了同样的行时，后合入的一方会被拒绝并收到冲突信息，它读取新的最新提交后再试一次即可。

## 工作区：几秒内给每个 agent 一个工作副本

需要磁盘上有文件（用来编译、跑测试）的 agent，可以用 `scn workspace add` 各开一个工作副本，相当于 git 的 `git worktree add`：

```sh
scn workspace add ../agent-1            # 在分支 agent-1 上，在 HEAD 新建
scn workspace add ../agent-2 main       # 在 main 上
scn workspace list
scn workspace remove ../agent-1
```

新工作副本里，当前工作副本已有且没改过的文件都直接复制。在 APFS（macOS）、Btrfs 和 XFS 上这是写时复制的克隆，既不花时间也不占磁盘空间；只有内容不同的文件才从服务端下载。所以它准备好所需的时间，大约就是列出文件的时间。每个工作区都是完整的工作副本，有自己的分支和待合入的提交；它们和其他工作副本一样在服务端合入，所以同一分支上的两个工作区会在服务端互相 rebase。

## 撤销：出错之后回到原样

每个会改变工作副本的命令（commit、restore、clean、sync、switch、merge、stash、land……）执行前，都会先记录工作副本当时的样子，包括本地改动。agent 出错之后，不需要知道是哪个命令造成的，就能回到原样：

```sh
scn undo               # 回到上一个命令之前；再执行一次就是重做
scn op log             # 最近 100 个，从新到旧
scn op restore 12      # 回到第 12 个操作之前
```

回去之后，工作区是当时那个提交的文件加上当时的本地改动，状态（分支、待合入的提交、进行中的冲突）也和当时一样。撤销本身也会被记录。land 已经放到服务端的内容会留在那里，要在分支上撤销请用 `scn revert`。
