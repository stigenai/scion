# 从 git 迁移

[English](coming-from-git.md) | 简体中文

`scn` 的参数沿用 git 的写法（`HEAD~1`、`A..B`、`-- PATH`、`-am`、`branch -d`），只有 Scion 的模型确实和 git 不同的地方才用自己的命令名。本文列出两者的对应关系；简明速查见 [scn 中的 git 命令](git-commands.zh-CN.md)。

## 模型上的不同

- **没有暂存区。** `scn commit` 提交所有本地改动；带路径时（`scn commit -m MSG src/`）只提交这些路径里的改动。新增、删除和改名的文件通过扫描工作区发现，没有 `add`、`rm`、`mv`。
- **没有本地历史。** 工作副本只有一份快照。历史在服务端，所以 `log`、`blame`、`show` 和 `diff REV` 都是服务端查询，`cat`、`ls` 和 `archive` 不需要工作副本。
- **提交不等于发布。**
  - `scn commit` 在服务端创建一个提交，它还不在任何分支上。
  - `scn land` 发布它，相当于 `git push`。分支已经前进时，服务端会 rebase 这个提交，并且只合入一次。
- **待合入的提交可以叠起来。** 用 `scn commit` 做出、还没合入的提交一个接一个叠在一起，`scn land` 按顺序全部合入。没有本地分支：分支都在服务端（`scn switch -c NAME`）。
- **只有一个 remote。** 一个工作副本只属于一个服务端的一个仓库，即它的 `origin`；`scn remote set-url` 可以换成新地址。

## 命令对照

| git | scn | 说明 |
|---|---|---|
| `git init` | `scn repo create NAME` | 仓库在服务端 |
| `git clone URL [DIR]` | `scn clone URL [DIR] [-b B]`，或 `scn --endpoint SERVER clone REPO` | `URL` 为 `http(s)://host[:port]/REPO`。下载快照，不下载历史；进入工作副本后命令不用再写地址 |
| `git clone --sparse` + `git sparse-checkout set` | `scn clone REPO --sparse src --sparse docs`，`scn sparse set/add/rm/disable` | 按目录；也可以写 `--sparse a,b` |
| `git status` | `scn status` | git 的长格式；`-s` 显示 `A ` / `M ` / `D `（所有改动都算已暂存），冲突是 `UU`。和 git 对比 `origin/main` 一样，会说明服务端的分支比工作副本多几个提交（`-sb`：`## main [ahead 1, behind 2]`）；这要问一次服务端，`--no-ahead-behind` 可以跳过 |
| `git add`、`git rm`、`git mv` | 不需要 | 直接改文件 |
| `git commit -m MSG`、`-am MSG` | `scn commit -m MSG`、`-am MSG` | 接受 `-a`，但没有作用。作者依次取 `--author`、`scn config`、git 的 `user.name` / `user.email` |
| `git commit -m MSG PATH...` | `scn commit -m MSG PATH...` | 只提交这些路径（相对当前目录）及其下的改动，其余仍是本地改动。每个路径都要有改动；不能和 `-a`、`--amend` 一起用，完成冲突时也不能用 |
| `git commit --amend` | `scn commit --amend` | 只修改最新的一个待合入提交 |
| `git reset --soft HEAD~1` | `scn uncommit` | 撤掉最新的一个待合入提交，保留它的改动 |
| `git restore [-s REV] PATH`、`git checkout [REV] -- PATH` | `scn restore [-s REV] PATH` | `.` 表示全部；新增的文件会保留（没有 `git clean`）；`--source REV` 从 REV 取文件，成为本地改动 |
| `git remote -v`、`git remote set-url origin URL` | `scn remote -v`、`scn remote set-url URL` | 服务端的新地址，或有相同提交的另一个仓库（如 fork）；保存前会先检查 |
| `git push` | `scn land` | 分支已经前进时由服务端 rebase |
| `git pull --rebase` | `scn sync` | 移到分支最新提交，并把待合入的提交逐个 rebase；没有单独的 `fetch` |
| `git rebase --abort` | `scn sync --abort` | 放弃停在冲突上的 sync：工作区回到 sync 之前所在的提交和待合入提交 |
| `git fetch` + `git log origin/main` | `scn log main`（或 `origin/main`）、`scn diff HEAD main` | 命令直接读服务端，不需要 fetch；工作副本保持不变 |
| `git branch` | `scn branch`（或 `scn branch ls`） | |
| `git branch NAME [REV]` | `scn branch NAME [REV]`（或 `scn branch create`） | 分支是服务端的 ref。名为 `ls`、`create` 或 `rm` 的分支要用 `create` 的写法 |
| `git branch -d NAME` / `-D` | `scn branch -d NAME` / `-D`（或 `scn branch rm`） | |
| `git tag`、`git tag NAME [REV]`、`git tag -d NAME` | `scn tag`、`scn tag NAME [REV]`、`scn tag -d NAME` | 只有轻量标签（没有说明）。存成服务端永远不会移动的 ref `tags/NAME`。标签名可以当版本号用（`scn log v1.0`），除非有同名分支 |
| `git switch B`、`git switch -c B` | `scn switch B`、`scn switch -c B` | 有待合入的提交或改动过的文件时拒绝；新增的文件会带过去 |
| `git merge B` | `scn merge B` | 生成一个待合入的合并提交；`scn land` 发布它 |
| `git merge --abort` | `scn merge --abort` | |
| `git revert REV` | `scn revert REV` | 生成一个撤销 REV 的待合入提交（`Revert "…"`、`This reverts commit …`）；之后对同一行的改动会保留，有重叠时和合并一样产生冲突。合并提交需要 `-m 1`。`scn revert --abort` 放弃 |
| `git cherry-pick REV` | `scn cherry-pick REV` | 把 REV 的改动应用到当前分支，生成待合入的提交，保留原提交说明和作者（`--author` 改作者，`-x` 注明原提交）；有重叠时和合并一样产生冲突。`scn cherry-pick --abort` 放弃 |
| 解决冲突后的 `git add` | `scn resolve PATH`，`--ours` / `--theirs` | 还有冲突标记时拒绝，除非加 `--force`；不带路径时列出冲突 |
| `git rebase --continue`、`git merge --continue`、`git revert --continue`、`git cherry-pick --continue` | `scn commit` | `-m` 可以省略，沿用原来的提交说明 |
| `git log [REV] [-n N] [--oneline] [-- PATH]` | `scn log [REV] [-n N] [--oneline] [-- PATH]` | 与 git 格式相同（默认 20 个提交）；也可以用 `--path PATH`。和 git 一样从工作副本当前的提交（HEAD）开始；`scn log main` 看服务端的分支，包括上次 `scn sync` 之后别人合入的提交。服务端 rebase 过的提交还会显示 `CommitDate:` 和 `Rebased-from:`（提交原来的 id）；提交时间按合入顺序排列，作者时间是写下改动的时间 |
| `git show [REV] [-- PATH]` | `scn show [REV] [--name-only | --name-status | --stat] [-- PATH]` | |
| `git diff`、`git diff A B`、`A..B`、`A...B` | `scn diff`、`scn diff A B`、`A..B`、`A...B` | `--name-only`、`--name-status`、`--stat`、`-- PATH` |
| `git blame [REV] -- PATH` | `scn blame [REV:]PATH` | |
| `git show REV:PATH` | `scn cat REV:PATH` | 不需要工作副本 |
| `git ls-tree REV DIR` | `scn ls REV:DIR`，根目录用 `scn ls REV` | 不是版本号的单个参数按目录处理；`:DIR` 强制当作目录 |
| `git archive REV` | `scn archive REV`，`scn archive [REV:][DIR] --format tar.gz\|zip\|tar [--prefix P] [-o FILE]` | |
| `git config user.name` / `user.email` | `scn config set user.name` / `user.email` | 目前只有这两项设置 |
| `.gitignore` | `.gitignore` 和 `.scionignore` | 语法相同；同一目录下以 `.scionignore` 为准 |
| `git fast-import` | `scn import git PATH --repo R` | 保留原始的 git SHA |

## 过渡用的 git 写法

为了方便迁移，下面这些 git 命令也能用。每个都会执行右边的 scn 命令，并在 stderr 上提示对应的写法，例如：

```
hint: `scn push` is a transitional git-style spelling; use `scn land` instead
```

它们不出现在 `scn --help` 里，以后可能会去掉，脚本里请用 scn 的命令名。

| 你输入的 | scn 执行的 |
|---|---|
| `scn push` | `scn land` |
| `scn pull [--rebase]` | `scn sync` |
| `scn checkout BRANCH`、`scn checkout -b NAME` | `scn switch BRANCH`、`scn switch -c NAME` |
| `scn checkout -- PATH`、`scn checkout PATH`（已存在的路径） | `scn restore PATH` |
| `scn reset --soft [HEAD~1]` | `scn uncommit` |
| `scn reset --hard [HEAD]` | `scn restore .` |
| `scn rebase --continue`、`scn merge --continue`、`scn revert --continue`、`scn cherry-pick --continue` | `scn commit` |
| `scn rebase --abort` | `scn sync --abort` |
| `scn add ...` | 什么也不做：没有暂存区 |
| `scn rm [-r] PATH...`、`scn mv FROM TO` | 删除 / 移动文件；下一次 `scn commit` 会带上 |

其它写法，例如 `reset --hard HEAD~3` 或不带参数的 `rebase`，会报错并说明 scn 里的做法。

## 版本号

| 写法 | 含义 |
|---|---|
| `main`、`feature/x` | 分支 |
| `v1.0`、`tags/v1.0` | 标签 |
| `c1…` | Scion 提交 id |
| 40 位十六进制 | git SHA。每个提交都有一个，见 [git-sha.zh-CN.md](git-sha.zh-CN.md) |
| `HEAD`、`@` | 工作副本的当前提交：有待合入的提交时是它，否则是同步到的提交 |
| `REV~N` | 沿第一父提交往上第 N 个祖先（单独的 `~` 表示 `~1`） |
| `REV^`、`REV^N` | 第一个或第 N 个父提交（`^0` 是提交本身） |

后缀可以像 git 一样组合，例如 `main~2^2`。分支名本身含有 `~` 或 `^` 时，优先按分支名解析。

## 尚未支持

- **工作副本**：检出旧版本、`stash`、`clean`、交互式 rebase、只提交文件的一部分（`add -p`）。
- **查询**：附注标签、`grep`、`bisect`、`reflog`。
- **协作**：`remote add`、`fetch`。git 命令行本身可以克隆、拉取和推送 Scion 仓库（见 [git 客户端](git-clients.zh-CN.md)）。
