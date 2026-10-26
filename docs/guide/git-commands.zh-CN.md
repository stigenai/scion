# scn 中的 git 命令

[English](git-commands.md) | 简体中文

速查表：哪些 git 命令在 `scn` 里可以照原样输入，哪些作为过渡写法也能用，哪些写法不同。两者模型上的区别见[从 git 迁移](coming-from-git.zh-CN.md)。

## 与 git 写法相同

| git | scn | 说明 |
|---|---|---|
| `git clone URL [DIR] [-b B]` | 相同 | `URL` 为 `http(s)://host[:port]/REPO`；`-b`、`--branch`、`--ref` 都可以指定分支。进入克隆目录后，所有命令都不用再写地址 |
| `git status [-s] [-b] [--porcelain]` | `scn status` | git 的长格式；`-s` 显示 `A ` / `M ` / `D `（所有改动都算已暂存），冲突是 `UU` |
| `git config user.name NAME`、`git config --list`、`--unset` | 相同 | 只有 `user.name` 和 `user.email`；设置按用户保存（接受 `--global`） |
| `git commit -m MSG`、`-am MSG` | `scn commit -m MSG`、`-am MSG` | 提交所有本地改动（接受 `-a`，但没有作用）；多个 `-m` 成为多段说明 |
| `git commit -m MSG PATH...` | `scn commit -m MSG PATH...` | 只提交这些路径（及其下）的改动，其余仍是本地改动 |
| `git commit --amend` | `scn commit --amend` | |
| `git restore [-s REV] PATH`、`git checkout REV -- PATH` | `scn restore [-s REV] PATH` | `--source` 从 REV 取文件，成为本地改动 |
| `git clean -n`、`-f`、`-fd`、`-fdx` [PATH...] | 相同 | 删除未跟踪的文件；`-x` 连被忽略的文件一起删；嵌套的仓库不动 |
| `git stash [push] [-m MSG]`、`list`、`show [-p]`、`pop`、`apply`、`drop` [`stash@{N}`] | 相同 | 包括新增文件在内的所有本地改动；`pop` 和 `apply` 要求工作区干净 |
| `git switch B`、`git switch -c B` | `scn switch B`、`scn switch -c B` | |
| `git switch --detach REV`、`git checkout REV` | `scn switch --detach REV` | 不在任何分支上：不能在这里提交 |
| `git merge B`、`git merge --abort` | `scn merge B`、`scn merge --abort` | 之后用 `scn land` 发布 |
| `git revert REV`、`-m N`、`--abort` | 相同 | 一次撤销一个提交，生成待合入的提交；之后用 `scn land` 发布 |
| `git cherry-pick REV`、`-x`、`-m N`、`--abort` | 相同 | 一次一个提交；保留原提交说明和作者 |
| `git branch [-v] [-a]`、`git branch NAME [REV]`、`git branch -d NAME`、`--show-current` | 相同 | 分支都在服务端，所以 `-a` 列出的是同一批 |
| `git tag [-l] [-n]`、`git tag NAME [REV]`、`git tag -a NAME -m MSG [REV]`、`git tag -d NAME` | 相同 | 轻量标签和附注标签（不支持 `-s`） |
| `git log [REV] [-n N \| -N] [--oneline] [-- PATH]` | 相同 | |
| `git show [REV] [-- PATH]` | 相同 | |
| `git diff [--cached]`、`git diff A B`、`A..B`、`A...B` | 相同 | `--name-only`、`--name-status`、`--stat`、`-- PATH`；没有暂存区，`--cached` / `--staged` 显示同样的本地改动 |
| `git blame [-L N,M] PATH` | `scn blame [-L N,M] PATH` | 指定版本用 `scn blame REV:PATH` |
| `git grep [-n] [-l] [-i] [-F] PATTERN [REV] [-- PATH...]` | 相同 | 在服务端搜索提交（默认 HEAD）里的文件，不搜索本地改动 |
| `git reflog [BRANCH]` | `scn reflog [BRANCH] [-n N]` | 分支在服务端的移动记录：新建、快进或强制移动，以及时间和操作人 |
| `git remote -v`、`git remote get-url origin`、`git remote set-url origin URL` | 相同 | 只有一个 remote，`origin`；`set-url` 会先检查新地址上有工作副本所在的提交 |
| `git archive REV` | `scn archive REV` | `--format tar.gz\|zip\|tar`、`--prefix`、`-o FILE` |
| 版本号：`HEAD`、`@`、`HEAD~1`、`main^2`、`A..B`、40 位 SHA | 相同 | 每个提交都有 git SHA |
| `.gitignore` | `.gitignore` | 也支持 `.scionignore` |

## 过渡写法

下面这些也能用，执行时会提示对应的 scn 命令。它们不出现在 `scn --help` 里，以后可能去掉，脚本里请用 scn 的命令名。

| 你输入的 | scn 执行的 |
|---|---|
| `scn push` | `scn land` |
| `scn pull`、`scn pull --rebase` | `scn sync` |
| `scn checkout BRANCH`、`scn checkout -b NAME` | `scn switch BRANCH`、`scn switch -c NAME` |
| `scn checkout REV`（没有这个分支时） | `scn switch --detach REV` |
| `scn checkout -- PATH`、`scn checkout PATH` | `scn restore PATH` |
| `scn reset --soft HEAD~1` | `scn uncommit` |
| `scn reset --hard` | `scn restore .` |
| `scn rebase --continue`、`scn merge --continue`、`scn revert --continue`、`scn cherry-pick --continue` | `scn commit` |
| `scn rebase --abort` | `scn sync --abort` |
| `scn add …` | 什么也不做：没有暂存区，`scn commit` 会带上所有改动 |
| `scn rm [-r] PATH…`、`scn mv FROM TO` | 删除或移动文件；下一次 `scn commit` 会带上 |

## 写法不同

| git | scn | 原因 |
|---|---|---|
| `git init` | `scn repo create NAME` 或 `scn repo create URL` | 仓库在服务端 |
| `git push [-u] [origin [BRANCH]]` | `scn land` | 分支已经前进时由服务端 rebase，并且只合入一次；拒绝 `--force`（合入从不改写分支） |
| `git pull [--rebase \| --ff-only] [origin [BRANCH]]` | `scn sync` | 没有单独的 `fetch`：本地没有历史 |
| `git reset --soft HEAD~1` | `scn uncommit` | |
| 解决冲突后的 `git add` | `scn resolve PATH` | 用 `--ours` / `--theirs` 直接取一边 |
| `git show REV:PATH` | `scn cat REV:PATH` | 不需要工作副本 |
| `git ls-tree REV DIR` | `scn ls REV:DIR` | |
| `git sparse-checkout set DIRS` | `scn clone … --sparse DIRS`，`scn sparse set/add/rm/disable` | |
| `git config user.name` / `user.email` | `scn config set user.name` / `user.email` | |
| `git fast-import`、迁移仓库 | `scn import git PATH --repo REPO` | 保留原始 SHA |

读取历史的命令在工作副本外也能用，加 `--repo URL` 即可，例如 `scn log --repo http://host/org/app`。

## 尚未支持

交互式 rebase、`add -p`、`bisect`、`REF@{N}` 写法的版本、签名标签、`remote add` 和 `fetch`。`scn init`、`fetch`、`remote add` 会提示该改用什么。git 命令行本身可以通过 [git 网关](git-clients.zh-CN.md) clone、fetch 和 push Scion 仓库。
