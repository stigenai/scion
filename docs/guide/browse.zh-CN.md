# 文件列表页：最后提交与目录统计

[English](browse.md) | 简体中文

Web 页面展示目录时，通常要显示两类信息：
- 每个文件或子目录最后一次被哪个提交改动（「最后提交」列）；
- 仓库或目录的文件数和大小。

服务端提供这两种查询，并缓存计算结果，重复查看同一个版本时不用重新计算。

## 每个条目的最后提交

```
QueryService.ListLastCommits(repo, revision, path, offset, limit)
  → { changeset_id, entries: [{name, changeset_id}], commits: [LogEntry], total }
```

- **范围**：`path` 为目录路径，空表示根目录。条目按名字排序，用 `offset` 和 `limit` 分页，`limit` 默认和最大都是 1000，`total` 是条目总数。
- **`commits`**：`entries` 引用到的提交，去重后按代数从新到旧排列，包含作者、时间和提交说明，不需要再逐个查询。需要 40 位 git sha 时，用这些提交 id 调用 `GetGitShas`（见 [git 兼容的提交 sha](git-sha.zh-CN.md)）。
- **语义**：沿第一父提交链追溯：
  - 条目出现在某个提交的改动中（相对第一父提交），就算被这个提交改动；
  - 子目录被改动，指它下面的任意文件被改动；
  - 经由合并带进来的改动，归到合并提交上。
- **只能读部分路径的用户**：只列出可见的条目。看不到全部内容的条目（例如只能读 `src/ui` 时的 `src`）只返回名字，不返回最后提交。
- **SDK**：`Client::last_commits(revision, path, offset, limit)`。

### 计算与缓存

1. 结果按「提交 + 目录」缓存。只有一次请求覆盖目录的全部条目时才写入缓存。
2. 父提交已有缓存时，新提交只需要按自己的改动增量更新：改动到的条目归到新提交，其余沿用父提交的结果。合入后查看分支最新版本的常见情况，就走这条路径。
3. 没有缓存的条目，用 PathHistory 索引找第一父提交链上最近的改动。
4. 索引不可用时（建立索引之前写入的旧数据），沿第一父提交链逐个检查，最多 2000 个提交。超过上限仍未找到的条目，`changeset_id` 为空，这种结果不写入缓存。

## 目录统计

```
QueryService.GetTreeStats(repo, revision, path)
  → { changeset_id, tree_id, files, directories, bytes }
```

- **`files`**：文件数，包括可执行文件和符号链接。
- **`directories`**：子目录数，不含自身。
- **`bytes`**：文件字节数之和。在 S1 阶段，LFS 文件按指针文件的大小计算。
- **权限**：统计覆盖整棵子树，所以需要能读其中的全部内容：
  - `path` 为空时，需要整个仓库的读权限；
  - 指定 `path` 时，需要该路径的读权限。
- **缓存**：结果只由目录树内容决定，按目录树 id 缓存。没改动的子目录直接命中缓存，fork 出来的仓库也会一并复制这些数据。
- **SDK**：`Client::tree_stats(revision, path)`。

服务端在 `GetServerInfo` 中声明特性 `query.last_commits` 与 `query.tree_stats`。
