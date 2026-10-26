# Git 网关：在 Scion 仓库上使用 git clone、fetch 和 push

[English](git-gateway.md) | 简体中文

状态：G1（拉取）和 G2（推送）已实现；G3（效率）除热门 ref 的 pack 缓存外已完成，下一步是 G4（starhub）。本文原是[路线图](roadmap.md)的 2.5 项。使用方法见 [git 客户端](../guide/git-clients.zh-CN.md)。

Scion 自己的客户端是 `scn`，但很多工具只会说 git：CI 系统、IDE、包管理器、`git submodule`，以及 starhub 的用户。网关让 git 命令行直接用 `scn` 的同一个 URL 克隆、拉取和推送 Scion 仓库：

```sh
git clone http://scion.example.com:7080/agentlab/app
```

## 1. 目标和边界

**目标：**
- **clone 和 fetch**：任何 git 2.x 客户端，走 smart HTTP（协议 v0 和 v2）。
- **push**：快进推送分支，创建和删除分支、标签。
- **SHA 稳定**：一个提交只有一个 git SHA，就是 `GetGitShas` 现在返回的那个（见 [git-sha 指南](../guide/git-sha.zh-CN.md)）。通过 git 推送的提交保留推送时的 SHA，从 git 导入的提交也一样。
- **权限和 `scn` 相同**：同样的 token、同一条授权链、同样的合入前检查。
- **不影响 `scn`**：网关读同一份存储和派生数据，`scn` 的读写路径不变。

**不做（或放到后面，见 §9）：**
- SSH（starhub 已经有 gitlab-shell，G4 阶段由网关对接）；
- Scion 目录树中的子模块（gitlink）：添加子模块的推送会被拒绝；
- `git push --atomic`：Scion 一次只更新一个 ref；
- 对 git 推送做服务端 rebase。不是快进的推送会被拒绝，这符合 git 用户的预期，在客户端用 `git pull --rebase` 解决。

## 2. 运行在哪里

- **在 `scion server` 里，和 gRPC 共用一个监听端口。** gRPC 请求是 HTTP/2，带 `content-type: application/grpc`；git 请求是 HTTP/1.1（或 HTTP/2），路径为 `/<repo>/info/refs`、`/<repo>/git-upload-pack` 和 `/<repo>/git-receive-pack`。由一个路由器把请求分给对应的一侧，所以一个仓库对两种客户端只有一个 URL。
  - 仓库名带 `.git` 后缀也可以，后缀会被忽略。
  - 网关默认开启，用 `--no-git`（或 `[git] enabled = false`）关闭。
- **代码放在独立的 crate：`server/git`（`scion-git`）。** 它依赖 `scion-store`、`scion-query` 和 `scion-land`，不依赖 gRPC 服务。以后可以不改代码，把它作为单独的角色（`scion git-gateway`）和 `scion cache` 并列运行。
- **直接访问存储。** 网关读 `MetaStore` 和 `BlobStore`（带提交记录缓存和对象缓存），合入走和 `LandService` 相同的 `Coordinator`。如果走 gRPC，每读一个目录树都要一次往返。

## 3. ref 和对象的对应关系

**ref：**

| Scion | git |
|---|---|
| 分支 `main` | `refs/heads/main` |
| 标签 `tags/v1` | `refs/tags/v1` |
| 默认分支 | `HEAD`，符号引用（`symref=HEAD:refs/heads/main`） |

其他 git ref（`refs/notes/*`、`refs/pull/*` 等）不能推送。

**对象**：每个 Scion 对象都对应一个 git 对象，由 `GitSha` 派生数据构造：

| Scion | git 对象 |
|---|---|
| 文件 | `blob`：文件内容 |
| 目录 | `tree`：条目按 git 的规则排序，模式为 `100644`、`100755`、`120000`、`40000` |
| 提交 | `commit`：见下文 |

**commit 对象。** 一个提交的 git 对象按以下顺序确定：
1. **提交带有原始对象时，直接使用它。** 新的扩展字段 `git.commit` 保存 git 写出的原始 commit 对象，签名（`gpgsig`）、`encoding`、`mergetag` 等头都原样保留。推送和新的导入会记录这个字段（§5、§6）。
2. **否则，如果提交带有 `git.sha1`**（在 `git.commit` 出现之前导入的提交）：用提交的字段重建对象，不加 `scion-id` 头。重建结果的 SHA 等于记录的 `git.sha1` 时，它就是这个提交的对象。不相等时（原提交带签名，或者导入时跳过了子模块），网关无法提供这个提交，并会说明原因（§7）。
3. **否则**（用 `scn` 创建的提交）：带 `scion-id` 头的计算结果，与 `GetGitShas` 现在的算法相同。

无论哪种情况，网关都会检查：对象的 SHA 等于提交的 git SHA，`tree` 和 `parent` 行写的是提交根目录树和各父提交的 git SHA。检查不通过的提交，绝不会以与内容不符的 SHA 发出去。

**反向查找。** `git-sha1` 已经能把提交 SHA 映射回 Scion 提交。新增两种派生数据，把 tree 和 blob 的 SHA 映射回 Scion 对象（`git-tree-id`、`git-blob-id`），和 `git-tree`、`git-blob` 同时写入。推送时用它们找到新提交所基于的已有对象。

## 4. 拉取（`git-upload-pack`）

**公布 ref。**
- **v2**（git 2.26 起默认使用）：支持 `ls-refs`（含 `ref-prefix`、`symrefs`、`peel`）和 `fetch` 命令。
- **v0/v1**：ref 列表，能力为 `multi_ack_detailed`、`side-band-64k`、`thin-pack`、`ofs-delta`、`no-done`、`shallow`、`deepen-since`、`symref`、`agent`；v2 提供 `fetch=shallow`。
- 每个 ref 的 SHA 来自 `GetGitShas`；还没算过的提交先计算。

**协商。**
- 客户端给出想要的提交（**want**，即各个 tip）和已有的提交（**have**）。
- 某个 `have` 能通过 `git-sha1` 映射到仓库中的提交时，网关就报告它是共同提交，客户端据此停止继续往回找。

**要发送哪些对象。**
- **提交**：want 的祖先中，不是共同 have 的祖先的那些。用提交图计算，generation 号限定遍历范围。
- **tree 和 blob**：从每个新提交的根目录树开始遍历，跳过已经发送过的，以及从共同 have 可达的。
  - Scion 的目录树和 git 一样按内容寻址，没有变化的子目录在两个提交中 id 相同，会整棵跳过。
  - 共同提交的子目录按需加入跳过集合：只比较新提交改动到的路径。

**浅拉取**（`--depth`、`--shallow-since`、`--unshallow`）。
- 从 want 开始逐层遍历确定边界：`deepen N` 时，深度为 N 的提交不保留父提交；`deepen-since T` 时，只有当父提交全部在 T 或之后提交时，一个提交才保留父提交。
- 有父提交的边界提交以 `shallow` 行发给客户端（v2：pack 之前的 `shallow-info` 段；v0：确认信息之前，每次响应都发）。
- 无论 have 怎么说，客户端自己的浅提交在客户端那边都没有父提交，所以遍历在它们那里停止。新深度越过某个浅提交时，发送 `unshallow` 行，并把它的父提交加入 want。
- `deepen-relative`（`git fetch --deepen`）和 `deepen-not`（`--shallow-exclude`）会被拒绝并给出提示。

**部分克隆**（`--filter=blob:none`、`--filter=blob:limit=N`）。
- 遍历目录树时跳过所有文件，或超过 N 字节的文件；目录树全部发送。
- 之后 git 按 SHA 拉取需要的文件（v0 声明 `allow-reachable-sha1-in-want`）。不是提交的 want 通过 `git-tree-id`、`git-blob-id` 查找；对于这两种映射出现之前派生的数据，会对各 ref 的目录树建一次索引（用标记记录），然后重新查找。
- 其他过滤条件（`tree:N`、`sparse:oid`）会被拒绝。

**pack。**
- 格式为 pack 第 2 版，边生成边发送，不写临时文件；末尾是整个 pack 的 SHA-1。
- **只沿路径找 delta。** 提交改动的目录树或文件，以父提交同一路径上的对象为基准发送 delta（delta 明显更小时）；不搜索其他候选，所以每个改动的对象只比较一次。
  - 基准在 pack 中更靠前时按偏移引用（`ofs-delta`）；基准在后面，或者客户端声明了 `thin-pack` 且已有基准时，按 SHA 引用（`ref-delta`）：增量拉取时，改动的文件以客户端已有的版本为基准发送 delta。
  - 目录树和文件按从旧到新的顺序写入，基准大多先出现。最近写出的内容（64 MiB）留作 delta 基准，其余基准重新读取。
  - delta 按 16 字节分块索引基准，每个匹配向前后扩展。超过 32 MiB 的文件整体发送。
  - 以本仓库（254 个提交）实测：克隆的 pack 为 10.0 MB，git 为 9.1 MB（不用 delta 时为 22 MB）；克隆也比不用 delta 快（3.6 秒对 5.9 秒），因为压缩和发送的字节更少。
- 数据走 side band 1，进度走 side band 2（`Counting objects`、`Writing objects`）。
- **内存有上限。** 对象边读边发；记录已发送 SHA 的集合每个对象占 20 字节。

## 5. 推送（`git-receive-pack`）

**公布能力。**
- ref 列表，能力为 `report-status`、`report-status-v2`、`delete-refs`、`ofs-delta`、`side-band-64k`、`quiet`、`agent`。
- 不声明 `atomic`：Scion 一次只更新一个 ref，所以 `git push --atomic` 会被 git 自己拒绝。也不声明 `push-options`。
- 另外声明 `no-thin`：客户端就会发送自包含的 pack。其中的 delta 只能以同一个 pack 里的对象为基准，网关不需要为了应用 delta 去重建已有对象。

**接收 pack。**
- 边接收边写入临时文件（每次推送最多 16 GiB），再在阻塞线程中从文件读取。解出的对象在 256 MiB 以内留在内存，超出部分写入另一个临时文件；实测 1 GB 的推送服务端内存峰值 555 MB，400 MB 的推送为 457 MB：
  - 完整对象，以及基准在同一 pack 中的 `ofs-delta`、`ref-delta` 条目；
  - 计算每个对象的 SHA，并校验 pack 末尾的校验和；
  - 单个文件最大为 `max_upload_object_bytes`，与 `scn` 上传的限制相同。
- 文件在需要它的提交之前写入存储，和 `scn` 上传一样去重：Scion 已有的文件不会再写一次。

**把 git 提交转换成 Scion 提交。**
- 按父提交在前的顺序处理每个推送来的提交：
  - **父提交**是 Scion 提交：通过 `git-sha1` 找到，或者是本次推送前面刚创建的。
  - **目录树**从 git 的 tree 转换而来：每个条目来自 pack，或者通过 `git-tree-id`、`git-blob-id` 找到（例如改名的目录不会重新发送）。对于在这两种映射出现之前派生的目录树，查找不到时会对第一父提交的目录树建一次索引。
  - **改动**是与第一父提交的目录树比较得到的，两边 SHA 相同的子目录直接跳过。
  - **拒绝以下情况**（推送停止，并给出说明）：
    - gitlink；
    - Scion 四种模式以外的模式（老的 `100664` 模式 git 会按 `100644` 读取，但这会改变 tree 的 SHA，所以同样拒绝）；
    - 不是 UTF-8 的路径；
    - Scion 路径规则不允许的路径。
  - **生成的 Scion 提交**：
    - 作者、提交者和说明取自 git 提交，`change_id` 由 git SHA 推导，和导入时一样；
    - 带扩展字段 `git.sha1`（SHA），无法用字段重建时还带 `git.commit`（原始对象）。
  - **附注标签**：ref `tags/NAME` 指向提交，标签对象存进标签的附注（`TagAnnotation.git_object`，原样保存；服务端会检查它指向的就是这个提交）。fetch 时通告标签对象的 SHA 和它指向的提交（`^{}`，v2 的 `peel` 时为 `peeled:`），客户端要时把对象发过去。指向目录树、文件或其他标签的标签会被拒绝。
- **检查。**
  - 创建提交之前，网关检查 Scion 目录树的 git SHA 等于 commit 的 `tree` 行。无法原样还原的 git tree 会被拒绝，因此 git 用户看到的 SHA 始终就是内容的 SHA。
  - 对于非管理员写入者，只有服务端可以写 `git.sha1` 和 `git.commit`，而且要在通过上述检查之后。非管理员客户端仍然不能通过 `CreateChangeset` 设置这两个字段。

**更新 ref。**
- 每条命令（`old new ref`）对应：
  - 已有分支：`fast_forward_only` 的 `Land`，以 old 作为预期值；
  - 新分支或新标签：`CreateRef`；
  - new 为 `0000…`：`DeleteRef`；
  - `--force` 推送到不是后继的提交：以 old 作为预期值的 `UpdateRef`。它和用 `scn` 移动分支一样需要该 ref 的 `land` 权限，所以保护分支的策略也会拒绝它。
- **授权：**
  - 拉取需要仓库的 `read` 权限；
  - 发送 pack 需要 `write`；
  - 更新每个 ref 需要该 ref 的 `land`，按 ref 的策略（例如保护分支）判断；
  - 合入前检查和 `scn land` 一样执行，可以拒绝某个 ref 并给出原因。
- **逐个 ref 处理：** 每个 ref 单独报告 `ok` 或 `ng <原因>`。命令中的 old 不是 ref 当前值时返回 `ng … fetch first`。
- **事件：** 推送和合入一样产生 `ref.updated` 事件，其中带有 git SHA。

## 6. 导入时也保存原始提交

当 §3 的规则 2 无法重建提交时（签名提交、`encoding` 头、不是 UTF-8 的说明），`scn import git`（以及以后的 `ImportService`）在 `git.sha1` 之外也记录 `git.commit`，这些提交从网关出来时逐字节相同，其余提交的大小不变。之前导入的、没有这个字段的提交，按 §3 处理。

## 7. git 用户会看到的错误

拉取时先做规划、生成提交对象，再给客户端应答。因此出错时服务端用一个 `ERR` 包代替应答，git 只显示一行 `fatal: remote error: …`。如果错误在发送 pack 的过程中才出现，则通过 side band 3 发出，git 显示为 `remote: …`，后面还会跟着 git 自己对 pack 中断的报错。推送的错误以 `ng` 行返回：

| 情况 | 提示 |
|---|---|
| 没有权限 | HTTP 401 并带 `WWW-Authenticate: Basic`，git 会提示输入凭据；token 无权读取仓库时为 403 |
| 不是快进 | `ng refs/heads/main non-fast-forward`（后面跟着 git 自己的提示） |
| 无法提供的提交（§3） | `git commit 3f2a… was imported without its raw git object, …; import the git repository again into a new Scion repository with scn 0.1.11 or later (this one keeps serving the old import), or clone it with scn (Scion commit C1…)` |
| 推送中有 gitlink 或不支持的模式 | `path vendor/lib is a submodule; Scion repositories cannot hold submodules` |
| 合入前检查拒绝 | 检查返回的说明 |

## 8. 认证

- HTTP Basic：用户名任意，密码是 Scion token（PAT、访问令牌或外部签发方的令牌）。也支持 `Authorization: Bearer <token>`。
- git 的凭据助手可以像保存其他 HTTPS 密码一样保存 token。
- 匿名访问遵循服务端的配置：允许匿名读取时，公开仓库无需凭据即可克隆。

## 9. 阶段

| 阶段 | 内容 | 完成标准 |
|---|---|---|
| **G1 拉取**（已完成） | gRPC 端口上的路由器；ref；`upload-pack` v0 和 v2；不带 delta 的 pack；Basic 认证；按 §3 生成 commit 对象 | 在 Mac mini 上 `git clone`、`git fetch`、`git pull` 可用，结果通过 `git fsck --strict` |
| **G2 推送**（已完成） | `git-tree-id`、`git-blob-id` 反向查找；`receive-pack`：解析 pack，git 提交转为 Scion 提交，快进合入，创建和删除 ref，`git.commit`（`scn import git` 已经记录） | 可以 `git push` 新提交、分支和标签；签名提交保留 SHA；非快进推送被拒绝 |
| **G3 效率**（除 pack 缓存外已完成） | pack 中的 delta；浅克隆（`--depth`）；部分克隆（`--filter=blob:none`）；缓存热门 ref 的最近一个 pack | 克隆 starhub-server 传输的字节不超过 git 的 1.5 倍 |
| **G4 starhub** | 通过 gRPC 提供 Gitaly 的 `SmartHTTPService` 和 `SSHService`（含 sidechannel），对接 gitlab-workhorse 和 gitlab-shell；除非有需求，不做原生 SSH | starhub 的 git 路由在 Scion 仓库上可用 |

## 10. 测试

- **协议测试：**
  - pkt-line、能力协商、pack 的写入和读取，使用固定的测试向量；
  - 能解析 git 自己写出的 pack（`git pack-objects`）；
  - 网关写出的 pack 用 `git index-pack --strict` 校验。
- **使用真实 git 命令行的端到端测试：**
  - `server/api/tests/git.rs` 启动服务端，运行 `git clone`、`fetch`、`pull`、`push` 和 `push --force`；
  - 每个测试都检查 `git log` 与 `scn log` 一致，`GetGitShas` 返回的 SHA 与 git 看到的相同；
  - 没有安装 git 时测试自动跳过。
- **往返测试：** 导入一个 git 仓库（含签名提交和合并提交），通过网关克隆出来，与原仓库逐个对象比较，没有差异。
- **Mac mini**：每次发版前测试，在 `scn` 测试之外再加一套 git 测试。
