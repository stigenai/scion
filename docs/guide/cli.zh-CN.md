# scion 与 scn 命令导览

[English](cli.md) | 简体中文

先启动一个服务端，再走一遍日常工作流里的命令。[从 git 迁移](coming-from-git.zh-CN.md)列出了 git 命令和 scn 命令的对应关系；每个命令都有 `--help`，脚本可以用 `--json`。

```sh
# 方式一：docker compose，带 PostgreSQL + S3 兼容存储（见 deploy.zh-CN.md）
SCION_IMAGE=opencsg-registry.cn-beijing.cr.aliyuncs.com/opencsghq/scion:latest REGISTRY=docker.m.daocloud.io docker compose -f deploy/dev/docker-compose.yml --profile server up -d   # 匿名运行；生产用 deploy/prd

# 方式二：从源码启动 Lite 模式的服务端（SQLite + 本地目录，数据持久保存）
cargo run -p scion -- server --lite --data-dir ./.scion-data

# 其它后端：--meta mem|sqlite://PATH|postgres://…  --blobs mem|DIR|s3://bucket/prefix
# S3 配置：SCION_S3_ENDPOINT、SCION_S3_ACCESS_KEY_ID、SCION_S3_SECRET_ACCESS_KEY、SCION_S3_REGION、
# SCION_S3_PROVIDER=auto|minio|seaweedfs|aws|generic（CI 会测试 SeaweedFS 和 MinIO，见 object-storage.zh-CN.md）

# 在另一个终端：安装 scn 并检查连接
cargo install --path client/cli
scn doctor
scn config set user.name "Ada Lovelace"       # 默认的提交作者（没设置时用 git 的 user.name / user.email）
scn config set user.email ada@example.com
scn repo create mono                # 仓库和分支：scn repo / scn branch（见 repos.zh-CN.md）

# 日常工作流
scn clone mono && cd mono          # 检出（默认 ref 为 main；空仓库也可以）
# 也可以像 git 一样用一个 URL 同时写服务端和仓库，进入工作副本后不用再写地址：
# scn clone http://127.0.0.1:7080/mono
echo 'hello' > README.md
echo 'target/' > .gitignore         # 支持 .gitignore 和 .scionignore（gitignore 语法）
scn status                          # git 的长格式，并说明服务端的 main 前进了多少；`scn status -s`：A  .gitignore、A  README.md
scn commit -m "add readme"          # 或者 --author "Ada <ada@example.com>"
scn restore src/main.rs             # 丢弃已跟踪文件的本地改动（`.` 表示全部）；新增的文件会保留
scn commit -m "add tests"           # 再提交一个：待合入的提交会叠起来，`scn land` 一次全部合入
scn commit -m "docs only" docs/     # 只提交 docs/ 下的改动，其余留在本地
scn commit --amend                  # 把更多本地改动并入最新的待合入提交（保留提交说明和作者）
scn uncommit                        # 撤掉最新的一个待合入提交，它的改动留作本地改动
scn land                            # 合入 main；main 已经前进时，服务端会 rebase（三方合并）
scn sync                            # 拉取别人的改动；待合入的提交逐个按行三方合并 rebase
scn tag v1.0                        # 给当前提交打标签；`scn tag` 列出标签，`scn log v1.0` 可以直接用它
scn switch -c feature               # 在当前快照上建分支并切换过去；`scn switch main` 切回来
scn merge feature                   # 把一个分支合并成待合入的合并提交（三方、按行合并）；然后 `scn land`
scn revert c1a2b3c4d5e6             # 生成一个撤销已合入提交的待合入提交（合并提交加 -m 1）；然后 `scn land`
scn cherry-pick c1a2b3c4d5e6        # 把别的分支上的提交应用到这里，保留原提交说明和作者（-x 注明原提交）
# 有冲突时（scn sync、merge、revert 或 cherry-pick），文件里会出现 <<<<<<< / >>>>>>> 标记，`scn status` 把它们列在 Unmerged paths 下（`-s` 显示 UU）：
scn resolve src/main.rs             # 删掉冲突标记之后执行；或者用 --ours / --theirs 直接取一边
scn commit                          # 完成合并，沿用原来的提交说明；`scn merge --abort`（或 revert / cherry-pick --abort）放弃，sync 用 `scn sync --abort`
scn cat main:README.md              # 不需要工作副本就能读文件（在工作副本外加 --repo）
scn ls main:                        # 列出目录；所有命令都支持 --json
scn log -n 10 -- src                # 从 HEAD 开始的历史，从新到旧，可以只看某个路径（git 写法：HEAD~1、A..B）
scn log main                        # 服务端的分支，包括上次 sync 之后别人合入的提交
scn watch                           # 跟踪服务端的分支：每合入一个提交打印一行（agent 用 --json）
scn diff                            # 未提交的本地改动（unified diff）
scn diff main                       # main 最新提交改了什么；`scn diff A B` 比较两个版本
scn blame src/lib.rs                # 每一行最后被哪个提交改动
scn clone mono part --sparse src/   # 稀疏检出：只有 src/ 和根目录下的文件；之后可以 `scn sparse add docs/`
scn archive -o mono.tar.gz          # 把快照下载成 tar.gz / zip / tar（见 archive.zh-CN.md）

# scn 经本机 daemon（scn daemon）连接服务端，需要时自动启动；SCION_NO_DAEMON=1 不使用 daemon
scn daemon status

# 放在一个或多个服务端前面的区域缓存；用 scion server --read-endpoint 推荐给客户端
# （见 cache.zh-CN.md）
cargo run -p scion -- cache --upstream http://127.0.0.1:7080 --listen 127.0.0.1:7090 --warm mono:main

# 导入已有的 git 仓库，保留历史、作者和时间
# （见 git-import.zh-CN.md）
scn import git ~/src/my-repo --repo mono

# 升级已有数据的服务端后、启动之前：修复提交谱系并补建派生索引（可以重复执行）
cargo run -p scion -- admin reindex --lite --data-dir ./.scion-data
```

## 服务端地址

仓库有两种写法：

- **URL，与 git 相同**：`http(s)://host[:port]/REPO`，例如 `scn clone http://scion.example.com:7080/org/app`。凡是写仓库的地方都可以用：`clone`、`--repo`（`scn cat README.md --repo http://host/org/app`），以及 `repo create / info / set / rm / restore / fork / ls`。只涉及服务端的命令直接写地址：`scn login URL`、`scn logout URL`、`scn whoami URL`、`scn doctor URL`。
- **仓库名加服务端**：`scn --endpoint http://host:7080 clone org/app`，或者设置一次 `SCION_ENDPOINT`。

工作副本会记住克隆时的服务端，所以进入目录后 `status`、`commit`、`land`、`sync`、`log` 等命令都不用再写地址。同时给出多个地址时，优先级依次是 URL 中的地址、`--endpoint` / `SCION_ENDPOINT`、工作副本记录的地址、`http://127.0.0.1:7080`。

服务端换了地址（新的 IP 或主机名）时，用 `scn remote set-url http://新地址:7080/仓库` 让工作副本指向它；`scn remote -v` 显示当前地址。新地址上必须有工作副本所在的提交，所以也可以换成 fork 或迁移后的同一个仓库。

## 输出

文本输出与 git 一致：同一步骤 git 打印什么，scn 就打印什么；git 不出声的命令，scn 也什么都不打印。进度和 `Cloning into 'mono'...` 这类提示写到标准错误。标准错误是终端时，`clone`、`import git` 和 `archive` 显示 git 风格的进度（导入时是 `Importing commits:  45% (2852/6338)`，下载归档时是 `Receiving archive: 12.40 MiB | 8.10 MiB/s`）：

```text
Cloning into 'mono'...
Listing files: 18342, done.
Receiving objects: 100% (18342/18342), 212.40 MiB | 38.12 MiB/s, done.
```

`-q` / `--quiet` 关掉进度和提示，错误照常输出。`--json` 改为输出一个带 `schema` 字段的 JSON 对象，不显示进度。

## 检出不会写入的路径

仓库里的路径来自服务端，所以恶意仓库不能借此改动工作副本以外的任何东西。遇到以下路径，`scn` 拒绝写入或删除，并以 `refusing to write <path>` 停止：

- 含有 `.scion` 或 `.git` 路径段的路径，不分大小写（`.SCION`、`.Git`）：这是工作副本自己的元数据，以及 git 的元数据；
- 不是普通相对路径的写法（`..`、空路径段、反斜杠）；
- 要经过符号链接才能到达的路径，例如旧版本留下的链接在新版本里是一个目录，或者在不区分大小写的文件系统（macOS、Windows）上，与 `a/...` 并存的链接 `A`。

符号链接本身照常检出为链接，与 git 相同；只是不允许经过它写入。
