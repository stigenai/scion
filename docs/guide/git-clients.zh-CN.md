# 用 git 访问 Scion 仓库

[English](git-clients.md) | 简体中文

服务端在 `scn` 的同一个地址和 URL 上提供 git 的 smart HTTP 协议，所以 git 命令行、CI 系统和 IDE 都可以克隆、拉取和推送 Scion 仓库。网关的工作原理见[设计文档](../design/git-gateway.zh-CN.md)。

- [克隆和拉取](#克隆和拉取)
- [推送](#推送)
- [凭据](#凭据)
- [SHA](#sha)
- [暂不支持](#暂不支持)
- [关闭](#关闭)

## 克隆和拉取

仓库的 git URL 就是它的 `scn` URL，加 `.git` 后缀也可以：

```sh
git clone http://scion.example.com:7080/agentlab/app
cd app
git pull                        # 快进到之后合入的提交
git fetch origin feature        # 任意分支
git ls-remote origin            # HEAD、refs/heads/*、refs/tags/*
```

- 分支显示为 `refs/heads/<name>`，标签（Scion 中的 `tags/<name>`）显示为 `refs/tags/<name>`，`HEAD` 指向仓库的默认分支。
- 支持 git 2.x 客户端，协议 v2（git 2.26 起默认）和较老的 v0 都可以。
- 克隆会拿到所拉取分支的全部提交，而不只是 `scn clone` 下载的快照。CI 可以用 `git clone --depth 1` 只取最新提交，也可以用 `--shallow-since=日期`；之后用 `git fetch --depth N` 或 `--unshallow` 加深。
- 大仓库可以用部分克隆：先不下载文件，git 需要时（检出、`git log -p` 等）再按需拉取：`git clone --filter=blob:none URL`，或者 `--filter=blob:limit=1m` 只跳过超过 1 MiB 的文件。

## 推送

```sh
git push origin main            # 把新提交合入 main
git push origin feature         # 创建或移动分支
git push origin v1.0            # 标签，轻量标签或附注标签
git push origin :old-branch     # 删除分支
```

- **附注标签保留标签对象。** 服务端原样保存推送上来的标签对象（包括签名），所以之后 fetch 得到的标签 SHA 相同；`scn show v1.0` 和 `scn tag -n` 显示它的说明。用 `scn tag -a` 创建的附注标签，会按它的说明和打标签的人生成标签对象。`ls-remote` 同时列出标签和它指向的提交（`v1.0^{}`）。
- **提交保留 SHA。** 推送的每个提交都变成一个 Scion 提交，`scn log` 可以看到，作者、提交者、时间和说明都不变；git、`scn` 和 `GetGitShas` 看到的都是推送时的 SHA。签名提交保留签名。
- **分支只能快进。** 推送和 `scn land` 一样合入，但不做 rebase：别人先合入时，git 会提示 `rejected … (fetch first)`，执行 `git pull --rebase` 后再推送即可。`git push --force` 可以强制移动分支，所需权限和 `scn` 一样，是该分支的 `land` 权限。
- **权限：** 推送需要仓库的 `write` 权限（策略按路径限制时，需要改动路径的权限），以及每个 ref 的 `land` 权限，和 `scn` 相同。合入前检查和 ref 事件同样适用于推送。
- **限制：** 每次推送最多 16 GiB（服务端会先写入临时文件），单个文件不超过服务端的上传限制（`max_upload_object_bytes`）。

## 凭据

网关接受和 `scn` 相同的 token，作为 HTTP Basic 认证的密码，用户名任意：

```sh
git clone http://git:$SCION_TOKEN@scion.example.com:7080/agentlab/app
```

也可以让凭据助手保存 token（`git config --global credential.helper store`，git 询问时输入任意用户名和 token）。能发送请求头的工具也可以用 `Authorization: Bearer <token>`（`git -c http.extraHeader="Authorization: Bearer $SCION_TOKEN" clone …`）。

- 仓库不允许匿名读取时，没有凭据 git 会提示输入。
- token 无权读取仓库时返回 `403`。
- 权限策略和 `scn` 相同（见[认证](auth.zh-CN.md)）：拉取需要仓库的 `read` 权限。

## SHA

每个提交只有一个 git SHA，就是 `GetGitShas` 和 SDK 的 `git_shas` 返回的那个（见 [git SHA](git-sha.zh-CN.md)）：

- **从 git 导入的提交**保留原来的 SHA，克隆得到的对象和源仓库完全一致，签名提交也一样。含子模块的提交，以及 0.1.11 之前导入的签名提交，暂时无法提供（见下文）。
- **用 `scn` 创建的提交**的 SHA 由内容计算得出，commit 对象中带一行 `scion-id` 头，`git fsck --strict` 可以通过。

## 暂不支持

| 功能 | 现象 |
|---|---|
| `git push --atomic` | git 会拒绝：Scion 一次只更新一个 ref |
| 指向目录树、文件或其他标签的标签 | `git push` 提示 `only tags of commits are supported` |
| 子模块，以及文件、可执行文件、符号链接以外的模式 | 推送被拒绝，并给出路径 |
| `refs/heads/*` 和 `refs/tags/*` 以外的 ref（如 `refs/notes/*`） | 该 ref 的推送被拒绝 |
| `blob:none` 和 `blob:limit` 以外的部分克隆过滤条件（`tree:0`、`sparse:…`） | 拒绝并给出提示 |
| `git fetch --deepen` 和 `--shallow-exclude` | 拒绝并给出提示；`--depth`、`--shallow-since` 和 `--unshallow` 可用 |
| 含子模块的提交，或 0.1.11 之前导入的签名提交 | 拉取失败，提示 `fatal: remote error: git commit … was imported without its raw git object, …; import the git repository again into a new Scion repository with scn 0.1.11 or later (this one keeps serving the old import), or clone it with scn` |
| 历史中包含被 [redact](redaction.zh-CN.md) 的文件 | 拉取失败：文件的 SHA 覆盖的内容已不允许任何人读取 |
| SSH URL | 请使用 HTTP(S) |

## 关闭

`scion server --no-git`（或 `SCION_NO_GIT=1`）停止提供 git 服务（包括推送），gRPC 接口不受影响。
