# 认证与鉴权

[English](auth.md) | 简体中文

Scion 默认不要求认证（适合本机和 Lite 模式）。生产环境用 `--auth-required` 开启。服务端监听在回环地址（127.0.0.1、::1）以外时必须开启认证，否则拒绝启动；确实要匿名提供服务时显式加 `--allow-anonymous`（`SCION_ALLOW_ANONYMOUS=true`）。Docker 镜像默认开启认证，见 [deploy.md](deploy.zh-CN.md)。认证与鉴权只用标准协议（JWT / JWKS、RFC 7662 令牌内省、RFC 8628 device flow、OPA 风格的鉴权回调），不包含针对某个系统的代码。接入 starhub-server、Keycloak、Casdoor 或 OPA 都只需要配置。

## 开启认证

```sh
scion admin jwt-keygen > /etc/scion/jwt.pem          # 签发访问令牌用的 ES256 私钥，集群各节点共用
scion server --auth-required --jwt-key /etc/scion/jwt.pem --auth-config /etc/scion/auth.toml ...
```

| 参数 | 环境变量 | 说明 |
|---|---|---|
| `--auth-required` | `SCION_AUTH_REQUIRED` | 要求认证。没有有效令牌的请求只能访问策略允许 `anonymous` 的仓库 |
| `--jwt-key FILE` | `SCION_JWT_KEY_FILE` | 签发访问令牌的私钥。没有它也能用 PAT 和外部令牌，但不能换取访问令牌、不能用浏览器登录 |
| `--auth-config FILE` | `SCION_AUTH_CONFIG` | `auth.toml`：外部签发方、令牌内省、鉴权回调、OIDC |

配置有误（例如密钥读不到、环境变量没设）时服务端拒绝启动。

## 令牌

请求在 metadata `authorization: Bearer <token>` 中带令牌。服务端接受以下几种：

| 令牌 | 形式 | 校验 |
|---|---|---|
| 访问令牌 | ES256 JWT，默认 15 分钟 | 服务端自己的公钥 |
| PAT / service token | `scn_pat_…`，可设有效期 | 元数据库只存 SHA-256，可随时吊销 |
| 刷新令牌 | `scn_refresh_…`，30 天 | 只能用来换取访问令牌（`scn login` 得到） |
| 外部 JWT | 受信任签发方的 JWT | JWKS（RS*/PS*/ES*）或 HS256 共享密钥 |
| 外部不透明令牌 | 任意 | RFC 7662 内省接口 |

客户端通常用长期凭据（PAT、刷新令牌、外部 JWT）调用 `AuthService.ExchangeToken` 换取访问令牌，并在过期前 2 分钟自动重新换取（SDK 的 `Auth::Exchange`，`scn` 默认如此）。服务端没有签名密钥时，客户端直接发送原凭据。

管理令牌：

```sh
scion admin token create --subject ci-bot --group ci --ttl 90d --description "release pipeline"
scion admin token list [--subject ci-bot]
scion admin token revoke <id>
```

这些命令直接读写元数据库，存储参数与 `scion server` 相同，例如 `--meta postgres://…` 或 `--lite --data-dir DIR`。也可以通过 `AdminService` 远程管理。

## 访问控制策略

策略是一组「允许」规则，存在元数据库里，所有节点在 30 秒内生效：

```sh
scion admin policy set acl.toml     # 校验后保存
scion admin policy show
```

```toml
[[rule]]
subjects = ["group:eng"]              # user:NAME、group:NAME、"*"（任何已认证身份）、"anonymous"
repos    = ["mono", "models/*"]       # 仓库名，* 匹配任意字符（包括 /）
actions  = ["land"]                   # read < write < land < admin，高的包含低的

[[rule]]
subjects = ["user:release-bot"]
repos    = ["mono"]
actions  = ["land"]
refs     = ["release/*"]              # 只约束合入的目标 ref

[[rule]]
subjects = ["group:contractors"]
repos    = ["mono"]
actions  = ["write"]
paths    = ["src/ui", "docs"]         # 目录前缀；不写表示整个仓库

[[rule]]
subjects = ["anonymous", "*"]
repos    = ["public/*"]
actions  = ["read"]

[[rule]]
subjects = ["group:admins"]
repos    = ["*"]
actions  = ["admin"]                  # repos 含 "*" 的 admin 规则授予系统管理（策略、令牌）
```

**拒绝规则**（`effect = "deny"`）优先于所有允许规则。拒绝规则拒绝它的动作以及更高的动作，所以拒绝 `write` 后仍然可以读。带 `paths` 的拒绝规则也会拒绝不按路径的请求（例如检出时按 id 读对象），因为这类请求涉及整个仓库：

```toml
[[rule]]
effect   = "deny"
subjects = ["group:contractors"]
repos    = ["mono"]
paths    = ["secrets"]
actions  = ["read"]
```

**组**也可以直接在策略里定义，成员是用户或其他组。它们会在匹配规则之前补到令牌或 IdP 给出的组里，组之间不能循环包含：

```toml
[groups]
eng    = ["user:alice", "user:bob", "group:agents"]
agents = ["user:ci-bot"]
```

各接口需要的权限：

| 操作 | 需要 |
|---|---|
| 按 id 读对象（`GetObjects`、检出） | 整个仓库的 read（带 `paths` 的规则不够：按 id 读对象无法判断路径） |
| 读文件、Stat、Blame、按路径的 Log / Diff | 该路径的 read |
| 列目录 | 仓库中任意 read；只显示可读的条目和通向可读路径的上级目录 |
| 不限路径的 Diff | 只返回可读路径的改动 |
| 读 ref、Log、祖先判断、ResolveRevision | 仓库中任意 read |
| 上传对象、`HasObjects` | 仓库中任意 write（或整个仓库的 read） |
| 创建提交 | 每个改动路径的 write |
| 合入 | 目标 ref 的 land |
| 管理接口 | 系统管理 |

没有保存策略、也没有配置鉴权回调时，生效的是默认策略：admins 组管理一切（所以用 `--group admins` 签发的令牌就是管理员），任何已登录的身份都可以读写、合入所有仓库：

```toml
[[rule]]
subjects = ["group:admins"]
repos    = ["*"]
actions  = ["admin"]

[[rule]]
subjects = ["*"]
repos    = ["*"]
actions  = ["land"]
```

要修改，写一份自己的策略（以它为起点，免得丢了管理员），用 `scion admin policy set` 保存；保存的策略整个取代默认策略。

- **空策略**：显式保存的空策略（没有任何规则）拒绝一切，不等于「没有设置策略」。
- **策略加载失败**：例如元数据库不可用，或库里的策略无法解析。
  - 启动时加载失败，`scion server --auth-required` 拒绝启动。
  - 嵌入使用 `scion-api` 时，服务端在加载成功前对所有请求返回 `UNAVAILABLE`，每秒重试。
  - 运行中重新加载失败，保留上一份策略。

## auth.toml

```toml
[token]                                # 服务端签发的访问令牌
issuer = "https://scion.example.com"   # JWT 的 iss，默认 "scion"
ttl = "15m"

[[issuer]]                             # 受信任的外部签发方，可以有多个
issuer = "https://sso.example.com/realms/dev"
jwks_url = "https://sso.example.com/realms/dev/protocol/openid-connect/certs"
audience = "scion"                     # 可选
subject_claim = "preferred_username"   # 默认 sub
groups_claim = "groups"                # 可选，字符串数组或逗号分隔
repos_claim = "repos"                  # 可选：{"models/a": "write"} 形式的仓库授权

[introspection]                        # RFC 7662：外部签发的不透明令牌
url = "https://sso.example.com/oauth2/introspect"
client_id = "scion"
client_secret_env = "SCION_INTROSPECT_SECRET"   # HTTP Basic；也可以用 bearer_env
cache_ttl = "60s"

[authz_webhook]                        # 外部鉴权（OPA 兼容）
url = "http://opa:8181/v1/data/scion/allow"
cache_ttl = "30s"
timeout = "2s"
on_error = "closed"                    # 回调不可用时：closed 拒绝（默认），open 只按 ACL 判断

[oidc]                                 # scn login（浏览器登录）
issuer = "https://sso.example.com/realms/dev"
client_id = "scn"
subject_claim = "preferred_username"
groups_claim = "groups"
```

密钥一律可以用 `*_env`（环境变量）或 `*_file`（文件）给出，不必写在配置文件里。

### 外部鉴权回调

服务端在 ACL 和令牌自带的授权都不允许时调用回调。请求与响应格式与 OPA 的 REST 数据接口相同：

```json
POST /v1/data/scion/allow
{"input": {"subject": "alice", "groups": ["eng"], "authenticated": true,
           "repo": "models/ns/qwen", "action": "write", "ref": null, "path": "README.md"}}

{"result": true}                 // 或 {"result": {"allow": true}}；其它（包括没有 result）为拒绝
```

允许的结果缓存 `cache_ttl`，拒绝的结果缓存 5 秒。直接用 OPA 时，一条最简单的策略：

```rego
package scion
default allow := false
allow if input.action == "read"; startswith(input.repo, "public/")
allow if "eng" in input.groups
```

### 多个判定器

鉴权按顺序询问一条判定器链。每个判定器给出允许、拒绝或不表态；第一个给出允许或拒绝的判定器决定结果，全都不表态则拒绝。不写 `[authorization]` 时，链依次是 `policy`、`grants`（令牌自带的 `repos_claim`）、`[authz_webhook]`。用 `[authorization]` 指定顺序，回调需要几个就配几个：

```toml
[authorization]
chain = ["webhook:starhub", "policy", "grants", "webhook:opa"]

[[authorization.webhook]]
name = "starhub"
url = "http://starhub-server:8080/api/v1/internal/scion/authz"
bearer_env = "STARHUB_API_TOKEN"
decides = "allow-deny"            # 它返回 false 就拒绝；allow-only（默认）：false 表示不表态
repos = ["models/*", "datasets/*"] # 只问这些仓库，其他仓库不表态

[[authorization.webhook]]
name = "opa"
url = "http://opa:8181/v1/data/scion/allow"
```

链里还可以加两个内置判定器。`orgs` 让组织成员在 `ORG/…` 下做角色允许的事（owner：admin，maintainer：land，developer：write，reporter：read），并为成员补上 `org:ORG:ROLE` 组，策略里可以引用。`personal` 让每个登录用户拥有 `user/SUBJECT/…`。每个回调都支持和 `[authz_webhook]` 相同的 `bearer`、`bearer_env`、`cache_ttl`、`timeout`、`on_error`。回调不可用时，`on_error = "open"` 让它不表态，转而询问下一个判定器。原来的 `[authz_webhook]` 可以保留，在链里叫 `webhook`。设计见[授权](../design/authorization.zh-CN.md)。

## 组织

链里加上 `orgs` 后，独立部署的服务端就有了可以自助管理的组织；不加时，组织接口返回 `FAILED_PRECONDITION`。

```toml
# auth.toml
[authorization]
chain = ["policy", "orgs", "personal", "grants"]
```

- **命名空间。** 组织拥有所有名为 `ORG/…` 的仓库。
- **角色。** 每个成员有一个角色：

  | 角色 | 允许 |
  |---|---|
  | `owner` | admin：管理组织，在其中建库、删库、fork |
  | `maintainer` | land |
  | `developer` | write |
  | `reporter` | read |

- **可见性。** `internal` 组织所有登录用户都能读；`private`（默认）只有成员能读。
- **策略。** 成员还会得到 `org:ORG:ROLE` 组，策略里可以直接引用，例如 `subjects = ["group:org:acme:owner"]`。
- **个人命名空间。** `personal` 让每个登录用户拥有 `user/SUBJECT/…`。
- **谁能管什么。**
  - 任何登录用户都能建组织，并成为它的 owner。如果这个名字下已经有仓库，需要系统管理员来建。
  - owner 管理成员和可见性；成员可以自己退出。
  - 最后一个 owner 不能被移除或降级；还有仓库的组织不能删除。
- **何时生效。** 通过某台服务端做的修改在这台服务端上立即生效，其他节点在 30 秒内生效。

用户用 `scn` 管理组织；组织参数也可以写成 URL，例如 `http://host:7080/acme`：

```sh
scn org create acme [--internal]
scn org member add acme bob --role developer     # 或 maintainer、owner、reporter；再执行一次即可改角色
scn org member ls acme
scn org member rm acme bob                      # 成员可以移除自己
scn org visibility acme internal
scn org ls [--mine]
scn org info acme
scn org rm acme
```

运维也可以直接操作元数据库，存储参数与 `scion server` 相同：

```sh
scion admin org create acme --owner alice [--visibility internal]
scion admin org member add acme bob --role developer
scion admin org member ls acme
scion admin org visibility acme internal
scion admin org ls
scion admin org rm acme
```

## GitHub 登录

用户可以直接用 GitHub 账号登录。这是开源部署最省事的方式。

1. **注册 OAuth app。** 在 GitHub 的 Settings → Developer settings → OAuth Apps → New OAuth App 里创建。主页和回调地址随便填，device flow 两个都用不到。勾选 **Enable Device Flow**。
2. **配置服务端。** 填上这个 app 的 client id，并用签名密钥（`--jwt-key`）启动服务端：

   ```toml
   # auth.toml
   [github]
   client_id = "Ov23li…"
   # subject_prefix = "gh-"                 # 可选：subject 变成 gh-LOGIN
   # web_url = "https://github.example.com" # GitHub Enterprise
   # api_url = "https://github.example.com/api/v3"
   ```

3. **登录。** 执行 `scn login http://scion.example.com:7080` 后会显示一个代码，到 github.com/login/device 输入即可。服务端用 GitHub 的令牌读出用户名，再签发自己的刷新令牌和访问令牌；GitHub 的令牌不保存。

**把 GitHub 组织和 team 作为组。** 配置 `orgs`，并给一个能读取这些组织成员的令牌（带 `read:org` 的 PAT 或 app 令牌）后，这些组织的成员得到 `github:ORG` 组，各个 team 的成员再得到 `github:ORG/TEAM` 组。成员列表保存在内存里，每隔 `refresh`（默认 5 分钟）在后台重新拉取，期间继续用上一次的结果；拉取失败时保留原来的组。之后策略里就可以引用这些组：

```toml
[github]
client_id = "Ov23li…"
orgs = ["opencsg"]
token_env = "GITHUB_TOKEN"
# refresh = "5m"
```

```toml
# 策略
[[rule]]
subjects = ["group:github:opencsg/core"]
repos    = ["opencsg/*"]
actions  = ["land"]
```

只要 subject 是带 `subject_prefix` 的 GitHub 用户名，就会得到这些组，不论是用哪种方式登录的。

subject 就是 GitHub 用户名，所以策略、组织和个人命名空间（`user/LOGIN/…`）都直接用它。设置 `subject_prefix` 时，要让 subject 仍是合法的名字段（字母、数字、`-`、`_`、`.`），个人命名空间才能用。`[github]` 和 `[oidc]` 二选一。

## 接入 starhub-server

starhub-server 的实际情况：
- JWT 用 HS256 共享密钥签发（`STARHUB_JWT_SIGNING_KEY`），`iss` 是 `OpenCSG`，用户名在 `current_user` claim。
- 另有不透明的 access token，由 user 服务校验。
- 权限在 starhub 自己的数据库里按 rebac 计算，区分公开和私有仓库。

对应的配置：

```toml
[[issuer]]
issuer = "OpenCSG"
hs256_secret_env = "STARHUB_JWT_SIGNING_KEY"
subject_claim = "current_user"

[introspection]                        # starhub 的 access token
url = "http://starhub-server:8080/api/v1/internal/scion/introspect"
bearer_env = "STARHUB_API_TOKEN"
subject_claim = "username"

[authz_webhook]                        # 权限由 starhub 的 rebac 决定（含公开仓库的匿名读）
url = "http://starhub-server:8080/api/v1/internal/scion/authz"
bearer_env = "STARHUB_API_TOKEN"
cache_ttl = "30s"
```

starhub 侧需要提供两个内部接口，都只是对现有逻辑的薄包装：
- **`introspect`**：按 RFC 7662 接收 `token=…`，内部调用 `VerifyByAccessToken`，返回 `{"active": true, "username": "…"}`。
- **`authz`**：按上面的回调格式接收请求，把 Scion 的仓库名（例如 `models/ns/name`）映射回 starhub 的仓库，再调用 `CheckUserRepoPermission`（read / write / admin）。
  - `land` 映射为 write，或者按需要再加一层保护分支规则。
  - `anonymous` 只对公开仓库的 read 返回允许。

`scn login` 可以走 starhub 使用的 Casdoor OIDC（`[oidc]`），也可以用 `scn login --token <starhub access token>`。

## 缓存与 daemon

区域缓存（`scion cache`）和本机 daemon 不解析令牌。
- **读权限**：返回对象前（包括本地命中），用调用者自己的令牌向服务端调用 `CheckAccess(repo, read)`，结果按（令牌, 仓库）缓存 60 秒。吊销令牌或修改策略后，缓存层最多 60 秒内生效。
- **daemon 离线**：服务端不可达（而不是拒绝）时，24 小时内确认过的身份仍通过读权限检查；但读取前还要确认 redaction（[redaction.md](redaction.zh-CN.md)），距上次成功同步超过 10 秒的仓库返回 `UNAVAILABLE`，所以离线时基本不能读本机缓存。
- **预热**：区域缓存在服务端要求认证时用 `--warm-token`（`SCION_CACHE_WARM_TOKEN`）指定预热用的身份。

## 客户端

```sh
scn login                 # 在浏览器中登录（服务端配置了 [oidc]）
scn login --token TOKEN   # 用 PAT 或外部令牌；--with-token 从标准输入读取
scn login SERVER TOKEN     # 同上，并指定服务端（顺序任意）
scn whoami
scn logout
```

- **凭据存放**：按服务端地址保存。签名的 macOS 版本（正式发布的版本）存进钥匙串；其它构建和其它平台存进 `~/.config/scion/credentials.json`（权限 0600）。macOS 对每个不同的未签名二进制都会重新询问能否读取钥匙串条目，所以本地编译的 `scn` 每编译一次就会问一次，没有屏幕的 agent 会一直等待回答；签名的 `scn` 换了版本也会被继续信任。`SCION_CREDENTIALS=file` 或 `keychain` 可以指定使用哪种，`SCION_CREDENTIALS_FILE` 可以指定文件。钥匙串 3 秒内没有回应时会跳过，并提示改用 `SCION_TOKEN`。
- **CI**：设置 `SCION_TOKEN`（PAT 或外部令牌），优先于保存的凭据。
- **TLS**：`https://` 服务端的证书按 `scn` 内置的 Mozilla 根证书校验。`SCION_TLS_CA_FILE=PATH` 加入私有 CA（PEM）；`SCION_TLS_ROOTS=native` 同时信任操作系统的证书库，在 macOS 上可能让每次连接多花好几秒。
