# Authentication and access control

English | [简体中文](auth.zh-CN.md)

By default Scion does not require authentication, which suits a local machine and Lite mode. Turn it on in production with `--auth-required`. A server listening beyond loopback (127.0.0.1, ::1) must have it on or refuses to start; to serve anonymously anyway, pass `--allow-anonymous` (`SCION_ALLOW_ANONYMOUS=true`). The Docker image requires authentication by default; see [deploy.md](deploy.md). Authentication and authorization use only standard protocols (JWT and JWKS, RFC 7662 token introspection, the RFC 8628 device flow and an OPA-style authorization callback), with no code for any particular system: connecting starhub-server, Keycloak, Casdoor or OPA is configuration only.

## Turning on authentication

```sh
scion admin jwt-keygen > /etc/scion/jwt.pem          # ES256 key that signs access tokens, shared by a cluster's nodes
scion server --auth-required --jwt-key /etc/scion/jwt.pem --auth-config /etc/scion/auth.toml ...
```

| Option | Environment variable | What |
|---|---|---|
| `--auth-required` | `SCION_AUTH_REQUIRED` | Require authentication. Requests without a valid token can reach only repositories the policy opens to `anonymous` |
| `--jwt-key FILE` | `SCION_JWT_KEY_FILE` | The key that signs access tokens. Without it, PATs and external tokens still work, but tokens cannot be exchanged for access tokens and browser login is unavailable |
| `--auth-config FILE` | `SCION_AUTH_CONFIG` | `auth.toml`: external issuers, token introspection, the authorization callback, OIDC |

The server refuses to start when the configuration is wrong (an unreadable key, an unset environment variable and so on).

## Tokens

Requests carry a token in the metadata `authorization: Bearer <token>`. The server accepts:

| Token | Form | Checked against |
|---|---|---|
| Access token | ES256 JWT, 15 minutes by default | The server's own public key |
| PAT / service token | `scn_pat_…`, with an optional expiry | Only its SHA-256 is stored in the metadata database; revocable at any time |
| Refresh token | `scn_refresh_…`, 30 days | Exchangeable for access tokens only (from `scn login`) |
| External JWT | A JWT from a trusted issuer | JWKS (RS*/PS*/ES*) or an HS256 shared secret |
| External opaque token | Anything | An RFC 7662 introspection endpoint |

Clients usually exchange a long-lived credential (a PAT, a refresh token, an external JWT) for an access token through `AuthService.ExchangeToken`, and exchange again 2 minutes before it expires (the SDK's `Auth::Exchange`, and `scn` by default). When the server has no signing key, clients send the credential itself.

Managing tokens:

```sh
scion admin token create --subject ci-bot --group ci --ttl 90d --description "release pipeline"
scion admin token list [--subject ci-bot]
scion admin token revoke <id>
```

These commands read and write the metadata database directly, with the same storage options as `scion server`, such as `--meta postgres://…` or `--lite --data-dir DIR`. `AdminService` manages tokens remotely too.

## Access control policy

A policy is a list of "allow" rules kept in the metadata database; every node applies a new one within 30 seconds:

```sh
scion admin policy set acl.toml     # validate and save
scion admin policy show
```

```toml
[[rule]]
subjects = ["group:eng"]              # user:NAME, group:NAME, "*" (anyone authenticated), "anonymous"
repos    = ["mono", "models/*"]       # repository names; * matches anything, / included
actions  = ["land"]                   # read < write < land < admin; each includes the ones before

[[rule]]
subjects = ["user:release-bot"]
repos    = ["mono"]
actions  = ["land"]
refs     = ["release/*"]              # limits only the refs landed on

[[rule]]
subjects = ["group:contractors"]
repos    = ["mono"]
actions  = ["write"]
paths    = ["src/ui", "docs"]         # directory prefixes; without it, the whole repository

[[rule]]
subjects = ["anonymous", "*"]
repos    = ["public/*"]
actions  = ["read"]

[[rule]]
subjects = ["group:admins"]
repos    = ["*"]
actions  = ["admin"]                  # admin on repos "*" grants system administration (policy, tokens)
```

**Deny rules** (`effect = "deny"`) win over every allow rule. A deny rule denies its action and every higher one, so denying `write` still lets the subject read. A deny rule with `paths` also denies requests not made by path, such as reading objects by id for a checkout, because those cover the whole repository:

```toml
[[rule]]
effect   = "deny"
subjects = ["group:contractors"]
repos    = ["mono"]
paths    = ["secrets"]
actions  = ["read"]
```

**Groups** can be defined in the policy itself, from users and other groups; they are added to the groups a token or IdP gives before any rule is matched, and cycles are rejected:

```toml
[groups]
eng    = ["user:alice", "user:bob", "group:agents"]
agents = ["user:ci-bot"]
```

What each API needs:

| Operation | Needs |
|---|---|
| Reading objects by id (`GetObjects`, checkouts) | read on the whole repository (rules with `paths` are not enough: an object read by id has no path to check) |
| Reading a file, Stat, Blame, Log or Diff by path | read on that path |
| Listing a directory | any read in the repository; only readable entries, and the parents leading to readable paths, are shown |
| Diff without a path | only changes to readable paths are returned |
| Reading refs, Log, ancestry, ResolveRevision | any read in the repository |
| Uploading objects, `HasObjects` | any write in the repository (or read on the whole repository) |
| Creating a commit | write on every changed path |
| Landing | land on the target ref |
| Admin APIs | system administration |

With no policy saved and no authorization callback configured, the default policy applies: the `admins` group manages everything (so a token created with `--group admins` is an administrator), and anyone signed in can read, write and land in every repository:

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

To change it, write a policy of your own (start from this one to keep an administrator) and save it with `scion admin policy set`; a saved policy replaces the default entirely.

- **An empty policy:** an explicitly saved policy with no rules denies everything; it is not the same as no policy.
- **When the policy fails to load** (the metadata database is unavailable, or the stored policy does not parse):
  - at start, `scion server --auth-required` refuses to start;
  - embedded through `scion-api`, the server answers every request with `UNAVAILABLE` until a load succeeds, retrying every second;
  - when a reload fails while running, the previous policy stays in effect.

## auth.toml

```toml
[token]                                # access tokens the server issues
issuer = "https://scion.example.com"   # the JWT's iss, "scion" by default
ttl = "15m"

[[issuer]]                             # trusted external issuers; there can be several
issuer = "https://sso.example.com/realms/dev"
jwks_url = "https://sso.example.com/realms/dev/protocol/openid-connect/certs"
audience = "scion"                     # optional
subject_claim = "preferred_username"   # sub by default
groups_claim = "groups"                # optional; an array of strings or comma-separated
repos_claim = "repos"                  # optional: repository grants such as {"models/a": "write"}

[introspection]                        # RFC 7662: opaque tokens from an external issuer
url = "https://sso.example.com/oauth2/introspect"
client_id = "scion"
client_secret_env = "SCION_INTROSPECT_SECRET"   # HTTP Basic; or bearer_env
cache_ttl = "60s"

[authz_webhook]                        # external authorization (OPA-compatible)
url = "http://opa:8181/v1/data/scion/allow"
cache_ttl = "30s"
timeout = "2s"
on_error = "closed"                    # when the callback is unavailable: closed denies (default), open uses only the ACL

[oidc]                                 # scn login (browser login)
issuer = "https://sso.example.com/realms/dev"
client_id = "scn"
subject_claim = "preferred_username"
groups_claim = "groups"
```

Every secret can be given as `*_env` (an environment variable) or `*_file` (a file) instead of in the configuration file.

### The external authorization callback

The server calls the callback when neither the ACL nor the token's own grants allow a request. Requests and responses follow OPA's REST data API:

```json
POST /v1/data/scion/allow
{"input": {"subject": "alice", "groups": ["eng"], "authenticated": true,
           "repo": "models/ns/qwen", "action": "write", "ref": null, "path": "README.md"}}

{"result": true}                 // or {"result": {"allow": true}}; anything else (no result included) denies
```

Allows are cached for `cache_ttl`, denials for 5 seconds. With OPA itself, a minimal policy:

```rego
package scion
default allow := false
allow if input.action == "read"; startswith(input.repo, "public/")
allow if "eng" in input.groups
```

### Several deciders

Authorization asks a chain of deciders in order. Each one allows, denies or has no opinion; the first that allows or denies settles the request, and if none does it is denied. Without `[authorization]` the chain is `policy`, `grants` (the token's own `repos_claim`), then `[authz_webhook]`. Name the order, and add as many webhooks as you need, with `[authorization]`:

```toml
[authorization]
chain = ["webhook:starhub", "policy", "grants", "webhook:opa"]

[[authorization.webhook]]
name = "starhub"
url = "http://starhub-server:8080/api/v1/internal/scion/authz"
bearer_env = "STARHUB_API_TOKEN"
decides = "allow-deny"            # its false denies; allow-only (default): false has no opinion
repos = ["models/*", "datasets/*"] # asked only about these; no opinion elsewhere

[[authorization.webhook]]
name = "opa"
url = "http://opa:8181/v1/data/scion/allow"
```

Two built-in deciders can join the chain. `orgs` lets organization members do what their role covers in `ORG/…` (owner: admin, maintainer: land, developer: write, reporter: read) and adds their `org:ORG:ROLE` groups, which policies can name. `personal` gives every signed-in user `user/SUBJECT/…`. Each webhook takes the same `bearer`, `bearer_env`, `cache_ttl`, `timeout` and `on_error` as `[authz_webhook]`. When one is unavailable, `on_error = "open"` makes it have no opinion, so the next decider is asked. An `[authz_webhook]` can stay; in a chain it is called `webhook`. The design is in [authorization](../design/authorization.md).

## Organizations

With `orgs` in the chain, a standalone server can have self-service organizations. Without it, the organization API answers `FAILED_PRECONDITION`.

```toml
# auth.toml
[authorization]
chain = ["policy", "orgs", "personal", "grants"]
```

- **Namespaces.** An organization owns the repositories named `ORG/…`.
- **Roles.** Members have a role:

  | Role | Allows |
  |---|---|
  | `owner` | admin: manage the organization, create, delete and fork its repositories |
  | `maintainer` | land |
  | `developer` | write |
  | `reporter` | read |

- **Visibility.** `internal` organizations can be read by every signed-in user; `private` ones (the default) only by members.
- **Policies.** Members also get the group `org:ORG:ROLE`, so a policy can name it, for example `subjects = ["group:org:acme:owner"]`.
- **Personal namespaces.** `personal` gives each signed-in user `user/SUBJECT/…`.
- **Who manages what.**
  - Any signed-in user can create an organization and becomes its owner. A system administrator is needed when repositories already exist under the name.
  - Owners manage members and visibility; members can leave on their own.
  - The last owner cannot be removed or demoted, and an organization with repositories cannot be deleted.
- **When changes apply.** Changes made through a server apply on it at once; other nodes see them within 30 seconds.

Users manage organizations with `scn`; an organization or member argument can also be a URL, `http://host:7080/acme`:

```sh
scn org create acme [--internal]
scn org member add acme bob --role developer     # or maintainer, owner, reporter; again to change the role
scn org member ls acme
scn org member rm acme bob                      # members can remove themselves
scn org visibility acme internal
scn org ls [--mine]
scn org info acme
scn org rm acme
```

Operators can manage organizations directly in the metadata database, with the same storage options as `scion server`:

```sh
scion admin org create acme --owner alice [--visibility internal]
scion admin org member add acme bob --role developer
scion admin org member ls acme
scion admin org visibility acme internal
scion admin org ls
scion admin org rm acme
```

## GitHub login

People can sign in with their GitHub accounts. This is the easiest setup for open-source deployments.

1. **Register an OAuth app.** On GitHub, go to Settings → Developer settings → OAuth Apps → New OAuth App. Any homepage and callback URL will do, since the device flow uses neither. Tick **Enable Device Flow**.
2. **Configure the server.** Give it the app's client id, and run it with a signing key (`--jwt-key`):

   ```toml
   # auth.toml
   [github]
   client_id = "Ov23li…"
   # subject_prefix = "gh-"                 # optional: subjects become gh-LOGIN
   # web_url = "https://github.example.com" # GitHub Enterprise
   # api_url = "https://github.example.com/api/v3"
   ```

3. **Sign in.** `scn login http://scion.example.com:7080` shows a code to enter at github.com/login/device. The server reads the GitHub login with GitHub's token, then issues its own refresh and access tokens; GitHub's token is not kept.

**GitHub organizations and teams as groups.** With `orgs` and a token that can read their members (a PAT or app token with `read:org`), members of those organizations get the group `github:ORG`, and members of each team also get `github:ORG/TEAM`. The members are kept in memory and fetched again every `refresh` (5 minutes by default), in the background, while the previous answer is served. If a fetch fails, the previous groups stay. Policies can then name the groups:

```toml
[github]
client_id = "Ov23li…"
orgs = ["opencsg"]
token_env = "GITHUB_TOKEN"
# refresh = "5m"
```

```toml
# policy
[[rule]]
subjects = ["group:github:opencsg/core"]
repos    = ["opencsg/*"]
actions  = ["land"]
```

The groups apply to subjects that are a GitHub login with the `subject_prefix`, whichever way they signed in.

The subject is the GitHub login, so policies, organizations and personal namespaces (`user/LOGIN/…`) use it directly. A `subject_prefix` must keep the subject a valid name segment (letters, digits, `-`, `_`, `.`) for personal namespaces to work. `[github]` and `[oidc]` are alternatives: set one of them.

## Connecting starhub-server

What starhub-server does:
- It signs JWTs with an HS256 shared secret (`STARHUB_JWT_SIGNING_KEY`); `iss` is `OpenCSG`, and the user name is in the `current_user` claim.
- It also has opaque access tokens, checked by its user service.
- It computes permissions with rebac in its own database, telling public and private repositories apart.

The matching configuration:

```toml
[[issuer]]
issuer = "OpenCSG"
hs256_secret_env = "STARHUB_JWT_SIGNING_KEY"
subject_claim = "current_user"

[introspection]                        # starhub's access tokens
url = "http://starhub-server:8080/api/v1/internal/scion/introspect"
bearer_env = "STARHUB_API_TOKEN"
subject_claim = "username"

[authz_webhook]                        # permissions come from starhub's rebac (anonymous reads of public repositories included)
url = "http://starhub-server:8080/api/v1/internal/scion/authz"
bearer_env = "STARHUB_API_TOKEN"
cache_ttl = "30s"
```

starhub needs two internal endpoints, both thin wrappers over its existing logic:
- **`introspect`:** takes `token=…` as in RFC 7662, calls `VerifyByAccessToken` and returns `{"active": true, "username": "…"}`.
- **`authz`:** takes requests in the callback format above, maps Scion's repository name (such as `models/ns/name`) back to the starhub repository and calls `CheckUserRepoPermission` (read, write or admin).
  - `land` maps to write, or to write plus protected-branch rules if needed.
  - `anonymous` is allowed only to read public repositories.

`scn login` can go through the Casdoor OIDC that starhub uses (`[oidc]`), or use `scn login --token <starhub access token>`.

## Caches and the daemon

The regional cache (`scion cache`) and the local daemon do not parse tokens.
- **Read permission:** before returning objects (local hits included), they call `CheckAccess(repo, read)` on the server with the caller's own token, and cache the result per token and repository for 60 seconds. A revoked token or a changed policy takes effect in cache layers within 60 seconds.
- **The daemon offline:** when the server is unreachable (rather than refusing), identities confirmed within 24 hours still pass the read check. Reads also need redactions to be current, though ([redaction.md](redaction.md)): repositories last synced more than 10 seconds ago return `UNAVAILABLE`, so offline the local cache can mostly not be read.
- **Warming:** when the server requires authentication, the regional cache warms up as the identity given by `--warm-token` (`SCION_CACHE_WARM_TOKEN`).

## The client

```sh
scn login                 # log in in the browser (the server has [oidc] configured)
scn login --token TOKEN   # a PAT or external token; --with-token reads it from standard input
scn login SERVER TOKEN     # the same, with the server (either order)
scn whoami
scn logout
```

- **Where credentials go:** per server address. Signed macOS builds (the released ones) keep them in the Keychain; other builds, and other platforms, in `~/.config/scion/credentials.json` (mode 0600). macOS asks whether a program may read a Keychain item each time it is a different unsigned binary, so a locally built `scn` would ask after every build, and an agent with no screen would wait for the answer; a signed `scn` is trusted across versions. `SCION_CREDENTIALS=file` or `keychain` chooses, and `SCION_CREDENTIALS_FILE` names another file. A Keychain that does not answer within 3 seconds is skipped, with a note to use `SCION_TOKEN`.
- **CI:** set `SCION_TOKEN` (a PAT or an external token), which takes precedence over saved credentials.
- **TLS:** `https://` servers are checked against the Mozilla root certificates built into `scn`. `SCION_TLS_CA_FILE=PATH` adds a private CA (PEM); `SCION_TLS_ROOTS=native` also trusts the operating system's store, which on macOS can add many seconds to every connection.
