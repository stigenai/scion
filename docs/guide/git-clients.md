# Using git with Scion repositories

English | [简体中文](git-clients.zh-CN.md)

The server speaks git's smart HTTP protocol at the same address and URL as `scn`, so the git CLI, CI systems and IDEs can clone, fetch and push a Scion repository. How the gateway works is in the [design](../design/git-gateway.md).

- [Clone and fetch](#clone-and-fetch)
- [Push](#push)
- [Credentials](#credentials)
- [SHAs](#shas)
- [Not available yet](#not-available-yet)
- [Turning it off](#turning-it-off)

## Clone and fetch

A repository's git URL is its `scn` URL; a `.git` suffix works too:

```sh
git clone http://scion.example.com:7080/agentlab/app
cd app
git pull                        # fast-forwards to what landed since
git fetch origin feature        # any branch
git ls-remote origin            # HEAD, refs/heads/*, refs/tags/*
```

- Branches appear as `refs/heads/<name>`, tags (`tags/<name>` in Scion) as `refs/tags/<name>`, and `HEAD` points at the repository's default branch.
- git 2.x clients work, with protocol v2 (git's default since 2.26) and the older v0.
- A clone receives every commit of the branches it fetches, not just the snapshot `scn clone` downloads. For CI, `git clone --depth 1` fetches the newest commit only; `--shallow-since=DATE` works too, and `git fetch --depth N` or `--unshallow` deepens a shallow clone later.
- For large repositories, a partial clone leaves files out and fetches them when git needs them (at checkout, for `git log -p` and so on): `git clone --filter=blob:none URL`, or `--filter=blob:limit=1m` for files over 1 MiB only.

## Push

```sh
git push origin main            # lands the new commits on main
git push origin feature         # creates or moves a branch
git push origin v1.0            # a tag, lightweight or annotated
git push origin :old-branch     # deletes a branch
```

- **Annotated tags keep their tag objects.** The server keeps the pushed object byte for byte (a signature included), so a later fetch gets the same tag SHA; `scn show v1.0` and `scn tag -n` show its message. An annotated tag made with `scn tag -a` is served as a tag object built from its message and tagger. `ls-remote` lists both the tag and the commit it names (`v1.0^{}`).
- **Commits keep their SHAs.** Each pushed commit becomes a Scion commit that `scn log` shows, with the same author, committer, times and message; git, `scn` and `GetGitShas` all see the SHA that was pushed. Signed commits keep their signatures.
- **Branches only fast-forward.** A push lands like `scn land` with no rebase: when someone landed first, git says `rejected … (fetch first)`; run `git pull --rebase` and push again. `git push --force` moves the branch anyway, with the same `land` permission on the branch as `scn` needs.
- **Permissions:** pushing needs `write` on the repository (on the changed paths, when the policy limits paths) and `land` on each ref, as for `scn`. Pre-land checks and ref events apply to pushes too.
- **Limits:** a push may be up to 16 GiB (the server spools it to a temporary file), and each file up to the server's upload limit (`max_upload_object_bytes`).

## Credentials

The gateway takes the same tokens as `scn`, as the password of HTTP Basic authentication with any user name:

```sh
git clone http://git:$SCION_TOKEN@scion.example.com:7080/agentlab/app
```

Or let a credential helper keep the token (`git config --global credential.helper store`, then enter any user name and the token when git asks). `Authorization: Bearer <token>` works too, for tools that send headers (`git -c http.extraHeader="Authorization: Bearer $SCION_TOKEN" clone …`).

- Without credentials, git asks for them when the repository is not readable anonymously.
- A token that cannot read the repository gets `403`.
- The same policy applies as for `scn` (see [authentication](auth.md)): fetching needs `read` on the repository.

## SHAs

Every commit has one git SHA, the one `GetGitShas` and the SDK's `git_shas` report ([git SHAs](git-sha.md)):

- **Commits imported from git** keep their original SHAs, so a clone has exactly the source repository's objects, signed commits included. Commits with submodules cannot be served yet, nor signed commits imported before 0.1.11 (see below).
- **Commits made with `scn`** get a SHA computed from their content, with a `scion-id` header in the commit object. `git fsck --strict` accepts it.

## Not available yet

| What | What happens |
|---|---|
| `git push --atomic` | git refuses it: Scion updates one ref at a time |
| Tags of trees, blobs or other tags | `git push` reports `only tags of commits are supported` |
| Submodules, and modes other than files, executables and symbolic links | The push is refused with the path |
| Refs outside `refs/heads/*` and `refs/tags/*` (`refs/notes/*` and so on) | The push is refused for that ref |
| Partial clones with filters other than `blob:none` and `blob:limit` (`tree:0`, `sparse:…`) | Refused with a message |
| `git fetch --deepen` and `--shallow-exclude` | Refused with a message; `--depth`, `--shallow-since` and `--unshallow` work |
| Commits with submodules, or signed commits imported before 0.1.11 | The fetch fails with `fatal: remote error: git commit … was imported without its raw git object, …; import the git repository again into a new Scion repository with scn 0.1.11 or later (this one keeps serving the old import), or clone it with scn` |
| History containing a [redacted](redaction.md) file | The fetch fails: the file's SHA covers content nobody may read any more |
| SSH URLs | Use HTTP(S) |

## Turning it off

`scion server --no-git` (or `SCION_NO_GIT=1`) stops serving git, pushes included; the gRPC API is unaffected.
