# Development

## Repository layout

```
proto/scion/v1/      protocol definitions (.proto), language-neutral; buf lint / breaking checks
SPEC.md              object model and canonical encoding
testdata/vectors/    golden test vectors every implementation must pass

common/              shared by server and client (may depend only on common)
  protocol/          scion-proto       Rust code generated from the protos
  object/            scion-object      ObjectId, canonical encoding, Tree / Changeset (pure, no IO)
  flight/            scion-flight      single-flight request coalescing (server caches and the daemon)

server/              server side (may depend on common and lower server crates)
  obs/               scion-obs         Prometheus metrics, OpenTelemetry tracing, gRPC deadlines
  store/             scion-store       MetaStore / BlobStore traits, in-memory backend, conformance suite
  store-sql/         scion-store-sql   SQLite / PostgreSQL metadata store
  store-blob/        scion-store-blob  local directory / S3-compatible object store
  auth/              scion-auth        tokens, external issuers (JWKS / HS256), introspection, OIDC, ACL
  tree/              scion-tree        incremental tree building
  merge/             scion-merge       server-side rebase: replay a delta onto a newer tree with three-way merge
  land/              scion-land        changeset creation, stack rebase, LandCoordinator, leases
  query/             scion-query       history queries (log / diff / blame / merge-base) and reindexing
  cache/             scion-cache       regional object cache (scion cache): foyer, rendezvous, hedging, warm-up
  api/               scion-api         gRPC services and server bootstrap
  scion/             scion             server binary (scion server / scion cache / scion admin)

client/              client side (may depend on common and client, never on server)
  sdk/               scion-client      Rust SDK
  wc/                scion-wc          working copy (clone / status / commit / land / sync)
  daemon/            scion-daemon      local L1 cache (scn daemon): object cache, transparent forwarding, prefetch
  gitimport/         scion-gitimport   git history import
  cli/               scion-cli         the scn binary

xtask/               maintenance tasks (cargo xtask layers: dependency direction check)
deploy/dev/          local development dependencies (SeaweedFS, PostgreSQL), and the server for trials
deploy/prd/          single-machine production compose (authentication required, secrets from .env)
docs/guide/          user guides
docs/design/         design docs and diagrams
```

Directory names describe location and role; crate names carry a `scion-` prefix to avoid clashing with crates.io packages such as `object`. Dependency direction is enforced in CI by `cargo xtask layers`.

## Building and testing

```sh
cargo test --workspace                                    # all tests
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask layers                                        # dependency direction check
cargo llvm-cov --workspace                                # coverage (cargo install cargo-llvm-cov)
SCION_BLESS=1 cargo test -p scion-object --test vectors   # regenerate golden vectors after adding some
docker compose -f deploy/dev/docker-compose.yml up -d     # SeaweedFS (S3) + PostgreSQL
```

**Testing requirements** ([server design doc §12.1](design/rust-server-architecture.md)): every function with logic has unit tests covering the normal path, edge cases and every error branch. Core crates need at least 90% line coverage, and new or changed code in a pull request needs at least 90%; CI enforces both.

## Releasing

1. Set the version in `Cargo.toml` (`[workspace.package] version`) and run `cargo build` so `Cargo.lock` follows.
2. Write the release notes in `docs/releases/vX.Y.Z.md` (without it, GitHub generates notes from the commits).
3. Commit, then tag and push: `git tag vX.Y.Z && git push origin vX.Y.Z`.

The [release workflow](../.github/workflows/release.yml) checks that the tag matches the version, builds `scion` and `scn` for Linux (x86_64, aarch64) and macOS (arm64, x86_64), and publishes `scion-server-vX.Y.Z-<target>.tar.gz`, `scn-vX.Y.Z-<target>.tar.gz` and `SHA256SUMS` as a GitHub release. `gh workflow run release -f tag=vX.Y.Z` reruns it for an existing tag, replacing the files. The macOS binaries are signed with OpenCSG's Developer ID, the certificate [csglite](https://github.com/OpenCSGs/csglite) uses: without it, macOS refuses `scn` and `scion` connections to other machines on the local network. The release fails without the repository secrets `APPLE_CERTIFICATE` (the Developer ID `.p12`, base64-encoded) and `APPLE_CERTIFICATE_PASSWORD`; the variables `APPLE_SIGNING_IDENTITY` and `APPLE_TEAM_ID` (default `3S93B6Z434`) are optional. `scripts/sign-macos.sh BINARY…` signs a local build the same way. The container image is built and pushed separately (see [deployment](guide/deploy.md)).
