# Scion 镜像：包含服务端 `scion` 和客户端 `scn`。
#
#   docker build -t scion .
#   docker run -p 7080:7080 -v scion-data:/data scion            # Lite 档（SQLite + 本地目录），要求认证
#
# 拉不到 Docker Hub 时把基础镜像换成镜像站，例如
#   --build-arg RUST_IMAGE=docker.m.daocloud.io/library/rust:1.99-slim-bookworm \
#   --build-arg RUNTIME_IMAGE=docker.m.daocloud.io/library/debian:bookworm-slim \
#   --build-arg DEBIAN_MIRROR=mirrors.ustc.edu.cn
# 同时构建 linux/amd64 与 linux/arm64（交叉编译，不需要模拟）：
#   docker buildx build --platform linux/amd64,linux/arm64 -t opencsghq/scion:latest --push .
ARG RUST_IMAGE=rust:1.99-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim
# A Debian mirror host for apt, such as mirrors.ustc.edu.cn, when deb.debian.org is unreachable
ARG DEBIAN_MIRROR=

# The build stage runs on the build machine's own architecture and cross-compiles for the image's
# (TARGETARCH), so a multi-platform build (linux/amd64 + linux/arm64) needs no emulation
FROM --platform=$BUILDPLATFORM ${RUST_IMAGE} AS build
ARG TARGETARCH
ARG BUILDARCH
ARG DEBIAN_MIRROR
# release 用于发布；CI 的冒烟测试用 dev 缩短编译时间
ARG CARGO_PROFILE=release
WORKDIR /src
RUN case "${TARGETARCH}" in \
      amd64) echo x86_64-unknown-linux-gnu > /target; pkg=x86-64-linux-gnu; gcc=x86_64-linux-gnu ;; \
      arm64) echo aarch64-unknown-linux-gnu > /target; pkg=aarch64-linux-gnu; gcc=aarch64-linux-gnu ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && rustup target add "$(cat /target)" \
    && if [ "${TARGETARCH}" != "${BUILDARCH}" ]; then \
           if [ -n "${DEBIAN_MIRROR}" ]; then sed -i "s|deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources; fi \
         && apt-get update && apt-get install -y --no-install-recommends "gcc-${pkg}" "libc6-dev-${TARGETARCH}-cross" \
         && rm -rf /var/lib/apt/lists/* && echo "${gcc}-gcc" > /cc; \
       else echo gcc > /cc; fi
COPY . .
# 依赖与编译结果放在 BuildKit 缓存里，第三方依赖不用重新编译
# Cargo compares source mtimes, and a normalized source snapshot has mtime 0, so a cached workspace
# crate would look fresh. Clean the workspace crates first. sharing=locked stops a second build
# from using the cache while this one cleans it.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,id=scion-target-${TARGETARCH},sharing=locked \
    t=$(cat /target) && cc=$(cat /cc) && T=$(echo "$t" | tr a-z- A-Z_) \
    && export "CC_$(echo "$t" | tr - _)=$cc" "CARGO_TARGET_${T}_LINKER=$cc" \
    && cargo clean --workspace --profile "${CARGO_PROFILE}" --target "$t" \
    && cargo build --profile "${CARGO_PROFILE}" --locked --target "$t" -p scion -p scion-cli \
    && dir=$([ "${CARGO_PROFILE}" = dev ] && echo debug || echo "${CARGO_PROFILE}") \
    && mkdir -p /out \
    && cp "target/$t/${dir}/scion" "target/$t/${dir}/scn" /out/

FROM ${RUNTIME_IMAGE}
ARG DEBIAN_MIRROR
RUN if [ -n "${DEBIAN_MIRROR}" ]; then sed -i "s|deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources; fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data scion \
    && mkdir -p /data \
    && chown scion /data
COPY --from=build /out/scion /out/scn /usr/local/bin/
USER scion
# 镜像里的 scn 直连服务端，不拉起本机 daemon；管理命令（scion admin … --lite）默认用 /data
ENV SCION_NO_DAEMON=1 SCION_DATA_DIR=/data
# 7080 gRPC（scion server）、7090 gRPC（scion cache）、9464 Prometheus 指标（--metrics-listen）
EXPOSE 7080 7090 9464
VOLUME ["/data"]
ENTRYPOINT ["scion"]
# Requests need a token by default; see docs/guide/deploy.md for the first admin token
CMD ["--log-format", "json", "server", "--listen", "0.0.0.0:7080", "--lite", "--data-dir", "/data", "--auth-required"]
