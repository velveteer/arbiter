# syntax=docker/dockerfile:1
#
# Multi-stage build for the arbiter demo server. Build context is the repo
# root (the binary embeds arbiter-servant-ui/static at compile time via
# embedDir, and cabal.project references every local package).
#
#   docker build -f arbiter-demo/Dockerfile -t arbiter-demo .

# ---- build ----------------------------------------------------------------
FROM haskell:9.12-slim-bookworm AS build
WORKDIR /src

# Build-time C deps: libpq headers + pg_config for postgresql-libpq, plus
# pkg-config and zlib headers. (The runtime stage carries the shared libs.)
RUN apt-get update \
 && apt-get install -y --no-install-recommends libpq-dev pkg-config zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

# The exe depends on the local library packages and embeds
# arbiter-servant-ui/static via embedDir, so the whole repo is the build
# context. The cabal store and build tree live in BuildKit caches, so rebuilds
# reuse already-compiled dependencies.
ENV CABAL_DIR=/root/.cabal
COPY . .
RUN --mount=type=cache,target=/root/.cabal \
    --mount=type=cache,target=/src/dist-newstyle \
    cabal update \
 && cabal build exe:arbiter-demo --disable-tests --disable-benchmarks \
 && mkdir -p /out \
 && cp "$(cabal list-bin exe:arbiter-demo)" /out/arbiter-demo

# ---- runtime --------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libpq5 libgmp10 libffi8 libnuma1 zlib1g ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 10001 demo
COPY --from=build /out/arbiter-demo /usr/local/bin/arbiter-demo

USER demo
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["arbiter-demo"]
