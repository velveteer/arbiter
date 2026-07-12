# syntax=docker/dockerfile:1
#
# Container image for the `arbiter` binary (migrate + serve). Build context is
# the repo root (cabal.project references every local package):
#
#   docker build -f arbiter-cli/Dockerfile -t arbiter:latest .
#
# Run:
#   docker run -p 8080:8080 -v $PWD/arbiter.toml:/etc/arbiter/arbiter.toml:ro arbiter:latest

# ---- build ----------------------------------------------------------------
FROM haskell:9.14-slim-bookworm AS build
WORKDIR /src

# Build-time C deps: libpq headers + pg_config for postgresql-libpq.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libpq-dev pkg-config zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

ENV CABAL_DIR=/root/.cabal
COPY . .
RUN --mount=type=cache,target=/root/.cabal \
    --mount=type=cache,target=/src/dist-newstyle \
    cabal update \
 && cabal build exe:arbiter --disable-tests --disable-benchmarks \
 && mkdir -p /out \
 && cp "$(cabal list-bin exe:arbiter)" /out/arbiter

# ---- runtime --------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libpq5 libgmp10 libffi8 libnuma1 zlib1g ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Otherwise the locale encoder is ASCII and a non-ASCII byte aborts the write.
ENV LANG=C.UTF-8

RUN useradd -r -u 10001 arbiter
COPY --from=build /out/arbiter /usr/local/bin/arbiter

USER arbiter
EXPOSE 8080
ENTRYPOINT ["arbiter"]
# Default to serving; override with ["migrate","-c","/etc/arbiter/arbiter.toml"].
CMD ["serve", "-c", "/etc/arbiter/arbiter.toml"]
