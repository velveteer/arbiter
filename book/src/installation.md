# Installation

Add the packages to `build-depends`:

```text
build-depends:
    arbiter-core
  , arbiter-worker
  , arbiter-simple
  , arbiter-migrations
```

Replace `arbiter-simple` with `arbiter-orville` or `arbiter-hasql` to use that
backend. Make the same change in the `subdir` lists below. In `build-depends`,
Orville also needs `arbiter-libpq` for the listener, and Hasql also needs `pqi-ffi`.

To build from GitHub, add the repository.

**Cabal:** Add this source repository to `cabal.project`:

```text
source-repository-package
  type: git
  location: https://github.com/velveteer/arbiter.git
  tag: <commit-sha>
  subdir:
    arbiter-core
    arbiter-worker
    arbiter-simple
    arbiter-libpq
    arbiter-migrations
```

**Stack:** Add this source repository to `stack.yaml`:

```yaml
extra-deps:
  - git: https://github.com/velveteer/arbiter.git
    commit: <commit-sha>
    subdirs:
      - arbiter-core
      - arbiter-worker
      - arbiter-simple
      - arbiter-libpq
      - arbiter-migrations
```
