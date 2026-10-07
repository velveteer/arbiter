# Installation

Add the repository to your project. Replace `arbiter-simple` with
`arbiter-orville` or `arbiter-hasql` in the lists below to use that backend.
Orville also needs `arbiter-libpq` for the listener, and Hasql also needs `pqi-ffi`.

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
