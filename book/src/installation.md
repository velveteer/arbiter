# Installation

Install directly from GitHub:

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
      - arbiter-migrations
```

Replace `arbiter-simple` with `arbiter-orville` or `arbiter-hasql` depending on your backend.
