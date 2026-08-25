# Installation

Install directly from GitHub:

**Cabal** - add to your `cabal.project`:

```
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

**Stack** - add to your `stack.yaml`:

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
