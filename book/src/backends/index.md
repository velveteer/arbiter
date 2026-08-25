# Backend Integration

Arbiter's core is backend-agnostic through the `MonadArbiter` typeclass. Three
adapters ship with it: [arbiter-simple](simple.md),
[arbiter-orville](orville.md), and [arbiter-hasql](hasql.md). Pick the one that
matches the database library your application already uses. If throughput
decides it instead, the benchmarks below measure all three.

Throughput in jobs/sec, 4 pools × 10 workers, PostgreSQL 18, Apple M5 Pro.
hasql runs with prepared claim statements (the default).

**Pre-loaded queue** (1M jobs, 50k groups):

| Backend | Single | Batched | Grouped single | Grouped batched |
|---------|--------|---------|----------------|-----------------|
| hasql | 8,613 | 25,279 | 6,957 | 28,523 |
| orville | 6,926 | 19,195 | 5,051 | 21,195 |
| postgresql-simple | 5,639 | 19,634 | 4,387 | 20,903 |

**Steady-state** (10 producers inserting continuously, 5k groups):

| Backend | Single | Batched | Grouped single | Grouped batched |
|---------|--------|---------|----------------|-----------------|
| hasql | 8,964 | 15,933 | 3,676 | 16,333 |
| orville | 5,913 | 16,402 | 4,258 | 16,621 |
| postgresql-simple | 6,237 | 17,172 | 3,164 | 16,386 |

**Under a scheduled backlog** (1M jobs, 50k groups, cells are single /
batched):

| Backend | ungrouped dormant | grouped stress | grouped dormant |
|---------|-------------------|----------------|-----------------|
| hasql | 8,602 / 32,007 | 2,516 / 14,888 | 7,115 / 27,854 |
| orville | 7,012 / 22,113 | 2,459 / 13,253 | 5,056 / 21,032 |
| postgresql-simple | 5,703 / 22,523 | 2,393 / 13,981 | 4,389 / 20,724 |

*stress*: a fifth of jobs scheduled seconds-out, a fifth failing once into
backoff. *dormant*: half the backlog parked 30 days out.

**Admission gating** (hasql, prepared claims, 1 pool × 10 workers, 256 keys,
cells are single / batched):

| | no gate | rate limit | concurrency | both |
|---------|--------|---------|----------------|-----------------|
| ungrouped | 2,188 / 10,077 | 2,196 / 6,931 | 1,638 / 6,080 | 1,491 / 4,346 |
| grouped (5k groups) | 781 / 5,432 | 795 / 3,974 | 675 / 3,650 | 625 / 2,768 |

In single-job mode a rate limit is free and a concurrency cap costs 14% grouped
to 25% ungrouped. Batched mode pays more: one gate costs 27% to 40%, and both
together cost about half the throughput. A grouped workload pays these
percentages on top of the cost of grouping.
