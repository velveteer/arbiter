# Backend Integration

The `MonadArbiter` typeclass separates the core from database libraries.
Arbiter provides three adapters: [arbiter-simple](simple.md),
[arbiter-orville](orville.md), and [arbiter-hasql](hasql.md). Use the adapter for
the database library in your application if you want to share connections.

## Benchmarks

Throughput in jobs/sec with `arbiter-hasql` and prepared claim statements (the
default). PostgreSQL 18, GHC 9.14.1, Apple M5 Pro. Cells are single-job mode /
batched mode (batch size 10). Trial-to-trial variance is 1-6% unless noted.

**Pre-loaded queue** (1M jobs, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 10,491 | 39,918 |
| ungrouped, dormant | 10,407 | 40,219 |
| 50k groups | 8,116 | 31,559 |
| 50k groups, scheduled + backoff | 2,550 | 22,126 |
| 50k groups, dormant | 8,106 | 30,878 |

*scheduled + backoff*: a fifth of jobs scheduled seconds out, a fifth failing
once into backoff. *dormant*: half the backlog parked 30 days out.

**Steady state** (10 producers inserting continuously, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 10,143 | 16,940 |
| 5k groups | 5,322 | 16,547 |
| 5k groups, scheduled + backoff | 2,302 | 12,685 |

With OpenTelemetry tracing and metrics on, the ungrouped single-job rate is
9,522, a 6% cost. Producers and workers contend for the same rows, so the
ungrouped batched cell varies by 17% and the 5k-group single cell by 20%.

**Group size and skew** (300k jobs, 4 pools × 10 workers, 3 trials):

| Groups | Single | Batched | Group triggers, µs/job |
|--------|--------|---------|------------------------|
| 1 job/group | 8,475 | 6,840 | 328 / 429 |
| 10 jobs/group | 8,364 | 35,637 | 353 / 66 |
| 100 jobs/group | 6,807 | 36,867 | 352 / 72 |
| 1,000 jobs/group | 3,209 | 24,857 | 417 / 48 |
| 10,000 jobs/group | 888 | 9,791 | 511 / 60 |
| 80/20 skew, 1k groups, 10 hot | 2,719 | 17,043 | 577 / 87 |

A batch takes jobs from one group. With one job per group, batched mode claims
one job per batch and pays the batch overhead for it. The ungrouped queue runs
the group triggers at 7 µs/job.

**Admission gating** (steady state, 1 pool × 10 workers, 256 keys):

| | no gate | rate limit | concurrency | both |
|---------|--------|---------|----------------|-----------------|
| ungrouped | 3,370 / 12,236 | 2,359 / 6,826 | 1,719 / 7,208 | 1,441 / 4,407 |
| 5k groups | 817 / 5,327 | 886 / 4,288 | 681 / 3,716 | 619 / 3,115 |

These cells vary by 4-23% between trials. In single-job mode the rate limit
costs up to 30% and the concurrency limit 17-49%. In batched mode one control
costs 20-44% and both together 42-64%. These reductions are in addition to the
cost of grouping.
