# Backend Integration

The `MonadArbiter` typeclass separates the core from database libraries.
Arbiter provides three adapters: [arbiter-simple](simple.md),
[arbiter-orville](orville.md), and [arbiter-hasql](hasql.md). Use the adapter for
the database library in your application if you want to share connections.

## Benchmarks

Throughput in jobs/sec with `arbiter-hasql` and prepared claim statements (the
default). PostgreSQL 18, GHC 9.14.1, Apple M5 Pro. Cells are single-job mode /
batched mode (batch size 10). Trial-to-trial variance is 1-5% unless noted.

**Pre-loaded queue** (1M jobs, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 9,799 | 38,122 |
| ungrouped, dormant | 9,765 | 38,187 |
| 50k groups | 7,587 | 31,293 |
| 50k groups, scheduled + backoff | 2,506 | 21,134 |
| 50k groups, dormant | 7,930 | 30,711 |

*scheduled + backoff*: a fifth of jobs scheduled seconds out, a fifth failing
once into backoff. *dormant*: half the backlog parked 30 days out.

**Steady state** (10 producers inserting continuously, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 9,368 | 17,666 |
| 5k groups | 6,521 | 16,840 |
| 5k groups, scheduled + backoff | 2,321 | 13,399 |

With OpenTelemetry tracing and metrics on, the ungrouped single-job rate is
8,921, a 5% cost. Batched steady state varies by 10%: producers and workers
contend for the same rows.

**Group size and skew** (300k jobs, 4 pools × 10 workers, 3 trials):

| Groups | Single | Batched | Group triggers, µs/job |
|--------|--------|---------|------------------------|
| 1 job/group | 7,993 | 6,363 | 384 / 415 |
| 10 jobs/group | 7,639 | 34,369 | 401 / 71 |
| 100 jobs/group | 6,540 | 32,642 | 420 / 95 |
| 1,000 jobs/group | 3,337 | 26,134 | 456 / 50 |
| 10,000 jobs/group | 1,037 | 9,882 | 463 / 49 |
| 80/20 skew, 1k groups, 10 hot | 2,806 | 17,126 | 620 / 93 |

A batch takes jobs from one group. With one job per group, batched mode claims
one job per batch and pays the batch overhead for it. The ungrouped queue runs
the group triggers at 7 µs/job.

**Admission gating** (steady state, 1 pool × 10 workers, 256 keys):

| | no gate | rate limit | concurrency | both |
|---------|--------|---------|----------------|-----------------|
| ungrouped | 2,678 / 11,452 | 2,452 / 8,030 | 1,800 / 7,478 | 1,786 / 5,355 |
| 5k groups | 863 / 5,528 | 817 / 4,153 | 673 / 3,780 | 655 / 3,098 |

In single-job mode the rate limit costs 5-8% and the concurrency limit 22-33%.
In batched mode one control costs 25-35% and both together 44-53%. These
reductions are in addition to the cost of grouping.
