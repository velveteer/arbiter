# Backend Integration

The `MonadArbiter` typeclass separates the core from database libraries.
Arbiter provides three adapters: [arbiter-simple](simple.md),
[arbiter-orville](orville.md), and [arbiter-hasql](hasql.md). Use the adapter for
the database library in your application if you want to share connections.

## Benchmarks

Jobs/sec with `arbiter-hasql`, prepared claims, PostgreSQL 18, GHC 9.14.1,
Apple M5 Pro. Single-job mode / batched mode (batch size 10).

**Pre-loaded queue** (1M jobs, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 14,354 | 47,018 |
| ungrouped, dormant | 14,247 | 46,188 |
| 50k groups | 9,289 | 31,959 |
| 50k groups, scheduled + backoff | 2,698 | 23,505 |
| 50k groups, dormant | 9,855 | 30,873 |

*scheduled + backoff*: a fifth of jobs scheduled seconds out, a fifth failing
once into backoff. *dormant*: half the backlog parked 30 days out.

**Steady state** (10 producers inserting continuously, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 13,924 | 18,482 |
| ungrouped, OpenTelemetry on | 13,738 | |
| 5k groups | 7,129 | 16,362 |
| 5k groups, scheduled + backoff | 2,707 | 13,542 |

**Group size and skew** (300k jobs, 4 pools × 10 workers):

| Groups | Single | Batched | Group triggers, µs/job |
|--------|--------|---------|------------------------|
| 1 job/group | 10,275 | 7,193 | 363 / 415 |
| 10 jobs/group | 10,156 | 33,148 | 387 / 52 |
| 100 jobs/group | 8,707 | 34,625 | 399 / 73 |
| 1,000 jobs/group | 3,745 | 24,505 | 455 / 39 |
| 10,000 jobs/group | 1,046 | 10,362 | 384 / 37 |
| 80/20 skew, 1k groups, 10 hot | 2,896 | 17,567 | 635 / 82 |

**Admission gating** (steady state, 10 producers, 256 keys):

| | no gate | rate limit | concurrency | both |
|---|---|---|---|---|
| ungrouped, single, 1 pool | 5,037 | 3,482 | 2,683 | 2,506 |
| ungrouped, single, 4 pools | 7,781 | 4,956 | 3,744 | 3,499 |
| ungrouped, batched, 1 pool | 17,249 | 12,033 | 8,142 | 7,195 |
| 5k groups, single, 1 pool | 962 | 929 | 749 | 766 |
| 5k groups, single, 4 pools | 2,654 | 2,583 | 2,336 | 2,268 |
| 5k groups, batched, 1 pool | 5,988 | 5,266 | 4,046 | 3,799 |

One pool is 10 workers and one dispatcher.
