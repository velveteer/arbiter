# Backend Integration

`MonadArbiter` separates the core from the database library. Three adapters
ship: [arbiter-simple](simple.md), [arbiter-orville](orville.md), and
[arbiter-hasql](hasql.md). Each shares connections with its database library.

## Benchmarks

Jobs/sec with `arbiter-hasql`, prepared claims, PostgreSQL 18, GHC 9.14.1,
Apple M5 Pro. Single-job mode / batched mode (batch size 10).

**Pre-loaded queue** (1M jobs, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 17,472 | 64,823 |
| ungrouped, dormant | 17,569 | 49,952\* |
| 50k groups | 9,089 | 30,533 |
| 50k groups, scheduled + backoff | 2,539 | 23,743 |
| 50k groups, dormant | 9,582 | 30,887 |

*scheduled + backoff*: a fifth of jobs scheduled seconds out, a fifth failing
once into backoff. *dormant*: half the backlog parked 30 days out.

\* The workers drain all 500k ready jobs before the 10-second trial ends. The
real rate is higher.

**Steady state** (10 producers inserting continuously, 4 pools × 10 workers):

| Queue | Single | Batched |
|-------|--------|---------|
| ungrouped | 15,185 | 18,336 |
| ungrouped, OpenTelemetry on | 15,192 | |
| 5k groups | 7,117 | 16,190 |
| 5k groups, scheduled + backoff | 2,307 | 13,499 |

**Group size and skew** (300k jobs, 4 pools × 10 workers):

| Groups | Single | Batched | Group triggers, µs/job |
|--------|--------|---------|------------------------|
| 1 job/group | 10,154 | 6,682 | 341 / 410 |
| 10 jobs/group | 10,031 | 33,665 | 363 / 58 |
| 100 jobs/group | 8,101 | 34,529 | 396 / 64 |
| 1,000 jobs/group | 3,669 | 23,664 | 424 / 42 |
| 10,000 jobs/group | 953 | 9,878 | 409 / 48 |
| 80/20 skew, 1k groups, 10 hot | 2,955 | 17,578 | 621 / 86 |

**Admission gating** (steady state, 10 producers, 256 keys):

| | no gate | rate limit | concurrency | both |
|---|---|---|---|---|
| ungrouped, single, 1 pool | 3,594 | 3,244 | 2,025 | 1,547 |
| ungrouped, single, 4 pools | 6,956 | 6,242 | 4,514 | 4,461 |
| ungrouped, batched, 1 pool | 13,480 | 12,027 | 7,104 | 6,358 |
| 5k groups, single, 1 pool | 936 | 939 | 754 | 750 |
| 5k groups, single, 4 pools | 2,864 | 2,611 | 1,926 | 1,894 |
| 5k groups, batched, 1 pool | 6,811 | 5,270 | 3,744 | 3,572 |

One pool is 10 workers and one dispatcher.
