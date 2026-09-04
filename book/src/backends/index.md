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
| ungrouped, OpenTelemetry on | 9,522 | |
| 5k groups | 5,322 | 16,547 |
| 5k groups, scheduled + backoff | 2,302 | 12,685 |

**Group size and skew** (300k jobs, 4 pools × 10 workers):

| Groups | Single | Batched | Group triggers, µs/job |
|--------|--------|---------|------------------------|
| 1 job/group | 8,475 | 6,840 | 328 / 429 |
| 10 jobs/group | 8,364 | 35,637 | 353 / 66 |
| 100 jobs/group | 6,807 | 36,867 | 352 / 72 |
| 1,000 jobs/group | 3,209 | 24,857 | 417 / 48 |
| 10,000 jobs/group | 888 | 9,791 | 511 / 60 |
| 80/20 skew, 1k groups, 10 hot | 2,719 | 17,043 | 577 / 87 |

**Admission gating** (steady state, 10 producers, 256 keys):

| | no gate | rate limit | concurrency | both |
|---|---|---|---|---|
| ungrouped, single, 1 pool | 4,027 | 2,946 | 2,162 | 1,831 |
| ungrouped, single, 4 pools | 5,318 | 5,346 | 3,686 | 2,736 |
| ungrouped, batched, 1 pool | 16,521 | 10,304 | 7,659 | 6,246 |
| 5k groups, single, 1 pool | 831 | 871 | 688 | 661 |
| 5k groups, single, 4 pools | 2,270 | 2,220 | 1,987 | 2,011 |
| 5k groups, batched, 1 pool | 5,512 | 5,188 | 3,735 | 3,379 |

One pool is 10 workers and one dispatcher.
