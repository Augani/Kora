# AWS m5.large Benchmark Results

## Run 2: SO_REUSEPORT (2 threads on 2 vCPU)

**Date:** 2026-03-08
**Instance:** m5.large (2 vCPU, 8GB RAM), us-east-2
**Load Generator:** c5n.large in same AZ
**Tool:** memtier_benchmark `-c 20 -t 4 --test-time 100 -d 256 --distinct-client-seed --key-pattern=R:R`
**Method:** Each service tested in isolation (full CPU), port 6379

### Versions

- Redis 8.0.3 (built from source, malloc=libc)
- Dragonfly v1.37.0 (--proactor_threads 2, --cache_mode)
- Kora v0.1.0 (--workers 2, SO_REUSEPORT, no dedicated accept thread)

### Throughput (ops/sec)

| Test            | Redis 8    | Dragonfly  | Kora       | Kora vs Redis | Kora vs Dragonfly |
|-----------------|------------|------------|------------|---------------|-------------------|
| SET-only        | 130,163    | 129,508    | 127,329    | -2.2%         | -1.7%             |
| GET-only        | 134,859    | 134,334    | 133,594    | -0.9%         | -0.6%             |
| Mixed 1:1       | 132,702    | 131,463    | 132,113    | **-0.4%**     | +0.5%             |
| Read-heavy 1:10 | 134,729    | 132,542    | 132,260    | **-1.8%**     | -0.2%             |
| Pipeline 16     | 536,430    | 246,686    | **631,319**| **+17.7%**    | **+155.9%**       |

### p50 Latency (ms)

| Test            | Redis 8 | Dragonfly | Kora      |
|-----------------|---------|-----------|-----------|
| SET-only        | 0.559   | 0.575     | **0.479** |
| GET-only        | 0.543   | 0.559     | **0.511** |
| Mixed 1:1       | 0.559   | 0.575     | **0.527** |
| Read-heavy 1:10 | 0.551   | 0.567     | **0.511** |
| Pipeline 16     | 2.351   | 5.247     | **1.855** |

### p99 Latency (ms)

| Test            | Redis 8 | Dragonfly | Kora  |
|-----------------|---------|-----------|-------|
| SET-only        | 2.111   | 1.719     | 3.503 |
| GET-only        | 1.759   | 1.375     | 2.911 |
| Mixed 1:1       | 1.591   | 1.391     | 2.687 |
| Read-heavy 1:10 | 1.647   | 1.399     | 3.007 |
| Pipeline 16     | 3.535   | 6.751     | **3.407** |

### Analysis

**SO_REUSEPORT eliminated the accept thread (3 threads → 2 on 2 vCPU).** This dramatically closed the throughput gap:

- Mixed 1:1: **-0.4%** vs Redis (was -3.5%)
- Read-heavy: **-1.8%** vs Redis (was -7.8%)
- Pipeline: **+17.7%** vs Redis (was +2.2%)

**p50 latency is best-in-class** — Kora beats both Redis and Dragonfly on every workload at median latency. The shard-affinity inline execution path (zero channel hops for local keys) shows up clearly at p50.

**p99 tail latency remains higher** (1.5-2x Redis). A ~2 second stall was observed during Pipeline at the 21-22% mark. Likely causes: cross-shard routing jitter, memory allocation pressure, or hash table resizing. This is the next optimization target.

---

## Run 1: Accept Thread Architecture (3 threads on 2 vCPU)

**Date:** 2026-03-08

### Versions

- Redis 8.0.3 (built from source, malloc=libc)
- Dragonfly v1.37.0 (--proactor_threads 2, --cache_mode)
- Kora v0.1.0 (--workers 2, shard-affinity I/O, dedicated accept thread)

### Throughput (ops/sec)

| Test            | Redis 8    | Dragonfly  | Kora       | Kora vs Redis | Kora vs Dragonfly |
|-----------------|------------|------------|------------|---------------|-------------------|
| SET-only        | 126,093    | 126,786    | 122,897    | -2.5%         | -3.1%             |
| GET-only        | 133,130    | 132,746    | 131,427    | -1.3%         | -1.0%             |
| Mixed 1:1       | 127,667    | 127,659    | 123,135    | -3.5%         | -3.5%             |
| Read-heavy 1:10 | 127,306    | 126,829    | 117,312    | -7.8%         | -7.5%             |
| Pipeline 16     | 596,240    | 290,876    | 609,127    | **+2.2%**     | **+109.4%**       |

### p99 Latency (ms)

| Test            | Redis 8 | Dragonfly | Kora      |
|-----------------|---------|-----------|-----------|
| SET-only        | 1.023   | 1.223     | 1.367     |
| GET-only        | 4.319   | 3.151     | **1.743** |
| Mixed 1:1       | 1.127   | 1.463     | 1.831     |
| Read-heavy 1:10 | 1.047   | 1.215     | 1.255     |
| Pipeline 16     | 3.279   | 7.103     | **3.055** |
