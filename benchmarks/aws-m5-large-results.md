# AWS Benchmark Results

## Run 3: m5.xlarge (4 vCPU) — Multi-Core Scaling

**Date:** 2026-03-08
**Instance:** m5.xlarge (4 vCPU, 16GB RAM), us-east-2
**Load Generator:** c5n.xlarge (4 vCPU) in same AZ
**Tool:** memtier_benchmark `-c 50 -t 4 --test-time 100 -d 256 --distinct-client-seed --key-pattern=R:R`
**Method:** Each service tested in isolation (full CPU), port 6379

### Versions

- Redis 8.0.3 (built from source, malloc=libc) — single-threaded
- Dragonfly v1.37.0 (--proactor_threads 4, --cache_mode)
- Kora v0.1.0 (--workers 4, SO_REUSEPORT, shard-affinity I/O)

### Throughput (ops/sec)

| Test            | Redis 8    | Dragonfly  | Kora       | Kora vs Redis | Kora vs Dragonfly |
|-----------------|------------|------------|------------|---------------|-------------------|
| SET-only        | 138,239    | 236,885    | **229,535**| **+66.0%**    | -3.1%             |
| GET-only        | 144,240    | 241,305    | **239,230**| **+65.9%**    | -0.9%             |
| Mixed 1:1       | 139,014    | 232,507    | **233,377**| **+67.9%**    | **+0.4%**         |
| Read-heavy 1:10 | 143,479    | 238,386    | **234,749**| **+63.6%**    | -1.5%             |
| Pipeline 16     | 510,705    | 389,286    | **769,374**| **+50.7%**    | **+97.7%**        |

### p50 Latency (ms)

| Test            | Redis 8 | Dragonfly | Kora      |
|-----------------|---------|-----------|-----------|
| SET-only        | 1.415   | 0.847     | **0.831** |
| GET-only        | 1.359   | 0.839     | **0.839** |
| Mixed 1:1       | 1.415   | 0.871     | **0.847** |
| Read-heavy 1:10 | 1.367   | 0.839     | 0.847     |
| Pipeline 16     | 6.303   | 8.191     | **4.063** |

### p99 Latency (ms)

| Test            | Redis 8 | Dragonfly | Kora      |
|-----------------|---------|-----------|-----------|
| SET-only        | 2.111   | 1.175     | 1.223     |
| GET-only        | 1.943   | 1.143     | 1.327     |
| Mixed 1:1       | 1.999   | 1.175     | 1.631     |
| Read-heavy 1:10 | 1.951   | 1.151     | 1.991     |
| Pipeline 16     | 9.087   | 10.815    | **7.519** |

### Analysis

**On 4 vCPU, Kora's multi-shard architecture dominates:**

- **66% faster than Redis** across all single-command workloads. Redis can only use 1 of 4 cores.
- **Matches Dragonfly** on single-command workloads (within 3%), despite Dragonfly's years of optimization.
- **Pipeline throughput**: 769K ops/sec — **51% over Redis, 98% over Dragonfly**.
- **Best p50 latency** on SET, Mixed, and Pipeline workloads.
- **Pipeline p99**: 7.5ms vs Redis 9.1ms (-17%) vs Dragonfly 10.8ms (-30%).

**Auto-tuning**: `optimal_worker_count()` selects 1 worker on 1-2 cores, N workers on 3+ cores. The 4 vCPU run used `--workers 4` explicitly. Single-shard mode (workers=1) on small instances has not been benchmarked yet.

---

## Run 2: m5.large (2 vCPU) — SO_REUSEPORT

**Date:** 2026-03-08
**Instance:** m5.large (2 vCPU, 8GB RAM), us-east-2
**Load Generator:** c5n.large in same AZ
**Tool:** memtier_benchmark `-c 20 -t 4 --test-time 100 -d 256 --distinct-client-seed --key-pattern=R:R`

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

SO_REUSEPORT eliminated the accept thread (3 threads → 2 on 2 vCPU). This run still used `--workers 2` (two shards with cross-shard routing). On 2 vCPU, Kora nearly matches Redis/Dragonfly (-0.4% to -2.2%) and dominates Pipeline (+17.7% over Redis, +156% over Dragonfly). p50 latency is best-in-class. p99 tail latency is 1.5-2x higher — next optimization target.

**TODO**: Benchmark `--workers 1` (single-shard, zero cross-shard overhead) on 2 vCPU. The auto-tuning default now selects workers=1 on 1-2 cores, but this has not been validated with benchmarks yet.

---

## Run 1: m5.large (2 vCPU) — Accept Thread Architecture

**Date:** 2026-03-08

### Throughput (ops/sec)

| Test            | Redis 8    | Dragonfly  | Kora       | Kora vs Redis | Kora vs Dragonfly |
|-----------------|------------|------------|------------|---------------|-------------------|
| SET-only        | 126,093    | 126,786    | 122,897    | -2.5%         | -3.1%             |
| GET-only        | 133,130    | 132,746    | 131,427    | -1.3%         | -1.0%             |
| Mixed 1:1       | 127,667    | 127,659    | 123,135    | -3.5%         | -3.5%             |
| Read-heavy 1:10 | 127,306    | 126,829    | 117,312    | -7.8%         | -7.5%             |
| Pipeline 16     | 596,240    | 290,876    | 609,127    | **+2.2%**     | **+109.4%**       |
