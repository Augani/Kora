#!/usr/bin/env bash
set -euo pipefail

REQUESTS=${REQUESTS:-200000}
CLIENTS=${CLIENTS:-50}
KORA_PORT=6399
REDIS_PORT=6400
DRAGONFLY_PORT=6401

echo "=== Building and starting containers ==="
docker compose up -d --build --wait

sleep 2

echo ""
echo "=== Container resource baseline ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}"

echo ""
echo "=== Redis Benchmark: SET/GET ($REQUESTS requests, $CLIENTS clients) ==="

echo ""
echo "--- Redis (port $REDIS_PORT) ---"
redis-benchmark -h 127.0.0.1 -p $REDIS_PORT -n $REQUESTS -c $CLIENTS -t SET,GET -q

echo ""
echo "--- Dragonfly (port $DRAGONFLY_PORT) ---"
redis-benchmark -h 127.0.0.1 -p $DRAGONFLY_PORT -n $REQUESTS -c $CLIENTS -t SET,GET -q

echo ""
echo "--- Kora (port $KORA_PORT) ---"
redis-benchmark -h 127.0.0.1 -p $KORA_PORT -n $REQUESTS -c $CLIENTS -t SET,GET -q

echo ""
echo "=== Resource usage during SET/GET ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.PIDs}}"

echo ""
echo "=== Redis Benchmark: Pipeline (16 commands per batch) ==="

echo ""
echo "--- Redis ---"
redis-benchmark -h 127.0.0.1 -p $REDIS_PORT -n $REQUESTS -c $CLIENTS -P 16 -t SET,GET -q

echo ""
echo "--- Dragonfly ---"
redis-benchmark -h 127.0.0.1 -p $DRAGONFLY_PORT -n $REQUESTS -c $CLIENTS -P 16 -t SET,GET -q

echo ""
echo "--- Kora ---"
redis-benchmark -h 127.0.0.1 -p $KORA_PORT -n $REQUESTS -c $CLIENTS -P 16 -t SET,GET -q

echo ""
echo "=== Resource usage during pipeline ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.PIDs}}"

echo ""
echo "=== Redis Benchmark: Mixed workload ==="

echo ""
echo "--- Redis ---"
redis-benchmark -h 127.0.0.1 -p $REDIS_PORT -n $REQUESTS -c $CLIENTS -t SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET -q

echo ""
echo "--- Dragonfly ---"
redis-benchmark -h 127.0.0.1 -p $DRAGONFLY_PORT -n $REQUESTS -c $CLIENTS -t SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET -q

echo ""
echo "--- Kora ---"
redis-benchmark -h 127.0.0.1 -p $KORA_PORT -n $REQUESTS -c $CLIENTS -t SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET -q

echo ""
echo "=== Final resource usage ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.PIDs}}"

echo ""
echo "=== Cleanup ==="
docker compose down
echo "Done."
