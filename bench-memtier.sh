#!/usr/bin/env bash
set -euo pipefail

CLIENTS=${CLIENTS:-20}
THREADS=${THREADS:-4}
TEST_TIME=${TEST_TIME:-100}
DATA_SIZE=${DATA_SIZE:-256}
KORA_PORT=6399
REDIS_PORT=6400
DRAGONFLY_PORT=6401
TARGET_HOST=${TARGET_HOST:-127.0.0.1}

run_memtier() {
    local name=$1
    local port=$2
    local ratio=$3
    local label=$4

    echo ""
    echo "--- $name ($label, ratio=$ratio) ---"
    memtier_benchmark \
        -s "$TARGET_HOST" -p "$port" \
        -c "$CLIENTS" -t "$THREADS" \
        --test-time "$TEST_TIME" \
        -d "$DATA_SIZE" \
        --ratio "$ratio" \
        --distinct-client-seed \
        --hide-histogram \
        --key-pattern=R:R
}

echo "========================================"
echo " Kora vs Redis vs Dragonfly Benchmark"
echo " memtier_benchmark | ${CLIENTS}c ${THREADS}t ${TEST_TIME}s ${DATA_SIZE}B"
echo "========================================"

echo ""
echo "=== Container resource baseline ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}"

echo ""
echo "=== Test 1: SET-only (ratio 1:0) ==="
run_memtier "Redis"     $REDIS_PORT     "1:0" "SET-only"
run_memtier "Dragonfly" $DRAGONFLY_PORT "1:0" "SET-only"
run_memtier "Kora"      $KORA_PORT      "1:0" "SET-only"

echo ""
echo "=== Resource usage after SET ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.PIDs}}"

echo ""
echo "=== Test 2: GET-only (ratio 0:1) ==="
run_memtier "Redis"     $REDIS_PORT     "0:1" "GET-only"
run_memtier "Dragonfly" $DRAGONFLY_PORT "0:1" "GET-only"
run_memtier "Kora"      $KORA_PORT      "0:1" "GET-only"

echo ""
echo "=== Test 3: Mixed read/write (ratio 1:1) ==="
run_memtier "Redis"     $REDIS_PORT     "1:1" "50/50"
run_memtier "Dragonfly" $DRAGONFLY_PORT "1:1" "50/50"
run_memtier "Kora"      $KORA_PORT      "1:1" "50/50"

echo ""
echo "=== Test 4: Read-heavy (ratio 1:10) ==="
run_memtier "Redis"     $REDIS_PORT     "1:10" "read-heavy"
run_memtier "Dragonfly" $DRAGONFLY_PORT "1:10" "read-heavy"
run_memtier "Kora"      $KORA_PORT      "1:10" "read-heavy"

echo ""
echo "=== Test 5: Pipeline (ratio 1:1, pipeline=16) ==="
for name_port in "Redis:$REDIS_PORT" "Dragonfly:$DRAGONFLY_PORT" "Kora:$KORA_PORT"; do
    IFS=: read -r name port <<< "$name_port"
    echo ""
    echo "--- $name (pipeline=16) ---"
    memtier_benchmark \
        -s "$TARGET_HOST" -p "$port" \
        -c "$CLIENTS" -t "$THREADS" \
        --test-time "$TEST_TIME" \
        -d "$DATA_SIZE" \
        --ratio "1:1" \
        --pipeline 16 \
        --distinct-client-seed \
        --hide-histogram \
        --key-pattern=R:R
done

echo ""
echo "=== Final resource usage ==="
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.PIDs}}"

echo ""
echo "Done."
