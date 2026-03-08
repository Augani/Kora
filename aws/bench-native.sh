#!/usr/bin/env bash
set -euo pipefail

PORT=6379
CLIENTS=${CLIENTS:-20}
THREADS=${THREADS:-4}
TEST_TIME=${TEST_TIME:-100}
DATA_SIZE=${DATA_SIZE:-256}
TARGET_HOST=${TARGET_HOST:-127.0.0.1}
KORA_WORKERS=${KORA_WORKERS:-2}
SSH_KEY=${SSH_KEY:-~/.ssh/kora-bench.pem}
SSH="ssh -o StrictHostKeyChecking=no -i $SSH_KEY ec2-user@$TARGET_HOST"

run_memtier() {
    local label=$1
    local ratio=$2
    local extra=${3:-}

    echo ""
    echo "--- $label (ratio=$ratio $extra) ---"
    memtier_benchmark \
        -s "$TARGET_HOST" -p "$PORT" \
        -c "$CLIENTS" -t "$THREADS" \
        --test-time "$TEST_TIME" \
        -d "$DATA_SIZE" \
        --ratio "$ratio" \
        --distinct-client-seed \
        --hide-histogram \
        --key-pattern=R:R \
        $extra
}

run_suite() {
    local name=$1
    echo ""
    echo "========================================"
    echo " $name — all tests"
    echo "========================================"

    run_memtier "$name SET-only"     "1:0"
    run_memtier "$name GET-only"     "0:1"
    run_memtier "$name Mixed-1:1"    "1:1"
    run_memtier "$name Read-heavy"   "1:10"
    run_memtier "$name Pipeline-16"  "1:1" "--pipeline 16"
}

wait_for_port() {
    echo "Waiting for port $PORT on $TARGET_HOST..."
    for i in $(seq 1 30); do
        if bash -c "echo > /dev/tcp/$TARGET_HOST/$PORT" 2>/dev/null; then
            echo "Ready."
            return 0
        fi
        sleep 1
    done
    echo "TIMEOUT waiting for port $PORT"
    return 1
}

echo "========================================"
echo " Kora vs Redis vs Dragonfly (native)"
echo " memtier: ${CLIENTS}c ${THREADS}t ${TEST_TIME}s ${DATA_SIZE}B"
echo " Server: $TARGET_HOST:$PORT"
echo "========================================"

echo ""
echo "========== REDIS 8 =========="
$SSH "redis-server --bind 0.0.0.0 --port $PORT --protected-mode no --save '' --appendonly no --daemonize yes"
sleep 2
wait_for_port
run_suite "Redis"
$SSH "redis-cli -p $PORT shutdown nosave 2>/dev/null || true"
sleep 2

echo ""
echo "========== DRAGONFLY =========="
$SSH "nohup dragonfly --bind 0.0.0.0 --port $PORT --cache_mode --proactor_threads $KORA_WORKERS --dbfilename '' > /tmp/dragonfly.log 2>&1 &"
sleep 3
wait_for_port
run_suite "Dragonfly"
$SSH "pkill dragonfly || true"
sleep 2

echo ""
echo "========== KORA =========="
$SSH "nohup ~/kora/target/release/kora --bind 0.0.0.0 --port $PORT --workers $KORA_WORKERS > /tmp/kora.log 2>&1 &"
sleep 2
wait_for_port
run_suite "Kora"
$SSH "pkill kora || true"
sleep 2

echo ""
echo "========================================"
echo " All benchmarks complete"
echo "========================================"
