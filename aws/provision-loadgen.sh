#!/usr/bin/env bash
set -euo pipefail

echo "=== Installing memtier_benchmark ==="
sudo dnf update -y
sudo dnf install -y gcc gcc-c++ make automake autoconf libtool openssl-devel git libevent-devel

cd /tmp
git clone https://github.com/RedisLabs/memtier_benchmark.git
cd memtier_benchmark
autoreconf -ivf
./configure
make -j$(nproc)
sudo make install

echo "=== Verifying ==="
memtier_benchmark --version

echo ""
echo "=== Load generator ready ==="
echo "Run: TARGET_HOST=<server-private-ip> ./bench-memtier.sh"
