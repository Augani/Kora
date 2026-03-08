#!/usr/bin/env bash
set -euo pipefail

echo "=== Tuning kernel for benchmarks ==="
sudo tee /etc/sysctl.d/99-bench.conf > /dev/null <<SYSCTL
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.core.netdev_max_backlog = 65535
vm.overcommit_memory = 1
SYSCTL
sudo sysctl --system

echo "=== Installing build dependencies ==="
sudo dnf update -y
sudo dnf install -y gcc gcc-c++ make openssl-devel git jq

echo "=== Installing Rust ==="
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

echo "=== Building Redis 8 from source ==="
cd /tmp
curl -LO https://github.com/redis/redis/archive/refs/tags/8.0.3.tar.gz
tar xzf 8.0.3.tar.gz
cd redis-8.0.3
make -j$(nproc) BUILD_TLS=no
sudo make install
redis-server --version

echo "=== Installing Dragonfly ==="
cd /tmp
DRAGONFLY_VERSION=$(curl -s https://api.github.com/repos/dragonflydb/dragonfly/releases/latest | jq -r .tag_name)
curl -LO "https://github.com/dragonflydb/dragonfly/releases/download/${DRAGONFLY_VERSION}/dragonfly-x86_64.tar.gz"
tar xzf dragonfly-x86_64.tar.gz
sudo cp dragonfly-x86_64 /usr/local/bin/dragonfly
dragonfly --version

echo "=== Building Kora ==="
cd ~/kora
cargo build --release -p kora-cli
echo "Kora binary: $(ls -lh target/release/kora)"

echo ""
echo "=== Server ready ==="
echo "All three services installed. Use bench-native.sh to run benchmarks."
