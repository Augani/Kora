FROM rust:1.86-slim AS builder

WORKDIR /build
COPY . .
RUN cargo build --release -p kora-cli

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/kora /usr/local/bin/kora

EXPOSE 6379
ENTRYPOINT ["kora"]
CMD ["--bind", "0.0.0.0", "--port", "6379", "--workers", "4"]
