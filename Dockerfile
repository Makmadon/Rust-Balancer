# syntax=docker/dockerfile:1

# ==============================================================================
# Stage 1: Dependency Cache & Binary Builder
# ==============================================================================
FROM rust:1.95-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies using a dummy target first
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && \
    printf "//! Dummy crate doc.\n/// Dummy function doc.\npub const fn dummy() {}\n" > src/lib.rs && \
    printf "//! Dummy binary doc.\nfn main() {}\n" > src/main.rs && \
    cargo build --release --locked && \
    rm -rf src

# Copy actual project source and build release binary
COPY src ./src
COPY tests ./tests
RUN touch src/lib.rs src/main.rs && \
    cargo build --release --locked --bin rust-balancer

# ==============================================================================
# Stage 2: Minimal Hardened Runtime Image
# ==============================================================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r balancer && useradd -r -g balancer balancer

WORKDIR /app

COPY --from=builder /app/target/release/rust-balancer /usr/local/bin/rust-balancer
COPY config/balancer.toml /app/config/balancer.toml

RUN chown -R balancer:balancer /app

USER balancer

ENV BALANCER_CONFIG=/app/config/balancer.toml
ENV RUST_LOG=rust_balancer=info,axum=info

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/_balancer/health || exit 1

ENTRYPOINT ["/usr/local/bin/rust-balancer"]
