# syntax=docker/dockerfile:1
# Build multi-étapes. Le binaire est statique vis-à-vis d'OpenSSL (TLS via rustls) ;
# le runtime n'a besoin que des certificats CA (pour JWKS).

FROM rust:1-bookworm AS builder
WORKDIR /build
# Cache des dépendances : on copie d'abord les manifestes et on compile des cibles factices.
COPY Cargo.toml Cargo.lock ./
COPY backend/Cargo.toml backend/Cargo.toml
RUN mkdir -p backend/src/bin \
    && echo "fn main() {}" > backend/src/main.rs \
    && echo "fn main() {}" > backend/src/bin/mint_dev_token.rs \
    && echo "" > backend/src/lib.rs \
    && cargo build --release \
    && rm -rf backend/src
# Code réel + migrations (embarquées au compile-time par sqlx::migrate!).
COPY backend/ backend/
# `touch` indispensable : Docker COPY préserve des mtimes anciens, sinon Cargo croit le
# binaire (factice) à jour et ne recompile pas.
RUN find backend/src -name '*.rs' -exec touch {} + \
    && cargo build --release --bin datacat-ingest

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 datacat
COPY --from=builder /build/target/release/datacat-ingest /usr/local/bin/datacat-ingest
USER datacat
EXPOSE 8080
ENV BIND_ADDR=0.0.0.0:8080 LOG_FORMAT=json
ENTRYPOINT ["/usr/local/bin/datacat-ingest"]
