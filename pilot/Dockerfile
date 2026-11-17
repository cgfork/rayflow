FROM rust:bookworm AS build
WORKDIR /build
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY server/ server/
COPY pilot/ pilot/
COPY agent/ agent/
RUN cargo build --locked --release -p pilot --bin pilot

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/target/release/pilot /usr/local/bin/pilot
ENV RUST_LOG=info
USER 10001:10001
# Pilot listens for Ctrl-C to finish its current consumer pass.
STOPSIGNAL SIGINT
ENTRYPOINT ["pilot"]
