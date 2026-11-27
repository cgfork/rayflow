FROM rust:bookworm AS build
WORKDIR /build
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY server/ server/
COPY pilot/ pilot/
COPY agent/ agent/
COPY examples/runlua/ examples/runlua/
RUN cargo build --locked --release -p runlua

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/runlua /usr/local/bin/runlua
ENTRYPOINT ["runlua"]
