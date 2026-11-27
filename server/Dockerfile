FROM node:22-bookworm-slim AS ui
WORKDIR /build/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --engine-strict
COPY ui/ ./
RUN npm run build

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
COPY --from=ui /build/ui/dist/ ui/dist/
RUN cargo build --locked --release -p server --features embed-ui --bin server --bin init-db

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/target/release/server /usr/local/bin/server
COPY --from=build /build/target/release/init-db /usr/local/bin/init-db
ENV RAYFLOW_ADDR=0.0.0.0:8080 RUST_LOG=info
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["server"]
