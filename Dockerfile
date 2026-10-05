# syntax=docker/dockerfile:1

# ---- build -----------------------------------------------------------------
FROM rust:1.95-slim-bookworm AS build
WORKDIR /src

# Build dependencies first so they're cached when only src/ changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
 && echo 'fn main() {}' > src/main.rs \
 && touch src/lib.rs \
 && cargo build --release --locked \
 && rm -rf src

COPY src ./src
# touch so cargo sees the real sources as newer than the stub.
RUN touch src/main.rs src/lib.rs \
 && cargo build --release --locked

# ---- runtime ---------------------------------------------------------------
# Debian 12 to match the build stage's glibc. TLS roots are built into the
# binary, so no CA certificates are needed.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/trenchers-arena-backend /usr/local/bin/trenchers-arena-backend
ENV PORT=8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/trenchers-arena-backend"]
