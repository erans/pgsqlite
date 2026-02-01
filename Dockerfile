FROM rust:1.90-bookworm AS chef
WORKDIR /app
RUN cargo install cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin pgsqlite


FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    tini \
    postgresql-client \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pgsqlite /usr/local/bin/pgsqlite

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/pgsqlite /usr/local/bin/docker-entrypoint.sh \
  && mkdir -p /var/run/postgresql \
  && chmod 0777 /var/run/postgresql

EXPOSE 5432

ENV PGDATA=/var/lib/postgresql/data
ENV PGSQLITE_DATABASE=/var/lib/postgresql/data
VOLUME ["/var/lib/postgresql/data"]

WORKDIR /var/lib/postgresql

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD pg_isready -h /var/run/postgresql -p 5432 || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["pgsqlite", "--port", "5432"]
