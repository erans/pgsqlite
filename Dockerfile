# Base stage for cargo-chef
FROM rust:bookworm as chef
WORKDIR /app
RUN cargo install cargo-chef

# Planner stage: Computes the recipe file
FROM chef as planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Builder stage: Caches dependencies and builds the binary
FROM chef as builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the cached layer
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin pgsqlite

# Runtime stage
FROM debian:bookworm-slim as runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash pgsqlite

# Create data directory
RUN mkdir -p /data && chown pgsqlite:pgsqlite /data

# Copy binary from builder
COPY --from=builder /app/target/release/pgsqlite /usr/local/bin/pgsqlite

# Switch to non-root user
USER pgsqlite

# Set working directory
WORKDIR /data

# Expose PostgreSQL default port
EXPOSE 5432

# Set default database path
ENV PGSQLITE_DATABASE=/data/database.db

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD timeout 5 bash -c "</dev/tcp/localhost/5432" || exit 1

# Run pgsqlite
CMD ["pgsqlite", "--database", "/data/database.db"]
