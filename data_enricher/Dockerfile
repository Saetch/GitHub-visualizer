FROM rust:1.96-alpine as BUILDER
RUN apk add --no-cache musl-dev musl-utils linux-headers

COPY data_enricher/src/ ./app/src/
COPY data_enricher/Cargo.toml data_enricher/Cargo.lock /app/
COPY visualizer_protocol /visualizer_protocol
WORKDIR /app/

RUN cargo build --release


FROM alpine:3.24.1
RUN apk add curl
COPY --from=BUILDER /app/target/release/data_enricher /app/
WORKDIR /app/
CMD ["./data_enricher"]