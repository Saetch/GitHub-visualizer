FROM rust:1.96-alpine as BUILDER
RUN apk add --no-cache musl-dev musl-utils linux-headers

COPY ./src/ ./app/src/
COPY ./Cargo.toml ./Cargo.lock /app/
WORKDIR /app/

RUN cargo build --release


FROM alpine:3.24.1
RUN apk add curl
COPY --from=BUILDER /app/target/release/archive_reader /app/
WORKDIR /app/
CMD ["./archive_reader"]