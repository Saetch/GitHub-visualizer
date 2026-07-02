FROM rust:1.96-alpine as BUILDER
RUN apk add --no-cache musl-dev musl-utils linux-headers

COPY websocket_server/src/ ./app/src/
COPY websocket_server/Cargo.toml websocket_server/Cargo.lock /app/
COPY visualizer_protocol /visualizer_protocol

WORKDIR /app/

RUN cargo build --release


FROM alpine:3.24.1
RUN apk add curl
COPY --from=BUILDER /app/target/release/websocket_server /app/
WORKDIR /app/
CMD ["./websocket_server"]