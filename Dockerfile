# syntax=docker/dockerfile:1.7
FROM rust:1.96.1-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p qcli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 qcli \
    && useradd --system --uid 10001 --gid qcli --home-dir /var/lib/qcli qcli \
    && install -d -o qcli -g qcli /etc/qcli /var/lib/qcli
COPY --from=builder /src/target/release/qcli /usr/local/bin/qcli
COPY packaging/qcli.1 /usr/share/man/man1/qcli.1
USER 10001:10001
EXPOSE 8088 32010
ENTRYPOINT ["qcli"]
CMD ["serve", "--bind", "0.0.0.0:8088", "--trusted-proxy"]
