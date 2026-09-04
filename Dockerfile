# syntax=docker/dockerfile:1.7
FROM rust:1.96.1-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p qcli

FROM debian:bookworm-slim
ARG QCLI_VERSION=development
ARG QCLI_REVISION=unknown
ARG QCLI_RELEASED_AT=unknown
ARG QCLI_RELEASED_BY=local
ARG QCLI_RELEASE_TAG=unreleased
ARG QCLI_PRERELEASE=false
ARG QCLI_REPOSITORY=ConfluxData/ConfluxQuery
ARG QCLI_WORKFLOW_RUN=local
ARG QCLI_WORKFLOW_ATTEMPT=0
LABEL org.opencontainers.image.title="ConfluxQuery Gateway" \
      org.opencontainers.image.description="Multi-engine SQL CLI and governed query gateway" \
      org.opencontainers.image.source="https://github.com/ConfluxData/ConfluxQuery" \
      org.opencontainers.image.documentation="https://confluxdata.github.io/ConfluxQuery/" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${QCLI_VERSION}"
RUN install -d /usr/share/doc/qcli \
    && printf '%s\n' \
      '{' \
      '  "schema_version": 1,' \
      '  "product": "ConfluxQuery",' \
      '  "executable": "qcli",' \
      "  \"version\": \"${QCLI_VERSION}\"," \
      "  \"tag\": \"${QCLI_RELEASE_TAG}\"," \
      "  \"prerelease\": ${QCLI_PRERELEASE}," \
      "  \"git_commit\": \"${QCLI_REVISION}\"," \
      "  \"released_at\": \"${QCLI_RELEASED_AT}\"," \
      "  \"released_by\": {\"login\": \"${QCLI_RELEASED_BY}\", \"profile\": \"https://github.com/${QCLI_RELEASED_BY}\"}," \
      "  \"repository\": \"${QCLI_REPOSITORY}\"," \
      "  \"workflow_run\": \"${QCLI_WORKFLOW_RUN}\"," \
      "  \"workflow_attempt\": ${QCLI_WORKFLOW_ATTEMPT}" \
      '}' \
      > /usr/share/doc/qcli/RELEASE-METADATA.json
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
