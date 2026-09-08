FROM node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 AS node-runtime

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:c31ff9abcb1910f3ab25c7957bdaf0bfe12a01eb546e8df2282f1c8f682b606c

ARG RUNKU_VERSION
ARG RUNKU_REVISION

LABEL org.opencontainers.image.title="Runku Server" \
      org.opencontainers.image.description="Runku Self-Hosted Safe V8 server with opt-in dedicated-host Node" \
      org.opencontainers.image.source="https://github.com/aldemi-tech/runku" \
      org.opencontainers.image.version="$RUNKU_VERSION" \
      org.opencontainers.image.revision="$RUNKU_REVISION" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --chmod=0555 runku-server /usr/local/bin/runku-server
COPY --chmod=0555 runku /usr/local/bin/runku
COPY --from=node-runtime --chmod=0555 /usr/local/bin/node /usr/local/bin/node

USER 65532:65532
EXPOSE 3220
ENTRYPOINT ["/usr/local/bin/runku-server"]
CMD ["serve"]
