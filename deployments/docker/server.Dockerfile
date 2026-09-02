FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f

ARG RUNKU_VERSION
ARG RUNKU_REVISION

LABEL org.opencontainers.image.title="Runku Server" \
      org.opencontainers.image.description="Runku Self-Hosted compact Safe V8 server" \
      org.opencontainers.image.source="https://github.com/aldemi-tech/runku" \
      org.opencontainers.image.version="$RUNKU_VERSION" \
      org.opencontainers.image.revision="$RUNKU_REVISION" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --chmod=0555 runku-server /usr/local/bin/runku-server
COPY --chmod=0555 runku /usr/local/bin/runku

USER 65532:65532
EXPOSE 3220
ENTRYPOINT ["/usr/local/bin/runku-server"]
CMD ["serve"]
