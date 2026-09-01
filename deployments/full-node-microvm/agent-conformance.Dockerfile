FROM ubuntu:24.04@sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517

LABEL org.opencontainers.image.description="Runku Full Node Agent microVM conformance harness; not a product server image"

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        ca-certificates iproute2 nftables openssl util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY assets/ /opt/runku/assets/
COPY runku-firecracker-controller.sh /opt/runku/runku-firecracker-controller.sh
COPY firecracker-kubernetes-agent.sh /opt/runku/firecracker-kubernetes-agent.sh

RUN chmod 0755 \
    /opt/runku/runku-firecracker-controller.sh \
    /opt/runku/firecracker-kubernetes-agent.sh

ENTRYPOINT ["/opt/runku/firecracker-kubernetes-agent.sh"]
