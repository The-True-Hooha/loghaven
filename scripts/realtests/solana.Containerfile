# Agave's test validator for the Solana real-service test. Version 4 asserts io_uring, which is
# not there under the x86 emulation podman uses on Apple silicon, so 2.3 is used.
#   podman build --platform linux/amd64 -t lh-solana-test:2.3.13 -f scripts/realtests/solana.Containerfile .
FROM docker.io/library/ubuntu:22.04
ARG AGAVE=v2.3.13
RUN apt-get update && apt-get install -y --no-install-recommends curl bzip2 ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
RUN curl -sL -o /r.tar.bz2 https://github.com/anza-xyz/agave/releases/download/${AGAVE}/solana-release-x86_64-unknown-linux-gnu.tar.bz2 \
 && tar xjf /r.tar.bz2 -C / && rm /r.tar.bz2
ENV PATH=/solana-release/bin:$PATH
ENTRYPOINT ["solana-test-validator"]
