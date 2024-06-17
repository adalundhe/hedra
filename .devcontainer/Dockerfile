FROM corpheus91/devcontainers:python-3.12-bookworm


RUN apt-get update && \
    apt-get install -y libssl-dev && \
    pip install uv && \
    wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2.22_amd64.deb && \
    dpkg -i libssl1.1_1.1.1f-1ubuntu2.22_amd64.deb


