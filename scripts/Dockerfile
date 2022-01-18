FROM ubuntu:20.04 AS base

USER root
WORKDIR /

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends vim \
    openssh-server && \
    rm -rf /var/lib/apt/lists/*

COPY ./scripts-omniscidb /scripts-omniscidb
# Only 4 files in omniscidb/scripts are needed:
    # mapd-deps-prebuilt.sh
    # mapd-deps-ubuntu.sh
    # common-functions.sh
    # llvm-9-glibc-2.31-708430.patch

RUN cd /scripts-omniscidb && \
    sed -i 's/sudo //g' mapd-deps-prebuilt.sh && \
    sed -i 's/sudo //g' mapd-deps-ubuntu.sh && \
    sed -i 's/sudo //g' common-functions.sh && \
    sed -i 's/PREFIX=\/usr\/local\/mapd-deps/PREFIX=\/usr\/local/g' mapd-deps-prebuilt.sh && \
    sed -i 's/PREFIX=\/usr\/local\/mapd-deps/PREFIX=\/usr\/local/g' mapd-deps-ubuntu.sh && \
    sed -i 's/install_folly/# install_folly/g' mapd-deps-ubuntu.sh && \
    apt-get update && \
    ./mapd-deps-prebuilt.sh && \
    source /usr/local/mapd-deps.sh && \
    ./mapd-deps-ubuntu.sh --compress --nocuda && \
    rm -rf /var/lib/apt/lists/* && \
    cd / && \
    rm -rf scripts-omniscidb

COPY ./scripts-velox /scripts-velox
# Only 1 file in velox/scripts is needed:
    # setup-ubuntu.sh

RUN cd scripts-velox && \
    export GIT_SSL_NO_VERIFY=1 && \
    sed -i 's/sudo //g' setup-ubuntu.sh && \
    apt-get update && \
    ./setup-ubuntu.sh && \
    rm -rf /var/lib/apt/lists/* && \
    cd .. && \
    rm -rf scripts-velox
