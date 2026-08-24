FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG DEBIAN_FRONTEND=noninteractive
ARG EMSDK_VERSION=3.1.68

# hadolint ignore=DL3008
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    ccache \
    cmake \
    curl \
    git \
    make \
    ninja-build \
    pkg-config \
    python3 \
    python3-venv \
    rsync \
    nodejs \
    npm \
    unzip \
    zip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/ninja-build/ninja/releases/download/v1.13.1/ninja-linux.zip \
      -o /tmp/ninja.zip \
    && unzip /tmp/ninja.zip ninja -d /usr/local/bin \
    && rm /tmp/ninja.zip \
    && chmod +x /usr/local/bin/ninja \
    && ninja --version

RUN mkdir -p /opt/emsdk
WORKDIR /opt/emsdk
RUN curl -sSL "https://github.com/emscripten-core/emsdk/archive/${EMSDK_VERSION}.tar.gz" \
      | tar -xz --strip-components=1 \
    && ./emsdk install ${EMSDK_VERSION} \
    && ./emsdk activate ${EMSDK_VERSION}

ENV EMSDK=/opt/emsdk
ENV PATH="/opt/emsdk:/opt/emsdk/upstream/emscripten:${PATH}"

WORKDIR /work
