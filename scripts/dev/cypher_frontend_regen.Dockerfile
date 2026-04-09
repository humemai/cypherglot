FROM eclipse-temurin:25-jdk

ARG DEBIAN_FRONTEND=noninteractive
ARG ANTLR_VERSION=4.13.2

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        python3 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/antlr \
    && curl -fsSL \
        "https://www.antlr.org/download/antlr-${ANTLR_VERSION}-complete.jar" \
        -o "/opt/antlr/antlr-${ANTLR_VERSION}-complete.jar"

WORKDIR /workspace