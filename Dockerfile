#
# Dockerfile for building coordinator node images
#

FROM adoptopenjdk/openjdk8:jre8u352-b08 as base

RUN apt update -qq \
    && apt install iproute2 libaio1 libjemalloc2 -y \
    && apt autoremove --yes \
    && apt clean all \
    && rm -rf /var/lib/apt/lists/*

RUN set -x \
    && groupadd -r stargate --gid=999 \
    && useradd -r -g stargate -d /stargate --uid=999 stargate

USER stargate
WORKDIR /stargate

# CQL
EXPOSE 9042

# GraphQL
EXPOSE 8080

# Auth
EXPOSE 8081

# Document API (until extracted to separate service)
EXPOSE 8083

# Health
EXPOSE 8084

# we use a script to build a directory with contents of ./stargate-lib, omitting all persistence jars
# we can then add the proper set of persistence jars to each of the images below
ARG LIBDIR
COPY ${LIBDIR} stargate-lib/

COPY ./starctl ./starctl
ENTRYPOINT ["./starctl"]

FROM base as coordinator-4_0
COPY stargate-lib/persistence-api*.jar stargate-lib/persistence-cassandra-4.0*.jar stargate-lib/

FROM base as coordinator-3_11
COPY stargate-lib/persistence-api*.jar stargate-lib/persistence-cassandra-3.11*.jar stargate-lib/

FROM base as coordinator-dse-68
COPY stargate-lib/persistence-api*.jar stargate-lib/persistence-dse*.jar stargate-lib/
