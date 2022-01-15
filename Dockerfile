#
# Dockerfile for building coordinator node images
#

FROM openjdk:8u312-jre-slim as base

RUN apt update -qq \
    && apt install iproute2 libaio1 -y \
    && apt autoremove --yes \
    && apt clean all \
    && rm -rf /var/lib/apt/lists/*

# CQL
EXPOSE 9042

# GraphQL
EXPOSE 8080

# Auth
EXPOSE 8081

# Health
EXPOSE 8084

# we use a script to build a directory with contents of ./stargate-lib, omitting all persistence jars
# we can then add the proper set of persistence jars to each of the images below
ARG LIBDIR
COPY ${LIBDIR} /stargate-lib/

COPY ./starctl /starctl
RUN chmod +x starctl
ENTRYPOINT ["./starctl"]

FROM base as coordinator-4_0
COPY stargate-lib/persistence-api*.jar stargate-lib/persistence-cassandra-4.0*.jar /stargate-lib/

FROM base as coordinator-3_11
COPY stargate-lib/persistence-api*.jar stargate-lib/persistence-cassandra-3.11*.jar /stargate-lib/

FROM base as coordinator-dse-68
COPY stargate-lib/persistence-api*.jar stargate-lib/persistence-dse*.jar /stargate-lib/

#
# Dockerfile for building REST API image
#

# Using Java 11
FROM openjdk:11.0.13-jre-slim as restapi

# REST
EXPOSE 8082

ENV STARGATE_GRPC_HOST=localhost
ENV STARGATE_GRPC_PORT=8090
ENV STARGATE_REST_PORT=8082

COPY ./sgv2-restapi/target/sgv2*.jar /
ENTRYPOINT ["/bin/sh", "-c", \
            "exec java $JAVA_OPTS \
            -Ddw.stargate.grpc.host=$STARGATE_GRPC_HOST \
            -Ddw.stargate.grpc.port=$STARGATE_GRPC_PORT \
            -Ddw.server.connector.port=$STARGATE_REST_PORT \
            -jar sgv2-rest-service*.jar"]
