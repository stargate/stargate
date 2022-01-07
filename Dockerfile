#
# Dockerfile for building coordinator node images
#

FROM openjdk:8u312-jre-slim as base

RUN apt update -qq \
    && apt install iproute2 libaio1 -y \
    && apt autoremove --yes \
    && apt clean all \
    && rm -rf /var/lib/apt/lists/*

#RUN apt update -qq \
#    && apt-get upgrade -y \
#    && apt install iproute2 libaio1 -y \
#    && apt autoremove --yes \
#    && apt clean all \
#    && rm -rf /var/lib/{apt,dpkg,cache,log}/

# CQL
EXPOSE 9042

# GraphQL
EXPOSE 8080

# Auth
EXPOSE 8081

# Health
EXPOSE 8084

# this will include all persistence jars, we'll remove the ones we don't want below
COPY ./stargate-lib/* /stargate-lib/

COPY ./starctl /starctl
RUN chmod +x starctl
ENTRYPOINT ["./starctl"]

FROM base as sgv2-coordinator-4_0
RUN rm -rf stargate-lib/persistence-cassandra-3.11*.jar stargate-lib/persistence-dse*.jar

FROM base as sgv2-coordinator-3_11
RUN rm -rf stargate-lib/persistence-cassandra-4.0*.jar stargate-lib/persistence-dse*.jar

FROM base as sgv2-coordinator-dse-68
RUN rm -rf stargate-lib/persistence-cassandra-4.0*.jar stargate-lib/persistence-cassandra-3.11*.jar


#
# Dockerfile for building REST API image
#

# Using Java 11
FROM openjdk:11.0.13-jre-slim as sgv2-restapi

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
