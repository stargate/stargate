#
# Dockerfile for building coordinator node images
#

FROM openjdk:8-jre-slim as base

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

# Document API (until extracted to separate service)
EXPOSE 8083

# Health
EXPOSE 8084

# we use a script to build a directory with contents of ./stargate-lib, omitting all persistence jars
# we can then add the proper set of persistence jars to each of the images below
ARG LIBDIR
COPY ${LIBDIR} /stargate-lib/

COPY ./starctl-coordinator /starctl-coordinator
RUN chmod +x starctl-coordinator
ENTRYPOINT ["./starctl-coordinator"]

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
FROM openjdk:11-jre-slim as restapi

# REST
EXPOSE 8082

ENV STARGATE_BRIDGE_HOST=localhost
ENV STARGATE_BRIDGE_PORT=8091
ENV STARGATE_REST_PORT=8082

COPY stargate-lib/rest/sgv2*.jar stargate-lib/rest/
COPY ./starctl-service-rest /starctl-service-rest
ENTRYPOINT ["./starctl-service-rest"]


#
# Dockerfile for building GraphQL API image
#

FROM openjdk:11-jre-slim as graphqlapi

EXPOSE 8080

ENV STARGATE_BRIDGE_HOST=localhost
ENV STARGATE_BRIDGE_PORT=8091
ENV STARGATE_GRAPHQL_PORT=8080

COPY stargate-lib/graphql/sgv2*.jar stargate-lib/graphql/
COPY ./starctl-service-graphql /starctl-service-graphql
ENTRYPOINT ["./starctl-service-graphql"]
