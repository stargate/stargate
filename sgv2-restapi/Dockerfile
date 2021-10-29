# TODO: change to more modern JVM
FROM openjdk:8u292-slim

ARG STARGATE_VERSION=1.0.40-SNAPSHOT
ENV SERVICE_JAR=sgv2-rest-service-${STARGATE_VERSION}.jar

COPY target/sgv2-rest-service-${STARGATE_VERSION}.jar sgv2-rest-service.jar
CMD ["java","-jar","sgv2-rest-service.jar"]
