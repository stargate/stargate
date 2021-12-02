# Stargate V2 - REST service/API

## General

This sub-project is for prototype version of stand-alone REST API
for Stargate V2, extracted from monolithic Stargate V1 Coordinator.
It does not use OSGi or run on a Coordinator: in its current form
all database access is by calling gRPC API which runs on a Coordinator node.

## Packaging

Project produces a single uber/fat jar like:

    sgv2-restapi/target/sgv2-rest-service-1.0.45-SNAPSHOT.jar

which can be run with something like:

    java -jar sgv2-rest-service-1.0.45-SNAPSHOT.jar

and assumes existence of an already running Stargate V1 instance (to use its gRPC API).

## Configuration

Configuration of the prototype uses DropWizard standard style in which there
are 3 layers of configuration (from lowest to highest precedence)

1. Configuration class -- `io.stargate.sgv2.restsvc.impl.RestServiceServerConfiguration` which defines structure and defaults
2. A single YAML file (under `sgv2-restapi/src/main/resources/config.yaml`) for configuration overrides
3. System properties from command-line: uses prefix "dw."

While the first two are usually packaged into the uber-jar, system properties are specified by command-line, like so:

```
java -Ddw.stargate.grpc.host=127.0.0.2 -Ddw.stargate.grpc.port=8090 \
   -jar sgv2-rest-service-1.0.45-SNAPSHOT.jar
```

## Configuration options: standard DropWizard

(To be filled)

## Configuration options: Stargate-specific

Options available can be seen from `io.stargate.sgv2.restsvc.impl.RestServiceServerConfiguration`:

(To be filled)





