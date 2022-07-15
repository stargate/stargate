# Stargate V2 - REST service/API

## General

This sub-project is for prototype version of stand-alone REST API
for Stargate V2, extracted from monolithic Stargate V1 Coordinator.
It does not use OSGi or run on a Coordinator: in its current form
all database access is by calling gRPC API which runs on a Coordinator node.

REST API runs as a DropWizard (2.0.x) service, same as Stargate V1 APIs
and uses standard DropWizard configuration approach.

## Packaging

Project produces a single runnable uber/fat jar like:

    sgv2-restapi/target/sgv2-restapi-2.0.0-ALPHA-3-SNAPSHOT.jar

which can be run with something like:

    java -jar sgv2-restapi-2.0.0-ALPHA-3-SNAPSHOT.jar

and assumes existence of an already running Stargate V1 instance (to use its gRPC API).
Jar contains everything needed for running including DropWizard platform
as well as Swagger set up.

## Configuration

Configuration of the prototype uses DropWizard standard style in which there
are 3 layers of configuration (from lowest to highest precedence)

1. Configuration class -- `io.stargate.sgv2.restsvc.impl.RestServiceServerConfiguration` which defines structure and defaults
2. A single YAML file (under `sgv2-restapi/src/main/resources/config.yaml`) for configuration overrides
3. System properties from command-line: uses prefix "dw."

While the first two are usually packaged into the uber-jar, system properties are specified by command-line, like so:

```
java -Ddw.server.connector.port=8085 \
   -Ddw.stargate.bridge.host=127.0.0.2 -Ddw.stargate.bridge.port=8091 \
   -jar sgv2-restapi-2.0.0-ALPHA-3-SNAPSHOT.jar
```

Also note that file `sgv2-restapi/src/main/resources/config.yaml` contains
explicit values for many things that have defaults in DropWizard default
`Configuration` or our `RestServiceServerConfiguration`.

Finally, note that linkage between server type "sgv2-restapi" and actual
service implementation class is defined by resource file at:

    sgv2-restapi/src/main/resources/META-INF/services/io.dropwizard.server.ServerFactory

which is cumbersome, non-obvious and odd, but is the way it is handled
to support polymorphism in POJOs used for configuration access.

## Configuration options: standard DropWizard

Note: many settings changed between DropWizard 0.6 and 1.0; we are using
DropWizard 2.0.x)

Note: need to prefix properties with `dw.` when passing in command-line; included below.

* `dw.server.connector.port`: Main HTTP port service listens to; defaults to `8082` in `config.yaml`

For example:

```
java -Ddw.server.connector.port=8082 \
   -jar sgv2-restapi-v2.0.0-ALPHA-3.jar

```

## Configuration options: Stargate-specific

Options available can be seen from `io.stargate.sgv2.restsvc.impl.RestServiceServerConfiguration`:

Note: need to prefix properties with `dw.` when passing in command-line; included below.

* `dw.stargate.bridge.host` (default: `localhost`): Host where gRPC service to use runs on
* `dw.stargate.bridge.port` (default: `8091`): Port number of gRPC service to use runs on
