# Docker Compose scripts for Stargate with Cassandra 3.11 

This directory provides two configurations to start Stargate with Cassandra 3.11 using `docker compose`.

## Prerequisites

### Docker / Docker Compose Versions

Make sure that you have Docker engine 20.x installed, which should include Docker compose 2.x. Our compose files rely on features only available in the Docker compose v2 file format.

### Building the local Docker image
If you want to use locally built versions of the Docker images rather than pulling released versions from Docker Hub, build the snapshot version locally using instructions in the [developer guide](../../DEV_GUIDE.md).

Follow instructions under the [Script options](#script-options) section to use the locally built image.

## Stargate with 3-node Cassandra 3.11 cluster

You can start a simple Stargate configuration with the following command:

```
./start_cass_311.sh
``` 

This convenience script verifies your Docker installation meets minimum requirements and brings up the configuration described in the `docker-compose.yml` file. The configuration includes health criteria for each container that is used to ensure the containers come up in the correct order.

The convenience script uses the `-d` and `--wait` options to track the startup progress, so that the compose command exits when all containers have started and reported healthy status within a specified timeout.

The default environment settings in the `.env` file include variables that describe which image tags to use, typically Stargate `v2` and Cassandra `3.11`. The `start_cass_311.sh` script supports [options](#script-options) for overriding which image tags are used, including using a locally generated image as described [above](#building-the-local-docker-image). We recommend doing a `docker compose pull` periodically to ensure you always have the latest patch versions of these tags.

Once done using the containers, you can stop them using the command `docker compose down`.

## Stargate with embedded Cassandra 3.11 (developer mode)

This alternate configuration runs the Stargate coordinator node in developer mode, so that no separate Cassandra cluster is required. This configuration is useful for development and testing since it initializes more quickly, but is not recommended for production deployments. This can be run with the command:

```
./start_cass_311_dev_mode.sh
``` 

This script supports the same [options](#script-options) as the `start_cass_311.sh` script.

To stop the configuration, use the command:

```
docker compose -f docker-compose-dev-mode.yml down
``` 

## Script options

Both convenience scripts support the following options: 

* You can specify a released image tag (version) using `-t [VERSION]`. Consult [Docker Hub](https://hub.docker.com/r/stargateio/coordinator-3_11/tags) for a list of available tags.

* Alternatively, build the snapshot version locally using instructions in the [developer guide](../../DEV_GUIDE.md) and run the script using the `-l` option.

* You can change the default root log level for the API services using `-r [LEVEL]` (default `INFO`). Valid values: `ERROR`, `WARN`, `INFO`, `DEBUG`

* You can enable reguest logging for the API services using `-q`: if so, each request is logged under category `io.quarkus.http.access-log`


## Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`), the Cassandra Docker image tag to use (`CASSTAG`), and the Stargate Docker image tag to use (`SGTAG`).

* When using the convenience scripts, the Stargate Docker image (`SGTAG`) used for the `-l` option is the current (snapshot) version as defined in the top level project `pom.xml` file. It can be overridden with the `-t` option on either script:

  `./start_cass_311.sh -t v2.0.1`

* Running more than one of these multi-container environments on one host may require changing the port mapping to be changed to avoid conflicts on the host machine.

## Troubleshooting

If you see an error like:
```
Pulling coordinator (stargateio/coordinator-3_11:2.0.1-SNAPSHOT)...
ERROR: manifest for stargateio/coordinator-3_11:2.0.1-SNAPSHOT not found: manifest unknown: manifest unknown
```

you are trying to deploy a version that is neither publicly available (official release) nor built locally.

