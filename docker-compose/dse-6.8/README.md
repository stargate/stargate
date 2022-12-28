# Docker Compose scripts for Stargate with DSE 6.8

This directory provides two ways to start Stargate with DSE 6.8 using `docker compose`.

## Stargate with 3-node DSE 6.8 cluster

You can start a simple Stargate configuration with the following command:

```
docker compose up -d
``` 

This brings up the configuration described in the `docker-compose.yml` file. The configuration includes health criteria for each container that is used to ensure the containers come up in the correct order.

Using the `-d` option tracks the startup progress, so that the compose command exits when all containers have started or a failure is detected. Omitting the `-d` option causes the command to track the progress of all containers, including all log output, and a `Ctrl-C` will cause all the containers to exit.

The default environment settings in the `.env` file include variables that describe which image tags to use, typically Stargate `v2` and DSE `6.8.X` (where `X` is the latest supported patch version). We recommend doing a `docker compose pull` periodically to ensure you always have the latest patch versions of these tags.

You can override the default environment settings in your local shell, or use the convenient shell script `start_dse_68.sh`.

Whether you use the shell script or start `docker compose` directly, you can remove the stack of containers created by executing `docker compose down`.

## Stargate with embedded DSE 6.8 (developer mode)

This alternate configuration runs the Stargate coordinator node in developer mode, so that no separate Cassandra cluster is required. This can be run with the command:

```
docker compose up -f docker-compose-dev-mode.yaml -d
``` 

This configuration is useful for development and testing since it initializes more quickly, but is not recommended for production deployments. This configuration also has a convenience script: `start_dse_68_dev_mode.sh`.

## Script options

Both convenience scripts support the following options:

* You can specify a released image tag (version) using `-t [VERSION]`. Consult [Docker Hub](https://hub.docker.com/r/stargateio/coordinator-dse-68/tags) for a list of available tags.

* Alternatively, build the snapshot version locally using instructions in the [developer guide](../../DEV_GUIDE.md) and run the script using the `-l` option.

* You can change the default root log level using `-r [LEVEL]` (default `INFO`). Valid values: `ERROR`, `WARN`, `INFO`, `DEBUG`

* You can enable reguest logging using `-q`: if so, each request is logged under category `io.quarkus.http.access-log`

## Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`),
 the DSE Docker image tag to use (`DSETAG`), and the Stargate Docker image tag to use (`SGTAG`).

* When using the convenience scripts, the Stargate Docker image (`SGTAG`) used for the `-l` option is the current (snapshot) version as defined in the top level project `pom.xml` file. It can be overridden with the `-t` option on either script:

  `./start_dse_68.sh -t v2.0.1`

* Running more than one of these multi-container environments on one host may require changing the port mapping to be changed to avoid conflicts on the host machine.

## Troubleshooting

If you see an error like:
```
Pulling coordinator (stargateio/coordinator-dse-68:2.0.1-SNAPSHOT)...
ERROR: manifest for stargateio/coordinator-dse-68:2.0.1-SNAPSHOT not found: manifest unknown: manifest unknown
```

you are trying to deploy a version that is neither publicly available (official release) nor built locally.

