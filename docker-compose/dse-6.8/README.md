This directory provides two ways to start Stargate with DSE 6.8 using docker-compose.

# Stargate with 3-node DSE 6.8 cluster

Use the shell script `start_dse_68.sh`, which performs an orderly startup of containers 
specified in `docker-compose.yml`:

`./start_dse_68.sh`

This script exits when all containers have started or a failure is detected. You can remove
the stack of containers created by executing `docker compose down`.

# Stargate with embedded DSE 6.8 (developer mode)

This alternate configuration that runs the Stargate coordinator node in developer mode, so that no
separate Cassandra cluster is required. This can be run directly with the command:

`./start_dse_68_dev_mode.sh`

You can stop execution of this script with `Ctrl-C` and the stack will be torn down.

# Script options

Both scripts support the following options:

* You can specify a released image tag (version) using `-t [VERSION]`. Consult [Docker Hub](https://hub.docker.com/r/stargateio/coordinator-dse-68/tags) for a list of available tags.

* Alternatively, build the snapshot version locally using instructions in the [developer guide](../../DEV_GUIDE.md) and run the script using the `-l` option.
* 
* You can change the default root log level using `-r [LEVEL]` (default `INFO`). Valid values: `ERROR`, `WARN`, `INFO`, `DEBUG`

* You can enable reguest logging using `-q`: if so, each request is logged under category `io.quarkus.http.access-log`

# Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`),
  and the DSE Docker image tag to use (`DSETAG`).

* The Stargate Docker image tag to use (`SGTAG`) defaults to the current version as defined in the 
  top level project `pom.xml` file. It can be overridden with the `-t` option on either script:

  `./start_dse_68.sh -t v2.0.0-BETA-2`

* Running more than one of these multi-container environments on one host may require
  changing the port mapping to be changed to avoid conflicts on the host machine.

# Troubleshooting

If you see an error like:
```
Pulling coordinator (stargateio/coordinator-dse-68:2.0.0-BETA-4-SNAPSHOT)...
ERROR: manifest for stargateio/coordinator-dse-68:2.0.0-BETA-4-SNAPSHOT not found: manifest unknown: manifest unknown
```

you are trying to deploy a version that is neither publicly available (official release) nor built locally.

