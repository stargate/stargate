This directory provides two ways to start Stargate with Cassandra 3.11 using docker-compose.

# Stargate with 3-node Cassandra 3.11 cluster

Use the shell script `start_cass_311.sh`, which performs an orderly startup of containers 
specified in `docker-compose.yml`:

`./start_cass_311.sh`

This script exits when all containers have started or a failure is detected. You can remove
the stack of containers created by executing `docker compose down`.

# Stargate with embedded Cassandra 3.11 (developer mode)

This alternate configuration that runs the Stargate coordinator node in developer mode, so that no
separate Cassandra cluster is required. This can be run with the command:

`./start_cass_311_dev_mode.sh`

You can stop execution of this script with `Ctrl-C` and the stack will be torn down.

# Script options

Both scripts support the following options: 

* You can specify a released image tag (version) using `-t [VERSION]`. Consult [Docker Hub](https://hub.docker.com/r/stargateio/coordinator-3_11/tags) for a list of available tags.

* Alternatively, build the snapshot version locally using instructions in the [developer guide](../../DEV_GUIDE.md) and run the script using the `-l` option.

# Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`),
  and the Cassandra Docker image tag to use (`CASSTAG`).

* The Stargate Docker image tag to use (`SGTAG`) defaults to the current version as defined in the
  top level project `pom.xml` file. It can be overridden with the `-t` option on either script:

  `./start_cass_311.sh -t v2.0.0-BETA-2`

* Running more than one of these multi-container environments on one host may require
  changing the port mapping to be changed to avoid conflicts on the host machine.

# Troubleshooting

If you see an error like:
```
Pulling coordinator (stargateio/coordinator-3_11:2.0.0-BETA-4-SNAPSHOT)...
ERROR: manifest for stargateio/coordinator-3_11:2.0.0-BETA-4-SNAPSHOT not found: manifest unknown: manifest unknown
```

you are trying to deploy a version that is neither publicly available (official release) nor built locally.

