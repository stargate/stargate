This directory provides two ways to start Stargate with Cassandra 3.11 using docker-compose.

# Stargate with 3-node Cassandra 3.11 cluster

Use the shell script `start_cass_311.sh`, which performs an orderly startup of containers 
specified in `docker-compose.yml`:

`./start_cass_311.sh`

# Stargate with embedded Cassandra 3.11 (developer mode)

This alternate configuration that runs the Stargate coordinator node in developer mode, so that no
separate Cassandra cluster is required. This can be run directly with the command:

`docker-compose -f docker-compose-dev-mode.yml up`

# Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`),
  the Cassandra Docker image tag to use (`CASSTAG`), and the Stargate Docker image tag to use
  (`SGTAG`).

* Running more than one of these multi-container environments on one host may require
  changing the port mapping to be changed to avoid conflicts on the host machine.


