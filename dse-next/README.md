## dse-next Docker image

This folder contains pieces needed to create a Docker image for Stargate integration
tests for `dse-next` Persistence backend, problem being that while `dse-db-all` jar
artifacts exists, no publicly available Docker images do.

Docker image in question can be seen here:

    https://hub.docker.com/r/stargateio/dse-next

## Publishing new `dse-next` Docker image

To publish a new version of `stargateio/dse-next` you need to:

0. Figure out hash version to use (f.ex `<dse.version>` in `pom.xml` of CNDB `vsearch` branch)
1. Update hash version in 4 places of Stargate/v2.1 repo:
    - apis/pom.xml
    - coordinator/persistence-dse-next/pom.xml
    - coordinator/persistence-dse-next/README.md
    - docker-compose/dse-next/.env
2. Go to /dse-next
3. Run ./download-cassandra.sh
    - may need to `rm -rf ./cassandra` first
    - need to use JDK 11
4. Do `docker login`
    - Need to use account with access to `stargateio` at Docker Hub
5. Run ./build-docker-image.sh -p
6. Verify new image at: https://hub.docker.com/r/stargateio/dse-next/tags
