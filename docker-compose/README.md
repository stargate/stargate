# Stargate Docker Compose Scripts
This directory provides Docker compose scripts with sample configurations for the various supported Stargate backends. Pick a backend for more detailed instructions:

- [Cassandra 3.11](cassandra-3.11)
- [Cassandra 4.0](cassandra-4.0)
- [DataStax Enterprise 6.8](dse-6.8)

Once you have used one of the above options, give the APIs a try using the Swagger or Playground pages listed below, or use one of the Postman collections from the [DataStax Stargate workspace](https://www.postman.com/datastax/workspace/stargate-cassandra/overview). See the Stargate [documentation](https://stargate.io/docs/latest/develop/tooling.html) for more information.

## Exposed endpoints

Each of the Docker compose scripts starts one Stargate coordinator node and one instance of each API service, exposing the following endpoints on `localhost`:

| Service     | Endpoint URL                                                                                                                                                                                                                                                                                                                                                                                   |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Coordinator | CQL: port 9042 <br>Auth API: http://localhost:8081/v1/auth/token/generate <br>gRPC: http://localhost:8090 <br>Health: http://localhost:8084/checker/liveness, http://localhost:8084/checker/readiness <br>Metrics: http://localhost:8084/metrics <br>REST v1 (port not exposed by default): http://localhost:8082/v1/keyspaces <br>Bridge (port not exposed by default): http://localhost:8091 |
| REST API    | v2 Schema: http://localhost:8082/v2/schemas/keyspaces <br>v2 Keyspaces: http://localhost:8082/v2/keyspaces <br>Swagger: http://localhost:8082/swagger-ui <br> Health: http://localhost:8082/stargate/health <br>Metrics: http://localhost:8082/metrics                                                                                                                                         |
| Docs API    | v2 Schema: http://localhost:8180/v2/schemas/namespaces <br>v2 Namespaces: http://localhost:8180/v2/namespaces <br>Swagger: http://localhost:8180/swagger-ui <br>Health: http://localhost:8180/stargate/health <br>Metrics: http://localhost:8180/metrics                                                                                                                                       |
| GraphQL     | Schema: http://localhost:8080/graphql-schema <br> Admin: http://localhost:8080/graphql-admin <br>Queries: http://localhost:8080/graphql <br>Playground: http://localhost:8080/playground <br>Health: http://localhost:8080/stargate/health <br>Metrics: http://localhost:8080/metrics                                                                                                          |

### Endpoints not exposed by default
As noted in the table above, the Docker compose files do not expose the following ports on the Coordinator by default, but you can enable them as needed:
- port 8082 for the REST API `/v1` endpoints, which were not implemented in the Stargate v2 REST API Service)
- port 8091 for the Bridge service used by the Stargate v2 API services. 

To enable access to these ports from outside the Docker network, you'll need to modify your compose file as follows:

```
  coordinator:
    ...
    ports:
      ...
      - "8082:8082"
      - "8091:8091"
```

## Docker Images
Stargate Docker images are publicly available on [Docker Hub](https://hub.docker.com/r/stargateio/). To learn how the images are built or to build your own local versions, see the [coordinator node developer guide](../DEV_GUIDE.md) or API Service developer guides (under the [apis](apis) directory).

The fastest way to build your own local images involves the following steps:

* Local build of coordinator images
  * Make sure you are in the repository root directory and have `JAVA_HOME` set to point to a JDK 1.8 installation
  * Do a local build of jar files for coordinator:
    ```
    ./mvnw clean install -DskipTests -P dse
    ```
  * Generate docker images (image tag will default to the Stargate version specified in the `pom.xml`):
    ```
    ./build_docker_images.sh 
    ```
* Local build of API images
  * Make sure you are in the `apis` directory and have `JAVA_HOME` set to point to a JDK 17 installation
  * Do a Quarkus build including the option to generate images:
    ```
    ./mvnw clean package -DskipTests -Dquarkus.container-image.build=true
    ```
* Verify the images exist
  ```
  docker images | grep <stargate version>
  ```    
* Select one of the supported backends listed at the top of this page and follow the instructions for using one of the provided convenience script with the `-l` (local) option

## Docker Troubleshooting

If you have problems running Stargate Docker images there are couple of things you can check that might be causing these problems.

### Docker Engine does not have enough memory to run full Stargate

Many Docker engines have default settings for low memory usage. For example:

* Docker Desktop defaults to 2 GB: depending on your set up, you may need to increase this up to 6 GBs (although some developers report 4 GB being sufficient)

### OS may too low limit for File Descriptors

If you see a message like this:

```
Jars will not be watched due to unexpected error: User limit of inotify instances reached or too many open files
```

on Docker logs for Coordinator, you are probably hitting it.

Solution depends on OS you are on; here are some suggestions

#### MacOS: too low FD limit

On MacOS a solution is to increase `maxfiles` limit; you can do that by:

```
sudo launchctl limit maxfiles 999999 999999
```

but note that this setting will not persist over restart.

For more information see f.ex:

* https://wilsonmar.github.io/maximum-limits/
* https://superuser.com/questions/433746/is-there-a-fix-for-the-too-many-open-files-in-system-error-on-os-x-10-7-1

Unfortunately the "permanent" solution appears to be different across different MacOS versions!
